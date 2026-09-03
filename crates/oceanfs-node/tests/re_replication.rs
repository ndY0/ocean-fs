//! Integration test (g5 `re-replication-worker`, ADR-0030 target-pull):
//! a killed data pool's segments return to RF via the holder-side
//! dispatcher → `RequestReReplication` RPC → acquiring node's
//! `ReRepWorker` (pull + write + register + stamp).
//!
//! Scenario (RF=2, 3 nodes): each sealed segment's ring replica set is a
//! hash-derived 2 of the 3 nodes. When A's data pool dies, a segment
//! whose replica set contained A loses one copy → it is held by exactly
//! ONE surviving node pre-kill; that holder dispatches to the
//! non-holder, which pulls + writes + registers + stamps. Segments whose
//! replica set was {B, C} are untouched (still 2 live copies).
//!
//! The test waits for all replication to SETTLE (every node's replicator
//! drained + stable file sets), snapshots the pre-kill holder sets, and
//! derives the at-risk subset — segments held by exactly one of B/C
//! (their A-copy's death drops them below RF). It REQUIRES the subset to
//! be non-empty — a run that happens to place every segment's replicas
//! on {B, C} FAILS loudly instead of passing vacuously — and asserts
//! every at-risk segment converges to both B and C (file + registry)
//! after the repair. Reads of the affected keys serve without data loss.
//!
//! Two paths (the epic's "re-replication restores RF" DoD):
//!   1. announcement path — `announcements_enabled = true` (g3);
//!   2. reconciliation path — `announcements_enabled = false` (g4 alone).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{collections::HashSet, time::Duration};

use oceanfs_core::{
    MissingRootPolicy, NodeConfig, PoolHealthConfig, PoolRole, PoolTech, StorageConfig,
    StoragePoolConfig,
};
use oceanfs_node::Node;
use oceanfs_storage::{io::IoErrorKind, PoolStatus};

fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> =
        (0..n).map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0")).collect();
    let ports = listeners.iter().map(|l| l.local_addr().expect("local addr").port()).collect();
    drop(listeners);
    ports
}

struct NodeAddrs {
    grpc: String,
    membership: String,
}

/// One data pool per node (id 0), RF=2 (the segment's ring replica set
/// is 2 of 3 nodes), fast health knobs, fast GC off.
async fn boot_node(
    id: &str,
    seed: Option<&str>,
    addrs: &NodeAddrs,
    announcements: bool,
) -> (Node, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig {
        pools: vec![StoragePoolConfig {
            name: "data-a".to_string(),
            role: PoolRole::Data,
            root: tmp.path().join("nvme0"),
            weight: None,
            tech: PoolTech::Auto,
            health: PoolHealthConfig {
                min_errors: 1,
                detection_window_secs: 1,
                recovery_window_secs: 1,
                ..PoolHealthConfig::default()
            },
        }],
        missing_root_policy: MissingRootPolicy::Fatal,
    };
    let config = NodeConfig {
        node_id: id.to_string(),
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: addrs.grpc.clone(),
        membership_listen_addr: addrs.membership.clone(),
        storage,
        announcements_enabled: announcements,
        // RF=2: the segment's ring replica set has exactly 2 nodes, so a
        // third node is always a viable acquiring target.
        replication_factor: 2,
        gossip: oceanfs_core::GossipConfig {
            interval_ms: 250,
            suspicion_timeout_ms: 60_000,
            failure_timeout_ms: 120_000,
            seed_nodes: seed.map(|s| vec![s.to_string()]).unwrap_or_default(),
            ..Default::default()
        },
        ..NodeConfig::default()
    };
    let node = Node::start(config).await.expect("node boots");
    (node, tmp)
}

async fn wait_for_cluster_convergence(node: &Node) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let ring_nodes = node.segment_replicator().ring_node_count();
        if ring_nodes >= 3 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cluster must converge to 3 nodes within 30s (ring has {ring_nodes})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Drives the pool Healthy → Degraded → Dead via the health monitor.
async fn kill_data_pool(node: &Node, pool_id: u32) {
    for _ in 0..3 {
        node.io_observer().record_error(pool_id, IoErrorKind::TimedOut);
        node.io_observer().record_latency(
            pool_id,
            oceanfs_storage::io::IoOp::Read,
            Duration::from_micros(1),
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let status = node.pool_registry().pool_by_id(pool_id).expect("pool registered").status();
        if status == PoolStatus::Degraded {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "data pool must reach Degraded within 15s (status: {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for _ in 0..3 {
        node.io_observer().record_error(pool_id, IoErrorKind::NotFound);
        node.io_observer().record_latency(
            pool_id,
            oceanfs_storage::io::IoOp::Read,
            Duration::from_micros(1),
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let status = node.pool_registry().pool_by_id(pool_id).expect("pool registered").status();
        if status == PoolStatus::Dead {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "data pool must reach Dead within 15s (status: {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The set of segment ids present as `.dat` files in a node's segment
/// directory (pool root).
fn segment_ids_in(dir: &std::path::Path) -> HashSet<oceanfs_core::SegmentId> {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().to_string();
                    name.strip_suffix(".dat").map(|s| s.to_string())
                })
                .filter_map(|s| {
                    uuid::Uuid::parse_str(&s)
                        .ok()
                        .map(|u| oceanfs_core::SegmentId::from_uuid_bytes(u.into_bytes()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Counts `.dat` files in a node's segment directory.
async fn segment_file_count(dir: &std::path::Path) -> usize {
    segment_ids_in(dir).len()
}

/// One end-to-end scenario shared by both paths.
///
/// `announcements` selects the detector: true → g3 fast path,
/// false → g4 reconciliation alone.
async fn run_repair_scenario(announcements: bool) {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let ports = free_ports(6);
    let a_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[0]),
        membership: format!("127.0.0.1:{}", ports[1]),
    };
    let b_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[2]),
        membership: format!("127.0.0.1:{}", ports[3]),
    };
    let c_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[4]),
        membership: format!("127.0.0.1:{}", ports[5]),
    };
    let (node_a, tmp_a) = boot_node("node-a", None, &a_addrs, announcements).await;
    let (node_b, tmp_b) =
        boot_node("node-b", Some(&a_addrs.membership), &b_addrs, announcements).await;
    let (node_c, tmp_c) =
        boot_node("node-c", Some(&a_addrs.membership), &c_addrs, announcements).await;

    wait_for_cluster_convergence(&node_a).await;
    wait_for_cluster_convergence(&node_b).await;
    wait_for_cluster_convergence(&node_c).await;

    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("client");
    let addr_a = node_a.server_addr();

    // ---- PUT objects on A in batches until the at-risk precondition holds ----
    // A segment's ring replica set is hash-derived from its (random)
    // segment id — per segment, P(excludes A) ≈ 1/3. A single batch can
    // therefore land every segment on {B, C} (nothing at risk — the old
    // vacuous pass). Instead of failing, keep adding batches until at
    // least one segment is held by exactly one of B/C (the loud
    // precondition below), capped at a few batches.
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let mut keys: Vec<String> = Vec::new();
    let segments_dir_a = tmp_a.path().join("nvme0");
    let segments_dir_b = tmp_b.path().join("nvme0");
    let segments_dir_c = tmp_c.path().join("nvme0");
    // First batch with a non-empty at-risk set wins; later batches are
    // only added while every segment's ring set landed on {B, C}
    // (nothing at risk — the old vacuous pass).
    let mut at_risk: Vec<oceanfs_core::SegmentId> = Vec::new();
    for batch in 0..4 {
        let batch_keys: Vec<String> = (0..12).map(|i| format!("obj-{batch}-{i:02}")).collect();
        let mut handles = Vec::new();
        for key in &batch_keys {
            let client = client.clone();
            let body = body.clone();
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                let resp = client
                    .put(format!("http://{addr_a}/durability/{key}"))
                    .body(body)
                    .send()
                    .await
                    .expect("PUT must succeed");
                assert_eq!(resp.status(), 200, "PUT {key} returns 200");
            }));
        }
        for h in handles {
            h.await.expect("PUT task");
        }
        keys.extend(batch_keys);

        // ---- Wait for seals + replication to land ----
        // In pool mode, segment files live under the DATA POOL ROOT
        // (`nvme0`), not `data/segments`. A must hold at least one
        // sealed segment for the kill to be meaningful.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if segment_file_count(&segments_dir_a).await >= 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "owner must seal at least 1 segment within 30s"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // ---- Replication SETTLE, then snapshot ----
        // With RF=2 the ring places each segment on a hash-derived 2 of
        // 3 nodes: A's segments are NOT all pushed to both B and C. The
        // honest pre-kill snapshot waits for quiescence — every node's
        // replicator drained AND the B/C file sets stable across
        // consecutive polls — then records whatever the ring delivered.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let mut prev_snapshot: Option<(
            HashSet<oceanfs_core::SegmentId>,
            HashSet<oceanfs_core::SegmentId>,
        )> = None;
        let mut stable_polls = 0u32;
        loop {
            let drained = node_a.segment_replicator().needs_len() == 0
                && node_b.segment_replicator().needs_len() == 0
                && node_c.segment_replicator().needs_len() == 0;
            let snapshot = (segment_ids_in(&segments_dir_b), segment_ids_in(&segments_dir_c));
            if drained && prev_snapshot.as_ref() == Some(&snapshot) {
                stable_polls += 1;
                if stable_polls >= 3 {
                    break;
                }
            } else {
                stable_polls = 0;
                prev_snapshot = Some(snapshot);
            }
            assert!(
                std::time::Instant::now() < deadline,
                "replication must settle within 60s (B: {} files, C: {} files, \
                 drains: a={} b={} c={})",
                segment_file_count(&segments_dir_b).await,
                segment_file_count(&segments_dir_c).await,
                node_a.segment_replicator().needs_len(),
                node_b.segment_replicator().needs_len(),
                node_c.segment_replicator().needs_len(),
            );
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let held_by_b = segment_ids_in(&segments_dir_b);
        let held_by_c = segment_ids_in(&segments_dir_c);
        // The at-risk subset: segments NOT held by both B and C (held by
        // exactly one) → their A-copy's death drops them below RF and
        // the surviving holder must re-replicate them onto the other.
        at_risk = held_by_b.symmetric_difference(&held_by_c).copied().collect();
        if !at_risk.is_empty() {
            break;
        }
        tracing::info!(
            "batch {batch}: every segment's ring set landed on {{B, C}}; adding another batch"
        );
    }
    assert!(
        !at_risk.is_empty(),
        "test precondition failed: no segment held by exactly one of B/C after 4 batches; \
         re-run (the flow was not exercised)"
    );

    // ---- Kill A's data pool (pool id 0) ----
    // The pool goes Dead; the consequence applier derives the affected
    // segments. The at-risk segments are now held by exactly ONE live
    // node (B or C); that holder dispatches to the non-holder.
    kill_data_pool(&node_a, 0).await;

    // ---- The holder's dispatcher → RPC → acquiring node's worker ----
    // Each at-risk segment is re-replicated onto the node that did NOT
    // hold it pre-kill. Assert BOTH nodes converge to hold every
    // at-risk segment's file (per-segment — a plain count can pass with
    // the wrong files when B/C hold unrelated local segments).
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let b_now = segment_ids_in(&segments_dir_b);
        let c_now = segment_ids_in(&segments_dir_c);
        if at_risk.iter().all(|sid| b_now.contains(sid) && c_now.contains(sid)) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "every at-risk segment must land on both B and C within 60s (missing from B: {} / \
             C: {} / at-risk: {})",
            at_risk.iter().filter(|sid| !b_now.contains(sid)).count(),
            at_risk.iter().filter(|sid| !c_now.contains(sid)).count(),
            at_risk.len(),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // ---- `storage_locations` converges ----
    // The acquiring node's registry entry for each at-risk segment lists
    // ITSELF (the worker stamps it durably); the holder's entry lists
    // the target too (the dispatcher converges its own registry,
    // ADR-0030 Decision 3). Poll until every at-risk segment's holder
    // set on BOTH nodes covers both B and C.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut converged = true;
        for sid in &at_risk {
            let b_locs = node_b.segment_locations(sid).unwrap_or_default();
            let c_locs = node_c.segment_locations(sid).unwrap_or_default();
            let b_has_b = b_locs.iter().any(|n| n.as_str() == "node-b");
            let b_has_c = b_locs.iter().any(|n| n.as_str() == "node-c");
            let c_has_b = c_locs.iter().any(|n| n.as_str() == "node-b");
            let c_has_c = c_locs.iter().any(|n| n.as_str() == "node-c");
            if !(b_has_b && b_has_c && c_has_b && c_has_c) {
                converged = false;
                break;
            }
        }
        if converged {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "storage_locations must converge: every at-risk segment's holder set on B and C \
             must cover {{node-b, node-c}}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // ---- Reads serve byte-identical data through A (the dead-pool node) ----
    // A's read path falls back to the segment's live replicas — the
    // re-replicated copy is part of the converged replica set.
    for key in &keys {
        let resp = client
            .get(format!("http://{addr_a}/durability/{key}"))
            .send()
            .await
            .expect("GET must succeed");
        assert_eq!(resp.status(), 200, "GET {key} through the owner must succeed after repair");
        let got = resp.bytes().await.expect("read body");
        assert_eq!(&got[..], &body[..], "object {key} must be byte-identical after re-replication");
    }

    node_a.shutdown().await.expect("node A shutdown");
    node_b.shutdown().await.expect("node B shutdown");
    node_c.shutdown().await.expect("node C shutdown");
}

/// Announcement path (g3): A's pool death → A announces to the holder →
/// the holder dispatches the RequestReReplication RPC.
#[tokio::test]
async fn re_replication_restores_rf_via_announcement() {
    run_repair_scenario(true).await;
}

/// Reconciliation path (g4): announcements disabled — the reconciliation
/// loop alone detects the under-replication and drives the dispatch.
#[tokio::test]
async fn re_replication_restores_rf_via_reconciliation_alone() {
    run_repair_scenario(false).await;
}
