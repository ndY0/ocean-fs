//! Integration test (g5 `re-replication-worker`, ADR-0030 target-pull):
//! a killed data pool's segments return to RF via the holder-side
//! dispatcher → `RequestReReplication` RPC → acquiring node's
//! `ReRepWorker` (pull + write + register + stamp).
//!
//! Scenario (RF=2, 3 nodes): each sealed segment's ring replica set is
//! 2 of the 3 nodes. When A's data pool dies:
//!   - a segment whose replica set included A loses one copy → it is
//!     held by exactly ONE surviving node (the other replica holder);
//!     that holder dispatches to the non-holder, which pulls + writes +
//!     registers + stamps;
//!   - a segment whose replica set was {B, C} is untouched (still 2
//!     live copies).
//!
//! The test tracks the pre-kill holder set per segment, REQUIRES the
//! at-risk subset (held by exactly one of B/C) to be non-empty — so a
//! run that happens to place every segment's replicas on {B, C} FAILS
//! loudly instead of passing vacuously — and asserts every at-risk
//! segment converges to both B and C (file + registry) after the repair.
//! Reads of the affected keys serve without data loss.
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

/// Waits until the node's segment directory holds `expected` `.dat`
/// files (the re-replication landed on this node's store).
async fn wait_for_segment_files(dir: &std::path::Path, expected: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let count = segment_file_count(dir).await;
        if count >= expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "target store must receive {expected} segments within 60s (has {count})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
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

    // ---- PUT 12 × 32 KiB objects concurrently on A ----
    // Enough segments that at least one has A in its ring replica set
    // (the at-risk precondition below) — the test FAILS loudly if the
    // under-replicated subset is empty, so a pass always exercises the
    // target-pull flow.
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let keys: Vec<String> = (0..12).map(|i| format!("obj-{i:02}")).collect();
    let mut handles = Vec::new();
    for key in &keys {
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

    // ---- Wait for seals + replication to land on the RF=2 replicas ----
    // In pool mode, segment files live under the DATA POOL ROOT
    // (`nvme0`), not `data/segments`.
    let segments_dir_a = tmp_a.path().join("nvme0");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let owner_segment_count = loop {
        let count = segment_file_count(&segments_dir_a).await;
        if count >= 2 {
            break count;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "owner must seal at least 2 segments within 60s (has {count})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    // The owner's replicator drained → the RF=2 replica set is stamped.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if node_a.segment_replicator().needs_len() == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replicator must drain before the pool is killed"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(owner_segment_count >= 2);

    // ---- Track the pre-kill holder set per segment ----
    // A segment whose ring replica set was {A, X} (X = B or C) is held
    // by exactly one of B/C pre-kill; killing A leaves it with ONE live
    // copy → it must be re-replicated onto the OTHER of B/C. A segment
    // whose replica set was {B, C} is held by both pre-kill → untouched.
    let segments_dir_b = tmp_b.path().join("nvme0");
    let segments_dir_c = tmp_c.path().join("nvme0");
    wait_for_segment_files(&segments_dir_b, owner_segment_count).await;
    wait_for_segment_files(&segments_dir_c, owner_segment_count).await;
    let held_by_b = segment_ids_in(&segments_dir_b);
    let held_by_c = segment_ids_in(&segments_dir_c);
    // The at-risk subset: segments NOT held by both B and C pre-kill
    // (held by exactly one) → they lose a copy when A dies.
    let at_risk: Vec<oceanfs_core::SegmentId> =
        held_by_b.symmetric_difference(&held_by_c).copied().collect();
    assert!(
        !at_risk.is_empty(),
        "test precondition failed: every segment's ring replica set landed on {{B, C}}; \
         re-run (the flow was not exercised). Held by B only: {} / C only: {}",
        held_by_b.difference(&held_by_c).count(),
        held_by_c.difference(&held_by_b).count(),
    );

    // ---- Kill A's data pool (pool id 0) ----
    // The pool goes Dead; the consequence applier derives the affected
    // segments. The at-risk segments are now held by exactly ONE live
    // node (B or C); that holder dispatches to the non-holder.
    kill_data_pool(&node_a, 0).await;

    // ---- The holder's dispatcher → RPC → acquiring node's worker ----
    // Each at-risk segment is re-replicated onto the node that did NOT
    // hold it pre-kill. Assert both B and C converge to hold every
    // segment (RF=2 live copies: the pre-existing holder + the target).
    wait_for_segment_files(&segments_dir_b, owner_segment_count).await;
    wait_for_segment_files(&segments_dir_c, owner_segment_count).await;

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
