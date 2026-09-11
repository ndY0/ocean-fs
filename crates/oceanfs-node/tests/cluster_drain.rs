//! Integration test: cluster drain (d4, ADR-0036 C1b).
//!
//! A 3-node RF=2 cluster. Node A's data pool is drained in `Cluster`
//! mode over the admin HTTP surface; the `"drain_cluster"` mover
//! re-replicates A's held segments to non-holder nodes (B/C) through the
//! ADR-0030 target-pull machinery and **source-releases** each local copy
//! (self dropped from `storage_locations`, `.dat` unlinked) only after the
//! target confirms a durable copy.
//!
//! Scenarios:
//! - **node-level drain to `Detachable`** — A's held segments land on B/C,
//!   A's `.dat` set empties, self disappears from A's `storage_locations`,
//!   reads of every key keep serving through A (replica failover), the
//!   pool becomes `Detachable`, no restart;
//! - **blocked, no-destructive-failure** — with no eligible target
//!   anywhere the drain parks: pool stays `Draining`, blocked reason
//!   surfaced, zero `.dat` deleted.
//!
//! No load suite is run locally (PIPELINE §6).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{collections::HashSet, path::PathBuf, time::Duration};

use oceanfs_core::{
    MissingRootPolicy, NodeConfig, PoolHealthConfig, PoolRole, PoolTech, StorageConfig,
    StoragePoolConfig,
};
use oceanfs_node::Node;
use oceanfs_storage::{DrainState, PoolStatus};

fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> =
        (0..n).map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0")).collect();
    let ports = listeners.iter().map(|l| l.local_addr().expect("local addr").port()).collect();
    drop(listeners);
    ports
}

struct NodeAddrs {
    http: String,
    grpc: String,
    membership: String,
}

/// Reserves the three explicit ports (HTTP, gRPC, membership) for one
/// node out of a `free_ports(node_count * 3)` block.
///
/// Every listener a node opens must be explicit: if the HTTP listener
/// binds `127.0.0.1:0`, the kernel can hand it a port that `free_ports`
/// just released but another node's gRPC listener has not bound yet —
/// that node then boots with a dead data plane (and its gRPC port is
/// answered by the other node's S3 server). Explicit ports remove the
/// race entirely.
fn node_addrs(ports: &[u16], node: usize) -> NodeAddrs {
    let base = node * 3;
    NodeAddrs {
        http: format!("127.0.0.1:{}", ports[base]),
        grpc: format!("127.0.0.1:{}", ports[base + 1]),
        membership: format!("127.0.0.1:{}", ports[base + 2]),
    }
}

struct Booted {
    node: Node,
    tmp: tempfile::TempDir,
}

/// One data pool per node (id 0), RF=2 (ring replica set = 2 of 3), fast
/// gossip. The d3 intra-node worker is pinned off so it never relocates a
/// cluster-mode pool's segments to a sibling.
async fn boot_node(id: &str, seed: Option<&str>, addrs: &NodeAddrs) -> Booted {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = node_config(id, seed, addrs, tmp.path());
    let node = Node::start(config).await.expect("node boots");
    Booted { node, tmp }
}

/// Builds a node config over an EXISTING data directory (used to restart
/// a node in place, preserving its event-WAL fold).
fn node_config(
    id: &str,
    seed: Option<&str>,
    addrs: &NodeAddrs,
    tmp: &std::path::Path,
) -> NodeConfig {
    let storage = StorageConfig {
        pools: vec![
            StoragePoolConfig {
                name: "data-a".to_string(),
                role: PoolRole::Data,
                root: tmp.join("nvme0"),
                weight: None,
                tech: PoolTech::Auto,
                health: PoolHealthConfig {
                    min_errors: 1,
                    detection_window_secs: 1,
                    recovery_window_secs: 1,
                    ..PoolHealthConfig::default()
                },
            },
            StoragePoolConfig {
                name: "journal".to_string(),
                role: PoolRole::Wal,
                root: tmp.join("optane0"),
                weight: None,
                tech: PoolTech::Auto,
                health: PoolHealthConfig::default(),
            },
            StoragePoolConfig {
                name: "meta".to_string(),
                role: PoolRole::Metadata,
                root: tmp.join("optane1"),
                weight: None,
                tech: PoolTech::Auto,
                health: PoolHealthConfig::default(),
            },
            StoragePoolConfig {
                name: "hints".to_string(),
                role: PoolRole::Hints,
                root: tmp.join("hints0"),
                weight: None,
                tech: PoolTech::Auto,
                health: PoolHealthConfig::default(),
            },
        ],
        missing_root_policy: MissingRootPolicy::Fatal,
    };
    NodeConfig {
        node_id: id.to_string(),
        data_dir: tmp.join("data"),
        listen_addr: addrs.http.clone(),
        grpc_listen_addr: addrs.grpc.clone(),
        membership_listen_addr: addrs.membership.clone(),
        storage,
        // No g3 announcements: the drain is the only mover in the test and
        // g4 reconciliation stays the safety net (source-release is what
        // must not be undone).
        announcements_enabled: false,
        replication_factor: 2,
        gossip: oceanfs_core::GossipConfig {
            interval_ms: 250,
            suspicion_timeout_ms: 60_000,
            failure_timeout_ms: 120_000,
            seed_nodes: seed.map(|s| vec![s.to_string()]).unwrap_or_default(),
            ..Default::default()
        },
        durability: oceanfs_core::DurabilityConfig {
            drain_interval_sec: 3600,
            ..oceanfs_core::DurabilityConfig::default()
        },
        ..NodeConfig::default()
    }
}

fn pool_root(booted: &Booted) -> PathBuf {
    booted.tmp.path().join("nvme0")
}

async fn wait_for_cluster_convergence_to(node: &Node, expected: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let ring_nodes = node.segment_replicator().ring_node_count();
        if ring_nodes >= expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cluster must converge to {expected} nodes within 30s (ring has {ring_nodes})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_cluster_convergence(node: &Node) {
    wait_for_cluster_convergence_to(node, 3).await;
}

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

async fn wait_until(label: &str, deadline_s: u64, mut predicate: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(deadline_s);
    while !predicate() {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for: {label}");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// PUT batches until A holds at least one sealed segment (ring sets are
/// hash-derived; a batch can land entirely off A).
async fn seed_on_a(
    root_a: &std::path::Path,
    addr_a: std::net::SocketAddr,
    client: &reqwest::Client,
    body: &[u8],
) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    for batch in 0..4 {
        let batch_keys: Vec<String> = (0..8).map(|i| format!("obj-{batch}-{i:02}")).collect();
        for key in &batch_keys {
            let resp = client
                .put(format!("http://{addr_a}/durability/{key}"))
                .body(body.to_vec())
                .send()
                .await
                .expect("PUT must succeed");
            assert_eq!(resp.status(), 200, "PUT {key} returns 200");
        }
        keys.extend(batch_keys);
        if !segment_ids_in(root_a).is_empty() {
            return keys;
        }
        assert!(batch < 3, "A must hold at least one sealed segment after seeding");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    keys
}

/// Waits for replication to settle (replicators drained + file sets
/// stable) and returns the set of segments A holds.
async fn settle_replication(
    nodes: &[&Node],
    roots: &[&PathBuf],
) -> HashSet<oceanfs_core::SegmentId> {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut prev: Option<Vec<HashSet<oceanfs_core::SegmentId>>> = None;
    let mut stable = 0u32;
    loop {
        let drained = nodes.iter().all(|n| n.segment_replicator().needs_len() == 0);
        let snap: Vec<HashSet<oceanfs_core::SegmentId>> =
            roots.iter().map(|r| segment_ids_in(r)).collect();
        if drained && prev.as_ref() == Some(&snap) {
            stable += 1;
            if stable >= 3 {
                return snap[0].clone();
            }
        } else {
            stable = 0;
            prev = Some(snap);
        }
        assert!(std::time::Instant::now() < deadline, "replication must settle within 60s");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn put_and_read_back(
    addr_a: std::net::SocketAddr,
    client: &reqwest::Client,
    keys: &[String],
    body: &[u8],
) {
    for key in keys {
        let resp = client
            .get(format!("http://{addr_a}/durability/{key}"))
            .send()
            .await
            .expect("GET must succeed");
        assert_eq!(resp.status(), 200, "GET {key} returns 200");
        let got = resp.bytes().await.expect("body");
        assert_eq!(&got[..], body, "object {key} is byte-identical");
    }
}

/// Scenario 1: node-level cluster drain to Detachable with live reads.
#[tokio::test]
async fn node_level_cluster_drain_serves_reads_to_detachable() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let ports = free_ports(9);
    let a_addrs = node_addrs(&ports, 0);
    let b_addrs = node_addrs(&ports, 1);
    let c_addrs = node_addrs(&ports, 2);
    let booted_a = boot_node("node-a", None, &a_addrs).await;
    let booted_b = boot_node("node-b", Some(&a_addrs.membership), &b_addrs).await;
    let booted_c = boot_node("node-c", Some(&a_addrs.membership), &c_addrs).await;

    wait_for_cluster_convergence(&booted_a.node).await;
    wait_for_cluster_convergence(&booted_b.node).await;
    wait_for_cluster_convergence(&booted_c.node).await;

    let client = reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("c");
    let addr_a = booted_a.node.server_addr();
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let root_a = pool_root(&booted_a);
    let root_b = pool_root(&booted_b);
    let root_c = pool_root(&booted_c);
    let keys = seed_on_a(&root_a, addr_a, &client, &body).await;
    put_and_read_back(addr_a, &client, &keys, &body).await;

    let held_a = settle_replication(
        &[&booted_a.node, &booted_b.node, &booted_c.node],
        &[&root_a, &root_b, &root_c],
    )
    .await;
    assert!(!held_a.is_empty(), "A holds sealed segments to drain");

    // ---- Begin the node-level cluster drain over the admin route ----
    let begin = client
        .post(format!("http://{addr_a}/admin/nodes/node-a/drain"))
        .send()
        .await
        .expect("begin node drain");
    assert_eq!(begin.status(), 202, "node drain returns 202");
    let begin_json: serde_json::Value = begin.json().await.expect("node drain JSON");
    assert_eq!(begin_json["mode"], "cluster");
    assert!(booted_a.node.pool_registry().is_draining(0));

    // Drive the cluster controller directly (deterministic) until the pool
    // becomes Detachable — dispatch → durable gate → source-release against
    // the real B/C workers.
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    while booted_a.node.pool_registry().drain_state(0) != DrainState::Detachable {
        assert!(
            std::time::Instant::now() < deadline,
            "cluster drain must empty pool 0 within 180s"
        );
        let stats = booted_a.node.cluster_drain().run_drain_cycle().await;
        if stats.released == 0 && stats.blocked.is_empty() {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    // A source-released every held segment: .dat gone from A, present on
    // B or C; self dropped from A's own storage_locations.
    assert!(segment_ids_in(&root_a).is_empty(), "A's data root is empty after the drain");
    for sid in &held_a {
        let on_b = segment_ids_in(&root_b).contains(sid);
        let on_c = segment_ids_in(&root_c).contains(sid);
        assert!(on_b || on_c, "drained segment {sid} exists on B or C");
        wait_until(&format!("segment {sid} self-drops on A"), 60, || {
            booted_a
                .node
                .segment_locations(sid)
                .map(|locs| !locs.iter().any(|n| n.as_str() == "node-a"))
                .unwrap_or(false)
        })
        .await;
    }

    assert_eq!(
        booted_a.node.pool_registry().drain_state(0),
        DrainState::Detachable,
        "pool 0 is Detachable after the drain"
    );
    assert_eq!(
        booted_a.node.pool_registry().pool_by_id(0).unwrap().status(),
        PoolStatus::Draining,
        "a Detachable pool's status stays Draining"
    );

    // Reads keep serving through A (replica failover) — no data loss.
    put_and_read_back(addr_a, &client, &keys, &body).await;

    booted_a.node.shutdown().await.expect("A shutdown");
    booted_b.node.shutdown().await.expect("B shutdown");
    booted_c.node.shutdown().await.expect("C shutdown");
}

/// Scenario 2: blocked drain — no eligible target anywhere ⇒ parked +
/// reason surfaced + zero deletes.
#[tokio::test]
async fn blocked_cluster_drain_deletes_nothing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let ports = free_ports(6);
    let a_addrs = node_addrs(&ports, 0);
    let b_addrs = node_addrs(&ports, 1);
    let booted_a = boot_node("node-a", None, &a_addrs).await;
    let booted_b = boot_node("node-b", Some(&a_addrs.membership), &b_addrs).await;

    wait_for_cluster_convergence_to(&booted_a.node, 2).await;
    wait_for_cluster_convergence_to(&booted_b.node, 2).await;

    let client = reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("c");
    let addr_a = booted_a.node.server_addr();
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let root_a = pool_root(&booted_a);
    let root_b = pool_root(&booted_b);
    let keys = seed_on_a(&root_a, addr_a, &client, &body).await;
    let held_a = settle_replication(&[&booted_a.node, &booted_b.node], &[&root_a, &root_b]).await;
    assert!(!held_a.is_empty(), "A holds sealed segments to drain");

    // The only other node leaves: no eligible target remains.
    booted_b.node.shutdown().await.expect("B leaves");
    tokio::time::sleep(Duration::from_secs(4)).await;

    let begin = client
        .post(format!("http://{addr_a}/admin/pools/0/drain"))
        .json(&serde_json::json!({ "mode": "cluster" }))
        .send()
        .await
        .expect("begin pool drain");
    assert_eq!(begin.status(), 202, "cluster pool drain begins");

    let dats_before = segment_ids_in(&root_a).len();
    // Drive the controller: no target ⇒ the pool parks blocked; nothing is
    // deleted and nothing half-removed.
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    while !booted_a.node.pool_registry().drain_state(0).is_blocked() {
        assert!(
            std::time::Instant::now() < deadline,
            "drain must park blocked when no target exists"
        );
        booted_a.node.cluster_drain().run_drain_cycle().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert_eq!(segment_ids_in(&root_a).len(), dats_before, "zero .dat deleted while blocked");
    assert_eq!(
        booted_a.node.pool_registry().pool_by_id(0).unwrap().status(),
        PoolStatus::Draining,
        "a blocked drain keeps the pool Draining"
    );
    assert!(
        booted_a.node.pool_registry().drain_state(0).blocked_reason().is_some(),
        "the blocked reason is surfaced"
    );
    // Data still served from A while it holds the copies.
    put_and_read_back(addr_a, &client, &keys, &body).await;

    booted_a.node.shutdown().await.expect("A shutdown");
}

/// Scenario 3: restart mid-drain. A stops while a drain is in flight
/// (nothing released yet — it still holds every copy), restarts over the
/// same data dir, and a re-issued drain completes idempotently. The
/// holder-set stamps made durable in the d4 fix (event-WAL
/// `MetadataRefresh`) are what let the restarted node recognize the
/// segments it still holds.
#[tokio::test]
async fn cluster_drain_survives_a_restart_mid_drain() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let ports = free_ports(9);
    let a_addrs = node_addrs(&ports, 0);
    let b_addrs = node_addrs(&ports, 1);
    let c_addrs = node_addrs(&ports, 2);
    let booted_a = boot_node("node-a", None, &a_addrs).await;
    let booted_b = boot_node("node-b", Some(&a_addrs.membership), &b_addrs).await;
    let booted_c = boot_node("node-c", Some(&a_addrs.membership), &c_addrs).await;

    wait_for_cluster_convergence(&booted_a.node).await;
    wait_for_cluster_convergence(&booted_b.node).await;
    wait_for_cluster_convergence(&booted_c.node).await;

    let client = reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("c");
    let addr_a = booted_a.node.server_addr();
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let root_a = pool_root(&booted_a);
    let root_b = pool_root(&booted_b);
    let root_c = pool_root(&booted_c);
    let keys = seed_on_a(&root_a, addr_a, &client, &body).await;
    let held_a = settle_replication(
        &[&booted_a.node, &booted_b.node, &booted_c.node],
        &[&root_a, &root_b, &root_c],
    )
    .await;
    assert!(!held_a.is_empty());

    // Begin the node drain, then stop A before any segment is released —
    // the drain intent is in flight at the stop. A still holds every copy.
    let begin = client
        .post(format!("http://{addr_a}/admin/nodes/node-a/drain"))
        .send()
        .await
        .expect("begin node drain");
    assert_eq!(begin.status(), 202);
    booted_a.node.shutdown().await.expect("A stops mid-drain");

    // Restart A over the same data dir (same node id + membership seed).
    let a2 = Node::start(node_config(
        "node-a",
        Some(&b_addrs.membership),
        &a_addrs,
        &booted_a.tmp.path(),
    ))
    .await
    .expect("A restarts");
    let booted_a2 = Booted { node: a2, tmp: booted_a.tmp };
    wait_for_cluster_convergence_to(&booted_a2.node, 3).await;
    let addr_a2 = booted_a2.node.server_addr();

    // The restarted node must still recognize every pre-restart held
    // segment as held by itself (the durable holder stamp folded back).
    for sid in &held_a {
        wait_until(&format!("segment {sid} self-listed on restarted A"), 60, || {
            booted_a2
                .node
                .segment_locations(sid)
                .map(|locs| locs.iter().any(|n| n.as_str() == "node-a"))
                .unwrap_or(false)
        })
        .await;
    }

    // The drain intent is ephemeral (registry state, rebuilt from config
    // at boot): re-issue it, then drive to completion.
    let begin2 = client
        .post(format!("http://{addr_a2}/admin/nodes/node-a/drain"))
        .send()
        .await
        .expect("re-issue node drain");
    assert_eq!(begin2.status(), 202);

    let root_a2 = booted_a2.tmp.path().join("nvme0");
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    while booted_a2.node.pool_registry().drain_state(0) != DrainState::Detachable {
        assert!(
            std::time::Instant::now() < deadline,
            "re-issued drain must empty pool 0 within 180s"
        );
        booted_a2.node.cluster_drain().run_drain_cycle().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(segment_ids_in(&root_a2).is_empty(), "A's data root is empty after re-issue");
    for sid in &held_a {
        let on_b = segment_ids_in(&root_b).contains(sid);
        let on_c = segment_ids_in(&root_c).contains(sid);
        assert!(on_b || on_c, "drained segment {sid} exists on B or C after the restart");
    }
    // Zero data loss: every object reads byte-identical through A again.
    for key in &keys {
        let resp = client
            .get(format!("http://{addr_a2}/durability/{key}"))
            .send()
            .await
            .expect("GET after restart drain");
        assert_eq!(resp.status(), 200, "GET {key} serves after the restart");
        let got = resp.bytes().await.expect("body");
        assert_eq!(&got[..], &body[..], "object {key} is byte-identical after the restart");
    }

    booted_a2.node.shutdown().await.expect("A2 shutdown");
    booted_b.node.shutdown().await.expect("B shutdown");
    booted_c.node.shutdown().await.expect("C shutdown");
}
