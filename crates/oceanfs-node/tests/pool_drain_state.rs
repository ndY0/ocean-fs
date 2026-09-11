//! Integration test: pool drain state (d1, ADR-0036 D6).
//!
//! A live node runs real S3 cycles against its data pools; the operator
//! marks a data pool draining through the node seam. Assertions:
//! - **read-through-drain**: GETs of objects whose segments live on the
//!   draining pool keep serving (the read path is status-agnostic);
//! - **placement exclusion**: once a pool is `Draining`, new sealed
//!   segments land only on the healthy sibling — never on the draining
//!   root;
//! - **manifest re-gossip**: the node's `NodeManifest` carries the
//!   `"draining"` row after `Node::begin_pool_drain` (peers treat it as
//!   not-a-placement-target through the existing manifest-aware seams);
//! - **no-destructive-failure**: a blocked drain (no eligible target)
//!   keeps the pool `Draining`, surfaces the blocked reason in the admin
//!   status route + `oceanfs_pool_drain_blocked_reason`, and deletes no
//!   `.dat`;
//! - **single-last-data-pool precondition** (d4): a node whose only data
//!   pool is draining keeps serving reads and manifests zero healthy data
//!   pools.
//!
//! The mover workers are d3/d4 — d1 provides state only; blocked
//! scenarios are driven directly through the registry.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};

use oceanfs_core::{
    MissingRootPolicy, NodeConfig, PoolRole, PoolTech, StorageConfig, StoragePoolConfig,
};
use oceanfs_node::Node;

fn pool(name: &str, role: PoolRole, root: &Path) -> StoragePoolConfig {
    StoragePoolConfig {
        name: name.to_string(),
        role,
        root: root.to_path_buf(),
        weight: Some(1),
        tech: PoolTech::Auto,
        health: Default::default(),
    }
}

/// A role-complete topology (data×`data_pools`, wal, metadata, hints)
/// with sibling roots; returns the config + the data roots in id order.
fn config_with_data_pools(
    tmp: &tempfile::TempDir,
    data_pools: usize,
) -> (NodeConfig, Vec<PathBuf>) {
    let data_dir = tmp.path().join("data");
    let mut pools = Vec::new();
    let mut data_roots = Vec::new();
    for index in 0..data_pools {
        let root = tmp.path().join(format!("nvme{index}"));
        pools.push(pool(&format!("data-{index}"), PoolRole::Data, &root));
        data_roots.push(root);
    }
    pools.push(pool("journal", PoolRole::Wal, &tmp.path().join("optane0")));
    pools.push(pool("meta", PoolRole::Metadata, &tmp.path().join("optane1")));
    pools.push(pool("hints", PoolRole::Hints, &tmp.path().join("hints0")));

    let mut config = NodeConfig {
        data_dir,
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: "127.0.0.1:0".into(),
        membership_listen_addr: "127.0.0.1:0".into(),
        storage: StorageConfig {
            pools,
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        },
        ..NodeConfig::default()
    };
    // d1 is the drain-STATE feature: it asserts blocked/no-delete and
    // static placement while a pool is Draining. Since d3 registers a live
    // `"drain_intra"` Tier-1 task (default 1 s cadence), these scenarios
    // pin the worker effectively off so its relocation can never race the
    // state assertions mid-scenario.
    config.durability.drain_interval_sec = 3600;
    (config, data_roots)
}

/// The `.dat` segment files directly under a pool root.
fn root_dats(root: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(root)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|ext| ext == "dat"))
                .collect()
        })
        .unwrap_or_default()
}

async fn put(client: &reqwest::Client, addr: &std::net::SocketAddr, key: &str, body: Vec<u8>) {
    let resp = client
        .put(format!("http://{addr}/bucket/{key}"))
        .body(body)
        .send()
        .await
        .expect("PUT must succeed");
    assert_eq!(resp.status(), 200, "PUT {key} returns 200");
}

async fn get_ok(client: &reqwest::Client, addr: &std::net::SocketAddr, key: &str, size: usize) {
    let resp =
        client.get(format!("http://{addr}/bucket/{key}")).send().await.expect("GET must succeed");
    assert_eq!(resp.status(), 200, "GET {key} returns 200");
    assert_eq!(resp.bytes().await.expect("body").len(), size, "GET {key} body size");
}

/// Steers placement to the given data pool id (f2's capacity override).
///
/// All data pools start on the same filesystem (same free space); the
/// chosen pool gets 9 GiB free, every other data pool 1 GiB.
fn steer(registry: &oceanfs_storage::PoolRegistry, id: u32) {
    let data_ids: Vec<u32> = registry.data_pools().iter().map(|p| p.id()).collect();
    for data_id in data_ids {
        let free = if data_id == id { 9 << 30 } else { 1 << 30 };
        registry.set_pool_capacity(data_id, 10 << 30, free);
    }
}

/// Waits (max ~30s) until the predicate holds.
async fn wait_until(label: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !predicate() {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for: {label}");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// d1 scenario 1: reads keep serving from a draining pool while placement
/// sends new sealed segments only to the healthy sibling, and the manifest
/// re-gossips `"draining"`.
#[tokio::test]
async fn read_through_drain_placement_exclusion_and_manifest() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, data_roots) = config_with_data_pools(&tmp, 2);
    let (root0, root1) = (&data_roots[0], &data_roots[1]);

    let node = Node::start(config).await.expect("node with 2 data pools boots");
    let addr = node.server_addr();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("client");

    // ---- Seed data on pool 0 (steer placement, wait for a sealed .dat). ----
    steer(&node.pool_registry(), 0);
    for i in 0..4 {
        put(&client, &addr, &format!("pre-drain-{i:02}"), vec![(i % 251) as u8; 64 * 1024]).await;
    }
    wait_until("a sealed segment lands on pool 0", || !root_dats(root0).is_empty()).await;
    get_ok(&client, &addr, "pre-drain-00", 64 * 1024).await;
    let pool0_dats_before = root_dats(root0).len();
    assert!(pool0_dats_before >= 1, "pool 0 holds the seeded segments");

    // ---- Mark pool 0 draining (the operator seam, re-gossips). ----
    node.begin_pool_drain(0).expect("begin drain on data pool 0");
    assert_eq!(
        node.pool_registry().pool_by_id(0).unwrap().status(),
        oceanfs_storage::PoolStatus::Draining
    );
    assert!(node.pool_registry().is_draining(0));
    assert_eq!(
        node.pool_registry().drain_state(0),
        oceanfs_storage::DrainState::Draining { blocked_reason: None, paused: false }
    );

    // The node's manifest re-declared: pool 0 reads "draining", the
    // sibling stays healthy.
    let manifest = node.self_manifest().expect("self manifest");
    let pool0_row = manifest.pools().iter().find(|p| p.id() == 0).expect("pool 0 row");
    assert_eq!(pool0_row.status(), "draining", "the gossiped manifest carries draining");
    let pool1_row = manifest.pools().iter().find(|p| p.id() == 1).expect("pool 1 row");
    assert_eq!(pool1_row.status(), "healthy", "the sibling is untouched");

    // ---- Read-through-drain: objects whose segments live on pool 0. ----
    get_ok(&client, &addr, "pre-drain-01", 64 * 1024).await;

    // ---- New writes seal only on the healthy sibling. ----
    steer(&node.pool_registry(), 1);
    for i in 0..6 {
        put(&client, &addr, &format!("post-drain-{i:02}"), vec![(i % 251) as u8; 64 * 1024]).await;
    }
    wait_until("new sealed segments land on pool 1", || !root_dats(root1).is_empty()).await;
    assert_eq!(
        root_dats(root0).len(),
        pool0_dats_before,
        "a draining pool receives no new sealed segments"
    );
    get_ok(&client, &addr, "post-drain-00", 64 * 1024).await;

    node.shutdown().await.expect("graceful shutdown");
    drop(tmp);
}

/// d1 scenario 2 (no-destructive-failure): a drain with no eligible target
/// stays `Draining`, surfaces the blocked reason (admin JSON + metric),
/// and deletes nothing.
#[tokio::test]
async fn blocked_drain_stays_draining_and_deletes_nothing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, data_roots) = config_with_data_pools(&tmp, 2);
    let (root0, _root1) = (&data_roots[0], &data_roots[1]);

    let node = Node::start(config).await.expect("node boots");
    let addr = node.server_addr();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("client");

    // Seed an object on pool 0.
    steer(&node.pool_registry(), 0);
    put(&client, &addr, "blocked-obj", vec![42; 64 * 1024]).await;
    wait_until("a sealed segment lands on pool 0", || !root_dats(root0).is_empty()).await;
    let files_before: Vec<PathBuf> = {
        let mut v = root_dats(root0);
        v.sort();
        v
    };

    // Drain BOTH data pools: pool 0 now has no eligible sibling target.
    node.begin_pool_drain(0).expect("begin drain on pool 0");
    node.pool_registry().begin_drain(1).expect("begin drain on pool 1");

    // The worker (d3/d4, not in d1) would report blocked; d1 drives it.
    node.pool_registry().set_drain_blocked(0, Some("no sibling headroom")).expect("set blocked");

    // State: pool 0 stays Draining with the reason surfaced.
    assert_eq!(
        node.pool_registry().pool_by_id(0).unwrap().status(),
        oceanfs_storage::PoolStatus::Draining
    );
    let drain = node.pool_registry().drain_state(0);
    assert!(drain.is_blocked(), "the blocked drain is surfaced");
    assert_eq!(drain.blocked_reason(), Some("no sibling headroom"));

    // Reads keep serving (both pools are draining — no new writes needed).
    get_ok(&client, &addr, "blocked-obj", 64 * 1024).await;

    // Nothing was deleted by the blocked drain.
    let mut files_after = root_dats(root0);
    files_after.sort();
    assert_eq!(files_after, files_before, "a blocked drain deletes nothing");

    // The admin status route exposes the per-pool drain state.
    let resp = client
        .get(format!("http://{addr}/admin/pools"))
        .send()
        .await
        .expect("GET /admin/pools must be reachable");
    assert_eq!(resp.status(), 200);
    let view: serde_json::Value = resp.json().await.expect("json");
    let pool0 = view["pools"].as_array().unwrap().iter().find(|p| p["pool_id"] == 0).expect("row");
    assert_eq!(pool0["status"], "draining");
    assert_eq!(pool0["drain_state"], "draining");
    assert_eq!(pool0["drain_blocked"], true);
    assert_eq!(pool0["blocked_reason"], "no sibling headroom");

    // The drain metric series are set (blocked = 1, drain_state = 1).
    let metrics = client
        .get(format!("http://{addr}/admin/metrics"))
        .send()
        .await
        .expect("GET /admin/metrics must be reachable")
        .text()
        .await
        .expect("metrics text");
    assert!(
        metrics.contains("oceanfs_pool_drain_blocked_reason{pool_id=\"0\"} 1"),
        "blocked metric set: {metrics}"
    );
    assert!(
        metrics.contains("oceanfs_pool_drain_state{pool_id=\"0\"} 1"),
        "drain_state metric set: {metrics}"
    );

    node.shutdown().await.expect("graceful shutdown");
    drop(tmp);
}

/// d1 scenario 3 (d4 precondition): a node whose LAST data pool is draining
/// keeps serving reads and manifests zero healthy data pools (peers route
/// around it as a write/repair target).
#[tokio::test]
async fn last_data_pool_drain_reads_serve_and_zero_healthy_manifest() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, data_roots) = config_with_data_pools(&tmp, 1);
    let (root0,) = (&data_roots[0],);

    let node = Node::start(config).await.expect("node with one data pool boots");
    let addr = node.server_addr();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("client");

    // Seed an object on the only data pool.
    steer(&node.pool_registry(), 0);
    put(&client, &addr, "last-pool-obj", vec![7; 64 * 1024]).await;
    wait_until("a sealed segment lands on the data pool", || !root_dats(root0).is_empty()).await;

    // Drain the last data pool (read-only retirement precondition).
    node.begin_pool_drain(0).expect("begin drain on the last data pool");
    assert!(node.pool_registry().is_draining(0));

    // Reads keep serving from the draining last pool.
    get_ok(&client, &addr, "last-pool-obj", 64 * 1024).await;

    // The manifest shows zero healthy data pools — peers exclude the node
    // as a write/repair target through the existing seams.
    let manifest = node.self_manifest().expect("self manifest");
    let data_rows: Vec<_> = manifest.pools().iter().filter(|p| p.role() == "data").collect();
    assert_eq!(data_rows.len(), 1);
    assert_eq!(data_rows[0].status(), "draining");
    assert!(
        manifest.pools().iter().all(|p| p.role() != "data" || p.status() == "draining"),
        "no healthy data pool remains"
    );

    node.shutdown().await.expect("graceful shutdown");
    drop(tmp);
}
