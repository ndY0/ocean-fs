//! Integration test: intra-node drain (d3, ADR-0036 C1a).
//!
//! A live node runs real S3 cycles against two data pools; the operator
//! begins a drain over the admin HTTP surface (`POST
//! /admin/pools/{id}/drain`, mode `intra-node`), and the real
//! `"drain_intra"` Tier-1 task (registered in `DurabilityModule` and
//! driven by the durability scheduler) relocates the pool's sealed
//! segments to the healthy sibling. Assertions:
//!
//! - **admin begin/pause/resume surface** — the mutation verbs map onto
//!   the registry drain record (`Draining`, `paused`) with the expected
//!   response codes;
//! - **scheduled drain to `Detachable`** — the pool's sealed `.dat` set
//!   moves to the sibling root with no restart and the pool transitions
//!   `Draining → Detachable` (status stays `Draining`);
//! - **live reads throughout** — GETs of pool-0 objects keep serving while
//!   the drain relocates their segments (read-while-draining via d1/d2);
//! - **attach → drain workflow** — a sibling attached at runtime (f8
//!   `POST /admin/pools`) receives the drained segments;
//! - **`storage_locations` untouched** — the topology stays two pools
//!   (single-node relocation never touches the holder set).
//!
//! No restart happens anywhere in these scenarios; no load suite is run
//! locally (PIPELINE §6).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

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

    let config = NodeConfig {
        data_dir,
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: "127.0.0.1:0".into(),
        membership_listen_addr: "127.0.0.1:0".into(),
        storage: StorageConfig { pools, missing_root_policy: MissingRootPolicy::Fatal },
        ..NodeConfig::default()
    };
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

/// A live reader task that GETs every object key until `stop` is set.
/// Returns the number of failed (non-200 / error) responses.
async fn spawn_live_reader(
    client: reqwest::Client,
    addr: std::net::SocketAddr,
    keys: Vec<String>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<u64> {
    tokio::spawn(async move {
        let mut failures = 0u64;
        while !stop.load(Ordering::Relaxed) {
            for key in &keys {
                match client.get(format!("http://{addr}/bucket/{key}")).send().await {
                    Ok(resp) if resp.status() == 200 => {}
                    other => {
                        failures += 1;
                        tracing::warn!("live-read failure on {key}: {other:?}");
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        failures
    })
}

/// d3 scenario 1: begin a drain over the admin HTTP surface; the real
/// `"drain_intra"` scheduler task empties pool 0 to the sibling while a
/// live reader keeps GETting pool-0 objects; no restart.
#[tokio::test]
async fn scheduled_intra_node_drain_serves_live_reads_to_detachable() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, data_roots) = config_with_data_pools(&tmp, 2);
    let (root0, root1) = (&data_roots[0], &data_roots[1]);

    let node = Node::start(config).await.expect("node with 2 data pools boots");
    let addr = node.server_addr();
    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("client");

    // Seed several objects on pool 0 (steer placement, wait for sealed
    // .dat files).
    steer(&node.pool_registry(), 0);
    let keys: Vec<String> = (0..12).map(|i| format!("drain-live-{i:02}")).collect();
    for key in &keys {
        put(&client, &addr, key, vec![7u8; 64 * 1024]).await;
    }
    wait_until("sealed segments land on pool 0", || !root_dats(root0).is_empty()).await;
    for key in &keys {
        get_ok(&client, &addr, key, 64 * 1024).await;
    }
    let pool0_dats_before = root_dats(root0).len();
    assert!(pool0_dats_before >= 1, "pool 0 holds the seeded segments");

    // Begin the drain through the admin mutation route (mode intra-node).
    let begin = client
        .post(format!("http://{addr}/admin/pools/0/drain"))
        .json(&serde_json::json!({ "mode": "intra-node" }))
        .send()
        .await
        .expect("drain begin request");
    assert_eq!(begin.status(), 202, "begin drain returns 202");
    let begin_json: serde_json::Value = begin.json().await.expect("begin drain JSON");
    assert_eq!(begin_json["drain_state"], "draining");

    // Start a live reader over all seeded objects while the drain runs.
    let stop = Arc::new(AtomicBool::new(false));
    let reader = spawn_live_reader(client.clone(), addr, keys.clone(), Arc::clone(&stop)).await;

    // The real scheduler-driven "drain_intra" task empties the pool (no
    // manual run_cycle, no restart) → Detachable.
    wait_until("pool 0 drains to Detachable", || {
        node.pool_registry().drain_state(0) == oceanfs_storage::DrainState::Detachable
    })
    .await;
    stop.store(true, Ordering::Relaxed);
    let read_failures = reader.await.expect("reader task joins");
    assert_eq!(read_failures, 0, "every GET served during the drain");

    // All pool-0 .dat moved to the sibling; pool 0's root is empty.
    assert!(root_dats(root0).is_empty(), "no .dat left on the drained root");
    assert_eq!(root_dats(root1).len(), pool0_dats_before, "every drained segment landed on pool 1");
    assert_eq!(
        node.pool_registry().pool_by_id(0).unwrap().status(),
        oceanfs_storage::PoolStatus::Draining,
        "a Detachable pool's status stays Draining (no silent refill)"
    );

    // storage_locations untouched: the topology is still the two pools and
    // every object still serves after the drain.
    let manifest = node.self_manifest().expect("self manifest");
    assert_eq!(manifest.pools().len(), 5, "wal/meta/hints + the two data pools remain");
    for key in &keys {
        get_ok(&client, &addr, key, 64 * 1024).await;
    }
    node.shutdown().await.expect("clean shutdown");
}

/// d3 scenario 2: the admin pause/resume surface. A paused drain moves
/// nothing even when a cycle runs; resume lets the drain complete.
#[tokio::test]
async fn admin_pause_and_resume_control_the_drain() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, data_roots) = config_with_data_pools(&tmp, 2);
    let (root0, root1) = (&data_roots[0], &data_roots[1]);

    let node = Node::start(config).await.expect("node boots");
    let addr = node.server_addr();
    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("client");

    steer(&node.pool_registry(), 0);
    let keys: Vec<String> = (0..6).map(|i| format!("drain-pause-{i:02}")).collect();
    for key in &keys {
        put(&client, &addr, key, vec![3u8; 64 * 1024]).await;
    }
    wait_until("sealed segments land on pool 0", || !root_dats(root0).is_empty()).await;

    // Begin + pause immediately (the drain must not move anything while
    // paused even though the scheduler task ticks).
    let begin = client
        .post(format!("http://{addr}/admin/pools/0/drain"))
        .send()
        .await
        .expect("begin request");
    assert_eq!(begin.status(), 202);
    let pause = client
        .post(format!("http://{addr}/admin/pools/0/drain/pause"))
        .send()
        .await
        .expect("pause request");
    assert_eq!(pause.status(), 200, "pause returns 200");
    let pause_json: serde_json::Value = pause.json().await.expect("pause JSON");
    assert_eq!(pause_json["drain_paused"], true);
    assert!(node.pool_registry().drain_state(0).is_paused());

    // A manual drain cycle (and any scheduler tick) is a no-op while
    // paused: no segment moves.
    let before = root_dats(root0).len();
    let stats = node.drain_worker().run_cycle().await;
    assert_eq!(stats.segments_moved, 0, "paused drain moves nothing");
    assert!(root_dats(root0).len() >= before, "paused drain leaves .dat untouched");
    assert!(root_dats(root1).is_empty(), "no .dat on the sibling yet while paused");

    // Resume: the drain completes to Detachable.
    let resume = client
        .post(format!("http://{addr}/admin/pools/0/drain/resume"))
        .send()
        .await
        .expect("resume request");
    assert_eq!(resume.status(), 200, "resume returns 200");
    let resume_json: serde_json::Value = resume.json().await.expect("resume JSON");
    assert_eq!(resume_json["drain_paused"], false);
    assert!(!node.pool_registry().drain_state(0).is_paused());

    wait_until("drain completes after resume", || {
        node.pool_registry().drain_state(0) == oceanfs_storage::DrainState::Detachable
    })
    .await;
    assert!(root_dats(root0).is_empty(), "pool 0 drained after resume");
    assert_eq!(root_dats(root1).len(), before, "all segments moved after resume");
    for key in &keys {
        get_ok(&client, &addr, key, 64 * 1024).await;
    }
    node.shutdown().await.expect("clean shutdown");
}

/// d3 scenario 3 (workflow): boot with one data pool, attach a sibling at
/// runtime (f8), then drain the original pool into it — attach → drain →
/// `Detachable`, no restart.
#[tokio::test]
async fn attach_sibling_then_drain_pool_to_detachable() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, mut data_roots) = config_with_data_pools(&tmp, 1);
    let root0 = data_roots[0].clone();

    let node = Node::start(config).await.expect("node with 1 data pool boots");
    let addr = node.server_addr();
    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("client");

    steer(&node.pool_registry(), 0);
    let keys: Vec<String> = (0..6).map(|i| format!("drain-attach-{i:02}")).collect();
    for key in &keys {
        put(&client, &addr, key, vec![5u8; 64 * 1024]).await;
    }
    wait_until("sealed segments land on pool 0", || !root_dats(&root0).is_empty()).await;

    // Attach a sibling data pool (f8, no restart).
    let root1 = tmp.path().join("nvme1");
    std::fs::create_dir_all(&root1).expect("attached root dir");
    let attached = oceanfs_core::StoragePoolConfig {
        name: "data-1".into(),
        role: PoolRole::Data,
        root: root1.clone(),
        weight: Some(1),
        tech: PoolTech::Auto,
        health: Default::default(),
    };
    let attach = client
        .post(format!("http://{addr}/admin/pools"))
        .json(&attached)
        .send()
        .await
        .expect("attach request");
    assert_eq!(attach.status(), 201, "attach sibling returns 201");
    let pool1_id = node
        .pool_registry()
        .data_pools()
        .iter()
        .find(|p| p.id() != 0)
        .expect("attached sibling")
        .id();
    data_roots.push(root1);

    // Drain pool 0 over the admin route; the real scheduler task moves the
    // segments into the attached sibling → Detachable, no restart.
    let begin = client
        .post(format!("http://{addr}/admin/pools/0/drain"))
        .send()
        .await
        .expect("begin request");
    assert_eq!(begin.status(), 202);
    let before = root_dats(&data_roots[0]).len();
    wait_until("pool 0 drains to Detachable", || {
        node.pool_registry().drain_state(0) == oceanfs_storage::DrainState::Detachable
    })
    .await;
    assert!(root_dats(&data_roots[0]).is_empty(), "source root emptied");
    assert_eq!(root_dats(&data_roots[1]).len(), before, "all segments landed on the sibling");
    assert_eq!(
        node.pool_registry().drain_state(pool1_id),
        oceanfs_storage::DrainState::Idle,
        "the sibling is not draining"
    );
    for key in &keys {
        get_ok(&client, &addr, key, 64 * 1024).await;
    }
    node.shutdown().await.expect("clean shutdown");
}
