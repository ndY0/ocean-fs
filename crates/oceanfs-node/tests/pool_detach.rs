//! Integration test: pool detach and drop (d5, ADR-0036 D1/D6/D8).
//!
//! The inverse of f8 attach, exercised end-to-end on a live node:
//!
//! - **drain → detach → restart-honors-detach** — a 2-data-pool node
//!   steers placement to pool 0, drains it empty (d3 `"drain_intra"`
//!   scheduler task, `Detachable`), detaches it over
//!   `POST /admin/pools/0/detach`, keeps serving reads from pool 1 (whose
//!   **id is unchanged** — the removed-pool overlay skips pool 0 at boot
//!   instead of renumbering the survivor), then restarts over the same
//!   data dir: the detached pool does NOT resurrect and every object still
//!   reads;
//! - **refusals (no-destructive-failure)** — detach of a healthy pool /
//!   a non-empty `Draining` pool returns 409 and the pool keeps serving;
//!   detach of a `wal` pool returns 400; an unknown pool 404;
//! - **hot-swap re-attach round-trip** — after drain + detach + restart,
//!   re-attaching the same name+root (a hot-swapped device) succeeds with
//!   the freed id, clears the tombstone, and a second restart shows both
//!   pools again (config order restored).
//!
//! No load suite is run locally (PIPELINE §6).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use oceanfs_core::{
    MissingRootPolicy, NodeConfig, PoolRole, PoolTech, StorageConfig, StoragePoolConfig,
};
use oceanfs_node::Node;
use oceanfs_storage::DrainState;

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

/// A role-complete topology (data×2, wal, metadata, hints) with sibling
/// roots; returns the config + the data roots in id order. The config can
/// be rebuilt over the SAME tempdir for an in-place restart.
fn config_with_two_data_pools(tmp: &tempfile::TempDir) -> (NodeConfig, Vec<PathBuf>) {
    let data_dir = tmp.path().join("data");
    let mut pools = Vec::new();
    let mut data_roots = Vec::new();
    for index in 0..2 {
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

/// Steers placement to the given data pool id (f2's capacity override).
fn steer(registry: &oceanfs_storage::PoolRegistry, id: u32) {
    for data_pool in registry.data_pools() {
        let free = if data_pool.id() == id { 9 << 30 } else { 1 << 30 };
        registry.set_pool_capacity(data_pool.id(), 10 << 30, free);
    }
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

async fn wait_until(label: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !predicate() {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for: {label}");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// d5 scenario 1: drain pool 0 to `Detachable`, detach it over the admin
/// surface, keep serving reads from pool 1 (no restart), then RESTART over
/// the same data dir — the removed-pool overlay (`config − removed`) keeps
/// the detached pool out and the survivor keeps its original id, so every
/// object still reads.
#[tokio::test]
async fn drain_detach_keeps_serving_and_restart_honors_the_removal() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, data_roots) = config_with_two_data_pools(&tmp);
    let root0 = data_roots[0].clone();
    let root1 = data_roots[1].clone();

    let node = Node::start(config).await.expect("node with 2 data pools boots");
    let addr = node.server_addr();
    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("client");

    // Seed objects on pool 0 (steer placement) and confirm two data pools.
    steer(&node.pool_registry(), 0);
    let keys: Vec<String> = (0..8).map(|i| format!("detach-before-{i:02}")).collect();
    for key in &keys {
        put(&client, &addr, key, vec![7u8; 64 * 1024]).await;
    }
    wait_until("sealed segments land on pool 0", || !root_dats(&root0).is_empty()).await;
    assert_eq!(node.pool_registry().data_pools().len(), 2);

    // Drain pool 0 (intra-node; the scheduler task empties it).
    let begin = client
        .post(format!("http://{addr}/admin/pools/0/drain"))
        .json(&serde_json::json!({ "mode": "intra-node" }))
        .send()
        .await
        .expect("begin drain");
    assert_eq!(begin.status(), 202, "begin drain returns 202");
    wait_until("pool 0 drains to Detachable", || {
        node.pool_registry().drain_state(0) == DrainState::Detachable
    })
    .await;
    assert!(root_dats(&root0).is_empty(), "drained root emptied");
    for key in &keys {
        get_ok(&client, &addr, key, 64 * 1024).await;
    }

    // Detach pool 0: registry 2→1, manifest 2→1, placement writes only to
    // pool 1, reads keep serving, no restart.
    let detach = client
        .post(format!("http://{addr}/admin/pools/0/detach"))
        .send()
        .await
        .expect("detach request");
    assert_eq!(detach.status(), 200, "detach returns 200");
    let detach_json: serde_json::Value = detach.json().await.expect("detach JSON");
    assert_eq!(detach_json["detached"], true);
    assert_eq!(detach_json["data_pools"], 1);
    assert!(node.pool_registry().pool_by_id(0).is_none(), "pool 0 left the registry");
    assert_eq!(node.pool_registry().data_pools().len(), 1);
    assert_eq!(node.pool_registry().data_pools()[0].id(), 1, "survivor keeps its durable id");

    let manifest = node.self_manifest().expect("self manifest");
    assert_eq!(
        manifest.pools().iter().filter(|p| p.role() == "data").count(),
        1,
        "manifest drops the detached pool row"
    );

    // New writes land on the remaining pool; the empty detached root stays
    // empty (detach never wipes or refills it).
    let pool1_dats_before = root_dats(&root1).len();
    put(&client, &addr, "detach-after", vec![9u8; 64 * 1024]).await;
    wait_until("the post-detach write lands on pool 1", || {
        root_dats(&root1).len() > pool1_dats_before
    })
    .await;
    assert!(root_dats(&root0).is_empty(), "the detached root receives no new writes");
    for key in keys.iter().map(|k| k.as_str()).chain(["detach-after"]) {
        get_ok(&client, &addr, key, 64 * 1024).await;
    }

    node.shutdown().await.expect("clean shutdown");

    // ---- restart: the removal is persistent (ADR-0036 D8) ----
    let (restart_config, _) = config_with_two_data_pools(&tmp);
    let restarted = Node::start(restart_config).await.expect("restart over the same data dir");
    let raddr = restarted.server_addr();
    let rclient =
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("client");
    // config still declares both data pools; the overlay suppressed data-0.
    assert_eq!(restarted.pool_registry().data_pools().len(), 1, "detached pool did not resurrect");
    assert_eq!(
        restarted.pool_registry().data_pools()[0].id(),
        1,
        "survivor keeps its durable id across the restart"
    );
    // Every pre-detach object still reads from pool 1 (id preserved).
    for key in keys.iter().map(|k| k.as_str()).chain(["detach-after"]) {
        get_ok(&rclient, &raddr, key, 64 * 1024).await;
    }
    restarted.shutdown().await.expect("clean shutdown");
}

/// d5 scenario 2: no-destructive-failure refusals — detach of a healthy
/// pool, of a non-empty `Draining` pool, of a `wal` pool, and of an
/// unknown pool are all refused with the documented status codes and the
/// pool keeps serving.
#[tokio::test]
async fn detach_refuses_non_empty_wrong_role_and_unknown_pools() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, data_roots) = config_with_two_data_pools(&tmp);
    let root0 = data_roots[0].clone();

    let node = Node::start(config).await.expect("node boots");
    let addr = node.server_addr();
    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("client");

    steer(&node.pool_registry(), 0);
    let keys: Vec<String> = (0..6).map(|i| format!("detach-refuse-{i:02}")).collect();
    for key in &keys {
        put(&client, &addr, key, vec![4u8; 64 * 1024]).await;
    }
    wait_until("sealed segments land on pool 0", || !root_dats(&root0).is_empty()).await;

    // Healthy (never drained): 409.
    let healthy = client
        .post(format!("http://{addr}/admin/pools/0/detach"))
        .send()
        .await
        .expect("detach healthy");
    assert_eq!(healthy.status(), 409, "healthy pool detach is refused");

    // Draining but not empty: 409 (still not Detachable).
    let begin = client
        .post(format!("http://{addr}/admin/pools/0/drain"))
        .send()
        .await
        .expect("begin drain");
    assert_eq!(begin.status(), 202);
    let draining = client
        .post(format!("http://{addr}/admin/pools/0/detach"))
        .send()
        .await
        .expect("detach draining");
    assert_eq!(draining.status(), 409, "non-empty draining pool detach is refused");

    // wal pool (id 2): 400; unknown pool: 404.
    let wal = client
        .post(format!("http://{addr}/admin/pools/2/detach"))
        .send()
        .await
        .expect("detach wal pool");
    assert_eq!(wal.status(), 400, "wal pool detach is refused (g7/g8 path)");
    let unknown = client
        .post(format!("http://{addr}/admin/pools/99/detach"))
        .send()
        .await
        .expect("detach unknown pool");
    assert_eq!(unknown.status(), 404, "unknown pool detach is refused");

    // The pool was never removed: reads keep serving.
    assert_eq!(node.pool_registry().data_pools().len(), 2);
    for key in &keys {
        get_ok(&client, &addr, key, 64 * 1024).await;
    }
    node.shutdown().await.expect("clean shutdown");
}

/// d5 scenario 3 (hot-swap round-trip): drain + detach pool 0, restart
/// (overlay keeps it out), then re-attach the SAME name+root (a hot-swapped
/// device with its original identity) — the freed id is reused, the
/// tombstone is cleared, and a second restart shows both pools again.
#[tokio::test]
async fn reattach_same_identity_after_detach_round_trips() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, data_roots) = config_with_two_data_pools(&tmp);
    let root0 = data_roots[0].clone();
    let root1 = data_roots[1].clone();

    // Boot 1: seed on pool 0, drain it, detach it, stop.
    {
        let node = Node::start(config).await.expect("node boots");
        let addr = node.server_addr();
        let client =
            reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("client");
        steer(&node.pool_registry(), 0);
        let keys: Vec<String> = (0..4).map(|i| format!("swap-{i:02}")).collect();
        for key in &keys {
            put(&client, &addr, key, vec![6u8; 64 * 1024]).await;
        }
        wait_until("sealed segments land on pool 0", || !root_dats(&root0).is_empty()).await;
        client
            .post(format!("http://{addr}/admin/pools/0/drain"))
            .send()
            .await
            .expect("begin drain");
        wait_until("pool 0 drains to Detachable", || {
            node.pool_registry().drain_state(0) == DrainState::Detachable
        })
        .await;
        let detach = client
            .post(format!("http://{addr}/admin/pools/0/detach"))
            .send()
            .await
            .expect("detach");
        assert_eq!(detach.status(), 200);
        node.shutdown().await.expect("clean shutdown");
    }

    // Boot 2: the overlay keeps pool 0 out; the survivor (pool 1) still
    // serves the drained objects.
    let (restart_config, _) = config_with_two_data_pools(&tmp);
    let node = Node::start(restart_config).await.expect("second boot");
    let addr = node.server_addr();
    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("client");
    assert_eq!(node.pool_registry().data_pools().len(), 1);
    for key in ["swap-00", "swap-01", "swap-02", "swap-03"] {
        get_ok(&client, &addr, key, 64 * 1024).await;
    }

    // Re-attach the hot-swapped device with its ORIGINAL identity.
    std::fs::create_dir_all(&root0).expect("reattached root dir");
    let attach = client
        .post(format!("http://{addr}/admin/pools"))
        .json(&oceanfs_core::StoragePoolConfig {
            name: "data-0".into(),
            role: PoolRole::Data,
            root: root0.clone(),
            weight: Some(1),
            tech: PoolTech::Auto,
            health: Default::default(),
        })
        .send()
        .await
        .expect("re-attach request");
    assert_eq!(attach.status(), 201, "re-attach of the same name+root succeeds (hot-swap)");
    let attach_json: serde_json::Value = attach.json().await.expect("attach JSON");
    assert_eq!(attach_json["pool_id"], 0, "the freed id is reused (lowest-free)");
    assert_eq!(node.pool_registry().data_pools().len(), 2);

    // Steer placement to the re-attached pool (both pools share one temp
    // filesystem — without the capacity override every new segment would
    // land on the survivor pool 1).
    steer(&node.pool_registry(), 0);
    // New writes land on the re-attached pool.
    put(&client, &addr, "swap-after", vec![3u8; 64 * 1024]).await;
    wait_until("the re-attached pool receives writes", || !root_dats(&root0).is_empty()).await;
    get_ok(&client, &addr, "swap-after", 64 * 1024).await;
    node.shutdown().await.expect("clean shutdown");

    // Boot 3: the re-attach cleared the tombstone, so config order is
    // restored — both data pools register again and everything still reads.
    let (third_config, _) = config_with_two_data_pools(&tmp);
    let node = Node::start(third_config).await.expect("third boot");
    let addr = node.server_addr();
    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("client");
    assert_eq!(node.pool_registry().data_pools().len(), 2, "both pools return after re-attach");
    assert_eq!(node.pool_registry().data_pools()[0].id(), 0);
    assert_eq!(node.pool_registry().data_pools()[1].id(), 1);
    for key in ["swap-00", "swap-01", "swap-02", "swap-03", "swap-after"] {
        get_ok(&client, &addr, key, 64 * 1024).await;
    }
    // pool 1 still holds the drained segments (id preserved); pool 0 holds
    // the post-attach writes.
    assert!(!root_dats(&root1).is_empty(), "pool 1 keeps the pre-detach data");
    assert!(!root_dats(&root0).is_empty(), "pool 0 keeps the post-attach data");
    node.shutdown().await.expect("clean shutdown");
}
