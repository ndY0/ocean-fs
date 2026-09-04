//! Integration test: multi-data-pool segment store (ADR-0029 f5).
//!
//! A node with 2 data pools runs a real S3 write+read+delete cycle
//! (restart persistence is covered at the storage level: the event-WAL
//! fold test `restart_fold_preserves_pool_ids_and_resolution` in
//! oceanfs-storage):
//! - every sealed segment `.dat` lands on a data pool root (never the
//!   legacy `data_dir/segments`);
//! - after DELETE + GC cycles the segment `.dat` is unlinked from its
//!   pool root (the GC unlink passes the pool id held in the metadata).
//!
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
        weight: None,
        tech: PoolTech::Auto,
        health: Default::default(),
    }
}

/// A 5-pool topology (data×2, wal, metadata, hints) with sibling roots.
fn five_pool_config(tmp: &tempfile::TempDir) -> (NodeConfig, Vec<PathBuf>, PathBuf) {
    let data_dir = tmp.path().join("data");
    let data_roots = vec![tmp.path().join("nvme0"), tmp.path().join("nvme1")];
    let storage = StorageConfig {
        pools: vec![
            pool("data-a", PoolRole::Data, &data_roots[0]),
            pool("data-b", PoolRole::Data, &data_roots[1]),
            pool("journal", PoolRole::Wal, &tmp.path().join("optane0")),
            pool("meta", PoolRole::Metadata, &tmp.path().join("optane1")),
            pool("hints", PoolRole::Hints, &tmp.path().join("hints-dev")),
        ],
        missing_root_policy: MissingRootPolicy::Fatal,
    };
    let config = NodeConfig {
        data_dir: data_dir.clone(),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: "127.0.0.1:0".into(),
        membership_listen_addr: "127.0.0.1:0".into(),
        storage,
        // Fast GC for the delete-reclamation assertion.
        gc_interval_sec: 1,
        tombstone_ttl_sec: 1,
        ..NodeConfig::default()
    };
    (config, data_roots, data_dir)
}

/// Puts `count` small objects and returns (keys, bodies) plus the sealed
/// segment `.dat` files present on the data pool roots.
async fn put_objects(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    count: usize,
) -> Vec<(String, Vec<u8>)> {
    let mut objects = Vec::new();
    for i in 0..count {
        let key = format!("obj-{i:02}");
        // 64 KiB — above the 8 KiB inline threshold, so the blob lands
        // in a segment (the segment path this feature is about).
        let body = vec![(i % 251) as u8; 64 * 1024];
        let resp = client
            .put(format!("http://{addr}/bucket/{key}"))
            .body(body.clone())
            .send()
            .await
            .expect("PUT must succeed");
        assert_eq!(resp.status(), 200, "PUT {key} returns 200");
        objects.push((key, body));
    }
    objects
}

async fn get_object(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    key: &str,
    expected: &[u8],
) {
    let resp =
        client.get(format!("http://{addr}/bucket/{key}")).send().await.expect("GET must succeed");
    assert_eq!(resp.status(), 200, "GET {key} returns 200");
    assert_eq!(resp.bytes().await.expect("body").to_vec(), expected);
}

/// Every sealed `.dat` under the pool roots (any depth), keyed by segment id.
fn pool_root_dats(data_roots: &[PathBuf]) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    for root in data_roots {
        if let Ok(entries) = std::fs::read_dir(root) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".dat") {
                    out.push((name, entry.path()));
                }
            }
        }
    }
    out
}

/// Writes 6 objects on a 2-data-pool node; every sealed segment lands on
/// a pool root (never `data_dir/segments`), GETs round-trip, and DELETE +
/// GC reclaims the pool-root `.dat`. Restart persistence is covered at
/// the storage level (`restart_fold_preserves_pool_ids_and_resolution` in
/// oceanfs-storage): an in-process node restart is blocked by the
/// pre-existing seal-worker-not-joined leak (RocksDB lock held after
/// shutdown).
#[tokio::test]
async fn two_data_pool_node_roundtrip_gc() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, data_roots, data_dir) = five_pool_config(&tmp);
    let legacy_segments = data_dir.join("segments");

    let node = Node::start(config.clone()).await.expect("2-data-pool node must boot");
    let addr = node.server_addr();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("client");
    let objects = put_objects(&client, addr, 6).await;
    for (key, body) in &objects {
        get_object(&client, addr, key, body).await;
    }

    // Sealed segments landed on the data pool roots — never the legacy
    // segments dir. The seal is async (writer-leave triggered), so poll
    // for the `.dat` files. (Same-filesystem statvfs ties to pool 0; the
    // spread across pools is unit-tested via simulated capacity.)
    let seal_deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while pool_root_dats(&data_roots).is_empty() {
        assert!(
            std::time::Instant::now() < seal_deadline,
            "sealed segments must appear on the pool roots within 30s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let dats = pool_root_dats(&data_roots);
    assert!(!dats.is_empty(), "sealed segments must exist on the pool roots");
    let legacy_dats = std::fs::read_dir(&legacy_segments)
        .map(|entries| {
            entries.flatten().filter(|e| e.file_name().to_string_lossy().ends_with(".dat")).count()
        })
        .unwrap_or(0);
    assert_eq!(legacy_dats, 0, "pool-mode segments must not land on data_dir/segments");
    // DELETE everything → GC (1s interval, 1s tombstone TTL) reclaims
    // the pool-root `.dat` files through the pool-aware unlink.
    for (key, _) in &objects {
        let resp = client
            .delete(format!("http://{addr}/bucket/{key}"))
            .send()
            .await
            .expect("DELETE must succeed");
        // S3 DELETE returns 204 No Content on success.
        assert!(
            resp.status() == 200 || resp.status() == 204,
            "DELETE {key} returns 2xx, got {}",
            resp.status()
        );
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !pool_root_dats(&data_roots).is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "GC must unlink the pool-root .dat files within 30s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    node.shutdown().await.expect("graceful shutdown");
    drop(tmp);
}

/// ADR-0031 (f1): a node whose config has no `[storage.pools]` is refused
/// at boot with the role-listing error — the legacy fallback is gone.
#[tokio::test]
async fn node_without_pools_refuses_to_boot() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = NodeConfig {
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: "127.0.0.1:0".into(),
        membership_listen_addr: "127.0.0.1:0".into(),
        ..NodeConfig::default()
    };

    let err = match Node::start(config).await {
        Ok(_) => panic!("boot without pools must fail"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(msg.contains("'data'"), "message: {msg}");
    assert!(msg.contains("'wal'"), "message: {msg}");
    assert!(msg.contains("'metadata'"), "message: {msg}");
    assert!(msg.contains("'hints'"), "message: {msg}");
    assert!(msg.contains("mandatory"), "message: {msg}");
}
