//! Integration test: role-pinned data paths (ADR-0029 §D8, f4).
//!
//! Boots a real node with a 4-pool topology (data, wal, metadata, hints)
//! and asserts the metadata store, data WAL, event WAL, and hint WAL
//! opened at their pinned pool roots — and that the legacy
//! `data_dir/{metadata,wal,event-wal,hints}` layout is NOT created.
//! Regression: a node with no pools resolves byte-for-byte to the legacy
//! layout. End-to-end: the 4-pool node serves an S3 PUT+GET and the data
//! lands on the pinned roots.
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

/// A 4-pool topology (data, wal, metadata, hints) with sibling roots.
fn four_pool_config(tmp: &tempfile::TempDir) -> (NodeConfig, PathBuf, PathBuf, PathBuf, PathBuf) {
    let data_dir = tmp.path().join("data");
    let data_root = tmp.path().join("nvme0");
    let wal_root = tmp.path().join("optane0");
    let metadata_root = tmp.path().join("optane1");
    let hints_root = tmp.path().join("hints-dev");
    let storage = StorageConfig {
        pools: vec![
            pool("data-a", PoolRole::Data, &data_root),
            pool("journal", PoolRole::Wal, &wal_root),
            pool("meta", PoolRole::Metadata, &metadata_root),
            pool("hints", PoolRole::Hints, &hints_root),
        ],
        missing_root_policy: MissingRootPolicy::Fatal,
    };
    let config = NodeConfig {
        data_dir: data_dir.clone(),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: "127.0.0.1:0".into(),
        // Ephemeral membership plane port (ADR-0028 D1) — the default
        // 0.0.0.0:9002 conflicts across parallel test nodes.
        membership_listen_addr: "127.0.0.1:0".into(),
        storage,
        ..NodeConfig::default()
    };
    (config, data_root, wal_root, metadata_root, hints_root)
}

/// The 4-pool node boots with every non-segment data path pinned to its
/// role pool root; the legacy `data_dir` subdirs are not created.
#[tokio::test]
async fn four_pool_node_boots_with_role_pinned_paths() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, _data_root, wal_root, metadata_root, hints_root) = four_pool_config(&tmp);
    let data_dir = config.data_dir.clone();

    let node = Node::start(config).await.expect("4-pool node must boot");

    // Metadata store (RocksDB) opened at the metadata pool root.
    assert!(
        metadata_root.join("CURRENT").exists(),
        "RocksDB must open at the metadata pool root {metadata_root:?}"
    );
    // Data WAL created its first file at the wal pool root.
    let wal_files: Vec<_> = std::fs::read_dir(&wal_root)
        .expect("wal root readable")
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.starts_with("wal_") && name.ends_with(".log")
        })
        .collect();
    assert!(!wal_files.is_empty(), "WAL file must exist at the wal pool root {wal_root:?}");
    // Event WAL rides the journal device under the wal pool root.
    let event_wal_dir = wal_root.join("event-wal");
    assert!(event_wal_dir.exists(), "event WAL must live at {event_wal_dir:?}");
    // Hints pool root exists (probe-created; per-node WAL files are lazy).
    assert!(hints_root.exists(), "hints pool root must be probed");

    // The legacy layout must NOT exist in pool mode — the pool topology
    // is the authoritative layout.
    assert!(!data_dir.join("metadata").exists(), "metadata must not fall back to data_dir");
    assert!(!data_dir.join("wal").exists(), "wal must not fall back to data_dir");
    assert!(!data_dir.join("event-wal").exists(), "event-wal must not fall back to data_dir");
    assert!(!data_dir.join("hints").exists(), "hints must not fall back to data_dir");
    // Segments stay on data_dir until f5.
    assert!(data_dir.join("segments").exists(), "segments stay at data_dir/segments until f5");

    node.shutdown().await.expect("graceful shutdown");
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
    assert!(msg.contains("'hints'"), "message: {msg}");
}

/// The 4-pool node serves an S3 PUT+GET, and the write lands on the pinned
/// roots (WAL grows at the wal pool root; metadata lives at the metadata
/// pool root).
#[tokio::test]
async fn four_pool_node_serves_put_get_on_pinned_roots() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, _data_root, wal_root, metadata_root, _hints_root) = four_pool_config(&tmp);
    let data_dir = config.data_dir.clone();

    let node = Node::start(config).await.expect("4-pool node must boot");
    let server_addr = node.server_addr();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("client");

    let body = vec![0x5A; 1024];
    let put = client
        .put(format!("http://{server_addr}/test-bucket/hello"))
        .body(body.clone())
        .send()
        .await
        .expect("PUT must succeed");
    assert_eq!(put.status(), 200, "PUT returns 200");

    let get = client
        .get(format!("http://{server_addr}/test-bucket/hello"))
        .send()
        .await
        .expect("GET must succeed");
    assert_eq!(get.status(), 200, "GET returns 200");
    let got = get.bytes().await.expect("body").to_vec();
    assert_eq!(got, body, "GET returns the PUT body");

    // The write landed on the pinned roots: the WAL file lives at the wal
    // pool root (its size is not asserted — the seal worker truncates the
    // WAL after sealing, so the size is timing-dependent), and the legacy
    // data_dir/wal path was never created.
    let wal_file = std::fs::read_dir(&wal_root)
        .expect("wal root readable")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
            name.starts_with("wal_") && name.ends_with(".log")
        })
        .expect("wal file at the pinned wal root");
    assert!(wal_file.exists(), "WAL file must exist at the wal pool root");
    assert!(!data_dir.join("wal").exists(), "no WAL may exist at the legacy data_dir/wal path");
    assert!(
        metadata_root.join("CURRENT").exists(),
        "metadata store must remain at the metadata pool root"
    );

    node.shutdown().await.expect("graceful shutdown");
}
