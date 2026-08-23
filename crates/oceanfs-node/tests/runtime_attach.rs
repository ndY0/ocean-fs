//! Integration test: runtime pool attach via `POST /admin/pools` (f8,
//! ADR-0029 §D8).
//!
//! A 4-pool node (data, wal, metadata, hints) runs a real S3 cycle; the
//! operator attaches a second data pool mid-run through the admin HTTP
//! surface. Assertions:
//! - `201 {pool_id: 4}` (sequential id after the 4 boot pools);
//! - the live `PoolRegistry` grows to 5 and the `NodeManifest` (the
//!   object f6 gossips to peers) re-declares with 5 pools — the epic
//!   DoD's "node gains a pool mid-run and the cluster observes the
//!   manifest change" (peer observation of the re-gossiped manifest is
//!   the f7-proven path);
//! - placement sees the new pool: sealed segments land on the new root;
//! - the node NEVER restarts — the same process keeps serving (GETs
//!   round-trip after the attach).

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
fn four_pool_config(tmp: &tempfile::TempDir) -> (NodeConfig, PathBuf, PathBuf) {
    let data_dir = tmp.path().join("data");
    let data_root = tmp.path().join("nvme0");
    let storage = StorageConfig {
        pools: vec![
            pool("data-a", PoolRole::Data, &data_root),
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
        ..NodeConfig::default()
    };
    (config, data_root, data_dir)
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

/// The f8 DoD integration item: attach a second data pool mid-run.
#[tokio::test]
async fn attach_second_data_pool_mid_run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (config, data_root, _data_dir) = four_pool_config(&tmp);
    let attach_root = tmp.path().join("nvme-attach");

    let node = Node::start(config.clone()).await.expect("4-pool node must boot");
    let addr = node.server_addr();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("client");

    // Boot state: 4 pools, 4 pools in the gossiped manifest.
    assert_eq!(node.pool_registry().pool_count(), 4);
    assert_eq!(node.self_manifest().expect("manifest").pools().len(), 4);

    // ---- Runtime attach: a second data pool. ----
    let resp = client
        .post(format!("http://{addr}/admin/pools"))
        .json(&pool("data-b", PoolRole::Data, &attach_root))
        .send()
        .await
        .expect("POST /admin/pools must be reachable");
    assert_eq!(resp.status(), 201, "attach returns 201 Created");
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["pool_id"], 4, "the attached pool gets the next sequential id");

    // The live registry grew; the manifest re-declared with 5 pools.
    assert_eq!(node.pool_registry().pool_count(), 5, "registry must observe the attach");
    let manifest = node.self_manifest().expect("manifest");
    assert_eq!(
        manifest.pools().len(),
        5,
        "the gossiped NodeManifest must gain the attached pool (4 → 5)"
    );
    assert!(
        manifest.pools().iter().any(|p| p.role() == "data"),
        "the manifest still carries the data pools"
    );

    // The probe created the new root.
    assert!(attach_root.exists(), "the attach probe must create the root");

    // ---- Placement sees the new pool: sealed segments land on it. ----
    // Same-filesystem statvfs makes the real free capacities ~equal, so
    // the least-free/weight selection would tie to the lower id (pool 0).
    // Simulate capacity evolution (the f2 `set_pool_capacity` override):
    // batch 1 prefers the ATTACHED pool (id 4) so its sealed segments
    // prove placement sees it; batch 2 prefers the ORIGINAL pool (id 0)
    // so the spread (not migration) is proven.
    //
    // Batch 1 → attached pool (9 GiB free vs the original's 5 GiB).
    node.pool_registry().set_pool_capacity(0, 10 << 30, 5 << 30);
    node.pool_registry().set_pool_capacity(4, 10 << 30, 9 << 30);
    for i in 0..6 {
        let key = format!("attach-new-{i:02}");
        let body = vec![(i % 251) as u8; 64 * 1024];
        let resp = client
            .put(format!("http://{addr}/bucket/{key}"))
            .body(body)
            .send()
            .await
            .expect("PUT must succeed");
        assert_eq!(resp.status(), 200, "PUT {key} returns 200");
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while root_dats(&attach_root).is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the attached pool root must receive sealed segments"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Batch 2 → original pool (9 GiB free vs the attached's 5 GiB).
    node.pool_registry().set_pool_capacity(0, 10 << 30, 9 << 30);
    node.pool_registry().set_pool_capacity(4, 10 << 30, 5 << 30);
    for i in 0..6 {
        let key = format!("attach-orig-{i:02}");
        let body = vec![(i % 251) as u8; 64 * 1024];
        let resp = client
            .put(format!("http://{addr}/bucket/{key}"))
            .body(body)
            .send()
            .await
            .expect("PUT must succeed");
        assert_eq!(resp.status(), 200, "PUT {key} returns 200");
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while root_dats(&data_root).is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the original data root keeps receiving sealed segments"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Both roots hold sealed segments — the registry snapshot drives the
    // choice and both pools participate.
    assert!(!root_dats(&attach_root).is_empty(), "attached pool root has segments");
    assert!(!root_dats(&data_root).is_empty(), "original data root has segments");

    // ---- The node never restarted: the same process still serves. ----
    let resp = client
        .get(format!("http://{addr}/bucket/attach-new-00"))
        .send()
        .await
        .expect("GET after attach must succeed");
    assert_eq!(resp.status(), 200, "the node keeps serving after the attach");
    assert_eq!(resp.bytes().await.expect("body").len(), 64 * 1024);

    // ---- Conflict path: re-attaching the same root is a 409. ----
    let resp = client
        .post(format!("http://{addr}/admin/pools"))
        .json(&pool("data-b-dup", PoolRole::Data, &attach_root))
        .send()
        .await
        .expect("duplicate POST must be reachable");
    assert_eq!(resp.status(), 409, "duplicate root must be rejected with 409");

    node.shutdown().await.expect("graceful shutdown");
    drop(tmp);
}
