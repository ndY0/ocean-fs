//! Integration test (g1 `disk-io-observability` DoD): the node's seal
//! pipeline feeds the shared [`IoObserver`] through the observed
//! [`DiskIo`] surface.
//!
//! A real node (legacy mode) serves a PUT; the async seal pipeline
//! performs its temp-file writes + the flush coordinator's fsync through
//! a pool-aware `ObservedIo`. Assertions:
//! - the observer's window for pool 0 accumulates op counts + write/fsync
//!   latency once a segment seals (proving the observer is fed
//!   end-to-end, not just unit-testable);
//! - no faults injected → zero recorded errors (the observer's
//!   `oceanfs_pool_io_errors_total` stays 0).
//!
//! Fault *injection* through a `FaultyIo`-wrapped store is exercised in
//! `io_observer_faulty.rs` (the injector lives at the storage/io layer,
//! so the node's real sealer cannot wrap it from a test).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use oceanfs_core::NodeConfig;
use oceanfs_node::Node;
use oceanfs_storage::io::IoOp;

#[tokio::test]
async fn seal_pipeline_feeds_the_io_observer() {
    let tmp = tempfile::tempdir().expect("tempdir");
    /// ADR-0031 (f1): mandatory role-complete pool topology for tests — one
    /// data (id 0), one wal, one metadata, one hints pool on sibling roots.
    fn storage_pools(tmp: &tempfile::TempDir) -> oceanfs_core::StorageConfig {
        fn pool(
            name: &str,
            role: oceanfs_core::PoolRole,
            root: std::path::PathBuf,
        ) -> oceanfs_core::StoragePoolConfig {
            oceanfs_core::StoragePoolConfig {
                name: name.into(),
                role,
                root,
                weight: None,
                tech: Default::default(),
                health: Default::default(),
            }
        }
        oceanfs_core::StorageConfig {
            pools: vec![
                pool("data-0", oceanfs_core::PoolRole::Data, tmp.path().join("pool-data")),
                pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.path().join("pool-wal")),
                pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.path().join("pool-meta")),
                pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.path().join("pool-hints")),
            ],
            missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
        }
    }

    let config = NodeConfig {
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: "127.0.0.1:0".into(),
        membership_listen_addr: "127.0.0.1:0".into(),
        storage: storage_pools(&tmp),
        ..NodeConfig::default()
    };
    let node = Node::start(config).await.expect("node boots");
    let observer = node.io_observer();
    // ADR-0031 (f1): the boot pools are registered with the observer;
    // pool 0 is the data pool (configured first).
    assert!(observer.snapshot(0).is_some(), "boot pools must be registered");

    // A real write cycle: PUT a 64 KiB body (the node e2e convention —
    // large enough to be a real segment append).
    let addr = node.server_addr();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("client");
    let body = vec![0xABu8; 64 * 1024];
    let resp = client
        .put(format!("http://{addr}/bucket/g1-wiring"))
        .body(body.clone())
        .send()
        .await
        .expect("PUT must succeed");
    assert_eq!(resp.status(), 200, "PUT returns 200");

    // The seal is async: seal_work writes the temp file (observed
    // write_handle ops) THEN the flush coordinator's fsync lands a group
    // window later (observed fsync_handle op) — possibly in a different
    // observer window. Each snapshot rotates the ring, so poll until a
    // window carries the fsync, remembering whether a write window was
    // seen.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut saw_write = false;
    let fsync_signal = loop {
        let signal = observer.snapshot(0).expect("pool 0 registered");
        if signal.latency_for(IoOp::Write).p50.is_some() {
            saw_write = true;
        }
        if signal.latency_for(IoOp::Fsync).p50.is_some() {
            break signal;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the seal pipeline must feed the observer within 30s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };

    // The seal performed temp-file writes + the flush coordinator's
    // fsync, all through the observed DiskIo.
    assert!(saw_write, "write latency must be recorded for the seal");
    assert!(fsync_signal.ops >= 1, "fsync op must be recorded");
    assert_eq!(fsync_signal.errors, 0, "no faults injected → no errors");
    assert_eq!(observer.io_error_count(0), 0);

    node.shutdown().await.expect("node shutdown");
}
