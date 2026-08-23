//! Integration test (g2 `failure-state-machine`, ADR-0029 §D3): the
//! node's health monitor drives a pool through the state machine and the
//! manifest's status/write_degraded flags flip accordingly (verified via
//! the f6 membership API), and the role-consequence applier flips
//! `node_unavailable` when the metadata pool dies.
//!
//! Fault injection surface: the monitor consumes the g1 [`IoObserver`],
//! so the test feeds synthetic error kinds directly (the exact signals
//! a `FaultyIo`-wrapped store would produce — the injector itself lives
//! at the storage/io layer, see `io_observer_faulty.rs`).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use oceanfs_core::{
    MissingRootPolicy, NodeConfig, PoolHealthConfig, PoolRole, PoolTech, StorageConfig,
    StoragePoolConfig,
};
use oceanfs_node::Node;
use oceanfs_storage::{
    io::{IoErrorKind, IoOp},
    PoolStatus,
};

fn pool(name: &str, role: PoolRole, root: &std::path::Path) -> StoragePoolConfig {
    StoragePoolConfig {
        name: name.to_string(),
        role,
        root: root.to_path_buf(),
        weight: None,
        tech: PoolTech::Auto,
        // Fast detection/recovery so the test runs in seconds: any
        // error spikes (min_errors 1), one clean tick recovers.
        health: PoolHealthConfig {
            min_errors: 1,
            detection_window_secs: 1,
            recovery_window_secs: 1,
            ..PoolHealthConfig::default()
        },
    }
}

/// A 4-pool node (data, wal, metadata, hints) with fast health knobs.
fn fast_config(tmp: &tempfile::TempDir) -> NodeConfig {
    let storage = StorageConfig {
        pools: vec![
            pool("data-a", PoolRole::Data, &tmp.path().join("nvme0")),
            pool("journal", PoolRole::Wal, &tmp.path().join("optane0")),
            pool("meta", PoolRole::Metadata, &tmp.path().join("optane1")),
            pool("hints", PoolRole::Hints, &tmp.path().join("hints-dev")),
        ],
        missing_root_policy: MissingRootPolicy::Fatal,
    };
    NodeConfig {
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: "127.0.0.1:0".into(),
        membership_listen_addr: "127.0.0.1:0".into(),
        storage,
        ..NodeConfig::default()
    }
}

/// Feeds a window of errors of `kind` for a pool, then waits (polling)
/// until the monitor's snapshot consumes them into the registry state
/// satisfying `matches` — or times out.
async fn feed_and_wait(
    node: &Node,
    pool_id: u32,
    kind: IoErrorKind,
    matches: impl Fn(PoolStatus) -> bool,
    what: &str,
) {
    // Feed enough signals that the spike rule trips (errors >= min_errors
    // AND error rate 1.0) plus a confirming kind where relevant.
    for _ in 0..3 {
        node.io_observer().record_error(pool_id, kind);
        node.io_observer().record_latency(pool_id, IoOp::Read, Duration::from_micros(1));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let status = node.pool_registry().pool_by_id(pool_id).expect("pool registered").status();
        if matches(status) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "monitor must reach '{what}' for pool {pool_id} within 15s (status: {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn manifest_status(node: &Node, role: &str) -> Option<(String, bool)> {
    let manifest = node.self_manifest()?;
    manifest
        .pools()
        .iter()
        .find(|pool| pool.role() == role)
        .map(|pool| (pool.status().to_string(), pool.write_degraded()))
}

#[tokio::test]
async fn monitor_drives_wal_dead_and_manifest_flags() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let node = Node::start(fast_config(&tmp)).await.expect("4-pool node boots");
    // Pool ids follow the f2 config-order scheme: 0=data, 1=wal,
    // 2=metadata, 3=hints.
    let wal_id = 1;

    // ---- Degrade (error spike) → manifest shows degraded. ----
    feed_and_wait(&node, wal_id, IoErrorKind::TimedOut, |s| s == PoolStatus::Degraded, "Degraded")
        .await;
    // The consequence applier re-declares the manifest (async — poll).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if manifest_status(&node, "wal") == Some(("degraded".into(), false)) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "manifest must show wal degraded");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        !node.pool_registry().pool_by_id(wal_id).unwrap().write_degraded(),
        "Degraded never sets write_degraded (D3 matrix)"
    );

    // ---- Recover (clean windows, hysteresis) → manifest healthy. ----
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let status = node.pool_registry().pool_by_id(wal_id).unwrap().status();
        if status == PoolStatus::Healthy {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "monitor must recover the pool");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // ---- Degrade again, then confirm Dead (ENOENT kind) → write_degraded. ----
    feed_and_wait(&node, wal_id, IoErrorKind::TimedOut, |s| s == PoolStatus::Degraded, "Degraded")
        .await;
    feed_and_wait(&node, wal_id, IoErrorKind::NotFound, |s| s == PoolStatus::Dead, "Dead").await;
    assert!(
        node.pool_registry().pool_by_id(wal_id).unwrap().write_degraded(),
        "wal Dead must set write_degraded"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if manifest_status(&node, "wal") == Some(("dead".into(), true)) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "manifest must show wal dead + write_degraded"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    node.shutdown().await.expect("node shutdown");
}

#[tokio::test]
async fn metadata_dead_flips_node_unavailable() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let node = Node::start(fast_config(&tmp)).await.expect("4-pool node boots");
    let metadata_id = 2;

    assert!(!node.node_unavailable(), "boots available");
    feed_and_wait(
        &node,
        metadata_id,
        IoErrorKind::TimedOut,
        |s| s == PoolStatus::Degraded,
        "Degraded",
    )
    .await;
    feed_and_wait(&node, metadata_id, IoErrorKind::NotFound, |s| s == PoolStatus::Dead, "Dead")
        .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if node.node_unavailable() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node_unavailable must flip after metadata Dead"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    node.shutdown().await.expect("node shutdown");
}
