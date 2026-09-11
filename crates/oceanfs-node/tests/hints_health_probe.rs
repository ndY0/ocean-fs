//! Integration test (f0 D1/D2): the hints pool has a health producer, and
//! the conditional debt gate does not turn that health state into a blanket
//! write outage.
//!
//! A real node boots with a fast-ticking hints health config. The hints
//! root is then broken, repaired, and finally removed while the node runs.
//! Assertions:
//! - the periodic hints-root probe records observed errors into the shared
//!   [`IoObserver`](oceanfs_storage::io::IoObserver) (the producer);
//! - the real health monitor drives `GET /admin/pools` through
//!   `Healthy → Degraded → Healthy` (recovery) `→ Dead` (missing root =
//!   confirmed loss) within the configured detection windows (the
//!   consumer);
//! - a write that needs no hint debt still succeeds in every state,
//!   including Dead — the gate is conditional, never a blanket node flag;
//! - the `hinted_handoff_hints_rejected_total` series are registered and
//!   stay at 0 because no needed debt was refused.
//!
//! The multi-node "needed debt is refused / never silently acked" invariant
//! is asserted at the coordinator boundary in
//! `oceanfs-server::write::coordinator` (including the durable hint-WAL
//! assertion); the fleet scenario drives it black-box through f4.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::{Duration, Instant};

use oceanfs_core::{
    MissingRootPolicy, NodeConfig, PoolRole, PoolTech, StorageConfig, StoragePoolConfig,
};
use oceanfs_node::Node;

/// Hints pool id in the config below (data-0=0, wal=1, meta=2, hints=3).
const HINTS_POOL_ID: u32 = 3;

fn pool(name: &str, role: PoolRole, root: &std::path::Path) -> StoragePoolConfig {
    StoragePoolConfig {
        name: name.into(),
        role,
        root: root.to_path_buf(),
        weight: None,
        tech: PoolTech::Auto,
        health: Default::default(),
    }
}

#[tokio::test]
async fn hints_probe_drives_healthy_degraded_dead_without_gating_needless_writes() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let hints_root = tmp.path().join("hints0");

    // Fast windows so the test observes a real Degraded transition without
    // waiting the production 30s detection window.
    let fast_health = oceanfs_core::PoolHealthOverride {
        min_errors: Some(1),
        detection_window_secs: Some(1),
        trend_window_secs: Some(1),
        recovery_window_secs: Some(1),
        ..Default::default()
    };
    let mut hints = pool("hints-0", PoolRole::Hints, &hints_root);
    hints.health = fast_health;

    let config = NodeConfig {
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: "127.0.0.1:0".into(),
        membership_listen_addr: "127.0.0.1:0".into(),
        storage: StorageConfig {
            pools: vec![
                pool("data-0", PoolRole::Data, &tmp.path().join("pool-data")),
                pool("wal-0", PoolRole::Wal, &tmp.path().join("pool-wal")),
                pool("meta-0", PoolRole::Metadata, &tmp.path().join("pool-meta")),
                hints,
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        },
        ..NodeConfig::default()
    };

    let node = Node::start(config).await.expect("node boots");
    let observer = node.io_observer();
    let addr = node.server_addr();
    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("client");

    // Healthy at boot: the probe has run clean cycles (or none yet).
    assert!(observer.snapshot(HINTS_POOL_ID).is_some(), "hints pool must be registered");

    // Destroy the hints root AT RUNTIME: the next probe cycle fails with
    // NotADirectory and the health monitor sees observed errors.
    std::fs::remove_dir_all(&hints_root).expect("remove hints root");
    std::fs::write(&hints_root, b"not a directory").expect("replace with file");

    // ---- Phase 1: break the root (a regular file) → Degraded ----
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut degraded = false;
    while Instant::now() < deadline {
        if hints_status(&client, addr).await == "degraded" {
            degraded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(degraded, "the hints probe must degrade the pool within the detection windows");
    assert!(
        observer.io_error_count(HINTS_POOL_ID) > 0,
        "the probe's failures are observed by the storage observer"
    );

    // Conditional gate: this write has no failed targets (single-node ring),
    // so no debt is needed and the degraded hints pool must NOT reject it.
    put_needless(&client, addr, "f0-needless-degraded").await;

    // ---- Phase 2: repair the root → Healthy (clean probe windows) ----
    std::fs::remove_file(&hints_root).expect("remove the file root");
    std::fs::create_dir(&hints_root).expect("recreate the hints root");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut healthy = false;
    while Instant::now() < deadline {
        if hints_status(&client, addr).await == "healthy" {
            healthy = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(healthy, "a repaired root must recover the pool to Healthy");
    put_needless(&client, addr, "f0-needless-recovered").await;

    // ---- Phase 3: root gone entirely → NotFound (confirmed loss) → Dead ----
    std::fs::remove_dir_all(&hints_root).expect("remove the hints root");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut dead = false;
    while Instant::now() < deadline {
        if hints_status(&client, addr).await == "dead" {
            dead = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(dead, "a missing root is a confirmed loss and must reach Dead");

    // The conditional gate holds even at Dead: no debt is needed here.
    put_needless(&client, addr, "f0-needless-dead").await;

    // No needed debt was refused: both rejection series must be REGISTERED
    // and stay at 0 (a missing series would make the old loop vacuous).
    let metrics = client
        .get(format!("http://{addr}/admin/metrics"))
        .send()
        .await
        .expect("GET /admin/metrics")
        .text()
        .await
        .expect("metrics text");
    for path in ["write", "delete"] {
        let needle = format!(
            "hinted_handoff_hints_rejected_total{{reason=\"pool_dead\",path=\"{path}\"}} 0"
        );
        assert!(metrics.contains(&needle), "series must be registered and zero: {needle}");
    }

    node.shutdown().await.expect("node shutdown");
}

/// Reads the hints pool's current status from `GET /admin/pools`.
async fn hints_status(client: &reqwest::Client, addr: std::net::SocketAddr) -> String {
    let resp =
        client.get(format!("http://{addr}/admin/pools")).send().await.expect("GET /admin/pools");
    assert_eq!(resp.status(), 200);
    let view: serde_json::Value = resp.json().await.expect("json");
    view["pools"]
        .as_array()
        .expect("pools array")
        .iter()
        .find(|p| p["pool_id"] == HINTS_POOL_ID)
        .expect("hints pool row")["status"]
        .as_str()
        .unwrap_or("")
        .to_string()
}

/// PUTs a body whose single-node replica set has no failed targets, so no
/// hint debt is needed; asserts the conditional gate lets it through.
async fn put_needless(client: &reqwest::Client, addr: std::net::SocketAddr, key: &str) {
    let resp = client
        .put(format!("http://{addr}/bucket/{key}"))
        .body(vec![0xABu8; 64 * 1024])
        .send()
        .await
        .expect("PUT must return");
    assert_eq!(resp.status(), 200, "a write needing no hints must proceed: {key}");
}

/// f0 boot semantics: a hints root that cannot even be created (missing
/// mount, broken parent path) must NOT refuse node startup. The pool
/// registers Degraded, the node boots with zero replayed hints (the WAL
/// replay path tolerates the unusable directory), and a write needing no
/// debt still succeeds — the gate, not the boot, enforces honesty.
#[tokio::test]
async fn uncreatable_hints_root_still_boots_degraded() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // A regular FILE blocks the hints root's parent path so
    // `create_dir_all` fails with NotADirectory for every user (root
    // included — no permission-dependent test).
    let blocker = tmp.path().join("blocked");
    std::fs::write(&blocker, b"not a directory").expect("blocker file");
    let hints_root = blocker.join("hints0");

    let config = NodeConfig {
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: "127.0.0.1:0".into(),
        membership_listen_addr: "127.0.0.1:0".into(),
        storage: StorageConfig {
            pools: vec![
                pool("data-0", PoolRole::Data, &tmp.path().join("pool-data")),
                pool("wal-0", PoolRole::Wal, &tmp.path().join("pool-wal")),
                pool("meta-0", PoolRole::Metadata, &tmp.path().join("pool-meta")),
                pool("hints-0", PoolRole::Hints, &hints_root),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        },
        ..NodeConfig::default()
    };

    let node =
        Node::start(config).await.expect("the node must boot with an uncreatable hints root");
    let addr = node.server_addr();
    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("client");

    assert_eq!(
        hints_status(&client, addr).await,
        "degraded",
        "the uncreatable hints root must register the pool Degraded"
    );

    put_needless(&client, addr, "f0-boot-needless").await;

    // No needed debt was refused (single-node ring): registered at zero.
    let metrics = client
        .get(format!("http://{addr}/admin/metrics"))
        .send()
        .await
        .expect("GET /admin/metrics")
        .text()
        .await
        .expect("metrics text");
    for path in ["write", "delete"] {
        let needle = format!(
            "hinted_handoff_hints_rejected_total{{reason=\"pool_dead\",path=\"{path}\"}} 0"
        );
        assert!(metrics.contains(&needle), "series must be registered and zero: {needle}");
    }

    node.shutdown().await.expect("node shutdown");
}
