//! Integration test (g7 `wal-loss-recovery`, ADR-0035): replaced-wal-pool
//! recovery — LIVE remount (mandatory, g7 D2).
//!
//! A 3-node local cluster (RF=3). Objects are written through the owner
//! A; A seals and the seal-time replicator pushes each sealed segment's
//! full data to its ring replicas (B and C). The wal pool on A then
//! "dies" and its device is replaced:
//!
//! - **Live remount**: A's wal pool is driven Dead (the D3 health
//!   monitor) → local writes 503. The operator replaces the journal
//!   device (the wal root is emptied) and triggers
//!   `POST /admin/wal-remount` — no restart. Recovery re-opens the fresh
//!   WALs, rebuilds the lifecycle registry from holders (B/C), drains
//!   catch-up, verifies the fresh WAL and clears the write gate.
//!
//! Assertions:
//! - no data-pool `.dat` is swept by recovery (the residue sweep is
//!   suppressed on the replaced branch — audit C1);
//! - the registry is rebuilt from holders: every pre-kill key reads back
//!   byte-identical THROUGH A;
//! - writes resume after recovery (the write gate clears) and the fresh
//!   WAL accepted the post-recovery write;
//! - reads served throughout the live outage (the metadata pool and data
//!   pools are intact).
//!
//! NOTE: the BOOT variant (restart A after an out-of-band replacement,
//! the boot heuristic selects the rebuild branch) is covered at the unit
//! level (detection + residue-sweep suppression in
//! `modules::storage::tests` / `modules::wal_recovery::tests`) and is
//! pending an e2e/process-level test — an in-process same-dir RocksDB
//! reopen is blocked by server tasks that hold the store past shutdown.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{path::PathBuf, time::Duration};

use oceanfs_core::{
    MissingRootPolicy, NodeConfig, PoolHealthConfig, PoolRole, PoolTech, StorageConfig,
    StoragePoolConfig,
};
use oceanfs_node::Node;
use oceanfs_storage::io::IoErrorKind;

/// One cluster node's fixed addresses (membership plane needs a stable
/// address the seeds point at; gRPC is announced via gossip).
struct NodeAddrs {
    grpc: String,
    membership: String,
}

/// Reserves `n` distinct free TCP ports by binding `n` listeners at once.
fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> =
        (0..n).map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0")).collect();
    let ports = listeners.iter().map(|l| l.local_addr().expect("local addr").port()).collect();
    drop(listeners);
    ports
}

/// A node with fast pool-health knobs (so the wal Dead transition happens
/// in seconds, not the 300 s default window).
fn pool(
    name: &str,
    role: PoolRole,
    root: std::path::PathBuf,
    fast_health: bool,
) -> StoragePoolConfig {
    StoragePoolConfig {
        name: name.into(),
        role,
        root,
        weight: None,
        tech: PoolTech::Auto,
        health: if fast_health {
            PoolHealthConfig {
                min_errors: 1,
                detection_window_secs: 1,
                recovery_window_secs: 1,
                ..PoolHealthConfig::default()
            }
        } else {
            Default::default()
        },
    }
}

fn storage_pools(tmp: &tempfile::TempDir, fast_health: bool) -> StorageConfig {
    StorageConfig {
        pools: vec![
            pool("data-0", PoolRole::Data, tmp.path().join("pool-data"), false),
            // The wal pool uses the fast health knobs so the kill + Dead
            // transition is testable in seconds.
            pool("wal-0", PoolRole::Wal, tmp.path().join("pool-wal"), fast_health),
            pool("meta-0", PoolRole::Metadata, tmp.path().join("pool-meta"), false),
            pool("hints-0", PoolRole::Hints, tmp.path().join("pool-hints"), false),
        ],
        missing_root_policy: MissingRootPolicy::Fatal,
    }
}

/// Boots one node. The tempdir is returned so the caller can inspect /
/// re-use the pool roots (the boot variant restarts A on the same
/// data/metadata pools).
async fn boot_node(
    id: &str,
    seed: Option<&str>,
    addrs: &NodeAddrs,
    tmp: &tempfile::TempDir,
    fast_wal_health: bool,
) -> Node {
    let config = NodeConfig {
        node_id: id.to_string(),
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: addrs.grpc.clone(),
        membership_listen_addr: addrs.membership.clone(),
        storage: storage_pools(tmp, fast_wal_health),
        gossip: oceanfs_core::GossipConfig {
            interval_ms: 250,
            suspicion_timeout_ms: 60_000,
            failure_timeout_ms: 120_000,
            seed_nodes: seed.map(|s| vec![s.to_string()]).unwrap_or_default(),
            ..Default::default()
        },
        // RF=3 so A's sealed segments replicate to BOTH B and C (two live
        // holders for the fold to pull from).
        replication_factor: 3,
        ..NodeConfig::default()
    };
    Node::start(config).await.expect("node boots")
}

/// Waits until the node's ring view contains all 3 cluster nodes.
async fn wait_for_cluster_convergence(node: &Node) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if node.segment_replicator().ring_node_count() >= 3 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cluster must converge to 3 nodes within 30s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// PUTs `body` under `key`; asserts 200.
async fn put(client: &reqwest::Client, addr: std::net::SocketAddr, key: &str, body: &[u8]) {
    let resp = client
        .put(format!("http://{addr}/durability/{key}"))
        .body(body.to_vec())
        .send()
        .await
        .expect("PUT must reach the node");
    assert_eq!(resp.status(), 200, "PUT {key} returns 200");
}

/// The `.dat` segment files directly under a data-pool root.
fn data_dats(root: &std::path::Path) -> Vec<PathBuf> {
    std::fs::read_dir(root)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.to_string_lossy().ends_with(".dat"))
                .collect()
        })
        .unwrap_or_default()
}

/// Waits until `root` holds at least `expected` `.dat` files.
async fn wait_for_dat_count(root: &std::path::Path, expected: usize, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let count = data_dats(root).len();
        if count >= expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: expected ≥ {expected} .dat files within 60s (has {count})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Empties a directory's contents (simulates a replaced device mount).
fn empty_dir(dir: &std::path::Path) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                std::fs::remove_dir_all(&path).ok();
            } else {
                std::fs::remove_file(&path).ok();
            }
        }
    }
}

/// Asserts that after recovery every pre-kill key reads back byte-identical
/// THROUGH the owner (its rebuilt registry + intact data pool serve), that
/// a fresh write succeeds, and that the owner's data `.dat` files were NOT
/// swept (the residue sweep was suppressed on the replaced branch).
async fn assert_post_recovery(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    keys: &[String],
    body: &[u8],
    data_root: &std::path::Path,
    owner_dats: &[PathBuf],
) {
    // Reads of every pre-kill object succeed and are byte-identical.
    for key in keys {
        let resp = client
            .get(format!("http://{addr}/durability/{key}"))
            .send()
            .await
            .expect("GET must reach the owner");
        assert_eq!(resp.status(), 200, "GET {key} must succeed after recovery");
        let got = resp.bytes().await.expect("read body");
        assert_eq!(&got[..], body, "object {key} must be byte-identical after recovery");
    }
    // A fresh write lands on the fresh WAL (the write gate cleared).
    put(client, addr, "post-recovery-write", body).await;
    let resp = client
        .get(format!("http://{addr}/durability/post-recovery-write"))
        .send()
        .await
        .expect("GET post-recovery write");
    assert_eq!(resp.status(), 200, "post-recovery write reads back");
    // No data-pool `.dat` was swept (the replaced branch suppresses the
    // once-per-boot residue sweep). The owner's sealed segments survive.
    let current = data_dats(data_root);
    for expected in owner_dats {
        assert!(
            current.contains(expected),
            "recovery must not sweep the intact data .dat {}",
            expected.display()
        );
    }
}

#[tokio::test]
async fn live_remount_heals_without_restart() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let ports = free_ports(6);
    let a_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[0]),
        membership: format!("127.0.0.1:{}", ports[1]),
    };
    let b_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[2]),
        membership: format!("127.0.0.1:{}", ports[3]),
    };
    let c_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[4]),
        membership: format!("127.0.0.1:{}", ports[5]),
    };

    let tmp_a = tempfile::tempdir().expect("tempdir A");
    let tmp_b = tempfile::tempdir().expect("tempdir B");
    let tmp_c = tempfile::tempdir().expect("tempdir C");

    let node_a = boot_node("node-a", None, &a_addrs, &tmp_a, true).await;
    let node_b = boot_node("node-b", Some(&a_addrs.membership), &b_addrs, &tmp_b, false).await;
    let node_c = boot_node("node-c", Some(&a_addrs.membership), &c_addrs, &tmp_c, false).await;

    wait_for_cluster_convergence(&node_a).await;
    wait_for_cluster_convergence(&node_b).await;
    wait_for_cluster_convergence(&node_c).await;

    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("client");
    let addr_a = node_a.server_addr();

    // Write 6 × 32 KiB objects (Small tier — packed, so several sealed
    // segments land on A's data pool), wait for seals + replication to B/C.
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let keys: Vec<String> = (0..6).map(|i| format!("pre-kill-{i:02}")).collect();
    for key in &keys {
        put(&client, addr_a, key, &body).await;
    }
    let data_root_a = tmp_a.path().join("pool-data");
    let data_root_b = tmp_b.path().join("pool-data");
    let data_root_c = tmp_c.path().join("pool-data");
    wait_for_dat_count(&data_root_a, 1, "owner A data pool").await;
    let owner_dats = data_dats(&data_root_a);
    assert!(!owner_dats.is_empty(), "owner sealed ≥ 1 segment before the kill");
    wait_for_dat_count(&data_root_b, owner_dats.len(), "replica B data pool").await;
    wait_for_dat_count(&data_root_c, owner_dats.len(), "replica C data pool").await;
    // ---- Kill A's wal pool: drive it Dead through the health monitor ----
    let wal_pool_id = node_a.pool_registry().pool_by_role(PoolRole::Wal).expect("A wal pool").id();
    // Phase 1: degrade (TimedOut spike) and WAIT for Degraded.
    for _ in 0..3 {
        node_a.io_observer().record_error(wal_pool_id, IoErrorKind::TimedOut);
        node_a.io_observer().record_latency(
            wal_pool_id,
            oceanfs_storage::io::IoOp::Read,
            Duration::from_micros(1),
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let status = node_a.pool_registry().pool_by_id(wal_pool_id).expect("A wal pool").status();
        if status == oceanfs_storage::PoolStatus::Degraded {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "A's wal pool must degrade first (status: {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Phase 2: confirmed loss (NotFound → `ConfirmedLoss::SegmentNotFound`).
    for _ in 0..3 {
        node_a.io_observer().record_error(wal_pool_id, IoErrorKind::NotFound);
        node_a.io_observer().record_latency(
            wal_pool_id,
            oceanfs_storage::io::IoOp::Read,
            Duration::from_micros(1),
        );
    }
    // The health monitor ticks every second; wait for Dead + write_degraded.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let pool = node_a.pool_registry().pool_by_role(PoolRole::Wal).expect("A wal pool");
        if pool.status() == oceanfs_storage::PoolStatus::Dead && pool.write_degraded() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "A's wal pool must reach Dead + write_degraded within 20s (status {:?})",
            pool.status()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Reads still serve (the metadata pool + data pools are intact).
    let resp = client
        .get(format!("http://{addr_a}/durability/{}", keys[0]))
        .send()
        .await
        .expect("GET during wal outage");
    assert_eq!(resp.status(), 200, "reads serve while the wal pool is dead");

    // ---- Replace the journal device (empty the wal root) ----
    empty_dir(&tmp_a.path().join("pool-wal"));

    // ---- Live remount (no restart): POST /admin/wal-remount ----
    let remount_resp = client
        .post(format!("http://{addr_a}/admin/wal-remount"))
        .send()
        .await
        .expect("POST /admin/wal-remount must be reachable");
    if remount_resp.status() != 200 {
        let body = remount_resp.text().await.unwrap_or_default();
        panic!("wal remount failed: {body}");
    }

    assert_post_recovery(&client, addr_a, &keys, &body, &data_root_a, &owner_dats).await;

    node_a.shutdown().await.expect("A shutdown");
    node_b.shutdown().await.expect("B shutdown");
    node_c.shutdown().await.expect("C shutdown");
}
