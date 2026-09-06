//! Integration test (g7 `wal-loss-recovery`, ADR-0035): replaced-wal-pool
//! recovery — LIVE remount (mandatory, g7 D2).
//!
//! A 3-node local cluster (RF=3). Objects are written through the owner
//! A; A seals and the seal-time replicator pushes each sealed segment's
//! full data to its ring replicas (B and C). The wal pool on A is then
//! driven Dead (the D3 health monitor) and its device is replaced:
//!
//! - during the wal-pool outage, A's local writes are rejected (503) while
//!   reads keep serving (the metadata pool and data pools are intact);
//! - the operator empties the journal device and triggers
//!   `POST /admin/wal-remount` — no restart. Recovery re-opens the fresh
//!   WALs, verifies the fresh WAL and clears the write gate.
//!
//! Assertions:
//! - a local write DURING the outage is rejected (503);
//! - reads serve throughout the outage;
//! - no data-pool `.dat` is deleted by the remount (the residue sweep is
//!   never run on the replaced branch — audit C1);
//! - after remount, the write gate clears: a new write succeeds, and
//!   every pre-outage key still reads back byte-identical THROUGH A.
//!
//! NOTE on coverage: a live remount never loses A's in-memory registry,
//! so the holder-metadata fold / catch-up drain are a no-op here
//! (`restored=0 missing=0 caught_up=0`) — this test proves the live
//! remount surface + write gate, NOT the rebuild-from-holders machinery.
//! The BOOT variant (restart A after an out-of-band replacement, where
//! the registry is genuinely empty and the holder fold DOES run) is the
//! path that exercises ADR-0035 D2/D3 end-to-end:
//! [`boot_variant_heals_after_out_of_band_wal_replacement`] below shuts
//! A down, empties its wal device, restarts A in-process on the same
//! directories and asserts the boot branch (registry rebuilt from
//! holders > 0, write gate cleared, byte-identical read-back, no `.dat`
//! swept). That test is only possible because every background worker is
//! cancellable + awaited at shutdown (the RocksDB LOCK and the fixed
//! data-plane/membership listeners are released before the restart).

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

/// PUTs `body` under `key`; returns the HTTP status.
async fn put_status(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    key: &str,
    body: &[u8],
) -> u16 {
    client
        .put(format!("http://{addr}/durability/{key}"))
        .body(body.to_vec())
        .send()
        .await
        .expect("PUT must reach the node")
        .status()
        .as_u16()
}

/// Writes `count` Small-tier objects through `addr_a`, waits for A to
/// seal ≥ 1 segment and for BOTH replicas to hold the same `.dat` set.
/// Returns the keys, body, A's data root and A's `.dat` list.
async fn write_seal_and_replicate(
    client: &reqwest::Client,
    addr_a: std::net::SocketAddr,
    count: usize,
    data_root_a: &std::path::Path,
    data_root_b: &std::path::Path,
    data_root_c: &std::path::Path,
) -> (Vec<String>, Vec<u8>, Vec<PathBuf>) {
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let keys: Vec<String> = (0..count).map(|i| format!("pre-kill-{i:02}")).collect();
    for key in &keys {
        put(client, addr_a, key, &body).await;
    }
    wait_for_dat_count(data_root_a, 1, "owner A data pool").await;
    let owner_dats = data_dats(data_root_a);
    assert!(!owner_dats.is_empty(), "owner sealed ≥ 1 segment before the kill");
    wait_for_dat_count(data_root_b, owner_dats.len(), "replica B data pool").await;
    wait_for_dat_count(data_root_c, owner_dats.len(), "replica C data pool").await;
    (keys, body, owner_dats)
}

/// Parses a `name value` sample line from the Prometheus text exposition
/// returned by `GET /admin/metrics`.
fn parse_metric(text: &str, name: &str) -> u64 {
    text.lines()
        .find(|l| l.trim_start().starts_with(name) && !l.trim_start().starts_with('#'))
        .and_then(|l| l.trim().strip_prefix(name).map(|rest| rest.trim_start()))
        .and_then(|v| v.trim_end().split_whitespace().next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Polls `GET /admin/metrics` until `name` reads ≥ `min`.
async fn wait_for_metric(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    name: &str,
    min: u64,
    what: &str,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let resp = client
            .get(format!("http://{addr}/admin/metrics"))
            .send()
            .await
            .expect("GET /admin/metrics must reach the node");
        assert_eq!(resp.status(), 200, "metrics endpoint serves");
        let text = resp.text().await.expect("metrics body");
        let value = parse_metric(&text, name);
        if value >= min {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: metric {name} must reach ≥ {min} within 60s (now {value})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Polls a PUT until it succeeds (the wal write gate cleared).
async fn wait_for_write_resume(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    key: &str,
    body: &[u8],
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let status = put_status(client, addr, key, body).await;
        if status == 200 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "write gate must clear within 90s (last PUT status {status})"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
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
    let data_root_a = tmp_a.path().join("pool-data");
    let data_root_b = tmp_b.path().join("pool-data");
    let data_root_c = tmp_c.path().join("pool-data");
    let (keys, body, owner_dats) =
        write_seal_and_replicate(&client, addr_a, 6, &data_root_a, &data_root_b, &data_root_c)
            .await;
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

    // A LOCAL write during the outage is rejected with a retryable 503
    // (the wal-Dead 503 gate — the DoD's "writes rejected during outage").
    let outage_write = client
        .put(format!("http://{addr_a}/durability/write-during-outage"))
        .body(body.clone())
        .send()
        .await
        .expect("PUT during wal outage must be reachable");
    assert_eq!(
        outage_write.status(),
        503,
        "local writes must be rejected (503) while the wal pool is dead"
    );

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

#[tokio::test]
async fn boot_variant_heals_after_out_of_band_wal_replacement() {
    // g7 (ADR-0035) BOOT variant: A is shut down cleanly, its journal
    // device is replaced out-of-band (the pool-wal root emptied while A
    // is down) and A is restarted IN-PROCESS on the same directories.
    // On boot the registry is genuinely empty (the event WAL that held
    // A's segment lifecycle state is gone), so the boot branch runs the
    // D2 holder fold + catch-up drain against the live replicas B and C.
    //
    // This test is only possible because every worker is cancellable and
    // awaited at shutdown: the RocksDB LOCK and A's fixed data-plane +
    // membership listeners are released, so the second boot reopens the
    // same metadata dir and re-binds the same addresses.
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

    // A is the seed; B and C join through A's membership plane. A boots
    // with fast wal-health knobs (harmless here — recovery is boot
    // driven, not health driven).
    let node_a = boot_node("node-a", None, &a_addrs, &tmp_a, true).await;
    let node_b = boot_node("node-b", Some(&a_addrs.membership), &b_addrs, &tmp_b, false).await;
    let node_c = boot_node("node-c", Some(&a_addrs.membership), &c_addrs, &tmp_c, false).await;

    wait_for_cluster_convergence(&node_a).await;
    wait_for_cluster_convergence(&node_b).await;
    wait_for_cluster_convergence(&node_c).await;

    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("client");
    let addr_a = node_a.server_addr();

    let data_root_a = tmp_a.path().join("pool-data");
    let data_root_b = tmp_b.path().join("pool-data");
    let data_root_c = tmp_c.path().join("pool-data");
    let (keys, body, owner_dats) =
        write_seal_and_replicate(&client, addr_a, 6, &data_root_a, &data_root_b, &data_root_c)
            .await;

    // ---- Replace A's journal device out-of-band ----
    node_a.shutdown().await.expect("A shutdown");
    empty_dir(&tmp_a.path().join("pool-wal"));

    // ---- Restart A in-process on the SAME directories/addresses ----
    let node_a2 = boot_node("node-a", None, &a_addrs, &tmp_a, true).await;
    let addr_a2 = node_a2.server_addr();
    // A must rejoin the ring (B and C still hold its data).
    wait_for_cluster_convergence(&node_a2).await;

    // The boot branch must have recorded a replaced-wal recovery with a
    // NON-EMPTY rebuilt registry (the holder fold restored A's own
    // sealed segment entries from B/C — this is the assertion that
    // distinguishes the boot variant from the live remount, where the
    // registry survives in memory and `restored == 0`).
    wait_for_metric(&client, addr_a2, "oceanfs_wal_replaced_total", 1, "boot recovery recorded")
        .await;
    wait_for_metric(
        &client,
        addr_a2,
        "oceanfs_wal_recovery_registry_rebuilt_segments",
        1,
        "holder fold restored segments",
    )
    .await;

    // The write gate clears once the drain + verification finish; then
    // every pre-kill key reads back byte-identical THROUGH A2 and no
    // data-pool `.dat` was swept.
    wait_for_write_resume(&client, addr_a2, "post-restart-write", &body).await;
    assert_post_recovery(&client, addr_a2, &keys, &body, &data_root_a, &owner_dats).await;

    node_a2.shutdown().await.expect("A2 shutdown");
    node_b.shutdown().await.expect("B shutdown");
    node_c.shutdown().await.expect("C shutdown");
}
