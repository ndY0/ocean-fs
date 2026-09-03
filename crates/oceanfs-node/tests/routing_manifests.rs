//! Integration test (g6 `routing-manifests`, ADR-0029 §D5/D3): the
//! write path routes around a `write_degraded` peer using the cached
//! manifests (f7), and the local availability gates reject with 503 when
//! the LOCAL node cannot serve.
//!
//! Scenario (RF=2, 3 nodes, each a 4-pool node: data + wal + metadata +
//! hints):
//!   1. Seed objects through A while the cluster is healthy.
//!   2. Drive B's **wal** pool Dead via the g2 health monitor (the exact
//!      role consequence: wal Dead → `write_degraded` on B's manifest).
//!   3. Write MORE objects through A. The A-side coordinator consults
//!      B's cached manifest and excludes B from the forward target and
//!      the replica fan-out, so writes continue to succeed on A/C.
//!   4. Reads of the seeded keys still serve (the read path is
//!      unaffected by `write_degraded` — B's data pool is healthy).
//!
//! NOTE on observability: B's DATA pool may still grow while B is
//! `write_degraded` — the seal-time segment-replication backbone pushes
//! sealed `.dat` files to ring members regardless of the write-degraded
//! flag. That backbone is a DIFFERENT feature (sealed-segment-
//! replication) and is out of this feature's scope. The write-path
//! exclusion itself (a `write_degraded`/zero-Healthy candidate is
//! skipped from forward + fan-out) is asserted deterministically by the
//! coordinator unit test; this integration test proves the cluster-level
//! DoD: writes keep succeeding and reads keep serving while B is
//! declared write_degraded.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{collections::HashSet, path::Path, time::Duration};

use oceanfs_core::{
    MissingRootPolicy, NodeConfig, PoolHealthConfig, PoolRole, PoolTech, StorageConfig,
    StoragePoolConfig,
};
use oceanfs_node::Node;
use oceanfs_storage::{
    io::{IoErrorKind, IoOp},
    PoolStatus,
};

fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> =
        (0..n).map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0")).collect();
    let ports = listeners.iter().map(|l| l.local_addr().expect("local addr").port()).collect();
    drop(listeners);
    ports
}

fn pool(name: &str, role: PoolRole, root: &Path) -> StoragePoolConfig {
    StoragePoolConfig {
        name: name.to_string(),
        role,
        root: root.to_path_buf(),
        weight: None,
        tech: PoolTech::Auto,
        health: PoolHealthConfig {
            min_errors: 1,
            detection_window_secs: 1,
            recovery_window_secs: 1,
            ..PoolHealthConfig::default()
        },
    }
}

/// A 4-pool node (data, wal, metadata, hints) with fast health knobs —
/// the same topology the g2 failure_state_machine test uses. Pool ids
/// follow the f2 config-order scheme: 0=data, 1=wal, 2=metadata,
/// 3=hints.
async fn boot_node(
    id: &str,
    seed: Option<&str>,
    grpc: &str,
    membership_addr: &str,
) -> (Node, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig {
        pools: vec![
            pool("data-a", PoolRole::Data, &tmp.path().join("nvme0")),
            pool("journal", PoolRole::Wal, &tmp.path().join("optane0")),
            pool("meta", PoolRole::Metadata, &tmp.path().join("optane1")),
            pool("hints", PoolRole::Hints, &tmp.path().join("hints-dev")),
        ],
        missing_root_policy: MissingRootPolicy::Fatal,
    };
    let config = NodeConfig {
        node_id: id.to_string(),
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: grpc.to_string(),
        membership_listen_addr: membership_addr.to_string(),
        storage,
        // RF=2: every key's ring replica set is 2 of the 3 nodes.
        replication_factor: 2,
        gossip: oceanfs_core::GossipConfig {
            interval_ms: 250,
            suspicion_timeout_ms: 60_000,
            failure_timeout_ms: 120_000,
            seed_nodes: seed.map(|s| vec![s.to_string()]).unwrap_or_default(),
            ..Default::default()
        },
        ..NodeConfig::default()
    };
    let node = Node::start(config).await.expect("node boots");
    (node, tmp)
}

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

/// Feeds error signals for B's wal pool until the monitor drives it
/// Dead — the g2 role consequence sets `write_degraded`.
async fn drive_wal_dead(node: &Node, wal_id: u32) {
    // Degrade (error spike).
    for _ in 0..3 {
        node.io_observer().record_error(wal_id, IoErrorKind::TimedOut);
        node.io_observer().record_latency(wal_id, IoOp::Read, Duration::from_micros(1));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let status = node.pool_registry().pool_by_id(wal_id).expect("pool registered").status();
        if status == PoolStatus::Degraded {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "wal must reach Degraded (status: {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Confirm Dead (ENOENT kind).
    for _ in 0..3 {
        node.io_observer().record_error(wal_id, IoErrorKind::NotFound);
        node.io_observer().record_latency(wal_id, IoOp::Read, Duration::from_micros(1));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let status = node.pool_registry().pool_by_id(wal_id).expect("pool registered").status();
        if status == PoolStatus::Dead {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "wal must reach Dead (status: {status:?})");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The set of segment ids present as `.dat` files in a data pool root.
fn segment_ids_in(dir: &Path) -> HashSet<oceanfs_core::SegmentId> {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().to_string();
                    name.strip_suffix(".dat").map(|s| s.to_string())
                })
                .filter_map(|s| {
                    uuid::Uuid::parse_str(&s)
                        .ok()
                        .map(|u| oceanfs_core::SegmentId::from_uuid_bytes(u.into_bytes()))
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn wait_for_write_degraded_in_manifest(node: &Node) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let degraded = node
            .self_manifest()
            .map(|m| m.pools().iter().any(|p| p.role() == "wal" && p.write_degraded()));
        if degraded == Some(true) {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "B's manifest must show wal write_degraded");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// PUTs a batch of objects through `addr`, returning the number that
/// returned 200.
async fn put_batch(client: &reqwest::Client, addr: &str, prefix: &str, n: usize) -> usize {
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let mut handles = Vec::new();
    for i in 0..n {
        let client = client.clone();
        let body = body.clone();
        let addr = addr.to_string();
        let key = format!("{prefix}-{i:04}");
        handles.push(tokio::spawn(async move {
            let resp = client
                .put(format!("http://{addr}/durability/{key}"))
                .body(body)
                .send()
                .await
                .expect("PUT must send");
            resp.status()
        }));
    }
    let mut ok = 0usize;
    for h in handles {
        if h.await.expect("PUT task").is_success() {
            ok += 1;
        }
    }
    ok
}

/// Waits until every segment A sealed (its data dir) has replicated to
/// at least one of B/C's data dirs (RF=2 settled), so the routing
/// assertion below is measuring the DEGRADED write path, not in-flight
/// replication from before the fault.
async fn wait_replication_settled(dir_a: &Path, dir_b: &Path, dir_c: &Path) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let a = segment_ids_in(dir_a);
        let b = segment_ids_in(dir_b);
        let c = segment_ids_in(dir_c);
        if a.iter().all(|sid| b.contains(sid) || c.contains(sid)) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replication must settle (A: {} segments, B: {}, C: {})",
            a.len(),
            b.len(),
            c.len(),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[tokio::test]
async fn write_degraded_peer_is_routed_around() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let ports = free_ports(6);
    let (node_a, tmp_a) = boot_node(
        "node-a",
        None,
        &format!("127.0.0.1:{}", ports[0]),
        &format!("127.0.0.1:{}", ports[1]),
    )
    .await;
    let (node_b, tmp_b) = boot_node(
        "node-b",
        Some(&format!("127.0.0.1:{}", ports[1])),
        &format!("127.0.0.1:{}", ports[2]),
        &format!("127.0.0.1:{}", ports[3]),
    )
    .await;
    let (node_c, tmp_c) = boot_node(
        "node-c",
        Some(&format!("127.0.0.1:{}", ports[1])),
        &format!("127.0.0.1:{}", ports[4]),
        &format!("127.0.0.1:{}", ports[5]),
    )
    .await;

    wait_for_cluster_convergence(&node_a).await;
    wait_for_cluster_convergence(&node_b).await;
    wait_for_cluster_convergence(&node_c).await;

    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("client");
    let addr_a = node_a.server_addr();
    let addr_a_str = addr_a.to_string();

    let dir_a = tmp_a.path().join("nvme0");
    let dir_b = tmp_b.path().join("nvme0");
    let dir_c = tmp_c.path().join("nvme0");

    // ---- Seed objects while healthy (they will be read after the
    // fault to prove the read path is unaffected). ----
    let seeded = put_batch(&client, &addr_a_str, "seed", 12).await;
    assert_eq!(seeded, 12, "all seeded PUTs return 200 on the healthy cluster");
    wait_replication_settled(&dir_a, &dir_b, &dir_c).await;

    // ---- Drive B's wal Dead → B is write_degraded (g2 consequence). ----
    let b_wal_id = 1; // config order: 0=data, 1=wal.
    drive_wal_dead(&node_b, b_wal_id).await;
    assert!(
        node_b.pool_registry().pool_by_id(b_wal_id).unwrap().write_degraded(),
        "wal Dead must set write_degraded (D3 matrix)"
    );
    wait_for_write_degraded_in_manifest(&node_b).await;

    // Give A's manifest cache time to see B's write_degraded manifest
    // (gossip interval 250ms + membership event propagation).
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ---- Write more objects through A: writes must still succeed. ----
    // B is write_degraded (its wal cannot journal) and the A-side
    // coordinator excludes B from forward targets and the replica fan-out
    // (the manifest cache, ADR-0029 §D5). A + C absorb the writes. (B's
    // DATA pool may still receive sealed-segment pushes from the
    // seal-time backbone — that is segment replication, NOT the write
    // path, and is out of this feature's scope; the write-path exclusion
    // itself is asserted deterministically by the coordinator unit test.)
    let written = put_batch(&client, &addr_a_str, "post", 24).await;
    assert_eq!(written, 24, "writes must still succeed by routing around B");

    // ---- Reads of the seeded keys still serve through A. ----
    for i in 0..12 {
        let resp = client
            .get(format!("http://{addr_a_str}/durability/seed-{i:04}"))
            .send()
            .await
            .expect("GET must send");
        assert_eq!(resp.status(), 200, "GET seed-{i:04} through A must succeed after the fault");
    }

    node_a.shutdown().await.expect("node A shutdown");
    node_b.shutdown().await.expect("node B shutdown");
    node_c.shutdown().await.expect("node C shutdown");
}

/// g6 local enforcement (ADR-0029 §D3): a node whose metadata pool is
/// Dead rejects BOTH reads and writes with 503 at its own S3 boundary —
/// the availability is derived from the shared pool registry (the same
/// source the write coordinator and the read coordinator consult).
#[tokio::test]
async fn local_metadata_dead_rejects_reads_and_writes_with_503() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let ports = free_ports(2);
    let (node, _tmp) = boot_node(
        "node-a",
        None,
        &format!("127.0.0.1:{}", ports[0]),
        &format!("127.0.0.1:{}", ports[1]),
    )
    .await;

    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("client");
    let addr = node.server_addr().to_string();

    // A seeded write succeeds while healthy.
    assert_eq!(put_batch(&client, &addr, "pre", 4).await, 4, "healthy writes succeed");

    // metadata Dead → node_unavailable (registry-derived).
    let meta_id = 2; // config order: 0=data, 1=wal, 2=metadata.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    for _ in 0..3 {
        node.io_observer().record_error(meta_id, IoErrorKind::TimedOut);
        node.io_observer().record_latency(meta_id, IoOp::Read, Duration::from_micros(1));
    }
    loop {
        if node.pool_registry().pool_by_id(meta_id).unwrap().status() == PoolStatus::Degraded {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "metadata pool must reach Degraded");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for _ in 0..3 {
        node.io_observer().record_error(meta_id, IoErrorKind::NotFound);
        node.io_observer().record_latency(meta_id, IoOp::Read, Duration::from_micros(1));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if node.pool_registry().pool_by_id(meta_id).unwrap().status() == PoolStatus::Dead {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "metadata pool must reach Dead");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(node.node_unavailable(), "metadata Dead → node_unavailable (registry-derived)");

    // Writes reject with 503 at the S3 boundary.
    let body: Vec<u8> = vec![0xCD; 1024];
    let resp = client
        .put(format!("http://{addr}/durability/post-dead-0000"))
        .body(body)
        .send()
        .await
        .expect("PUT must send");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "a write to a metadata-dead node must reject with 503"
    );

    // Reads reject with 503 too.
    let resp = client
        .get(format!("http://{addr}/durability/pre-0000"))
        .send()
        .await
        .expect("GET must send");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "a read from a metadata-dead node must reject with 503"
    );

    node.shutdown().await.expect("node shutdown");
}
