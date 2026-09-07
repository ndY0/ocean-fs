//! Integration test (g8 `metadata-loss-recovery`, ADR-0029 §D7): a 3-node
//! cluster loses node A's metadata pool and recovers by rebuilding a fresh
//! objects+deletions store from peers over A's owned ring ranges.
//!
//! Flow:
//! 1. Objects are written through A (RF=3 → replicated to B and C); one
//!    object is DELETED so a deletion row must survive the loss.
//! 2. A's metadata pool is driven Dead (the D3 health monitor). A stays
//!    alive (sockets up) but serves 503 for object operations; its
//!    manifest carries `node_unavailable` (the peer routing rule —
//!    asserted at the predicate level in routing_cache/repair unit tests,
//!    and observable here as the cluster continuing to serve through
//!    B/C while A is unavailable, with no re-replication).
//! 3. A shuts down; its metadata root is replaced out-of-band (emptied);
//!    A restarts IN-PROCESS on the same dirs/addresses (the g7 shutdown
//!    discipline releases the RocksDB LOCK + listeners).
//! 4. The boot branch detects the replaced store (fresh CFs + intact
//!    lifecycle registry), gates the node, rebuilds objects + deletions
//!    from B/C over its owned ranges, then reopens. Assertions: the
//!    rebuild metrics show folded objects AND deletions ≥ 1, the write
//!    gate clears, every pre-kill key reads back byte-identical through
//!    A, a key written DURING the loss window is readable through A, the
//!    pre-kill DELETE stays honored (404), and no data-pool `.dat` was
//!    swept.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{path::PathBuf, time::Duration};

use oceanfs_core::{
    MissingRootPolicy, NodeConfig, PoolHealthConfig, PoolRole, PoolTech, StorageConfig,
    StoragePoolConfig,
};
use oceanfs_node::Node;
use oceanfs_storage::io::IoErrorKind;

struct NodeAddrs {
    grpc: String,
    membership: String,
}

fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> =
        (0..n).map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0")).collect();
    let ports = listeners.iter().map(|l| l.local_addr().expect("local addr").port()).collect();
    drop(listeners);
    ports
}

fn pool(name: &str, role: PoolRole, root: PathBuf, fast_health: bool) -> StoragePoolConfig {
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

fn storage_pools(tmp: &tempfile::TempDir, fast_meta_health: bool) -> StorageConfig {
    StorageConfig {
        pools: vec![
            pool("data-0", PoolRole::Data, tmp.path().join("pool-data"), false),
            pool("wal-0", PoolRole::Wal, tmp.path().join("pool-wal"), false),
            // The metadata pool uses the fast health knobs so the loss is
            // testable in seconds.
            pool("meta-0", PoolRole::Metadata, tmp.path().join("pool-meta"), fast_meta_health),
            pool("hints-0", PoolRole::Hints, tmp.path().join("pool-hints"), false),
        ],
        missing_root_policy: MissingRootPolicy::Fatal,
    }
}

async fn boot_node(
    id: &str,
    seed: Option<&str>,
    addrs: &NodeAddrs,
    tmp: &tempfile::TempDir,
    fast_meta_health: bool,
) -> Node {
    let config = NodeConfig {
        node_id: id.to_string(),
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: addrs.grpc.clone(),
        membership_listen_addr: addrs.membership.clone(),
        storage: storage_pools(tmp, fast_meta_health),
        gossip: oceanfs_core::GossipConfig {
            interval_ms: 250,
            suspicion_timeout_ms: 60_000,
            failure_timeout_ms: 120_000,
            seed_nodes: seed.map(|s| vec![s.to_string()]).unwrap_or_default(),
            ..Default::default()
        },
        replication_factor: 3,
        ..NodeConfig::default()
    };
    Node::start(config).await.expect("node boots")
}

async fn wait_for_cluster_convergence(node: &Node) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if node.segment_replicator().ring_node_count() >= 3 {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "cluster must converge to 3 nodes");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn put(client: &reqwest::Client, addr: std::net::SocketAddr, key: &str, body: &[u8]) -> u16 {
    client
        .put(format!("http://{addr}/durability/{key}"))
        .body(body.to_vec())
        .send()
        .await
        .expect("PUT must reach the node")
        .status()
        .as_u16()
}

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

async fn wait_for_dat_count(root: &std::path::Path, expected: usize, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let count = data_dats(root).len();
        if count >= expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: expected ≥ {expected} .dat within 60s (has {count})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

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

fn parse_metric(text: &str, name: &str) -> u64 {
    text.lines()
        .find(|l| l.trim_start().starts_with(name) && !l.trim_start().starts_with('#'))
        .and_then(|l| l.trim().strip_prefix(name).map(|rest| rest.trim_start()))
        .and_then(|v| v.trim_end().split_whitespace().next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

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

async fn wait_for_write_resume(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    key: &str,
    body: &[u8],
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let status = put(client, addr, key, body).await;
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

async fn read_metric(client: &reqwest::Client, addr: std::net::SocketAddr, name: &str) -> u64 {
    let resp = client
        .get(format!("http://{addr}/admin/metrics"))
        .send()
        .await
        .expect("GET /admin/metrics must reach the node");
    assert_eq!(resp.status(), 200, "metrics endpoint serves");
    parse_metric(&resp.text().await.expect("metrics body"), name)
}

#[tokio::test]
async fn metadata_loss_rebuilds_fresh_store_from_peers() {
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
    let addr_b = node_b.server_addr();
    let addr_c = node_c.server_addr();

    // Phase 1: write objects through A; delete one so a deletion row must
    // survive the metadata loss. RF=3 → every row replicates to B and C.
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let keys: Vec<String> = (0..6).map(|i| format!("pre-kill-{i:02}")).collect();
    for key in &keys {
        assert_eq!(put(&client, addr_a, key, &body).await, 200);
    }
    let deleted_key = "pre-kill-deleted";
    assert_eq!(put(&client, addr_a, deleted_key, &body).await, 200);
    let del_resp = client
        .delete(format!("http://{addr_a}/durability/{deleted_key}"))
        .send()
        .await
        .expect("DELETE must reach A");
    assert!(del_resp.status().is_success(), "pre-kill DELETE succeeds (got {})", del_resp.status());

    let data_root_a = tmp_a.path().join("pool-data");
    let data_root_b = tmp_b.path().join("pool-data");
    let data_root_c = tmp_c.path().join("pool-data");
    wait_for_dat_count(&data_root_a, 1, "owner A data pool").await;
    let owner_dats = data_dats(&data_root_a);
    assert!(!owner_dats.is_empty(), "A sealed ≥ 1 segment before the kill");
    wait_for_dat_count(&data_root_b, owner_dats.len(), "replica B data pool").await;
    wait_for_dat_count(&data_root_c, owner_dats.len(), "replica C data pool").await;

    // Baseline re-replication counters (the epic DoD: metadata loss must
    // NOT re-replicate — data pools + registry are intact). The counters
    // must stay flat through the loss window and the rebuild.
    let rerep_base_b = read_metric(&client, addr_b, "oceanfs_ranges_re_replicated_total").await;
    let rerep_base_c = read_metric(&client, addr_c, "oceanfs_ranges_re_replicated_total").await;

    // A local write from B during the (future) window will be created
    // later; capture nothing yet.

    // Phase 2: drive A's metadata pool Dead (D3) — the loss window.
    let meta_pool_id =
        node_a.pool_registry().pool_by_role(PoolRole::Metadata).expect("A meta pool").id();
    for _ in 0..3 {
        node_a.io_observer().record_error(meta_pool_id, IoErrorKind::TimedOut);
        node_a.io_observer().record_latency(
            meta_pool_id,
            oceanfs_storage::io::IoOp::Read,
            Duration::from_micros(1),
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if node_a.pool_registry().pool_by_id(meta_pool_id).expect("A meta pool").status()
            == oceanfs_storage::PoolStatus::Degraded
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "A's metadata pool must degrade first");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for _ in 0..3 {
        node_a.io_observer().record_error(meta_pool_id, IoErrorKind::NotFound);
        node_a.io_observer().record_latency(
            meta_pool_id,
            oceanfs_storage::io::IoOp::Read,
            Duration::from_micros(1),
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if node_a.pool_registry().pool_by_id(meta_pool_id).expect("A meta pool").status()
            == oceanfs_storage::PoolStatus::Dead
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "A's metadata pool must reach Dead");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // A is alive but cannot serve object operations (local 503 gate)…
    let dead_get = client
        .get(format!("http://{addr_a}/durability/{}", keys[0]))
        .send()
        .await
        .expect("GET A during the window");
    assert_eq!(
        dead_get.status().as_u16(),
        503,
        "A serves 503 for object reads while its metadata pool is Dead"
    );
    // …while B and C keep serving the cluster (no data loss, no SWIM wait).
    let healthy_get = client
        .get(format!("http://{addr_b}/durability/{}", keys[0]))
        .send()
        .await
        .expect("GET B during the window");
    assert_eq!(healthy_get.status().as_u16(), 200, "B serves reads while A is unavailable");

    // Writes landing on a healthy replica succeed during the window (the
    // row must be recovered by A's rebuild).
    let during_key = "written-during-loss-window";
    assert_eq!(put(&client, addr_b, during_key, &body).await, 200);

    // Phase 3: replace A's metadata root out-of-band and restart A.
    node_a.shutdown().await.expect("A shutdown");
    empty_dir(&tmp_a.path().join("pool-meta"));

    let node_a2 = boot_node("node-a", None, &a_addrs, &tmp_a, true).await;
    let addr_a2 = node_a2.server_addr();
    wait_for_cluster_convergence(&node_a2).await;

    // Phase 4: the boot branch rebuilt a fresh store from peers.
    wait_for_metric(
        &client,
        addr_a2,
        "oceanfs_metadata_rebuild_objects_total",
        1,
        "objects folded",
    )
    .await;
    wait_for_metric(
        &client,
        addr_a2,
        "oceanfs_metadata_rebuild_deletions_total",
        1,
        "deletions folded",
    )
    .await;

    // Zero re-replication traffic (epic metadata-pool-kill DoD): the
    // peers' re-replication counters stay flat and A2 started at zero —
    // only the local index was rebuilt, nothing was re-replicated.
    assert_eq!(
        read_metric(&client, addr_b, "oceanfs_ranges_re_replicated_total").await,
        rerep_base_b,
        "B must not re-replicate during A's metadata loss/rebuild"
    );
    assert_eq!(
        read_metric(&client, addr_c, "oceanfs_ranges_re_replicated_total").await,
        rerep_base_c,
        "C must not re-replicate during A's metadata loss/rebuild"
    );
    assert_eq!(
        read_metric(&client, addr_a2, "oceanfs_ranges_re_replicated_total").await,
        0,
        "A2 must not re-replicate during the rebuild"
    );

    wait_for_write_resume(&client, addr_a2, "post-recovery-write", &body).await;

    // Every pre-kill key reads back byte-identical THROUGH A2.
    for key in &keys {
        let resp =
            client.get(format!("http://{addr_a2}/durability/{key}")).send().await.expect("GET A2");
        assert_eq!(resp.status().as_u16(), 200, "GET {key} succeeds after rebuild");
        assert_eq!(
            &resp.bytes().await.expect("body")[..],
            &body[..],
            "object {key} byte-identical"
        );
    }
    // The key written DURING the loss window is recovered through A2 too.
    let during = client
        .get(format!("http://{addr_a2}/durability/{during_key}"))
        .send()
        .await
        .expect("GET during");
    assert_eq!(during.status().as_u16(), 200, "loss-window write recovered through A2");
    assert_eq!(&during.bytes().await.expect("body")[..], &body[..]);

    // The pre-kill DELETE is honored (the deletion row was rebuilt).
    let deleted = client
        .get(format!("http://{addr_a2}/durability/{deleted_key}"))
        .send()
        .await
        .expect("GET deleted");
    assert_eq!(deleted.status().as_u16(), 404, "pre-kill delete stays deleted after rebuild");

    // No data-pool `.dat` was swept by the recovery.
    let current = data_dats(&data_root_a);
    for expected in &owner_dats {
        assert!(
            current.contains(expected),
            "recovery must not sweep intact .dat {}",
            expected.display()
        );
    }

    node_a2.shutdown().await.expect("A2 shutdown");
    node_b.shutdown().await.expect("B shutdown");
    node_c.shutdown().await.expect("C shutdown");
}
