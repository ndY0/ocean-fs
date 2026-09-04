//! Integration test (g3 `loss-announcement`, ADR-0029 §D4 fast path):
//! when a data pool on node A is confirmed Dead, A announces the
//! affected segment set to the replica holders (union of
//! `storage_locations − self`), and the holders verify + enqueue
//! re-replication repairs (g5 drains the queue).
//!
//! Flow:
//!   1. 3-node cluster, each with a data pool (pool id 0 on A, B, C).
//!   2. PUT objects on A → segments seal → the backbone pushes them to
//!      the segment ring replicas (B/C) and stamps `storage_locations`.
//!   3. Feed `NotFound` (ENOENT) signals to A's data pool observer →
//!      the health monitor drives the pool Dead → the consequence
//!      applier derives the affected segments and fans the announcement
//!      out to `union(storage_locations − self)` = {B, C}.
//!   4. B/C verify they hold the announced segments (registry contains
//!      them AND storage_locations includes A) and enqueue repairs.
//!   5. Assert B's and C's `pending_repairs()` grew by the held count.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use oceanfs_core::{
    MissingRootPolicy, NodeConfig, PoolHealthConfig, PoolRole, PoolTech, StorageConfig,
    StoragePoolConfig,
};
use oceanfs_node::Node;
use oceanfs_storage::{io::IoErrorKind, PoolStatus};

fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> =
        (0..n).map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0")).collect();
    let ports = listeners.iter().map(|l| l.local_addr().expect("local addr").port()).collect();
    drop(listeners);
    ports
}

struct NodeAddrs {
    grpc: String,
    membership: String,
}

/// One data pool per node (id 0), fast health knobs, fast GC off
/// (compaction would remap segments and change the announcement set —
/// out of scope here; the backbone's compaction tests cover that path).
async fn boot_node(id: &str, seed: Option<&str>, addrs: &NodeAddrs) -> (Node, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig {
        pools: vec![
            StoragePoolConfig {
                name: "data-a".to_string(),
                role: PoolRole::Data,
                root: tmp.path().join("nvme0"),
                weight: None,
                tech: PoolTech::Auto,
                // Fast detection: any error spikes, one clean tick recovers.
                health: PoolHealthConfig {
                    min_errors: 1,
                    detection_window_secs: 1,
                    recovery_window_secs: 1,
                    ..PoolHealthConfig::default()
                },
            },
            StoragePoolConfig {
                name: "journal".to_string(),
                role: PoolRole::Wal,
                root: tmp.path().join("optane0"),
                weight: None,
                tech: PoolTech::Auto,
                health: PoolHealthConfig::default(),
            },
            StoragePoolConfig {
                name: "meta".to_string(),
                role: PoolRole::Metadata,
                root: tmp.path().join("optane1"),
                weight: None,
                tech: PoolTech::Auto,
                health: PoolHealthConfig::default(),
            },
            StoragePoolConfig {
                name: "hints".to_string(),
                role: PoolRole::Hints,
                root: tmp.path().join("hints0"),
                weight: None,
                tech: PoolTech::Auto,
                health: PoolHealthConfig::default(),
            },
        ],
        missing_root_policy: MissingRootPolicy::Fatal,
    };
    let config = NodeConfig {
        node_id: id.to_string(),
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: addrs.grpc.clone(),
        membership_listen_addr: addrs.membership.clone(),
        storage,
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
        let ring_nodes = node.segment_replicator().ring_node_count();
        if ring_nodes >= 3 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cluster must converge to 3 nodes within 30s (ring has {ring_nodes})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Feeds error signals to drive the pool Healthy → Degraded (error
/// spike), then confirms Dead (ENOENT), then polls until Dead.
async fn kill_data_pool(node: &Node, pool_id: u32) {
    // Phase 1: error spike → Degraded (suspicion).
    for _ in 0..3 {
        node.io_observer().record_error(pool_id, IoErrorKind::TimedOut);
        node.io_observer().record_latency(
            pool_id,
            oceanfs_storage::io::IoOp::Read,
            Duration::from_micros(1),
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let status = node.pool_registry().pool_by_id(pool_id).expect("pool registered").status();
        if status == PoolStatus::Degraded {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "data pool must reach Degraded within 15s (status: {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Phase 2: confirmed loss (ENOENT) → Dead.
    for _ in 0..3 {
        node.io_observer().record_error(pool_id, IoErrorKind::NotFound);
        node.io_observer().record_latency(
            pool_id,
            oceanfs_storage::io::IoOp::Read,
            Duration::from_micros(1),
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let status = node.pool_registry().pool_by_id(pool_id).expect("pool registered").status();
        if status == PoolStatus::Dead {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "data pool must reach Dead within 15s (status: {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn data_pool_death_announces_affected_segments_to_holders() {
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
    let (node_a, tmp_a) = boot_node("node-a", None, &a_addrs).await;
    let (node_b, _tmp_b) = boot_node("node-b", Some(&a_addrs.membership), &b_addrs).await;
    let (node_c, _tmp_c) = boot_node("node-c", Some(&a_addrs.membership), &c_addrs).await;

    wait_for_cluster_convergence(&node_a).await;
    wait_for_cluster_convergence(&node_b).await;
    wait_for_cluster_convergence(&node_c).await;

    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("client");
    let addr_a = node_a.server_addr();

    // ---- PUT 4 × 32 KiB objects concurrently on A ----
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let keys: Vec<String> = (0..4).map(|i| format!("obj-{i:02}")).collect();
    let mut handles = Vec::new();
    for key in &keys {
        let client = client.clone();
        let body = body.clone();
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            let resp = client
                .put(format!("http://{addr_a}/durability/{key}"))
                .body(body)
                .send()
                .await
                .expect("PUT must succeed");
            assert_eq!(resp.status(), 200, "PUT {key} returns 200");
        }));
    }
    for h in handles {
        h.await.expect("PUT task");
    }

    // ---- Wait for seals + replication to land on B/C ----
    // In pool mode, segment files live under the DATA POOL ROOT
    // (`nvme0`), not `data/segments`.
    let segments_dir_a = tmp_a.path().join("nvme0");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let owner_segment_count = loop {
        let count = std::fs::read_dir(&segments_dir_a)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".dat"))
                    .count()
            })
            .unwrap_or(0);
        if count >= 1 {
            break count;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "owner must seal at least 1 segment within 60s (has {count})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    // The owner's replicator drained → every sealed segment's
    // storage_locations is stamped (the announcement target set).
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if node_a.segment_replicator().needs_len() == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replicator must drain before the pool is killed"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(owner_segment_count >= 1);

    // ---- Kill A's data pool (pool id 0 = the first config-order pool) ----
    // The health monitor drives it Dead; the consequence applier derives
    // the affected segments and fans the announcement to B/C.
    kill_data_pool(&node_a, 0).await;

    // ---- B and C must have received the announcement and enqueued ----
    // Each announced segment that B holds + A is a verified holder →
    // one repair enqueued. Every sealed segment's storage_locations is
    // [A, B, C] (RF=3), so B and C each enqueue owner_segment_count
    // repairs (minus any segment B does not hold — none here).
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let b_repairs = node_b.pending_repairs();
        let c_repairs = node_c.pending_repairs();
        if b_repairs >= owner_segment_count && c_repairs >= owner_segment_count {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "B and C must enqueue repairs for the announced segments \
             within 30s (B={b_repairs}, C={c_repairs}, expected ≥{owner_segment_count})"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    node_a.shutdown().await.expect("node A shutdown");
    node_b.shutdown().await.expect("node B shutdown");
    node_c.shutdown().await.expect("node C shutdown");
}
