//! Integration test: write coordinator and quorum replication.
//!
//! Tests quorum fan-out, ack collection, and error handling.

#![cfg(all(feature = "membership", feature = "network"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_core::{BucketId, HashKey, HlcClock, NodeId, ObjectKey};
use oceanfs_routing::{hash_key, Ring, RingCache};
use oceanfs_server::{WriteCoordinator, WriteRequest};

#[tokio::test]
async fn write_quorum_1_with_local_node_succeeds() {
    let coord = make_coordinator("n1", &["n1", "n2", "n3"]);
    let req = write_request("obj-1", b"hello", 1);
    let result = coord.put(req).await;
    assert!(result.is_ok(), "write with quorum 1 should succeed");
    let wr = result.unwrap();
    assert_eq!(wr.size, 5);
    assert_eq!(wr.object_key.as_str(), "obj-1");
}

#[tokio::test]
async fn write_triggers_hlc_advance() {
    let coord = make_coordinator("n1", &["n1"]);
    let before = coord.hlc_clock().now();
    let req = write_request("hlc-test", b"data", 1);
    coord.put(req).await.unwrap();
    let after = coord.hlc_clock().now();
    assert!(after > before, "HLC must advance after write");
}

#[tokio::test]
async fn write_with_quorum_capped_to_replica_count() {
    // 1 node in ring, but requested quorum of 3 — capped to 1, succeeds.
    let coord = make_coordinator("n1", &["n1"]);
    let mut req = write_request("capped", b"x", 3);
    req.write_quorum = 3;
    let result = coord.put(req).await;
    assert!(result.is_ok(), "quorum capped to 1 should succeed");
}

fn make_coordinator(node_id: &str, nodes: &[&str]) -> WriteCoordinator {
    use oceanfs_core::{GossipConfig, Incarnation, NodeState, RingConfig, RpcConfig};
    use oceanfs_membership::Membership;
    use oceanfs_network::ConnectionPool;
    use std::net::SocketAddr;

    let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
    for n in nodes {
        ring.add_node(NodeId::new(*n));
    }
    let ring_cache = Arc::new(RingCache::new(ring));
    let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let membership = Arc::new(Membership::new(
        NodeId::new(node_id), addr, GossipConfig::default(), ring_cache.clone(),
    ));
    for n in nodes {
        membership.upsert_node(NodeId::new(*n), NodeState::Alive, Incarnation::new(1), addr);
    }
    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    WriteCoordinator::new(ring_cache, membership, pool, NodeId::new(node_id), Arc::new(HlcClock::new()))
}

fn write_request(key: &str, data: &[u8], quorum: u8) -> WriteRequest {
    WriteRequest {
        bucket: BucketId::new("test"),
        key: ObjectKey::new(key),
        hash_key: HashKey::from_bytes(hash_key(key.as_bytes())),
        data: Bytes::copy_from_slice(data),
        write_quorum: quorum,
        ack_after_wal: true,
        ec_async: false,
        policy: None,
    }
}
