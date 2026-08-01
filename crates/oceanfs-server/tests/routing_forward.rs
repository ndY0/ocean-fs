//! Integration test: key routing and request forwarding.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::sync::Arc;

use oceanfs_core::{GossipConfig, HashKey, Incarnation, NodeId, NodeState, RingConfig, RpcConfig};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::{hash_key, Ring, RingCache};
use oceanfs_server::Router;

fn make_router(local_node: &str, ring_nodes: &[&str]) -> Router {
    let mut ring = Ring::new(RingConfig { vnodes_per_node: 16, replication_factor: 3 });
    for node in ring_nodes {
        ring.add_node(NodeId::new(*node));
    }
    let ring_cache = Arc::new(RingCache::new(ring));

    let membership = Arc::new(Membership::new(
        NodeId::new(local_node),
        "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
        GossipConfig::default(),
        ring_cache.clone(),
    ));

    // Add all ring nodes to membership.
    for node in ring_nodes {
        membership.upsert_node(
            NodeId::new(*node),
            NodeState::Alive,
            Incarnation::new(1),
            "127.0.0.1:9001".parse().unwrap(),
        );
    }

    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    Router::new(ring_cache, membership, pool, NodeId::new(local_node))
}

fn make_hash(s: &str) -> HashKey {
    HashKey::from_bytes(hash_key(s.as_bytes()))
}

#[tokio::test]
async fn local_node_in_replica_set_returns_is_local_true() {
    let router = make_router("node-a", &["node-a", "node-b", "node-c"]);
    let key = make_hash("test-object");

    let response = router.route(key).await.expect("route should succeed");
    assert!(response.is_local, "local node should be in replica set");
    assert!(response.forward_target.is_none());
    assert!(!response.replica_set.is_empty());
}

#[tokio::test]
async fn remote_node_in_replica_set_returns_forward_target() {
    let router = make_router("node-x", &["node-a", "node-b", "node-c"]);
    let key = make_hash("test-object");

    let response = router.route(key).await.expect("route should succeed");
    assert!(!response.is_local, "node-x should not be in replica set");
    assert!(response.forward_target.is_some(), "forward target should be set");
}

#[tokio::test]
async fn route_with_retry_local_handles_locally() {
    let router = make_router("node-a", &["node-a", "node-b"]);
    let key = make_hash("local-test");

    let response = router.route_with_retry(key).await.expect("retry route should succeed");
    assert!(response.is_local);
    assert!(response.forward_target.is_none());
}

#[tokio::test]
async fn route_with_retry_skips_dead_nodes() {
    let router = {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 16, replication_factor: 3 });
        ring.add_node(NodeId::new("dead-node"));
        ring.add_node(NodeId::new("alive-node"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let membership = Arc::new(Membership::new(
            NodeId::new("requester"),
            "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
            GossipConfig::default(),
            ring_cache.clone(),
        ));

        // Add nodes — one dead, one alive.
        membership.upsert_node(
            NodeId::new("dead-node"),
            NodeState::Dead,
            Incarnation::new(1),
            "127.0.0.1:9002".parse().unwrap(),
        );
        membership.upsert_node(
            NodeId::new("alive-node"),
            NodeState::Alive,
            Incarnation::new(1),
            "127.0.0.1:9003".parse().unwrap(),
        );

        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        Router::new(ring_cache, membership, pool, NodeId::new("requester"))
    };

    let key = make_hash("retry-test");
    let response = router.route_with_retry(key).await.expect("retry should succeed");

    // The forward target should be the alive node, not the dead one.
    assert!(!response.is_local);
    assert_eq!(
        response.forward_target.as_ref().map(|n| n.as_str()),
        Some("alive-node"),
        "retry should skip dead node and select alive successor"
    );
}

#[tokio::test]
async fn retry_fails_when_all_successors_are_dead() {
    let router = {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 16, replication_factor: 2 });
        ring.add_node(NodeId::new("dead-1"));
        ring.add_node(NodeId::new("dead-2"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let membership = Arc::new(Membership::new(
            NodeId::new("requester"),
            "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
            GossipConfig::default(),
            ring_cache.clone(),
        ));

        membership.upsert_node(
            NodeId::new("dead-1"),
            NodeState::Dead,
            Incarnation::new(1),
            "127.0.0.1:9002".parse().unwrap(),
        );
        membership.upsert_node(
            NodeId::new("dead-2"),
            NodeState::Dead,
            Incarnation::new(1),
            "127.0.0.1:9003".parse().unwrap(),
        );

        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        Router::new(ring_cache, membership, pool, NodeId::new("requester"))
    };

    let key = make_hash("all-dead");
    let result = router.route_with_retry(key).await;
    assert!(result.is_err(), "routing should fail when all successors are dead");
}

#[tokio::test]
async fn hash_key_produces_consistent_results() {
    let k1 = make_hash("object-a");
    let k2 = make_hash("object-a");
    let k3 = make_hash("object-b");

    assert_eq!(k1.as_bytes(), k2.as_bytes());
    assert_ne!(k1.as_bytes(), k3.as_bytes());
}

#[test]
fn router_exposes_dependencies() {
    let router = make_router("n1", &["n1", "n2"]);
    // Verify Router exposes its components for inspection.
    let _ring = router.ring();
    let _membership = router.membership();
    let _pool = router.pool();
}
