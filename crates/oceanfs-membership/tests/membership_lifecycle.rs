//! Integration test: SWIM membership lifecycle.
//!
//! Tests the public API of the membership subsystem: node addition,
//! state transitions, event broadcasting, join/leave flow, and
//! incarnation tracking across state changes.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{net::SocketAddr, sync::Arc};

use oceanfs_core::{GossipConfig, Incarnation, NodeId, NodeState, RingConfig, RpcConfig};
use oceanfs_membership::{grpc::gossip_service::GossipGrpcService, Membership};
use oceanfs_network::{gossip::gossip_rpc_server::GossipRpcServer, ConnectionPool};
use oceanfs_routing::{Ring, RingCache};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn make_ring() -> Arc<RingCache> {
    let mut ring = Ring::new(RingConfig::default());
    ring.add_node(NodeId::new("node-1"));
    ring.add_node(NodeId::new("node-2"));
    Arc::new(RingCache::new(ring))
}

fn test_addr() -> SocketAddr {
    "127.0.0.1:9001".parse().unwrap()
}

#[test]
fn membership_creation_has_correct_node_id() {
    let ring = make_ring();
    let membership =
        Membership::new(NodeId::new("test-node"), test_addr(), GossipConfig::default(), ring);
    assert_eq!(membership.node_id().as_str(), "test-node");
}

#[test]
fn subscribe_receives_state_change_events() {
    let ring = make_ring();
    let membership =
        Membership::new(NodeId::new("observer"), test_addr(), GossipConfig::default(), ring);

    let mut rx = membership.subscribe();

    // Upsert a node — should emit an event.
    membership.upsert_node(
        NodeId::new("remote-node"),
        NodeState::Alive,
        Incarnation::new(1),
        "127.0.0.1:9002".parse().unwrap(),
    );

    // The upsert of a new node emits an event (old_state=Alive → new_state=Alive).
    let event = rx.try_recv().expect("should receive membership event");
    assert_eq!(event.node_id.as_str(), "remote-node");
    assert_eq!(event.new_state, NodeState::Alive);
}

#[test]
fn state_transition_emits_event_with_correct_old_and_new_state() {
    let ring = make_ring();
    let membership =
        Membership::new(NodeId::new("observer"), test_addr(), GossipConfig::default(), ring);

    let mut rx = membership.subscribe();

    // Add a node as ALIVE.
    membership.upsert_node(
        NodeId::new("target"),
        NodeState::Alive,
        Incarnation::new(1),
        "127.0.0.1:9003".parse().unwrap(),
    );
    // Drain the initial add event.
    let _ = rx.try_recv();

    // Transition to SUSPECT.
    membership.upsert_node(
        NodeId::new("target"),
        NodeState::Suspect,
        Incarnation::new(1),
        "127.0.0.1:9003".parse().unwrap(),
    );
    let event = rx.try_recv().expect("should receive SUSPECT event");
    assert_eq!(event.old_state, NodeState::Alive);
    assert_eq!(event.new_state, NodeState::Suspect);
}

#[test]
fn nodes_returns_all_known_nodes() {
    let ring = make_ring();
    let membership =
        Membership::new(NodeId::new("local"), test_addr(), GossipConfig::default(), ring);

    membership.upsert_node(
        NodeId::new("a"),
        NodeState::Alive,
        Incarnation::new(1),
        "127.0.0.1:9010".parse().unwrap(),
    );
    membership.upsert_node(
        NodeId::new("b"),
        NodeState::Suspect,
        Incarnation::new(1),
        "127.0.0.1:9011".parse().unwrap(),
    );

    let nodes = membership.nodes();
    assert_eq!(nodes.len(), 2);
    assert!(nodes.iter().any(|(id, _)| id.as_str() == "a"));
    assert!(nodes.iter().any(|(id, _)| id.as_str() == "b"));
}

#[test]
fn state_of_returns_correct_state_for_known_node() {
    let ring = make_ring();
    let membership =
        Membership::new(NodeId::new("local"), test_addr(), GossipConfig::default(), ring);

    membership.upsert_node(
        NodeId::new("known"),
        NodeState::Suspect,
        Incarnation::new(1),
        "127.0.0.1:9020".parse().unwrap(),
    );

    assert_eq!(membership.state_of(&NodeId::new("known")), Some(NodeState::Suspect));
    assert_eq!(membership.state_of(&NodeId::new("unknown")), None);
}

#[test]
fn join_without_seed_nodes_adds_self_to_ring() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("existing"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let membership = Membership::new(
            NodeId::new("joiner"),
            test_addr(),
            GossipConfig { seed_nodes: vec![], ..GossipConfig::default() },
            ring_cache.clone(),
        );

        membership.join().await.expect("join should succeed");

        // After join, the ring should include the joiner.
        let snap = ring_cache.snapshot();
        assert!(snap.nodes().contains(&NodeId::new("joiner")));
    });
}

#[test]
fn leave_transitions_state_and_removes_self_from_ring() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("leaver"));
        ring.add_node(NodeId::new("other"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let membership = Arc::new(Membership::new(
            NodeId::new("leaver"),
            test_addr(),
            GossipConfig::default(),
            ring_cache.clone(),
        ));

        // Start background tasks.
        membership.start().expect("start should succeed");

        // Leave.
        membership.leave().await.expect("leave should succeed");

        // After leave, the ring should NOT include the leaver.
        let snap = ring_cache.snapshot();
        assert!(!snap.nodes().contains(&NodeId::new("leaver")));
        assert!(snap.nodes().contains(&NodeId::new("other")));
    });
}

/// PR5: Verifies that after a joiner calls `join()` on a seed,
/// the seed's ring contains the joiner (and vice versa).
#[test]
fn seed_learns_joiner_on_join_via_push_after_pull() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // ---- Seed setup ----
        let mut seed_ring = Ring::new(RingConfig::default());
        seed_ring.add_node(NodeId::new("seed-node"));
        let seed_ring_cache = Arc::new(RingCache::new(seed_ring));
        let seed_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

        let seed_membership = Arc::new(Membership::new(
            NodeId::new("seed-node"),
            seed_addr,
            GossipConfig::default(),
            seed_ring_cache.clone(),
        ));

        // Start a gRPC server for the seed's gossip service.
        let seed_grpc_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let seed_grpc = seed_membership.clone();
        let listener = tokio::net::TcpListener::bind(seed_grpc_addr).await.expect("bind seed gRPC");
        let seed_listen_addr = listener.local_addr().expect("seed listen addr");

        tokio::spawn(async move {
            Server::builder()
                .add_service(GossipRpcServer::new(GossipGrpcService::new(seed_grpc)))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("seed server failed");
        });

        // Give the server a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // ---- Joiner setup ----
        let mut joiner_ring = Ring::new(RingConfig::default());
        joiner_ring.add_node(NodeId::new("joiner-node"));
        let joiner_ring_cache = Arc::new(RingCache::new(joiner_ring));
        let joiner_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();

        let joiner_config = GossipConfig {
            seed_nodes: vec![seed_listen_addr.to_string()],
            ..GossipConfig::default()
        };

        let joiner_membership = Arc::new(Membership::new(
            NodeId::new("joiner-node"),
            joiner_addr,
            joiner_config,
            joiner_ring_cache.clone(),
        ));

        // Set up connection pool for the joiner so it can talk to the seed.
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        joiner_membership.set_pool(pool);

        // ---- Join ----
        joiner_membership.join().await.expect("joiner should join successfully");

        // ---- Assertions ----
        // The joiner's ring should contain itself.
        let joiner_snap = joiner_ring_cache.snapshot();
        assert!(
            joiner_snap.nodes().contains(&NodeId::new("joiner-node")),
            "joiner's ring should contain the joiner"
        );

        // PR5: The seed's ring should contain the joiner (push after pull).
        let seed_snap = seed_ring_cache.snapshot();
        assert!(
            seed_snap.nodes().contains(&NodeId::new("joiner-node")),
            "seed's ring should contain the joiner after join push"
        );

        // The seed should still have itself in the ring.
        assert!(
            seed_snap.nodes().contains(&NodeId::new("seed-node")),
            "seed's ring should contain the seed"
        );
    });
}
