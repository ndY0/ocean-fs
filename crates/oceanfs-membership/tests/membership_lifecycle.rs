//! Integration test: SWIM membership lifecycle.
//!
//! Tests the public API of the membership subsystem: node addition,
//! state transitions, event broadcasting, join/leave flow, and
//! incarnation tracking across state changes.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{net::SocketAddr, sync::Arc};

use oceanfs_core::{GossipConfig, Incarnation, NodeId, NodeState, RingConfig};
use oceanfs_membership::Membership;
use oceanfs_routing::{Ring, RingCache};

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
        NodeState::Dead,
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
        NodeState::Dead,
        Incarnation::new(1),
        "127.0.0.1:9020".parse().unwrap(),
    );

    assert_eq!(membership.state_of(&NodeId::new("known")), Some(NodeState::Dead));
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

        let membership = Membership::new(
            NodeId::new("leaver"),
            test_addr(),
            GossipConfig::default(),
            ring_cache.clone(),
        );

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
