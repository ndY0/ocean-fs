//! Integration test: routing with consistent hashing ring lifecycle.

#![allow(clippy::unwrap_used)]

use oceanfs_core::{NodeId, RingConfig};
use oceanfs_routing::Ring;

#[test]
fn ring_empty_returns_zero_node_count() {
    let config = RingConfig { vnodes_per_node: 16, replication_factor: 3 };
    let ring = Ring::new(config);
    assert_eq!(ring.node_count(), 0);
}

#[test]
fn ring_add_and_remove_node() {
    let config = RingConfig { vnodes_per_node: 8, replication_factor: 2 };
    let mut ring = Ring::new(config);
    let node = NodeId::new("test-node");
    ring.add_node(node.clone());
    assert_eq!(ring.node_count(), 1);
    ring.remove_node(node).unwrap();
    assert_eq!(ring.node_count(), 0);
}

#[test]
fn ring_serialization_roundtrip_preserves_lookups() {
    let mut ring = Ring::new(RingConfig { vnodes_per_node: 16, replication_factor: 2 });
    ring.add_node(NodeId::new("alpha"));
    ring.add_node(NodeId::new("beta"));

    let encoded = serde_json::to_string(&ring).unwrap();
    let decoded: Ring = serde_json::from_str(&encoded).unwrap();

    let hash = [0x42u8; 32];
    assert_eq!(ring.lookup(&hash), decoded.lookup(&hash));
}

#[test]
fn ring_lookup_returns_replication_factor_nodes() {
    let mut ring = Ring::new(RingConfig { vnodes_per_node: 32, replication_factor: 2 });
    ring.add_node(NodeId::new("n1"));
    ring.add_node(NodeId::new("n2"));
    ring.add_node(NodeId::new("n3"));

    let hash = [0xABu8; 32];
    let successors = ring.lookup(&hash);
    assert_eq!(successors.len(), 2, "should return exactly replication_factor nodes");
}

#[test]
fn ring_config_propagates() {
    let config = RingConfig { vnodes_per_node: 32, replication_factor: 5 };
    let ring = Ring::new(config);
    assert_eq!(ring.config().vnodes_per_node, 32);
    assert_eq!(ring.config().replication_factor, 5);
}

#[test]
fn remove_nonexistent_node_errors() {
    let config = RingConfig { vnodes_per_node: 8, replication_factor: 2 };
    let mut ring = Ring::new(config);
    assert!(ring.remove_node(NodeId::new("ghost")).is_err());
}
