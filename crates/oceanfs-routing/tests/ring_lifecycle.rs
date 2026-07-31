//! Integration test: DHT ring lifecycle.
//!
//! Creates a ring with 3 nodes, looks up 100 keys for uniform distribution,
//! adds a node, verifies rebalance ranges, and confirms all keys still resolve.

use oceanfs_core::{NodeId, RingConfig};
use oceanfs_routing::{hash_key, Ring, RingCache};
use std::collections::HashSet;
use std::sync::Arc;

#[test]
fn ring_lifecycle_three_nodes_uniform_distribution() {
    let config = RingConfig { vnodes_per_node: 64, replication_factor: 3 };
    let mut ring = Ring::new(config);

    // Add 3 nodes.
    ring.add_node(NodeId::new("node-a"));
    ring.add_node(NodeId::new("node-b"));
    ring.add_node(NodeId::new("node-c"));
    assert_eq!(ring.node_count(), 3);

    // Look up 100 keys — all should resolve to at least 1 node.
    for i in 0u32..100 {
        let key = format!("object-{}", i);
        let hash = hash_key(key.as_bytes());
        let successors = ring.lookup(&hash);
        assert!(!successors.is_empty(), "key '{}' resolved to empty set", key);
    }

    // Look up 100 keys — verify distribution is roughly uniform.
    let mut node_counts: std::collections::HashMap<NodeId, usize> =
        std::collections::HashMap::new();
    for i in 0u32..500 {
        let key = format!("uniform-test-{}", i);
        let hash = hash_key(key.as_bytes());
        let successors = ring.lookup(&hash);
        if let Some(first) = successors.first() {
            *node_counts.entry(first.clone()).or_insert(0) += 1;
        }
    }
    // With 3 nodes, each should get roughly 1/3 of keys (±30%).
    let expected: usize = 500 / 3;
    for count in node_counts.values() {
        let lower = expected.saturating_sub(expected * 3 / 10);
        let upper = expected + expected * 3 / 10;
        assert!(
            *count >= lower && *count <= upper,
            "node distribution uneven: {} not in [{}, {}]",
            count, lower, upper
        );
    }
}

#[test]
fn add_node_rebalances_and_all_keys_resolve() {
    let mut ring = Ring::new(RingConfig { vnodes_per_node: 32, replication_factor: 3 });

    ring.add_node(NodeId::new("a"));
    ring.add_node(NodeId::new("b"));

    // Record pre-add lookups for 50 keys.
    let keys: Vec<[u8; 32]> = (0..50)
        .map(|i| hash_key(format!("key-{}", i).as_bytes()))
        .collect();

    // Add a new node.
    let ranges = ring.add_node(NodeId::new("c"));
    assert!(!ranges.is_empty(), "add_node should return rebalance ranges");
    assert_eq!(ring.node_count(), 3);

    // All keys must still resolve.
    for hash in &keys {
        let successors = ring.lookup(hash);
        assert!(!successors.is_empty(), "key did not resolve after add_node");
    }
}

#[test]
fn remove_node_rebalances_and_remaining_keys_resolve() {
    let config = RingConfig { vnodes_per_node: 16, replication_factor: 3 };
    let mut ring = Ring::new(config);

    ring.add_node(NodeId::new("x"));
    ring.add_node(NodeId::new("y"));
    ring.add_node(NodeId::new("z"));
    assert_eq!(ring.node_count(), 3);

    // Remove one node.
    ring.remove_node(NodeId::new("y")).expect("remove_node should succeed");
    assert_eq!(ring.node_count(), 2);

    // All keys must still resolve.
    for i in 0..30 {
        let hash = hash_key(format!("rm-key-{}", i).as_bytes());
        let successors = ring.lookup(&hash);
        assert!(!successors.is_empty(), "key did not resolve after remove_node");
    }
}

#[test]
fn ring_cache_snapshot_reflects_recent_update() {
    let config = RingConfig { vnodes_per_node: 8, replication_factor: 3 };
    let mut ring = Ring::new(config.clone());
    ring.add_node(NodeId::new("n1"));

    let cache = RingCache::new(ring);
    assert_eq!(cache.snapshot().node_count(), 1);

    // Update with new ring.
    let mut ring2 = Ring::new(config);
    ring2.add_node(NodeId::new("n2"));
    ring2.add_node(NodeId::new("n3"));
    cache.update(ring2);

    assert_eq!(cache.snapshot().node_count(), 2);
}

#[test]
fn serialization_round_trip_preserves_ring_functionality() {
    let config = RingConfig { vnodes_per_node: 16, replication_factor: 3 };
    let mut ring = Ring::new(config);
    ring.add_node(NodeId::new("alpha"));
    ring.add_node(NodeId::new("beta"));
    ring.add_node(NodeId::new("gamma"));

    // Serialize.
    let json = serde_json::to_string(&ring).expect("serialization should succeed");

    // Deserialize.
    let restored: Ring = serde_json::from_str(&json).expect("deserialization should succeed");

    // Verify lookups match.
    for i in 0..20 {
        let hash = hash_key(format!("serde-{}", i).as_bytes());
        assert_eq!(ring.lookup(&hash), restored.lookup(&hash));
    }

    assert_eq!(ring.node_count(), restored.node_count());
}
