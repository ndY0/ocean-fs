//! Consistent hashing ring with virtual nodes.
//!
//! Maps keys to node replica sets using a 256-bit ring with virtual nodes.
//! Lookup is O(log N) via binary search on a sorted BTreeMap.

use std::collections::BTreeMap;

use oceanfs_core::{NodeId, RingConfig, VnodeRange};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    error::{Error, Result},
    hash::hash_node,
};

/// Key position on the ring (256-bit hash).
type RingPosition = [u8; 32];

/// A consistent hashing ring with virtual nodes.
///
/// Each physical node owns `vnodes_per_node` virtual positions.
/// Key lookups find the N successors starting from the key's hash position.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{NodeId, RingConfig};
/// use oceanfs_routing::Ring;
///
/// let config = RingConfig { vnodes_per_node: 64, replication_factor: 3 };
/// let mut ring = Ring::new(config);
/// ring.add_node(NodeId::new("node-1"));
/// ring.add_node(NodeId::new("node-2"));
/// ring.add_node(NodeId::new("node-3"));
///
/// let successors = ring.lookup(&[0u8; 32]);
/// assert_eq!(successors.len(), 3);
/// ```
#[derive(Debug, Clone)]
pub struct Ring {
    /// Sorted map from ring position → node ID.
    positions: BTreeMap<RingPosition, NodeId>,
    /// Ring configuration.
    config: RingConfig,
    /// Set of known node IDs.
    node_ids: Vec<NodeId>,
}

impl Serialize for Ring {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("Ring", 3)?;
        // Serialize positions as Vec of (pos, node_id) tuples.
        let entries: Vec<(&RingPosition, &NodeId)> = self.positions.iter().collect();
        s.serialize_field("positions", &entries)?;
        s.serialize_field("config", &self.config)?;
        s.serialize_field("node_ids", &self.node_ids)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for Ring {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct RingData {
            positions: Vec<(RingPosition, NodeId)>,
            config: RingConfig,
            node_ids: Vec<NodeId>,
        }

        let data = RingData::deserialize(deserializer)?;
        Ok(Ring {
            positions: data.positions.into_iter().collect(),
            config: data.config,
            node_ids: data.node_ids,
        })
    }
}

impl Ring {
    /// Creates an empty ring with the given configuration.
    pub fn new(config: RingConfig) -> Self {
        Self { positions: BTreeMap::new(), config, node_ids: Vec::new() }
    }

    /// Adds a node to the ring, creating `vnodes_per_node` virtual positions.
    ///
    /// Returns the key ranges affected by this addition (for data migration).
    pub fn add_node(&mut self, node: NodeId) -> Vec<VnodeRange> {
        let mut ranges = Vec::new();

        for i in 0..self.config.vnodes_per_node {
            let pos = hash_node(node.as_str(), i);
            self.positions.insert(pos, node.clone());
            ranges.push(VnodeRange { start: pos, end: pos });
        }

        if !self.node_ids.contains(&node) {
            self.node_ids.push(node);
        }

        ranges
    }

    /// Removes a node and all its virtual positions from the ring.
    ///
    /// Returns the key ranges affected by this removal.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NodeNotFound`] if the node is not in the ring.
    pub fn remove_node(&mut self, node: NodeId) -> Result<Vec<VnodeRange>> {
        let mut ranges = Vec::new();
        let mut positions_to_remove = Vec::new();

        for (&pos, n) in &self.positions {
            if n == &node {
                positions_to_remove.push(pos);
                ranges.push(VnodeRange { start: pos, end: pos });
            }
        }

        if positions_to_remove.is_empty() {
            return Err(Error::NodeNotFound(node.to_string()));
        }

        for pos in positions_to_remove {
            self.positions.remove(&pos);
        }

        self.node_ids.retain(|n| n != &node);

        Ok(ranges)
    }

    /// Looks up the N successors for a key hash.
    ///
    /// Returns `replication_factor` distinct node IDs. If the ring has
    /// fewer nodes than the replication factor, returns all nodes.
    pub fn lookup(&self, key_hash: &[u8; 32]) -> Vec<NodeId> {
        if self.positions.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.config.replication_factor as usize);
        let mut seen = std::collections::HashSet::new();

        // Convert positions to a sorted vec for range iteration.
        let entries: Vec<(&RingPosition, &NodeId)> = self.positions.iter().collect();

        // Find start index: first entry with position >= key_hash.
        let start_idx = entries.partition_point(|(pos, _)| pos < &key_hash);

        // Iterate from start_idx, wrapping around.
        for i in 0..entries.len() {
            let idx = (start_idx + i) % entries.len();
            let node = entries[idx].1;
            if seen.insert((*node).clone()) {
                result.push((*node).clone());
                if result.len() >= self.config.replication_factor as usize {
                    break;
                }
            }
        }

        result
    }

    /// Returns the number of physical nodes in the ring.
    pub fn node_count(&self) -> usize {
        self.node_ids.len()
    }

    /// Returns a reference to the ring configuration.
    pub fn config(&self) -> &RingConfig {
        &self.config
    }

    /// Returns all node IDs in the ring.
    pub fn nodes(&self) -> &[NodeId] {
        &self.node_ids
    }

    /// Returns the first successor of the given node in the ring.
    ///
    /// Hashes the node ID to find its ring position, then returns
    /// the next distinct node clockwise. Returns `None` if no other
    /// nodes are in the ring.
    pub fn successor_of(&self, node_id: &NodeId) -> Option<NodeId> {
        let key_hash = hash_node(node_id.as_str(), 0);
        let successors = self.lookup(&key_hash);
        successors.into_iter().find(|n| n != node_id)
    }

    /// Returns the vnode arcs this node is a replica holder for
    /// (g8 `metadata-loss-recovery`).
    ///
    /// A key position `h` is served first by the first vnode position
    /// `>= h`; its replica set is that vnode's RF successors. Between two
    /// consecutive vnode positions the replica set is constant, so the
    /// ring partitions into arcs — this node holds the rows of exactly
    /// the arcs whose replica set contains it. The returned ranges
    /// partition that holding set (boundary positions are measure-zero
    /// and deliberately excluded from the fold semantics).
    ///
    /// The full-circle case (this node is in every arc's replica set —
    /// e.g. RF == node count) is returned as a single sentinel range
    /// `[0..0)` (see [`range_contains`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::{NodeId, RingConfig};
    /// use oceanfs_routing::{range_contains, Ring};
    ///
    /// let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
    /// ring.add_node(NodeId::new("a"));
    /// ring.add_node(NodeId::new("b"));
    /// ring.add_node(NodeId::new("c"));
    /// let a = NodeId::new("a");
    /// // With RF == node count every key replicates to all three nodes,
    /// // so `a` holds the full circle.
    /// let ranges = ring.ranges_owned_by(&a);
    /// assert!(ranges.iter().all(|r| range_contains(r, &[0u8; 32])));
    /// ```
    pub fn ranges_owned_by(&self, node: &NodeId) -> Vec<VnodeRange> {
        if self.positions.is_empty() {
            return Vec::new();
        }
        let entries: Vec<([u8; 32], NodeId)> =
            self.positions.iter().map(|(p, n)| (*p, n.clone())).collect();
        let m = entries.len();
        // Arc i covers (entries[i].0, entries[(i+1) % m].0] — the keys
        // whose replica set starts at the arc's end position.
        let selected: Vec<bool> = (0..m)
            .map(|i| {
                let end = entries[(i + 1) % m].0;
                self.lookup(&end).iter().any(|n| n == node)
            })
            .collect();
        if !selected.iter().any(|s| *s) {
            return Vec::new();
        }
        // Full circle: every arc's replica set contains the node.
        if selected.iter().all(|s| *s) {
            return vec![VnodeRange { start: [0u8; 32], end: [0u8; 32] }];
        }
        // Merge maximal runs of selected arcs on the circle. Each run
        // from arc `a` through arc `b` (exclusive end `b`) covers keys
        // [entries[a].0, entries[b].0); a run that wraps past position 0
        // is represented with start > end (range_contains handles it).
        let mut ranges = Vec::new();
        let mut i = 0;
        while i < m {
            if !selected[i] {
                i += 1;
                continue;
            }
            let mut end = (i + 1) % m;
            while end != i && selected[end] {
                end = (end + 1) % m;
            }
            // Run covers arcs i..end-1; exclusive boundary at entries[end].
            let (a, b) = (entries[i].0, entries[end].0);
            ranges.push(VnodeRange { start: a, end: b });
            if end <= i {
                break; // wrapped run already consumed every selected arc
            }
            i = end;
        }
        ranges
    }
}

/// Whether a ring position `h` falls inside a [`VnodeRange`].
///
/// `start` is inclusive, `end` exclusive for the non-wrap case
/// (`start < end`). A wrap-around range (`start >= end`) covers
/// `h >= start || h < end`, so the full-circle sentinel
/// `{ start: [0; 32], end: [0; 32] }` (start == end == 0) matches every
/// position. Comparisons are the ring's own byte-lexicographic order.
///
/// # Examples
///
/// ```
/// use oceanfs_core::VnodeRange;
/// use oceanfs_routing::range_contains;
///
/// let mut start = [0u8; 32];
/// let mut end = [0u8; 32];
/// end[31] = 42;
/// let half = VnodeRange { start, end };
/// assert!(range_contains(&half, &[0u8; 32]));
/// let mut h = end;
/// h[31] = 43;
/// assert!(!range_contains(&half, &h));
/// ```
pub fn range_contains(range: &VnodeRange, h: &[u8; 32]) -> bool {
    range.contains(h)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn test_config() -> RingConfig {
        RingConfig { vnodes_per_node: 16, replication_factor: 3 }
    }

    #[test]
    fn empty_ring_lookup_returns_empty() {
        let ring = Ring::new(test_config());
        assert!(ring.lookup(&[0u8; 32]).is_empty());
    }

    #[test]
    fn single_node_ring_lookup_returns_that_node() {
        let mut ring = Ring::new(test_config());
        ring.add_node(NodeId::new("n1"));
        let result = ring.lookup(&[0u8; 32]);
        assert!(!result.is_empty());
        // With 1 node, can't return 3 distinct successors.
        assert!(result.iter().all(|n| n.as_str() == "n1"));
    }

    #[test]
    fn lookup_returns_distinct_nodes() {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 32, replication_factor: 3 });
        ring.add_node(NodeId::new("a"));
        ring.add_node(NodeId::new("b"));
        ring.add_node(NodeId::new("c"));

        let successors = ring.lookup(&[0u8; 32]);
        assert_eq!(successors.len(), 3);
        // All should be distinct since we have 3 nodes.
        let set: std::collections::HashSet<_> = successors.iter().collect();
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn lookup_deterministic_for_same_key() {
        let mut ring = Ring::new(test_config());
        ring.add_node(NodeId::new("x"));
        ring.add_node(NodeId::new("y"));

        let h1 = ring.lookup(&[42u8; 32]);
        let h2 = ring.lookup(&[42u8; 32]);
        assert_eq!(h1, h2);
    }

    #[test]
    fn add_node_increases_count() {
        let mut ring = Ring::new(test_config());
        assert_eq!(ring.node_count(), 0);
        ring.add_node(NodeId::new("n1"));
        assert_eq!(ring.node_count(), 1);
    }

    #[test]
    fn remove_node_decreases_count() {
        let mut ring = Ring::new(test_config());
        let node = NodeId::new("n1");
        ring.add_node(node.clone());
        ring.remove_node(node).unwrap();
        assert_eq!(ring.node_count(), 0);
    }

    #[test]
    fn remove_nonexistent_node_errors() {
        let mut ring = Ring::new(test_config());
        assert!(ring.remove_node(NodeId::new("ghost")).is_err());
    }

    #[test]
    fn remove_node_cleans_up_vnodes() {
        let mut ring = Ring::new(test_config());
        let node = NodeId::new("temp");
        ring.add_node(node.clone());
        ring.remove_node(node).unwrap();
        // All positions should be gone.
        assert!(ring.positions.is_empty());
    }

    #[test]
    fn serialization_round_trip() {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
        ring.add_node(NodeId::new("a"));
        ring.add_node(NodeId::new("b"));

        let encoded = serde_json::to_string(&ring).unwrap();
        let decoded: Ring = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded.node_count(), 2);
        // Verify that lookups produce the same result after round-trip.
        let original = ring.lookup(&[42u8; 32]);
        let after = decoded.lookup(&[42u8; 32]);
        assert_eq!(original, after);
    }

    /// g8: `ranges_owned_by` must cover EXACTLY the keys the node is a
    /// replica for — for any (interior) key hash, membership in the
    /// node's ranges is equivalent to the node appearing in
    /// `ring.lookup(hash)`. Also: ranges of a node are pairwise
    /// disjoint, so a fold over them never double-writes a row.
    #[test]
    fn ranges_owned_by_cover_exactly_the_lookup_replica_set() {
        let config = RingConfig { vnodes_per_node: 8, replication_factor: 2 };
        let mut ring = Ring::new(config);
        for name in ["a", "b", "c", "d"] {
            ring.add_node(NodeId::new(name));
        }
        let nodes = ring.nodes().to_vec();

        for node in &nodes {
            let ranges = ring.ranges_owned_by(node);

            // Equivalence with ring.lookup for many interior keys: a key
            // is in the node's ranges iff the node is in its replica set.
            // (Overlapping ranges would not break the idempotent rebuild
            // fold, but this still guards against a range covering keys
            // the node does not hold — or missing keys it does.)
            for i in 0..500 {
                let h = crate::hash_key(format!("obj-{node}-{i}").as_bytes());
                let in_ring = ring.lookup(&h).iter().any(|n| n == node);
                let in_ranges = ranges.iter().any(|r| range_contains(r, &h));
                assert_eq!(
                    in_ring, in_ranges,
                    "node {node}: key {i} lookup={in_ring} ranges={in_ranges} mismatch"
                );
            }
        }
    }

    #[test]
    fn vnode_distribution_is_uniform() {
        // With a sufficient number of vnodes per node, the ring positions
        // should be well-distributed across the 256-bit space.
        let config = RingConfig { vnodes_per_node: 64, replication_factor: 3 };
        let mut ring = Ring::new(config);
        let node_count = 10;
        for i in 0..node_count {
            ring.add_node(NodeId::new(format!("node-{}", i)));
        }

        // Partition the 256-bit space into 8 buckets and count vnodes per bucket.
        let mut buckets = [0usize; 8];
        for &pos in ring.positions.keys() {
            let bucket_idx = (pos[0] as usize) / 32; // top byte / 32 → 0..7
            buckets[bucket_idx] += 1;
        }

        let expected_per_bucket = (node_count * ring.config().vnodes_per_node as usize) / 8;
        // Allow ±20% deviation.
        for &count in &buckets {
            let lower = expected_per_bucket.saturating_sub(expected_per_bucket / 5);
            let upper = expected_per_bucket + expected_per_bucket / 5;
            assert!(
                count >= lower && count <= upper,
                "bucket count {} not in range [{}, {}]",
                count,
                lower,
                upper
            );
        }
    }
}
