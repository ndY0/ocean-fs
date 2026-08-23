//! Shard batching utilities for efficient multi-node fetch.
//!
//! Groups shard fetch requests by their target node so that multiple
//! shards destined for the same peer can be sent in a single batched
//! gRPC call rather than one RPC per shard.

use oceanfs_core::NodeId;

/// A shard fetch request targeting a specific data shard.
///
/// Identifies a shard by its owning segment and position within
/// the erasure-coded stripe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRequest {
    /// The segment containing this shard.
    pub segment_id: oceanfs_core::SegmentId,
    /// Index of this shard within the erasure-coded stripe (0..=k+m-1).
    pub shard_index: u32,
    /// Offset within the shard data to read.
    pub offset: u64,
    /// Length of data to read from this shard.
    pub length: u64,
}

/// Group shard fetch requests by target node.
///
/// Callers provide a resolution function that maps each shard to its
/// owning node (e.g., via the DHT ring and membership state). Shards
/// whose owner cannot be resolved are silently excluded.
///
/// The returned groups preserve FIRST-SEEN (insertion) order — the
/// order of `shards` — so callers that iterate the groups (e.g. the
/// read path's replica fallback loop) honor the ring's replica order
/// deterministically. A `HashMap`-backed variant would randomize that
/// order per process (a random hash seed), breaking the intended
/// first-replica preference (f7's error-driven failover counts on it).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{NodeId, SegmentId};
/// use oceanfs_routing::shard_batch::{group_by_node, ShardRequest};
///
/// let shards = vec![
///     ShardRequest {
///         segment_id: SegmentId::new(),
///         shard_index: 0,
///         offset: 0,
///         length: 64,
///     },
/// ];
/// let node_a = NodeId::new("a");
/// let groups = group_by_node(&shards, |_req| Some(node_a.clone()));
/// assert_eq!(groups.len(), 1);
/// assert_eq!(groups[0].1.len(), 1);
/// ```
pub fn group_by_node<F>(
    shards: &[ShardRequest],
    resolve_owner: F,
) -> Vec<(NodeId, Vec<ShardRequest>)>
where
    F: Fn(&ShardRequest) -> Option<NodeId>,
{
    // Linear first-seen grouping: pool counts are 5–20 and a fetch
    // batches at most a handful of shards, so the O(n·m) scan is
    // negligible and preserves order (perf 1.3 pre-size the result).
    let mut groups: Vec<(NodeId, Vec<ShardRequest>)> = Vec::new();
    for shard in shards {
        let Some(node_id) = resolve_owner(shard) else { continue };
        match groups.iter_mut().find(|(id, _)| *id == node_id) {
            Some((_, shards_for_node)) => shards_for_node.push(shard.clone()),
            None => groups.push((node_id, vec![shard.clone()])),
        }
    }
    groups
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::SegmentId;

    use super::*;

    fn make_shard(shard_index: u32) -> ShardRequest {
        ShardRequest { segment_id: SegmentId::new(), shard_index, offset: 0, length: 64 }
    }

    #[test]
    fn clusters_by_owner() {
        let shards = vec![
            make_shard(0),
            make_shard(1),
            make_shard(2),
            make_shard(3),
            make_shard(4),
            make_shard(5),
        ];
        // Map: even shards → node-a, odd shards → node-b. First-seen
        // order is preserved (node-a first — shard 0 is even).
        let groups = group_by_node(&shards, |req| {
            if req.shard_index % 2 == 0 {
                Some(NodeId::new("node-a"))
            } else {
                Some(NodeId::new("node-b"))
            }
        });
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, NodeId::new("node-a"));
        assert_eq!(groups[0].1.len(), 3); // 0, 2, 4
        assert_eq!(groups[1].0, NodeId::new("node-b"));
        assert_eq!(groups[1].1.len(), 3); // 1, 3, 5
    }

    #[test]
    fn handles_empty_list() {
        let groups = group_by_node(&[], |_req| Some(NodeId::new("x")));
        assert!(groups.is_empty());
    }

    #[test]
    fn handles_unowned_shard() {
        let shards = vec![make_shard(0), make_shard(1)];
        let groups = group_by_node(&shards, |req| {
            if req.shard_index == 0 {
                Some(NodeId::new("a"))
            } else {
                None // shard 1 has no owner
            }
        });
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, NodeId::new("a"));
        assert_eq!(groups[0].1.len(), 1);
    }
}
