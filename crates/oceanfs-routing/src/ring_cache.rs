//! Wait-free ring cache using ArcSwap.
//!
//! The ring topology is updated infrequently (only on gossip membership
//! changes). Readers access the ring atomically via `ArcSwap` without
//! blocking writers.

use std::sync::Arc;

use arc_swap::ArcSwap;
use oceanfs_core::{NodeId, SegmentId};

use crate::ring::Ring;

/// A wait-free cache for the consistent hashing ring.
///
/// Readers call [`Self::lookup`] without any lock acquisition. Writers call
/// [`Self::update`] to swap in a new ring topology atomically.
///
/// # Examples
///
/// ```
/// use oceanfs_core::RingConfig;
/// use oceanfs_routing::{Ring, RingCache};
///
/// let config = RingConfig::default();
/// let ring = Ring::new(config);
/// let cache = RingCache::new(ring);
/// let successors = cache.lookup(&[0u8; 32]);
/// ```
pub struct RingCache {
    inner: ArcSwap<Ring>,
}

impl RingCache {
    /// Creates a new ring cache from an initial ring.
    pub fn new(ring: Ring) -> Self {
        Self { inner: ArcSwap::new(Arc::new(ring)) }
    }

    /// Looks up the N successors for a key hash.
    ///
    /// Wait-free — readers never block.
    pub fn lookup(&self, key_hash: &[u8; 32]) -> Vec<NodeId> {
        self.inner.load().lookup(key_hash)
    }

    /// Atomically replaces the ring with a new topology.
    ///
    /// All subsequent [`Self::lookup`] calls will see the new ring.
    pub fn update(&self, ring: Ring) {
        self.inner.store(Arc::new(ring));
    }

    /// Returns a snapshot of the current ring (for serialization).
    pub fn snapshot(&self) -> Arc<Ring> {
        self.inner.load_full()
    }
}

/// Derives the replica set for a segment's data: the ring's successors of
/// `blake3(segment_id)`.
///
/// This is the ONE derivation the data plane uses for "which nodes hold a
/// segment's data": the read path's gRPC fallback (fetch.rs), the seal-time
/// segment replicator, g3's announcement fan-out, and g4's live-copy count
/// all consult `segment_replica_set`. It MUST stay identical across call
/// sites — a divergence means the replicator pushes to a set the read path
/// never fetches from (the phase-2 replication defect this helper exists
/// to prevent).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{NodeId, RingConfig, SegmentId};
/// use oceanfs_routing::{Ring, RingCache};
///
/// let mut ring = Ring::new(RingConfig::default());
/// ring.add_node(NodeId::new("a"));
/// ring.add_node(NodeId::new("b"));
/// ring.add_node(NodeId::new("c"));
/// let cache = RingCache::new(ring);
/// let replicas = oceanfs_routing::segment_replica_set(&cache, &SegmentId::new());
/// assert_eq!(replicas.len(), 3);
/// ```
pub fn segment_replica_set(ring: &RingCache, segment_id: &SegmentId) -> Vec<NodeId> {
    let segment_hash = blake3::hash(segment_id.to_string().as_bytes());
    ring.lookup(segment_hash.as_bytes())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::RingConfig;

    use super::*;

    fn make_ring() -> Ring {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 16, replication_factor: 3 });
        ring.add_node(NodeId::new("a"));
        ring.add_node(NodeId::new("b"));
        ring
    }

    #[test]
    fn cache_lookup_matches_ring() {
        let ring = make_ring();
        let expected = ring.lookup(&[1u8; 32]);
        let cache = RingCache::new(ring);
        assert_eq!(cache.lookup(&[1u8; 32]), expected);
    }

    #[test]
    fn update_changes_lookup_result() {
        let ring1 = make_ring();
        let cache = RingCache::new(ring1);
        let before = cache.lookup(&[1u8; 32]);

        let mut ring2 = Ring::new(RingConfig { vnodes_per_node: 16, replication_factor: 3 });
        ring2.add_node(NodeId::new("z"));
        cache.update(ring2);

        let after = cache.lookup(&[1u8; 32]);
        assert_ne!(before, after);
    }

    #[test]
    fn snapshot_returns_readable_ring() {
        let ring = make_ring();
        let cache = RingCache::new(ring);
        let snap = cache.snapshot();
        assert_eq!(snap.node_count(), 2);
    }

    /// The segment-replica derivation is exactly `ring.lookup(hash(id))`:
    /// the seal-time replicator pushes to the same set the read path
    /// fetches from. A divergence is the phase-2 replication defect.
    #[test]
    fn segment_replica_set_matches_ring_lookup_of_segment_hash() {
        let ring = make_ring();
        let cache = RingCache::new(ring);
        let id = SegmentId::new();
        let hash = blake3::hash(id.to_string().as_bytes());
        assert_eq!(segment_replica_set(&cache, &id), cache.lookup(hash.as_bytes()));
        // The set is the ring's successors (both nodes in this 2-node
        // ring), never empty.
        assert_eq!(segment_replica_set(&cache, &id).len(), 2);
        // Distinct segment ids may map to distinct sets — but each is the
        // ring's derivation, not an arbitrary list.
        let id2 = SegmentId::new();
        let hash2 = blake3::hash(id2.to_string().as_bytes());
        assert_eq!(segment_replica_set(&cache, &id2), cache.lookup(hash2.as_bytes()));
    }
}
