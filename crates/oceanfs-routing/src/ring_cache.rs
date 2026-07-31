//! Wait-free ring cache using ArcSwap.
//!
//! The ring topology is updated infrequently (only on gossip membership
//! changes). Readers access the ring atomically via `ArcSwap` without
//! blocking writers.

use std::sync::Arc;

use arc_swap::ArcSwap;
use oceanfs_core::NodeId;

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
}
