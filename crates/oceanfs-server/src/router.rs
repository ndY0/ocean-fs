//! Request router — integrates ring, membership, and connection pool
//! to dispatch blob operations to the correct replica set.

use std::sync::Arc;

use oceanfs_core::{NodeId, ObjectKey};
use oceanfs_routing::{hash_key, RingCache};

/// A pre-computed key hash that flows through all routing layers.
///
/// Computed once at the HTTP entry point and passed through routing,
/// metadata lookup, and segment operations — never re-hashed.
#[derive(Debug, Clone)]
pub struct HashKey([u8; 32]);

impl HashKey {
    /// Creates a `HashKey` from an object key.
    pub fn from_key(key: &ObjectKey) -> Self {
        Self(hash_key(key.as_str().as_bytes()))
    }

    /// Returns the raw hash bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// The result of a routing decision.
#[derive(Debug, Clone)]
pub struct RouteResponse {
    /// Whether this node is part of the replica set.
    pub is_local: bool,
    /// The ordered list of successor nodes for this key.
    pub replica_set: Vec<NodeId>,
    /// The node to forward the request to (if not local).
    pub forward_target: Option<NodeId>,
}

/// Routes requests to the correct replica set.
///
/// Integrates the ring cache for consistent-hashing lookups.
pub struct Router {
    ring: Arc<RingCache>,
}

impl Router {
    /// Creates a new router.
    pub fn new(ring: Arc<RingCache>) -> Self {
        Self { ring }
    }

    /// Routes a request by key hash.
    ///
    /// Determines the replica set and whether this node is local.
    pub fn route(&self, key: &HashKey) -> RouteResponse {
        let replica_set = self.ring.lookup(key.as_bytes());

        let is_local = false; // In a real implementation, compare with local node ID.

        RouteResponse {
            is_local,
            forward_target: if is_local { None } else { replica_set.first().cloned() },
            replica_set,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{NodeId, RingConfig};
    use oceanfs_routing::Ring;

    use super::*;

    #[test]
    fn hash_key_deterministic() {
        let k1 = HashKey::from_key(&ObjectKey::new("a"));
        let k2 = HashKey::from_key(&ObjectKey::new("a"));
        assert_eq!(k1.as_bytes(), k2.as_bytes());
    }

    #[test]
    fn route_returns_replica_set() {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 16, replication_factor: 3 });
        ring.add_node(NodeId::new("a"));
        ring.add_node(NodeId::new("b"));
        ring.add_node(NodeId::new("c"));

        let cache = Arc::new(RingCache::new(ring));
        let router = Router::new(cache);
        let key = HashKey::from_key(&ObjectKey::new("test"));

        let response = router.route(&key);
        assert!(!response.replica_set.is_empty());
    }
}
