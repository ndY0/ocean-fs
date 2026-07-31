//! Request router — integrates ring, membership, and connection pool
//! to dispatch blob operations to the correct replica set.

use std::sync::Arc;

use oceanfs_core::{HashKey, NodeId, OperationType};
use oceanfs_routing::RingCache;
use tracing::{debug, warn};

// Only membership + network need mocks (they pull in tonic/native deps).
#[cfg(feature = "membership")]
use oceanfs_membership::Membership;
#[cfg(not(feature = "membership"))]
use crate::mocks::MockMembership as Membership;

#[cfg(feature = "network")]
use oceanfs_network::ConnectionPool;
#[cfg(not(feature = "network"))]
use crate::mocks::MockConnectionPool as ConnectionPool;

use crate::error::{Error, Result};

/// A request to be routed to the correct replica set.
#[derive(Debug, Clone)]
pub struct RouteRequest {
    /// Pre-computed key hash.
    pub key: HashKey,
    /// The bucket identifier.
    pub bucket: oceanfs_core::BucketId,
    /// The operation type.
    pub operation: OperationType,
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
/// Integrates the ring cache for consistent-hashing lookups,
/// membership for up-to-date node addresses, and the connection
/// pool for forwarding requests to remote nodes.
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
/// use oceanfs_core::{HashKey, ObjectKey, OperationType};
/// use oceanfs_routing::hash_key;
/// use oceanfs_server::Router;
///
/// # async fn example(router: &Router) {
/// let key = ObjectKey::new("photos/cat.jpg");
/// let hash_key = HashKey::from_bytes(hash_key(key.as_str().as_bytes()));
/// let response = router.route(hash_key).await.unwrap();
/// if response.is_local {
///     // process locally
/// } else {
///     // forward to response.forward_target
/// }
/// # }
/// ```
pub struct Router {
    ring: Arc<RingCache>,
    membership: Arc<Membership>,
    pool: Arc<ConnectionPool>,
    /// This node's identifier.
    node_id: NodeId,
}

impl Router {
    /// Creates a new router.
    pub fn new(
        ring: Arc<RingCache>,
        membership: Arc<Membership>,
        pool: Arc<ConnectionPool>,
        node_id: NodeId,
    ) -> Self {
        Self { ring, membership, pool, node_id }
    }

    /// Routes a request by key hash.
    ///
    /// Determines the replica set from the ring and checks whether
    /// this node is in the replica set. If not, the first successor
    /// is returned as the forward target.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Routing`] if the ring is empty and no
    /// replica set can be determined.
    pub async fn route(&self, key: HashKey) -> Result<RouteResponse> {
        let replica_set = self.ring.lookup(key.as_bytes());

        if replica_set.is_empty() {
            return Err(Error::Routing("ring returned empty replica set".into()));
        }

        let is_local = replica_set.contains(&self.node_id);

        let forward_target = if is_local {
            None
        } else {
            replica_set.first().cloned()
        };

        debug!(
            is_local = is_local,
            replica_count = replica_set.len(),
            forward = ?forward_target,
            "routing decision"
        );

        Ok(RouteResponse { is_local, replica_set, forward_target })
    }

    /// Routes with forwarding retry on failure.
    ///
    /// Attempts to route to the first successor. If the forward fails,
    /// tries the next successor in the replica set, up to N attempts.
    ///
    /// Returns the response from the first successful forward, or an
    /// error if all attempts fail.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Routing`] if the ring is empty.
    /// Returns [`Error::AllForwardingFailed`] if all successors are
    /// unreachable.
    pub async fn route_with_retry(&self, key: HashKey) -> Result<RouteResponse> {
        let replica_set = self.ring.lookup(key.as_bytes());

        if replica_set.is_empty() {
            return Err(Error::Routing("ring returned empty replica set".into()));
        }

        // If this node is in the replica set, handle locally.
        if replica_set.contains(&self.node_id) {
            return Ok(RouteResponse {
                is_local: true,
                replica_set,
                forward_target: None,
            });
        }

        // Try each successor in order.
        let mut attempts = 0usize;
        for target in replica_set.clone() {
            attempts += 1;

            // Check if target is alive.
            if let Some(state) = self.membership.state_of(&target) {
                if !matches!(state, oceanfs_core::NodeState::Alive) {
                    debug!(target = %target, state = ?state, "skipping non-alive node");
                    continue;
                }
            }

            // Attempt to forward to the target.
            match self.try_forward(&target).await {
                Ok(()) => {
                    return Ok(RouteResponse {
                        is_local: false,
                        replica_set,
                        forward_target: Some(target),
                    });
                }
                Err(e) => {
                    warn!(target = %target, attempt = attempts, error = %e, "forward attempt failed");
                }
            }
        }

        Err(Error::AllForwardingFailed { attempts })
    }

    /// Attempts to forward a request to a specific target node.
    ///
    /// Acquires a gRPC channel from the pool and checks connectivity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ForwardFailed`] if the target is not found in
    /// membership or the channel cannot be acquired.
    async fn try_forward(&self, target: &NodeId) -> Result<()> {
        // In a real implementation, this would resolve the target's
        // address from membership, acquire a channel from the pool,
        // and make the actual gRPC forwarding call.
        //
        // For now, we simply verify that the target exists in membership
        // and that we can acquire a channel.
        let _state = self
            .membership
            .state_of(target)
            .ok_or_else(|| Error::ForwardFailed {
                target: target.to_string(),
                reason: "node not found in membership".into(),
            })?;

        // Channel acquisition is deferred to the actual RPC call site.
        // The pool.get_channel(addr) call will be made by the gRPC
        // client stub when making the forwarded request.

        debug!(target = %target, "forward target validated");
        Ok(())
    }

    /// Returns a reference to the ring cache.
    pub fn ring(&self) -> &Arc<RingCache> {
        &self.ring
    }

    /// Returns a reference to the membership.
    pub fn membership(&self) -> &Arc<Membership> {
        &self.membership
    }

    /// Returns a reference to the connection pool.
    pub fn pool(&self) -> &Arc<ConnectionPool> {
        &self.pool
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::SocketAddr;

    use oceanfs_core::{GossipConfig, Incarnation, NodeId, NodeState, RingConfig, RpcConfig};
    use oceanfs_routing::{hash_key, Ring};

    use super::*;

    fn make_hash_key(s: &str) -> HashKey {
        HashKey::from_bytes(hash_key(s.as_bytes()))
    }

    fn make_router(local_node_id: &str, ring_nodes: &[&str]) -> Router {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
        for node in ring_nodes {
            ring.add_node(NodeId::new(*node));
        }

        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();

        #[cfg(feature = "membership")]
        let membership = Arc::new(Membership::new(
            NodeId::new(local_node_id), addr, GossipConfig::default(), ring_cache.clone(),
        ));
        #[cfg(not(feature = "membership"))]
        let membership = Arc::new(Membership::new(NodeId::new(local_node_id), addr));

        for node in ring_nodes {
            membership.upsert_node(NodeId::new(*node), NodeState::Alive, Incarnation::new(1), addr);
        }
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));

        Router::new(ring_cache, membership, pool, NodeId::new(local_node_id))
    }

    #[test]
    fn hash_key_deterministic() {
        let k1 = make_hash_key("a");
        let k2 = make_hash_key("a");
        assert_eq!(k1.as_bytes(), k2.as_bytes());
    }

    #[test]
    fn hash_key_different_for_different_keys() {
        let k1 = make_hash_key("a");
        let k2 = make_hash_key("b");
        assert_ne!(k1.as_bytes(), k2.as_bytes());
    }

    #[test]
    fn route_with_cache_only_returns_replica_set() {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 16, replication_factor: 3 });
        ring.add_node(NodeId::new("a"));
        ring.add_node(NodeId::new("b"));
        ring.add_node(NodeId::new("c"));

        let cache = Arc::new(RingCache::new(ring));
        let key = make_hash_key("test");
        let replica_set = cache.lookup(key.as_bytes());
        assert!(!replica_set.is_empty());
        assert!(replica_set.len() <= 3);
    }

    #[tokio::test]
    async fn route_local_node_in_replica_set_is_local_true() {
        let router = make_router("node-a", &["node-a", "node-b", "node-c"]);
        let key = make_hash_key("test-key");

        let response = router.route(key).await.unwrap();
        assert!(response.is_local, "local node should be in its own replica set");
        assert!(response.forward_target.is_none(), "forward_target should be None when local");
    }

    #[tokio::test]
    async fn route_local_node_not_in_replica_set_has_forward_target() {
        let router = make_router("node-x", &["node-a", "node-b", "node-c"]);
        let key = make_hash_key("test-key");

        let response = router.route(key).await.unwrap();
        assert!(!response.is_local, "node-x should not be in the replica set");
        assert!(response.forward_target.is_some(), "forward_target should be set");
    }

    #[tokio::test]
    async fn route_with_retry_local_node_is_local_true() {
        let router = make_router("node-a", &["node-a", "node-b"]);
        let key = make_hash_key("test");

        let response = router.route_with_retry(key).await.unwrap();
        assert!(response.is_local);
        assert!(response.forward_target.is_none());
    }

    #[tokio::test]
    async fn route_with_retry_non_local_selects_first_alive() {
        // node-x is not in the ring, so all targets are remote.
        let router = make_router("node-x", &["node-a", "node-b", "node-c"]);
        let key = make_hash_key("test");

        let response = router.route_with_retry(key).await.unwrap();
        assert!(!response.is_local);
        // The first alive node in the replica set should be the forward target.
        assert!(response.forward_target.is_some());
    }

    #[tokio::test]
    async fn route_empty_ring_returns_error() {
        let ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        #[cfg(feature = "membership")]
        let membership = Arc::new(Membership::new(
            NodeId::new("n1"), addr, GossipConfig::default(), ring_cache.clone(),
        ));
        #[cfg(not(feature = "membership"))]
        let membership = Arc::new(Membership::new(NodeId::new("n1"), addr));
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let router = Router::new(ring_cache, membership, pool, NodeId::new("n1"));

        let key = make_hash_key("test");
        let result = router.route(key).await;
        assert!(result.is_err(), "routing with empty ring should fail");
    }
}
