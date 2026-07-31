//! Lightweight mock types for testing without heavy native dependencies.
//!
//! When the `testing` feature is enabled (and `membership`/`network` are
//! not linked), these types provide API-compatible replacements so that
//! `oceanfs-server` compiles and tests in seconds without RocksDB or tonic.

use std::collections::HashMap;
use std::net::SocketAddr;

use oceanfs_core::{Incarnation, NodeId, NodeState};
use parking_lot::RwLock;

// ============================================================================
// MockMembership
// ============================================================================

/// A lightweight cluster membership tracker.
#[derive(Debug)]
pub struct MockMembership {
    states: RwLock<HashMap<NodeId, (NodeState, Incarnation, SocketAddr)>>,
    local_id: NodeId,
    _local_addr: SocketAddr,
}

impl MockMembership {
    /// Creates a new membership tracker.
    pub fn new(local_id: NodeId, _local_addr: SocketAddr) -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
            local_id,
            _local_addr,
        }
    }

    /// Inserts or updates a node's state.
    pub fn upsert_node(
        &self,
        node_id: NodeId,
        state: NodeState,
        incarnation: Incarnation,
        addr: SocketAddr,
    ) {
        let mut states = self.states.write();
        states.insert(node_id, (state, incarnation, addr));
    }

    /// Returns the state of a node.
    pub fn state_of(&self, node: &NodeId) -> Option<NodeState> {
        let states = self.states.read();
        states.get(node).map(|(state, _, _)| *state)
    }

    /// Returns the local node ID.
    pub fn local_id(&self) -> &NodeId {
        &self.local_id
    }
}

// ============================================================================
// MockConnectionPool
// ============================================================================

/// A placeholder connection pool.
#[derive(Debug, Default)]
pub struct MockConnectionPool {
    _pool_size: usize,
}

impl MockConnectionPool {
    /// Creates a new mock pool. Accepts any argument (RpcConfig, usize, etc.)
    /// and simply ignores it.
    pub fn new(_config: impl std::any::Any) -> Self {
        Self { _pool_size: 4 }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn mock_membership_state_of_returns_inserted_state() {
        let m = MockMembership::new(
            NodeId::new("local"),
            "127.0.0.1:9001".parse().unwrap(),
        );
        m.upsert_node(
            NodeId::new("remote"),
            NodeState::Alive,
            Incarnation::new(1),
            "127.0.0.1:9002".parse().unwrap(),
        );
        assert_eq!(m.state_of(&NodeId::new("remote")), Some(NodeState::Alive));
    }

    #[test]
    fn mock_connection_pool() {
        let pool = MockConnectionPool::new(4);
        assert_eq!(pool._pool_size, 4);
    }
}
