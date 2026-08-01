//! Gossip state and message types for SWIM membership protocol.
//!
//! Defines the serializable representations of membership state used
//! in gossip push/pull exchanges between peers.

use std::collections::HashMap;
use std::net::SocketAddr;

use oceanfs_core::{Incarnation, NodeId, NodeState};
use serde::{Deserialize, Serialize};

/// A single node's membership entry for gossip exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeEntry {
    /// The node's unique identifier.
    pub node_id: NodeId,
    /// Current incarnation number.
    pub incarnation: Incarnation,
    /// Current state (Alive, Suspect, Dead, Leaving, Left).
    pub state: NodeState,
    /// The node's gRPC address.
    pub address: SocketAddr,
}

/// Full membership state for gossip exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GossipState {
    /// All known nodes and their state.
    pub nodes: HashMap<NodeId, NodeEntry>,
    /// Serialized ring topology (JSON).
    pub ring_json: Option<String>,
}

impl GossipState {
    /// Creates an empty gossip state.
    pub fn new() -> Self {
        Self { nodes: HashMap::new(), ring_json: None }
    }
}

impl Default for GossipState {
    fn default() -> Self {
        Self::new()
    }
}

/// A delta of membership changes since the last gossip exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GossipDelta {
    /// Nodes that changed state since the last exchange.
    pub changed: Vec<NodeEntry>,
}

/// Direction of a gossip exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GossipDirection {
    /// Sender is pushing state to receiver.
    Push,
    /// Sender is requesting state from receiver (pull).
    Pull,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gossip_state_new_is_empty() {
        let state = GossipState::new();
        assert!(state.nodes.is_empty());
        assert!(state.ring_json.is_none());
    }

    #[test]
    fn gossip_state_default_equals_new() {
        let a = GossipState::new();
        let b = GossipState::default();
        assert_eq!(a.nodes.len(), b.nodes.len());
        assert_eq!(a.ring_json, b.ring_json);
    }
}
