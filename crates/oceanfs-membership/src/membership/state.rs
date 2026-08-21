//! Membership state and gossip exchange types.
//!
//! Defines the core state representation (`MembershipState`) used by
//! the membership coordinator, plus the serializable types
//! (`NodeEntry`, `GossipState`, `GossipDelta`) exchanged between
//! peers during gossip push/pull.

use std::{collections::HashMap, net::SocketAddr};

use oceanfs_core::{Incarnation, NodeId, NodeState};
use serde::{Deserialize, Serialize};

/// A single node's membership entry for gossip exchange (ADR-0028 D3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeEntry {
    /// The node's unique identifier.
    pub node_id: NodeId,
    /// Current incarnation number (node-authoritative, ADR-0022).
    pub incarnation: Incarnation,
    /// Current state (Alive, Suspect, Dead, Leaving, Left).
    pub state: NodeState,
    /// The node's membership plane address (gossip + probes, ADR-0028
    /// D1) — the address peers dial for the protocol.
    pub address: SocketAddr,
    /// The node's data-plane gRPC address (replication, hints, healing).
    pub grpc_address: SocketAddr,
    /// Per-(node, origin) logical clock: every state change by the
    /// observer bumps the version it announces for that node. Orders
    /// same-origin entries at the same incarnation.
    pub version: u64,
    /// The node that last observed/changed this entry (self for
    /// announcements, the detector node for Suspect/Dead, the leaver
    /// for Leaving/Left).
    pub origin: NodeId,
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

/// A stored membership entry in the coordinator state (ADR-0028 D3).
#[derive(Debug, Clone)]
pub(crate) struct StoredEntry {
    /// Current state.
    pub state: NodeState,
    /// Current incarnation.
    pub incarnation: Incarnation,
    /// The node's data-plane gRPC address (replication, hints, healing).
    pub address: SocketAddr,
    /// The node's membership plane address (gossip + probes, ADR-0028
    /// D1) — the address peers dial for the protocol.
    pub membership_address: SocketAddr,
    /// The observer's version for this node (per-(node, origin) clock).
    pub version: u64,
    /// The observer that last changed this entry.
    pub origin: NodeId,
}

/// Aggregate membership state for the [`Membership`] coordinator.
///
/// Tracks per-node state, incarnation, address, and attribution
/// (version + origin). This is the internal state used by the
/// coordinator — distinct from the gossip-exchange types above.
#[derive(Debug, Clone)]
pub(crate) struct MembershipState {
    /// Per-node entries.
    pub(crate) nodes: HashMap<NodeId, StoredEntry>,
    /// Last-known incarnation per node, **retained after Dead/Left
    /// removal** (F1d). A node absent from `nodes` but present here
    /// may only be re-admitted at a strictly higher incarnation —
    /// this closes the Dead↔Alive oscillation loop (t24).
    pub(crate) incarnations: HashMap<NodeId, Incarnation>,
}

impl MembershipState {
    /// Creates an empty membership state.
    pub(crate) fn new() -> Self {
        Self { nodes: HashMap::new(), incarnations: HashMap::new() }
    }
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
