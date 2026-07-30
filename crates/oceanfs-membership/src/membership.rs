//! Cluster membership state machine.
//!
//! Tracks the state of every known node in the cluster. Node states
//! transition through ALIVE → SUSPECT → DEAD (failure) or
//! ALIVE → LEAVING → LEFT (graceful leave).

use std::collections::HashMap;

use oceanfs_core::{NodeId, NodeState};
use tokio::sync::broadcast;

use crate::error::{Error, Result};

/// An event emitted when a node's state changes.
#[derive(Debug, Clone)]
pub struct MembershipEvent {
    /// The node whose state changed.
    pub node_id: NodeId,
    /// Previous state.
    pub old_state: NodeState,
    /// New state.
    pub new_state: NodeState,
}

/// Cluster membership tracker with state-change broadcasting.
pub struct Membership {
    /// Current state of each known node.
    states: parking_lot::RwLock<HashMap<NodeId, NodeState>>,
    /// Broadcast channel for state-change events.
    tx: broadcast::Sender<MembershipEvent>,
}

impl Membership {
    /// Creates a new empty membership.
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self { states: parking_lot::RwLock::new(HashMap::new()), tx }
    }

    /// Adds or updates a node.
    pub fn add_node(&self, node_id: NodeId, state: NodeState) {
        let mut states = self.states.write();
        let old = states.insert(node_id.clone(), state);
        if old != Some(state) {
            let _ = self.tx.send(MembershipEvent {
                node_id,
                old_state: old.unwrap_or(NodeState::Alive),
                new_state: state,
            });
        }
    }

    /// Transitions a node to a new state.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NodeNotFound`] if the node is not in the membership.
    pub fn transition(&self, node_id: &NodeId, new_state: NodeState) -> Result<()> {
        let mut states = self.states.write();
        let old_state =
            states.get(node_id).copied().ok_or_else(|| Error::NodeNotFound(node_id.to_string()))?;

        if old_state != new_state {
            states.insert(node_id.clone(), new_state);
            let _ =
                self.tx.send(MembershipEvent { node_id: node_id.clone(), old_state, new_state });
        }
        Ok(())
    }

    /// Returns all known nodes and their states.
    pub fn nodes(&self) -> Vec<(NodeId, NodeState)> {
        self.states.read().iter().map(|(id, state)| (id.clone(), *state)).collect()
    }

    /// Returns the state of a specific node.
    pub fn state_of(&self, node_id: &NodeId) -> Option<NodeState> {
        self.states.read().get(node_id).copied()
    }

    /// Subscribes to membership change events.
    pub fn subscribe(&self) -> broadcast::Receiver<MembershipEvent> {
        self.tx.subscribe()
    }

    /// Removes a node from membership.
    pub fn remove(&self, node_id: &NodeId) {
        self.states.write().remove(node_id);
    }
}

impl Default for Membership {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn add_and_retrieve_node() {
        let m = Membership::new();
        m.add_node(NodeId::new("n1"), NodeState::Alive);
        let nodes = m.nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].1, NodeState::Alive);
    }

    #[test]
    fn transition_emits_event() {
        let m = Membership::new();
        m.add_node(NodeId::new("n1"), NodeState::Alive);
        let mut rx = m.subscribe();

        m.transition(&NodeId::new("n1"), NodeState::Suspect).unwrap();
        let event = rx.try_recv().unwrap();
        assert_eq!(event.old_state, NodeState::Alive);
        assert_eq!(event.new_state, NodeState::Suspect);
    }

    #[test]
    fn state_of_returns_none_for_unknown() {
        let m = Membership::new();
        assert!(m.state_of(&NodeId::new("ghost")).is_none());
    }
}
