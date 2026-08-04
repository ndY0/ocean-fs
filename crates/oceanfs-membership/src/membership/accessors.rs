//! Membership state accessors and query methods.
//!
//! Provides read-only access to the membership state, including
//! node listing, state lookup, address resolution, and event
//! subscription. These methods are separated from the lifecycle
//! logic to keep each file under the 500-line threshold.

use std::{net::SocketAddr, sync::Arc};

use oceanfs_core::{Incarnation, NodeId, NodeState};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::{Membership, MembershipEvent};

impl Membership {
    /// Returns all known nodes with their states, incarnations, and addresses.
    pub fn nodes_full(&self) -> Vec<(NodeId, NodeState, Incarnation, SocketAddr)> {
        self.state
            .read()
            .nodes
            .iter()
            .map(|(id, (state, incarnation, addr))| (id.clone(), *state, *incarnation, *addr))
            .collect()
    }

    /// Returns all known nodes and their states.
    pub fn nodes(&self) -> Vec<(NodeId, NodeState)> {
        self.state.read().nodes.iter().map(|(id, (state, _, _))| (id.clone(), *state)).collect()
    }

    /// Returns the state of a specific node.
    pub fn state_of(&self, node_id: &NodeId) -> Option<NodeState> {
        self.state.read().nodes.get(node_id).map(|(state, _, _)| *state)
    }

    /// Returns the network address of a specific node.
    pub fn address_of(&self, node_id: &NodeId) -> Option<SocketAddr> {
        self.state.read().nodes.get(node_id).map(|(_, _, addr)| *addr)
    }

    /// Subscribes to membership change events.
    ///
    /// Returns a [`broadcast::Receiver`] that receives [`MembershipEvent`]
    /// whenever a node's state changes (ALIVE → SUSPECT → DEAD, etc.).
    pub fn subscribe(&self) -> broadcast::Receiver<MembershipEvent> {
        self.event_tx.subscribe()
    }

    /// Shuts down all background tasks gracefully.
    ///
    /// Cancels the detector and gossip tasks via the shared cancellation
    /// token. After calling this, the membership is no longer usable.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
        info!(node_id = %self.node_id, "membership shut down");
    }

    /// Returns a clone of the cancellation token for use by callers.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Returns this node's identifier.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns the ring cache reference.
    pub fn ring(&self) -> &Arc<oceanfs_routing::RingCache> {
        &self.ring
    }
}
