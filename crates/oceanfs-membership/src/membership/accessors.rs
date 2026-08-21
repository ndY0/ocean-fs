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
    /// Returns all known nodes with their states, incarnations,
    /// addresses, and attribution (version + origin, ADR-0028 D3).
    pub fn nodes_full(&self) -> Vec<(NodeId, NodeState, Incarnation, SocketAddr, u64, NodeId)> {
        self.state
            .read()
            .nodes
            .iter()
            .map(|(id, e)| {
                (id.clone(), e.state, e.incarnation, e.address, e.version, e.origin.clone())
            })
            .collect()
    }

    /// Returns all known nodes and their states.
    pub fn nodes(&self) -> Vec<(NodeId, NodeState)> {
        self.state.read().nodes.iter().map(|(id, e)| (id.clone(), e.state)).collect()
    }

    /// Returns the state of a specific node.
    pub fn state_of(&self, node_id: &NodeId) -> Option<NodeState> {
        self.state.read().nodes.get(node_id).map(|e| e.state)
    }

    /// Returns the recorded incarnation of a specific node.
    ///
    /// Used by the SWIM probe service to answer direct probes with the
    /// target's current incarnation (ADR-0028 D2).
    pub fn incarnation_of(&self, node_id: &NodeId) -> Option<Incarnation> {
        self.state.read().nodes.get(node_id).map(|e| e.incarnation)
    }

    /// Returns the network address of a specific node.
    pub fn address_of(&self, node_id: &NodeId) -> Option<SocketAddr> {
        self.state.read().nodes.get(node_id).map(|e| e.address)
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

    /// Returns the gossip protocol command sender, if the background
    /// gossip task has been started.
    ///
    /// Used by the gossip gRPC service to route peer pushes through the
    /// protocol's guarded `merge_delta` instead of bypassing it with a
    /// direct `upsert_node` (F1d).
    pub(crate) fn gossip_command_sender(
        &self,
    ) -> Option<tokio::sync::mpsc::Sender<crate::gossip::GossipCommand>> {
        self.gossip_tx.read().clone()
    }
}
