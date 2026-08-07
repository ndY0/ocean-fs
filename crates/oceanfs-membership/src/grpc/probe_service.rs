//! SWIM probe handler.
//!
//! Handles SWIM protocol probe requests for failure detection.
//! The probe verifies whether a target node is alive and responds
//! with its current incarnation number.
//!
//! ## Wire Protocol
//!
//! **Probe:** A unary RPC defined by the SWIM protocol (spec §12.3):
//! - The origin node sends a `ProbeRequest` to the handler at the target.
//! - If the target matches the local node, respond with `ack: true`
//!   and the current incarnation.
//! - If the probe is indirect (origin asks this node to ping the target
//!   on its behalf), this node forwards the ping to the target and
//!   relays the response back to the origin.
//!
//! ## Usage
//!
//! The probe handler is called by the failure detector's ping loop.
//! It is not a full tonic service (no generated `ProbeRpc` trait exists
//! in the current protobuf definitions); instead it is invoked as a
//! method on the handler struct by internal callers.

use oceanfs_core::{
    proto::membership::{ProbeRequest, ProbeResponse},
    Incarnation, NodeId,
};

/// SWIM probe handler for failure detection.
///
/// Handles direct and indirect probe requests per the SWIM protocol.
/// Returns the target's current incarnation number on success.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_membership::grpc::probe_service::ProbeHandler;
/// use oceanfs_core::NodeId;
///
/// let local_node_id = NodeId::new("node-1");
/// let handler = ProbeHandler::new(local_node_id, Incarnation::new(1));
///
/// let request = ProbeRequest {
///     target: Some(NodeId::new("node-1").into()),
///     origin: Some(NodeId::new("node-2").into()),
///     is_indirect: false,
/// };
///
/// let response = handler.handle_probe(&request);
/// assert!(response.ack);
/// ```
pub(crate) struct ProbeHandler {
    /// The local node's identifier.
    node_id: NodeId,
    /// The local node's current incarnation number.
    incarnation: Incarnation,
}

impl ProbeHandler {
    /// Creates a new probe handler for the given node.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The local node's identifier.
    /// * `incarnation` - The local node's current incarnation number.
    pub fn new(node_id: NodeId, incarnation: Incarnation) -> Self {
        Self { node_id, incarnation }
    }

    /// Handles a probe request.
    ///
    /// If the target matches the local node, returns `ack: true` with
    /// the current incarnation. If the probe is indirect (origin asks
    /// this node to ping the target on its behalf), the forwarding to
    /// the target node would be handled by the caller (the failure
    /// detector).
    ///
    /// # Arguments
    ///
    /// * `request` - The probe request from the origin node.
    ///
    /// # Returns
    ///
    /// A `ProbeResponse` with `ack: true` if this is the target and it
    /// is alive, or `ack: false` otherwise.
    pub fn handle_probe(&self, request: &ProbeRequest) -> ProbeResponse {
        let target_id = request.target.as_ref().map(|nid| NodeId::new(&nid.id));

        // If the target matches the local node, respond with ack.
        if target_id.as_ref() == Some(&self.node_id) {
            tracing::trace!(
                node_id = %self.node_id,
                incarnation = self.incarnation.value(),
                "direct probe received: responding with ack"
            );
            return ProbeResponse { ack: true, incarnation: self.incarnation.value() };
        }

        // For indirect pings: the caller (failure detector) forwards the
        // probe to the actual target and relays the result. This handler
        // simply returns ack: false when the target does not match the
        // local node — the caller is responsible for forwarding.
        if request.is_indirect {
            tracing::trace!(
                target = ?target_id,
                origin = ?request.origin.as_ref().map(|n| &n.id),
                "indirect probe received for non-local target"
            );
        }

        ProbeResponse { ack: false, incarnation: 0 }
    }

    /// Returns the local node's identifier.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns the local node's current incarnation.
    pub fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    /// Updates the local node's incarnation (e.g., after a restart).
    pub fn set_incarnation(&mut self, incarnation: Incarnation) {
        self.incarnation = incarnation;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn direct_ping_to_self_returns_ack() {
        let node_id = NodeId::new("test-node");
        let handler = ProbeHandler::new(node_id.clone(), Incarnation::new(42));

        let request = ProbeRequest {
            target: Some(oceanfs_core::proto::common::NodeId { id: "test-node".to_string() }),
            origin: Some(oceanfs_core::proto::common::NodeId { id: "other-node".to_string() }),
            is_indirect: false,
        };

        let response = handler.handle_probe(&request);
        assert!(response.ack);
        assert_eq!(response.incarnation, 42);
    }

    #[test]
    fn ping_to_other_returns_no_ack() {
        let node_id = NodeId::new("test-node");
        let handler = ProbeHandler::new(node_id, Incarnation::new(1));

        let request = ProbeRequest {
            target: Some(oceanfs_core::proto::common::NodeId { id: "other-node".to_string() }),
            origin: Some(oceanfs_core::proto::common::NodeId { id: "origin-node".to_string() }),
            is_indirect: false,
        };

        let response = handler.handle_probe(&request);
        assert!(!response.ack);
        assert_eq!(response.incarnation, 0);
    }

    #[test]
    fn indirect_ping_to_other_returns_no_ack() {
        let node_id = NodeId::new("relay-node");
        let handler = ProbeHandler::new(node_id, Incarnation::new(5));

        let request = ProbeRequest {
            target: Some(oceanfs_core::proto::common::NodeId { id: "actual-target".to_string() }),
            origin: Some(oceanfs_core::proto::common::NodeId { id: "origin-node".to_string() }),
            is_indirect: true,
        };

        let response = handler.handle_probe(&request);
        assert!(!response.ack);
        // The caller (failure detector) will forward to the actual target.
    }

    #[test]
    fn incarnation_update_reflected() {
        let node_id = NodeId::new("node");
        let mut handler = ProbeHandler::new(node_id.clone(), Incarnation::new(1));

        handler.set_incarnation(Incarnation::new(10));
        assert_eq!(handler.incarnation().value(), 10);

        let request = ProbeRequest {
            target: Some(oceanfs_core::proto::common::NodeId { id: "node".to_string() }),
            origin: None,
            is_indirect: false,
        };

        let response = handler.handle_probe(&request);
        assert!(response.ack);
        assert_eq!(response.incarnation, 10);
    }

    #[test]
    fn probe_with_missing_target_returns_no_ack() {
        let node_id = NodeId::new("test-node");
        let handler = ProbeHandler::new(node_id, Incarnation::new(1));

        let request = ProbeRequest { target: None, origin: None, is_indirect: false };

        let response = handler.handle_probe(&request);
        assert!(!response.ack);
    }
}
