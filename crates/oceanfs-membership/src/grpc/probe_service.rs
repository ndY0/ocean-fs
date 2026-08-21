//! SWIM probe handler.
//!
//! Handles SWIM protocol probe requests for failure detection.
//! The probe verifies whether a target node is alive and responds
//! with its current incarnation number.
//!
//! ## Wire Protocol
//!
//! **Probe:** A unary RPC defined by the SWIM protocol (spec §12.3,
//! ADR-0028 D2):
//! - The origin node sends a `ProbeRequest` to the handler at the target.
//! - If the target matches the local node, respond with `ack: true`
//!   and the current incarnation.
//! - If the probe is indirect (origin asks this node to ping the target
//!   on its behalf), this node forwards the ping to the target and
//!   relays the response back to the origin.
//!
//! ## Usage
//!
//! [`ProbeGrpcService`] is the tonic service registered on the
//! membership plane (ADR-0028 D1); [`ProbeHandler`] remains the
//! in-process direct-probe responder used by the failure detector.

use std::{sync::Arc, time::Duration};

use oceanfs_core::{
    proto::membership::{ProbeRequest, ProbeResponse},
    Incarnation, NodeId,
};
use oceanfs_network::{
    gossip::{probe_rpc_client::ProbeRpcClient, probe_rpc_server::ProbeRpc},
    ConnectionPool,
};
use tonic::{Request, Response, Status};

use crate::Membership;

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
pub struct ProbeHandler {
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

/// The membership plane's tonic probe service (ADR-0028 D2).
///
/// Serves `ProbeRpc` on the membership listener:
/// - **Direct probe** (`is_indirect = false`): if the target is this
///   node, ack with the local incarnation; otherwise nack.
/// - **Indirect probe** (`is_indirect = true`): this node is the relay.
///   If the target is this node, ack directly; otherwise forward a
///   direct probe to the target over the membership plane's connection
///   pool and relay the response back to the origin.
///
/// The forward is bounded by `ping_timeout_ms` so a relay never hangs
/// past the origin's probe budget.
pub struct ProbeGrpcService {
    /// The local node's identifier.
    node_id: NodeId,
    /// Local membership, used to resolve target addresses and the
    /// local incarnation.
    membership: Arc<Membership>,
    /// The membership plane's connection pool (relay forwarding).
    pool: Arc<ConnectionPool>,
    /// Timeout for relay-forwarded probes in milliseconds.
    ping_timeout_ms: u64,
}

impl ProbeGrpcService {
    /// Creates the probe service for the membership plane.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::sync::Arc;
    /// use oceanfs_core::NodeId;
    /// use oceanfs_membership::grpc::probe_service::ProbeGrpcService;
    ///
    /// // membership and pool are wired by the node composition root.
    /// # let membership: Arc<oceanfs_membership::Membership> = unimplemented!();
    /// # let pool: Arc<oceanfs_network::ConnectionPool> = unimplemented!();
    /// let service = ProbeGrpcService::new(NodeId::new("node-1"), membership, pool, 1000);
    /// ```
    pub fn new(
        node_id: NodeId,
        membership: Arc<Membership>,
        pool: Arc<ConnectionPool>,
        ping_timeout_ms: u64,
    ) -> Self {
        Self { node_id, membership, pool, ping_timeout_ms }
    }

    /// Resolves the response for a probe targeting `target_id`.
    ///
    /// A direct probe to self acks with the local incarnation. Any other
    /// probe (indirect relay where the target is a third node, or a
    /// misdirected direct probe) is resolved by the caller.
    fn ack_for_target(&self, target_id: &NodeId) -> Option<ProbeResponse> {
        if target_id == &self.node_id {
            let incarnation =
                self.membership.incarnation_of(target_id).unwrap_or(Incarnation::new(1));
            return Some(ProbeResponse { ack: true, incarnation: incarnation.value() });
        }
        None
    }

    /// Forwards a direct probe to `target_id` and relays the response.
    ///
    /// Resolves the target's announced membership address and probes it
    /// over the membership plane pool, bounded by `ping_timeout_ms`.
    async fn forward_to(&self, target_id: &NodeId) -> ProbeResponse {
        let Some(addr) = self.membership.address_of(target_id) else {
            tracing::trace!(target = %target_id, "relay: target unknown locally — nack");
            return ProbeResponse { ack: false, incarnation: 0 };
        };

        let pooled = match tokio::time::timeout(
            Duration::from_millis(self.ping_timeout_ms),
            self.pool.get_channel(addr),
        )
        .await
        {
            Ok(Ok(pooled)) => pooled,
            Ok(Err(e)) => {
                tracing::warn!(target = %target_id, error = %e, "relay: channel acquisition failed");
                return ProbeResponse { ack: false, incarnation: 0 };
            }
            Err(_) => {
                tracing::warn!(target = %target_id, "relay: channel acquisition timed out");
                return ProbeResponse { ack: false, incarnation: 0 };
            }
        };

        let channel = pooled.channel().clone();
        drop(pooled);

        let mut client = ProbeRpcClient::new(channel);
        let request = ProbeRequest {
            target: Some(oceanfs_core::proto::common::NodeId { id: target_id.to_string() }),
            origin: Some(oceanfs_core::proto::common::NodeId { id: self.node_id.to_string() }),
            is_indirect: false,
        };

        match tokio::time::timeout(
            Duration::from_millis(self.ping_timeout_ms),
            client.probe(Request::new(request)),
        )
        .await
        {
            Ok(Ok(response)) => response.into_inner(),
            Ok(Err(status)) => {
                tracing::warn!(target = %target_id, error = %status, "relay: forwarded probe failed");
                ProbeResponse { ack: false, incarnation: 0 }
            }
            Err(_) => {
                tracing::warn!(target = %target_id, "relay: forwarded probe timed out");
                ProbeResponse { ack: false, incarnation: 0 }
            }
        }
    }
}

#[tonic::async_trait]
impl ProbeRpc for ProbeGrpcService {
    async fn probe(
        &self,
        request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        let req = request.into_inner();
        let target_id = req.target.as_ref().map(|n| NodeId::new(&n.id));

        tracing::trace!(
            target = ?target_id,
            origin = ?req.origin.as_ref().map(|n| &n.id),
            is_indirect = req.is_indirect,
            "probe received"
        );

        let Some(target_id) = target_id else {
            return Ok(Response::new(ProbeResponse { ack: false, incarnation: 0 }));
        };

        // A probe that names this node — direct or relayed — acks
        // immediately with the local incarnation.
        if let Some(response) = self.ack_for_target(&target_id) {
            return Ok(Response::new(response));
        }

        // Indirect probe for a third node: we are the relay. Forward a
        // direct probe to the target and relay the response.
        if req.is_indirect {
            let response = self.forward_to(&target_id).await;
            return Ok(Response::new(response));
        }

        // Direct probe for a node that is not us: misdirected.
        Ok(Response::new(ProbeResponse { ack: false, incarnation: 0 }))
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
