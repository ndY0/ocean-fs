//! Gossip gRPC service.
//!
//! Handles `GossipRpc::Push` (client-streaming membership delta push)
//! and `GossipRpc::Pull` (server-streaming membership pull) for
//! SWIM-based cluster membership dissemination.
//!
//! ## Wire Protocol
//!
//! **Push:** The remote node streams `GossipMessage` entries containing
//! membership deltas. The service merges each entry into the local
//! `Membership` state via `upsert_node`. Returns a `GossipAck` with
//! the number of updated entries.
//!
//! **Pull:** The remote node requests membership entries newer than
//! a given version. The service computes the delta from local
//! membership state and streams it back as `GossipMessage` chunks.

use std::{net::SocketAddr, sync::Arc};

use oceanfs_core::{Incarnation, NodeId, NodeState};
use oceanfs_network::gossip::{
    gossip_rpc_server::GossipRpc, GossipAck, GossipMessage, GossipPullRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

/// gRPC service for gossip protocol.
///
/// Exposes the push/pull interface for cluster membership
/// dissemination. Uses the local `Membership` instance to
/// merge incoming deltas and compute outgoing deltas.
pub struct GossipGrpcService {
    /// Local membership state for delta computation and merging.
    membership: Arc<crate::Membership>,
}

impl GossipGrpcService {
    /// Creates a new gossip gRPC service.
    ///
    /// # Arguments
    ///
    /// * `membership` - The local membership state used for merge and delta computation.
    pub fn new(membership: Arc<crate::Membership>) -> Self {
        Self { membership }
    }
}

#[tonic::async_trait]
impl GossipRpc for GossipGrpcService {
    /// Handles a client-streaming gossip push.
    ///
    /// Each `GossipMessage` in the stream may contain a membership
    /// delta with one or more entries. Each entry is merged into the
    /// local membership state via `Membership::upsert_node`.
    ///
    /// Returns `GossipAck { accepted: true, updated_entries: N }`.
    async fn push(
        &self,
        request: Request<Streaming<GossipMessage>>,
    ) -> Result<Response<GossipAck>, Status> {
        let mut stream = request.into_inner();
        let mut entry_count: u32 = 0;

        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("gossip push stream error: {e}")))?
        {
            if let Some(delta) = msg.delta {
                for entry in &delta.entries {
                    let node_id = entry
                        .node_id
                        .as_ref()
                        .map(|nid| NodeId::new(&nid.id))
                        .unwrap_or_else(|| NodeId::new("unknown"));

                    let state = match entry.state {
                        0 => NodeState::Alive,
                        1 => NodeState::Suspect,
                        2 => NodeState::Dead,
                        3 => NodeState::Leaving,
                        4 => NodeState::Left,
                        _ => NodeState::Alive,
                    };

                    let incarnation = Incarnation::new(entry.incarnation);

                    // Parse address from the protobuf string.
                    let address = entry
                        .address
                        .parse::<SocketAddr>()
                        .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 9001)));

                    self.membership.upsert_node(node_id, state, incarnation, address);
                    entry_count += 1;
                }
            }
        }

        tracing::debug!(updated_entries = entry_count, "gossip push received and merged");

        Ok(Response::new(GossipAck { accepted: true, updated_entries: entry_count }))
    }

    type PullStream = ReceiverStream<Result<GossipMessage, Status>>;

    /// Handles a server-streaming gossip pull.
    ///
    /// Reads the full membership list from the local `Membership`
    /// instance and streams it back as `GossipMessage` entries.
    ///
    /// The `last_known_version` field in the request is used to
    /// filter entries by incarnation (in a full implementation,
    /// this would track a centralized version counter).
    async fn pull(
        &self,
        request: Request<GossipPullRequest>,
    ) -> Result<Response<Self::PullStream>, Status> {
        let req = request.into_inner();
        let last_known_version = req.last_known_version;

        tracing::debug!(
            node_id = ?req.node_id,
            last_version = last_known_version,
            "gossip pull requested"
        );

        // Collect all known nodes from the membership.
        let nodes = self.membership.nodes_full();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            // Filter nodes by incarnation > last_known_version.
            // incarnation is a u64 that acts as a logical version.
            let filtered: Vec<_> = nodes
                .into_iter()
                .filter(|(_, _, incarnation, _)| incarnation.value() > last_known_version)
                .collect();

            if filtered.is_empty() {
                // Send an empty delta to acknowledge the pull.
                let _ =
                    tx.send(Ok(GossipMessage { delta: None, ring_version: 0, hlc: None })).await;
            } else {
                for (node_id, state, incarnation, address) in filtered {
                    let proto_node_id =
                        oceanfs_core::proto::common::NodeId { id: node_id.to_string() };

                    let proto_state = match state {
                        NodeState::Alive => 0,
                        NodeState::Suspect => 1,
                        NodeState::Dead => 2,
                        NodeState::Leaving => 3,
                        NodeState::Left => 4,
                    };

                    let entry = oceanfs_core::proto::membership::MembershipEntry {
                        node_id: Some(proto_node_id),
                        state: proto_state,
                        incarnation: incarnation.value(),
                        address: address.to_string(),
                        last_seen: None,
                    };

                    let delta =
                        oceanfs_core::proto::membership::MembershipList { entries: vec![entry] };

                    if tx
                        .send(Ok(GossipMessage { delta: Some(delta), ring_version: 0, hlc: None }))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::SocketAddr;

    use oceanfs_core::{GossipConfig, Incarnation, NodeId, NodeState, RingConfig};
    use oceanfs_network::gossip::{
        gossip_rpc_client::GossipRpcClient, gossip_rpc_server::GossipRpcServer,
    };
    use oceanfs_routing::{Ring, RingCache};
    use tonic::transport::Server;

    use super::*;
    use crate::Membership;

    fn make_membership(node_id: &str) -> Arc<Membership> {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new(node_id));
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        Arc::new(Membership::new(NodeId::new(node_id), addr, GossipConfig::default(), ring_cache))
    }

    /// Helper to start a test gRPC server with the gossip service and return a client.
    async fn test_server(
        membership: Arc<Membership>,
    ) -> GossipRpcClient<tonic::transport::Channel> {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let service = GossipGrpcService::new(membership);
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            Server::builder()
                .add_service(GossipRpcServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        // Give the server a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        GossipRpcClient::connect(format!("http://{addr}")).await.unwrap()
    }

    #[tokio::test]
    async fn push_new_entries_updates_membership() {
        let membership = make_membership("node-a");
        let mut client = test_server(membership.clone()).await;

        // Construct a GossipMessage with a delta containing one entry.
        let entry = oceanfs_core::proto::membership::MembershipEntry {
            node_id: Some(oceanfs_core::proto::common::NodeId { id: "node-b".to_string() }),
            state: 0, // ALIVE
            incarnation: 1,
            address: "127.0.0.1:9002".to_string(),
            last_seen: None,
        };

        let delta = oceanfs_core::proto::membership::MembershipList { entries: vec![entry] };

        let msg = GossipMessage { delta: Some(delta), ring_version: 0, hlc: None };

        let stream = tokio_stream::iter(vec![msg]);
        let request = tonic::Request::new(stream);

        let response = client.push(request).await.unwrap();
        let ack = response.into_inner();

        assert!(ack.accepted);
        assert_eq!(ack.updated_entries, 1);

        // Verify the node was added to membership.
        let state = membership.state_of(&NodeId::new("node-b"));
        assert_eq!(state, Some(NodeState::Alive));
    }

    #[tokio::test]
    async fn pull_with_version_returns_delta() {
        let membership = make_membership("node-a");

        // Add a node at incarnation 5.
        membership.upsert_node(
            NodeId::new("node-b"),
            NodeState::Alive,
            Incarnation::new(5),
            "127.0.0.1:9002".parse().unwrap(),
        );

        let mut client = test_server(membership).await;

        // Request delta for anything with version > 3.
        let request = tonic::Request::new(GossipPullRequest {
            node_id: Some(oceanfs_core::proto::common::NodeId { id: "node-x".to_string() }),
            last_known_version: 3,
        });

        let mut response_stream = client.pull(request).await.unwrap().into_inner();

        let mut has_node_b = false;
        while let Some(msg) = response_stream.message().await.unwrap() {
            if let Some(ref delta) = msg.delta {
                for entry in &delta.entries {
                    if entry.node_id.as_ref().map(|n| n.id == "node-b").unwrap_or(false)
                        && entry.incarnation == 5
                    {
                        has_node_b = true;
                    }
                }
            }
        }

        assert!(has_node_b, "should include node-b at incarnation 5");
    }

    #[tokio::test]
    async fn pull_with_current_version_returns_empty() {
        let membership = make_membership("node-a");

        // Add a node at incarnation 1.
        membership.upsert_node(
            NodeId::new("node-b"),
            NodeState::Alive,
            Incarnation::new(1),
            "127.0.0.1:9002".parse().unwrap(),
        );

        let mut client = test_server(membership).await;

        // Request delta for anything with version > 100 (nothing).
        let request = tonic::Request::new(GossipPullRequest {
            node_id: Some(oceanfs_core::proto::common::NodeId { id: "node-x".to_string() }),
            last_known_version: 100,
        });

        let mut response_stream = client.pull(request).await.unwrap().into_inner();

        let mut count = 0u32;
        let mut has_data = false;
        while let Some(msg) = response_stream.message().await.unwrap() {
            count += 1;
            if msg.delta.is_some() {
                has_data = true;
            }
        }
        assert!(count >= 1, "should receive at least one response");
        assert!(!has_data, "all deltas should be empty when version is current");
    }
}
