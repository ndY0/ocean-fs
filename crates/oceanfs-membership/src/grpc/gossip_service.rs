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
    /// delta with one or more entries. Entries are routed through the
    /// gossip protocol's guarded `merge_delta` (F1d + the ADR-0028 D3
    /// authority-class rules): merging them directly via `upsert_node`
    /// would bypass the incarnation/attribution ordering and let a
    /// peer's stale view clobber the local Suspect/Dead state (t24
    /// oscillation). When the protocol task is not running (tests), the
    /// direct upsert fallback is used.
    ///
    /// Returns `GossipAck { accepted: true, updated_entries: N }`.
    async fn push(
        &self,
        request: Request<Streaming<GossipMessage>>,
    ) -> Result<Response<GossipAck>, Status> {
        let mut stream = request.into_inner();
        let mut entry_count: u32 = 0;
        // The requester's version vector (ADR-0028 D4): taken from the
        // last message of the push stream (our implementation sends one
        // message per round; the vector is identical across messages).
        let mut requester_vector: std::collections::HashMap<
            NodeId,
            std::collections::HashMap<NodeId, u64>,
        > = std::collections::HashMap::new();

        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("gossip push stream error: {e}")))?
        {
            requester_vector = msg
                .version_vector
                .iter()
                .map(|(node, vv)| {
                    (
                        NodeId::new(node),
                        vv.versions
                            .iter()
                            .map(|(o, v)| (NodeId::new(o), *v))
                            .collect::<std::collections::HashMap<_, _>>(),
                    )
                })
                .collect();
            if let Some(delta) = msg.delta {
                let mut changed = Vec::with_capacity(delta.entries.len());
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

                    changed.push(crate::membership::state::NodeEntry {
                        node_id,
                        incarnation,
                        state,
                        address,
                        version: entry.version,
                        origin: NodeId::new(&entry.origin),
                    });
                    entry_count += 1;
                }

                // Route through the guarded merge when the protocol is
                // running; fall back to direct upserts otherwise.
                if let Some(tx) = self.membership.gossip_command_sender() {
                    let _ = tx.try_send(crate::gossip::GossipCommand::ReceiveDelta {
                        from: NodeId::new("gossip-push"),
                        delta: crate::membership::state::GossipDelta { changed },
                    });
                } else {
                    for entry in changed {
                        self.membership.upsert_node_attributed(
                            entry.node_id,
                            entry.state,
                            entry.incarnation,
                            Some(entry.address),
                            entry.version,
                            entry.origin,
                        );
                    }
                }
            }
        }

        tracing::debug!(updated_entries = entry_count, "gossip push received and merged");

        // ADR-0028 D4: the push response carries the peer's pull (entries
        // the requester lacks, per its version vector) plus the peer's
        // version vector, so the requester can advance its watermark.
        let nodes = self.membership.nodes_full();
        let pull_delta = oceanfs_core::proto::membership::MembershipList {
            entries: nodes
                .iter()
                .filter(|(node_id, _, _, _, version, origin)| {
                    requester_vector
                        .get(node_id)
                        .and_then(|origins| origins.get(origin))
                        .map_or(true, |known| *version > *known)
                })
                .map(|(node_id, state, incarnation, address, version, origin)| {
                    oceanfs_core::proto::membership::MembershipEntry {
                        node_id: Some(oceanfs_core::proto::common::NodeId {
                            id: node_id.to_string(),
                        }),
                        state: match state {
                            NodeState::Alive => 0,
                            NodeState::Suspect => 1,
                            NodeState::Dead => 2,
                            NodeState::Leaving => 3,
                            NodeState::Left => 4,
                        },
                        incarnation: incarnation.value(),
                        address: address.to_string(),
                        last_seen: None,
                        version: *version,
                        origin: origin.to_string(),
                    }
                })
                .collect(),
        };
        let ack_vector = nodes
            .iter()
            .map(|(node_id, _, _, _, version, origin)| {
                (node_id.to_string(), (origin.to_string(), *version))
            })
            .fold(
                std::collections::HashMap::<String, oceanfs_network::gossip::VersionVector>::new(),
                |mut acc, (node, (origin, version))| {
                    acc.entry(node).or_default().versions.insert(origin, version);
                    acc
                },
            );

        Ok(Response::new(GossipAck {
            accepted: true,
            updated_entries: entry_count,
            delta: Some(pull_delta),
            version_vector: ack_vector,
        }))
    }

    type PullStream = ReceiverStream<Result<GossipMessage, Status>>;

    /// Handles a server-streaming gossip pull.
    ///
    /// Reads the full membership list from the local `Membership`
    /// instance and streams back the entries newer than the requester's
    /// version vector (ADR-0028 D4). An empty vector returns everything
    /// (join).
    async fn pull(
        &self,
        request: Request<GossipPullRequest>,
    ) -> Result<Response<Self::PullStream>, Status> {
        let req = request.into_inner();
        let version_vector: std::collections::HashMap<
            NodeId,
            std::collections::HashMap<NodeId, u64>,
        > = req
            .version_vector
            .iter()
            .map(|(node, vv)| {
                (
                    NodeId::new(node),
                    vv.versions
                        .iter()
                        .map(|(o, v)| (NodeId::new(o), *v))
                        .collect::<std::collections::HashMap<_, _>>(),
                )
            })
            .collect();

        tracing::debug!(
            node_id = ?req.node_id,
            vector_len = version_vector.len(),
            "gossip pull requested"
        );

        // Collect all known nodes from the membership.
        let nodes = self.membership.nodes_full();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            // Per-(node, origin) filter: entries whose version is newer
            // than the requester's recorded value for that attributed
            // entry.
            let filtered: Vec<_> = nodes
                .into_iter()
                .filter(|(node_id, _, _, _, version, origin)| {
                    version_vector
                        .get(node_id)
                        .and_then(|origins| origins.get(origin))
                        .map_or(true, |known| *version > *known)
                })
                .collect();

            if filtered.is_empty() {
                // Send an empty delta to acknowledge the pull.
                let _ = tx
                    .send(Ok(GossipMessage {
                        delta: None,
                        version_vector: std::collections::HashMap::new(),
                    }))
                    .await;
            } else {
                for (node_id, state, incarnation, address, version, origin) in filtered {
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
                        // ADR-0028 D3: attribution travels with the entry.
                        version,
                        origin: origin.to_string(),
                    };

                    let delta =
                        oceanfs_core::proto::membership::MembershipList { entries: vec![entry] };

                    if tx
                        .send(Ok(GossipMessage {
                            delta: Some(delta),
                            version_vector: std::collections::HashMap::new(),
                        }))
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
            version: 0,
            origin: String::new(),
        };

        let delta = oceanfs_core::proto::membership::MembershipList { entries: vec![entry] };

        let msg =
            GossipMessage { delta: Some(delta), version_vector: std::collections::HashMap::new() };

        let stream = tokio_stream::iter(vec![msg]);
        let request = tonic::Request::new(stream);

        let response = client.push(request).await.unwrap();
        let ack = response.into_inner();

        assert!(ack.accepted);
        assert_eq!(ack.updated_entries, 1);

        // Verify the node was added to membership.
        let state = membership.state_of(&NodeId::new("node-b"));
        assert_eq!(state, Some(NodeState::Alive));

        // PR4: Verify the ring was updated via upsert_node.
        let ring_snapshot = membership.ring().snapshot();
        assert!(
            ring_snapshot.nodes().contains(&NodeId::new("node-b")),
            "push should add the new node to the ring via upsert_node"
        );
    }

    #[tokio::test]
    async fn pull_with_version_returns_delta() {
        let membership = make_membership("node-a");

        // Add a node at incarnation 5.
        membership.upsert_node(
            NodeId::new("node-b"),
            NodeState::Alive,
            Incarnation::new(5),
            Some("127.0.0.1:9002".parse().unwrap()),
        );

        let mut client = test_server(membership).await;

        // Request delta for node-b with a vector that only knows
        // version 3 (node-b's entry is at version 1 from the pull...
        // the test asserts inclusion by incarnation, so use a vector
        // that knows nothing about node-b's origins: an entry is
        // included when its (node, origin) key is unknown).
        let mut vector = std::collections::HashMap::new();
        vector.insert("node-b".to_string(), oceanfs_network::gossip::VersionVector::default());
        let request = tonic::Request::new(GossipPullRequest {
            node_id: Some(oceanfs_core::proto::common::NodeId { id: "node-x".to_string() }),
            version_vector: vector,
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

        // Add a node at incarnation 1 with full attribution (origin =
        // the node itself, version 1).
        membership.upsert_node_attributed(
            NodeId::new("node-b"),
            NodeState::Alive,
            Incarnation::new(1),
            Some("127.0.0.1:9002".parse().unwrap()),
            1,
            NodeId::new("node-b"),
        );

        let mut client = test_server(membership).await;

        // Request delta with a vector that knows node-b's (node-b →
        // node-b) attribution at version 100 — the entry's version (1)
        // is not newer → nothing to pull.
        let mut vector = std::collections::HashMap::new();
        let mut vv = oceanfs_network::gossip::VersionVector::default();
        vv.versions.insert("node-b".to_string(), 100u64);
        vector.insert("node-b".to_string(), vv);
        let request = tonic::Request::new(GossipPullRequest {
            node_id: Some(oceanfs_core::proto::common::NodeId { id: "node-x".to_string() }),
            version_vector: vector,
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
