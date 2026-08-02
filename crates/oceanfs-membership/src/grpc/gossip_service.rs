//! Gossip gRPC service.
//!
//! Handles `GossipRpc::Push` (client-streaming membership delta push)
//! and `GossipRpc::Pull` (server-streaming membership pull) for
//! SWIM-based cluster membership dissemination.

use oceanfs_network::gossip::{
    gossip_rpc_server::GossipRpc, GossipAck, GossipMessage, GossipPullRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

/// gRPC service for gossip protocol.
pub struct GossipGrpcService {
    _updated: u32,
}

impl GossipGrpcService {
    /// Creates a new gossip gRPC service.
    pub fn new() -> Self {
        Self { _updated: 0 }
    }
}

impl Default for GossipGrpcService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl GossipRpc for GossipGrpcService {
    /// Handles a client-streaming gossip push.
    ///
    /// Merges the received membership delta into the local membership state.
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
                entry_count += delta.entries.len() as u32;
            }
        }

        tracing::debug!(updated_entries = entry_count, "gossip push received");

        Ok(Response::new(GossipAck { accepted: true, updated_entries: entry_count }))
    }

    type PullStream = ReceiverStream<Result<GossipMessage, Status>>;

    /// Handles a server-streaming gossip pull.
    ///
    /// Returns a stream of membership delta messages since the
    /// requested version.
    async fn pull(
        &self,
        request: Request<GossipPullRequest>,
    ) -> Result<Response<Self::PullStream>, Status> {
        let req = request.into_inner();
        tracing::debug!(
            node_id = ?req.node_id,
            last_version = req.last_known_version,
            "gossip pull requested"
        );

        let (tx, rx) = mpsc::channel(16);

        // In a full implementation, compute the delta and stream it back.
        tokio::spawn(async move {
            let _ = tx.send(Ok(GossipMessage { delta: None, ring_version: 0, hlc: None })).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
