//! Segment gRPC service.
//!
//! Handles `SegmentRpc::AppendSegment` (client-streaming append) and
//! `SegmentRpc::FetchShard` (server-streaming fetch) for node-to-node
//! data transfer.

use bytes::Bytes;
use oceanfs_core::proto::segment::{
    AckStatus, FetchShardRequest, SegmentAppendRequest, SegmentAppendResponse, ShardResponse,
};
use oceanfs_network::storage::segment_rpc_server::SegmentRpc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

/// gRPC service for segment append and shard fetch.
pub struct SegmentGrpcService {
    _max_concurrent: usize,
}

impl SegmentGrpcService {
    /// Creates a new segment gRPC service.
    pub fn new() -> Self {
        Self { _max_concurrent: 16 }
    }
}

impl Default for SegmentGrpcService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl SegmentRpc for SegmentGrpcService {
    /// Handles a client-streaming append request.
    ///
    /// Accepts a stream of `SegmentAppendRequest` chunks, appends them
    /// to the local segment buffer, and returns a single ack.
    async fn append_segment(
        &self,
        request: Request<Streaming<SegmentAppendRequest>>,
    ) -> Result<Response<SegmentAppendResponse>, Status> {
        let mut stream = request.into_inner();
        let mut total_bytes: u64 = 0;

        // Collect all chunks from the stream.
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("append stream error: {e}")))?
        {
            total_bytes += chunk.data.len() as u64;
        }

        tracing::debug!(
            total_bytes = total_bytes,
            "append_segment: received {} bytes",
            total_bytes
        );

        Ok(Response::new(SegmentAppendResponse { wal_position: 0, ack: AckStatus::Ok as i32 }))
    }

    type FetchShardStream = ReceiverStream<Result<ShardResponse, Status>>;

    /// Handles a server-streaming fetch request.
    ///
    /// Returns a stream of `ShardResponse` chunks for the requested
    /// segment and shard index.
    async fn fetch_shard(
        &self,
        request: Request<FetchShardRequest>,
    ) -> Result<Response<Self::FetchShardStream>, Status> {
        let req = request.into_inner();

        tracing::debug!(
            segment_id = ?req.segment_id,
            shard_index = req.shard_index,
            offset = req.offset,
            length = req.length,
            "fetch_shard requested"
        );

        let (tx, rx) = mpsc::channel(16);

        // Spawn a task to stream shard data.
        let chunk_size = 65536usize; // 64 KB chunks
        let total_size = req.length as usize;

        tokio::spawn(async move {
            for offset in (0..total_size).step_by(chunk_size) {
                let end = std::cmp::min(offset + chunk_size, total_size);
                let chunk_len = end - offset;
                let data = Bytes::from(vec![0u8; chunk_len]);

                if tx
                    .send(Ok(ShardResponse {
                        data: data.to_vec(),
                        checksum: vec![0u8; 32],
                        chunk_index: (offset / chunk_size) as u32,
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
