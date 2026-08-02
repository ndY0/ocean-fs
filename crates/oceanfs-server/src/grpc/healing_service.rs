//! Healing gRPC service.
//!
//! Handles `HealingRpc` RPCs for hinted handoff, Merkle exchange,
//! shard fetch for EC reconstruction, and repaired shard push.

use std::sync::Arc;

use oceanfs_core::{Hlc, NodeId, SegmentId};
use oceanfs_network::healing::{
    healing_rpc_server::HealingRpc, FetchShardChunk, FetchShardRequest, HintRequest, HintResponse,
    MerkleRequest, MerkleResponse, PushRepairedShardRequest, PushRepairedShardResponse,
};
use oceanfs_storage::SegmentDataStore;
use tonic::{Request, Response, Status};

/// gRPC service for healing and anti-entropy operations.
pub struct HealingGrpcService {
    /// Handoff buffer for storing hints.
    handoff: Arc<crate::HintedHandoff>,
    /// Metadata store for Merkle root lookups during anti-entropy.
    metadata_store: Arc<oceanfs_storage::MetadataStore>,
    /// Segment data store for shard fetch and repair.
    data_store: Arc<dyn SegmentDataStore>,
}

impl HealingGrpcService {
    /// Creates a new healing gRPC service.
    pub fn new(
        handoff: Arc<crate::HintedHandoff>,
        metadata_store: Arc<oceanfs_storage::MetadataStore>,
        data_store: Arc<dyn SegmentDataStore>,
    ) -> Self {
        Self { handoff, metadata_store, data_store }
    }
}

#[tonic::async_trait]
impl HealingRpc for HealingGrpcService {
    type FetchShardStream =
        tokio_stream::wrappers::ReceiverStream<Result<FetchShardChunk, Status>>;

    async fn hinted_handoff(
        &self,
        request: Request<HintRequest>,
    ) -> Result<Response<HintResponse>, Status> {
        let req = request.into_inner();

        let intended_for =
            req.intended_for.map(NodeId::from).unwrap_or_else(|| NodeId::new("unknown"));
        let segment_id =
            req.segment_id.and_then(|sid| SegmentId::try_from(sid).ok()).unwrap_or_default();
        let hlc = req.hlc.and_then(|h| Hlc::try_from(h).ok()).unwrap_or_else(Hlc::zero);

        let hint = crate::HintRecord {
            intended_for: intended_for.clone(),
            segment_id,
            offset: 0,
            length: req.data.len() as u32,
            timestamp: hlc,
            data: req.data,
        };

        match self.handoff.handoff(intended_for.clone(), hint).await {
            Ok(()) => {
                tracing::debug!(
                    intended_for = %intended_for,
                    segment_id = %segment_id,
                    "received and stored hinted handoff"
                );

                let proto_segment_id: oceanfs_core::proto::common::SegmentId = segment_id.into();
                Ok(Response::new(HintResponse {
                    accepted: true,
                    stored_segment_id: Some(proto_segment_id),
                }))
            }
            Err(e) => {
                tracing::warn!(
                    intended_for = %intended_for,
                    error = %e,
                    "failed to store hinted handoff"
                );
                Ok(Response::new(HintResponse { accepted: false, stored_segment_id: None }))
            }
        }
    }

    async fn merkle_exchange(
        &self,
        request: Request<MerkleRequest>,
    ) -> Result<Response<MerkleResponse>, Status> {
        let req = request.into_inner();

        // Get the first requested segment ID and convert to domain type
        let proto_sid = req.segment_ids.first().cloned().unwrap_or_default();
        let sid = SegmentId::try_from(proto_sid).unwrap_or_default();

        // Look up the segment's Merkle root from the metadata store
        let root_hash = self
            .metadata_store
            .list_segments()
            .into_iter()
            .filter_map(|r| r.ok())
            .find(|s| s.segment_id == sid)
            .and_then(|seg| seg.merkle_root)
            .map(|h| h.as_bytes().to_vec())
            .unwrap_or_else(|| vec![0u8; 32]);

        // Return the segment's Merkle root and leaf hashes.
        // In a full implementation, leaf hashes are computed from segment data.
        Ok(Response::new(MerkleResponse {
            segment_id: req.segment_ids.first().cloned(),
            root_hash,
            leaf_hashes: Vec::new(),
        }))
    }

    async fn fetch_shard(
        &self,
        request: Request<FetchShardRequest>,
    ) -> Result<Response<Self::FetchShardStream>, Status> {
        let req = request.into_inner();
        let segment_id =
            req.segment_id.and_then(|sid| SegmentId::try_from(sid).ok()).unwrap_or_default();
        let shard_index = req.shard_index as usize;

        // Read the full segment data and extract the requested shard.
        let data = self.data_store.read_segment_data(&segment_id).map_err(|e| {
            Status::internal(format!("failed to read segment data for shard fetch: {e}"))
        })?;

        // Determine shard size from total data length and known k+m.
        // This is a simplification — in production, we'd look up ec_k/ec_m from metadata.
        let total_shards = 6; // default k=4, m=2
        let shard_size = if data.is_empty() { 0 } else { data.len() / total_shards };

        let start = shard_index * shard_size;
        let end = (start + shard_size).min(data.len());
        let shard_data: Vec<u8> = if start < data.len() {
            data[start..end].to_vec()
        } else {
            Vec::new()
        };

        // Stream the shard data in chunks.
        let chunk_size = 65536; // 64 KB chunks
        let chunks: Vec<FetchShardChunk> = shard_data
            .chunks(chunk_size)
            .enumerate()
            .map(|(i, chunk)| FetchShardChunk {
                chunk_index: i as u32,
                data: chunk.to_vec(),
            })
            .collect();

        let (tx, rx) = tokio::sync::mpsc::channel(chunks.len().max(1));
        for chunk in chunks {
            if tx.send(Ok(chunk)).await.is_err() {
                break;
            }
        }

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn push_repaired_shard(
        &self,
        request: Request<PushRepairedShardRequest>,
    ) -> Result<Response<PushRepairedShardResponse>, Status> {
        let req = request.into_inner();
        let segment_id =
            req.segment_id.and_then(|sid| SegmentId::try_from(sid).ok()).unwrap_or_default();

        // Write the repaired shard into the data store.
        // In production this would merge the shard into the correct position.
        match self.data_store.write_segment_data(&segment_id, &req.data) {
            Ok(()) => {
                tracing::info!(
                    segment_id = %segment_id,
                    shard_index = req.shard_index,
                    "received and stored repaired shard"
                );
                Ok(Response::new(PushRepairedShardResponse { accepted: true }))
            }
            Err(e) => {
                tracing::warn!(
                    segment_id = %segment_id,
                    error = %e,
                    "failed to store repaired shard"
                );
                Ok(Response::new(PushRepairedShardResponse { accepted: false }))
            }
        }
    }
}
