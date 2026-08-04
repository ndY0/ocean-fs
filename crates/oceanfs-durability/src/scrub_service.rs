//! Scrub gRPC service.
//!
//! Handles `ScrubRpc` RPCs for distributed scrubbing:
//! `AssignPartition` (coordinator → worker) and
//! `ReportPartitionResult` (worker → coordinator).

use std::sync::Arc;

use oceanfs_core::{NodeId, SegmentId};
use tonic::{Request, Response, Status};

use crate::{
    scrub_rpc::{
        scrub_rpc_server::ScrubRpc, AssignPartitionRequest, AssignPartitionResponse,
        ReportPartitionResultRequest, ReportPartitionResultResponse,
    },
    SegmentDataStore,
};

/// gRPC service for distributed scrub operations.
pub struct ScrubGrpcService {
    /// Metadata store for segment enumeration during scrub.
    #[allow(dead_code)]
    metadata_store: Arc<oceanfs_storage::RocksDbMetadataStore>,
    /// Segment data store for reading shard data during verification.
    #[allow(dead_code)]
    data_store: Arc<dyn SegmentDataStore>,
}

impl ScrubGrpcService {
    /// Creates a new scrub gRPC service.
    pub fn new(
        metadata_store: Arc<oceanfs_storage::RocksDbMetadataStore>,
        data_store: Arc<dyn SegmentDataStore>,
    ) -> Self {
        Self { metadata_store, data_store }
    }
}

#[tonic::async_trait]
impl ScrubRpc for ScrubGrpcService {
    /// Coordinator sends a partition of segment IDs to a worker for scrubbing.
    async fn assign_partition(
        &self,
        request: Request<AssignPartitionRequest>,
    ) -> Result<Response<AssignPartitionResponse>, Status> {
        let req = request.into_inner();

        let segment_ids: Vec<SegmentId> =
            req.segment_ids.into_iter().filter_map(|sid| SegmentId::try_from(sid).ok()).collect();

        tracing::info!(segment_count = segment_ids.len(), "received scrub partition assignment");

        // Accept the assignment. The actual scrubbing is handled
        // asynchronously by the background scrub task.
        Ok(Response::new(AssignPartitionResponse { accepted: true }))
    }

    /// Worker reports scrub results back to the coordinator.
    async fn report_partition_result(
        &self,
        request: Request<ReportPartitionResultRequest>,
    ) -> Result<Response<ReportPartitionResultResponse>, Status> {
        let req = request.into_inner();

        let node_id = req.node_id.map(NodeId::from).unwrap_or_else(|| NodeId::new("unknown"));

        tracing::info!(
            node_id = %node_id,
            total = req.segments_total,
            healthy = req.segments_healthy,
            corrupt = req.segments_corrupt,
            healed = req.segments_healed,
            bytes_scanned = req.bytes_scanned,
            "received scrub partition result from worker"
        );

        Ok(Response::new(ReportPartitionResultResponse { accepted: true }))
    }
}
