//! Scrub gRPC service.
//!
//! Handles `ScrubRpc` RPCs for distributed scrubbing:
//! `AssignPartition` (coordinator → worker) and
//! `ReportPartitionResult` (worker → coordinator).
//!
//! Per ADR-0033 D1 (scrub half) `assign_partition` is **wired**: the
//! service executes the assigned partition through the receiving node's
//! `ScrubWorker` (registry + data store) and only then acks with
//! `accepted: true`. A data-store failure returns an error `Status` —
//! there is no accept-and-ignore path.

use std::sync::Arc;

use oceanfs_core::{NodeId, SegmentId};
use oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry;
use oceanfs_storage_api::SegmentDataStore;
use tonic::{Request, Response, Status};

use crate::{
    scrub::{ScrubResult, ScrubWorker, SegmentPartition},
    scrub_rpc::{
        scrub_rpc_server::ScrubRpc, AssignPartitionRequest, AssignPartitionResponse,
        ReportPartitionResultRequest, ReportPartitionResultResponse,
    },
};

/// Summary of an executed partition scrub.
///
/// Made observable so the coordinator-side `report_partition_result`
/// aggregator (and tests) can see what a partition actually did — the
/// same healthy/corrupt/healed/bytes accounting as a local cycle.
#[derive(Debug, Clone, Default)]
pub(crate) struct ExecutedPartitionSummary {
    /// Total segments in the executed partition.
    pub(crate) segments_total: usize,
    /// Segments that verified healthy.
    pub(crate) segments_healthy: usize,
    /// Corrupt segments detected.
    pub(crate) segments_corrupt: usize,
    /// Corrupt segments enqueued for EC heal.
    pub(crate) segments_healed: usize,
    /// Bytes scanned while executing the partition.
    pub(crate) bytes_scanned: u64,
}

impl ExecutedPartitionSummary {
    /// Aggregates a partition's `ScrubResult`s (mirrors the local
    /// `run_cycle` accounting: skipped results are neither healthy nor
    /// corrupt).
    fn from_results(results: &[ScrubResult]) -> Self {
        let mut summary = ExecutedPartitionSummary::default();
        for result in results {
            summary.bytes_scanned += result.bytes_scanned;
            if result.skipped {
                continue;
            }
            if result.healthy {
                summary.segments_healthy += 1;
            } else {
                summary.segments_corrupt += 1;
            }
            if result.enqueued_heal {
                summary.segments_healed += 1;
            }
        }
        summary.segments_total = results.len();
        summary
    }
}

/// gRPC service for distributed scrub operations.
pub struct ScrubGrpcService {
    /// The receiving node's scrub worker (owns the local registry + data
    /// store — what `assign_partition` executes against).
    worker: Arc<ScrubWorker>,
    /// Last executed partition summary (tests + the future coordinator
    /// aggregator entry).
    last_result: parking_lot::Mutex<Option<ExecutedPartitionSummary>>,
}

impl ScrubGrpcService {
    /// Creates a scrub gRPC service over the receiving node's registry
    /// and data store.
    ///
    /// The worker is constructed internally so the executor and the data
    /// it reads are always the receiving node's own (ADR-0032 single
    /// store). `metadata_store` is no longer needed: scrub's segment
    /// set comes from the lifecycle registry's `Sealed` entries.
    pub fn new(
        registry: Arc<SegmentLifecycleRegistry>,
        data_store: Arc<dyn SegmentDataStore>,
    ) -> Self {
        Self {
            worker: Arc::new(ScrubWorker::new(registry, data_store, 0)),
            last_result: parking_lot::Mutex::new(None),
        }
    }

    /// Returns the most recently executed partition summary, if any.
    ///
    /// Tests assert a partition actually ran through this accessor; the
    /// future coordinator aggregator reads it when dispatch scheduling
    /// lands. Kept `pub(crate)` for that cross-module use.
    #[allow(dead_code)] // exercised by #[cfg(test)]; future aggregator consumer.
    pub(crate) fn last_result(&self) -> Option<ExecutedPartitionSummary> {
        self.last_result.lock().clone()
    }
}

#[tonic::async_trait]
impl ScrubRpc for ScrubGrpcService {
    /// Executes an assigned partition scrub and returns a truthful ack.
    ///
    /// Runs the assigned segment list through the local `ScrubWorker`;
    /// `accepted` is `true` only after the partition actually ran. An
    /// unavailable data store returns an error `Status` (ADR-0033 D1 —
    /// no silent acks). An empty assignment is trivially executed.
    async fn assign_partition(
        &self,
        request: Request<AssignPartitionRequest>,
    ) -> Result<Response<AssignPartitionResponse>, Status> {
        let req = request.into_inner();

        let segment_ids: Vec<SegmentId> =
            req.segment_ids.into_iter().filter_map(|sid| SegmentId::try_from(sid).ok()).collect();

        tracing::info!(segment_count = segment_ids.len(), "received scrub partition assignment");

        if segment_ids.is_empty() {
            // Nothing to execute — trivially truthful.
            return Ok(Response::new(AssignPartitionResponse { accepted: true }));
        }

        let partition = SegmentPartition { node_id: NodeId::new("local"), segment_ids };
        let worker = Arc::clone(&self.worker);
        let results = tokio::task::spawn(async move { worker.scrub_partition(&partition).await })
            .await
            .map_err(|e| Status::internal(format!("scrub partition task failed to join: {e}")))?;

        // A store I/O failure means the partition did NOT run: reject
        // instead of acking work that was never performed.
        if results.iter().any(ScrubResult::store_failure) {
            return Err(Status::internal(
                "segment data store unavailable — assigned partition was not scrubbed",
            ));
        }

        let summary = ExecutedPartitionSummary::from_results(&results);
        *self.last_result.lock() = Some(summary.clone());

        tracing::info!(
            total = summary.segments_total,
            healthy = summary.segments_healthy,
            corrupt = summary.segments_corrupt,
            healed = summary.segments_healed,
            bytes_scanned = summary.bytes_scanned,
            "scrub partition executed"
        );

        Ok(Response::new(AssignPartitionResponse { accepted: true }))
    }

    /// Aggregates a worker's executed summary on the coordinator side.
    ///
    /// Today the handler logs the truthful executed summary (a silent log
    /// was never a silent ack — the ack side is now real). When a
    /// coordinator pending-cycle aggregator exists (dispatch scheduling),
    /// the summary is forwarded to it.
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::SegmentId;
    use oceanfs_storage_api::SegmentDataStore;
    use tonic::Request;

    use super::*;
    use crate::anti_entropy::InMemorySegmentStore;

    /// An in-memory store that fails every read — simulates an
    /// unavailable data store.
    struct UnavailableStore;

    #[async_trait::async_trait]
    impl SegmentDataStore for UnavailableStore {
        async fn read_segment_data(
            &self,
            _segment_id: &SegmentId,
        ) -> oceanfs_storage_api::error::Result<Option<oceanfs_storage_api::SegmentFile>> {
            Err(oceanfs_storage_api::error::Error::Internal("store unavailable".into()))
        }

        async fn write_segment_data(
            &self,
            _segment_id: &SegmentId,
            _data: &[u8],
        ) -> oceanfs_storage_api::error::Result<()> {
            Err(oceanfs_storage_api::error::Error::Internal("store unavailable".into()))
        }

        async fn delete_shards(
            &self,
            _segment_id: &SegmentId,
        ) -> oceanfs_storage_api::error::Result<u64> {
            Err(oceanfs_storage_api::error::Error::Internal("store unavailable".into()))
        }

        async fn delete_shards_with_pool(
            &self,
            _segment_id: &SegmentId,
            _pool_id: u32,
        ) -> oceanfs_storage_api::error::Result<u64> {
            Err(oceanfs_storage_api::error::Error::Internal("store unavailable".into()))
        }

        fn list_segment_files(
            &self,
            _root: &std::path::Path,
        ) -> oceanfs_storage_api::error::Result<Vec<std::path::PathBuf>> {
            Ok(Vec::new())
        }
    }

    fn registry() -> Arc<SegmentLifecycleRegistry> {
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()))
    }

    #[test]
    fn executed_summary_aggregates_counts() {
        let results = vec![
            ScrubResult {
                segment_id: SegmentId::new(),
                healthy: true,
                corrupt_shard_indices: Vec::new(),
                merkle_mismatch: false,
                bytes_scanned: 10,
                enqueued_heal: false,
                skipped: false,
            },
            ScrubResult {
                segment_id: SegmentId::new(),
                healthy: false,
                corrupt_shard_indices: vec![0],
                merkle_mismatch: true,
                bytes_scanned: 20,
                enqueued_heal: true,
                skipped: false,
            },
            ScrubResult {
                segment_id: SegmentId::new(),
                healthy: true,
                corrupt_shard_indices: Vec::new(),
                merkle_mismatch: false,
                bytes_scanned: 0,
                enqueued_heal: false,
                skipped: true,
            },
        ];
        let summary = ExecutedPartitionSummary::from_results(&results);
        assert_eq!(summary.segments_total, 3);
        assert_eq!(summary.segments_healthy, 1);
        assert_eq!(summary.segments_corrupt, 1);
        assert_eq!(summary.segments_healed, 1);
        assert_eq!(summary.bytes_scanned, 30);
    }

    /// f3 DoD: assign_partition actually executes. A corrupt segment in
    /// the receiving worker's registry is scanned (heal enqueued via the
    /// global heal queue when initialized / result recorded) and the ack
    /// is truthful. This test asserts the executed summary records the
    /// corrupt segment.
    #[tokio::test]
    async fn assign_partition_executes_a_real_scrub() {
        let reg = registry();
        // Seed one sealed segment with corrupt data (correct root in
        // metadata, flipped byte on disk).
        let seg_id = SegmentId::new();
        let correct = vec![0xABu8; 65536];
        let root = crate::anti_entropy::MerkleTree::build(&correct, 0).expect("tree").root().hash();
        let mut corrupt = correct.clone();
        corrupt[100] ^= 0xFF;
        let store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegmentStore::new());
        store.write_segment_data(&seg_id, &corrupt).await.expect("write");

        let meta = oceanfs_core::SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: Some(root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };
        reg.reserve(seg_id, meta.clone()).expect("reserve");
        reg.seal(seg_id, meta).expect("seal");

        let service = ScrubGrpcService::new(reg, store);
        let request = Request::new(AssignPartitionRequest {
            coordinator_id: None,
            segment_ids: vec![seg_id.into()],
        });
        let response = service.assign_partition(request).await.expect("assign must succeed");

        assert!(response.into_inner().accepted, "ack is truthful after execution");
        let summary = service.last_result().expect("a partition was executed");
        assert_eq!(summary.segments_total, 1);
        assert_eq!(summary.segments_corrupt, 1, "the corrupt segment was actually scanned");
        assert!(summary.bytes_scanned > 0);
    }

    /// f3 DoD: an unavailable data store returns an error Status, never a
    /// silent ack.
    #[tokio::test]
    async fn assign_partition_rejects_unavailable_store() {
        let reg = registry();
        let seg_id = SegmentId::new();
        let meta = oceanfs_core::SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0u8; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };
        reg.reserve(seg_id, meta.clone()).expect("reserve");
        reg.seal(seg_id, meta).expect("seal");

        let service = ScrubGrpcService::new(reg, Arc::new(UnavailableStore));
        let request = Request::new(AssignPartitionRequest {
            coordinator_id: None,
            segment_ids: vec![seg_id.into()],
        });
        let result = service.assign_partition(request).await;
        assert!(result.is_err(), "store failure must surface as an error Status");
        assert!(service.last_result().is_none(), "nothing was recorded as executed");
    }

    /// An empty assignment is trivially executed (truthful ack).
    #[tokio::test]
    async fn assign_partition_empty_assignment_acks() {
        let service = ScrubGrpcService::new(registry(), Arc::new(InMemorySegmentStore::new()));
        let request =
            Request::new(AssignPartitionRequest { coordinator_id: None, segment_ids: Vec::new() });
        let response = service.assign_partition(request).await.expect("empty is accepted");
        assert!(response.into_inner().accepted);
    }
}
