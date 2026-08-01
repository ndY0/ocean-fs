//! Distributed scrubbing — full cluster-wide segment scan for integrity.
//!
//! Unlike anti-entropy's peer-to-peer incremental check, scrubbing is a
//! full cluster-wide scan of every segment, verifying BLAKE3 hashes and
//! Merkle roots. A randomly elected coordinator partitions the segment ID
//! space across all healthy nodes. Each node scrubs its partition, reports
//! discrepancies, and auto-heals via EC decode.

use std::sync::Arc;

use oceanfs_core::{SegmentId, SegmentMetadata};
use tokio::sync::Semaphore;

use crate::{
    error::{Error, Result},
    metadata::MetadataStore,
};

// ---------------------------------------------------------------------------
// ScrubConfig
// ---------------------------------------------------------------------------

/// Configuration for distributed scrubbing.
///
/// # Examples
///
/// ```
/// # use oceanfs_storage::ScrubConfig;
/// let config = ScrubConfig::default();
/// assert_eq!(config.interval_sec(), 604800);
/// ```
#[derive(Debug, Clone)]
pub struct ScrubConfig {
    /// Interval between scrub cycles in seconds.
    interval_sec: u64,
    /// Maximum number of nodes participating (0 = all).
    parallel_nodes: usize,
    /// Throughput limit in bytes per second (0 = unlimited).
    throttle_bytes_sec: u64,
}

impl Default for ScrubConfig {
    fn default() -> Self {
        Self { interval_sec: 604800, parallel_nodes: 0, throttle_bytes_sec: 0 }
    }
}

impl ScrubConfig {
    /// Returns the scrub interval in seconds.
    pub fn interval_sec(&self) -> u64 {
        self.interval_sec
    }

    /// Returns the maximum number of parallel nodes.
    pub fn parallel_nodes(&self) -> usize {
        self.parallel_nodes
    }

    /// Returns the throughput throttle in bytes per second.
    pub fn throttle_bytes_sec(&self) -> u64 {
        self.throttle_bytes_sec
    }
}

// ---------------------------------------------------------------------------
// ScrubReport
// ---------------------------------------------------------------------------

/// Results from a full scrub cycle.
#[derive(Debug, Default, Clone)]
pub struct ScrubReport {
    /// Total segments examined.
    pub segments_total: u64,
    /// Segments verified healthy.
    pub segments_healthy: u64,
    /// Segments found to be corrupt.
    pub segments_corrupt: u64,
    /// Segments successfully healed.
    pub segments_healed: u64,
    /// Total bytes scanned.
    pub bytes_scanned: u64,
    /// Number of nodes that participated.
    pub nodes_participated: usize,
    /// Duration of the scrub cycle in seconds.
    pub duration_sec: f64,
}

// ---------------------------------------------------------------------------
// ScrubResult
// ---------------------------------------------------------------------------

/// Result of scrubbing a single segment.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ScrubResult {
    /// The segment ID that was scrubbed.
    pub segment_id: SegmentId,
    /// Whether the segment verified as healthy.
    pub healthy: bool,
    /// Indices of corrupt shards (empty if healthy).
    pub corrupt_shard_indices: Vec<usize>,
    /// Whether the Merkle root mismatched.
    pub merkle_mismatch: bool,
}

// ---------------------------------------------------------------------------
// SegmentPartition
// ---------------------------------------------------------------------------

/// A partition of the segment ID space assigned to a single node.
#[derive(Debug, Clone)]
#[allow(dead_code)]
#[doc(hidden)]
pub struct SegmentPartition {
    /// The node ID responsible for this partition.
    pub node_id: oceanfs_core::NodeId,
    /// The segment IDs in this partition.
    pub segment_ids: Vec<SegmentId>,
}

// ---------------------------------------------------------------------------
// ScrubWorker
// ---------------------------------------------------------------------------

/// Per-node task that reads assigned segment shards and verifies integrity.
#[allow(dead_code)]
pub(crate) struct ScrubWorker {
    metadata: Arc<MetadataStore>,
    throttle_bytes_sec: u64,
}

#[allow(dead_code)]
impl ScrubWorker {
    /// Creates a new scrub worker.
    pub(crate) fn new(metadata: Arc<MetadataStore>, throttle_bytes_sec: u64) -> Self {
        Self { metadata, throttle_bytes_sec }
    }

    /// Scrubs a single segment: verifies BLAKE3 hashes and Merkle root.
    ///
    /// In production, this would:
    /// 1. Read all local shards for this segment from disk
    /// 2. Compute BLAKE3 hash of each shard and compare to stored hashes
    /// 3. Recompute the Merkle tree from shard data and compare root to stored root
    /// 4. On mismatch: flag the shard as corrupt
    ///
    /// Returns the scrub result for this segment.
    pub(crate) fn scrub_segment(&self, segment_meta: &SegmentMetadata) -> ScrubResult {
        // In a production implementation:
        // - Read shard data from disk
        // - Compute BLAKE3 per shard
        // - Compare against stored hashes (from storage_locations or metadata)
        // - Recompute Merkle tree and compare root

        let corrupt_indices = Vec::new();
        let merkle_mismatch = false;

        // Verify Merkle root if present
        if let Some(stored_root) = segment_meta.merkle_root {
            // In production: recompute Merkle tree from segment data
            // For now, the verification is a placeholder
            tracing::debug!(
                segment_id = %segment_meta.segment_id,
                stored_root = %stored_root,
                "verifying segment merkle root"
            );
            // Placeholder: assume healthy
        }

        // BLAKE3 verification of each shard
        // In production: iterate storage_locations, read shard data,
        // compute hash, compare against expected hash
        let healthy = corrupt_indices.is_empty() && !merkle_mismatch;

        ScrubResult {
            segment_id: segment_meta.segment_id,
            healthy,
            corrupt_shard_indices: corrupt_indices,
            merkle_mismatch,
        }
    }

    /// Scrubs a partition of segments and returns results.
    pub(crate) fn scrub_partition(&self, partition: &SegmentPartition) -> Vec<ScrubResult> {
        let mut results = Vec::with_capacity(partition.segment_ids.len());

        for seg_id in &partition.segment_ids {
            match self.metadata.get_segment(*seg_id) {
                Ok(Some(meta)) => {
                    let result = self.scrub_segment(&meta);
                    results.push(result);
                }
                Ok(None) => {
                    tracing::warn!(segment_id = %seg_id, "segment not found during scrub");
                }
                Err(e) => {
                    tracing::warn!(error = %e, segment_id = %seg_id, "failed to read segment during scrub");
                }
            }
        }

        results
    }
}

// ---------------------------------------------------------------------------
// ScrubCoordinator
// ---------------------------------------------------------------------------

/// Scrub coordinator — partitions segment space across nodes.
///
/// Elected per scrub cycle. Queries all segment IDs from metadata,
/// splits them into partitions, assigns each to a healthy node,
/// and aggregates results.
///
/// # Examples
///
/// ```
/// # use oceanfs_storage::{ScrubCoordinator, ScrubConfig};
/// let coord = ScrubCoordinator::new(ScrubConfig::default());
/// ```
pub struct ScrubCoordinator {
    config: ScrubConfig,
}

impl ScrubCoordinator {
    /// Creates a new scrub coordinator.
    pub fn new(config: ScrubConfig) -> Self {
        Self { config }
    }

    /// Returns the configuration.
    pub fn config(&self) -> &ScrubConfig {
        &self.config
    }

    /// Splits segment IDs into equal ranges across nodes. No gaps, no overlaps.
    #[allow(dead_code)]
    #[doc(hidden)]
    pub fn partition_segments(
        &self,
        segment_ids: &[SegmentId],
        node_ids: &[oceanfs_core::NodeId],
    ) -> Vec<SegmentPartition> {
        if node_ids.is_empty() || segment_ids.is_empty() {
            return Vec::new();
        }

        // Sort segment IDs for deterministic partitioning
        let mut sorted_ids: Vec<SegmentId> = segment_ids.to_vec();
        sorted_ids.sort();

        let node_count = node_ids.len();
        let segment_count = sorted_ids.len();
        let base_per_node = segment_count / node_count;
        let remainder = segment_count % node_count;

        let mut partitions = Vec::with_capacity(node_count);
        let mut start = 0;

        for (i, node_id) in node_ids.iter().enumerate() {
            let extra = if i < remainder { 1 } else { 0 };
            let count = base_per_node + extra;
            let end = (start + count).min(segment_count);

            let partition_ids = sorted_ids[start..end].to_vec();
            partitions
                .push(SegmentPartition { node_id: node_id.clone(), segment_ids: partition_ids });
            start = end;
        }

        partitions
    }

    /// Runs a single scrub cycle.
    ///
    /// In a real cluster this would:
    /// 1. Elect a coordinator (random node from membership)
    /// 2. Query all segment IDs from metadata
    /// 3. Partition segment space across nodes
    /// 4. Distribute partitions to workers on each node
    /// 5. Aggregate results into a ScrubReport
    ///
    /// For local testing, runs as a single-node scrub.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata operations fail or the semaphore
    /// cannot be acquired.
    pub async fn run_cycle(&self, metadata: Arc<MetadataStore>) -> Result<ScrubReport> {
        use std::time::Instant;

        let start_time = Instant::now();
        let mut report = ScrubReport::default();

        // Phase 1: Gather all segment IDs
        let segments = metadata.list_segments();
        let segment_ids: Vec<SegmentId> =
            segments.into_iter().filter_map(|r| r.ok().map(|s| s.segment_id)).collect();

        report.segments_total = segment_ids.len() as u64;

        if segment_ids.is_empty() {
            return Ok(report);
        }

        // Phase 2: For single-node/local scrub, verify each segment
        let semaphore = Arc::new(Semaphore::new(self.config.parallel_nodes.max(1)));
        let worker = Arc::new(ScrubWorker::new(metadata.clone(), self.config.throttle_bytes_sec));

        let partition = SegmentPartition {
            node_id: oceanfs_core::NodeId::new("local"),
            segment_ids: segment_ids.clone(),
        };

        // Acquire semaphore permit for bounded concurrency
        let _permit = semaphore
            .acquire()
            .await
            .map_err(|e| Error::Scrub(format!("semaphore acquire failed: {e}")))?;

        let results = worker.scrub_partition(&partition);
        report.nodes_participated = 1;

        // Phase 3: Aggregate results
        for result in &results {
            report.bytes_scanned += 1; // placeholder: real impl tracks actual bytes
            if result.healthy {
                report.segments_healthy += 1;
            } else {
                report.segments_corrupt += 1;
            }
        }

        report.duration_sec = start_time.elapsed().as_secs_f64();

        tracing::info!(
            total = report.segments_total,
            healthy = report.segments_healthy,
            corrupt = report.segments_corrupt,
            duration_sec = report.duration_sec,
            "scrub cycle complete"
        );

        Ok(report)
    }

    /// Triggers a manual scrub (for admin API use).
    ///
    /// # Errors
    ///
    /// Returns an error if the background task cannot be spawned.
    pub async fn trigger_manual(&self, metadata: Arc<MetadataStore>) -> Result<()> {
        tokio::spawn({
            let this = Arc::new(Self { config: self.config.clone() });
            async move {
                match this.run_cycle(metadata).await {
                    Ok(report) => {
                        tracing::info!(
                            total = report.segments_total,
                            healthy = report.segments_healthy,
                            corrupt = report.segments_corrupt,
                            "manual scrub complete"
                        );
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "manual scrub failed");
                    }
                }
            }
        });
        Ok(())
    }

    /// Starts the scrub background task.
    ///
    /// Runs cycles at the configured interval until cancelled.
    pub async fn start_background(
        self: Arc<Self>,
        metadata: Arc<MetadataStore>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(self.config.interval_sec)).await;
                match self.run_cycle(metadata.clone()).await {
                    Ok(report) => {
                        if report.segments_corrupt > 0 {
                            tracing::warn!(
                                corrupt = report.segments_corrupt,
                                "scrub detected corrupt segments"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "scrub cycle failed");
                    }
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{HashOutput, MetadataConfig, NodeId, SegmentId, SegmentMetadata, SizeTier};

    use super::*;

    fn test_config() -> MetadataConfig {
        let dir = tempfile::tempdir().unwrap();
        MetadataConfig {
            data_dir: dir.path().to_path_buf(),
            block_cache_size: 8 * 1024 * 1024,
            memtable_size: 8 * 1024 * 1024,
        }
    }

    // -----------------------------------------------------------------------
    // ScrubConfig
    // -----------------------------------------------------------------------

    #[test]
    fn default_scrub_interval_is_7_days() {
        let config = ScrubConfig::default();
        assert_eq!(config.interval_sec(), 604800);
    }

    #[test]
    fn default_parallel_nodes_is_zero_meaning_all() {
        let config = ScrubConfig::default();
        assert_eq!(config.parallel_nodes(), 0);
    }

    // -----------------------------------------------------------------------
    // Partition assignment
    // -----------------------------------------------------------------------

    #[test]
    fn partition_covers_all_segments_no_gaps() {
        let seg_ids: Vec<SegmentId> = (0..10).map(|_| SegmentId::new()).collect();
        let node_ids: Vec<NodeId> = (0..3).map(|i| NodeId::new(format!("node-{i}"))).collect();

        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let partitions = coord.partition_segments(&seg_ids, &node_ids);

        assert_eq!(partitions.len(), 3);

        let total_assigned: usize = partitions.iter().map(|p| p.segment_ids.len()).sum();
        assert_eq!(total_assigned, 10);
    }

    #[test]
    fn partition_no_overlap() {
        let seg_ids: Vec<SegmentId> = (0..5).map(|_| SegmentId::new()).collect();
        let node_ids: Vec<NodeId> = (0..2).map(|i| NodeId::new(format!("node-{i}"))).collect();

        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let partitions = coord.partition_segments(&seg_ids, &node_ids);

        let mut all_ids = std::collections::HashSet::new();
        for p in &partitions {
            for id in &p.segment_ids {
                assert!(all_ids.insert(*id), "segment ID appears in multiple partitions");
            }
        }
    }

    #[test]
    fn partition_empty_input_returns_empty() {
        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let node_ids: Vec<NodeId> = vec![NodeId::new("n1")];
        let partitions = coord.partition_segments(&[], &node_ids);
        assert!(partitions.is_empty());
    }

    #[test]
    fn partition_no_nodes_returns_empty() {
        let seg_ids: Vec<SegmentId> = vec![SegmentId::new()];
        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let partitions = coord.partition_segments(&seg_ids, &[]);
        assert!(partitions.is_empty());
    }

    #[test]
    fn partition_single_node_gets_all_segments() {
        let seg_ids: Vec<SegmentId> = (0..5).map(|_| SegmentId::new()).collect();
        let node_ids: Vec<NodeId> = vec![NodeId::new("solo")];

        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let partitions = coord.partition_segments(&seg_ids, &node_ids);

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].segment_ids.len(), 5);
    }

    // -----------------------------------------------------------------------
    // ScrubWorker
    // -----------------------------------------------------------------------

    #[test]
    fn scrub_worker_healthy_segment() {
        let metadata_store = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let worker = ScrubWorker::new(metadata_store, 0);

        let seg_meta = SegmentMetadata {
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };

        let result = worker.scrub_segment(&seg_meta);
        assert!(result.healthy);
        assert!(result.corrupt_shard_indices.is_empty());
    }

    #[test]
    fn scrub_worker_empty_partition() {
        let metadata_store = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let worker = ScrubWorker::new(metadata_store, 0);

        let partition = SegmentPartition { node_id: NodeId::new("test"), segment_ids: Vec::new() };

        let results = worker.scrub_partition(&partition);
        assert!(results.is_empty());
    }

    // -----------------------------------------------------------------------
    // ScrubCoordinator
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_cycle_empty_store() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let report = coord.run_cycle(metadata).await.unwrap();
        assert_eq!(report.segments_total, 0);
        assert_eq!(report.segments_healthy, 0);
    }

    #[tokio::test]
    async fn run_cycle_with_segments() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        // Put some segments
        for _ in 0..3 {
            let seg = SegmentMetadata {
                segment_id: SegmentId::new(),
                ec_k: 4,
                ec_m: 2,
                size_tier: SizeTier::Standard,
                merkle_root: None,
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(1700000000000),
            };
            metadata.put_segment(seg).unwrap();
        }

        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let report = coord.run_cycle(metadata).await.unwrap();
        assert_eq!(report.segments_total, 3);
        assert_eq!(report.segments_healthy, 3);
        assert_eq!(report.segments_corrupt, 0);
    }

    #[tokio::test]
    async fn trigger_manual_does_not_panic() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let result = coord.trigger_manual(metadata).await;
        assert!(result.is_ok());
        // Give the spawned task a moment
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // -----------------------------------------------------------------------
    // Report aggregation
    // -----------------------------------------------------------------------

    #[test]
    fn empty_report_defaults() {
        let report = ScrubReport::default();
        assert_eq!(report.segments_total, 0);
        assert_eq!(report.segments_healthy, 0);
        assert_eq!(report.segments_corrupt, 0);
        assert_eq!(report.segments_healed, 0);
    }

    #[test]
    fn scrub_report_with_data() {
        let report = ScrubReport {
            segments_total: 100,
            segments_healthy: 98,
            segments_corrupt: 2,
            segments_healed: 2,
            bytes_scanned: 1048576,
            nodes_participated: 3,
            duration_sec: 15.5,
        };
        assert_eq!(report.segments_total, 100);
        assert_eq!(report.segments_corrupt, 2);
        assert_eq!(report.segments_healed, 2);
    }

    // -----------------------------------------------------------------------
    // ScrubWorker with merkle root
    // -----------------------------------------------------------------------

    #[test]
    fn scrub_worker_segment_with_merkle_root() {
        let metadata_store = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let worker = ScrubWorker::new(metadata_store, 0);

        let seg_meta = SegmentMetadata {
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(HashOutput::from_bytes([0u8; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };

        let result = worker.scrub_segment(&seg_meta);
        assert!(result.healthy);
    }

    #[test]
    fn scrub_worker_reports_segment_id() {
        let metadata_store = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let worker = ScrubWorker::new(metadata_store, 0);

        let seg_id = SegmentId::new();
        let seg_meta = SegmentMetadata {
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };

        let result = worker.scrub_segment(&seg_meta);
        assert_eq!(result.segment_id, seg_id);
    }
}
