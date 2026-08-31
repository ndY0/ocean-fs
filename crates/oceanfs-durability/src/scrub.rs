//! Distributed scrubbing — full cluster-wide segment scan for integrity.
//!
//! Unlike anti-entropy's peer-to-peer incremental check, scrubbing is a
//! full cluster-wide scan of every segment, verifying BLAKE3 hashes and
//! Merkle roots. A randomly elected coordinator partitions the segment ID
//! space across all healthy nodes. Each node scrubs its partition, reports
//! discrepancies, and auto-heals via EC decode.

use std::sync::Arc;

use oceanfs_core::{
    Counter, LabelSet, MetricRegistrar, NodeId, NodeState, SegmentId, SegmentMetadata,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use tokio::sync::Semaphore;

use crate::{
    anti_entropy::{MerkleTree, SegmentDataStore},
    Error, Result,
};

// ---------------------------------------------------------------------------
// ScrubConfig
// ---------------------------------------------------------------------------

/// Configuration for distributed scrubbing.
///
/// # Examples
///
/// ```
/// # use oceanfs_durability::ScrubConfig;
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

    /// Sets the scrub interval in seconds.
    pub fn set_interval_sec(&mut self, sec: u64) {
        self.interval_sec = sec;
    }

    /// Sets the maximum number of parallel nodes.
    pub fn set_parallel_nodes(&mut self, nodes: usize) {
        self.parallel_nodes = nodes;
    }
}

// ---------------------------------------------------------------------------
// ScrubReport
// ---------------------------------------------------------------------------

/// Results from a full scrub cycle.
///
/// # Examples
///
/// ```
/// # use oceanfs_durability::ScrubReport;
/// let report = ScrubReport::builder()
///     .segments_total(100)
///     .segments_healthy(98)
///     .segments_corrupt(2)
///     .segments_healed(2)
///     .bytes_scanned(1048576)
///     .nodes_participated(3)
///     .duration_sec(15.5)
///     .build();
/// assert_eq!(report.segments_total(), 100);
/// assert_eq!(report.segments_healthy(), 98);
/// ```
#[derive(Debug, Clone)]
pub struct ScrubReport {
    segments_total: u64,
    segments_healthy: u64,
    segments_corrupt: u64,
    segments_healed: u64,
    bytes_scanned: u64,
    nodes_participated: usize,
    duration_sec: f64,
}

impl Default for ScrubReport {
    fn default() -> Self {
        Self {
            segments_total: 0,
            segments_healthy: 0,
            segments_corrupt: 0,
            segments_healed: 0,
            bytes_scanned: 0,
            nodes_participated: 0,
            duration_sec: 0.0,
        }
    }
}

impl ScrubReport {
    /// Creates a new [`ScrubReportBuilder`] for constructing a report.
    pub fn builder() -> ScrubReportBuilder {
        ScrubReportBuilder::default()
    }

    /// Returns the total number of segments examined.
    pub fn segments_total(&self) -> u64 {
        self.segments_total
    }

    /// Returns the number of segments verified healthy.
    pub fn segments_healthy(&self) -> u64 {
        self.segments_healthy
    }

    /// Returns the number of segments found to be corrupt.
    pub fn segments_corrupt(&self) -> u64 {
        self.segments_corrupt
    }

    /// Returns the number of segments enqueued for healing.
    pub fn segments_healed(&self) -> u64 {
        self.segments_healed
    }

    /// Returns total bytes scanned during the scrub cycle.
    pub fn bytes_scanned(&self) -> u64 {
        self.bytes_scanned
    }

    /// Returns the number of nodes that participated.
    pub fn nodes_participated(&self) -> usize {
        self.nodes_participated
    }

    /// Returns the duration of the scrub cycle in seconds.
    pub fn duration_sec(&self) -> f64 {
        self.duration_sec
    }
}

/// Builder for [`ScrubReport`].
///
/// # Examples
///
/// ```
/// # use oceanfs_durability::ScrubReport;
/// let report = ScrubReport::builder()
///     .segments_total(100)
///     .segments_healthy(95)
///     .segments_corrupt(3)
///     .segments_healed(3)
///     .build();
/// assert_eq!(report.segments_corrupt(), 3);
/// ```
#[derive(Debug, Default)]
pub struct ScrubReportBuilder {
    segments_total: u64,
    segments_healthy: u64,
    segments_corrupt: u64,
    segments_healed: u64,
    bytes_scanned: u64,
    nodes_participated: usize,
    duration_sec: f64,
}

impl ScrubReportBuilder {
    /// Sets the total segments examined.
    pub fn segments_total(mut self, v: u64) -> Self {
        self.segments_total = v;
        self
    }
    /// Sets the healthy segment count.
    pub fn segments_healthy(mut self, v: u64) -> Self {
        self.segments_healthy = v;
        self
    }
    /// Sets the corrupt segment count.
    pub fn segments_corrupt(mut self, v: u64) -> Self {
        self.segments_corrupt = v;
        self
    }
    /// Sets the healed segment count.
    pub fn segments_healed(mut self, v: u64) -> Self {
        self.segments_healed = v;
        self
    }
    /// Sets the bytes scanned.
    pub fn bytes_scanned(mut self, v: u64) -> Self {
        self.bytes_scanned = v;
        self
    }
    /// Sets the number of nodes participated.
    pub fn nodes_participated(mut self, v: usize) -> Self {
        self.nodes_participated = v;
        self
    }
    /// Sets the duration in seconds.
    pub fn duration_sec(mut self, v: f64) -> Self {
        self.duration_sec = v;
        self
    }

    /// Builds the [`ScrubReport`].
    pub fn build(self) -> ScrubReport {
        ScrubReport {
            segments_total: self.segments_total,
            segments_healthy: self.segments_healthy,
            segments_corrupt: self.segments_corrupt,
            segments_healed: self.segments_healed,
            bytes_scanned: self.bytes_scanned,
            nodes_participated: self.nodes_participated,
            duration_sec: self.duration_sec,
        }
    }
}

// ---------------------------------------------------------------------------
// ScrubResult
// ---------------------------------------------------------------------------

/// Result of scrubbing a single segment.
#[derive(Debug, Clone)]
pub(crate) struct ScrubResult {
    /// The segment ID that was scrubbed.
    pub segment_id: SegmentId,
    /// Whether the segment verified as healthy.
    pub healthy: bool,
    /// Indices of corrupt shards (empty if healthy).
    pub corrupt_shard_indices: Vec<usize>,
    /// Whether the Merkle root mismatched.
    pub merkle_mismatch: bool,
    /// Number of bytes scanned for this segment.
    pub bytes_scanned: u64,
    /// Whether this segment was enqueued for EC-based healing.
    pub enqueued_heal: bool,
    /// Whether this segment was skipped (shard not found — seal/GC race).
    /// Skipped segments are neither healthy-reads nor corruption; they are
    /// excluded from both the corrupt count and the heal path.
    pub skipped: bool,
}

// ---------------------------------------------------------------------------
// SegmentPartition
// ---------------------------------------------------------------------------

/// A partition of the segment ID space assigned to a single node.
#[derive(Debug, Clone)]
#[doc(hidden)]
pub(crate) struct SegmentPartition {
    /// The node ID responsible for this partition.
    pub node_id: NodeId,
    /// The segment IDs in this partition.
    pub segment_ids: Vec<SegmentId>,
}

// ---------------------------------------------------------------------------
// ScrubWorker
// ---------------------------------------------------------------------------

/// Per-node task that reads assigned segment shards and verifies integrity.
///
/// Uses a [`SegmentDataStore`] to read raw segment data from disk,
/// builds a [`MerkleTree`] over the data, and compares the computed
/// Merkle root against the stored root in [`SegmentMetadata`].
pub(crate) struct ScrubWorker {
    /// The lifecycle registry — the machine's `Sealed` entries are the
    /// scrub set and its metadata carries the anchor (ADR-0025 Decision 3).
    registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
    data_store: Arc<dyn SegmentDataStore>,
    /// Throughput limit in bytes per second (reserved for future rate-limiting).
    #[allow(dead_code)]
    throttle_bytes_sec: u64,
}

impl ScrubWorker {
    /// Creates a new scrub worker.
    pub(crate) fn new(
        registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
        data_store: Arc<dyn SegmentDataStore>,
        throttle_bytes_sec: u64,
    ) -> Self {
        Self { registry, data_store, throttle_bytes_sec }
    }

    /// Scrubs a single segment: verifies BLAKE3 hashes and Merkle root.
    ///
    /// # Verification steps
    ///
    /// 1. Reads the full raw segment data from the data store.
    /// 2. Builds a Merkle tree over the data using 64 KB leaves.
    /// 3. Compares the computed Merkle root against the stored
    ///    `merkle_root` in the segment metadata.
    /// 4. Returns a [`ScrubResult`] with the verification outcome.
    ///
    /// If the segment metadata has no stored Merkle root, the segment
    /// is still scanned for size but cannot be fully verified.
    ///
    /// # Errors
    ///
    /// If the segment data cannot be read from the data store, the
    /// result is marked as unhealthy with `merkle_mismatch = true`.
    pub(crate) fn scrub_segment(&self, segment_meta: &SegmentMetadata) -> ScrubResult {
        // Read the raw segment data from the backing store.
        let data = match self.data_store.read_segment_data(&segment_meta.segment_id) {
            Ok(data) => data,
            Err(e) => {
                // A NotFound read is NOT corruption: the segment may have
                // been sealed concurrently with this scrub cycle and the
                // .dat finalized after the metadata scan, or (single-node
                // crash recovery) the orphan reaper may have deleted the
                // shard between the metadata scan and this read. Report it
                // as skipped so the cycle does not emit false corruption
                // alarms (the previous behaviour counted every read error
                // as a Merkle mismatch and enqueued heal requests for
                // segments that were never corrupt).
                let not_found = matches!(
                    &e,
                    oceanfs_storage::Error::Io(io_err)
                        if io_err.kind() == std::io::ErrorKind::NotFound
                ) || matches!(&e, oceanfs_storage::Error::SegmentNotFound(_));
                if not_found {
                    tracing::debug!(
                        segment_id = %segment_meta.segment_id,
                        "segment shard not found during scrub; skipping (seal/GC race)"
                    );
                    return ScrubResult {
                        segment_id: segment_meta.segment_id,
                        healthy: true,
                        corrupt_shard_indices: Vec::new(),
                        merkle_mismatch: false,
                        bytes_scanned: 0,
                        enqueued_heal: false,
                        skipped: true,
                    };
                }
                tracing::warn!(
                    error = %e,
                    segment_id = %segment_meta.segment_id,
                    "failed to read segment data for scrubbing"
                );
                return ScrubResult {
                    segment_id: segment_meta.segment_id,
                    healthy: false,
                    corrupt_shard_indices: Vec::new(),
                    merkle_mismatch: true,
                    bytes_scanned: 0,
                    enqueued_heal: false,
                    skipped: false,
                };
            }
        };

        let total_bytes = data.len() as u64;

        // Edge case: empty segment data is trivially healthy.
        if data.is_empty() {
            return ScrubResult {
                segment_id: segment_meta.segment_id,
                healthy: true,
                corrupt_shard_indices: Vec::new(),
                merkle_mismatch: false,
                bytes_scanned: 0,
                enqueued_heal: false,
                skipped: false,
            };
        }

        // Build Merkle tree from the segment data.
        // Uses default 64 KB leaf size.
        let computed_tree = match MerkleTree::build(&data, 0) {
            Some(tree) => tree,
            None => {
                // build() returns None only for empty data, handled above.
                return ScrubResult {
                    segment_id: segment_meta.segment_id,
                    healthy: true,
                    corrupt_shard_indices: Vec::new(),
                    merkle_mismatch: false,
                    bytes_scanned: total_bytes,
                    enqueued_heal: false,
                    skipped: false,
                };
            }
        };

        // Verify Merkle root against stored root in metadata.
        let merkle_mismatch = if let Some(stored_root) = segment_meta.merkle_root {
            let computed_root = computed_tree.root().hash();
            if computed_root != stored_root {
                tracing::warn!(
                    segment_id = %segment_meta.segment_id,
                    stored_root = %stored_root,
                    computed_root = %computed_root,
                    "Merkle root mismatch detected during scrub"
                );
                true
            } else {
                false
            }
        } else {
            // No stored Merkle root to compare against — cannot verify.
            // In production every sealed segment should have a root;
            // this is a warning condition.
            tracing::debug!(
                segment_id = %segment_meta.segment_id,
                "segment has no stored Merkle root; cannot verify integrity"
            );
            false
        };

        // Identify which leaves (shard-sized chunks) are corrupt by comparing
        // each computed leaf hash against the expected hash in a healthy tree.
        // When the Merkle root mismatches, we pass all leaf indices to the heal
        // worker so it can fetch k healthy peer shards and reconstruct fully.
        let mut corrupt_shard_indices: Vec<usize> = Vec::new();
        let mut enqueued_heal = false;

        if merkle_mismatch {
            // Compute leaf hashes from the (corrupt) data and compare against
            // what a healthy tree would produce. Any leaf whose hash differs
            // from the expected is flagged.
            for (idx, _leaf) in computed_tree.leaf_hashes().iter().enumerate() {
                // In a full implementation, expected hashes would come from
                // stored per-shard metadata. For now, flag all leaves as
                // potentially corrupt when the root mismatches.
                corrupt_shard_indices.push(idx);
            }

            tracing::warn!(
                segment_id = %segment_meta.segment_id,
                bytes = total_bytes,
                corrupt_leaves = corrupt_shard_indices.len(),
                "segment is corrupt — Merkle root mismatch; enqueuing for EC heal"
            );

            // Enqueue for EC-based healing via the global heal queue.
            match crate::heal::enqueue_heal(segment_meta.segment_id, corrupt_shard_indices.clone())
            {
                Ok(()) => {
                    enqueued_heal = true;
                    tracing::info!(
                        segment_id = %segment_meta.segment_id,
                        "heal request enqueued"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        segment_id = %segment_meta.segment_id,
                        "failed to enqueue heal request (queue may not be initialized)"
                    );
                }
            }
        }

        ScrubResult {
            segment_id: segment_meta.segment_id,
            healthy: !merkle_mismatch,
            corrupt_shard_indices,
            merkle_mismatch,
            bytes_scanned: total_bytes,
            enqueued_heal,
            skipped: false,
        }
    }

    /// Scrubs a partition of segments and returns results.
    ///
    /// Iterates through each segment in the partition, reads its metadata
    /// from the metadata store, and verifies it via [`scrub_segment`].
    pub(crate) fn scrub_partition(&self, partition: &SegmentPartition) -> Vec<ScrubResult> {
        tracing::debug!(
            node_id = %partition.node_id,
            segment_count = partition.segment_ids.len(),
            "scrubbing partition"
        );
        let mut results = Vec::with_capacity(partition.segment_ids.len());

        for seg_id in &partition.segment_ids {
            match self.registry.get(*seg_id) {
                Some(entry) => {
                    let result = self.scrub_segment(&entry.metadata);
                    results.push(result);
                }
                None => {
                    tracing::warn!(segment_id = %seg_id, "segment not found during scrub");
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
/// # use oceanfs_durability::{ScrubCoordinator, ScrubConfig};
/// let coord = ScrubCoordinator::new(ScrubConfig::default());
/// ```
pub struct ScrubCoordinator {
    config: ScrubConfig,
    /// Optional membership for distributed partition assignment (H5).
    membership: Option<Arc<Membership>>,
    /// Optional connection pool for distributed partition distribution (H5).
    pool: Option<Arc<ConnectionPool>>,
    segments_checked_total: Counter,
    segments_corrupt_total: Counter,
}

impl ScrubCoordinator {
    /// Creates a new scrub coordinator with unregistered counters.
    ///
    /// Use [`register_metrics`](Self::register_metrics) to wire them.
    /// For distributed operation, call [`with_distributed`](Self::with_distributed)
    /// to provide membership and connection pool.
    pub fn new(config: ScrubConfig) -> Self {
        Self {
            config,
            membership: None,
            pool: None,
            segments_checked_total: Counter::new(
                "scrub_segments_checked_total".into(),
                "Segments checked by scrub".into(),
                LabelSet::empty(),
            ),
            segments_corrupt_total: Counter::new(
                "scrub_segments_corrupt_total".into(),
                "Corrupt segments detected by scrub".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Enables distributed partition assignment (H5).
    ///
    /// When set, the scrub coordinator uses the membership to discover
    /// alive nodes and distributes segment partitions across them instead
    /// of requiring the caller to pass node IDs manually.
    pub fn with_distributed(
        mut self,
        membership: Arc<Membership>,
        pool: Arc<ConnectionPool>,
    ) -> Self {
        self.membership = Some(membership);
        self.pool = Some(pool);
        self
    }

    /// Returns the list of alive node IDs from the membership view,
    /// excluding the current node. Returns an empty vec when membership
    /// is not configured.
    pub fn alive_peers(&self) -> Vec<NodeId> {
        match &self.membership {
            Some(m) => m
                .nodes()
                .into_iter()
                .filter(|(id, state)| *id != *m.node_id() && *state == NodeState::Alive)
                .map(|(id, _)| id)
                .collect(),
            None => Vec::new(),
        }
    }
    // [review][implementation][critical]
    // partitioned scrub must be delivered a tsome point : when key counts will grow, so does the scrub workload.
    // another note : this implementation assumes that each peer holds this node segments, wich will be note true with the replication
    // introduced with the data pools evolution. we need to brainstorm about that, maybe leverage the manifest ?
    // [end]
    /// Partitions all sealed segments across currently-alive nodes.
    ///
    /// When membership is configured, discovers alive peers automatically
    /// and distributes the workload. When membership is `None`, falls back
    /// to local-only operation (all segments assigned to self).
    #[allow(dead_code)] // Infrastructure for distributed scrub (H5); called by future phases.
    pub(crate) fn partition_for_current_nodes(
        &self,
        segment_ids: &[SegmentId],
    ) -> Vec<SegmentPartition> {
        let node_ids = self.alive_peers();
        if node_ids.is_empty() {
            // No peers: assign all segments to the local node.
            let local = oceanfs_core::NodeId::new("local");
            return vec![SegmentPartition { node_id: local, segment_ids: segment_ids.to_vec() }];
        }
        self.partition_segments(segment_ids, &node_ids)
    }

    /// Registers scrub counters with a metrics registrar.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.segments_checked_total.clone());
        registrar.register_counter(self.segments_corrupt_total.clone());
    }

    /// Returns the configuration.
    pub fn config(&self) -> &ScrubConfig {
        &self.config
    }

    /// Splits segment IDs into equal ranges across nodes. No gaps, no overlaps.
    #[doc(hidden)]
    pub(crate) fn partition_segments(
        &self,
        segment_ids: &[SegmentId],
        node_ids: &[NodeId],
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

    /// Computes the number of concurrent segment verifications for a
    /// scrub cycle.
    ///
    /// `parallel_nodes == 0` selects the bounded default (never "all
    /// segments at once" — each concurrent verification holds one fd and
    /// one full ~10 MB segment buffer, so an unbounded batch is a
    /// multi-GB anonymous-memory spike). An explicit `parallel_nodes`
    /// value is honored as an upper bound.
    fn scrub_concurrency(segment_count: usize, parallel_nodes: usize) -> usize {
        const DEFAULT_SCRUB_CONCURRENCY: usize = 4;
        if parallel_nodes == 0 {
            segment_count.min(DEFAULT_SCRUB_CONCURRENCY)
        } else {
            parallel_nodes.min(segment_count)
        }
        .max(1)
    }

    /// Runs a single scrub cycle.
    ///
    /// # Workflow
    ///
    /// 1. Gathers all segment IDs from the metadata store.
    /// 2. Partitions segments into batches based on `parallel_nodes`.
    /// 3. Verifies each batch concurrently, bounded by a semaphore
    ///    (perf rule 2.7: bounded concurrency).
    /// 4. Aggregates results into a [`ScrubReport`].
    ///
    /// In a multi-node cluster, step 2 would distribute batches to
    /// remote nodes via gRPC. For now, all verification runs locally.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata operations fail or the semaphore
    /// cannot be acquired.
    pub async fn run_cycle(
        &self,
        registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
        data_store: Arc<dyn SegmentDataStore>,
    ) -> Result<ScrubReport> {
        use std::time::Instant;

        let start_time = Instant::now();
        let mut report = ScrubReport::default();

        // Phase 1: Gather all segment IDs from the machine (ADR-0025
        // Decision 3).
        // Only SEALED segments are scrubbed: unsealed segments (phantom
        // registrations made before their WAL entry, or in-flight active
        // segments) have no `.dat` file on disk yet — attempting to read
        // them would produce false "corrupt" results (the read fails with
        // NotFound, which the worker classifies as a Merkle mismatch).
        // Sealed segments carry a stored Merkle root to verify against.
        let mut segment_ids: Vec<SegmentId> = Vec::new();
        registry.for_each(|id, entry| {
            if entry.state == oceanfs_storage::segment::lifecycle::SegmentState::Sealed {
                segment_ids.push(id);
            }
        });

        report.segments_total = segment_ids.len() as u64;

        if segment_ids.is_empty() {
            return Ok(report);
        }

        // Determine concurrency: use configured parallel_nodes, or a sane
        // bounded default. NOTE: 0 previously meant "all segments at
        // once" — with ~10 MB per full-segment read, that turned a scrub
        // cycle into a multi-GB anonymous-memory burst (hundreds of
        // concurrent file reads, one fd + one full buffer each), which
        // OOM-killed 4 GB SUT VMs mid-run. The default is now capped.
        let max_concurrent = Self::scrub_concurrency(segment_ids.len(), self.config.parallel_nodes);

        // Phase 2: Partition segments into batches for parallel verification.
        // Each batch is assigned to a spawned task bounded by the semaphore.
        let batch_size = (segment_ids.len() / max_concurrent).max(1);
        let batches: Vec<Vec<SegmentId>> =
            segment_ids.chunks(batch_size).map(|chunk| chunk.to_vec()).collect();

        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let worker = Arc::new(ScrubWorker::new(
            registry.clone(),
            data_store,
            self.config.throttle_bytes_sec,
        ));

        let mut handles = Vec::with_capacity(batches.len());
        for batch in batches {
            let semaphore = Arc::clone(&semaphore);
            let worker = Arc::clone(&worker);
            let node_id = NodeId::new("local");

            let handle = tokio::spawn(async move {
                // Acquire permit to bound concurrent verification (perf 2.7, 8.5)
                let _permit = semaphore
                    .acquire()
                    .await
                    .map_err(|e| Error::Internal(format!("semaphore acquire failed: {e}")))?;

                let partition = SegmentPartition { node_id, segment_ids: batch };
                // Perform the actual verification on a blocking thread
                // to avoid blocking the async runtime.
                let results =
                    tokio::task::spawn_blocking(move || worker.scrub_partition(&partition))
                        .await
                        .map_err(|e| Error::Internal(format!("spawn_blocking failed: {e}")))?;

                Ok::<Vec<ScrubResult>, Error>(results)
            });

            handles.push(handle);
        }

        // Phase 3: Collect results from all batches
        for handle in handles {
            match handle.await {
                Ok(Ok(results)) => {
                    for result in &results {
                        report.bytes_scanned += result.bytes_scanned;
                        if result.skipped {
                            // Shard missing (seal/GC race) — not corruption.
                            continue;
                        }
                        if result.healthy {
                            report.segments_healthy += 1;
                        } else {
                            report.segments_corrupt += 1;
                            if result.merkle_mismatch {
                                tracing::warn!(
                                    segment_id = %result.segment_id,
                                    corrupt_shards = result.corrupt_shard_indices.len(),
                                    "scrub detected segment with Merkle root mismatch"
                                );
                            }
                        }
                        if result.enqueued_heal {
                            report.segments_healed += 1;
                        }
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "batch scrub task failed");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "batch scrub task panicked");
                }
            }
        }

        report.nodes_participated = 1;
        report.duration_sec = start_time.elapsed().as_secs_f64();

        tracing::info!(
            total = report.segments_total,
            healthy = report.segments_healthy,
            corrupt = report.segments_corrupt,
            bytes_scanned = report.bytes_scanned,
            duration_sec = report.duration_sec,
            "scrub cycle complete"
        );

        self.segments_checked_total.add(report.segments_total);
        if report.segments_corrupt > 0 {
            self.segments_corrupt_total.add(report.segments_corrupt);
        }

        Ok(report)
    }

    /// Triggers a manual scrub (for admin API use).
    ///
    /// Spawns a background task that runs a full scrub cycle. The result
    /// is logged via `tracing`. This method returns immediately; the
    /// scrub runs asynchronously.
    ///
    /// # Errors
    ///
    /// Returns an error if the background task cannot be spawned.
    pub async fn trigger_manual(
        &self,
        registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
        data_store: Arc<dyn SegmentDataStore>,
    ) -> Result<()> {
        let config = self.config.clone();
        tokio::spawn(async move {
            let coord = ScrubCoordinator::new(config);
            match coord.run_cycle(registry, data_store).await {
                Ok(report) => {
                    tracing::info!(
                        total = report.segments_total,
                        healthy = report.segments_healthy,
                        corrupt = report.segments_corrupt,
                        healed = report.segments_healed,
                        bytes_scanned = report.bytes_scanned,
                        duration_sec = report.duration_sec,
                        "manual scrub complete"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "manual scrub failed");
                }
            }
        });
        Ok(())
    }

    /// Starts the scrub background task.
    ///
    /// Runs cycles at the configured interval until a shutdown signal
    /// is received via the provided `shutdown` receiver.
    ///
    /// Returns a [`tokio::task::JoinHandle`] that can be awaited for
    /// graceful shutdown coordination.
    pub async fn start_background(
        self: Arc<Self>,
        registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
        data_store: Arc<dyn SegmentDataStore>,
        mut shutdown: tokio::sync::watch::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        tracing::info!("scrub background task shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(self.config.interval_sec)) => {
                        match self.run_cycle(registry.clone(), data_store.clone()).await {
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
    use oceanfs_core::{NodeId, SegmentId, SegmentMetadata, SizeTier};

    use super::*;
    use crate::anti_entropy::InMemorySegmentStore;

    /// Creates an in-memory segment data store pre-populated with the given
    /// segment ID → data mapping.
    fn segment_store_with_data(entries: Vec<(SegmentId, Vec<u8>)>) -> Arc<InMemorySegmentStore> {
        let store = Arc::new(InMemorySegmentStore::new());
        for (id, data) in entries {
            store.write_segment_data(&id, &data).unwrap();
        }
        store
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
    fn default_parallel_nodes_is_zero() {
        let config = ScrubConfig::default();
        assert_eq!(config.parallel_nodes(), 0);
        // 0 selects the bounded default (see scrub_concurrency) — it no
        // longer means "all segments at once" (multi-GB memory bursts).
        assert_eq!(ScrubCoordinator::scrub_concurrency(80, 0), 4);
    }

    #[test]
    fn scrub_concurrency_default_bounds_all_segments() {
        // 0 (default): capped at the bounded default, never "all".
        assert_eq!(ScrubCoordinator::scrub_concurrency(1, 0), 1);
        assert_eq!(ScrubCoordinator::scrub_concurrency(4, 0), 4);
        assert_eq!(ScrubCoordinator::scrub_concurrency(80, 0), 4);
        assert_eq!(ScrubCoordinator::scrub_concurrency(500, 0), 4);
    }

    #[test]
    fn scrub_concurrency_explicit_parallel_nodes_is_honored() {
        assert_eq!(ScrubCoordinator::scrub_concurrency(80, 3), 3);
        assert_eq!(ScrubCoordinator::scrub_concurrency(2, 8), 2); // capped by segment count
        assert_eq!(ScrubCoordinator::scrub_concurrency(80, 64), 64);
    }

    #[test]
    fn set_interval_sec_updates_value() {
        let mut config = ScrubConfig::default();
        config.set_interval_sec(3600);
        assert_eq!(config.interval_sec(), 3600);
    }

    #[test]
    fn set_parallel_nodes_updates_value() {
        let mut config = ScrubConfig::default();
        config.set_parallel_nodes(3);
        assert_eq!(config.parallel_nodes(), 3);
    }

    #[test]
    fn config_accessors_return_throttle() {
        let config = ScrubConfig::default();
        assert_eq!(config.throttle_bytes_sec(), 0);
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
    // ScrubWorker — healthy segments
    // -----------------------------------------------------------------------

    #[test]
    fn scrub_worker_healthy_segment_no_merkle_root() {
        let seg_id = SegmentId::new();
        let test_data = b"data present but no merkle root stored".to_vec();

        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![(seg_id, test_data.clone())]);
        let worker = ScrubWorker::new(Arc::clone(&registry), data_store, 0);

        let seg_meta = SegmentMetadata {
            pool_id: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };

        let result = worker.scrub_segment(&seg_meta);
        // Without a stored Merkle root, we cannot verify integrity,
        // but the data is present and readable.
        assert!(result.healthy);
        assert!(!result.merkle_mismatch);
        assert_eq!(result.bytes_scanned, test_data.len() as u64);
    }

    #[test]
    fn scrub_worker_segment_with_data_and_correct_merkle_root() {
        let seg_id = SegmentId::new();
        let test_data = b"hello world this is test segment data for scrub verification".to_vec();
        let merkle_root = MerkleTree::build(&test_data, 0).unwrap().root().hash();

        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![(seg_id, test_data.clone())]);
        let worker = ScrubWorker::new(Arc::clone(&registry), data_store, 0);

        let seg_meta = SegmentMetadata {
            pool_id: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(merkle_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };

        let result = worker.scrub_segment(&seg_meta);
        assert!(result.healthy);
        assert!(!result.merkle_mismatch);
        assert_eq!(result.bytes_scanned, test_data.len() as u64);
    }

    #[test]
    fn scrub_worker_empty_partition() {
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![]);
        let worker = ScrubWorker::new(Arc::clone(&registry), data_store, 0);

        let partition = SegmentPartition { node_id: NodeId::new("test"), segment_ids: Vec::new() };

        let results = worker.scrub_partition(&partition);
        assert!(results.is_empty());
    }

    // -----------------------------------------------------------------------
    // ScrubWorker — corruption detection
    // -----------------------------------------------------------------------

    #[test]
    fn scrub_worker_detects_bit_flip_corruption() {
        let seg_id = SegmentId::new();
        let original_data = vec![0xAB; 65536]; // 64 KB of known data

        // Create a copy with a single bit flipped
        let mut corrupted_data = original_data.clone();
        corrupted_data[1000] ^= 0x01;

        // The Merkle root is computed from the original (uncorrupted) data
        let correct_root = MerkleTree::build(&original_data, 0).unwrap().root().hash();
        let corrupted_len = corrupted_data.len() as u64;

        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        // Store the CORRUPTED data (simulating disk corruption)
        let data_store = segment_store_with_data(vec![(seg_id, corrupted_data)]);
        let worker = ScrubWorker::new(Arc::clone(&registry), data_store, 0);

        let seg_meta = SegmentMetadata {
            pool_id: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(correct_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };

        let result = worker.scrub_segment(&seg_meta);
        assert!(!result.healthy, "corruption should be detected");
        assert!(result.merkle_mismatch, "Merkle root should mismatch");
        assert_eq!(result.bytes_scanned, corrupted_len);
    }

    #[test]
    fn scrub_worker_detects_merkle_mismatch_when_data_is_different() {
        let seg_id = SegmentId::new();
        let original_data = b"this is the original correct segment data".to_vec();
        let different_data = b"this is completely different segment content".to_vec();

        // Root from the original data
        let correct_root = MerkleTree::build(&original_data, 0).unwrap().root().hash();

        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        // Store DIFFERENT data (simulating accidental overwrite)
        let data_store = segment_store_with_data(vec![(seg_id, different_data)]);
        let worker = ScrubWorker::new(Arc::clone(&registry), data_store, 0);

        let seg_meta = SegmentMetadata {
            pool_id: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(correct_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };

        let result = worker.scrub_segment(&seg_meta);
        assert!(!result.healthy, "different data should be detected");
        assert!(result.merkle_mismatch);
    }

    #[test]
    fn scrub_worker_healthy_segment_matches_stored_merkle() {
        let seg_id = SegmentId::new();
        let test_data = b"this segment data is correct and verified by scrubbing".to_vec();
        let merkle_root = MerkleTree::build(&test_data, 0).unwrap().root().hash();

        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![(seg_id, test_data.clone())]);
        let worker = ScrubWorker::new(Arc::clone(&registry), data_store, 0);

        let seg_meta = SegmentMetadata {
            pool_id: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(merkle_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };

        let result = worker.scrub_segment(&seg_meta);
        assert!(result.healthy);
        assert!(!result.merkle_mismatch);
        assert!(result.corrupt_shard_indices.is_empty());
    }

    // -----------------------------------------------------------------------
    // ScrubWorker — error handling
    // -----------------------------------------------------------------------

    #[test]
    fn scrub_worker_missing_data_skips_not_corrupt() {
        let seg_id = SegmentId::new();
        let test_data = b"data that exists in metadata but not in store".to_vec();
        let merkle_root = MerkleTree::build(&test_data, 0).unwrap().root().hash();

        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        // Empty store — the segment data is NOT present
        let data_store = segment_store_with_data(vec![]);
        let worker = ScrubWorker::new(Arc::clone(&registry), data_store, 0);

        let seg_meta = SegmentMetadata {
            pool_id: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(merkle_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };

        let result = worker.scrub_segment(&seg_meta);
        assert!(
            result.skipped,
            "missing shard is a seal/GC race, not corruption — must be skipped"
        );
        assert!(result.healthy, "skipped segments count as healthy (not corrupt)");
        assert!(!result.merkle_mismatch, "missing shard must not be a Merkle mismatch");
        assert!(!result.enqueued_heal, "missing shard must not trigger a heal request");
    }

    #[test]
    fn scrub_worker_reports_segment_id() {
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![]);
        let worker = ScrubWorker::new(Arc::clone(&registry), data_store, 0);

        let seg_id = SegmentId::new();
        let seg_meta = SegmentMetadata {
            pool_id: 0,
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

    #[test]
    fn scrub_worker_large_data_verification() {
        let seg_id = SegmentId::new();
        // 128 KB = 2 Merkle leaves
        let large_data = vec![0xCD; 131072];
        let merkle_root = MerkleTree::build(&large_data, 0).unwrap().root().hash();

        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![(seg_id, large_data.clone())]);
        let worker = ScrubWorker::new(Arc::clone(&registry), data_store, 0);

        let seg_meta = SegmentMetadata {
            pool_id: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(merkle_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };

        let result = worker.scrub_segment(&seg_meta);
        assert!(result.healthy);
        assert_eq!(result.bytes_scanned, 131072);
    }

    // -----------------------------------------------------------------------
    // ScrubCoordinator — run_cycle
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_cycle_empty_store() {
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![]);
        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let report = coord.run_cycle(Arc::clone(&registry), data_store).await.unwrap();
        assert_eq!(report.segments_total, 0);
        assert_eq!(report.segments_healthy, 0);
    }

    #[tokio::test]
    async fn run_cycle_skips_unsealed_phantom_segments() {
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let mut stored_data = Vec::new();

        // One sealed segment with data on disk.
        let sealed_id = SegmentId::new();
        let sealed_data = vec![0xEF; 1024];
        let merkle_root = MerkleTree::build(&sealed_data, 0).unwrap().root().hash();
        stored_data.push((sealed_id, sealed_data.clone()));
        registry
            .reserve(
                sealed_id,
                SegmentMetadata {
                    pool_id: 0,
                    segment_id: sealed_id,
                    ec_k: 4,
                    ec_m: 2,
                    size_tier: SizeTier::Standard,
                    merkle_root: Some(merkle_root),
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: Some(1700000000000),
                },
            )
            .unwrap();
        registry
            .seal(
                sealed_id,
                SegmentMetadata {
                    pool_id: 0,
                    segment_id: sealed_id,
                    ec_k: 4,
                    ec_m: 2,
                    size_tier: SizeTier::Standard,
                    merkle_root: Some(merkle_root),
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: Some(1700000000000),
                },
            )
            .unwrap();

        // One PHANTOM segment: registered (sealed_at: None) but its .dat
        // does not exist yet — the write path registers it before the WAL
        // entry. Scrub must NOT read it (no file) nor count it corrupt.
        let phantom_id = SegmentId::new();
        registry
            .reserve(
                phantom_id,
                SegmentMetadata {
                    pool_id: 0,
                    segment_id: phantom_id,
                    ec_k: 4,
                    ec_m: 2,
                    size_tier: SizeTier::Standard,
                    merkle_root: None,
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: None,
                },
            )
            .unwrap();

        let data_store = segment_store_with_data(stored_data);
        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let report = coord.run_cycle(Arc::clone(&registry), data_store).await.unwrap();

        // Only the sealed segment is scrubbed; the phantom is skipped.
        assert_eq!(report.segments_total, 1, "unsealed phantom must not be scrubbed");
        assert_eq!(report.segments_healthy, 1);
        assert_eq!(report.segments_corrupt, 0, "phantom must not count as corrupt");
        assert!(report.bytes_scanned > 0);
    }

    #[tokio::test]
    async fn run_cycle_with_healthy_segments() {
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let mut stored_data = Vec::new();

        // Create 3 segments with known data
        for _ in 0..3 {
            let seg_id = SegmentId::new();
            let data = vec![0xEF; 1024];
            let merkle_root = MerkleTree::build(&data, 0).unwrap().root().hash();

            stored_data.push((seg_id, data));

            let seg_meta = SegmentMetadata {
                pool_id: 0,
                segment_id: seg_id,
                ec_k: 4,
                ec_m: 2,
                size_tier: SizeTier::Standard,
                merkle_root: Some(merkle_root),
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(1700000000000),
            };
            registry.reserve(seg_id, seg_meta.clone()).unwrap();
            registry.seal(seg_id, seg_meta).unwrap();
        }

        let data_store = segment_store_with_data(stored_data);
        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let report = coord.run_cycle(Arc::clone(&registry), data_store).await.unwrap();

        assert_eq!(report.segments_total, 3);
        assert_eq!(report.segments_healthy, 3);
        assert_eq!(report.segments_corrupt, 0);
        assert!(report.bytes_scanned > 0);
    }

    #[tokio::test]
    async fn run_cycle_detects_corrupt_segment() {
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));

        let seg_id = SegmentId::new();
        let correct_data = vec![0xAA; 4096];
        let mut corrupted_data = correct_data.clone();
        corrupted_data[500] ^= 0xFF; // Flip a byte

        let correct_root = MerkleTree::build(&correct_data, 0).unwrap().root().hash();

        // Metadata stores the correct Merkle root
        let seg_meta = SegmentMetadata {
            pool_id: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(correct_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };
        registry.reserve(seg_id, seg_meta.clone()).unwrap();
        registry.seal(seg_id, seg_meta).unwrap();

        // Data store has the CORRUPTED data
        let data_store = segment_store_with_data(vec![(seg_id, corrupted_data)]);

        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let report = coord.run_cycle(Arc::clone(&registry), data_store).await.unwrap();

        assert_eq!(report.segments_total, 1);
        assert_eq!(report.segments_healthy, 0);
        assert_eq!(report.segments_corrupt, 1);
    }

    #[tokio::test]
    async fn trigger_manual_spawns_and_completes() {
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![]);
        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let result = coord.trigger_manual(Arc::clone(&registry), data_store).await;
        assert!(result.is_ok());
        // Give the spawned task a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn start_background_with_shutdown_signal() {
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![]);
        let mut config = ScrubConfig::default();
        config.set_interval_sec(3600); // Long interval so it doesn't fire in test

        let coord = Arc::new(ScrubCoordinator::new(config));
        let (tx, rx) = tokio::sync::watch::channel(());

        let handle = coord.start_background(Arc::clone(&registry), data_store, rx).await;

        // Send shutdown signal
        drop(tx);

        // Wait for the task to shut down (with timeout)
        let timeout = tokio::time::timeout(std::time::Duration::from_secs(2), handle);
        assert!(timeout.await.is_ok(), "background task should shut down gracefully");
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
        assert_eq!(report.bytes_scanned, 0);
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
        assert_eq!(report.bytes_scanned, 1048576);
    }

    // -----------------------------------------------------------------------
    // Additional edge case coverage
    // -----------------------------------------------------------------------

    #[test]
    fn scrub_coordinator_config_getter() {
        let coord = ScrubCoordinator::new(ScrubConfig::default());
        let config = coord.config();
        assert_eq!(config.interval_sec(), 604800);
    }

    #[test]
    fn scrub_segment_empty_data_is_healthy() {
        let seg_id = SegmentId::new();

        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![(seg_id, vec![])]);
        let worker = ScrubWorker::new(Arc::clone(&registry), data_store, 0);

        let seg_meta = SegmentMetadata {
            pool_id: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };

        let result = worker.scrub_segment(&seg_meta);
        assert!(result.healthy);
        assert_eq!(result.bytes_scanned, 0);
    }

    // Tests that scrub_partition handles non-existent segments in metadata gracefully.
    // The worker asks the metadata store for a segment that was never put,
    // which triggers the Ok(None) branch.
    #[test]
    fn scrub_partition_handles_missing_segment() {
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![]);
        let worker = ScrubWorker::new(Arc::clone(&registry), data_store, 0);

        // Create a segment ID that was never stored in metadata
        let missing_id = SegmentId::new();
        let partition =
            SegmentPartition { node_id: NodeId::new("test"), segment_ids: vec![missing_id] };

        let results = worker.scrub_partition(&partition);
        // No scrub results should be produced for a missing segment
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn start_background_runs_cycle_on_expiry() {
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let data_store = segment_store_with_data(vec![]);
        let mut config = ScrubConfig::default();
        // Use a very short interval so the cycle fires quickly
        config.set_interval_sec(0);

        let coord = Arc::new(ScrubCoordinator::new(config));
        let (tx, rx) = tokio::sync::watch::channel(());

        let handle = coord.start_background(Arc::clone(&registry), data_store, rx).await;

        // Wait a tiny bit for the cycle to run, then shut down
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        drop(tx);

        let timeout = tokio::time::timeout(std::time::Duration::from_secs(2), handle);
        assert!(timeout.await.is_ok());
    }

    // -----------------------------------------------------------------------
    // ScrubReport builder + getters
    // -----------------------------------------------------------------------

    #[test]
    fn scrub_report_builder_chains_setters() {
        let report = ScrubReport::builder()
            .segments_total(42)
            .segments_healthy(40)
            .segments_corrupt(2)
            .segments_healed(2)
            .bytes_scanned(8192)
            .nodes_participated(3)
            .duration_sec(12.5)
            .build();

        assert_eq!(report.segments_total(), 42);
        assert_eq!(report.segments_healthy(), 40);
        assert_eq!(report.segments_corrupt(), 2);
        assert_eq!(report.segments_healed(), 2);
        assert_eq!(report.bytes_scanned(), 8192);
        assert_eq!(report.nodes_participated(), 3);
        assert!((report.duration_sec() - 12.5).abs() < f64::EPSILON);
    }

    #[test]
    fn scrub_report_default_has_all_zeros() {
        let report = ScrubReport::default();
        assert_eq!(report.segments_total(), 0);
        assert_eq!(report.segments_healthy(), 0);
        assert_eq!(report.segments_corrupt(), 0);
        assert_eq!(report.segments_healed(), 0);
        assert_eq!(report.bytes_scanned(), 0);
        assert_eq!(report.nodes_participated(), 0);
        assert_eq!(report.duration_sec(), 0.0);
    }

    // -----------------------------------------------------------------------
    // run_cycle with parallel_nodes > 0
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_cycle_with_parallel_nodes_limit() {
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let mut stored_data = Vec::new();

        // Create 4 segments
        for _ in 0..4 {
            let seg_id = SegmentId::new();
            let data = vec![0x11; 1024];
            let merkle_root = MerkleTree::build(&data, 0).unwrap().root().hash();

            stored_data.push((seg_id, data));

            let seg_meta = SegmentMetadata {
                pool_id: 0,
                segment_id: seg_id,
                ec_k: 4,
                ec_m: 2,
                size_tier: SizeTier::Standard,
                merkle_root: Some(merkle_root),
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(1700000000000),
            };
            registry.reserve(seg_id, seg_meta.clone()).unwrap();
            registry.seal(seg_id, seg_meta).unwrap();
        }

        let data_store = segment_store_with_data(stored_data);
        // Use parallel_nodes=2 to test the non-zero branch
        let mut config = ScrubConfig::default();
        config.set_parallel_nodes(2);
        let coord = ScrubCoordinator::new(config);
        let report = coord.run_cycle(Arc::clone(&registry), data_store).await.unwrap();

        assert_eq!(report.segments_total(), 4);
        assert_eq!(report.segments_healthy(), 4);
        assert_eq!(report.segments_corrupt(), 0);
    }

    // --- Metrics tests ---

    #[test]
    fn scrub_metrics_created_and_increment() {
        let coord = ScrubCoordinator::new(ScrubConfig::default());
        assert_eq!(coord.segments_checked_total.get(), 0);
        assert_eq!(coord.segments_corrupt_total.get(), 0);

        coord.segments_checked_total.add(100);
        coord.segments_corrupt_total.add(3);

        assert_eq!(coord.segments_checked_total.get(), 100);
        assert_eq!(coord.segments_corrupt_total.get(), 3);
    }
}
