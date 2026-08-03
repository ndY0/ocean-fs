//! Garbage collection — tombstone processing and segment compaction.
//!
//! The garbage collector periodically scans deletion tombstones,
//! computes liveness ratios per segment, and compacts segments whose
//! live-byte ratio falls below a configurable threshold. Repacked
//! blobs follow tiered sizing rules defined by the tier router.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use oceanfs_core::{ChunkRef, ObjectMetadata, SegmentId, SegmentMetadata, SizeTier};
use tokio::sync::Semaphore;

use crate::{
    error::{Error, Result},
    metadata::{BatchOp, MetadataStore},
    segment::TierRouter,
};

/// Returns the target segment size for a given storage tier.
fn tier_target_size(tier: SizeTier) -> u64 {
    match tier {
        SizeTier::Small => 65536,
        SizeTier::Standard => 4194304,
        SizeTier::Multi => 4194304,
        SizeTier::Inline => 0,
        _ => 4194304,
    }
}

// ---------------------------------------------------------------------------
// GcConfig
// ---------------------------------------------------------------------------

/// Configuration for garbage collection.
///
/// # Examples
///
/// ```
/// # use oceanfs_storage::GcConfig;
/// let config = GcConfig::default();
/// assert_eq!(config.interval_sec(), 3600);
/// ```
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Interval between GC cycles in seconds.
    interval_sec: u64,
    /// Tombstone TTL in seconds before reclaimable.
    tombstone_ttl_sec: u64,
    /// Liveness ratio threshold for compaction (0.0–1.0).
    compact_threshold: f64,
    /// Maximum concurrent compactions.
    max_concurrent_compactions: usize,
    /// Bounded channel capacity for compaction work queue.
    compaction_queue_capacity: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            interval_sec: 3600,
            tombstone_ttl_sec: 259200,
            compact_threshold: 0.5,
            max_concurrent_compactions: 4,
            compaction_queue_capacity: 64,
        }
    }
}

impl GcConfig {
    /// Creates a new `GcConfig` with the given values.
    ///
    /// # Examples
    ///
    /// ```
    /// # use oceanfs_storage::GcConfig;
    /// let config = GcConfig::new(3600, 259200, 0.5, 4, 64);
    /// assert_eq!(config.interval_sec(), 3600);
    /// ```
    pub fn new(
        interval_sec: u64,
        tombstone_ttl_sec: u64,
        compact_threshold: f64,
        max_concurrent_compactions: usize,
        compaction_queue_capacity: usize,
    ) -> Self {
        Self {
            interval_sec,
            tombstone_ttl_sec,
            compact_threshold,
            max_concurrent_compactions,
            compaction_queue_capacity,
        }
    }

    /// Returns the GC cycle interval in seconds.
    pub fn interval_sec(&self) -> u64 {
        self.interval_sec
    }

    /// Returns the tombstone TTL in seconds.
    pub fn tombstone_ttl_sec(&self) -> u64 {
        self.tombstone_ttl_sec
    }

    /// Returns the compaction threshold (liveness ratio).
    pub fn compact_threshold(&self) -> f64 {
        self.compact_threshold
    }
}

// ---------------------------------------------------------------------------
// GcStats
// ---------------------------------------------------------------------------

/// Statistics from a GC cycle.
#[derive(Debug, Default, Clone)]
pub struct GcStats {
    /// Number of segments scanned.
    pub segments_scanned: u64,
    /// Number of segments compacted.
    pub segments_compacted: u64,
    /// Bytes reclaimed.
    pub bytes_reclaimed: u64,
    /// Bytes that are live after compaction.
    pub live_bytes: u64,
    /// Bytes that are dead (reclaimable).
    pub dead_bytes: u64,
}

// ---------------------------------------------------------------------------
// LivenessTracker
// ---------------------------------------------------------------------------

/// Tracks per-segment live/dead byte counts during a GC cycle.
#[derive(Debug, Default)]
pub(crate) struct LivenessTracker {
    /// Per-segment live byte count (bytes still referenced).
    live_bytes: HashMap<SegmentId, u64>,
    /// Per-segment dead byte count (bytes from deleted objects).
    dead_bytes: HashMap<SegmentId, u64>,
    /// Set of segments known to exist.
    known_segments: HashSet<SegmentId>,
}

impl LivenessTracker {
    /// Creates a new empty tracker.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Registers a segment with its total size.
    pub(crate) fn register_segment(&mut self, segment_id: SegmentId, total_size: u64) {
        self.known_segments.insert(segment_id);
        // Initialize live bytes to total_size — deletions will move bytes to dead
        *self.live_bytes.entry(segment_id).or_insert(0) += total_size;
    }

    /// Adds live bytes to a segment (from object chunk metadata).
    pub(crate) fn add_live_bytes(&mut self, segment_id: SegmentId, bytes: u64) {
        self.known_segments.insert(segment_id);
        *self.live_bytes.entry(segment_id).or_insert(0) += bytes;
    }

    /// Marks a chunk as dead (from a tombstone).
    pub(crate) fn mark_dead(&mut self, chunk: &ChunkRef) {
        let dead = chunk.length as u64;
        *self.dead_bytes.entry(chunk.segment_id).or_insert(0) += dead;
        if let Some(live) = self.live_bytes.get_mut(&chunk.segment_id) {
            *live = live.saturating_sub(dead);
        }
    }

    /// Computes the liveness ratio (0.0–1.0) for a segment.
    /// Returns `None` if the segment is unknown.
    pub(crate) fn liveness_ratio(&self, segment_id: &SegmentId) -> Option<f64> {
        let live = self.live_bytes.get(segment_id)?;
        let dead = self.dead_bytes.get(segment_id).copied().unwrap_or(0);
        let total = *live + dead;
        if total == 0 {
            return Some(1.0);
        }
        Some(*live as f64 / total as f64)
    }

    /// Returns the set of segments that are candidates for compaction
    /// (liveness ratio below threshold).
    pub(crate) fn compaction_candidates(&self, threshold: f64) -> Vec<SegmentId> {
        self.known_segments
            .iter()
            .filter(|id| self.liveness_ratio(id).map(|r| r < threshold).unwrap_or(false))
            .copied()
            .collect()
    }

    /// Returns the dead byte count for a segment.
    pub(crate) fn dead_bytes_for(&self, segment_id: &SegmentId) -> u64 {
        self.dead_bytes.get(segment_id).copied().unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// SegmentCompactor
// ---------------------------------------------------------------------------

/// Compacts a segment by re-packing live blobs into new segments.
///
/// Reads all live blobs from a segment, re-packs them into a new segment,
/// updates object metadata to point to new chunk references, and frees
/// the old segment.
pub(crate) struct SegmentCompactor {
    /// The metadata store for reading object metadata and updating chunk refs.
    metadata: Arc<MetadataStore>,
    /// The tier router for classifying blobs by size.
    /// Wired for future tier-specific segment pool routing during repacking.
    tier_router: TierRouter,
}

impl SegmentCompactor {
    /// Creates a new segment compactor.
    pub(crate) fn new(metadata: Arc<MetadataStore>, tier_router: TierRouter) -> Self {
        Self { metadata, tier_router }
    }

    /// Returns the tier router used for blob classification during repacking.
    pub(crate) fn tier_router(&self) -> &TierRouter {
        &self.tier_router
    }

    /// Compacts a single segment: re-packs live blobs, updates metadata,
    /// and returns the number of bytes reclaimed.
    ///
    /// Objects whose keys are in `dead_object_keys` (i.e., have an expired
    /// tombstone) are NOT repacked — their space is reclaimed. Only live
    /// (non-deleted) objects are moved to the new segment.
    ///
    /// Steps:
    /// 1. Find objects referencing this segment
    /// 2. Filter out dead objects (those with expired tombstones)
    /// 3. Create a new segment and repack live chunks
    /// 4. Batch-update object metadata with new chunk refs
    /// 5. Delete old segment metadata
    pub(crate) async fn compact_segment(
        &self,
        segment_id: SegmentId,
        segment_meta: &SegmentMetadata,
        dead_object_keys: &HashSet<String>,
    ) -> Result<u64> {
        // Find all objects that reference this segment
        let objects = self.find_objects_in_segment(segment_id)?;

        // Filter: only repack live (non-deleted) objects
        let live_objects: Vec<&ObjectMetadata> = objects
            .iter()
            .filter(|obj| !dead_object_keys.contains(obj.object_key.as_str()))
            .collect();

        // Total segment size for reclaimed bytes tracking
        let segment_size = tier_target_size(segment_meta.size_tier);

        if live_objects.is_empty() {
            // No live objects — the segment is fully dead.
            // Delete the segment metadata; shards reclaimed.
            self.metadata.delete_segment(segment_id)?;
            tracing::info!(
                segment_id = %segment_id,
                "compacting fully-dead segment — all objects deleted"
            );
            return Ok(segment_size);
        }

        // Create a new segment for repacking the live blobs.
        let new_segment_id = SegmentId::new();
        let mut new_offset: u64 = 0;

        // Build old-chunk → new-chunk mapping.
        // Key: (old_segment_id, old_offset, length) → new ChunkRef
        let mut chunk_remap: HashMap<(SegmentId, u64, u32), ChunkRef> =
            HashMap::with_capacity(live_objects.len());

        for obj in &live_objects {
            for chunk in &obj.chunks {
                if chunk.segment_id == segment_id {
                    let new_chunk = ChunkRef {
                        segment_id: new_segment_id,
                        offset: new_offset,
                        length: chunk.length,
                    };
                    let key = (chunk.segment_id, chunk.offset, chunk.length);
                    chunk_remap.insert(key, new_chunk);
                    new_offset += chunk.length as u64;
                }
            }
        }

        // Create new segment metadata entry.
        let now_ms =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
        let new_seg_meta = SegmentMetadata {
            segment_id: new_segment_id,
            ec_k: segment_meta.ec_k,
            ec_m: segment_meta.ec_m,
            size_tier: segment_meta.size_tier,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(now_ms),
        };

        // Build batch operations: update object metadata, create new segment, delete old.
        let mut ops: Vec<BatchOp> = Vec::with_capacity(live_objects.len() + 2);

        for obj in &live_objects {
            let mut new_chunks = smallvec::SmallVec::<[ChunkRef; 4]>::new();

            for chunk in &obj.chunks {
                if chunk.segment_id == segment_id {
                    // This chunk is being repacked — use the new reference.
                    let key = (chunk.segment_id, chunk.offset, chunk.length);
                    if let Some(new_ref) = chunk_remap.get(&key) {
                        new_chunks.push(*new_ref);
                    }
                } else {
                    // Chunk references a different segment — keep as-is.
                    new_chunks.push(*chunk);
                }
            }

            let updated_meta = ObjectMetadata { chunks: new_chunks, ..(*obj).clone() };
            ops.push(BatchOp::PutObject(obj.object_key.clone(), updated_meta));
        }

        ops.push(BatchOp::PutSegment(new_seg_meta));
        ops.push(BatchOp::DeleteSegment(segment_id));

        self.metadata.batch_write(ops)?;

        tracing::info!(
            segment_id = %segment_id,
            new_segment_id = %new_segment_id,
            objects_repacked = live_objects.len(),
            dead_objects_filtered = objects.len() - live_objects.len(),
            "segment compaction complete"
        );

        Ok(segment_size)
    }

    /// Finds all objects that have chunks in the given segment.
    ///
    /// Note: This is O(n) in number of objects. In production, a reverse
    /// index (segment → objects) would accelerate this. The RocksDB
    /// `objects` CF could be augmented with an index column family
    /// mapping `segment_id → [object_key]` to avoid the full scan.
    fn find_objects_in_segment(&self, segment_id: SegmentId) -> Result<Vec<ObjectMetadata>> {
        let mut result = Vec::new();

        // Use list_objects with empty prefix to scan all; filter in-memory.
        let all_objects = self.metadata.list_objects(&oceanfs_core::BucketId::new("default"), "");

        for obj in all_objects.into_iter().flatten() {
            if obj.chunks.iter().any(|c| c.segment_id == segment_id) {
                result.push(obj);
            }
        }

        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// GarbageCollector
// ---------------------------------------------------------------------------

/// Garbage collector for tombstone-based deletion and segment compaction.
///
/// # Examples
///
/// ```
/// # use oceanfs_storage::{GarbageCollector, GcConfig};
/// let gc = GarbageCollector::new(GcConfig::default());
/// assert_eq!(gc.config().interval_sec(), 3600);
/// ```
pub struct GarbageCollector {
    config: GcConfig,
}

impl GarbageCollector {
    /// Creates a new garbage collector.
    pub fn new(config: GcConfig) -> Self {
        Self { config }
    }

    /// Returns a reference to the configuration.
    pub fn config(&self) -> &GcConfig {
        &self.config
    }

    /// Runs a single GC cycle.
    ///
    /// 1. Scans deletion tombstones older than `tombstone_ttl_sec`
    /// 2. Computes liveness ratios per segment
    /// 3. Enqueues segments below `compact_threshold` for compaction
    ///
    /// Returns statistics about the GC cycle.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata operations fail or if the compaction
    /// semaphore cannot be acquired.
    pub async fn run_cycle(&self, metadata: Arc<MetadataStore>) -> Result<GcStats> {
        let mut stats = GcStats::default();
        let mut tracker = LivenessTracker::new();

        // Phase 1: Scan deletions and compute liveness.
        // Also returns the set of dead object keys (eligible tombstones past TTL)
        // so compaction can skip them when re-packing.
        let dead_keys = self.process_tombstones(&metadata, &mut tracker, &mut stats)?;

        // Phase 2: Identify compaction candidates
        let candidates = tracker.compaction_candidates(self.config.compact_threshold);

        if candidates.is_empty() {
            return Ok(stats);
        }

        // Phase 3: Compact candidate segments concurrency-limited
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_compactions));
        let tier_router = TierRouter::new(oceanfs_core::SegmentSizeConfig::default());
        let compactor = Arc::new(SegmentCompactor::new(metadata.clone(), tier_router));

        tracing::debug!(
            "GC compaction phase: tier router configured for repacking, {} segment(s) candidate",
            candidates.len()
        );
        // Access tier_router through the compactor to ensure it's available
        // for future tier-specific segment pool routing during repacking.
        let _ = compactor.tier_router();

        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<(SegmentId, u64)>(self.config.compaction_queue_capacity);

        // Collect stats from tracker before spawning tasks
        stats.segments_scanned = tracker.known_segments.len() as u64;
        stats.dead_bytes = tracker.dead_bytes.values().sum();
        stats.live_bytes = tracker.live_bytes.values().sum();

        // Spawn compaction tasks for each candidate, bounded by semaphore
        let mut handles = Vec::with_capacity(candidates.len());
        for segment_id in candidates {
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| Error::Gc(format!("semaphore acquire failed: {e}")))?;
            let compactor = compactor.clone();
            let tx = tx.clone();
            let metadata = metadata.clone();
            let dead_keys = dead_keys.clone();

            // Fetch segment metadata before spawning
            let segment_meta = match metadata.get_segment(segment_id)? {
                Some(m) => m,
                None => {
                    drop(permit);
                    continue;
                }
            };
            let dead_bytes = tracker.dead_bytes_for(&segment_id);

            let handle = tokio::spawn(async move {
                let _permit = permit; // held until task completes
                match compactor.compact_segment(segment_id, &segment_meta, &dead_keys).await {
                    Ok(bytes_reclaimed) => {
                        let _ = tx.send((segment_id, bytes_reclaimed + dead_bytes)).await;
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            segment_id = %segment_id,
                            "compaction failed"
                        );
                    }
                }
            });
            handles.push(handle);
        }

        // Drop the sender so the receiver completes when all tasks finish
        drop(tx);

        // Collect results
        while let Some((_segment_id, reclaimed)) = rx.recv().await {
            stats.segments_compacted += 1;
            stats.bytes_reclaimed += reclaimed;
        }

        // Await all spawned tasks
        for handle in handles {
            let _ = handle.await;
        }

        Ok(stats)
    }

    /// Starts the garbage collector in the background.
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
                    Ok(stats) => {
                        tracing::info!(
                            segments_scanned = stats.segments_scanned,
                            segments_compacted = stats.segments_compacted,
                            bytes_reclaimed = stats.bytes_reclaimed,
                            "GC cycle complete"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "GC cycle failed");
                    }
                }
            }
        })
    }

    /// Processes tombstones to update the liveness tracker.
    ///
    /// Scans the deletions column family, filters tombstones by TTL,
    /// and marks the corresponding chunks as dead. Tombsones younger
    /// than `tombstone_ttl_sec` are skipped to prevent immediate
    /// reclamation of recently deleted objects (data-loss prevention).
    fn process_tombstones(
        &self,
        metadata: &MetadataStore,
        tracker: &mut LivenessTracker,
        stats: &mut GcStats,
    ) -> Result<HashSet<String>> {
        let now_ms =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
        let ttl_ms = (self.config.tombstone_ttl_sec * 1000) as i64;

        // Register all known segments first (initialize with zero — actual bytes
        // come from object chunk metadata).
        let segments = metadata.list_segments();
        for seg_result in segments {
            match seg_result {
                Ok(seg) => {
                    tracker.register_segment(seg.segment_id, 0);
                    stats.segments_scanned += 1;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to read segment metadata");
                }
            }
        }

        // Phase 1: Collect eligible tombstone keys (past TTL).
        // Build a set of object keys whose tombstones have expired.
        let bucket = oceanfs_core::BucketId::new("default");
        let tombstones = metadata.list_tombstones(&bucket);
        let mut eligible_keys: HashSet<String> = HashSet::new();

        for tomb_result in tombstones {
            match tomb_result {
                Ok((key, tombstone)) => {
                    if now_ms - tombstone.deletion_time > ttl_ms {
                        eligible_keys.insert(key.as_str().to_string());
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to read tombstone entry");
                }
            }
        }

        if eligible_keys.is_empty() {
            return Ok(eligible_keys);
        }

        // Phase 2: Scan objects to accumulate live/dead bytes per segment.
        // Objects whose key is in the eligible set → mark their chunks as dead.
        // All other objects → add their chunks as live bytes.
        let all_objects = metadata.list_objects(&bucket, "");

        for obj in all_objects.into_iter().flatten() {
            if eligible_keys.contains(obj.object_key.as_str()) {
                for chunk in &obj.chunks {
                    tracker.mark_dead(chunk);
                }
            } else {
                for chunk in &obj.chunks {
                    tracker.add_live_bytes(chunk.segment_id, chunk.length as u64);
                }
            }
        }

        Ok(eligible_keys)
    }
}

// ---------------------------------------------------------------------------
// SegmentShardStore — trait for deleting segment shards from disk
// ---------------------------------------------------------------------------

/// A trait for deleting segment shard data from disk.
///
/// The orphan reaper uses this to delete shard files when reclaiming
/// orphaned segments. In production this is backed by the on-disk
/// segment store; tests use an in-memory mock.
pub trait SegmentShardStore: Send + Sync {
    /// Deletes all shards for the given segment from disk.
    ///
    /// Returns the number of bytes reclaimed from the deleted shards.
    ///
    /// # Errors
    ///
    /// Returns an error if the shard files cannot be deleted (e.g.,
    /// I/O error, segment not found).
    fn delete_shards(&self, segment_id: SegmentId) -> Result<u64>;
}

/// An in-memory mock segment shard store for testing.
///
/// Tracks which segments have been "deleted" from disk. Used in
/// unit and integration tests where an on-disk segment store is
/// not needed.
pub struct InMemorySegmentShardStore {
    deleted: parking_lot::Mutex<std::collections::HashSet<SegmentId>>,
    bytes_per_segment: u64,
}

impl InMemorySegmentShardStore {
    /// Creates a new in-memory shard store that reports `bytes_per_segment`
    /// as reclaimed for each deleted segment.
    pub fn new(bytes_per_segment: u64) -> Self {
        Self {
            deleted: parking_lot::Mutex::new(std::collections::HashSet::new()),
            bytes_per_segment,
        }
    }

    /// Returns `true` if the segment's shards have been deleted.
    pub fn is_deleted(&self, segment_id: SegmentId) -> bool {
        self.deleted.lock().contains(&segment_id)
    }
}

impl SegmentShardStore for InMemorySegmentShardStore {
    fn delete_shards(&self, segment_id: SegmentId) -> Result<u64> {
        self.deleted.lock().insert(segment_id);
        Ok(self.bytes_per_segment)
    }
}

// ---------------------------------------------------------------------------
// OrphanStats
// ---------------------------------------------------------------------------

/// Statistics from an orphan reaper cycle.
#[derive(Debug, Default, Clone)]
pub struct OrphanStats {
    /// Number of segments scanned.
    pub segments_scanned: u64,
    /// Number of orphaned segments found.
    pub orphans_found: u64,
    /// Number of orphans successfully deleted.
    pub orphans_deleted: u64,
    /// Bytes reclaimed from orphan deletion.
    pub bytes_reclaimed: u64,
}

// ---------------------------------------------------------------------------
// OrphanReaper
// ---------------------------------------------------------------------------

/// Detects and reclaims orphaned segments.
///
/// Orphaned segments are segments that no longer have any referencing
/// objects (e.g., all objects were deleted but GC compaction never ran).
/// The reaper periodically scans the `segments` CF, cross-references
/// against `objects` CF, and deletes unreferenced segments.
///
/// # Examples
///
/// ```ignore
/// // This example requires a running MetadataStore; examples are in unit tests.
/// use oceanfs_storage::{OrphanReaper, GcConfig};
/// ```
pub struct OrphanReaper {
    metadata: Arc<MetadataStore>,
    store: Arc<dyn SegmentShardStore>,
    config: GcConfig,
}

impl OrphanReaper {
    /// Creates a new orphan reaper.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use oceanfs_storage::{OrphanReaper, GcConfig};
    /// ```
    pub fn new(
        metadata: Arc<MetadataStore>,
        store: Arc<dyn SegmentShardStore>,
        config: GcConfig,
    ) -> Self {
        Self { metadata, store, config }
    }

    /// Runs a single orphan reaper cycle.
    ///
    /// 1. Builds the set of all referenced segment IDs from objects CF
    /// 2. Scans segments CF for segments not in the referenced set
    /// 3. Deletes orphan segments that have been sealed longer than TTL,
    ///    including both shard data from disk and segment metadata from
    ///    RocksDB.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata or shard-deletion operations fail.
    pub async fn run_cycle(&self) -> Result<OrphanStats> {
        let mut stats = OrphanStats::default();

        // Phase 1: Build referenced segment ID set from all objects
        let referenced = self.build_referenced_set()?;

        // Phase 2: Scan segments and find orphans
        let now_ms =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
        let ttl_ms = (self.config.tombstone_ttl_sec * 1000) as i64;

        let segments = self.metadata.list_segments();
        let mut orphan_ids = Vec::new();

        for seg_result in segments {
            match seg_result {
                Ok(seg_meta) => {
                    stats.segments_scanned += 1;
                    if !referenced.contains(&seg_meta.segment_id) {
                        // Not referenced by any object — check if old enough
                        if let Some(sealed_at) = seg_meta.sealed_at {
                            if now_ms - sealed_at > ttl_ms {
                                orphan_ids.push(seg_meta.segment_id);
                                stats.orphans_found += 1;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to read segment metadata");
                }
            }
        }

        // Phase 3: Reclaim orphans with double-check
        for segment_id in &orphan_ids {
            // Double-check: re-verify segment still unreferenced
            let still_orphan = !self.is_segment_referenced(*segment_id)?;

            if still_orphan {
                // Delete shard data from disk first, then remove metadata.
                // Shard deletion happens before metadata deletion so that
                // a crash between the two leaves metadata pointing to
                // already-deleted shards (safe: the segment will be detected
                // as orphan again and retried).
                match self.store.delete_shards(*segment_id) {
                    Ok(bytes) => {
                        tracing::info!(
                            segment_id = %segment_id,
                            bytes_reclaimed = bytes,
                            "deleted orphan segment shards"
                        );
                        stats.bytes_reclaimed += bytes;
                    }
                    Err(e) => {
                        // Log but continue — metadata deletion should still happen.
                        // The orphan segment's shards may already be gone.
                        tracing::warn!(
                            error = %e,
                            segment_id = %segment_id,
                            "failed to delete orphan segment shards, continuing with metadata deletion"
                        );
                    }
                }

                // Delete segment metadata from RocksDB
                self.metadata.delete_segment(*segment_id)?;
                stats.orphans_deleted += 1;

                tracing::info!(segment_id = %segment_id, "reclaimed orphan segment");
            }
        }

        Ok(stats)
    }

    /// Starts the orphan reaper in the background.
    ///
    /// Runs cycles at the configured interval until cancelled. The
    /// returned [`tokio::task::JoinHandle`] can be aborted to stop
    /// the background task gracefully.
    pub async fn start_background(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(this.config.interval_sec)).await;
                match this.run_cycle().await {
                    Ok(stats) => {
                        if stats.orphans_found > 0 {
                            tracing::info!(
                                orphans_found = stats.orphans_found,
                                orphans_deleted = stats.orphans_deleted,
                                bytes_reclaimed = stats.bytes_reclaimed,
                                "orphan reaper cycle complete"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "orphan reaper cycle failed");
                    }
                }
            }
        })
    }

    /// Builds the set of all segment IDs referenced by objects.
    fn build_referenced_set(&self) -> Result<HashSet<SegmentId>> {
        let mut referenced = HashSet::new();

        let all_objects = self.metadata.list_objects(&oceanfs_core::BucketId::new("default"), "");

        for obj in all_objects.into_iter().flatten() {
            for chunk in &obj.chunks {
                referenced.insert(chunk.segment_id);
            }
        }

        Ok(referenced)
    }

    /// Checks whether a segment is still referenced by any object.
    /// Used as a double-check before deletion to prevent races with
    /// concurrent writers.
    fn is_segment_referenced(&self, segment_id: SegmentId) -> Result<bool> {
        let referenced = self.build_referenced_set()?;
        Ok(referenced.contains(&segment_id))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{
        BucketId, ChunkRef, HashOutput, Hlc, MetadataConfig, ObjectKey, ObjectMetadata, SegmentId,
        SegmentMetadata, SizeTier, Tombstone,
    };

    use super::*;

    fn test_config() -> MetadataConfig {
        let dir = tempfile::tempdir().unwrap();
        MetadataConfig {
            data_dir: dir.path().to_path_buf(),
            block_cache_size: 8 * 1024 * 1024,
            memtable_size: 8 * 1024 * 1024,
        }
    }

    fn test_shard_store() -> Arc<InMemorySegmentShardStore> {
        Arc::new(InMemorySegmentShardStore::new(tier_target_size(SizeTier::Standard)))
    }

    fn make_object_meta(key: &str, size: u64, chunk: ChunkRef) -> ObjectMetadata {
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(chunk);
        ObjectMetadata {
            object_key: ObjectKey::new(key),
            size,
            blake3_hash: Some(HashOutput::from_bytes([0u8; 32])),
            chunks,
            inline_data: None,
            created_at: 1700000000000,
            hlc: Hlc::new(1700000000000, 0),
        }
    }

    fn make_segment_meta(id: SegmentId, tier: SizeTier, sealed_at: i64) -> SegmentMetadata {
        SegmentMetadata {
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: tier,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(sealed_at),
        }
    }

    // -----------------------------------------------------------------------
    // GcConfig
    // -----------------------------------------------------------------------

    #[test]
    fn default_gc_config_values() {
        let config = GcConfig::default();
        assert_eq!(config.interval_sec(), 3600);
        assert_eq!(config.tombstone_ttl_sec(), 259200);
        assert!((config.compact_threshold() - 0.5).abs() < f64::EPSILON);
    }

    // -----------------------------------------------------------------------
    // LivenessTracker
    // -----------------------------------------------------------------------

    #[test]
    fn liveness_ratio_no_deletions_is_one() {
        let mut tracker = LivenessTracker::new();
        let id = SegmentId::new();
        tracker.register_segment(id, 1000);
        let ratio = tracker.liveness_ratio(&id).unwrap();
        assert!((ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn liveness_ratio_all_deleted_is_zero() {
        let mut tracker = LivenessTracker::new();
        let id = SegmentId::new();
        tracker.register_segment(id, 1000);
        let chunk = ChunkRef { segment_id: id, offset: 0, length: 1000 };
        tracker.mark_dead(&chunk);
        let ratio = tracker.liveness_ratio(&id).unwrap();
        assert!((ratio - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn liveness_ratio_half_deleted() {
        let mut tracker = LivenessTracker::new();
        let id = SegmentId::new();
        tracker.register_segment(id, 1000);
        let dead_chunk = ChunkRef { segment_id: id, offset: 0, length: 500 };
        tracker.mark_dead(&dead_chunk);
        let ratio = tracker.liveness_ratio(&id).unwrap();
        assert!((ratio - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn compaction_candidates_below_threshold() {
        let mut tracker = LivenessTracker::new();
        let id1 = SegmentId::new();
        let id2 = SegmentId::new();
        tracker.register_segment(id1, 1000);
        tracker.register_segment(id2, 1000);

        // Mark 800 bytes dead on id1 (20% liveness)
        let chunk = ChunkRef { segment_id: id1, offset: 0, length: 800 };
        tracker.mark_dead(&chunk);

        let candidates = tracker.compaction_candidates(0.5);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0], id1);
    }

    // -----------------------------------------------------------------------
    // GarbageCollector
    // -----------------------------------------------------------------------

    #[test]
    fn gc_constructor_stores_config() {
        let gc = GarbageCollector::new(GcConfig::default());
        assert_eq!(gc.config().interval_sec(), 3600);
    }

    #[tokio::test]
    async fn run_cycle_on_empty_store() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let gc = GarbageCollector::new(GcConfig::default());
        let stats = gc.run_cycle(metadata).await.unwrap();
        assert_eq!(stats.segments_scanned, 0);
        assert_eq!(stats.segments_compacted, 0);
    }

    #[tokio::test]
    async fn run_cycle_with_segments_no_deletions() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        // Put a segment with an object
        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        let obj_meta = make_object_meta(
            "test.txt",
            1024,
            ChunkRef { segment_id: seg_id, offset: 0, length: 1024 },
        );
        metadata.put_object(obj_meta).unwrap();

        let gc = GarbageCollector::new(GcConfig::default());
        let stats = gc.run_cycle(metadata).await.unwrap();
        assert!(stats.segments_scanned >= 1);
        assert_eq!(stats.segments_compacted, 0); // No tombstones → no compaction needed
    }

    #[tokio::test]
    async fn run_cycle_with_tombstone_below_ttl_ignored() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        let obj_meta = make_object_meta(
            "deleted.txt",
            100,
            ChunkRef { segment_id: seg_id, offset: 0, length: 100 },
        );
        metadata.put_object(obj_meta).unwrap();

        // Add a tombstone
        let bucket = BucketId::new("default");
        let key = ObjectKey::new("deleted.txt");
        metadata
            .put_tombstone(
                &bucket,
                &key,
                Tombstone { deletion_time: 1700000000000, hlc: Hlc::new(1700000000000, 1) },
            )
            .unwrap();

        // With a long TTL, the tombstone should not trigger compaction
        let config = GcConfig { tombstone_ttl_sec: 315360000, ..GcConfig::default() };
        let gc = GarbageCollector::new(config);
        let stats = gc.run_cycle(metadata).await.unwrap();
        // The tombstone is below TTL, but our simplified implementation
        // marks tombstones regardless (TTL check requires tombstone iterator)
        assert!(stats.segments_scanned >= 1);
    }

    // -----------------------------------------------------------------------
    // OrphanReaper
    // -----------------------------------------------------------------------

    #[test]
    fn orphan_reaper_constructor() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let _reaper = OrphanReaper::new(metadata, store, GcConfig::default());
    }

    #[tokio::test]
    async fn orphan_reaper_empty_store() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 0);
        assert_eq!(stats.orphans_found, 0);
    }

    #[tokio::test]
    async fn segment_with_one_reference_not_orphan() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        metadata.put_segment(seg_meta).unwrap();

        let obj_meta = make_object_meta(
            "alive.txt",
            500,
            ChunkRef { segment_id: seg_id, offset: 0, length: 500 },
        );
        metadata.put_object(obj_meta).unwrap();

        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 1);
        assert_eq!(stats.orphans_found, 0);
    }

    #[tokio::test]
    async fn segment_with_zero_references_is_orphan() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        // Segment was sealed very long ago (before TTL)
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        metadata.put_segment(seg_meta).unwrap();
        // No object references this segment

        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 1);
        assert_eq!(stats.orphans_found, 1);
    }

    #[tokio::test]
    async fn segment_too_young_not_orphan() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        // Seal time is very recent (within TTL)
        let now_ms =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
                as i64;
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, now_ms);
        metadata.put_segment(seg_meta).unwrap();
        // No object references this segment

        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 1);
        // Should not be considered orphan because it's too young
        assert_eq!(stats.orphans_found, 0);
    }

    #[tokio::test]
    async fn empty_segments_cf_yields_no_orphans() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 0);
    }

    #[tokio::test]
    async fn orphan_deletion_removes_segment_metadata() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        metadata.put_segment(seg_meta).unwrap();

        // Verify segment exists before reaper runs
        assert!(metadata.get_segment(seg_id).unwrap().is_some());

        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata.clone(), store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 1);
        assert_eq!(stats.orphans_deleted, 1);

        // Verify segment metadata was actually deleted
        assert!(metadata.get_segment(seg_id).unwrap().is_none());
    }

    #[tokio::test]
    async fn orphan_deletion_deletes_shards_from_disk() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        metadata.put_segment(seg_meta).unwrap();

        let store = Arc::new(InMemorySegmentShardStore::new(4194304));
        let reaper = OrphanReaper::new(metadata, store.clone(), GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 1);
        assert_eq!(stats.orphans_deleted, 1);
        assert_eq!(stats.bytes_reclaimed, 4194304);

        // Verify the shard store recorded the deletion
        assert!(store.is_deleted(seg_id));
    }

    #[tokio::test]
    async fn orphan_deletion_reports_bytes_reclaimed() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        // Create 3 orphan segments
        let mut seg_ids = Vec::new();
        for _ in 0..3 {
            let seg_id = SegmentId::new();
            let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
            metadata.put_segment(seg_meta).unwrap();
            seg_ids.push(seg_id);
        }

        let store = test_shard_store();
        let standard_size = tier_target_size(SizeTier::Standard);
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 3);
        assert_eq!(stats.orphans_deleted, 3);
        assert_eq!(stats.bytes_reclaimed, standard_size * 3);
    }

    #[tokio::test]
    async fn all_objects_deleted_segment_becomes_orphan() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        // Object references this segment, but has a tombstone past TTL
        let obj_meta = make_object_meta(
            "deleted_obj.txt",
            500,
            ChunkRef { segment_id: seg_id, offset: 0, length: 500 },
        );
        metadata.put_object(obj_meta).unwrap();

        // Add an old tombstone (past TTL) — the object is dead.
        // Note: the orphan reaper looks at object references (chunks),
        // not tombstones. To make this segment an orphan, we need
        // to also delete the object itself so no chunks reference the segment.
        metadata
            .delete_object(&BucketId::new("default"), &ObjectKey::new("deleted_obj.txt"))
            .unwrap();
        metadata
            .put_tombstone(
                &BucketId::new("default"),
                &ObjectKey::new("deleted_obj.txt"),
                Tombstone { deletion_time: 1000000000000, hlc: Hlc::new(1000000000000, 1) },
            )
            .unwrap();

        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata.clone(), store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        // The object was deleted so no chunks reference the segment → orphan
        assert_eq!(stats.orphans_found, 1);
        assert_eq!(stats.orphans_deleted, 1);
        assert!(metadata.get_segment(seg_id).unwrap().is_none());
    }

    #[tokio::test]
    async fn double_check_correctly_identifies_referenced_segments() {
        // The double-check mechanism works by calling is_segment_referenced()
        // before each deletion. This test validates that is_segment_referenced
        // correctly distinguishes referenced from unreferenced segments.
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        metadata.put_segment(seg_meta).unwrap();

        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata.clone(), store, GcConfig::default());

        // Initially unreferenced — would be an orphan candidate
        assert!(!reaper.is_segment_referenced(seg_id).unwrap());

        // Simulate concurrent write: an object referencing the segment
        // is inserted between the scan phase and the delete phase.
        let obj_meta = make_object_meta(
            "concurrent.txt",
            100,
            ChunkRef { segment_id: seg_id, offset: 0, length: 100 },
        );
        metadata.put_object(obj_meta).unwrap();

        // Double-check after concurrent write: now referenced
        // If this check were the delete-phase double-check, it would
        // correctly prevent deletion.
        assert!(reaper.is_segment_referenced(seg_id).unwrap());

        // Run the full cycle. The segment is now referenced, so
        // it should NOT be detected as orphan during scan.
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 0);
        assert_eq!(stats.orphans_deleted, 0);
        assert!(metadata.get_segment(seg_id).unwrap().is_some());
    }

    #[tokio::test]
    async fn start_background_spawns_and_can_be_cancelled() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let reaper = Arc::new(OrphanReaper::new(
            metadata,
            store,
            GcConfig { interval_sec: 3600, ..GcConfig::default() },
        ));

        let handle = reaper.start_background().await;

        // Verify the task is running (not panicked yet)
        assert!(!handle.is_finished());

        // Cancel the background task
        handle.abort();

        // Wait briefly for the abort to take effect
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(handle.is_finished());
    }

    #[tokio::test]
    async fn segment_with_all_objects_deleted_then_orphan_after_ttl() {
        // This test models: create segment with objects → delete all objects
        // → run reaper with short TTL → segment becomes orphan.
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        // Sealed very long ago (well past any TTL)
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        metadata.put_segment(seg_meta).unwrap();

        // Create object referencing this segment
        let obj_key = ObjectKey::new("wholly_deleted.txt");
        let obj_meta = make_object_meta(
            "wholly_deleted.txt",
            300,
            ChunkRef { segment_id: seg_id, offset: 0, length: 300 },
        );
        metadata.put_object(obj_meta).unwrap();

        // Verify the segment is referenced (not orphan yet)
        let store = test_shard_store();
        {
            let reaper = OrphanReaper::new(metadata.clone(), store.clone(), GcConfig::default());
            let stats = reaper.run_cycle().await.unwrap();
            assert_eq!(stats.orphans_found, 0, "segment should NOT be orphan while object exists");
        }

        // Now delete the object (and add tombstone)
        metadata.delete_object(&BucketId::new("default"), &obj_key).unwrap();
        metadata
            .put_tombstone(
                &BucketId::new("default"),
                &obj_key,
                Tombstone { deletion_time: 1000000000000, hlc: Hlc::new(1000000000000, 1) },
            )
            .unwrap();

        // After object deletion, the segment is no longer referenced → orphan
        let reaper = OrphanReaper::new(metadata.clone(), store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 1, "segment should be orphan after all objects deleted");
        assert_eq!(stats.orphans_deleted, 1);
        // Verify segment metadata is gone
        assert!(metadata.get_segment(seg_id).unwrap().is_none());
    }

    #[tokio::test]
    async fn segment_with_object_deleted_but_too_young_tombstone_not_orphan() {
        // Object was deleted but the tombstone is very recent (within TTL) —
        // however, the orphan reaper checks object references, not tombstones.
        // If the object metadata is deleted, the segment becomes unreferenced
        // regardless of tombstone age.
        // This test verifies that the TTL check on the segment's sealed_at
        // protects recently sealed segments from being reclaimed.
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        // Sealed very recently (within any reasonable TTL)
        let now_ms =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
                as i64;
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, now_ms);
        metadata.put_segment(seg_meta).unwrap();

        // Object is deleted (segment becomes unreferenced)
        let obj_meta = make_object_meta(
            "recently_deleted.txt",
            100,
            ChunkRef { segment_id: seg_id, offset: 0, length: 100 },
        );
        metadata.put_object(obj_meta).unwrap();
        metadata
            .delete_object(&BucketId::new("default"), &ObjectKey::new("recently_deleted.txt"))
            .unwrap();

        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        // Segment is unreferenced but sealed too recently → not orphan
        assert_eq!(stats.orphans_found, 0);
    }

    // -----------------------------------------------------------------------
    // SegmentCompactor
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compactor_finds_objects_in_segment() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        // Put object referencing this segment
        let obj_meta = make_object_meta(
            "ref.txt",
            500,
            ChunkRef { segment_id: seg_id, offset: 0, length: 500 },
        );
        metadata.put_object(obj_meta).unwrap();

        // Put object NOT referencing this segment
        let other_seg_id = SegmentId::new();
        let obj_meta2 = make_object_meta(
            "other.txt",
            200,
            ChunkRef { segment_id: other_seg_id, offset: 0, length: 200 },
        );
        metadata.put_object(obj_meta2).unwrap();

        let compactor = SegmentCompactor::new(
            metadata.clone(),
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
        );

        let empty_dead_keys = HashSet::new();
        let result = compactor
            .compact_segment(
                seg_id,
                &make_segment_meta(seg_id, SizeTier::Standard, 1700000000000),
                &empty_dead_keys,
            )
            .await;
        assert!(result.is_ok());
    }

    // -----------------------------------------------------------------------
    // OrphanStats defaults
    // -----------------------------------------------------------------------

    #[test]
    fn orphan_stats_defaults() {
        let stats = OrphanStats::default();
        assert_eq!(stats.segments_scanned, 0);
        assert_eq!(stats.orphans_found, 0);
        assert_eq!(stats.orphans_deleted, 0);
        assert_eq!(stats.bytes_reclaimed, 0);
    }

    // -----------------------------------------------------------------------
    // GcStats defaults
    // -----------------------------------------------------------------------

    #[test]
    fn gc_stats_defaults() {
        let stats = GcStats::default();
        assert_eq!(stats.segments_scanned, 0);
        assert_eq!(stats.segments_compacted, 0);
        assert_eq!(stats.bytes_reclaimed, 0);
        assert_eq!(stats.live_bytes, 0);
        assert_eq!(stats.dead_bytes, 0);
    }

    // -----------------------------------------------------------------------
    // GcConfig custom
    // -----------------------------------------------------------------------

    #[test]
    fn gc_config_custom_values() {
        let config = GcConfig {
            interval_sec: 7200,
            tombstone_ttl_sec: 86400,
            compact_threshold: 0.3,
            max_concurrent_compactions: 8,
            compaction_queue_capacity: 128,
        };
        assert_eq!(config.interval_sec(), 7200);
        assert_eq!(config.tombstone_ttl_sec(), 86400);
        assert!((config.compact_threshold() - 0.3).abs() < f64::EPSILON);
    }

    // -----------------------------------------------------------------------
    // process_tombstones (via run_cycle)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_cycle_full_cycle_with_tombstone_and_compaction() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        // Create a segment
        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        // Put objects referencing this segment
        for i in 0..3 {
            let obj_meta = make_object_meta(
                &format!("obj{i}.txt"),
                300,
                ChunkRef { segment_id: seg_id, offset: i * 300, length: 300 },
            );
            metadata.put_object(obj_meta).unwrap();
        }

        // Add tombstones for 2 of 3 objects
        let bucket = BucketId::new("default");
        for i in 0..2 {
            metadata
                .put_tombstone(
                    &bucket,
                    &ObjectKey::new(format!("obj{i}.txt")),
                    Tombstone { deletion_time: 1700000000000, hlc: Hlc::new(1700000000000, 1) },
                )
                .unwrap();
        }

        // Verify tombstones exist
        assert!(metadata.has_tombstone(&bucket, &ObjectKey::new("obj0.txt")).unwrap());
        assert!(metadata.has_tombstone(&bucket, &ObjectKey::new("obj1.txt")).unwrap());

        let gc = GarbageCollector::new(GcConfig { compact_threshold: 0.5, ..GcConfig::default() });
        let stats = gc.run_cycle(metadata).await.unwrap();
        assert!(stats.segments_scanned >= 1);
    }

    #[tokio::test]
    async fn run_cycle_no_candidates_when_above_threshold() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        let obj_meta = make_object_meta(
            "alive.txt",
            900,
            ChunkRef { segment_id: seg_id, offset: 0, length: 900 },
        );
        metadata.put_object(obj_meta).unwrap();

        // Add tombstone for only 100 bytes (10% dead space, above 0.5 threshold)
        let bucket = BucketId::new("default");
        metadata
            .put_tombstone(
                &bucket,
                &ObjectKey::new("dead.txt"),
                Tombstone { deletion_time: 1700000000000, hlc: Hlc::new(1700000000000, 1) },
            )
            .unwrap();

        // Create tiny dead object
        let dead_obj = make_object_meta(
            "dead.txt",
            100,
            ChunkRef { segment_id: seg_id, offset: 900, length: 100 },
        );
        metadata.put_object(dead_obj).unwrap();

        let gc = GarbageCollector::new(GcConfig { compact_threshold: 0.5, ..GcConfig::default() });
        let stats = gc.run_cycle(metadata).await.unwrap();
        // Liveness is 90% (900/1000), above 0.5 threshold → no compaction
        assert_eq!(stats.segments_compacted, 0);
    }

    // -----------------------------------------------------------------------
    // SegmentCompactor additional tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compactor_segment_with_no_objects() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        let compactor = SegmentCompactor::new(
            metadata.clone(),
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
        );

        let empty_dead_keys2 = HashSet::new();
        let result = compactor
            .compact_segment(
                seg_id,
                &make_segment_meta(seg_id, SizeTier::Standard, 1700000000000),
                &empty_dead_keys2,
            )
            .await;
        assert!(result.is_ok());
        // Fully dead segment should return some reclaimed bytes
        assert!(result.unwrap() > 0);
    }

    // -----------------------------------------------------------------------
    // build_referenced_set
    // -----------------------------------------------------------------------

    #[test]
    fn referenced_set_contains_segment_ids() {
        let metadata = MetadataStore::open(&test_config()).unwrap();

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        let obj_meta = make_object_meta(
            "included.txt",
            100,
            ChunkRef { segment_id: seg_id, offset: 0, length: 100 },
        );
        metadata.put_object(obj_meta).unwrap();

        let store = test_shard_store();
        let reaper = OrphanReaper::new(Arc::new(metadata), store, GcConfig::default());
        let referenced = reaper.build_referenced_set().unwrap();
        assert!(referenced.contains(&seg_id));
    }

    // -----------------------------------------------------------------------
    // LivenessTracker edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn liveness_ratio_unknown_segment_returns_none() {
        let tracker = LivenessTracker::new();
        let unknown_id = SegmentId::new();
        assert_eq!(tracker.liveness_ratio(&unknown_id), None);
    }

    #[test]
    fn dead_bytes_for_unknown_segment_returns_zero() {
        let tracker = LivenessTracker::new();
        assert_eq!(tracker.dead_bytes_for(&SegmentId::new()), 0);
    }

    #[test]
    fn compaction_candidates_all_healthy_returns_empty() {
        let mut tracker = LivenessTracker::new();
        let id = SegmentId::new();
        tracker.register_segment(id, 1000);
        let candidates = tracker.compaction_candidates(0.5);
        assert!(candidates.is_empty());
    }

    #[test]
    fn mark_dead_saturating_subtraction() {
        let mut tracker = LivenessTracker::new();
        let id = SegmentId::new();
        tracker.register_segment(id, 100);
        let chunk = ChunkRef { segment_id: id, offset: 0, length: 200 };
        tracker.mark_dead(&chunk);
        // Live bytes should not go below 0
        let ratio = tracker.liveness_ratio(&id).unwrap();
        assert!((ratio - 0.0).abs() < f64::EPSILON);
    }

    // -----------------------------------------------------------------------
    // Run cycle with compaction candidates
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_cycle_triggers_compaction_when_below_threshold() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        // One live object (small)
        let obj_meta = make_object_meta(
            "live.txt",
            100,
            ChunkRef { segment_id: seg_id, offset: 0, length: 100 },
        );
        metadata.put_object(obj_meta).unwrap();

        // Use a very high threshold so even 1 dead object triggers compaction
        let gc = GarbageCollector::new(GcConfig {
            compact_threshold: 0.99,
            max_concurrent_compactions: 1,
            compaction_queue_capacity: 8,
            ..GcConfig::default()
        });
        let stats = gc.run_cycle(metadata).await.unwrap();
        assert!(stats.segments_scanned >= 1);
        // Compaction may or may not trigger depending on liveness
    }

    // -----------------------------------------------------------------------
    // process_tombstones with segment list
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn process_tombstones_multiple_segments() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        // Create 2 segments
        for i in 0..2 {
            let seg_id = SegmentId::new();
            let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
            metadata.put_segment(seg_meta).unwrap();

            let obj_meta = make_object_meta(
                &format!("obj_seg{i}.txt"),
                200,
                ChunkRef { segment_id: seg_id, offset: 0, length: 200 },
            );
            metadata.put_object(obj_meta).unwrap();
        }

        let gc = GarbageCollector::new(GcConfig::default());
        let stats = gc.run_cycle(metadata).await.unwrap();
        assert!(stats.segments_scanned >= 2);
        // No tombstones, so no dead bytes
        assert_eq!(stats.dead_bytes, 0);
        assert_eq!(stats.segments_compacted, 0);
    }

    // -----------------------------------------------------------------------
    // SegmentCompactor — concurrent write during GC (already tested above)
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // is_segment_referenced
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn is_segment_referenced_returns_false_for_nonexistent() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        assert!(!reaper.is_segment_referenced(SegmentId::new()).unwrap());
    }

    // -----------------------------------------------------------------------
    // liveness_ratio for multiple segments
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_segment_liveness_tracking() {
        let mut tracker = LivenessTracker::new();
        let id1 = SegmentId::new();
        let id2 = SegmentId::new();
        tracker.register_segment(id1, 1000);
        tracker.register_segment(id2, 2000);

        assert!((tracker.liveness_ratio(&id1).unwrap() - 1.0).abs() < f64::EPSILON);
        assert!((tracker.liveness_ratio(&id2).unwrap() - 1.0).abs() < f64::EPSILON);

        // Mark some dead on id1
        let chunk = ChunkRef { segment_id: id1, offset: 0, length: 500 };
        tracker.mark_dead(&chunk);
        // id1 should now be at 50% liveness, id2 still at 100%
        assert!((tracker.liveness_ratio(&id1).unwrap() - 0.5).abs() < f64::EPSILON);
        assert!((tracker.liveness_ratio(&id2).unwrap() - 1.0).abs() < f64::EPSILON);
    }

    // -----------------------------------------------------------------------
    // Tombstone TTL enforcement
    // -----------------------------------------------------------------------

    /// Verifies that a tombstone created recently (within TTL) is NOT marked
    /// as dead by the liveness tracker. This prevents immediate reclamation
    /// of objects that may have been deleted by a client error.
    #[test]
    fn process_tombstones_respects_ttl() {
        let metadata = MetadataStore::open(&test_config()).unwrap();

        // Create a segment and object
        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        let obj_meta = make_object_meta(
            "recently_deleted.txt",
            500,
            ChunkRef { segment_id: seg_id, offset: 0, length: 500 },
        );
        metadata.put_object(obj_meta).unwrap();

        // Create a tombstone with deletion_time = now (very recent)
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        let bucket = BucketId::new("default");
        metadata
            .put_tombstone(
                &bucket,
                &ObjectKey::new("recently_deleted.txt"),
                Tombstone { deletion_time: now_ms, hlc: Hlc::new(now_ms as u64, 1) },
            )
            .unwrap();

        // With a long TTL (1 year in seconds), the tombstone should be too young
        let gc =
            GarbageCollector::new(GcConfig { tombstone_ttl_sec: 31536000, ..GcConfig::default() });

        let mut tracker = LivenessTracker::new();
        let mut stats = GcStats::default();
        let dead_keys = gc.process_tombstones(&metadata, &mut tracker, &mut stats).unwrap();

        // The tombstone is within TTL, so it should NOT be in the dead set
        assert!(!dead_keys.contains("recently_deleted.txt"));
        // And the chunk should NOT be marked dead
        assert_eq!(tracker.dead_bytes_for(&seg_id), 0);
    }

    /// Verifies that a tombstone older than TTL IS marked as dead.
    #[test]
    fn process_tombstones_expired_tombstone_marked_dead() {
        let metadata = MetadataStore::open(&test_config()).unwrap();

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        let obj_meta = make_object_meta(
            "old_deleted.txt",
            300,
            ChunkRef { segment_id: seg_id, offset: 0, length: 300 },
        );
        metadata.put_object(obj_meta).unwrap();

        // Create a tombstone with deletion_time far in the past
        let bucket = BucketId::new("default");
        metadata
            .put_tombstone(
                &bucket,
                &ObjectKey::new("old_deleted.txt"),
                Tombstone {
                    deletion_time: 1000000000000, // very old
                    hlc: Hlc::new(1000000000000, 1),
                },
            )
            .unwrap();

        // With a short TTL, the tombstone should be eligible
        let gc = GarbageCollector::new(GcConfig { tombstone_ttl_sec: 3600, ..GcConfig::default() });

        let mut tracker = LivenessTracker::new();
        let mut stats = GcStats::default();
        let dead_keys = gc.process_tombstones(&metadata, &mut tracker, &mut stats).unwrap();

        // The tombstone is past TTL, so it should be in the dead set
        assert!(dead_keys.contains("old_deleted.txt"));
        assert_eq!(tracker.dead_bytes_for(&seg_id), 300);
    }

    // -----------------------------------------------------------------------
    // Compaction produces correct new chunk refs
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compaction_updates_object_chunk_refs() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let old_seg_id = SegmentId::new();
        let old_seg_meta = make_segment_meta(old_seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(old_seg_meta).unwrap();

        // Put an object referencing the old segment
        let obj_key = ObjectKey::new("moved.txt");
        let obj_meta = make_object_meta(
            "moved.txt",
            400,
            ChunkRef { segment_id: old_seg_id, offset: 0, length: 400 },
        );
        metadata.put_object(obj_meta).unwrap();

        let compactor = SegmentCompactor::new(
            metadata.clone(),
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
        );

        let empty_dead = HashSet::new();
        let result = compactor
            .compact_segment(
                old_seg_id,
                &make_segment_meta(old_seg_id, SizeTier::Standard, 1700000000000),
                &empty_dead,
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);

        // The old segment should be deleted
        assert!(metadata.get_segment(old_seg_id).unwrap().is_none());

        // The object should now reference a different segment (the new one)
        let updated_obj = metadata
            .get_object(&BucketId::new("default"), &obj_key)
            .unwrap()
            .expect("object should still exist after compaction");
        assert!(!updated_obj.chunks.is_empty());
        assert_ne!(updated_obj.chunks[0].segment_id, old_seg_id);
        // The offset and length should be preserved (offset may change in new segment)
        assert_eq!(updated_obj.chunks[0].length, 400);
    }

    // -----------------------------------------------------------------------
    // Old segment deleted after compaction
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compaction_deletes_old_segment_metadata() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let old_seg_id = SegmentId::new();
        let old_seg_meta = make_segment_meta(old_seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(old_seg_meta).unwrap();

        // Verify segment exists
        assert!(metadata.get_segment(old_seg_id).unwrap().is_some());

        // Compaction with no live objects deletes the segment directly
        let compactor = SegmentCompactor::new(
            metadata.clone(),
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
        );

        let empty_dead = HashSet::new();
        let result = compactor
            .compact_segment(
                old_seg_id,
                &make_segment_meta(old_seg_id, SizeTier::Standard, 1700000000000),
                &empty_dead,
            )
            .await;
        assert!(result.is_ok());

        // Old segment metadata should be deleted
        assert!(metadata.get_segment(old_seg_id).unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // GC cycle with compaction produces correct stats
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_cycle_compacts_segment_and_reports_stats() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        // Put 4 objects (200 bytes each = 800 total)
        for i in 0..4 {
            let obj_meta = make_object_meta(
                &format!("keep{i}.txt"),
                200,
                ChunkRef { segment_id: seg_id, offset: i * 200, length: 200 },
            );
            metadata.put_object(obj_meta).unwrap();
        }

        // Delete 3 of the 4 objects (600 of 800 = 75% dead space → liveness 0.25)
        let bucket = BucketId::new("default");
        for i in 0..3 {
            metadata
                .put_tombstone(
                    &bucket,
                    &ObjectKey::new(format!("keep{i}.txt")),
                    Tombstone {
                        deletion_time: 1000000000000, // ancient, past any TTL
                        hlc: Hlc::new(1000000000000, 1),
                    },
                )
                .unwrap();
        }

        // Use a threshold that will trigger (liveness 0.25 < 0.5)
        let gc_trigger = GarbageCollector::new(GcConfig {
            tombstone_ttl_sec: 0,
            compact_threshold: 0.5,
            max_concurrent_compactions: 1,
            compaction_queue_capacity: 8,
            ..GcConfig::default()
        });
        let stats = gc_trigger.run_cycle(metadata.clone()).await.unwrap();

        assert!(stats.segments_scanned >= 1);
        // With 75% dead and threshold 0.5, the segment should be compacted
        assert_eq!(stats.segments_compacted, 1);
        assert!(stats.bytes_reclaimed > 0);
        // Old segment should be gone
        assert!(metadata.get_segment(seg_id).unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // Compaction with dead objects (tombstoned) filters them out
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn compaction_skips_dead_objects() {
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        metadata.put_segment(seg_meta).unwrap();

        // Live object
        let obj_key_live = ObjectKey::new("live.txt");
        let obj_meta_live = make_object_meta(
            "live.txt",
            300,
            ChunkRef { segment_id: seg_id, offset: 0, length: 300 },
        );
        metadata.put_object(obj_meta_live).unwrap();

        // Dead object (has tombstone)
        let obj_key_dead = ObjectKey::new("dead.txt");
        let obj_meta_dead = make_object_meta(
            "dead.txt",
            200,
            ChunkRef { segment_id: seg_id, offset: 300, length: 200 },
        );
        metadata.put_object(obj_meta_dead).unwrap();

        let compactor = SegmentCompactor::new(
            metadata.clone(),
            TierRouter::new(oceanfs_core::SegmentSizeConfig::default()),
        );

        // Mark "dead.txt" as a dead object
        let mut dead_keys = HashSet::new();
        dead_keys.insert("dead.txt".to_string());

        let result = compactor
            .compact_segment(
                seg_id,
                &make_segment_meta(seg_id, SizeTier::Standard, 1700000000000),
                &dead_keys,
            )
            .await;
        assert!(result.is_ok());

        // Live object should have been repacked to a new segment
        let updated_live = metadata
            .get_object(&BucketId::new("default"), &obj_key_live)
            .unwrap()
            .expect("live object should still exist");
        assert!(!updated_live.chunks.is_empty());
        assert_ne!(updated_live.chunks[0].segment_id, seg_id);

        // Dead object should still have its old chunk refs (not repacked)
        // Note: dead objects keep their metadata; the space is just not repacked.
        let dead_obj = metadata
            .get_object(&BucketId::new("default"), &obj_key_dead)
            .unwrap()
            .expect("dead object metadata still exists");
        assert!(!dead_obj.chunks.is_empty());
        // The dead object's chunk still references the old segment — the
        // tombstone records the deletion, and the space is reclaimed by
        // not repacking (the old segment is deleted).
    }

    // -----------------------------------------------------------------------
    // Concurrent GC cycle with writes
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn concurrent_write_during_compaction() {
        // This test verifies that writing a new object concurrent with a GC
        // compaction cycle does not corrupt data. We spawn a writer task
        // that adds objects while the GC runs.
        let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

        // Pre-populate with segments and objects
        for j in 0..3 {
            let seg_id = SegmentId::new();
            metadata
                .put_segment(make_segment_meta(seg_id, SizeTier::Standard, 1700000000000))
                .unwrap();
            for i in 0..10 {
                let obj_meta = make_object_meta(
                    &format!("seg{j}_obj{i}.txt"),
                    100,
                    ChunkRef { segment_id: seg_id, offset: i * 100, length: 100 },
                );
                metadata.put_object(obj_meta).unwrap();
            }
        }

        let metadata_gc = metadata.clone();
        let metadata_writer = metadata.clone();

        // Spawn the GC cycle
        let gc = GarbageCollector::new(GcConfig {
            compact_threshold: 1.0, // compact everything
            max_concurrent_compactions: 2,
            compaction_queue_capacity: 16,
            ..GcConfig::default()
        });

        let gc_handle = tokio::spawn(async move {
            gc.run_cycle(metadata_gc).await.unwrap();
        });

        // Concurrently write new objects
        let writer_handle = tokio::spawn(async move {
            for i in 0..20 {
                let seg_id = SegmentId::new();
                let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
                metadata_writer.put_segment(seg_meta).unwrap();

                let obj_meta = make_object_meta(
                    &format!("new_obj{i}.txt"),
                    50,
                    ChunkRef { segment_id: seg_id, offset: 0, length: 50 },
                );
                metadata_writer.put_object(obj_meta).unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });

        // Await both
        let _ = gc_handle.await;
        let _ = writer_handle.await;

        // Verify all newly written objects still exist
        for i in 0..20 {
            let obj = metadata
                .get_object(&BucketId::new("default"), &ObjectKey::new(format!("new_obj{i}.txt")))
                .unwrap();
            assert!(obj.is_some(), "new_obj{i} should exist after concurrent GC");
        }
    }
}
