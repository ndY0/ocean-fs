//! Orphan reaper — detects and reclaims segments with no live references.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use oceanfs_core::{Counter, LabelSet, MetricRegistrar, SegmentId};
use oceanfs_storage::Result;

use super::{config::GcConfig, garbage_collector::SegmentShardStore};

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
/// // This example requires a running RocksDbMetadataStore; examples are in unit tests.
/// use oceanfs_storage::{OrphanReaper, GcConfig};
/// ```
pub struct OrphanReaper {
    metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
    store: Arc<dyn SegmentShardStore>,
    config: GcConfig,
    orphans_deleted_total: Counter,
    bytes_reclaimed_total: Counter,
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
        metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
        store: Arc<dyn SegmentShardStore>,
        config: GcConfig,
    ) -> Self {
        Self {
            metadata,
            store,
            config,
            orphans_deleted_total: Counter::new(
                "orphan_segments_reaped_total".into(),
                "Orphan segments deleted".into(),
                LabelSet::empty(),
            ),
            bytes_reclaimed_total: Counter::new(
                "orphan_bytes_reclaimed_total".into(),
                "Bytes reclaimed from orphan deletion".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Registers all orphan reaper counters with a metrics registrar.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.orphans_deleted_total.clone());
        registrar.register_counter(self.bytes_reclaimed_total.clone());
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

        self.orphans_deleted_total.add(stats.orphans_deleted);
        self.bytes_reclaimed_total.add(stats.bytes_reclaimed);

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
    pub(crate) fn build_referenced_set(&self) -> Result<HashSet<SegmentId>> {
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
    pub(crate) fn is_segment_referenced(&self, segment_id: SegmentId) -> Result<bool> {
        let referenced = self.build_referenced_set()?;
        Ok(referenced.contains(&segment_id))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use oceanfs_core::{
        BucketId, ChunkRef, Hlc, MetadataConfig, ObjectKey, ObjectMetadata, SegmentId,
        SegmentMetadata, SizeTier, Tombstone,
    };
    use oceanfs_storage::metadata::RocksDbMetadataStore;

    use super::super::{
        config::tier_target_size, garbage_collector::InMemorySegmentShardStore,
        liveness_tracker::LivenessTracker, *,
    };

    fn test_config() -> MetadataConfig {
        let dir = tempfile::tempdir().unwrap();
        MetadataConfig {
            data_dir: dir.path().to_path_buf(),
            block_cache_size: 8 * 1024 * 1024,
            memtable_size: 8 * 1024 * 1024,
            ..Default::default()
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
            blake3_hash: None,
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
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

    // OrphanReaper
    // -----------------------------------------------------------------------

    #[test]
    fn orphan_reaper_constructor() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let _reaper = OrphanReaper::new(metadata, store, GcConfig::default());
    }

    #[tokio::test]
    async fn orphan_reaper_empty_store() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 0);
        assert_eq!(stats.orphans_found, 0);
    }

    #[tokio::test]
    async fn segment_with_one_reference_not_orphan() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 0);
    }

    #[tokio::test]
    async fn orphan_deletion_removes_segment_metadata() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

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
            .delete_object(
                &BucketId::new("default"),
                &ObjectKey::new("deleted_obj.txt"),
                oceanfs_core::Hlc::zero(),
            )
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
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

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
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
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
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

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
        metadata
            .delete_object(&BucketId::new("default"), &obj_key, oceanfs_core::Hlc::zero())
            .unwrap();
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
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

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
            .delete_object(
                &BucketId::new("default"),
                &ObjectKey::new("recently_deleted.txt"),
                oceanfs_core::Hlc::zero(),
            )
            .unwrap();

        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        let stats = reaper.run_cycle().await.unwrap();
        // Segment is unreferenced but sealed too recently → not orphan
        assert_eq!(stats.orphans_found, 0);
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

    // build_referenced_set
    // -----------------------------------------------------------------------

    #[test]
    fn referenced_set_contains_segment_ids() {
        let metadata = RocksDbMetadataStore::open(&test_config()).unwrap();

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

    // is_segment_referenced
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn is_segment_referenced_returns_false_for_nonexistent() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        assert!(!reaper.is_segment_referenced(SegmentId::new()).unwrap());
    }

    // -----------------------------------------------------------------------

    // Tombstone TTL enforcement
    // -----------------------------------------------------------------------

    /// Verifies that a tombstone created recently (within TTL) is NOT marked
    /// as dead by the liveness tracker. This prevents immediate reclamation
    /// of objects that may have been deleted by a client error.
    #[test]
    fn process_tombstones_respects_ttl() {
        let metadata = RocksDbMetadataStore::open(&test_config()).unwrap();

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
        let (dead_keys, _) = gc.process_tombstones(&metadata, &mut tracker, &mut stats).unwrap();

        // The tombstone is within TTL, so it should NOT be in the dead set
        assert!(!dead_keys.contains("recently_deleted.txt"));
        // And the chunk should NOT be marked dead
        assert_eq!(tracker.dead_bytes_for(&seg_id), 0);
    }

    // --- Metrics tests ---

    #[test]
    fn orphan_reaper_metrics_created_and_increment() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let reaper = OrphanReaper::new(metadata, store, GcConfig::default());
        assert_eq!(reaper.orphans_deleted_total.get(), 0);
        assert_eq!(reaper.bytes_reclaimed_total.get(), 0);

        reaper.orphans_deleted_total.add(5);
        reaper.bytes_reclaimed_total.add(4096);

        assert_eq!(reaper.orphans_deleted_total.get(), 5);
        assert_eq!(reaper.bytes_reclaimed_total.get(), 4096);
    }
}
