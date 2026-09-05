//! Orphan reaper — detects and reclaims segments with no live references.

// [review][pacement][critical]
// why is the orphan reaper placed under the garbage collection ?
// this is a separate mechanism.
// [end]

use std::{
    collections::HashSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use oceanfs_core::{Counter, LabelSet, MetricRegistrar, SegmentId};
use oceanfs_storage::{Result, SegmentLifecycleCoordinator, TransitionError};
use oceanfs_storage_api::SegmentDataStore;

use super::config::GcConfig;

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
/// Deletion is routed through the lifecycle coordinator (ADR-0025
/// phase 1) — the ONLY writer of segment lifecycle state: the reaper
/// *requests* the delete, and the coordinator makes the deleted-marker
/// CF write durable BEFORE the reaper unlinks the `.dat` files
/// (ADR-0024 invariant 3: delete before unlink).
///
/// # Examples
///
/// ```ignore
/// // This example requires a running RocksDbMetadataStore; examples are in unit tests.
/// use oceanfs_storage::{OrphanReaper, GcConfig};
/// ```
pub struct OrphanReaper {
    metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
    /// Lifecycle coordinator — the single writer of segment lifecycle
    /// state; the reaper requests `delete` through it before unlinking
    /// shards.
    lifecycle: Arc<SegmentLifecycleCoordinator>,
    /// The unified segment data store (ADR-0032 D1): per-root
    /// `list_segment_files` for the disk sweep + `delete_shards_with_pool`
    /// for the reclaim.
    store: Arc<dyn SegmentDataStore>,
    /// The data pool roots the reaper sweeps for registry-unknown
    /// `.dat` orphans (ADR-0032 D1 per-root listing). Each root carries
    /// the pool id the reclaim unlinks under.
    pool_roots: Vec<Arc<oceanfs_storage::StoragePool>>,
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
        lifecycle: Arc<SegmentLifecycleCoordinator>,
        store: Arc<dyn SegmentDataStore>,
        pool_roots: Vec<Arc<oceanfs_storage::StoragePool>>,
        config: GcConfig,
    ) -> Self {
        Self {
            metadata,
            lifecycle,
            store,
            pool_roots,
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

        // Phase 2: Scan the machine's live entries and find orphans
        // (ADR-0025 Decision 3 — the `segments` CF is removed).
        let now_ms =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
        let ttl_ms = (self.config.tombstone_ttl_sec * 1000) as i64;

        // (segment_id, pool_id) — the pool id names the root holding the
        // `.dat`: every listed id is a real registered pool id, so the
        // unlink lands on the right root (ADR-0029 f5; ADR-0031 D2 —
        // there is no legacy dir).
        let mut orphan_ids: Vec<(SegmentId, u32)> = Vec::new();
        self.lifecycle.registry().for_each(|segment_id, entry| {
            stats.segments_scanned += 1;
            if !referenced.contains(&segment_id) {
                // Not referenced by any object — check if old enough
                if let Some(sealed_at) = entry.metadata.sealed_at {
                    if now_ms - sealed_at > ttl_ms {
                        orphan_ids.push((segment_id, entry.metadata.pool_id));
                        stats.orphans_found += 1;
                    }
                }
            }
        });

        // Phase 2b: the ON-DISK segments the registry does not know.
        // The replication receiver (append_segment) historically wrote
        // raw `.dat` files WITHOUT lifecycle registration, so the
        // registry scan above never saw them — the fleet disk-fill
        // root cause (~10k unregistered files vs 32 registered). A
        // file with no object-row reference is garbage regardless of
        // the registry; the file's mtime stands in for `sealed_at`
        // (the TTL grace gate).
        let registered: std::collections::HashSet<SegmentId> = {
            let mut set = std::collections::HashSet::new();
            self.lifecycle.registry().for_each(|id, _| {
                set.insert(id);
            });
            set
        };
        // Per-root sweep (ADR-0032 D1): `list_segment_files(root)` names
        // the `.dat` files under one pool root; the pool id for a listed
        // orphan is the root's own id (no more store-wide
        // `(id, mtime, pool)` tuples). The file's mtime stands in for
        // `sealed_at` (the TTL grace gate) and the segment id is parsed
        // from the `{uuid}.dat` file name.
        for pool in &self.pool_roots {
            let root = pool.root();
            let pool_id = pool.id();
            let listed = self
                .store
                .list_segment_files(root)
                .map_err(|e| oceanfs_storage::Error::Io(std::io::Error::other(e.to_string())))?;
            for path in listed {
                let Some(id_str) =
                    path.file_name().and_then(|n| n.to_str()).and_then(|n| n.strip_suffix(".dat"))
                else {
                    continue;
                };
                let Ok(uuid) = uuid::Uuid::parse_str(id_str) else { continue };
                let segment_id = SegmentId::from_uuid_bytes(*uuid.as_bytes());
                if registered.contains(&segment_id) || referenced.contains(&segment_id) {
                    continue;
                }
                let file_mtime = std::fs::metadata(&path)
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                if file_mtime > 0 && now_ms - file_mtime > ttl_ms {
                    orphan_ids.push((segment_id, pool_id));
                    stats.orphans_found += 1;
                }
            }
        }

        // Phase 3: Reclaim orphans with double-check.
        //
        // The double-check re-uses the Phase-1 referenced set — a
        // per-orphan FULL metadata rescan made the cycle cost scale as
        // O(orphans × metadata): with ~1000 orphans per cycle the
        // reaper did ~1000 full scans per cycle, its reclaim rate
        // capped below the write rate, and the disk climbed
        // monotonically (the fleet churn disk-fill — the metadata was
        // already small after the hint-apply fix, so the orphan count
        // alone was enough to stall the reaper).
        //
        // The race a fresh rescan closed — a row written mid-cycle
        // referencing a just-reaped segment — is closed by
        // construction: sealed segments receive no new appends, and
        // the only row writers (direct PUT, hint apply) reference
        // freshly-appended segments; the read-repair push references
        // the winner's (foreign) segment ids. The durable Deleted
        // marker (delete-before-unlink, ADR-0024) remains the crash
        // guard between request_delete and the unlink.
        for (segment_id, pool_id) in &orphan_ids {
            // Double-check against the cycle's snapshot.
            let still_orphan = !referenced.contains(segment_id);

            if still_orphan {
                // Delete-before-unlink (ADR-0024 invariant 3): the
                // lifecycle coordinator makes the deleted-marker write
                // durable BEFORE the .dat files are unlinked. A crash
                // between the two leaves a Deleted marker + an orphan
                // `.dat`, which the reaper sweeps on the next cycle
                // (ADR-0025 crash-window row 6 is unrepresentable).
                match self.lifecycle.request_delete(*segment_id).await {
                    Ok(()) => {}
                    // The durable deletion already happened (this
                    // segment was deleted by an earlier run whose
                    // unlink never completed) — safe to unlink.
                    Err(TransitionError::AlreadyDeleted) | Err(TransitionError::Missing) => {}
                    Err(e) => {
                        // The deletion is NOT durable: unlinking the
                        // shards would violate delete-before-unlink.
                        // Retry on the next cycle.
                        tracing::warn!(
                            segment_id = %segment_id,
                            error = %e,
                            "failed to persist orphan deletion; shard unlink deferred"
                        );
                        continue;
                    }
                }

                // Delete shard data from disk after the deletion is
                // durable — from the root the file was listed in (or the
                // registry entry's pool id; ADR-0029 f5).
                match self.store.delete_shards_with_pool(segment_id, *pool_id).await {
                    Ok(bytes) => {
                        tracing::info!(
                            segment_id = %segment_id,
                            bytes_reclaimed = bytes,
                            "deleted orphan segment shards"
                        );
                        stats.bytes_reclaimed += bytes;
                    }
                    Err(e) => {
                        // Log but continue — metadata deletion already
                        // happened. The orphan segment's shards may
                        // already be gone, or the leftover `.dat` is
                        // swept by the next cycle.
                        tracing::warn!(
                            error = %e,
                            segment_id = %segment_id,
                            "failed to delete orphan segment shards, continuing"
                        );
                    }
                }

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

        // [review][architectural][high]
        // i am a bit worried on reading the list of all object on a prodcution sitting with millions of records
        // we should discuss about it and elaborate an eventual strategy
        // [end]
        // Scan EVERY bucket: a per-bucket scan would classify every
        // segment owned by other buckets as an orphan and delete live
        // data (e.g. the load-test bucket in Phase 2 runs).
        let all_objects = self.metadata.list_objects_all();

        for obj in all_objects.into_iter().flatten() {
            for chunk in &obj.chunks {
                referenced.insert(chunk.segment_id);
            }
        }

        Ok(referenced)
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
        BucketId, ChunkRef, Hlc, LifecycleConfig, MetadataConfig, ObjectKey, ObjectMetadata,
        SegmentId, SegmentMetadata, SizeTier, Tombstone,
    };
    use oceanfs_storage::{
        metadata::RocksDbMetadataStore,
        segment::lifecycle::{SegmentLifecycleCoordinator, SegmentLifecycleRegistry},
    };

    use super::super::{
        config::tier_target_size, garbage_collector::InMemoryShardStore,
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

    fn test_shard_store() -> Arc<InMemoryShardStore> {
        Arc::new(InMemoryShardStore::new(tier_target_size(SizeTier::Standard)))
    }

    /// Constructs a reaper whose coordinator is seeded from the store
    /// (mirroring the node's startup seed): orphan candidates seeded
    /// through the machine must be visible to the coordinator's
    /// `request_delete` validation.
    async fn make_reaper(
        metadata: Arc<RocksDbMetadataStore>,
        store: Arc<InMemoryShardStore>,
        config: GcConfig,
        registry: Arc<SegmentLifecycleRegistry>,
    ) -> OrphanReaper {
        let tmp = tempfile::TempDir::new().unwrap();
        let event_wal = Arc::new(
            oceanfs_storage::segment::event_wal::EventWal::open(
                tmp.path().join("event-wal"),
                &oceanfs_core::EventWalConfig {
                    event_wal_dir: tmp.path().join("event-wal"),
                    event_wal_file_size_bytes: 1024 * 1024,
                    event_wal_fsync_batch_timeout_ms: 10,
                    event_wal_checkpoint_bytes: 1024 * 1024,
                },
            )
            .await
            .unwrap(),
        );
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::with_registry(Arc::clone(&registry))
                .with_event_wal(event_wal),
        );
        OrphanReaper::new(metadata, lifecycle, store, vec![], config)
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
            pool_id: 0,
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: tier,
            // A sealed entry carries its seal-time anchor (the event log
            // requires the root at seal time).
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xAB; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(sealed_at),
        }
    }

    // OrphanReaper
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn orphan_reaper_constructor() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let _reaper =
            make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
    }

    #[tokio::test]
    async fn orphan_reaper_empty_store() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 0);
        assert_eq!(stats.orphans_found, 0);
    }

    #[tokio::test]
    async fn segment_with_one_reference_not_orphan() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        let obj_meta = make_object_meta(
            "alive.txt",
            500,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 500,
                compressed: false,
                logical_length: 500,
            },
        );
        metadata.put_object(obj_meta).unwrap();

        let store = test_shard_store();
        let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 1);
        assert_eq!(stats.orphans_found, 0);
    }

    #[tokio::test]
    async fn object_in_non_default_bucket_keeps_segment_alive() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        // Regression: the referenced set must scan ALL buckets. A
        // per-bucket scan (e.g. "default" only) classifies every segment
        // owned by other buckets as an orphan and deletes live data —
        // this is what lost Phase 2 pre-crash objects on restart.
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        // Object lives in the "load-test" bucket, not "default".
        let obj_meta = make_object_meta(
            "hot-1",
            500,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 500,
                compressed: false,
                logical_length: 500,
            },
        );
        metadata.put_object_in_bucket(&BucketId::new("load-test"), obj_meta).unwrap();

        let store = test_shard_store();
        let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 1);
        assert_eq!(stats.orphans_found, 0, "referenced segment must not be reaped");
    }

    #[tokio::test]
    async fn segment_with_zero_references_is_orphan() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        // Segment was sealed very long ago (before TTL)
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap(); // No object references this segment

        let store = test_shard_store();
        let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 1);
        assert_eq!(stats.orphans_found, 1);
    }

    #[tokio::test]
    async fn segment_too_young_not_orphan() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        // Seal time is very recent (within TTL)
        let now_ms =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
                as i64;
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, now_ms);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap(); // No object references this segment

        let store = test_shard_store();
        let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 1);
        // Should not be considered orphan because it's too young
        assert_eq!(stats.orphans_found, 0);
    }

    #[tokio::test]
    async fn empty_segments_cf_yields_no_orphans() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 0);
    }

    #[tokio::test]
    async fn orphan_deletion_removes_segment_metadata() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        // Verify segment exists before reaper runs
        assert!(registry.get(seg_id).is_some());

        let store = test_shard_store();
        let reaper =
            make_reaper(metadata.clone(), store, GcConfig::default(), Arc::clone(&registry)).await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 1);
        assert_eq!(stats.orphans_deleted, 1);

        // Verify segment metadata was actually deleted
        assert!(registry.get(seg_id).is_none());
    }

    #[tokio::test]
    async fn orphan_deletion_deletes_shards_from_disk() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        let store = Arc::new(InMemoryShardStore::new(4194304));
        let reaper =
            make_reaper(metadata, store.clone(), GcConfig::default(), Arc::clone(&registry)).await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 1);
        assert_eq!(stats.orphans_deleted, 1);
        assert_eq!(stats.bytes_reclaimed, 4194304);

        // Verify the shard store recorded the deletion
        assert!(store.is_deleted(seg_id));
    }

    #[tokio::test]
    async fn orphan_deletion_reports_bytes_reclaimed() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        // Create 3 orphan segments
        let mut seg_ids = Vec::new();
        for _ in 0..3 {
            let seg_id = SegmentId::new();
            let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
            registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
            registry.seal(seg_meta.segment_id, seg_meta).unwrap();
            seg_ids.push(seg_id);
        }

        let store = test_shard_store();
        let standard_size = tier_target_size(SizeTier::Standard);
        let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 3);
        assert_eq!(stats.orphans_deleted, 3);
        assert_eq!(stats.bytes_reclaimed, standard_size * 3);
    }

    #[tokio::test]
    async fn all_objects_deleted_segment_becomes_orphan() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        // Object references this segment, but has a tombstone past TTL
        let obj_meta = make_object_meta(
            "deleted_obj.txt",
            500,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 500,
                compressed: false,
                logical_length: 500,
            },
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
                Tombstone {
                    deletion_time: 1000000000000,
                    hlc: Hlc::new(1000000000000, 1),
                    chunks: smallvec::SmallVec::new(),
                },
            )
            .unwrap();

        let store = test_shard_store();
        let reaper =
            make_reaper(metadata.clone(), store, GcConfig::default(), Arc::clone(&registry)).await;
        let stats = reaper.run_cycle().await.unwrap();
        // The object was deleted so no chunks reference the segment → orphan
        assert_eq!(stats.orphans_found, 1);
        assert_eq!(stats.orphans_deleted, 1);
        assert!(registry.get(seg_id).is_none());
    }

    #[tokio::test]
    async fn double_check_correctly_identifies_referenced_segments() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        // The double-check re-uses the cycle's referenced-set snapshot
        // (one metadata scan per cycle — the per-orphan full rescan
        // scaled O(orphans × metadata) and stalled the reaper). This
        // test validates the SET semantics the double-check relies on.
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        let store = test_shard_store();
        let reaper =
            make_reaper(metadata.clone(), store, GcConfig::default(), Arc::clone(&registry)).await;

        // Initially unreferenced — would be an orphan candidate
        assert!(!reaper.build_referenced_set().unwrap().contains(&seg_id));

        // Simulate a write that happened before the cycle's snapshot:
        // an object referencing the segment is inserted.
        let obj_meta = make_object_meta(
            "concurrent.txt",
            100,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 100,
                compressed: false,
                logical_length: 100,
            },
        );
        metadata.put_object(obj_meta).unwrap();

        // A fresh snapshot now sees it as referenced — the cycle's
        // Phase-1 scan (which runs after this insert) would classify
        // the segment as live, not an orphan.
        assert!(reaper.build_referenced_set().unwrap().contains(&seg_id));

        // Run the full cycle. The segment is now referenced, so
        // it should NOT be detected as orphan during scan.
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 0);
        assert_eq!(stats.orphans_deleted, 0);
        assert!(registry.get(seg_id).is_some());
    }

    /// The on-disk sweep (the fleet disk-fill fix): a segment `.dat`
    /// file the lifecycle registry does NOT know — the replication
    /// receiver's unregistered appends — must be reclaimed when no
    /// object row references it. The registry-only scan never saw
    /// these files (~10k unregistered vs 32 registered on the fleet).
    #[tokio::test]
    async fn sweeps_unregistered_on_disk_segments() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        // Simulate the receiver's raw write: a .dat file with NO
        // registry entry and NO object-row reference. The InMemory
        // store's listing is empty, so use the DISK store directly
        // (pools-only since ADR-0031 D2: one data pool whose root is
        // the scan directory).
        let dir = tempfile::tempdir().unwrap();
        let data_root = dir.path().join("pool-data");
        std::fs::create_dir_all(&data_root).unwrap();
        let storage = oceanfs_core::StorageConfig {
            pools: vec![
                oceanfs_core::StoragePoolConfig {
                    name: "data-0".into(),
                    role: oceanfs_core::PoolRole::Data,
                    root: data_root.clone(),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
                oceanfs_core::StoragePoolConfig {
                    name: "wal-0".into(),
                    role: oceanfs_core::PoolRole::Wal,
                    root: dir.path().join("pool-wal"),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
                oceanfs_core::StoragePoolConfig {
                    name: "meta-0".into(),
                    role: oceanfs_core::PoolRole::Metadata,
                    root: dir.path().join("pool-meta"),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
                oceanfs_core::StoragePoolConfig {
                    name: "hints-0".into(),
                    role: oceanfs_core::PoolRole::Hints,
                    root: dir.path().join("pool-hints"),
                    weight: Some(1),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                },
            ],
            missing_root_policy: oceanfs_core::MissingRootPolicy::Degraded,
        };
        let pool_registry = Arc::new(
            oceanfs_storage::PoolRegistry::from_config(&storage, &dir.path().join("data"))
                .expect("registry"),
        );
        let data_pools = pool_registry.data_pools();
        // The unified store (ADR-0032 D2): per-root listing + explicit-
        // pool unlink need no registry entries — exactly the reaper's
        // sweep shape for registry-UNKNOWN files.
        let lifecycle_registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let observer = Arc::new(oceanfs_storage::io::IoObserver::new());
        observer.register_pool(0, None);
        let disk_store = Arc::new(oceanfs_storage::DiskSegmentStore::new(
            pool_registry,
            lifecycle_registry,
            Arc::new(oceanfs_storage::io::InMemorySegmentReader::new()),
            oceanfs_storage::io::IoReadMode::Buffered,
            Arc::new(oceanfs_storage::io::IoBackend::default()),
            observer,
        ));
        let unregistered = SegmentId::new();
        let mtime =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64 - 60_000; // older than the 5s TTL
        std::fs::write(data_root.join(format!("{unregistered}.dat")), vec![0xAB; 100]).unwrap();
        // Set the mtime to the past so the TTL gate passes.
        let past = SystemTime::now() - std::time::Duration::from_secs(60);
        let _ = std::fs::File::options()
            .write(true)
            .open(data_root.join(format!("{unregistered}.dat")))
            .and_then(|f| f.set_modified(past));
        let _ = mtime;

        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::with_registry(Arc::clone(&registry)).with_event_wal(
                Arc::new(
                    oceanfs_storage::segment::event_wal::EventWal::open(
                        tempfile::tempdir().unwrap().path().join("event-wal"),
                        &oceanfs_core::EventWalConfig {
                            event_wal_dir: tempfile::tempdir().unwrap().path().join("event-wal"),
                            event_wal_file_size_bytes: 1024 * 1024,
                            event_wal_fsync_batch_timeout_ms: 10,
                            event_wal_checkpoint_bytes: 1024 * 1024,
                        },
                    )
                    .await
                    .unwrap(),
                ),
            ),
        );
        // Load-profile-like config: a 5s tombstone TTL (the default is
        // 3 days — the file-scan's mtime gate would never pass).
        let config = GcConfig::new(10, 5, 0.5, 4, 64);
        // The reaper sweeps the data pool roots it is injected with
        // (ADR-0032 D1 per-root listing) — the same pool Arcs the disk
        // store resolves through.
        let reaper = OrphanReaper::new(metadata, lifecycle, disk_store.clone(), data_pools, config);
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 1, "the unregistered .dat must be found");
        assert_eq!(stats.orphans_deleted, 1);
        assert!(
            !data_root.join(format!("{unregistered}.dat")).exists(),
            "the unregistered .dat must be swept"
        );
    }

    #[tokio::test]
    async fn start_background_spawns_and_can_be_cancelled() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let reaper = Arc::new(
            make_reaper(
                metadata,
                store,
                GcConfig { interval_sec: 3600, ..GcConfig::default() },
                Arc::clone(&registry),
            )
            .await,
        );

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
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        // This test models: create segment with objects → delete all objects
        // → run reaper with short TTL → segment becomes orphan.
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

        let seg_id = SegmentId::new();
        // Sealed very long ago (well past any TTL)
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1000000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        // Create object referencing this segment
        let obj_key = ObjectKey::new("wholly_deleted.txt");
        let obj_meta = make_object_meta(
            "wholly_deleted.txt",
            300,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 300,
                compressed: false,
                logical_length: 300,
            },
        );
        metadata.put_object(obj_meta).unwrap();

        // Verify the segment is referenced (not orphan yet)
        let store = test_shard_store();
        {
            let reaper = make_reaper(
                metadata.clone(),
                store.clone(),
                GcConfig::default(),
                Arc::clone(&registry),
            )
            .await;
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
                Tombstone {
                    deletion_time: 1000000000000,
                    hlc: Hlc::new(1000000000000, 1),
                    chunks: smallvec::SmallVec::new(),
                },
            )
            .unwrap();

        // After object deletion, the segment is no longer referenced → orphan
        let reaper =
            make_reaper(metadata.clone(), store, GcConfig::default(), Arc::clone(&registry)).await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 1, "segment should be orphan after all objects deleted");
        assert_eq!(stats.orphans_deleted, 1);
        // Verify segment metadata is gone
        assert!(registry.get(seg_id).is_none());
    }

    #[tokio::test]
    async fn segment_with_object_deleted_but_too_young_tombstone_not_orphan() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
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
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        // Object is deleted (segment becomes unreferenced)
        let obj_meta = make_object_meta(
            "recently_deleted.txt",
            100,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 100,
                compressed: false,
                logical_length: 100,
            },
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
        let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
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

    #[tokio::test]
    async fn referenced_set_contains_segment_ids() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = RocksDbMetadataStore::open(&test_config()).unwrap();

        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        let obj_meta = make_object_meta(
            "included.txt",
            100,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 100,
                compressed: false,
                logical_length: 100,
            },
        );
        metadata.put_object(obj_meta).unwrap();

        let store = test_shard_store();
        let reaper =
            make_reaper(Arc::new(metadata), store, GcConfig::default(), Arc::clone(&registry))
                .await;
        let referenced = reaper.build_referenced_set().unwrap();
        assert!(referenced.contains(&seg_id));
    }

    // -----------------------------------------------------------------------

    // referenced-set snapshot
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn referenced_set_empty_for_no_objects() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
        assert!(!reaper.build_referenced_set().unwrap().contains(&SegmentId::new()));
    }

    // -----------------------------------------------------------------------

    // Tombstone TTL enforcement
    // -----------------------------------------------------------------------

    /// Verifies that a tombstone created recently (within TTL) is NOT marked
    /// as dead by the liveness tracker. This prevents immediate reclamation
    /// of objects that may have been deleted by a client error.
    #[test]
    fn process_tombstones_respects_ttl() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = RocksDbMetadataStore::open(&test_config()).unwrap();

        // Create a segment and object
        let seg_id = SegmentId::new();
        let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
        let obj_meta = make_object_meta(
            "recently_deleted.txt",
            500,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 500,
                compressed: false,
                logical_length: 500,
            },
        );
        metadata.put_object(obj_meta).unwrap();

        // Create a tombstone with deletion_time = now (very recent)
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        let bucket = BucketId::new("default");
        metadata
            .put_tombstone(
                &bucket,
                &ObjectKey::new("recently_deleted.txt"),
                Tombstone {
                    deletion_time: now_ms,
                    hlc: Hlc::new(now_ms as u64, 1),
                    chunks: smallvec::SmallVec::new(),
                },
            )
            .unwrap();

        // With a long TTL (1 year in seconds), the tombstone should be too young
        let gc =
            GarbageCollector::new(GcConfig { tombstone_ttl_sec: 31536000, ..GcConfig::default() });

        let mut tracker = LivenessTracker::new();
        let mut stats = GcStats::default();
        let (dead_keys, _) =
            gc.process_tombstones(&metadata, &registry, &mut tracker, &mut stats).unwrap();

        // The tombstone is within TTL, so it should NOT be in the dead set
        assert!(!dead_keys.contains(&("default".to_string(), "recently_deleted.txt".to_string())));
        // And the chunk should NOT be marked dead
        assert_eq!(tracker.dead_bytes_for(&seg_id), 0);
    }

    // --- Metrics tests ---

    #[tokio::test]
    async fn orphan_reaper_metrics_created_and_increment() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
        assert_eq!(reaper.orphans_deleted_total.get(), 0);
        assert_eq!(reaper.bytes_reclaimed_total.get(), 0);

        reaper.orphans_deleted_total.add(5);
        reaper.bytes_reclaimed_total.add(4096);

        assert_eq!(reaper.orphans_deleted_total.get(), 5);
        assert_eq!(reaper.bytes_reclaimed_total.get(), 4096);
    }
}
