//! Orphan reaper — fully-dead detection from byte accounting.

// [review][pacement][critical]
// why is the orphan reaper placed under the garbage collection ?
// this is a separate mechanism.
// [end]

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use oceanfs_core::{Counter, LabelSet, MetricRegistrar, SegmentId};
use oceanfs_storage::{Result, SegmentLifecycleCoordinator, TransitionError};
use oceanfs_storage_api::SegmentDataStore;

use super::{config::GcConfig, liveness_tracker::collect_aged_dead_chunk_records};

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
/// An orphaned segment is one whose captured dead bytes reached its
/// seal-time logical total — every object that referenced it has been
/// deleted/overwritten and the delete/supersede grace has elapsed. The
/// reaper derives orphanhood from byte ACCOUNTING (ADR-0034 D4, f2):
/// it iterates the ADR-0025 registry (the segment set) and the aged
/// dead-chunk records (the same feed GC consumes), and NEVER builds a
/// referenced-set from the objects CF and never sweeps the disk for
/// registry-unknown `.dat` files (the legacy phase-2b path is retired —
/// lifecycle registration is enforced everywhere, ADR-0031/0032).
///
/// A segment whose total is 0 ("unknown" — row-3 adopt / repair copies,
/// f3 notes) is never orphaned: without a known total, `dead >= total`
/// is undecidable.
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
    /// The unified segment data store (ADR-0032 D1): the reclaim unlinks
    /// a fully-dead segment's `.dat` under the pool id its registry entry
    /// carried (ADR-0029 f5). No per-root disk listing remains in the
    /// reaper's cycle.
    store: Arc<dyn SegmentDataStore>,
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
        config: GcConfig,
    ) -> Self {
        Self {
            metadata,
            lifecycle,
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
    /// Fully-dead detection from byte accounting (ADR-0034 D4, f2):
    ///
    /// 1. Aggregate `dead_bytes(S)` over the AGED dead-chunk records
    ///    (plain tombstones + supersedes) via the shared accounting feed —
    ///    the same records GC consumes. No objects-CF reference
    ///    set, no per-root disk sweep.
    /// 2. Scan the ADR-0025 registry: a Sealed entry is an orphan iff its
    ///    total is known (`total_bytes > 0`), its captured dead bytes
    ///    reached that total (`dead >= total`), and it is past the TTL
    ///    grace (`now − sealed_at > tombstone_ttl`). A segment with an
    ///    unknown total (0 — row-3 adopt / repair copies, f3 notes) is
    ///    never orphaned.
    /// 3. Delete each orphan: durable `request_delete` through the
    ///    coordinator (ADR-0025 Decision 4), then unlink the `.dat`
    ///    through the store under the entry's pool id (ADR-0024
    ///    invariant 3: delete before unlink).
    ///
    /// # Errors
    ///
    /// Returns an error if metadata or shard-deletion operations fail.
    pub async fn run_cycle(&self) -> Result<OrphanStats> {
        let mut stats = OrphanStats::default();

        let now_ms =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
        let ttl_ms = (self.config.tombstone_ttl_sec * 1000) as i64;

        // Step 1: the aged dead-chunk accounting feed (shared with GC).
        let records = collect_aged_dead_chunk_records(self.metadata.as_ref(), now_ms, ttl_ms);
        let mut dead_bytes: HashMap<SegmentId, u64> = HashMap::with_capacity(records.len());
        for (_bucket, _key, record) in records {
            for chunk in &record.chunks {
                *dead_bytes.entry(chunk.segment_id).or_insert(0) += chunk.length as u64;
            }
        }

        // Step 2: fully-dead candidates over the machine's entries.
        // (segment_id, pool_id) — the pool id names the root holding the
        // `.dat` (ADR-0029 f5; ADR-0031 D2 — there is no legacy dir).
        let mut orphan_ids: Vec<(SegmentId, u32)> = Vec::new();
        self.lifecycle.registry().for_each(|segment_id, entry| {
            stats.segments_scanned += 1;
            let total = entry.metadata.total_bytes;
            // Unknown total: dead >= total is undecidable — never orphan.
            if total == 0 {
                return;
            }
            let Some(sealed_at) = entry.metadata.sealed_at else {
                // Reserved (no seal) — not an orphan candidate.
                return;
            };
            if now_ms - sealed_at <= ttl_ms {
                // The seal TTL grace — a young segment is left alone
                // exactly as before.
                return;
            }
            let dead = dead_bytes.get(&segment_id).copied().unwrap_or(0);
            if dead >= total {
                orphan_ids.push((segment_id, entry.metadata.pool_id));
                stats.orphans_found += 1;
            }
        });

        // Step 3: Reclaim orphans with a bounded snapshot double-check.
        //
        // The double-check re-verifies the cycle's OWN accounting
        // snapshot (dead ≥ total for that segment) — it is a bounded map
        // lookup, never a store rescan and never a referenced-set rebuild.
        // The race a fresh rescan used to close — a row written mid-cycle
        // referencing a just-reaped segment — is closed by construction:
        // sealed segments receive no new appends, and the only row writers
        // (direct PUT, hint apply) reference freshly-appended segments;
        // the read-repair push references the winner's (foreign) segment
        // ids. The durable Deleted marker (delete-before-unlink,
        // ADR-0024) remains the crash guard between request_delete and
        // the unlink.
        for (segment_id, pool_id) in &orphan_ids {
            let total = self
                .lifecycle
                .registry()
                .get(*segment_id)
                .map(|entry| entry.metadata.total_bytes)
                .unwrap_or(0);
            let still_orphan =
                total > 0 && dead_bytes.get(segment_id).copied().unwrap_or(0) >= total;

            if still_orphan {
                // Delete-before-unlink (ADR-0024 invariant 3): the
                // lifecycle coordinator makes the deleted-marker write
                // durable BEFORE the .dat files are unlinked. A crash
                // between the two leaves a Deleted marker + an orphan
                // `.dat`; the registry entry is evicted after the delete
                // grace, and the STARTUP `.dat` residue sweep (modules/
                // storage.rs) reclaims the leftover file — the reaper's
                // former phase-2b periodic disk sweep is retired.
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
                // durable — from the registry entry's pool id
                // (ADR-0029 f5).
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
                        // startup residue-swept.
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
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use oceanfs_core::{
        BucketId, ChunkRef, Hlc, LifecycleConfig, MetadataConfig, ObjectKey, ObjectMetadata,
        SegmentId, SegmentMetadata, SizeTier, Tombstone,
    };
    use oceanfs_storage::{
        metadata::RocksDbMetadataStore,
        segment::lifecycle::{SegmentLifecycleCoordinator, SegmentLifecycleRegistry},
    };

    use super::{
        super::{config::tier_target_size, garbage_collector::InMemoryShardStore, GcConfig},
        *,
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

    /// Constructs a reaper whose coordinator shares the seeded registry,
    /// so `request_delete` validation sees every entry (mirroring the
    /// node's startup seed).
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
        OrphanReaper::new(metadata, lifecycle, store, config)
    }

    fn make_segment_meta(
        id: SegmentId,
        tier: SizeTier,
        sealed_at: i64,
        total_bytes: u64,
    ) -> SegmentMetadata {
        SegmentMetadata {
            pool_id: 0,
            total_bytes,
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: tier,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xAB; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(sealed_at),
        }
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

    /// Seeds a Sealed registry entry through the coordinator's registry
    /// (reserve then seal with the given metadata).
    fn seed_sealed(registry: &SegmentLifecycleRegistry, meta: SegmentMetadata) {
        registry.reserve(meta.segment_id, meta.clone()).unwrap();
        registry.seal(meta.segment_id, meta).unwrap();
    }

    /// Plants an AGED plain tombstone capturing `chunks` — the shape a
    /// `delete_object` leaves behind (chunk-carrying tombstone) but with a
    /// deterministic ancient `deletion_time` so the TTL grace has elapsed.
    fn plant_aged_tombstone(
        metadata: &RocksDbMetadataStore,
        bucket: &str,
        key: &str,
        chunks: smallvec::SmallVec<[ChunkRef; 4]>,
    ) {
        metadata
            .put_tombstone(
                &BucketId::new(bucket),
                &ObjectKey::new(key),
                Tombstone {
                    deletion_time: 1_000_000_000_000, // year 2001 — ancient
                    hlc: Hlc::zero(),
                    chunks,
                },
            )
            .unwrap();
    }

    fn one_chunk(
        segment_id: SegmentId,
        offset: u64,
        length: u32,
    ) -> smallvec::SmallVec<[ChunkRef; 4]> {
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id,
            offset,
            length,
            compressed: false,
            logical_length: length,
        });
        chunks
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
    async fn fully_dead_segment_is_reaped_and_metadata_removed() {
        // D4/D6 "DELETE then idle": an aged tombstone captures every byte
        // of the segment → dead == total → the segment is an orphan and
        // is deleted through the coordinator (metadata removed).
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let seg_id = SegmentId::new();
        seed_sealed(
            &registry,
            make_segment_meta(seg_id, SizeTier::Standard, 1_000_000_000_000, 1000),
        );
        plant_aged_tombstone(&metadata, "default", "gone.txt", one_chunk(seg_id, 0, 1000));

        let reaper =
            make_reaper(metadata, test_shard_store(), GcConfig::default(), Arc::clone(&registry))
                .await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 1);
        assert_eq!(stats.orphans_found, 1);
        assert_eq!(stats.orphans_deleted, 1);
        assert!(registry.get(seg_id).is_none(), "orphan metadata deleted through the machine");
    }

    #[tokio::test]
    async fn fully_dead_segment_shards_deleted_and_bytes_reclaimed() {
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let seg_id = SegmentId::new();
        seed_sealed(
            &registry,
            make_segment_meta(seg_id, SizeTier::Standard, 1_000_000_000_000, 1000),
        );
        plant_aged_tombstone(&metadata, "default", "gone.txt", one_chunk(seg_id, 0, 1000));
        let store = test_shard_store();

        let reaper =
            make_reaper(metadata, Arc::clone(&store), GcConfig::default(), Arc::clone(&registry))
                .await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 1);
        assert_eq!(stats.orphans_deleted, 1);
        assert_eq!(stats.bytes_reclaimed, tier_target_size(SizeTier::Standard));
        assert!(store.is_deleted(seg_id), "orphan shards unlinked after the durable delete");
    }

    #[tokio::test]
    async fn partially_dead_segment_not_reaped() {
        // Dead (400) < total (1000) → not an orphan, even though the dead
        // bytes exist and nothing references the remaining region.
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let seg_id = SegmentId::new();
        seed_sealed(
            &registry,
            make_segment_meta(seg_id, SizeTier::Standard, 1_000_000_000_000, 1000),
        );
        plant_aged_tombstone(&metadata, "default", "part.txt", one_chunk(seg_id, 0, 400));

        let reaper =
            make_reaper(metadata, test_shard_store(), GcConfig::default(), Arc::clone(&registry))
                .await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 1);
        assert_eq!(stats.orphans_found, 0, "dead < total is never an orphan");
        assert!(registry.get(seg_id).is_some());
    }

    #[tokio::test]
    async fn live_object_keeps_segment_alive() {
        // A live object row referencing the segment means its captures
        // never reach total (no delete/overwrite of that object).
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let seg_id = SegmentId::new();
        seed_sealed(
            &registry,
            make_segment_meta(seg_id, SizeTier::Standard, 1_000_000_000_000, 1000),
        );
        let obj = make_object_meta(
            "alive.txt",
            1000,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 1000,
                compressed: false,
                logical_length: 1000,
            },
        );
        metadata.put_object(obj).unwrap();

        let reaper =
            make_reaper(metadata, test_shard_store(), GcConfig::default(), Arc::clone(&registry))
                .await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 0, "a segment with a live object is never an orphan");
    }

    #[tokio::test]
    async fn object_in_non_default_bucket_keeps_segment_alive() {
        // Preserved in spirit: a live object in ANY bucket keeps its
        // segment alive under accounting (its bytes are never captured,
        // so dead can never reach total).
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let seg_id = SegmentId::new();
        seed_sealed(
            &registry,
            make_segment_meta(seg_id, SizeTier::Standard, 1_000_000_000_000, 1000),
        );
        let bucket = BucketId::new("load-test");
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: 1000,
            compressed: false,
            logical_length: 1000,
        });
        metadata
            .put_object_in_bucket(
                &bucket,
                ObjectMetadata {
                    object_key: ObjectKey::new("alive.txt"),
                    size: 1000,
                    blake3_hash: None,
                    chunks,
                    inline_data: None,
                    created_at: 0,
                    hlc: Hlc::zero(),
                },
            )
            .unwrap();

        let reaper =
            make_reaper(metadata, test_shard_store(), GcConfig::default(), Arc::clone(&registry))
                .await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(
            stats.orphans_found, 0,
            "a live object in a non-default bucket keeps its segment alive"
        );
    }

    #[tokio::test]
    async fn unknown_total_segment_never_reaped() {
        // total_bytes == 0 = "unknown" (row-3 adopt / repair copies, f3):
        // dead >= total is undecidable — never orphaned even when a
        // capture references it.
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let seg_id = SegmentId::new();
        seed_sealed(&registry, make_segment_meta(seg_id, SizeTier::Standard, 1_000_000_000_000, 0));
        plant_aged_tombstone(&metadata, "default", "gone.txt", one_chunk(seg_id, 0, 1000));

        let reaper =
            make_reaper(metadata, test_shard_store(), GcConfig::default(), Arc::clone(&registry))
                .await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.segments_scanned, 1);
        assert_eq!(
            stats.orphans_found, 0,
            "an unknown-total segment is never classified fully dead"
        );
    }

    #[tokio::test]
    async fn segment_too_young_not_reaped() {
        // The seal TTL grace: a fully-dead segment sealed recently is left
        // alone — exactly the pre-f2 behavior.
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let seg_id = SegmentId::new();
        let now_ms =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
                as i64;
        seed_sealed(&registry, make_segment_meta(seg_id, SizeTier::Standard, now_ms, 1000));
        plant_aged_tombstone(&metadata, "default", "gone.txt", one_chunk(seg_id, 0, 1000));

        let reaper =
            make_reaper(metadata, test_shard_store(), GcConfig::default(), Arc::clone(&registry))
                .await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 0, "a too-young fully-dead segment keeps the TTL grace");
    }

    #[tokio::test]
    async fn unaged_tombstone_keeps_segment_alive() {
        // The capture TTL gate: a fresh delete (tombstone not yet aged) does
        // not count as dead bytes — the delete grace prevents a read
        // window from reaping a segment whose delete may be reversed.
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let seg_id = SegmentId::new();
        seed_sealed(
            &registry,
            make_segment_meta(seg_id, SizeTier::Standard, 1_000_000_000_000, 1000),
        );
        // Fresh tombstone (deletion_time = now) under the default 3-day TTL.
        metadata
            .put_tombstone(
                &BucketId::new("default"),
                &ObjectKey::new("fresh.txt"),
                Tombstone {
                    deletion_time: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                    hlc: Hlc::zero(),
                    chunks: one_chunk(seg_id, 0, 1000),
                },
            )
            .unwrap();

        let reaper =
            make_reaper(metadata, test_shard_store(), GcConfig::default(), Arc::clone(&registry))
                .await;
        let stats = reaper.run_cycle().await.unwrap();
        assert_eq!(stats.orphans_found, 0, "an unaged delete capture never orphans a segment");
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
                GcConfig::new(3600, 259200, 0.5, 4, 64),
                Arc::clone(&registry),
            )
            .await,
        );
        let handle = reaper.clone().start_background().await;
        handle.abort();
        assert!(
            handle.await.is_err(),
            "aborted background task must finish with a cancellation error"
        );
    }

    #[test]
    fn orphan_stats_defaults() {
        let stats = OrphanStats::default();
        assert_eq!(stats.segments_scanned, 0);
        assert_eq!(stats.orphans_found, 0);
        assert_eq!(stats.orphans_deleted, 0);
        assert_eq!(stats.bytes_reclaimed, 0);
    }

    #[tokio::test]
    async fn orphan_reaper_metrics_created_and_increment() {
        let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
        let store = test_shard_store();
        let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
        let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
        reaper.orphans_deleted_total.add(5);
        reaper.bytes_reclaimed_total.add(4096);
        assert_eq!(reaper.orphans_deleted_total.get(), 5);
        assert_eq!(reaper.bytes_reclaimed_total.get(), 4096);
    }
}
