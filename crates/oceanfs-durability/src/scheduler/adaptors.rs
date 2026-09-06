//! Tier-1 task adaptors (ADR-0017 f1/f3).
//!
//! Each adaptor wraps a real durability worker and implements
//! [`DurabilityTask`] by delegating to the worker's `run_cycle`, mapping its
//! stats to the trait's "items processed" count. The four adaptors here are
//! the Tier-1 (housekeeping) members of the two-tier budget (ADR-0017
//! amendment): GC, orphan reaper, scrub, and AE.
//!
//! ## Scan shape and `keyspace_fraction == 1.0` (f3)
//!
//! All four adaptors run **full-space passes** (`KeyspaceWindow::Full`).
//! GC/orphan liveness is attributeable only at full-registry granularity
//! (ADR-0034 accounting — no `MetadataStore` range-scan API exists yet), so a
//! per-cycle fraction would multiply whole passes per unit time. Naive
//! sharding would make the periodic scans strictly worse — the mechanism
//! ships inert (see the f3 feature doc). Each adaptor therefore asserts the
//! `Full` window and rejects any `Shard` window with a loud internal error so
//! a wiring bug cannot silently run an unsharded scan labeled as sharded.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry;
use oceanfs_storage_api::{MetadataStore, SegmentDataStore};

use crate::{
    scheduler::task::{DurabilityTask, KeyspaceWindow},
    AntiEntropy, Error, GarbageCollector, OrphanReaper, Result, ScrubCoordinator,
};

/// Asserts a `Full` window. Shard-aware wiring does not exist for the four
/// Tier-1 adaptors (f3) — a `Shard` window is an internal error.
fn assert_full(task: &str, window: KeyspaceWindow) -> Result<()> {
    match window {
        KeyspaceWindow::Full => Ok(()),
        KeyspaceWindow::Shard { index, total } => Err(Error::Internal(format!(
            "{task} is registered with keyspace_fraction = 1.0 (full pass — see f3) \
             but received KeyspaceWindow::Shard {{ index: {index}, total: {total} }}"
        ))),
    }
}

/// Tier-1 adaptor for the garbage collector.
///
/// Delegates to [`GarbageCollector::run_cycle`] over the full registry +
/// accounting passes (ADR-0034). Registered with `keyspace_fraction() ==
/// 1.0` (see the module doc for the scan-shape constraint).
pub struct GcTask {
    /// The GC worker.
    gc: Arc<GarbageCollector>,
    /// Metadata store the GC worker scans.
    metadata: Arc<dyn MetadataStore>,
    /// ADR-0025 lifecycle registry — the segment set the worker sweeps.
    registry: Arc<SegmentLifecycleRegistry>,
    /// Cadence (`gc_interval_sec`, captured at construction).
    interval: Duration,
}

impl GcTask {
    /// Creates a GC Tier-1 task.
    pub fn new(
        gc: Arc<GarbageCollector>,
        metadata: Arc<dyn MetadataStore>,
        registry: Arc<SegmentLifecycleRegistry>,
        interval: Duration,
    ) -> Self {
        Self { gc, metadata, registry, interval }
    }
}

#[async_trait]
impl DurabilityTask for GcTask {
    fn name(&self) -> &'static str {
        "gc"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn keyspace_fraction(&self) -> f64 {
        1.0
    }

    async fn run_cycle(&self, window: KeyspaceWindow) -> Result<u64> {
        assert_full(self.name(), window)?;
        let stats = self.gc.run_cycle(self.metadata.clone(), self.registry.as_ref()).await?;
        Ok(stats.segments_scanned)
    }
}

/// Tier-1 adaptor for the orphan reaper.
///
/// Delegates to [`OrphanReaper::run_cycle`] (byte-accounting fully-dead
/// detection, ADR-0034 D4). Registered with `keyspace_fraction() == 1.0`.
pub struct OrphanTask {
    /// The orphan reaper worker.
    reaper: Arc<OrphanReaper>,
    /// Cadence (`orphan_reaper_interval_sec`, captured at construction).
    interval: Duration,
}

impl OrphanTask {
    /// Creates an orphan-reaper Tier-1 task.
    pub fn new(reaper: Arc<OrphanReaper>, interval: Duration) -> Self {
        Self { reaper, interval }
    }
}

#[async_trait]
impl DurabilityTask for OrphanTask {
    fn name(&self) -> &'static str {
        "orphan_reaper"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn keyspace_fraction(&self) -> f64 {
        1.0
    }

    async fn run_cycle(&self, window: KeyspaceWindow) -> Result<u64> {
        assert_full(self.name(), window)?;
        let stats = self.reaper.run_cycle().await?;
        Ok(stats.segments_scanned)
    }
}

/// Tier-1 adaptor for the scrub coordinator.
///
/// Delegates to [`ScrubCoordinator::run_cycle`] over the unified store.
/// Registered with `keyspace_fraction() == 1.0`; scrub partitions by alive
/// nodes (H5), not keyspace fraction.
pub struct ScrubTask {
    /// The scrub coordinator.
    scrub: Arc<ScrubCoordinator>,
    /// ADR-0025 lifecycle registry the scrub cycle enumerates.
    registry: Arc<SegmentLifecycleRegistry>,
    /// The unified segment data store the scrub cycle verifies against.
    data_store: Arc<dyn SegmentDataStore>,
    /// Cadence (`scrub_interval_sec`, captured at construction).
    interval: Duration,
}

impl ScrubTask {
    /// Creates a scrub Tier-1 task.
    pub fn new(
        scrub: Arc<ScrubCoordinator>,
        registry: Arc<SegmentLifecycleRegistry>,
        data_store: Arc<dyn SegmentDataStore>,
        interval: Duration,
    ) -> Self {
        Self { scrub, registry, data_store, interval }
    }
}

#[async_trait]
impl DurabilityTask for ScrubTask {
    fn name(&self) -> &'static str {
        "scrub"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn keyspace_fraction(&self) -> f64 {
        1.0
    }

    async fn run_cycle(&self, window: KeyspaceWindow) -> Result<u64> {
        assert_full(self.name(), window)?;
        let report = self.scrub.run_cycle(self.registry.clone(), self.data_store.clone()).await?;
        Ok(report.segments_total())
    }
}

/// Tier-1 adaptor for anti-entropy.
///
/// Preserves the dispatch today's spawn loop makes: continuous mode runs
/// [`AntiEntropy::run_continuous_cycle`], otherwise
/// [`AntiEntropy::run_cycle`]. AE keeps its ADR-0015 continuous/sampling
/// internals — it is registered with `keyspace_fraction() == 1.0` and is NOT
/// keyspace-sharded.
pub struct AeTask {
    /// The anti-entropy worker.
    ae: Arc<AntiEntropy>,
    /// Cadence (`ae_interval_sec`, captured at construction).
    interval: Duration,
}

impl AeTask {
    /// Creates an anti-entropy Tier-1 task.
    pub fn new(ae: Arc<AntiEntropy>, interval: Duration) -> Self {
        Self { ae, interval }
    }
}

#[async_trait]
impl DurabilityTask for AeTask {
    fn name(&self) -> &'static str {
        "anti_entropy"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn keyspace_fraction(&self) -> f64 {
        1.0
    }

    async fn run_cycle(&self, window: KeyspaceWindow) -> Result<u64> {
        assert_full(self.name(), window)?;
        let continuous = self.ae.config().core().continuous_enabled;
        let stats = if continuous {
            self.ae.run_continuous_cycle().await?
        } else {
            self.ae.run_cycle().await?
        };
        Ok(stats.segments_compared)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
mod tests {
    use std::io;

    use oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator;

    use super::*;

    /// The `Shard` guard fires before any delegation (f3) — proven with an
    /// erroring metadata double that would be hit if delegation happened.
    #[test]
    fn shard_window_is_rejected_before_delegation() {
        let task = GcTask::new(
            Arc::new(GarbageCollector::new(crate::GcConfig::default())),
            Arc::new(UnreachableMetadata),
            Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default())),
            Duration::from_secs(60),
        );
        let window = KeyspaceWindow::Shard { index: 0, total: 4 };
        let rt =
            tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
        let res = rt.block_on(task.run_cycle(window));
        assert!(matches!(res, Err(Error::Internal(_))));
    }

    /// The full window passes the guard and delegates to the worker; on an
    /// empty store the GC worker returns a zero-stat cycle without touching
    /// the (unreachable) metadata double.
    #[test]
    fn full_window_is_accepted() {
        let task = GcTask::new(
            Arc::new(GarbageCollector::new(crate::GcConfig::default())),
            Arc::new(UnreachableMetadata),
            Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default())),
            Duration::from_secs(60),
        );
        let window = KeyspaceWindow::Full;
        let rt =
            tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
        // The guard passes and the worker runs (empty registry => 0 scanned).
        let res = rt.block_on(task.run_cycle(window));
        assert_eq!(res.expect("Full window must delegate"), 0);
    }

    /// Fraction values are 1.0 for every Tier-1 adaptor.
    #[test]
    fn adaptors_report_full_keyspace_fraction() {
        let metadata: Arc<dyn MetadataStore> = Arc::new(UnreachableMetadata);
        let gc_task = GcTask::new(
            Arc::new(GarbageCollector::new(crate::GcConfig::default())),
            Arc::clone(&metadata),
            Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default())),
            Duration::from_secs(60),
        );
        let orphan_task = OrphanTask::new(
            Arc::new(OrphanReaper::new(
                Arc::clone(&metadata),
                Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                    &oceanfs_core::LifecycleConfig::default(),
                )),
                Arc::new(crate::anti_entropy::InMemorySegmentStore::new()),
                crate::GcConfig::default(),
            )),
            Duration::from_secs(60),
        );
        let scrub_task = ScrubTask::new(
            Arc::new(ScrubCoordinator::new(crate::ScrubConfig::default())),
            Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default())),
            Arc::new(crate::anti_entropy::InMemorySegmentStore::new()),
            Duration::from_secs(60),
        );

        assert_eq!(gc_task.keyspace_fraction(), 1.0);
        assert_eq!(orphan_task.keyspace_fraction(), 1.0);
        assert_eq!(scrub_task.keyspace_fraction(), 1.0);
        assert_eq!(gc_task.name(), "gc");
        assert_eq!(orphan_task.name(), "orphan_reaper");
        assert_eq!(scrub_task.name(), "scrub");
    }

    /// A test-only metadata store that errors on any call — used to prove
    /// the `Shard` guard fires before delegation.
    struct UnreachableMetadata;

    fn unreachable() -> io::Error {
        io::Error::new(io::ErrorKind::Other, "unreachable")
    }

    impl MetadataStore for UnreachableMetadata {
        fn list_object_keys(
            &self,
            _bucket: &oceanfs_core::BucketId,
        ) -> io::Result<Vec<(oceanfs_core::BucketId, oceanfs_core::ObjectKey)>> {
            Err(unreachable())
        }

        fn get_object_metadata(
            &self,
            _bucket: &oceanfs_core::BucketId,
            _key: &oceanfs_core::ObjectKey,
        ) -> io::Result<Option<oceanfs_core::ObjectMetadata>> {
            Err(unreachable())
        }

        fn list_objects(
            &self,
            _bucket: &oceanfs_core::BucketId,
            _prefix: &str,
        ) -> Vec<io::Result<oceanfs_core::ObjectMetadata>> {
            vec![Err(unreachable())]
        }

        fn list_tombstones(
            &self,
            _bucket: &oceanfs_core::BucketId,
        ) -> Vec<io::Result<(oceanfs_core::ObjectKey, oceanfs_core::Tombstone)>> {
            vec![Err(unreachable())]
        }

        fn delete_tombstone(
            &self,
            _bucket: &oceanfs_core::BucketId,
            _key: &oceanfs_core::ObjectKey,
        ) -> io::Result<()> {
            Err(unreachable())
        }

        fn put_object(
            &self,
            _bucket: &oceanfs_core::BucketId,
            _meta: oceanfs_core::ObjectMetadata,
        ) -> io::Result<()> {
            Err(unreachable())
        }

        fn delete_object(
            &self,
            _bucket: &oceanfs_core::BucketId,
            _key: &oceanfs_core::ObjectKey,
            _hlc: oceanfs_core::Hlc,
        ) -> io::Result<()> {
            Err(unreachable())
        }

        fn batch_write(&self, _ops: Vec<oceanfs_storage_api::BatchOp>) -> io::Result<()> {
            Err(unreachable())
        }

        fn list_dead_chunk_records_all(
            &self,
        ) -> Vec<
            io::Result<(
                oceanfs_core::BucketId,
                oceanfs_core::ObjectKey,
                oceanfs_core::DeadChunkRecord,
            )>,
        > {
            vec![Err(unreachable())]
        }
    }

    // ---------------------------------------------------------------------
    // Real-store behavior pins (f1/f3 DoD): running an adaptor over a
    // seeded RocksDB fixture must produce byte-identical results to calling
    // the worker's `run_cycle` directly.
    // ---------------------------------------------------------------------

    fn rocks_metadata() -> Arc<dyn MetadataStore> {
        let dir = tempfile::tempdir().expect("tmpdir");
        let store =
            oceanfs_storage::metadata::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().to_path_buf(),
                block_cache_size: 8 * 1024 * 1024,
                memtable_size: 8 * 1024 * 1024,
                ..Default::default()
            })
            .expect("open rocksdb");
        Arc::new(store)
    }

    fn seeded_registry(segment_count: u64) -> Arc<SegmentLifecycleRegistry> {
        use oceanfs_core::{SegmentId, SegmentMetadata};
        let registry =
            Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
        for _i in 0..segment_count {
            let id = SegmentId::new();
            let meta = SegmentMetadata {
                pool_id: 0,
                total_bytes: 1000,
                segment_id: id,
                ec_k: 4,
                ec_m: 2,
                size_tier: oceanfs_core::SizeTier::Standard,
                merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xAB; 32])),
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(1_000_000_000_000),
            };
            registry.reserve(id, meta.clone()).expect("reserve");
            registry.seal(id, meta).expect("seal");
        }
        registry
    }

    async fn lifecycle_over(
        registry: Arc<SegmentLifecycleRegistry>,
    ) -> Arc<SegmentLifecycleCoordinator> {
        let tmp = tempfile::TempDir::new().expect("tmpdir");
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
            .expect("event wal"),
        );
        Arc::new(
            oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::with_registry(
                registry,
            )
            .with_event_wal(event_wal),
        )
    }

    /// Orphan adaptor Full-window cycle == the worker's direct cycle on the
    /// same seeded store (scans + finds identical).
    #[tokio::test]
    async fn orphan_behavior_pin_full_matches_direct() {
        let registry = seeded_registry(2);
        let metadata = rocks_metadata();
        let store: Arc<dyn SegmentDataStore> =
            Arc::new(crate::anti_entropy::InMemorySegmentStore::new());
        let reaper = Arc::new(OrphanReaper::new(
            metadata,
            lifecycle_over(registry.clone()).await,
            store,
            crate::GcConfig::default(),
        ));
        let task = OrphanTask::new(Arc::clone(&reaper), Duration::from_secs(60));

        let direct = reaper.run_cycle().await.expect("direct run_cycle");
        let through_task = task.run_cycle(KeyspaceWindow::Full).await.expect("adaptor run_cycle");
        assert_eq!(through_task, direct.segments_scanned);
        assert_eq!(direct.orphans_found, 0, "no dead records seeded");
    }

    /// GC adaptor Full-window cycle == the worker's direct cycle on the same
    /// seeded store (scanned counts identical).
    #[tokio::test]
    async fn gc_behavior_pin_full_matches_direct() {
        let registry = seeded_registry(2);
        let metadata = rocks_metadata();
        let store: Arc<dyn SegmentDataStore> =
            Arc::new(crate::anti_entropy::InMemorySegmentStore::new());
        let gc = Arc::new(
            GarbageCollector::new(crate::GcConfig::default())
                .with_data_store(Arc::clone(&store))
                .with_lifecycle(lifecycle_over(registry.clone()).await),
        );
        let task = GcTask::new(
            Arc::clone(&gc),
            metadata.clone(),
            registry.clone(),
            Duration::from_secs(60),
        );

        let direct = gc.run_cycle(metadata, &registry).await.expect("direct run_cycle");
        let through_task = task.run_cycle(KeyspaceWindow::Full).await.expect("adaptor run_cycle");
        assert_eq!(through_task, direct.segments_scanned);
        assert_eq!(direct.segments_scanned, 2, "both sealed segments scanned");
    }

    /// OrphanTask rejects a `Shard` window loudly (f3 guard).
    #[tokio::test]
    async fn orphan_rejects_shard_window() {
        let metadata = rocks_metadata();
        let store: Arc<dyn SegmentDataStore> =
            Arc::new(crate::anti_entropy::InMemorySegmentStore::new());
        let registry =
            Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
        let reaper = Arc::new(OrphanReaper::new(
            metadata,
            lifecycle_over(registry).await,
            store,
            crate::GcConfig::default(),
        ));
        let task = OrphanTask::new(reaper, Duration::from_secs(60));
        let res = task.run_cycle(KeyspaceWindow::Shard { index: 0, total: 4 }).await;
        assert!(matches!(res, Err(Error::Internal(_))));
    }

    /// ScrubTask rejects a `Shard` window loudly (f3 guard).
    #[tokio::test]
    async fn scrub_rejects_shard_window() {
        let registry =
            Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
        let store: Arc<dyn SegmentDataStore> =
            Arc::new(crate::anti_entropy::InMemorySegmentStore::new());
        let task = ScrubTask::new(
            Arc::new(ScrubCoordinator::new(crate::ScrubConfig::default())),
            registry,
            store,
            Duration::from_secs(60),
        );
        let res = task.run_cycle(KeyspaceWindow::Shard { index: 1, total: 4 }).await;
        assert!(matches!(res, Err(Error::Internal(_))));
    }

    /// AeTask rejects a `Shard` window loudly (f3 guard) before any
    /// delegation.
    #[tokio::test]
    async fn ae_rejects_shard_window() {
        use oceanfs_core::{GossipConfig, NodeId, RingConfig, RpcConfig};
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;
        use oceanfs_routing::{Ring, RingCache};

        let registry =
            Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
        let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
        let membership = Arc::new(Membership::new(
            NodeId::new("n1"),
            "127.0.0.1:9200".parse().expect("addr"),
            "127.0.0.1:9201".parse().expect("addr"),
            GossipConfig::default(),
            ring,
        ));
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let store: Arc<dyn SegmentDataStore> =
            Arc::new(crate::anti_entropy::InMemorySegmentStore::new());
        let tree = Arc::new(crate::merkle::IncrementalMerkleTree::new(
            crate::merkle::MerkleTreeConfig::default(),
        ));
        let ae = Arc::new(AntiEntropy::new(
            crate::AntiEntropyConfig::new(1, 1),
            membership,
            Arc::clone(&registry),
            pool,
            store,
            tree,
        ));
        let task = AeTask::new(ae, Duration::from_secs(60));
        let res = task.run_cycle(KeyspaceWindow::Shard { index: 2, total: 4 }).await;
        assert!(matches!(res, Err(Error::Internal(_))));
    }
    /// Scrub adaptor Full-window cycle == the worker's direct cycle on the
    /// same seeded registry (counts identical).
    #[tokio::test]
    async fn scrub_behavior_pin_full_matches_direct() {
        let registry = seeded_registry(3);
        let store: Arc<dyn SegmentDataStore> =
            Arc::new(crate::anti_entropy::InMemorySegmentStore::new());
        let scrub = Arc::new(ScrubCoordinator::new(crate::ScrubConfig::default()));
        let task = ScrubTask::new(
            Arc::clone(&scrub),
            Arc::clone(&registry),
            Arc::clone(&store),
            Duration::from_secs(60),
        );
        let direct = scrub.run_cycle(registry, store).await.expect("direct run_cycle");
        let through_task = task.run_cycle(KeyspaceWindow::Full).await.expect("adaptor run_cycle");
        assert_eq!(through_task, direct.segments_total());
    }
}
