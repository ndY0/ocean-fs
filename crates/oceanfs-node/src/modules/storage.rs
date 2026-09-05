//! Storage subsystem construction module (c1 of the composition-root
//! decomposition).
//!
//! Extracted from `Node::start()` (feature
//! `docs/features/refactoring/composition-root-decomposition/c1-split-storage-builder.md`):
//! one plain builder returns the [`StorageModule`] bundle owning every
//! storage-side component the rest of the composition root consumes.
//!
//! This is a **pure move** — construction order and behavior are identical
//! to the inline code it replaces; the ADR-0017 scheduler and the
//! store-unification epic (ADR-0032) land later on top of this module.

use std::sync::Arc;

use oceanfs_core::{
    shard, MetricRegistrar, NodeConfig, NodeId, PoolConfig, SegmentId, SegmentSizeConfig, SizeTier,
    WalConfig,
};
use oceanfs_durability::{
    recover_incomplete_compactions, CompactionRecoveryAction, StoreObjectLookup,
};
use oceanfs_storage::{BufferPool, SegmentPool, SegmentShard};
use tracing::{info, warn};

use crate::{
    pool_paths::PoolPaths,
    segment_replicator::{ReplicationConfig, SegmentReplicator},
};

/// Storage subsystem bundle produced by [`StorageModule::build`].
///
/// Owns the pool registry, metadata store, WAL writer, segment lifecycle
/// machinery (registry, coordinator, event WAL + checkpoint), sealer, the
/// **one shared** unified segment store (ADR-0032 — the c1-era two-store
/// pair collapsed into a single `oceanfs_storage::DiskSegmentStore`
/// shared by both roles), the seal-time segment replicator, the segment
/// reader, the I/O observer, and the write-path pools the inline write
/// coordinator + metrics code consumes.
pub(crate) struct StorageModule {
    /// The live storage-pool registry (ADR-0029) — constructed from
    /// `config.storage` before the builder runs; every role-pinned path
    /// and pool-aware component resolves through it.
    pub(crate) registry: Arc<oceanfs_storage::PoolRegistry>,
    /// Role-pinned directories for the node's non-segment data paths
    /// (metadata, data WAL, event WAL, hint WAL).
    pub(crate) paths: PoolPaths,
    /// The metadata store (RocksDB) — held for recovery, the reaper, the
    /// gRPC services, and the shutdown flush.
    pub(crate) metadata_store: Arc<oceanfs_storage::RocksDbMetadataStore>,
    /// The acceleration dispatcher probed at startup (ADR-0006) — shared
    /// by the pools' EC encoder, the heal decoder, the segment reader and
    /// the server-side compressors.
    pub(crate) accel: Arc<oceanfs_accel::AccelDispatcher>,
    /// The data-WAL writer — the durable write-ahead log the sealer and
    /// recovery consume; held for the shutdown sync.
    pub(crate) wal_writer: Arc<oceanfs_storage::WalWriter>,
    /// The segment lifecycle registry (ADR-0025 Decision 2) — the
    /// coordinator's in-memory state; constructed first and shared by the
    /// pools, the GC/AE/scrub workers and the gRPC services.
    pub(crate) lifecycle_registry: Arc<oceanfs_storage::SegmentLifecycleRegistry>,
    /// The segment lifecycle event WAL (ADR-0024) — the coordinator's
    /// durable side-effect.
    pub(crate) event_wal: Arc<oceanfs_storage::EventWal>,
    /// The event WAL checkpoint manager (ADR-0024 Decision 3) — bounded
    /// snapshots of the folded registry.
    pub(crate) event_checkpoint: Arc<oceanfs_storage::EventCheckpoint>,
    /// The segment lifecycle coordinator (ADR-0025) — the single writer
    /// of segment lifecycle state.
    pub(crate) lifecycle: Arc<oceanfs_storage::SegmentLifecycleCoordinator>,
    /// The segment sealer — the authoritative persistence path for sealed
    /// segments (pool-aware placement, ADR-0029 f5).
    pub(crate) sealer: Arc<oceanfs_storage::SegmentSealer>,
    /// The ONE shared segment data store (ADR-0032 D4) — a single
    /// `oceanfs_storage::DiskSegmentStore` instance constructed exactly
    /// here, shared by the replicator, GC, orphan reaper, AE, heal,
    /// scrub, re-replication, the healing/segment gRPC services and
    /// startup recovery (reviews #57/#59/#60/#425). Both the data and
    /// delete/list roles run through it.
    pub(crate) data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore>,
    /// The seal-time segment replicator (sealed-segment-replication) —
    /// pushes sealed segments to their ring replicas off the seal path.
    pub(crate) segment_replicator: Arc<SegmentReplicator>,
    /// The segment reader (mmap / O_DIRECT / buffered, pool-aware) —
    /// consumed by the read coordinator.
    pub(crate) segment_reader: Arc<dyn oceanfs_storage::io::SegmentReader>,
    /// The compaction-remap alias (g3 `loss-announcement` Option A) —
    /// shared by the append handler and the healing service's remap
    /// handler.
    pub(crate) remap_alias: Arc<oceanfs_core::SegmentRemapAlias>,
    /// The g1 shared per-pool I/O signal observer (ADR-0029 §D3) — the
    /// seal pipeline records into it; the health monitor consumes it.
    pub(crate) io_observer: Arc<oceanfs_storage::io::IoObserver>,
    /// The write-path shard buffer pool (perf rule §2.5) — shared by the
    /// segment shards and the gRPC segment service.
    pub(crate) shard_buffer_pool: Arc<BufferPool>,
    /// The small-tier segment shard (write concurrency, perf §2.5).
    pub(crate) shard_small: Arc<SegmentShard>,
    /// The standard-tier segment shard (write concurrency, perf §2.5).
    pub(crate) shard_standard: Arc<SegmentShard>,
    /// The small-tier segment pool (pipeline parallelism, perf §2.7).
    pub(crate) segment_pool_small: Arc<SegmentPool>,
    /// The standard-tier segment pool (pipeline parallelism, perf §2.7).
    pub(crate) segment_pool_standard: Arc<SegmentPool>,
    /// The active segment pools — retained for the live
    /// `segment_active_count` metric poller.
    pub(crate) active_pools: Vec<Arc<SegmentPool>>,
    /// Startup-rebuild duration gauge: records the startup recovery
    /// duration (checkpoint + fold + data-WAL pass + compaction
    /// recovery); registered with the central metrics registry by the
    /// server-side metrics block after recovery runs.
    pub(crate) startup_rebuild_gauge: oceanfs_core::Gauge,
}

impl StorageModule {
    /// Builds the storage subsystem bundle.
    ///
    /// Owns the construction previously inline in `Node::start()` §6–§6c
    /// (segment pools + shards, WAL writer, lifecycle registry +
    /// coordinator + event WAL/checkpoint, sealer, the two shared segment
    /// stores, the seal-time replicator, the I/O reader) plus the
    /// compaction-remap alias. Purely sequential object construction with
    /// the same side effects (directory creation, WAL open) as the inline
    /// code it replaces.
    ///
    /// # Parameters
    ///
    /// `config` and `paths` come from the validated node config and the
    /// role-pinned path resolution; `registry`, `metadata_store` and
    /// `accel` are constructed before the builder call in `Node::start()`
    /// §0–§2 and owned by the returned module afterwards; `ring_cache`,
    /// `membership` and `pool` are network-side handles the replicator
    /// needs (still owned by `Node::start()` — c4 re-homes them).
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL writer, event WAL or checkpoint cannot
    /// be opened, a shard/pool construction fails, or the segment
    /// directory cannot be created.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn build(
        config: &NodeConfig,
        paths: &PoolPaths,
        registry: Arc<oceanfs_storage::PoolRegistry>,
        metadata_store: Arc<oceanfs_storage::RocksDbMetadataStore>,
        accel: Arc<oceanfs_accel::AccelDispatcher>,
        ring_cache: Arc<oceanfs_routing::RingCache>,
        membership: Arc<oceanfs_membership::Membership>,
        pool: Arc<oceanfs_network::ConnectionPool>,
    ) -> Result<Self, String> {
        // ---- 6. Construct storage components ----
        let segment_size = SegmentSizeConfig::default();
        // [review][config][critical]
        // wal configuration should be configurable by the end user too
        // [end]
        let wal_config = WalConfig { data_dir: paths.wal.clone(), ..WalConfig::default() };
        let wal_writer = Arc::new(
            oceanfs_storage::WalWriter::open(&wal_config)
                .await
                .map_err(|e| format!("failed to open WAL writer: {e}"))?,
        );

        // Per-core segment shards for write concurrency (perf rule §2.5).
        let shard_count =
            shard::derive_shard_count(config.segment_shard_count, config.segment_shard_count_max);
        // Scale buffer pool max chunks by shard count (Item 8, D8.5).
        let total_pool_chunks = config.buffer_pool_max_chunks * shard_count;
        // Validate memory budget (Item 8, D8.3). The budget is the real
        // buffer-pool memory: per-shard pool bytes (chunk bytes × max
        // chunks) × shard count. The old call multiplied by segment size
        // as well, producing a 2.2 TB false positive on every boot (F5).
        let _ = crate::startup::validate_shard_memory_budget(
            shard_count,
            config.buffer_pool_chunk_bytes * config.buffer_pool_max_chunks,
        );
        let shard_buffer_pool =
            Arc::new(BufferPool::new(config.buffer_pool_chunk_bytes, total_pool_chunks));
        let shard_small = Arc::new(
            SegmentShard::new(shard_count, SizeTier::Small, &segment_size, &shard_buffer_pool)
                .map_err(|e| format!("failed to create small segment shard: {e}"))?,
        );
        let shard_standard = Arc::new(
            SegmentShard::new(shard_count, SizeTier::Standard, &segment_size, &shard_buffer_pool)
                .map_err(|e| format!("failed to create standard segment shard: {e}"))?,
        );

        // [review][config][critical]
        // pools configurations should be configurable by the end user
        // [end]
        // Segment pools for pipeline parallelism (perf rule §2.7).
        // Created before WAL replay so that replayed entries can be
        // reconstructed into active segments (C4-storage, D6).
        let pool_config = PoolConfig::default();
        // [review][config][critical]
        // pools configurations should be configurable by the end user
        // [end]
        // EC codec for the segment pools: work items carry (k, m, strip)
        // so the seal worker computes and persists per-segment parity at
        // seal time (single scheduler — the parallel encode runs on the
        // blocking pool). Matches the heal codec configuration.
        let pool_ec_config = oceanfs_core::CodecConfig::default();
        // The pools consume `pool_ec_config` below (the machine's
        // seal-on-zero freeze uses the same codec).
        // Seal-time EC parity routes through the accel dispatcher so the
        // encode is observable (accel_encode_ops_total, duration
        // histograms, fallbacks) — the accel tier is exercised on the
        // write path, not just in isolation.
        let pool_ec_encoder: Option<Arc<dyn oceanfs_ec::Encoder>> = Some(accel.clone());
        // The lifecycle registry is constructed FIRST (ADR-0025
        // Decision 2): the pools hold it for the read path and the
        // in-flight attach, and the coordinator wraps the same instance
        // (construction order: registry → pools → coordinator).
        let lifecycle_registry =
            Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(&config.lifecycle));
        let segment_pool_small = Arc::new(
            SegmentPool::new(
                pool_config.clone(),
                SizeTier::Small,
                &segment_size,
                shard_buffer_pool.clone(),
                Some(pool_ec_config.clone()),
                pool_ec_encoder.clone(),
                Arc::clone(&lifecycle_registry),
            )
            .map_err(|e| format!("failed to create small segment pool: {e}"))?,
        );
        let segment_pool_standard = Arc::new(
            SegmentPool::new(
                pool_config,
                SizeTier::Standard,
                &segment_size,
                shard_buffer_pool.clone(),
                Some(pool_ec_config),
                pool_ec_encoder,
                Arc::clone(&lifecycle_registry),
            )
            .map_err(|e| format!("failed to create standard segment pool: {e}"))?,
        );

        // ADR-0001: tiered segment sizing driven by SegmentSizeConfig.
        // ---- f5: pool-aware segment placement + resolution ----
        // Sealed segments spread across the node's data pools: the sealer
        // selects the target once per segment (PlacementPolicy over the
        // registry snapshot) and stamps `pool_id` on the metadata; every
        // reader/GC store resolves the owning root through this resolver
        // (the lifecycle registry's `SegmentMetadata.pool_id`, durable via
        // the event WAL + checkpoint).
        // ADR-0031 (f1): pools are mandatory — the registry always holds
        // the declared data pools; the legacy empty-list branch was
        // removed here with the boot enforcement.
        let data_pools = registry.data_pools();
        let segment_legacy_dir = config.data_dir.join("segments");
        let pool_id_for: oceanfs_storage::PoolIdResolver = {
            let registry = Arc::clone(&lifecycle_registry);
            Arc::new(move |segment_id: &SegmentId| {
                registry.get(*segment_id).map(|entry| entry.metadata.pool_id)
            })
        };
        // g1 `disk-io-observability` (ADR-0029 §D3): the shared per-pool
        // I/O signal observer. The seal pipeline records write/fsync
        // latency + errors per pool through it immediately; the health
        // monitor (g2) consumes `snapshot`s. Every boot pool's signal
        // state is registered with its `oceanfs_pool_io_errors_total`
        // counter bound.
        let io_observer = Arc::new(oceanfs_storage::io::IoObserver::new());
        registry.observe_into(&io_observer);
        let io_backend = Arc::new(oceanfs_storage::io::IoBackend::new());
        // [review][implementation][critical]
        // seal config must be unique per pool path : write and read mode could differ
        // between each mount, since they depend on the nature of the FS
        // [end]
        let seal_config = oceanfs_storage::SealConfig {
            data_pools: data_pools.clone(),
            // f8 runtime attach: the sealer refreshes the data-pool list
            // from the LIVE registry at each seal, so a pool attached via
            // POST /admin/pools is a placement target immediately (no
            // restart). ADR-0031 (f1): the registry is always present —
            // the legacy `None` arm was removed with boot enforcement.
            registry: Some(registry.clone()),
            target_size_bytes: segment_size.default_target_size,
            // [review][config][high]
            // seal timeout should be allowed to be user configured since
            // and its default value cannot be a magic constant
            // [end]
            seal_timeout_ms: 5000,
            data_dir: segment_legacy_dir.clone(),
            io_mode: oceanfs_storage::io::IoReadMode::from_config(config.read_cache_segments),
            write_mode: oceanfs_storage::io::SegmentWriteMode::probe(segment_legacy_dir.clone()),
            // g1: the seal pipeline performs its writes/fsyncs through
            // the observed DiskIo (per-pool signals).
            io_backend: io_backend.clone(),
            observer: io_observer.clone(),
            // Seal pipeline batching (userland-configurable): the fsync
            // group-commit window and the early-flush trigger size.
            fsync_batch_timeout_ms: config.seal_fsync_batch_timeout_ms,
            fsync_max_waiters: config.seal_fsync_max_waiters,
        };
        // [review][cleanup][medium]
        // since roots are now handled within the pool registry, this part is probably
        // useless. if not, it is part of the legacy we must remove.
        // [end]
        // SegmentSealer is the authoritative persistence path. Sealed
        // segments are written to {data_dir}/segments/ (legacy) or the
        // selected data pool root (pool mode, ADR-0029 f5) with the
        // configured I/O mode (O_DIRECT or buffered). The shared segment
        // data store is used by anti-entropy and healing below.
        let segment_dir = segment_legacy_dir.clone();
        // The seal worker runs BEFORE the WAL replay (replayed segments
        // seal during replay), so the segment directory must already
        // exist when the first replay seal fires.
        if let Err(e) = std::fs::create_dir_all(&segment_dir) {
            return Err(format!("cannot create segments directory {:?}: {e}", segment_dir));
        }

        // ---- 6b. Segment lifecycle coordinator (ADR-0025 phase 2) ----
        // The ONLY writer of segment lifecycle state. The write path
        // reserves through it before the first WAL entry of each
        // segment; the seal path (via the flush coordinator) seals
        // through it; the orphan reaper deletes through it. The
        // registry is seeded from the segments CF so the coordinator
        // is the complete single writer over EXISTING data too (the
        // reaper's request_delete validates against it) — a pure
        // registry fold, no CF writes.
        //
        // The event WAL (ADR-0024) is the coordinator's durable
        // side-effect: every transition appends its event first; the CF
        // write becomes a derived mirror performed after the event
        // (dual-read verification surface — the event log is the source
        // of truth for segment lifecycle).
        // The event WAL dir is composed from `{data_dir}/event-wal` in
        // legacy mode exactly like every other data path (data WAL at
        // `{data_dir}/wal`, metadata at `{data_dir}/metadata`); in pool
        // mode it rides the pinned wal pool root (`{wal pool}/event-wal`,
        // ADR-0024 + ADR-0029 §D8 role pinning). The crate-level default
        // (`/var/lib/oceanfs/event-wal`) is the system-layout default, and
        // the node always overrides it — the same pattern applied to
        // `WalConfig` above. Without this, any non-root run (dev, e2e,
        // tests) fails to open the event WAL with permission denied.
        let event_wal_config = oceanfs_core::EventWalConfig {
            event_wal_dir: paths.event_wal.clone(),
            ..config.event_wal.clone()
        };
        let event_wal = Arc::new(
            oceanfs_storage::EventWal::open(
                event_wal_config.event_wal_dir.clone(),
                &event_wal_config,
            )
            .await
            .map_err(|e| format!("failed to open segment event WAL: {e}"))?,
        );
        // The event log's own GC (ADR-0024 Decision 3): byte-threshold
        // snapshots of the folded registry + truncation of covered
        // events. Checkpoint files live beside the event WAL files.
        let event_checkpoint = Arc::new(
            oceanfs_storage::EventCheckpoint::open(
                event_wal_config.event_wal_dir.clone(),
                event_wal.clone(),
            )
            .map_err(|e| format!("failed to open event WAL checkpoint manager: {e}"))?,
        );
        let lifecycle = Arc::new(
            oceanfs_storage::SegmentLifecycleCoordinator::with_registry(Arc::clone(
                &lifecycle_registry,
            ))
            // Phase 2: event appends become the durable side-effect;
            // the CF write is demoted to a derived mirror.
            .with_event_wal(event_wal.clone())
            // Checkpoint trigger: threshold-only, off the append path.
            .with_checkpoint(event_checkpoint.clone(), event_wal_config.clone())
            // The seal pools: the pending-seal drain freezes partial
            // segments through them (seal-on-zero — no idle timer).
            .with_seal_pools(vec![segment_pool_small.clone(), segment_pool_standard.clone()]),
        );
        let sealer = Arc::new(oceanfs_storage::SegmentSealer::new(
            seal_config,
            wal_writer.clone(),
            lifecycle.clone(),
        ));

        // [review][config][high]
        // segment relicator config shoudl be fully configurable by the end user
        // [end]
        // The compaction-remap alias (g3 `loss-announcement` Option A):
        // a single shared map consulted by the append handler (late
        // metadata) and the healing service's remap handler (records
        // old→new + chunk table).
        let remap_alias: Arc<oceanfs_core::SegmentRemapAlias> =
            Arc::new(oceanfs_core::SegmentRemapAlias::new());

        // ---- I/O infrastructure: disk-backed segment reader ----
        // Disk-backed segment reader: reads sealed segment files from disk
        // via the configured I/O mode (mmap / O_DIRECT / buffered).
        // Replaces the previous InMemorySegmentReader — segment data is read
        // on demand from the filesystem. No startup preload, no unbounded
        // HashMap growth.
        let io_mode = oceanfs_storage::io::IoReadMode::from_config(config.read_cache_segments);
        // Build the mmap segment cache when read-optimised mode is enabled.
        let mmap_cache = if io_mode == oceanfs_storage::io::IoReadMode::Mmap {
            Some(Arc::new(oceanfs_storage::io::SegmentFileCache::new(
                config.segment_cache_max_entries,
            )))
        } else {
            None
        };
        let disk_segment_reader: Arc<dyn oceanfs_storage::io::SegmentReader> = Arc::new(
            oceanfs_storage::io::DiskSegmentReader::new(
                io_mode,
                io_backend.clone(),
                mmap_cache,
                segment_dir.clone(),
                Some(accel.clone()),
                Some(accel.clone()),
            )
            // Pool-aware resolution (ADR-0029 f5): sealed segments read
            // from the owning data pool root. f8 runtime attach: the
            // live registry is wired so a pool attached mid-run resolves
            // (the resolved root is cached per segment — no registry
            // lock on the steady-state read path).
            .with_data_pools(data_pools, segment_legacy_dir, pool_id_for)
            .with_registry(registry.clone())
            .with_evict_after_read(!config.read_cache_segments),
        );
        // Clone pool Arcs for the read path — the originals are retained
        // by this module; the write coordinator receives its own clones
        // below. PoolFallbackReader checks active (unsealed) segments
        // before falling back to DiskSegmentReader, closing the
        // read-after-write gap for recently-written data.
        let active_pools = vec![segment_pool_small.clone(), segment_pool_standard.clone()];
        let segment_reader = Arc::new(oceanfs_storage::io::PoolFallbackReader::new(
            active_pools.clone(),
            disk_segment_reader,
        ));

        // ---- f2 store unification (ADR-0032 D2/D3) ----
        // ONE unified `oceanfs_storage::DiskSegmentStore` replaces the
        // durability crate's twin impls (reviews #57/#59/#60/#425): all
        // consumers (replicator, re-rep worker, GC, AE, reaper, heal,
        // healing-service, segment-service) receive clones of this one
        // Arc. The store reads through the same io file core as the
        // server reader above and purges its per-segment caches after
        // every whole-file rewrite.
        let unified_store = Arc::new(oceanfs_storage::DiskSegmentStore::new(
            Arc::clone(&registry),
            Arc::clone(&lifecycle_registry),
            Arc::clone(&segment_reader) as Arc<dyn oceanfs_storage::io::SegmentReader>,
            io_mode,
            Arc::clone(&io_backend),
            Arc::clone(&io_observer),
        ));
        // The ONE instance (ADR-0032 D4): `StorageModule.data_store` is
        // the only construction site in the node crate.
        let data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore> = unified_store;

        // [review][architecture][critical][resolved]
        // we have 3 abstractions to access disk : the durability data store,
        // the durability shard store and the disk reader.
        // each independently implements optimisations or not, without unified logic. this is awfull, and must be resolved.
        // RESOLVED by store-unification f2 (ADR-0032 D2): the durability
        // twin impls are deleted; the ONE unified store reads through the
        // same io file core as the reader and writes through the seal
        // pipeline's atomic observed discipline.
        // [end]
        // ---- 6c. Seal-time segment replicator (sealed-segment-replication) ----
        // The data-replication backbone: after a segment seals on this
        // node, its full data section is pushed to the segment's ring
        // replicas (segment_replica_set − self) — the exact set the read
        // path's gRPC fallback fetches from. Seal itself never makes a
        // network call: the seal worker / compactor only `enqueue` (one
        // atomic channel send); the decoupled `run` task does the pushes.
        // The replicator shares the module's single unified store (pool
        // resolution via the lifecycle registry).
        let segment_replicator = Arc::new(SegmentReplicator::new(
            ring_cache.clone(),
            membership.clone(),
            pool.clone(),
            data_store.clone(),
            lifecycle.clone(),
            NodeId::new(&config.node_id),
            ReplicationConfig {
                throttle_bytes_sec: config.replication_throttle_bytes_sec,
                ..Default::default()
            },
        ));

        Ok(Self {
            registry,
            paths: paths.clone(),
            metadata_store,
            accel,
            wal_writer,
            lifecycle_registry,
            event_wal,
            event_checkpoint,
            lifecycle,
            sealer,
            data_store,
            segment_replicator,
            segment_reader,
            remap_alias,
            io_observer,
            shard_buffer_pool,
            shard_small,
            shard_standard,
            segment_pool_small,
            segment_pool_standard,
            active_pools,
            startup_rebuild_gauge: oceanfs_core::Gauge::new(
                "oceanfs_startup_rebuild_ms".into(),
                "Startup rebuild duration (checkpoint + fold + data-WAL pass + compaction recovery), ms"
                    .into(),
                oceanfs_core::LabelSet::empty(),
            ),
        })
    }

    /// Starts the storage-side seal pipeline draining the active pools'
    /// seal queues (c3-Option-A: relocated from the write coordinator —
    /// recovery's replayed re-seals complete through it, so startup no
    /// longer depends on a server object).
    ///
    /// The merkle-root builder is wired to the durability crate's
    /// `MerkleTree` (injected — storage cannot depend on durability);
    /// `sealed_notifier` carries the node's continuous-anti-entropy +
    /// seal-time-replication fan-out. The returned handle is detached by
    /// the caller (the loop exits when the pools' seal queues close).
    pub(crate) fn start_seal_pipeline(
        &self,
        sealed_notifier: Option<oceanfs_storage::segment::seal_pipeline::SealedSegmentNotifier>,
    ) -> tokio::task::JoinHandle<()> {
        let merkle: oceanfs_storage::segment::seal_pipeline::SealMerkleBuilder =
            Arc::new(|data: &[u8]| {
                oceanfs_durability::MerkleTree::build(data, 0).map(|tree| tree.root().hash())
            });
        oceanfs_storage::segment::seal_pipeline::spawn_seal_pipeline(
            self.segment_pool_small.clone(),
            self.segment_pool_standard.clone(),
            self.sealer.clone(),
            self.lifecycle.clone(),
            merkle,
            sealed_notifier,
        )
    }

    /// Runs startup recovery: the machine path (ADR-0025 phase 2).
    ///
    /// Deterministic recovery — fold the event log into the registry
    /// (state = fold(events)), rebuild Reserved-unsealed segments from the
    /// data WAL, resolve incomplete compaction units (rows 7-9), then run
    /// the startup replication pass for sealed segments whose
    /// `storage_locations` was never stamped. The startup cost is bounded
    /// by the checkpoint threshold, never by lifetime event volume.
    ///
    /// Must be called after every component that consumes the recovery
    /// output (the AE merkle rebuild etc.) is constructed — and after
    /// [`Self::start_seal_pipeline`] (the replayed re-seals complete on
    /// the seal pipeline; recovery waits on their `.dat` files).
    /// Records the rebuild duration on [`Self::startup_rebuild_gauge`].
    ///
    /// # Errors
    ///
    /// Returns an error if the event-WAL fold, data-WAL replay or the
    /// incomplete-compaction recovery pass fails.
    pub(crate) async fn run_startup_recovery(&self) -> Result<(), String> {
        let rebuild_start = std::time::Instant::now();
        let wal_config = WalConfig { data_dir: self.paths.wal.clone(), ..WalConfig::default() };
        let wal_reader = oceanfs_storage::wal::WalReader::open(&wal_config)
            .map_err(|e| format!("failed to open WAL reader: {e}"))?;
        // Load the latest checkpoint (ADR-0024 Decision 3): its snapshot
        // seeds the registry; the fold starts at its covered position —
        // startup replay is bounded by the byte threshold, not by
        // lifetime event volume. Without a checkpoint the fold starts at
        // the earliest retained event.
        let fold_start = match self
            .event_checkpoint
            .load_checkpoint()
            .map_err(|e| format!("failed to load event WAL checkpoint: {e}"))?
        {
            Some((snapshot, covered)) => {
                self.lifecycle.seed_from_checkpoint(&snapshot);
                info!(covered = ?covered, "event WAL checkpoint loaded; folding events after it");
                covered
            }
            None => oceanfs_storage::EventWalPos { file_seq: 0, offset: 0 },
        };
        let recovery_outcome = self
            .lifecycle
            .rebuild_with_data_wal(
                self.event_wal.read_from(fold_start),
                &wal_reader,
                &self.sealer,
                |data| {
                    oceanfs_durability::MerkleTree::build(data, 0).map(|tree| tree.root().hash())
                },
                &self.wal_writer,
            )
            .await
            .map_err(|e| format!("event-WAL recovery failed: {e}"))?;
        info!(
            folded = recovery_outcome.folded_segments,
            dropped_empty = recovery_outcome.dropped_empty_reserves,
            re_sealed = recovery_outcome.re_sealed_segments,
            adopted = recovery_outcome.adopted_segments,
            swept = recovery_outcome.swept_entries,
            "event-WAL recovery complete (ADR-0025 phase 2)"
        );
        // Rows 7-9: incomplete compaction units — the folded registry's
        // `repacked_from` markers, one objects-CF read per unit. Each
        // action deletes through the coordinator (durable before
        // unlink) and sweeps the `.dat` (idempotent).
        let compaction_actions = recover_incomplete_compactions(
            self.lifecycle.registry(),
            &StoreObjectLookup(
                Arc::clone(&self.metadata_store) as Arc<dyn oceanfs_storage_api::MetadataStore>
            ),
        )
        .map_err(|e| format!("compaction recovery failed: {e}"))?;
        for action in &compaction_actions {
            let (segment_id, label) = match action {
                CompactionRecoveryAction::FinishOldDeletion(id) => (*id, "finish_old_deletion"),
                CompactionRecoveryAction::SweepNewOrphan(id) => (*id, "sweep_new_orphan"),
                CompactionRecoveryAction::SweepOldDat(id) => (*id, "sweep_old_dat"),
            };
            // The sweep's pool id: captured from the registry entry
            // BEFORE the durable delete (the delete evicts the entry —
            // the unified store's delete_shards is registry-resolved,
            // ADR-0032 D2). Pure-residue actions (SweepOldDat — the
            // entry is already gone) cannot resolve a pool here; the
            // orphan reaper's per-root sweep backstops the residue.
            let sweep_pool =
                self.lifecycle_registry.get(segment_id).map(|entry| entry.metadata.pool_id);
            if !matches!(action, CompactionRecoveryAction::SweepOldDat(_)) {
                if let Err(e) = self.lifecycle.request_delete(segment_id).await {
                    warn!(
                        segment_id = %segment_id,
                        error = %e,
                        "compaction recovery delete failed (startup continues; the reaper retries)"
                    );
                }
            }
            // Sweep the `.dat` (ADR-0029 f5): explicit pool when the
            // entry carried one; residue-only sweeps are left to the
            // reaper's per-root listing.
            if let Some(pool_id) = sweep_pool {
                if let Err(e) = self.data_store.delete_shards_with_pool(&segment_id, pool_id).await
                {
                    warn!(
                        segment_id = %segment_id,
                        error = %e,
                        "compaction recovery sweep failed (startup continues; the reaper retries)"
                    );
                }
            } else {
                tracing::debug!(
                    segment_id = %segment_id,
                    action = label,
                    "compaction recovery residue has no registry entry; the reaper's per-root sweep reclaims it"
                );
            }
            info!(segment_id = %segment_id, action = label, "compaction recovery action applied");
        }
        let rebuild_ms = rebuild_start.elapsed().as_millis() as u64;
        self.startup_rebuild_gauge.set(rebuild_ms);
        info!(rebuild_ms, "startup rebuild complete");
        // Retention liveness is machine-backed (ADR-0024 §Retention): an
        // entry at position p of segment S is garbage iff S is sealed
        // with data_wal_pos ≥ p, or deleted. Entries whose segment has
        // no registry entry are unreachable (the reserve-before-entry
        // invariant) — sweepable.
        {
            let registry = Arc::clone(&self.lifecycle_registry);
            self.wal_writer.set_liveness(Arc::new(move |id, pos| match registry.get(id) {
                Some(entry) => oceanfs_storage::entry_is_garbage(&entry, &pos),
                None => true,
            }));
        }

        // ---- Startup replication pass (sealed-segment-replication) ----
        // Segments whose storage_locations was never stamped (sealed but
        // the replicator never completed a push — a crash between the
        // SealEvent and the first ack, or a segment adopted/replayed by
        // recovery) must be re-published so the replicator fans them out.
        // Non-empty storage_locations = fully acked, skip. One pass, off
        // the hot path; the replicator's channel is bounded (overflow
        // routes to its needs set).
        {
            let mut pending = 0u64;
            self.lifecycle.registry().for_each(|segment_id, entry| {
                if entry.state == oceanfs_storage::SegmentState::Sealed
                    && entry.metadata.storage_locations.is_empty()
                {
                    self.segment_replicator.enqueue(segment_id);
                    pending += 1;
                }
            });
            if pending > 0 {
                info!(pending, "startup replication pass enqueued sealed segments");
            }
        }
        Ok(())
    }

    /// Registers the storage-owned metric series with the node's central
    /// registry (c5 — replaces the §12 per-component register lines the
    /// inline code carried) and starts the RocksDB property-gauge
    /// polling task (every 30s).
    pub(crate) fn register_metrics(&self, metrics: &oceanfs_server::admin::MetricsRegistry) {
        metrics.register_gauge(self.startup_rebuild_gauge.clone());
        self.accel.register_metrics(metrics);
        self.shard_buffer_pool.register_metrics(metrics);
        // Segment shard gauges (`segment_active_count` — Phase 2
        // asserts the segment pipeline is producing segments).
        self.shard_small.register_metrics(metrics);
        self.shard_standard.register_metrics(metrics);
        self.wal_writer.register_metrics(metrics);
        self.sealer.register_metrics(metrics);
        // Lifecycle registry-size gauges (ADR-0025 Decision 5 — the
        // registry's O(live segments) memory cost is metric-visible).
        self.lifecycle.register_metrics(metrics);
        // Event WAL metrics (ADR-0024 — bytes, files, append count).
        self.event_wal.register_metrics(metrics);
        // Checkpoint metrics (checkpoint bytes written, bytes truncated).
        self.event_checkpoint.register_metrics(metrics);
        // Storage pool metrics (ADR-0029 — status, bytes free/total,
        // I/O error counter per pool).
        self.registry.register_metrics(metrics);
        // Seal-time segment replication metrics (pushed/bytes/retries/
        // failures/needs gauge).
        self.segment_replicator.register_metrics(metrics);
        // Register RocksDB property gauges into the central registry.
        self.metadata_store.metrics().register(metrics);
        // Start the background RocksDB metrics polling task (every 30s).
        self.metadata_store.start_metrics_task();
    }

    /// Spawns the storage-owned background loops (c5 — each worker owns
    /// its startup sequence): the pool health monitor (g2,
    /// ADR-0029 §D3 — ticks each pool through the D3 state machine) and
    /// the seal-time segment replicator drain loop
    /// (sealed-segment-replication). Returns the monitor's status-event
    /// receiver — the health-consequence applier consumes it (spawned
    /// by the background bundler, which owns that composition).
    pub(crate) fn spawn_loops(
        &self,
        bg: &mut crate::node::BackgroundTasks,
    ) -> tokio::sync::mpsc::Receiver<oceanfs_storage::pool::health::HealthEvent> {
        use oceanfs_storage::pool::health::{HealthMonitor, HealthMonitorConfig};
        use tracing::info;

        let (health_monitor, health_events) = HealthMonitor::new(
            self.registry.clone(),
            self.io_observer.clone(),
            HealthMonitorConfig::default(),
        );
        let health_token = bg.health_cancel.clone();
        bg.health_monitor = Some(tokio::spawn(async move {
            health_monitor.run(health_token).await;
            info!("Pool health monitor stopped");
        }));

        // Seal-time segment replicator (sealed-segment-replication). The
        // drain loop consumes sealed-segment events (seal worker +
        // compactor + startup pass) and pushes each segment's data to
        // its ring replicas; the sweep retries the needs set. Runs until
        // shutdown.
        let replicator_token = bg.segment_replicator_cancel.clone();
        let replicator_for_spawn = Arc::clone(&self.segment_replicator);
        bg.segment_replicator = Some(tokio::spawn(async move {
            replicator_for_spawn.run(replicator_token).await;
            info!("Segment replicator stopped");
        }));

        health_events
    }
}

/// Shared module-test prelude (c2): the pre-builder environment
/// `Node::start()` §0–§5 composes — config, role-pinned paths, pool
/// registry, metadata store, accel, ring, membership, pool — plus the
/// built [`StorageModule`]. Used by this module's tests and by
/// `modules/durability.rs` tests (the durability builder consumes the
/// storage bundle).
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;

    use oceanfs_core::NodeConfig;
    use oceanfs_membership::Membership;
    use oceanfs_network::ConnectionPool;
    use tempfile::TempDir;

    use super::StorageModule;

    /// The §0–§5 prelude + the built storage module.
    pub(crate) struct StoragePrelude {
        /// The validated four-role node config the modules were built
        /// from (ADR-0031 f1 topology: data first = pool id 0).
        pub(crate) config: NodeConfig,
        /// The c1 storage bundle (the durability builder's input).
        pub(crate) module: StorageModule,
        /// The membership handle passed into the builders.
        pub(crate) membership: Arc<Membership>,
        /// The connection pool passed into the builders.
        pub(crate) pool: Arc<ConnectionPool>,
    }

    /// Builds the pre-builder prelude exactly as `Node::start()` §0–§5
    /// does (registry + paths + metadata + accel + ring + membership +
    /// pool) and runs `StorageModule::build`. ADR-0031 (f1): the config
    /// declares the mandatory four-role topology.
    pub(crate) async fn build_storage_prelude(tmp: &TempDir) -> StoragePrelude {
        // Pool roots are siblings under the tempdir, so `data_dir` is a
        // subdir (disjointness rule).
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        fn pool(
            name: &str,
            role: oceanfs_core::PoolRole,
            root: std::path::PathBuf,
        ) -> oceanfs_core::StoragePoolConfig {
            oceanfs_core::StoragePoolConfig {
                name: name.into(),
                role,
                root,
                weight: None,
                tech: Default::default(),
                health: Default::default(),
            }
        }
        let config = NodeConfig {
            data_dir: data_dir.clone(),
            // ADR-0031: pools are mandatory (data first = pool id 0).
            storage: oceanfs_core::StorageConfig {
                pools: vec![
                    pool("data-0", oceanfs_core::PoolRole::Data, tmp.path().join("pool-data")),
                    pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.path().join("pool-wal")),
                    pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.path().join("pool-meta")),
                    pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.path().join("pool-hints")),
                ],
                missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
            },
            // The event WAL lives under the temp data dir (the default
            // /var/lib/oceanfs/event-wal is not writable in tests).
            event_wal: oceanfs_core::EventWalConfig {
                event_wal_dir: tmp.path().join("event-wal"),
                ..Default::default()
            },
            ..NodeConfig::default()
        };
        let registry = Arc::new(
            oceanfs_storage::PoolRegistry::from_config(&config.storage, &data_dir)
                .expect("pool registry"),
        );
        let paths = crate::pool_paths::pool_paths(&registry);
        let metadata_store = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: paths.metadata.clone(),
                ..Default::default()
            })
            .expect("metadata store"),
        );
        let accel =
            Arc::new(oceanfs_accel::AccelDispatcher::new(oceanfs_core::AccelConfig::default()));
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(oceanfs_routing::Ring::new(
            oceanfs_core::RingConfig::default(),
        )));
        let membership = Arc::new(Membership::new(
            oceanfs_core::NodeId::new("test-node"),
            "127.0.0.1:0".parse().expect("addr"),
            "127.0.0.1:0".parse().expect("addr"),
            oceanfs_core::GossipConfig::default(),
            ring_cache.clone(),
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
        let module = StorageModule::build(
            &config,
            &paths,
            registry,
            metadata_store,
            accel,
            ring_cache,
            membership.clone(),
            pool.clone(),
        )
        .await
        .expect("storage module build");
        StoragePrelude { config, module, membership, pool }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::modules::storage::test_support::build_storage_prelude;

    /// Builds the pre-builder prelude exactly as `Node::start()` §0–§5
    /// does (registry + paths + metadata + accel + ring + membership +
    /// pool) and runs `StorageModule::build`. ADR-0031 (f1): the config
    /// declares the mandatory four-role topology.
    async fn build_module(tmp: &TempDir) -> StorageModule {
        build_storage_prelude(tmp).await.module
    }

    /// f3 DoD: the builder returns a consistent `StorageModule` whose
    /// store surface is exactly ONE `oceanfs_storage::DiskSegmentStore`
    /// instance (ADR-0032 D4) — the only construction site in the node
    /// crate (grep invariant). The replicator (the module's own
    /// store consumer) holds the SAME Arc; this test proves the shared
    /// instance is live under lifecycle-routed semantics (unregistered
    /// writes are rejected).
    #[tokio::test]
    async fn build_returns_module_with_single_shared_store() {
        let tmp = TempDir::new().expect("tempdir");
        let module = build_module(&tmp).await;

        // Registry is the boot-time one; the module owns it.
        assert!(module.registry.pool_count() >= 1, "registry must be populated");

        // The module's one store is the replicator's store (pointer
        // identity — the module constructs once and clones everywhere).
        assert!(
            Arc::ptr_eq(&module.data_store, &module.segment_replicator.data_store()),
            "the replicator must share the module's single store instance"
        );

        // An unregistered write is rejected (ADR-0032 D3 — the
        // write-before-register bridge is gone).
        let segment_id = SegmentId::new();
        let payload = b"f3 unified store round-trip payload";
        let err = module
            .data_store
            .write_segment_data(&segment_id, payload)
            .await
            .expect_err("write-before-register must be rejected");
        assert!(err.to_string().contains("not registered"), "{err}");

        // Register (reserve + seal through the module's coordinator —
        // the event-WAL-armed lifecycle), then write → read round-trips
        // through the shared store; the delete role runs on the same
        // instance.
        module
            .lifecycle
            .request_reserve(segment_id, oceanfs_core::SizeTier::Standard, 1, 0)
            .await
            .expect("reserve");
        module
            .lifecycle
            .request_seal(
                segment_id,
                oceanfs_core::SegmentMetadata {
                    pool_id: 0,
                    segment_id,
                    ec_k: 1,
                    ec_m: 0,
                    size_tier: oceanfs_core::SizeTier::Standard,
                    merkle_root: Some(oceanfs_core::HashOutput::from_bytes(
                        *blake3::hash(payload).as_bytes(),
                    )),
                    storage_locations: smallvec::smallvec![],
                    sealed_at: Some(1_700_000_000_000),
                },
                None,
            )
            .await
            .expect("seal");
        module
            .data_store
            .write_segment_data(&segment_id, payload)
            .await
            .expect("write through the shared store");
        let back = module
            .data_store
            .read_segment_data(&segment_id)
            .await
            .expect("read back through the shared store")
            .expect("segment present after write");
        assert_eq!(&back.data[..], &payload[..], "shared store round-trip");

        // Delete role on the same store; afterwards it must no longer
        // find the segment.
        module
            .data_store
            .delete_shards(&segment_id)
            .await
            .expect("delete through the shared store");
        assert!(
            module.data_store.read_segment_data(&segment_id).await.unwrap().is_none(),
            "segment must be gone after delete"
        );

        // Recovery on an empty (post-build) store completes.
        module.run_startup_recovery().await.expect("empty startup recovery");
    }

    /// The module exposes the write-path pools + reader the inline
    /// coordinator/metrics code consumes after `build`.
    #[tokio::test]
    async fn build_exposes_write_path_pools_and_reader() {
        let tmp = TempDir::new().expect("tempdir");
        let module = build_module(&tmp).await;

        assert_eq!(module.active_pools.len(), 2, "one small + one standard pool");
        assert!(Arc::ptr_eq(&module.active_pools[0], &module.segment_pool_small));
        assert!(Arc::ptr_eq(&module.active_pools[1], &module.segment_pool_standard));
        // Reader and replicator are constructed against the boot ring
        // (empty in this test — no nodes registered).
        let _ = &module.segment_reader;
        assert_eq!(
            module.segment_replicator.ring_node_count(),
            0,
            "replicator present, reading the empty boot ring"
        );
    }
}
