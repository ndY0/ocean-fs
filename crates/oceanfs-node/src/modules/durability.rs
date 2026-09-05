//! Durability subsystem builder (c2 — `DurabilityModule`).
//!
//! Owns the construction previously inline in `Node::start()` §7/§7c/§7d:
//! the GC worker, anti-entropy (+ its incremental merkle tree), scrub
//! coordinator, orphan reaper, heal pipeline (queue + worker + the
//! process-global heal queue init), the reconciliation loop, the
//! re-replication worker + dispatcher, and the per-operation timeouts.
//! Every worker is wired to the c1 `StorageModule`'s single shared
//! stores and lifecycle registry — no worker constructs its own store
//! (ADR-0032 D4 trajectory; the store-unification epic's one-site rule).
//!
//! Pure move (c2): construction order, side effects (the
//! `init_global_queue` heal singleton, the holder-index notifier wiring
//! into `storage.lifecycle`) and error propagation are identical to the
//! inline code this replaces. No behavior change.

use std::{net::SocketAddr, sync::Arc};

use oceanfs_core::{CodecConfig, MetricRegistrar, NodeConfig, NodeId, OperationTimeouts};
use oceanfs_durability::{
    AntiEntropy, GarbageCollector, GrpcHintDeliveryClient, HealConfig, HealQueue, HealWorker,
    HintedHandoff, HintedHandoffConfig, HintedHandoffManager, OrphanReaper, ReRepWorker,
    ScrubConfig, ScrubCoordinator,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;

use crate::{
    announce::AnnounceMetrics,
    membership_state::MembershipStateStore,
    modules::storage::StorageModule,
    node::BackgroundTasks,
    pool_paths::PoolPaths,
    repair::{ManifestRepairTargetSelector, RepairDispatcher},
};

/// The durability subsystem bundle (c2).
///
/// One `Arc` per background worker + the shared cross-worker handles the
/// node's later sections (spawns, gRPC services, admin, accessors)
/// consume. Metrics registration is centralized in
/// [`register_metrics`](Self::register_metrics); the ADR-0017 scheduler
/// epic will wrap this bundle later.
pub(crate) struct DurabilityModule {
    /// Garbage collector (compaction + reaping orchestration).
    pub(crate) gc: Arc<GarbageCollector>,
    /// Anti-entropy worker (merkle reconciliation + repair).
    pub(crate) ae: Arc<AntiEntropy>,
    /// Scrub coordinator (periodic integrity verification).
    pub(crate) scrub: Arc<ScrubCoordinator>,
    /// Orphan reaper (on-disk `.dat` sweep + delete).
    pub(crate) reaper: Arc<OrphanReaper>,
    /// EC heal worker (corruption repair pipeline).
    pub(crate) heal: Arc<HealWorker>,
    /// Periodic reconciliation loop (g4 — the holder-index pull safety
    /// net).
    pub(crate) reconciliation: Arc<oceanfs_durability::ReconciliationLoop>,
    /// Re-replication worker (g5 — the acquiring-side executor).
    pub(crate) rep_worker: Arc<ReRepWorker>,
    /// Re-replication dispatcher (g5 — the holder-side `RepairSink`
    /// g3/g4 enqueue into).
    pub(crate) repair_dispatcher: Arc<RepairDispatcher>,
    /// Shared per-operation timeouts (write/read coordinators, heal,
    /// hinted handoff — constructed here per the c2 §7d move).
    pub(crate) op_timeouts: Arc<OperationTimeouts>,
    /// The heal/read-path EC decoder (the accel dispatcher), shared with
    /// the read coordinator.
    pub(crate) ec_decoder: Arc<dyn oceanfs_ec::Decoder>,
    /// The EC geometry the read coordinator's fallback codec uses.
    pub(crate) codec_config: CodecConfig,
    /// g3 announcement transmit counters (shared by the compactor remap
    /// closure here and the node's §16b loss-announcer closure).
    pub(crate) announce_metrics: Arc<AnnounceMetrics>,
    /// The legacy gRPC hint receiver (`HintedHandoff`) — consumed by the
    /// c3 healing service (c5 re-seat of node.rs §11: the hinted-handoff
    /// machinery belongs to the durability domain).
    pub(crate) hinted_handoff: Arc<HintedHandoff>,
    /// The durable hinted-handoff manager (ADR-0018/ADR-0027) — the
    /// write path enqueues through it; its prune + delivery watcher
    /// loops are spawned by [`spawn_loops`](Self::spawn_loops).
    pub(crate) hinted_handoff_manager: Arc<HintedHandoffManager>,
}

impl DurabilityModule {
    /// Builds the durability subsystem bundle.
    ///
    /// Owns the construction previously inline in `Node::start()` §7
    /// (GC, re-replication worker + dispatcher, reconciliation loop +
    /// holder-index notifier wiring, AE + merkle tree, scrub, reaper,
    /// heal pipeline, op timeouts). Purely sequential object
    /// construction with the same side effects (the process-global heal
    /// queue init, the lifecycle storage-locations notifier) as the
    /// inline code it replaces.
    ///
    /// # Parameters
    ///
    /// `config` is the validated node config; `storage` is the c1
    /// `StorageModule` bundle whose single shared stores, lifecycle
    /// machinery, metadata store and segment replicator every worker is
    /// wired to; `membership` and `pool` are network-side handles the
    /// workers and the compaction-remap closure need (c4 re-homed them);
    /// `paths` provides the role-pinned hints pool root and `grpc_addr`
    /// the node's own data-plane listener address — both feed the
    /// durable hinted-handoff machinery (c5 re-seat of node.rs §11).
    ///
    /// # Errors
    ///
    /// Returns an error if the anti-entropy merkle tree cannot be
    /// rebuilt from the lifecycle registry scan (the same failure the
    /// inline §7 code propagated).
    pub(crate) async fn build(
        config: &NodeConfig,
        storage: &StorageModule,
        membership: Arc<Membership>,
        pool: Arc<ConnectionPool>,
        paths: &PoolPaths,
        grpc_addr: SocketAddr,
    ) -> Result<Self, String> {
        // ---- 7. Construct durability workers ----
        let gc_config = oceanfs_durability::GcConfig::new(
            config.gc_interval_sec,
            config.tombstone_ttl_sec,
            config.gc_compact_threshold,
            config.gc_max_concurrent_compactions,
            config.gc_compaction_queue_capacity,
        );
        // Announcement transmit metrics (g3 — ADR-0029 §D4
        // observability). Shared by the loss-announcer and the compactor
        // remap closures; registered with the central metrics registry.
        let announce_metrics = Arc::new(AnnounceMetrics::new());
        // ---- Re-replication (g5, ADR-0030 target-pull) ----
        // The HOLDER side dispatches (selects a target + sends the
        // RequestReReplication RPC); the ACQUIRING node's ReRepWorker
        // pulls + writes + stamps. The `RepairSink` that g3's
        // announce_loss and g4's reconciliation enqueue into is the
        // dispatcher; the target-side worker queue is fed by the
        // healing service's `request_re_replication` handler.
        let repair_selector: Arc<dyn oceanfs_durability::RepairTargetSelector> = Arc::new(
            ManifestRepairTargetSelector::new(membership.clone(), NodeId::new(&config.node_id)),
        );
        let repair_dispatcher = Arc::new(RepairDispatcher::new(
            repair_selector,
            pool.clone(),
            membership.clone(),
            storage.lifecycle.clone(),
            NodeId::new(&config.node_id),
        ));
        // The acquiring-side worker (bound to THIS node's pool-aware
        // store + lifecycle; the migration pool/membership are injected
        // plane-agnostically, ADR-0030 Decision 4).
        let rep_worker = Arc::new(ReRepWorker::new(
            oceanfs_durability::ReRepConfig::default(),
            storage.data_store.clone(),
            storage.lifecycle.clone(),
            pool.clone(),
            membership.clone(),
            Arc::new(config.operation_timeouts),
        ));
        // [review][config][high]
        // reconciliation configuration should be fully configurable by the end user
        // [end]
        // The periodic reconciliation loop (g4 `reconciliation` — the
        // ADR-0029 §D4 pull safety net): event-driven wake (a node died /
        // its pools died → exactly the affected segments via the holder
        // index), bounded risk-prioritized queue processing per tick, and
        // an hourly full drift scan. It runs INDEPENDENTLY of any
        // announcement — the complete safety net when announcements are
        // suppressed.
        let reconciliation = Arc::new(oceanfs_durability::ReconciliationLoop::new(
            Arc::clone(&storage.lifecycle_registry),
            membership.clone(),
            repair_dispatcher.clone(),
            NodeId::new(&config.node_id),
            config.replication_factor as usize,
            oceanfs_durability::ReconcileConfig::default(),
        ));
        // Wire the holder-index notifier: the reconciliation loop's
        // reverse index is maintained incrementally from the SINGLE
        // choke point where a segment's storage_locations is written.
        storage.lifecycle.set_storage_locations_notifier({
            let reconciliation = Arc::clone(&reconciliation);
            Arc::new(move |segment_id, locations| {
                reconciliation.on_storage_locations(segment_id, locations);
            })
        });
        // GC compaction repacks live blobs into new segments — it must
        // persist the repacked data through the segment data store (the
        // compactor reads the old segment's bytes and writes the new
        // segment's .dat before the metadata swap; without the store, a
        // metadata-only remap would leave objects pointing at a segment
        // with no on-disk data).
        // GC compaction is a state machine (ADR-0025 Decision 4): the
        // compactor requests every transition from the lifecycle
        // coordinator and unlinks the old .dat through the shared store
        // only after the durable delete.
        let gc_worker = Arc::new(
            oceanfs_durability::GarbageCollector::new(gc_config.clone())
                .with_data_store(storage.data_store.clone())
                .with_lifecycle(storage.lifecycle.clone())
                // The repacked segment is a NEW owner-side seal that
                // bypasses the write-path seal worker: publish it so the
                // segment replicator fans it out to its ring replicas
                // (sealed-segment-replication — without this hook,
                // post-compaction objects silently have zero replicas).
                .with_segment_sealed_notifier({
                    let replicator = Arc::clone(&storage.segment_replicator);
                    Arc::new(move |segment_id| {
                        replicator.enqueue(segment_id);
                    })
                })
                // The compaction remap (g3 `loss-announcement` Option A):
                // after the owner's metadata remap commits, tell the OLD
                // segment's holders so they re-point their own object
                // rows. Targets = `storage_locations(old) − self` (the
                // pinned fan-out). The announcement is a bounded-retry
                // best-effort push; g4's reconciliation is the failsafe.
                .with_compaction_remap_notifier({
                    let membership = Arc::clone(&membership);
                    let pool = Arc::clone(&pool);
                    let lifecycle_registry = Arc::clone(&storage.lifecycle_registry);
                    let self_id = NodeId::new(&config.node_id);
                    let announce_metrics = Arc::clone(&announce_metrics);
                    Arc::new(move |old_segment_id, new_segment_id, chunk_table| {
                        let lifecycle_registry = Arc::clone(&lifecycle_registry);
                        let membership = Arc::clone(&membership);
                        let pool = Arc::clone(&pool);
                        let self_id = self_id.clone();
                        let announce_metrics = Arc::clone(&announce_metrics);
                        tokio::spawn(async move {
                            // Resolve the old segment's holders — the
                            // remap goes to exactly the nodes that hold a
                            // stale copy referencing the old id.
                            let targets: Vec<NodeId> = lifecycle_registry
                                .get(old_segment_id)
                                .map(|entry| {
                                    entry
                                        .metadata
                                        .storage_locations
                                        .iter()
                                        .filter(|n| *n != &self_id)
                                        .cloned()
                                        .collect()
                                })
                                .unwrap_or_default();
                            if targets.is_empty() {
                                tracing::debug!(
                                    old_segment_id = %old_segment_id,
                                    "compaction remap: no peer holders to notify"
                                );
                                return;
                            }
                            match crate::announce::announce_segment_remap(
                                &self_id,
                                old_segment_id,
                                new_segment_id,
                                &chunk_table,
                                &targets,
                                &pool,
                                &membership,
                                None,
                                None,
                                Some(&announce_metrics),
                            )
                            .await
                            {
                                Ok(()) => {
                                    tracing::info!(
                                        old_segment_id = %old_segment_id,
                                        new_segment_id = %new_segment_id,
                                        targets = targets.len(),
                                        "compaction remap announced"
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        old_segment_id = %old_segment_id,
                                        new_segment_id = %new_segment_id,
                                        error = %e,
                                        "compaction remap not fully delivered (g4 failsafe)"
                                    );
                                }
                            }
                        });
                    })
                }),
        );

        // [review][config][high]
        // as previously stated, any config should be possibly driven by the end user
        // [end]
        // Construct IncrementalMerkleTree for anti-entropy by scanning
        // the machine's Sealed entries — supersedes ADR-0018 Decision
        // 1's segments-CF scan (ADR-0025 Decision 3).
        let merkle_tree_config = oceanfs_durability::merkle::MerkleTreeConfig::default();

        let merkle_tree = {
            Arc::new(
                oceanfs_durability::merkle::IncrementalMerkleTree::rebuild_from_segment_scan(
                    &storage.lifecycle_registry,
                    &merkle_tree_config,
                )
                .map_err(|e| format!("failed to rebuild Merkle tree from the machine scan: {e}"))?,
            )
        };

        // [review][architecture][critical][resolved]
        // AE no longer creates its own data store — c1 (composition-root
        // decomposition) wired it to the module's single shared store.
        // RESOLVED by store-unification f2/f3 (ADR-0032): the twin disk
        // impls are deleted, reads/writes share the io file core with
        // the server reader, and StorageModule constructs exactly ONE
        // unified store instance wired into every consumer.
        // [end]
        let ae_worker = Arc::new(AntiEntropy::new(
            oceanfs_durability::AntiEntropyConfig::new(
                config.ae_interval_sec,
                config.ae_peer_count,
            )
            .with_core(oceanfs_core::AntiEntropyConfig {
                continuous_enabled: config.anti_entropy.continuous_enabled,
                continuous_max_segments: config.anti_entropy.continuous_max_segments,
                sampling_enabled: config.anti_entropy.sampling_enabled,
                sampling_interval_sec: config.anti_entropy.sampling_interval_sec,
                sampling_fraction: config.anti_entropy.sampling_fraction,
            }),
            membership.clone(),
            Arc::clone(&storage.lifecycle_registry),
            pool.clone(),
            storage.data_store.clone(),
            merkle_tree.clone(),
        ));
        // [review][config][high]
        // scrub config is not fully customizable
        // [end]
        let mut scrub_config = ScrubConfig::default();
        scrub_config.set_interval_sec(config.scrub_interval_sec);
        scrub_config.set_parallel_nodes(config.scrub_parallel_nodes);
        let scrub_worker = Arc::new(ScrubCoordinator::new(scrub_config));
        // OrphanReaper deletes segment data files from disk when reclaiming
        // orphaned segments after GC compaction. The reaper sweeps the
        // data pool roots (ADR-0032 D1 per-root listing) — the registry's
        // live data pools.
        let reaper = Arc::new(OrphanReaper::new(
            storage.metadata_store.clone(),
            storage.lifecycle.clone(),
            storage.data_store.clone(),
            storage.registry.data_pools(),
            gc_config,
        ));

        // ---- 7c. Construct heal dispatch pipeline ----
        let heal_config = HealConfig::default()
            .with_max_concurrent_heals(config.heal_parallel_segments)
            .with_heal_throttle_bytes_sec(config.heal_throttle_bytes_sec);
        let heal_queue = Arc::new(HealQueue::new(heal_config.queue_capacity()));
        // Initialize the global heal sender so scrub and anti-entropy can
        // call enqueue_heal() without direct queue access.
        oceanfs_durability::heal::init_global_queue(heal_queue.sender());
        let codec_config = CodecConfig::default();
        // The heal decoder routes through the accel dispatcher so decode
        // repair work is observable (accel_decode_ops_total, duration
        // histograms) and the tier is consistent across sites.
        let ec_decoder: Arc<dyn oceanfs_ec::Decoder> = storage.accel.clone();

        // ---- 7d. Construct per-operation timeouts (Item 4) ----
        // Must be constructed before heal, hinted_handoff, write_coordinator,
        // and read_coordinator so they can accept it via their with_timeouts() setters.
        let op_timeouts = Arc::new(config.operation_timeouts);

        let heal_worker = Arc::new(
            HealWorker::new(
                heal_config,
                heal_queue.clone(),
                ec_decoder.clone(),
                storage.lifecycle.clone(),
                storage.data_store.clone(),
            )
            .with_timeouts(op_timeouts.clone()),
        );

        // ---- Hinted handoff machinery (c5 re-seat of node.rs §11) ----
        // The legacy gRPC receiver: the hint receiver fetches
        // segment-ref data back from THIS node's gRPC listener
        // (remote_addr on the receiver is the ephemeral source port —
        // dead by fetch time). The self address is the parsed
        // `grpc_listen_addr` — startup already failed if that address
        // was unparseable (B2: no silent default network address).
        let hinted_handoff = Arc::new(
            HintedHandoff::new_with_pool(pool.clone())
                .with_membership(membership.clone())
                .with_timeouts(op_timeouts.clone()),
        );

        // The persistent per-node HintWAL + manager for durable hinted
        // handoff (ADR-0018 Decision 2). The hints WAL lives on the
        // pinned hints pool root (resolved in pool_paths; the legacy
        // `hint_wal_dir` override was removed by ADR-0031 D2).
        let hints_dir = paths.hints.clone();
        let hint_delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> =
            Arc::new(GrpcHintDeliveryClient::new(pool.clone()).with_self_grpc_addr(grpc_addr));
        // [review][config][fhigh]
        // no magic constants, user should be able to configure the subsystem
        // [end]
        let hint_config = HintedHandoffConfig {
            wal_dir: hints_dir.clone(),
            inline_threshold_bytes: config.hint_inline_threshold_bytes,
            max_batch_size: config.hint_max_batch_size,
            max_batch_bytes: 32 * 1024 * 1024,
        };
        let hinted_handoff_manager = Arc::new(
            HintedHandoffManager::new(hints_dir.clone(), hint_delivery_client, hint_config.clone())
                .with_membership(membership.clone())
                .with_timeouts(op_timeouts.clone()), // Delivery contract (ADR-0027 as amended): hints are
                                                     // NEVER dropped at the sender — deliver everything, the
                                                     // receiver's HLC-LWW apply is the single gate. The old
                                                     // obsolete pre-check dropped hints based on the sender's
                                                     // view of distributed state, which could diverge from
                                                     // the truth (the churn residual class).
        );

        // Replay existing hints from the WAL into in-memory queues.
        hinted_handoff_manager
            .replay_and_enqueue()
            .await
            .map_err(|e| format!("hinted handoff WAL replay: {e}"))?;

        Ok(Self {
            gc: gc_worker,
            ae: ae_worker,
            scrub: scrub_worker,
            reaper,
            heal: heal_worker,
            reconciliation,
            rep_worker,
            repair_dispatcher,
            op_timeouts,
            ec_decoder,
            codec_config,
            announce_metrics,
            hinted_handoff,
            hinted_handoff_manager,
        })
    }

    /// Registers the durability workers' metrics with the node's central
    /// registry (one call replaces the §12 per-worker register lines the
    /// inline code carried).
    pub(crate) fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        self.heal.register_metrics(registrar);
        self.gc.register_metrics(registrar);
        self.reaper.register_metrics(registrar);
        self.scrub.register_metrics(registrar);
        self.ae.register_metrics(registrar);
        // g3 announcements (ADR-0029 §D4 observability).
        self.announce_metrics.register_metrics(registrar);
        // g4 reconciliation (ADR-0029 §D4 observability).
        self.reconciliation.register_metrics(registrar);
        // g5 re-replication (ADR-0030 observability).
        self.repair_dispatcher.register_metrics(registrar);
        // The manager is the component that actually stores and delivers
        // hints; its counters are the authoritative
        // hinted_handoff_hints_{stored,delivered,expired}_total series.
        // (The legacy HintedHandoff — the gRPC *receiver* — is not
        // registered: its counters had inverted semantics and stayed 0.)
        self.hinted_handoff_manager.register_metrics(registrar);
    }

    /// Spawns every durability-owned background loop (c5 — each worker
    /// owns its startup sequence) and fills the corresponding
    /// `BackgroundTasks` fields.
    ///
    /// Loops: GC, anti-entropy, scrub, orphan reaper, EC heal worker,
    /// the hint-WAL prune loop, the hinted-handoff delivery watcher
    /// (§17 — also records fallback seeds on Alive events, ADR-0022
    /// D3), the periodic reconciliation loop (g4) and the re-replication
    /// worker + dispatcher (g5). The delivery watcher needs the
    /// membership event stream + the durable state store, so it is
    /// spawned here and not in `build` (membership must be started
    /// first — `spawn_all` runs after the membership plane is up).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_loops(
        &self,
        config: &NodeConfig,
        storage: &StorageModule,
        membership: Arc<Membership>,
        membership_state_store: MembershipStateStore,
        bg: &mut BackgroundTasks,
    ) {
        use std::time::Duration;

        use tokio_util::sync::CancellationToken;
        use tracing::{info, warn};

        // The spawned loops hold the registry across 'static spawns.
        let gc_registry = Arc::clone(&storage.lifecycle_registry);
        let scrub_registry = Arc::clone(&storage.lifecycle_registry);

        // GC: runs every gc_interval_sec from config.
        let gc_cancel = CancellationToken::new();
        let gc_token = gc_cancel.clone();
        let gc_store = storage.metadata_store.clone();
        let gc_interval = Duration::from_secs(config.gc_interval_sec);
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let gc_worker = Arc::clone(&self.gc);
        bg.gc = Some(tokio::spawn(async move {
            if io_idle {
                oceanfs_storage::io::apply_background_io_class("gc");
            }
            if cpu_idle {
                oceanfs_storage::io::apply_background_cpu_sched("gc");
            }
            let mut interval = tokio::time::interval(gc_interval);
            loop {
                tokio::select! {
                    _ = gc_token.cancelled() => {
                        info!("GC task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = gc_worker.run_cycle(gc_store.clone(), &gc_registry).await
                        {
                            warn!("GC cycle error: {e}");
                        }
                    }
                }
            }
        }));
        bg.gc_cancel = gc_cancel;

        // Anti-entropy: runs every ae_interval_sec from config.
        // Continuous mode exchanges Merkle ROOTS with peers via the
        // incremental tree — it never reads segment data, so per-cycle
        // cost is O(sealed segments) metadata calls instead of reading
        // every segment file (GBs per cycle on the phase-2 SUT, which
        // stalled cycles for 90s+ under load and spiked RSS). The full
        // cycle (reads all data + rebuilds trees) stays available for
        // `continuous_enabled = false`.
        let ae_cancel = CancellationToken::new();
        let ae_token = ae_cancel.clone();
        let ae_interval_secs = config.ae_interval_sec;
        let ae_continuous = config.anti_entropy.continuous_enabled;
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let ae_worker = Arc::clone(&self.ae);
        bg.anti_entropy = Some(tokio::spawn(async move {
            if io_idle {
                oceanfs_storage::io::apply_background_io_class("anti-entropy");
            }
            if cpu_idle {
                oceanfs_storage::io::apply_background_cpu_sched("anti-entropy");
            }
            let mut interval = tokio::time::interval(Duration::from_secs(ae_interval_secs));
            loop {
                tokio::select! {
                    _ = ae_token.cancelled() => {
                        info!("Anti-entropy task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        let result = if ae_continuous {
                            ae_worker.run_continuous_cycle().await
                        } else {
                            ae_worker.run_cycle().await
                        };
                        if let Err(e) = result {
                            warn!("Anti-entropy cycle error: {e}");
                        }
                    }
                }
            }
        }));
        bg.ae_cancel = ae_cancel;

        // Scrub: runs every scrub_interval_sec from config.
        let scrub_cancel = CancellationToken::new();
        let scrub_token = scrub_cancel.clone();
        let scrub_data = storage.data_store.clone();
        let scrub_interval_secs = config.scrub_interval_sec;
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let scrub_worker = Arc::clone(&self.scrub);
        bg.scrub = Some(tokio::spawn(async move {
            if io_idle {
                oceanfs_storage::io::apply_background_io_class("scrub");
            }
            if cpu_idle {
                oceanfs_storage::io::apply_background_cpu_sched("scrub");
            }
            let mut interval = tokio::time::interval(Duration::from_secs(scrub_interval_secs));
            loop {
                tokio::select! {
                    _ = scrub_token.cancelled() => {
                        info!("Scrub task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        match scrub_worker
                            .run_cycle(Arc::clone(&scrub_registry), scrub_data.clone())
                            .await
                        {
                            Ok(report) => {
                                if report.segments_corrupt() > 0 {
                                    warn!(
                                        corrupt = report.segments_corrupt(),
                                        "scrub detected corrupt segments"
                                    );
                                }
                            }
                            Err(e) => warn!("Scrub cycle error: {e}"),
                        }
                    }
                }
            }
        }));
        bg.scrub_cancel = scrub_cancel;

        // Orphan reaper: runs every orphan_reaper_interval_sec from config.
        let reaper_cancel = CancellationToken::new();
        let reaper_token = reaper_cancel.clone();
        let reaper_interval = Duration::from_secs(config.orphan_reaper_interval_sec);
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let reaper = Arc::clone(&self.reaper);
        bg.orphan_reaper = Some(tokio::spawn(async move {
            if io_idle {
                oceanfs_storage::io::apply_background_io_class("orphan-reaper");
            }
            if cpu_idle {
                oceanfs_storage::io::apply_background_cpu_sched("orphan-reaper");
            }
            let mut interval = tokio::time::interval(reaper_interval);
            loop {
                tokio::select! {
                    _ = reaper_token.cancelled() => {
                        info!("Orphan reaper task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = reaper.run_cycle().await {
                            warn!("Orphan reaper cycle error: {e}");
                        }
                    }
                }
            }
        }));
        bg.reaper_cancel = reaper_cancel;

        // EC Heal worker: drains the HealQueue and repairs corrupt shards.
        let heal_cancel = CancellationToken::new();
        let heal_token = heal_cancel.clone();
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let heal_worker = Arc::clone(&self.heal);
        bg.heal = Some(tokio::spawn(async move {
            if io_idle {
                oceanfs_storage::io::apply_background_io_class("heal");
            }
            if cpu_idle {
                oceanfs_storage::io::apply_background_cpu_sched("heal");
            }
            heal_worker.run(heal_token).await;
            info!("Heal worker task completed");
        }));
        bg.heal_cancel = heal_cancel;

        // Hinted handoff delivery watcher token — the watcher below
        // consumes it; both live in this method.
        let delivery_cancel = CancellationToken::new();

        // Hinted handoff WAL periodic prune — removes expired entries
        // from all per-node WAL files to bound storage growth.
        let hint_prune_cancel = CancellationToken::new();
        let hint_prune_token = hint_prune_cancel.clone();
        let hint_ttl_secs = config.hint_ttl_sec;
        let hint_prune_interval = Duration::from_secs(config.hint_prune_interval_sec);
        let hh_prune = Arc::clone(&self.hinted_handoff_manager);
        bg.hinted_handoff_prune = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(hint_prune_interval);
            loop {
                tokio::select! {
                    _ = hint_prune_token.cancelled() => {
                        info!("Hinted handoff WAL prune task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        match hh_prune.prune_all_expired(hint_ttl_secs).await {
                            Ok(0) => {}
                            Ok(n) => {
                                info!(pruned = n, "pruned expired hinted handoff entries from per-node WALs");
                            }
                            Err(e) => {
                                warn!(error = %e, "hinted handoff WAL prune cycle error");
                            }
                        }
                    }
                }
            }
        }));
        bg.hint_prune_cancel = hint_prune_cancel;

        // Periodic reconciliation loop (g4 — ADR-0029 §D4 pull safety
        // net). Event-driven wake + bounded risk-prioritized queue +
        // hourly drift scan. Runs independently of announcements — the
        // complete safety net.
        let reconciliation_cancel = CancellationToken::new();
        let reconciliation_token = reconciliation_cancel.clone();
        let reconciliation_for_spawn = Arc::clone(&self.reconciliation);
        bg.reconciliation = Some(tokio::spawn(async move {
            reconciliation_for_spawn.run(reconciliation_token).await;
            info!("Reconciliation loop stopped");
        }));
        bg.reconciliation_cancel = reconciliation_cancel;

        // Re-replication worker + dispatcher (g5, ADR-0030 target-pull).
        // The worker drains the acquiring-side queue (fed by the
        // request_re_replication RPC handler) and pulls + writes +
        // stamps; the dispatcher retries parked requests that had no
        // eligible target.
        let rep_worker_cancel = CancellationToken::new();
        let rep_worker_token = rep_worker_cancel.clone();
        let rep_worker_for_spawn = Arc::clone(&self.rep_worker);
        bg.rep_worker = Some(tokio::spawn(async move {
            rep_worker_for_spawn.run(rep_worker_token).await;
            info!("Re-replication worker stopped");
        }));
        bg.rep_worker_cancel = rep_worker_cancel;

        let rep_dispatcher_cancel = CancellationToken::new();
        let rep_dispatcher_token = rep_dispatcher_cancel.clone();
        let rep_dispatcher_for_spawn = Arc::clone(&self.repair_dispatcher);
        bg.rep_dispatcher = Some(tokio::spawn(async move {
            rep_dispatcher_for_spawn.run(rep_dispatcher_token).await;
            info!("Re-replication dispatcher stopped");
        }));
        bg.rep_dispatcher_cancel = rep_dispatcher_cancel;

        // Hinted handoff delivery watcher (§17): watches for membership
        // events and drains the handoff buffer for nodes that are (or
        // return to) ALIVE. Any Alive event — including an Alive→Alive
        // address update from a rejoin (ADR-0022, t21) — triggers
        // delivery: `deliver_pending` is a no-op when nothing is
        // buffered. On the same events it also records the node's
        // address in the persisted fallback-seed list (ADR-0022 D3) —
        // incrementally from the event itself, so the write never races
        // the membership manager's apply step.
        let hh = Arc::clone(&self.hinted_handoff_manager);
        let seed_store = membership_state_store.clone();
        let self_node_id = NodeId::new(&config.node_id);
        let mut events = membership.subscribe();
        let delivery_token = delivery_cancel.clone();
        let mut sweep_interval =
            tokio::time::interval(Duration::from_secs(config.hint_delivery_sweep_sec.max(1)));
        let delivery_handle = tokio::spawn(async move {
            // Bounded retry helper shared by the event path and the sweep
            // path. The returning node's gRPC listener may still be
            // binding when the Alive event lands; a failed batch is
            // re-enqueued, so retries are safe (duplicates are
            // overwritten on the receiving side).
            //
            // Drains the ENTIRE queue per invocation: each iteration
            // delivers one batch (bounded by max_batch_size and
            // max_batch_bytes). The old single-batch drain could not
            // keep up with a full outage's hint debt — with the stable
            // N-node topology every mutation during an outage becomes
            // debt, and 7000 hints at 256/batch would take ~27 sweeps
            // (~135s) to drain, longer than the test settle window.
            // The batch cap (64) bounds one invocation so a
            // persistently-rejected batch cannot spin forever.
            async fn drain_hints(hh: &HintedHandoffManager, node_id: NodeId) {
                let mut batches = 0u32;
                while hh.pending_count(&node_id) > 0 && batches < 64 {
                    batches += 1;
                    match hh.deliver_pending(node_id.clone()).await {
                        Ok(0) => {
                            // Queue empty (or nothing deliverable this
                            // round) — stop.
                            break;
                        }
                        Ok(delivered) => {
                            info!(
                                node = %node_id,
                                delivered,
                                batch = batches,
                                "hinted handoff delivery"
                            );
                        }
                        Err(e) => {
                            warn!(
                                node = %node_id,
                                attempt = batches,
                                error = %e,
                                "hinted handoff delivery failed"
                            );
                            // A failed batch is re-enqueued at the
                            // front; a few quick retries cover the
                            // returning node's listener still binding,
                            // then give up this sweep (the next sweep
                            // retries).
                            if batches < 5 {
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                continue;
                            }
                            break;
                        }
                    }
                }
            }

            loop {
                tokio::select! {
                    _ = delivery_token.cancelled() => {
                        info!("Hinted handoff delivery watcher cancelled");
                        break;
                    }
                    _ = sweep_interval.tick() => {
                        // Periodic delivery sweep. Event-driven delivery
                        // is missed when THIS node is down during the
                        // recipient's Alive event, or when the event
                        // lands before the recipient's gRPC listener is
                        // ready. The sweep re-resolves addresses at sweep
                        // time, so pending hints drain as soon as the
                        // recipient is actually reachable — delivery is
                        // eventually-convergent under churn.
                        for node_id in hh.nodes_with_pending() {
                            drain_hints(&hh, node_id).await;
                        }
                    }
                    event = events.recv() => {
                        match event {
                            Ok(ev) if ev.new_state == oceanfs_core::NodeState::Alive => {
                                info!(
                                    node = %ev.node_id,
                                    "node returned to cluster; delivering pending hinted handoffs"
                                );
                                // Only spend retries when hints are actually
                                // buffered.
                                if hh.pending_count(&ev.node_id) > 0 {
                                    drain_hints(&hh, ev.node_id.clone()).await;
                                }
                                // Record the member address as a fallback
                                // seed (ADR-0022 D3). Self is skipped: the
                                // node's own old address is useless after a
                                // restart (t43). The MEMBERSHIP PLANE
                                // address is recorded — the join dials
                                // fallback seeds for the gossip pull
                                // (ADR-0028 D1); the data-plane address in
                                // `ev.address` would dial a port that does
                                // not serve GossipRpc (observed: persisted
                                // data addresses made every rejoin pull
                                // fail with Unimplemented and strand the
                                // restarted bootstrap node). Detector
                                // recovery events carry
                                // membership_address=None and are skipped —
                                // the address was recorded when the node
                                // joined.
                                if ev.node_id != self_node_id {
                                    if let Some(addr) = ev.membership_address {
                                        if let Err(e) = seed_store
                                            .add_fallback_seed(&addr.to_string())
                                        {
                                            warn!(error = %e, "failed to persist fallback seed");
                                        }
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                warn!(skipped = n, "hinted handoff watcher lagged");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                info!("Membership event channel closed; stopping delivery watcher");
                                break;
                            }
                        }
                    }
                }
            }
        });
        bg.delivery_cancel = delivery_cancel;
        bg.hinted_handoff_delivery = Some(delivery_handle);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use oceanfs_core::NodeConfig;

    use super::DurabilityModule;
    use crate::modules::storage::test_support::build_storage_prelude;

    async fn build_durability(
        tmp: &tempfile::TempDir,
    ) -> (NodeConfig, Arc<crate::modules::durability::DurabilityModule>) {
        let prelude = build_storage_prelude(tmp).await;
        let module = Arc::new(
            DurabilityModule::build(
                &prelude.config,
                &prelude.module,
                prelude.membership.clone(),
                prelude.pool.clone(),
                &prelude.module.paths,
                "127.0.0.1:0".parse().expect("grpc addr"),
            )
            .await
            .expect("durability module build"),
        );
        (prelude.config, module)
    }

    /// c2 DoD: the builder returns a live bundle whose workers are wired
    /// to the c1 storage module's single shared stores (no worker
    /// constructs its own store — the store-unification one-site rule).
    #[tokio::test]
    async fn build_returns_live_workers_wired_to_the_shared_stores() {
        let tmp = tempfile::tempdir().unwrap();
        let (_config, module) = build_durability(&tmp).await;

        // Distinct worker objects, all present.
        let ptrs = [
            Arc::as_ptr(&module.gc) as *const (),
            Arc::as_ptr(&module.ae) as *const (),
            Arc::as_ptr(&module.scrub) as *const (),
            Arc::as_ptr(&module.reaper) as *const (),
            Arc::as_ptr(&module.heal) as *const (),
            Arc::as_ptr(&module.reconciliation) as *const (),
            Arc::as_ptr(&module.rep_worker) as *const (),
            Arc::as_ptr(&module.repair_dispatcher) as *const (),
        ];
        for (i, a) in ptrs.iter().enumerate() {
            for b in &ptrs[i + 1..] {
                assert_ne!(a, b, "workers must be distinct objects");
            }
        }

        // The heal worker is a live HealWorker (Arc — runnable from
        // behind the Arc; c2 D2) and the codec/timeout handles exist.
        assert!(Arc::strong_count(&module.heal) >= 1);
        assert!(module.op_timeouts.write_queue_ms > 0);
        let _ = &module.ec_decoder;
        let _ = &module.codec_config;
    }

    /// c2 DoD: `register_metrics` registers every worker's counters with
    /// the central registry in one call (mirrors the node §12 call).
    #[tokio::test]
    async fn register_metrics_covers_all_workers() {
        let tmp = tempfile::tempdir().unwrap();
        let (_config, module) = build_durability(&tmp).await;

        let metrics = Arc::new(oceanfs_server::admin::MetricsRegistry::new());
        module.register_metrics(&*metrics);
        // Registration is idempotent (the registry dedups by name) and
        // must not panic on a second pass either.
        module.register_metrics(&*metrics);
    }
}
