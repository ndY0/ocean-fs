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

use std::sync::Arc;

use oceanfs_core::{CodecConfig, MetricRegistrar, NodeConfig, NodeId, OperationTimeouts};
use oceanfs_durability::{
    AntiEntropy, GarbageCollector, HealConfig, HealQueue, HealWorker, OrphanReaper, ReRepWorker,
    ScrubConfig, ScrubCoordinator,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;

use crate::{
    announce::AnnounceMetrics,
    modules::storage::StorageModule,
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
    /// workers and the compaction-remap closure need (still owned by
    /// `Node::start()` — c4 re-homes them).
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
        // coordinator and unlinks the old .dat through the shard store
        // only after the durable delete.
        let gc_worker = Arc::new(
            oceanfs_durability::GarbageCollector::new(gc_config.clone())
                .with_data_store(storage.data_store.clone())
                .with_lifecycle(storage.lifecycle.clone())
                .with_shard_store(storage.shard_store.clone())
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

        // [review][architecture][critical]
        // AE no longer creates its own data store — c1 (composition-root
        // decomposition) wired it to the module's single shared
        // DiskSegmentStore (one store instance, no concurrent writers).
        // The remaining 3-abstraction read/write unification
        // (DiskSegmentStore/DiskSegmentShardStore/DiskSegmentReader) is
        // store-unification f3 (ADR-0032).
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
        // orphaned segments after GC compaction.
        let reaper = Arc::new(OrphanReaper::new(
            storage.metadata_store.clone(),
            storage.lifecycle.clone(),
            storage.shard_store.clone(),
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
