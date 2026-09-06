//! Background bundler (c5 — composition-root decomposition).
//!
//! This module does NOT contain loop code: each subsystem worker owns
//! its startup sequence (the module-owned `spawn_*` methods in
//! storage/durability/server/membership/data_plane). This module only
//! *bundles* — it calls every module's spawn entry, spawns the two
//! node-level pieces that no subsystem owns (the process/WAL metric
//! poller — review #68: cancellable — and the health-consequence
//! applier composition over the storage monitor's event stream), and
//! assembles the single [`BackgroundTasks`] value `Node` stores.

use std::sync::Arc;

use oceanfs_core::NodeConfig;
use oceanfs_server::admin::MetricsRegistry;
use tracing::info;

use crate::{
    membership_state::MembershipStateStore,
    modules::{
        data_plane::DataPlaneModule, durability::DurabilityModule, membership::MembershipModule,
        server, storage::StorageModule,
    },
    node::BackgroundTasks,
};

/// Bundles every background loop of the running node (c5).
///
/// Calls the module-owned spawn entries in dependency order and fills
/// the returned [`BackgroundTasks`]. Must run AFTER the data-plane
/// binds and the membership-plane start (`serve` +
/// `start_plane_and_join`): the durability delivery watcher subscribes
/// to the started membership event stream.
///
/// # Parameters
///
/// `config` is the validated node config; `storage`/`durability` are the
/// c1/c2 bundles; `prefetch_engine` is the c3 server module's engine
/// (its other fields were moved into the data-plane binds);
/// `membership_module` provides the
/// membership Arc, the manifest cache, the announce incarnation and the
/// durable state store; `data_plane` provides the shared pool;
/// `membership_state_store` is the durable store the delivery watcher
/// records fallback seeds into; `metrics` is the central registry the
/// process-level gauges + poller register against; `wal_dir` is the
/// role-pinned storage WAL root the poller counts files in.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_all(
    config: &NodeConfig,
    storage: &StorageModule,
    durability: &DurabilityModule,
    prefetch_engine: Arc<oceanfs_cache::PrefetchEngine>,
    membership_module: &MembershipModule,
    data_plane: &DataPlaneModule,
    membership_state_store: MembershipStateStore,
    metrics: Arc<MetricsRegistry>,
    wal_dir: std::path::PathBuf,
) -> BackgroundTasks {
    let mut bg = BackgroundTasks::new();

    // Cluster-readiness gate (membership-plane owned).
    bg.ready_gate = membership_module.spawn_ready_gate();

    // Durability-owned loops: GC/AE/scrub/reaper/heal, hint prune +
    // delivery watcher, reconciliation, re-rep worker + dispatcher.
    durability.spawn_loops(
        config,
        storage,
        Arc::clone(&membership_module.membership),
        membership_state_store,
        &mut bg,
    );

    // Storage-owned loops: pool health monitor (returns the event
    // stream) + segment replicator.
    let health_events = storage.spawn_loops(&mut bg);

    // The health-consequence applier (crate::health — its own worker)
    // maps pool status → role consequences and re-declares the
    // manifest. The loss announcer (g3 fast path) is built here: it is
    // composition glue over membership/storage/durability handles, not
    // a loop of its own.
    let loss_announcer: Option<crate::health::LossAnnouncer> = if config.announcements_enabled {
        let lifecycle_registry = Arc::clone(&storage.lifecycle_registry);
        let membership = Arc::clone(&membership_module.membership);
        let pool = Arc::clone(&data_plane.pool);
        let self_id = oceanfs_core::NodeId::new(&config.node_id);
        let announce_metrics = Arc::clone(&durability.announce_metrics);
        Some(Arc::new(move |pool_id, affected| {
            let lifecycle_registry = Arc::clone(&lifecycle_registry);
            let membership = Arc::clone(&membership);
            let pool = Arc::clone(&pool);
            let self_id = self_id.clone();
            let announce_metrics = Arc::clone(&announce_metrics);
            tokio::spawn(async move {
                if affected.is_empty() {
                    return;
                }
                // Pinned fan-out: the union of every affected
                // segment's storage_locations, minus self. NOT the
                // whole cluster, NOT ring.lookup.
                let locations: Vec<(oceanfs_core::SegmentId, Vec<oceanfs_core::NodeId>)> = affected
                    .iter()
                    .filter_map(|segment_id| {
                        lifecycle_registry
                            .get(*segment_id)
                            .map(|entry| (*segment_id, entry.metadata.storage_locations.to_vec()))
                    })
                    .collect();
                let targets = crate::announce::derive_fan_out_targets(&locations, &self_id);
                if targets.is_empty() {
                    tracing::debug!(
                        pool_id,
                        affected = affected.len(),
                        "loss announcement: no peer holders to notify"
                    );
                    return;
                }
                match crate::announce::announce_pool_loss(
                    &self_id,
                    pool_id,
                    &affected,
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
                            pool_id,
                            affected = affected.len(),
                            targets = targets.len(),
                            "loss announcement fanned out"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            pool_id,
                            affected = affected.len(),
                            error = %e,
                            "loss announcement not fully delivered (g4 failsafe)"
                        );
                    }
                }
            });
        }))
    } else {
        tracing::info!("g3 loss announcements disabled (announcements_enabled=false)");
        None
    };
    bg.health_consequences = Some(crate::health::spawn_health_consequences(
        health_events,
        storage.registry.clone(),
        Arc::clone(&membership_module.membership),
        Arc::clone(&storage.lifecycle_registry),
        oceanfs_core::NodeId::new(&config.node_id),
        membership_module.announce_incarnation,
        Arc::clone(&membership_module.manifest_cache),
        loss_announcer,
        bg.health_consequences_cancel.clone(),
    ));

    // Server-owned loop: the prefetch pre-warmer keep-alive.
    server::spawn_prefetch_loop(prefetch_engine, &mut bg);

    // Data-plane pool health checks (perf rule §4.1 — the loop lives in
    // oceanfs-network; it runs until cancelled during shutdown).
    let health_check_cancel = bg.health_check_cancel.clone();
    data_plane.pool.start_health_check_loop(health_check_cancel);

    // Process-level gauges + the cancellable 15s poller (review #68).
    // RocksDB metrics are polled separately by the storage module's
    // `register_metrics` (start_metrics_task).
    let proc_mem_gauge = metrics.gauge("process_resident_memory_bytes", "Resident memory in bytes");
    let proc_fd_gauge = metrics.gauge("process_open_fds", "Open file descriptors");
    // Storage WAL file count — the Phase 2 `wal_not_unbounded`
    // invariant (sealed segments must keep the WAL consumed).
    let wal_count_gauge = metrics.gauge("wal_file_count", "Storage WAL files present");
    // Live segment-pipeline gauge: the shard registration above sets
    // the initial value; this poller refreshes it from the pools'
    // Appending slots, which churn as segments fill and seal.
    let active_segments_gauge =
        metrics.gauge("segment_active_count", "Active segment groups in the sharded pool");
    let active_pools_for_metrics = storage.active_pools.clone();
    let poller_token = bg.metric_poller_cancel.clone();
    bg.metric_poller = Some(tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            tokio::select! {
                _ = poller_token.cancelled() => {
                    info!("Metric poller cancelled");
                    break;
                }
                _ = interval.tick() => {
                    if let Ok(mem) = crate::node::read_process_memory_bytes() {
                        proc_mem_gauge.set(mem);
                    }
                    if let Ok(fds) = crate::node::read_process_open_fds() {
                        proc_fd_gauge.set(fds);
                    }
                    let wal_config = oceanfs_core::WalConfig {
                        data_dir: wal_dir.clone(),
                        ..oceanfs_core::WalConfig::default()
                    };
                    wal_count_gauge
                        .set(oceanfs_storage::count_wal_files(&wal_config) as u64);
                    let live = active_pools_for_metrics
                        .iter()
                        .map(|p| p.active_count())
                        .sum::<usize>();
                    active_segments_gauge.set(live as u64);
                }
            }
        }
    }));

    bg
}
