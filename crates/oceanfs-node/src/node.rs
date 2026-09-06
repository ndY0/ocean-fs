//! Composition root: wires all subsystem crates into a running OceanFS node.
//!
//! This is the **only** crate allowed to import concrete types from multiple
//! subsystem crates per architecture.md §4.1. It constructs every component,
//! injects dependencies via `Arc`, spawns background tasks, and binds the
//! HTTP + gRPC servers.
//!
//! Broader design remarks formerly kept in this header (adaptive scan
//! strategies, DI/composition, durability layout, in-memory scans) are
//! tracked in `docs/features/refactoring/review-2026-09-roadmap.md`.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use oceanfs_core::{AccelConfig, MetadataConfig, NodeConfig, NodeId, RingConfig, SegmentId};
#[cfg(test)]
use oceanfs_core::{BucketId, ObjectKey, SegmentSizeConfig};
/// A re-replication repair request (g5 ReRepWorker input).
pub use oceanfs_durability::healing_service::ReRepRequest as RepairRequest;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[cfg(test)]
use crate::modules::membership::cluster_ready_gate_opens;
#[cfg(test)]
use crate::modules::server::PrefetchStoreAdapter;

// ---------------------------------------------------------------------------
// BackgroundTasks
// ---------------------------------------------------------------------------
// [review][architecture][critical][resolved]
// RESOLVED by ADR-0017 (accepted) + its 2026-09-06 two-tier amendment and
// this epic: the four scheduled housekeeping cycles (GC/orphan/scrub/AE)
// run on a single DurabilityScheduler under a shared Tier-1 budget, and the
// data-layer repair workers (heal/re-rep/inbound hint apply) draw from a
// Tier-0 budget that is never gated behind housekeeping. The event-driven
// "reactor" alternative was explicitly rejected (ADR-0017 §Considered-C;
// review-2026-09 roadmap wave 5).
// [end]
/// Aggregated join handles and cancellation tokens for background loops.
pub struct BackgroundTasks {
    /// Durability scheduler (ADR-0017) — drives the four Tier-1
    /// housekeeping cycles (GC/orphan/scrub/AE) under the shared budget.
    pub(crate) durability_scheduler: Option<JoinHandle<()>>,
    /// Durability scheduler cancellation token.
    pub(crate) scheduler_cancel: CancellationToken,

    /// Prefetch engine background pre-warmer (only if prefetch is enabled).
    pub(crate) prefetch: Option<JoinHandle<()>>,
    /// Prefetch cancellation token.
    pub(crate) prefetch_cancel: CancellationToken,

    /// EC Heal worker task.
    pub(crate) heal: Option<JoinHandle<()>>,
    /// Heal worker cancellation token.
    pub(crate) heal_cancel: CancellationToken,

    /// Hinted handoff delivery watcher task.
    pub(crate) hinted_handoff_delivery: Option<JoinHandle<()>>,
    /// Hinted handoff delivery cancellation token.
    pub(crate) delivery_cancel: CancellationToken,

    /// Hinted handoff WAL prune task.
    pub(crate) hinted_handoff_prune: Option<JoinHandle<()>>,
    /// Hinted handoff WAL prune cancellation token.
    pub(crate) hint_prune_cancel: CancellationToken,

    /// gRPC server task handle for graceful shutdown.
    pub(crate) grpc_server: Option<JoinHandle<()>>,
    /// gRPC server cancellation token.
    pub(crate) grpc_shutdown: CancellationToken,
    /// HTTP server task handle for graceful shutdown (the axum router
    /// holds the coordinator/store clones — it must be awaited before
    /// the metadata store closes).
    pub(crate) http_server: Option<JoinHandle<()>>,

    /// Membership-plane gossip/probe gRPC serve task handle.
    pub(crate) membership_grpc: Option<JoinHandle<()>>,
    /// Membership-plane routing-cache event subscriber task handle.
    pub(crate) membership_subscriber: Option<JoinHandle<()>>,
    /// Membership-plane background rejoin loop handle (`None` for
    /// single-node deployments).
    pub(crate) membership_rejoin: Option<JoinHandle<()>>,
    /// Cluster-readiness gate loop handle (`None` for single-node
    /// deployments).
    pub(crate) ready_gate: Option<JoinHandle<()>>,

    /// Health check loop cancellation token.
    pub(crate) health_check_cancel: CancellationToken,

    /// Pool health monitor task (g2): ticks pools through the D3 state
    /// machine. `None` before the monitor is spawned.
    pub(crate) health_monitor: Option<JoinHandle<()>>,
    /// Pool health monitor cancellation token.
    pub(crate) health_cancel: CancellationToken,
    /// Health-consequence applier task (role matrix + manifest
    /// re-declaration). `None` before the applier is spawned.
    pub(crate) health_consequences: Option<JoinHandle<()>>,
    /// Health-consequence applier cancellation token.
    pub(crate) health_consequences_cancel: CancellationToken,

    /// Seal-time segment replicator task (sealed-segment-replication).
    /// `None` before the replicator is spawned.
    pub(crate) segment_replicator: Option<JoinHandle<()>>,
    /// Segment replicator cancellation token.
    pub(crate) segment_replicator_cancel: CancellationToken,

    /// Periodic reconciliation loop task (g4 `reconciliation` — the
    /// ADR-0029 §D4 pull safety net). `None` before the loop is spawned.
    pub(crate) reconciliation: Option<JoinHandle<()>>,
    /// Reconciliation loop cancellation token.
    pub(crate) reconciliation_cancel: CancellationToken,

    /// Re-replication worker task (g5, ADR-0030 target-pull — the
    /// acquiring-side executor). `None` before the worker is spawned.
    pub(crate) rep_worker: Option<JoinHandle<()>>,
    /// Re-replication worker cancellation token.
    pub(crate) rep_worker_cancel: CancellationToken,
    /// Re-replication dispatcher task (g5 — the holder-side router).
    /// `None` before the dispatcher is spawned.
    pub(crate) rep_dispatcher: Option<JoinHandle<()>>,
    /// Re-replication dispatcher cancellation token.
    pub(crate) rep_dispatcher_cancel: CancellationToken,

    /// Process/WAL metric poller task (review #68 — cancellable).
    pub(crate) metric_poller: Option<JoinHandle<()>>,
    /// Metric poller cancellation token.
    pub(crate) metric_poller_cancel: CancellationToken,
}

impl BackgroundTasks {
    /// Creates an empty handle set: every loop is `None`; every
    /// cancellation token fresh. The c5 module-owned spawn methods fill
    /// the per-loop fields (each worker owns its startup sequence; the
    /// background bundler calls them and assembles this value).
    pub(crate) fn new() -> Self {
        Self {
            durability_scheduler: None,
            scheduler_cancel: CancellationToken::new(),
            prefetch: None,
            prefetch_cancel: CancellationToken::new(),
            heal: None,
            heal_cancel: CancellationToken::new(),
            hinted_handoff_delivery: None,
            delivery_cancel: CancellationToken::new(),
            hinted_handoff_prune: None,
            hint_prune_cancel: CancellationToken::new(),
            grpc_server: None,
            grpc_shutdown: CancellationToken::new(),
            http_server: None,
            membership_grpc: None,
            membership_subscriber: None,
            membership_rejoin: None,
            ready_gate: None,
            health_check_cancel: CancellationToken::new(),
            health_monitor: None,
            health_cancel: CancellationToken::new(),
            health_consequences: None,
            health_consequences_cancel: CancellationToken::new(),
            segment_replicator: None,
            segment_replicator_cancel: CancellationToken::new(),
            reconciliation: None,
            reconciliation_cancel: CancellationToken::new(),
            rep_worker: None,
            rep_worker_cancel: CancellationToken::new(),
            rep_dispatcher: None,
            rep_dispatcher_cancel: CancellationToken::new(),
            metric_poller: None,
            metric_poller_cancel: CancellationToken::new(),
        }
    }
}

/// Resolves a RocksDB property string to a u64. Used by tests.
#[cfg(test)]
fn property_as_u64(store: &oceanfs_storage::RocksDbMetadataStore, name: &str) -> Option<u64> {
    store.property(name).and_then(|v| v.parse::<u64>().ok())
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

/// A running OceanFS node.
///
/// Owns live references to all subsystem components so they remain
/// alive for the lifetime of the node. Storage-side components live in
/// `crate::modules::storage::StorageModule` (c1); the durability
/// workers (GC/AE/scrub/reaper/heal/reconciliation/re-rep) live in
/// `crate::modules::durability::DurabilityModule` (c2); c3–c5 extract
/// the server/handlers, network, and background-spawn surfaces.
pub struct Node {
    /// Node configuration.
    config: Arc<NodeConfig>,
    /// Bound HTTP server socket address.
    server_addr: SocketAddr,
    /// Bound gRPC server socket address.
    grpc_addr: SocketAddr,
    /// Cancellation token for the HTTP server graceful shutdown.
    http_shutdown: CancellationToken,
    /// Cancellation token for the gRPC server graceful shutdown.
    grpc_shutdown: CancellationToken,
    /// Background task handles and cancellation tokens.
    background: BackgroundTasks,
    /// The prefetch engine (c3) — retained so shutdown can stop its
    /// background worker (`PrefetchEngine::shutdown`) before the
    /// metadata store closes.
    prefetch_engine: Option<Arc<oceanfs_cache::PrefetchEngine>>,
    /// Cluster membership for leave signaling and observability.
    membership: Arc<oceanfs_membership::Membership>,
    /// The storage subsystem bundle (c1 — `modules/storage.rs`): pool
    /// registry, metadata store, WAL, lifecycle machinery, the two shared
    /// segment stores, the replicator, and the write-path pools.
    storage: crate::modules::storage::StorageModule,
    /// The durability subsystem bundle (c2 — `modules/durability.rs`):
    /// GC/AE/scrub/reaper/heal workers, the reconciliation loop, the
    /// re-replication worker + dispatcher, op timeouts. Kept alive by
    /// the node; the worker task handles live in `BackgroundTasks`.
    durability: crate::modules::durability::DurabilityModule,
}

// [review][architectural][critical]
// the startup function is more than a thousand of lines. i would like
// to consider using compile time Dependency injection, to decompose the responsibilities of setup to the modules themselves,
// and to properly be able to compose the application. for this, i would like you to reviez the shaku crate,
// and we should have a discussion about it.
// this should also be a good point to discuss module organisation / distribution : right now, it seems we construct analogous
// or equal constructs for different submodules, because of the initial layout and incremental nature of the construction
// of the start method.
// i believe that maintainability will necessary stem from a clear dependency module graph,
// a rationalisation of the abstractions we currently use, and the use of a proper composability helper crate
// [end]
/// Builds the storage-side seal-pipeline notifier (c3a): the
/// sealed-segment fan-out to the continuous anti-entropy tree and the
/// seal-time segment replicator — a single non-blocking channel send,
/// NO network on the seal path.
fn sealed_segment_notifier(
    ae: &Arc<oceanfs_durability::AntiEntropy>,
    replicator: &Arc<crate::segment_replicator::SegmentReplicator>,
) -> oceanfs_storage::segment::seal_pipeline::SealedSegmentNotifier {
    let ae_for_seal_notify = Arc::clone(ae);
    let replicator_for_seal_notify = Arc::clone(replicator);
    Arc::new(move |segment_id, merkle_root| {
        ae_for_seal_notify.on_segment_sealed(segment_id, merkle_root);
        replicator_for_seal_notify.enqueue(segment_id);
    })
}

/// Configures the rayon global thread pool for EC encode/decode
/// (review marker: rayon was superseded by the tokio executor — the
/// consumer side still uses the pool, so the setup stays).
fn init_rayon_pool() {
    if let Err(e) = rayon::ThreadPoolBuilder::new()
        .num_threads(
            std::thread::available_parallelism()
                .map(|n| n.get().saturating_sub(2).max(1))
                .unwrap_or(2),
        )
        .thread_name(|i| format!("oceanfs-rayon-{i}"))
        .build_global()
    {
        tracing::warn!(error = %e, "rayon global pool already initialized; using existing");
    }
}

/// The pre-module shared infrastructure tuple
/// (registry, role-pinned paths, metadata store, accel, ring).
type Infra = (
    Arc<oceanfs_storage::PoolRegistry>,
    crate::pool_paths::PoolPaths,
    Arc<oceanfs_storage::RocksDbMetadataStore>,
    Arc<oceanfs_accel::AccelDispatcher>,
    Arc<oceanfs_routing::RingCache>,
);

/// Builds the shared pre-module infrastructure (`Node::start()` §1):
/// the storage pool registry + role-pinned paths (ADR-0029/ADR-0031),
/// the metadata store, the probed acceleration dispatcher and the
/// routing ring cache. Every module builder consumes these; no module
/// owns them (c5 keeps them in the composition root as plain inputs).
fn build_infrastructure(config: &NodeConfig) -> Result<Infra, String> {
    // ---- 0. Storage pool registry (ADR-0029) + role-pinned paths ----
    // The registry probes every configured pool root at boot: the
    // `Fatal` policy refuses to start on an unprobeable root, the
    // `Degraded` policy registers the pool as Degraded. Pools are
    // mandatory (ADR-0031): an empty `[storage.pools]` fails startup
    // here with the role-listing error. The role-pinned dirs resolve
    // ONCE here — the write path never re-resolves them (perf
    // guidelines 3.4/7.1: boot-time only, no locks in the hot path).
    let pool_registry = Arc::new(
        oceanfs_storage::PoolRegistry::from_config(&config.storage, &config.data_dir)
            .map_err(|e| format!("storage pool registry: {e}"))?,
    );
    let paths = crate::pool_paths::pool_paths(&pool_registry);

    // ---- 1. Open metadata store ----
    // [review][config][high]
    // metadata config : the configuration is mostly default hardcoded values, without any inputed user config.
    // the metadata store must be configurable, a part of the configuration is dedicated to it.
    // [end]
    let metadata_config = MetadataConfig { data_dir: paths.metadata.clone(), ..Default::default() };
    let metadata_store = Arc::new(
        oceanfs_storage::RocksDbMetadataStore::open(&metadata_config)
            .map_err(|e| format!("failed to open metadata store: {e}"))?,
    );
    // [review][config][high]
    // acceleration config : same comment that of the metadata store. what is the point of config if static
    // [end]
    // ---- 2. Probe acceleration hardware ----
    let accel_config = AccelConfig::default();
    let accel = Arc::new(oceanfs_accel::AccelDispatcher::new(accel_config));

    // ---- 3. Construct routing ----
    let ring_config = RingConfig {
        vnodes_per_node: config.vnodes_per_node,
        replication_factor: config.replication_factor as u8,
    };
    let ring = oceanfs_routing::Ring::new(ring_config);
    let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));

    Ok((pool_registry, paths, metadata_store, accel, ring_cache))
}

impl Node {
    /// Starts an OceanFS node: wires all subsystems, binds servers,
    /// spawns background tasks, and returns a running [`Node`].
    ///
    /// # Errors
    ///
    /// Returns an error if RocksDB cannot be opened, ports cannot be
    /// bound, or any subsystem fails to initialize.
    pub async fn start(config: NodeConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let config = Arc::new(Self::validate_config(config)?);

        init_rayon_pool();

        info!(
            node_id = %config.node_id,
            listen_addr = %config.listen_addr,
            grpc_addr = %config.grpc_listen_addr,
            "Starting OceanFS node"
        );

        // ---- 1. Shared infrastructure (registry, paths, metadata store, accel, ring) ----
        let (pool_registry, paths, metadata_store, accel, ring_cache) =
            build_infrastructure(&config)?;

        // ---- 2. Membership + data-plane modules (c4 - planes split) ----
        let membership_module =
            crate::modules::membership::MembershipModule::build(&config, ring_cache.clone())?;
        let data_plane_module = crate::modules::data_plane::DataPlaneModule::build(&config)?;
        // Startup aliases: the rest of the sequence consumes these; the
        // modules keep their own handles alive.
        let membership = Arc::clone(&membership_module.membership);
        let pool = Arc::clone(&data_plane_module.pool);
        let grpc_addr = membership_module.grpc_addr;

        // [review][config][critical]
        // segment tiers sizes should be configurable by the end user too
        // [end]
        // ---- 3. Storage subsystem (c1: modules/storage.rs) ----
        let storage = crate::modules::storage::StorageModule::build(
            &config,
            &paths,
            pool_registry,
            metadata_store,
            accel,
            ring_cache.clone(),
            membership.clone(),
            pool.clone(),
        )
        .await?;

        // ---- 4. Durability workers (c2: modules/durability.rs) ----
        let durability = crate::modules::durability::DurabilityModule::build(
            &config,
            &storage,
            membership.clone(),
            pool.clone(),
            &paths,
            grpc_addr,
        )
        .await?;

        // ---- 5. Start seal pipeline + startup recovery (c1/c3a) ----
        storage.start_seal_pipeline(Some(sealed_segment_notifier(
            &durability.ae,
            &storage.segment_replicator,
        )));
        // (c1: moved to modules/storage.rs — run_startup_recovery)
        storage.run_startup_recovery().await?;
        // The AE Merkle tree is empty at construction (the registry is
        // empty pre-recovery). On a NORMAL boot rebuild it from the
        // folded registry so continuous AE covers the machine's
        // pre-existing Sealed segments (the segments-CF-removal ordering
        // gap, closed). On a replaced-wal boot the registry is still
        // empty here (the rebuild-from-holders drain runs after
        // spawn_all, step 8b, which rebuilds the AE tree afterwards).
        if !storage.replaced_wal_recovery_pending.load(std::sync::atomic::Ordering::Acquire) {
            durability.rebuild_ae_tree()?;
        }

        // ---- 6. Server subsystem (c3: modules/server.rs) ----
        let metrics = Arc::new(oceanfs_server::admin::MetricsRegistry::new());
        let server = crate::modules::server::ServerModule::build(
            &config,
            &storage,
            &durability,
            membership.clone(),
            pool.clone(),
            ring_cache.clone(),
            membership_module.manifest_cache.clone(),
            durability.hinted_handoff.clone(),
            durability.hinted_handoff_manager.clone(),
            membership_module.ready_gate.clone(),
            membership_module.is_cluster_node,
            membership_module.announce_incarnation,
            metrics.clone(),
        )?;
        storage.register_metrics(&metrics);
        durability.register_metrics(&*metrics);
        data_plane_module.register_metrics(&*metrics);
        membership_module.register_metrics(&*metrics);

        // ---- 7. Bind data plane + start membership plane (c4) ----
        let crate::modules::server::ServerModule { router, grpc, prefetch_engine } = server;
        let bound = data_plane_module.serve(router, grpc).await?;
        let plane =
            membership_module.start_plane_and_join(metrics.clone(), &storage.registry).await?;

        // ---- 8. Bundle + spawn background loops (c5: modules/background.rs) ----
        let mut background = crate::modules::background::spawn_all(
            &config,
            &storage,
            &durability,
            Arc::clone(&prefetch_engine),
            &membership_module,
            &data_plane_module,
            membership_module.membership_state_store.clone(),
            metrics.clone(),
            paths.wal.clone(),
        );
        background.grpc_shutdown = bound.grpc_shutdown;
        background.grpc_server = Some(bound.grpc_server_handle);
        background.http_server = Some(bound.http_server_handle);
        background.membership_grpc = Some(plane.grpc_handle);
        background.membership_subscriber = Some(plane.subscriber_handle);
        background.membership_rejoin = plane.rejoin_handle;

        // ---- 8b. Deferred replaced-wal recovery (g7, ADR-0035) ----
        // When the boot path detected a replaced wal pool, the durability
        // module's wal-recovery coordinator runs the rebuild-from-holders
        // drain now that `spawn_all` started the ReRepWorker + membership
        // plane (the wal-Dead 503 gate holds writes throughout; reads may
        // serve — the objects CF and data pools are intact).
        durability.run_deferred_wal_recovery().await?;

        info!(
            node_id = %config.node_id,
            http_addr = %bound.server_addr,
            grpc_addr = %bound.grpc_addr,
            "OceanFS node started"
        );

        Ok(Node {
            config,
            server_addr: bound.server_addr,
            grpc_addr: bound.grpc_addr,
            http_shutdown: bound.http_shutdown,
            grpc_shutdown: background.grpc_shutdown.clone(),
            background,
            prefetch_engine: Some(prefetch_engine),
            membership,
            storage,
            durability,
        })
    }

    /// Returns the live storage-pool registry (ADR-0029) — the pool set
    /// the attach surface mutates and tests observe.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example() {
    /// use oceanfs_core::NodeConfig;
    /// use oceanfs_node::Node;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # fn storage_pools(tmp: &std::path::Path) -> oceanfs_core::StorageConfig {
    /// #     fn pool(name: &str, role: oceanfs_core::PoolRole, root: std::path::PathBuf) -> oceanfs_core::StoragePoolConfig {
    /// #         oceanfs_core::StoragePoolConfig {
    /// #             name: name.into(),
    /// #             role,
    /// #             root,
    /// #             weight: None,
    /// #             tech: Default::default(),
    /// #             health: Default::default(),
    /// #         }
    /// #     }
    /// #     oceanfs_core::StorageConfig {
    /// #         pools: vec![
    /// #             pool("data-0", oceanfs_core::PoolRole::Data, tmp.join("pool-data")),
    /// #             pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.join("pool-wal")),
    /// #             pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.join("pool-meta")),
    /// #             pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.join("pool-hints")),
    /// #         ],
    /// #         missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    /// #     }
    /// # }
    /// # let config = NodeConfig {
    /// #     data_dir: tmp.path().join("data"),
    /// #     listen_addr: "127.0.0.1:0".into(),
    /// #     grpc_listen_addr: "127.0.0.1:0".into(),
    /// #     membership_listen_addr: "127.0.0.1:0".into(),
    /// #     storage: storage_pools(&tmp.path()),
    /// #     ..NodeConfig::default()
    /// # };
    /// let node = Node::start(config).await.expect("node");
    /// assert!(node.pool_registry().pool_count() >= 1);
    /// node.shutdown().await.expect("shutdown");
    /// # }
    /// ```
    pub fn pool_registry(&self) -> Arc<oceanfs_storage::PoolRegistry> {
        self.storage.registry.clone()
    }

    /// Returns the g1 per-pool I/O signal observer (ADR-0029 §D3) the
    /// seal pipeline records write/fsync signals into.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example() {
    /// use oceanfs_core::NodeConfig;
    /// use oceanfs_node::Node;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # fn storage_pools(tmp: &std::path::Path) -> oceanfs_core::StorageConfig {
    /// #     fn pool(name: &str, role: oceanfs_core::PoolRole, root: std::path::PathBuf) -> oceanfs_core::StoragePoolConfig {
    /// #         oceanfs_core::StoragePoolConfig {
    /// #             name: name.into(),
    /// #             role,
    /// #             root,
    /// #             weight: None,
    /// #             tech: Default::default(),
    /// #             health: Default::default(),
    /// #         }
    /// #     }
    /// #     oceanfs_core::StorageConfig {
    /// #         pools: vec![
    /// #             pool("data-0", oceanfs_core::PoolRole::Data, tmp.join("pool-data")),
    /// #             pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.join("pool-wal")),
    /// #             pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.join("pool-meta")),
    /// #             pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.join("pool-hints")),
    /// #         ],
    /// #         missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    /// #     }
    /// # }
    /// # let config = NodeConfig {
    /// #     data_dir: tmp.path().join("data"),
    /// #     listen_addr: "127.0.0.1:0".into(),
    /// #     grpc_listen_addr: "127.0.0.1:0".into(),
    /// #     membership_listen_addr: "127.0.0.1:0".into(),
    /// #     storage: storage_pools(&tmp.path()),
    /// #     ..NodeConfig::default()
    /// # };
    /// let node = Node::start(config).await.expect("node");
    /// let observer = node.io_observer();
    /// assert!(observer.snapshot(0).is_some());
    /// node.shutdown().await.expect("shutdown");
    /// # }
    /// ```
    pub fn io_observer(&self) -> Arc<oceanfs_storage::io::IoObserver> {
        self.storage.io_observer.clone()
    }

    /// Returns whether the node is unavailable (the **metadata** pool
    /// is Dead — ADR-0029 §D3): it serves nothing. Derived from the pool
    /// registry — the single source of truth both the read and write
    /// coordinators' gates consult (`PoolRegistry::node_serves_requests`).
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example() {
    /// use oceanfs_core::NodeConfig;
    /// use oceanfs_node::Node;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # fn storage_pools(tmp: &std::path::Path) -> oceanfs_core::StorageConfig {
    /// #     fn pool(name: &str, role: oceanfs_core::PoolRole, root: std::path::PathBuf) -> oceanfs_core::StoragePoolConfig {
    /// #         oceanfs_core::StoragePoolConfig {
    /// #             name: name.into(),
    /// #             role,
    /// #             root,
    /// #             weight: None,
    /// #             tech: Default::default(),
    /// #             health: Default::default(),
    /// #         }
    /// #     }
    /// #     oceanfs_core::StorageConfig {
    /// #         pools: vec![
    /// #             pool("data-0", oceanfs_core::PoolRole::Data, tmp.join("pool-data")),
    /// #             pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.join("pool-wal")),
    /// #             pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.join("pool-meta")),
    /// #             pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.join("pool-hints")),
    /// #         ],
    /// #         missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    /// #     }
    /// # }
    /// # let config = NodeConfig {
    /// #     data_dir: tmp.path().join("data"),
    /// #     listen_addr: "127.0.0.1:0".into(),
    /// #     grpc_listen_addr: "127.0.0.1:0".into(),
    /// #     membership_listen_addr: "127.0.0.1:0".into(),
    /// #     storage: storage_pools(&tmp.path()),
    /// #     ..NodeConfig::default()
    /// # };
    /// let node = Node::start(config).await.expect("node");
    /// assert!(!node.node_unavailable());
    /// node.shutdown().await.expect("shutdown");
    /// # }
    /// ```
    pub fn node_unavailable(&self) -> bool {
        !self.storage.registry.node_serves_requests()
    }

    /// Returns the seal-time segment replicator
    /// (sealed-segment-replication) — the pipeline that pushes sealed
    /// segments to their ring replicas. Exposed for tests/observability.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example() {
    /// use oceanfs_core::NodeConfig;
    /// use oceanfs_node::Node;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # fn storage_pools(tmp: &std::path::Path) -> oceanfs_core::StorageConfig {
    /// #     fn pool(name: &str, role: oceanfs_core::PoolRole, root: std::path::PathBuf) -> oceanfs_core::StoragePoolConfig {
    /// #         oceanfs_core::StoragePoolConfig {
    /// #             name: name.into(),
    /// #             role,
    /// #             root,
    /// #             weight: None,
    /// #             tech: Default::default(),
    /// #             health: Default::default(),
    /// #         }
    /// #     }
    /// #     oceanfs_core::StorageConfig {
    /// #         pools: vec![
    /// #             pool("data-0", oceanfs_core::PoolRole::Data, tmp.join("pool-data")),
    /// #             pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.join("pool-wal")),
    /// #             pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.join("pool-meta")),
    /// #             pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.join("pool-hints")),
    /// #         ],
    /// #         missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    /// #     }
    /// # }
    /// # let config = NodeConfig {
    /// #     data_dir: tmp.path().join("data"),
    /// #     listen_addr: "127.0.0.1:0".into(),
    /// #     grpc_listen_addr: "127.0.0.1:0".into(),
    /// #     membership_listen_addr: "127.0.0.1:0".into(),
    /// #     storage: storage_pools(&tmp.path()),
    /// #     ..NodeConfig::default()
    /// # };
    /// let node = Node::start(config).await.expect("node");
    /// // Single-node ring: nothing to replicate, but the pipeline exists.
    /// node.shutdown().await.expect("shutdown");
    /// # }
    /// ```
    pub fn segment_replicator(&self) -> Arc<crate::segment_replicator::SegmentReplicator> {
        self.storage.segment_replicator.clone()
    }

    /// Returns the compaction-remap alias map (g3 `loss-announcement`
    /// Option A) — consulted by the append handler to translate late
    /// chunk refs and recorded by the healing service's remap handler.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example() {
    /// use oceanfs_core::NodeConfig;
    /// use oceanfs_node::Node;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # fn storage_pools(tmp: &std::path::Path) -> oceanfs_core::StorageConfig {
    /// #     fn pool(name: &str, role: oceanfs_core::PoolRole, root: std::path::PathBuf) -> oceanfs_core::StoragePoolConfig {
    /// #         oceanfs_core::StoragePoolConfig {
    /// #             name: name.into(),
    /// #             role,
    /// #             root,
    /// #             weight: None,
    /// #             tech: Default::default(),
    /// #             health: Default::default(),
    /// #         }
    /// #     }
    /// #     oceanfs_core::StorageConfig {
    /// #         pools: vec![
    /// #             pool("data-0", oceanfs_core::PoolRole::Data, tmp.join("pool-data")),
    /// #             pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.join("pool-wal")),
    /// #             pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.join("pool-meta")),
    /// #             pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.join("pool-hints")),
    /// #         ],
    /// #         missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    /// #     }
    /// # }
    /// # let config = NodeConfig {
    /// #     data_dir: tmp.path().join("data"),
    /// #     listen_addr: "127.0.0.1:0".into(),
    /// #     grpc_listen_addr: "127.0.0.1:0".into(),
    /// #     membership_listen_addr: "127.0.0.1:0".into(),
    /// #     storage: storage_pools(&tmp.path()),
    /// #     ..NodeConfig::default()
    /// # };
    /// let node = Node::start(config).await.expect("node");
    /// assert!(node.remap_alias().is_empty());
    /// node.shutdown().await.expect("shutdown");
    /// # }
    /// ```
    pub fn remap_alias(&self) -> Arc<oceanfs_core::SegmentRemapAlias> {
        self.storage.remap_alias.clone()
    }

    /// Returns the periodic reconciliation loop (g4 `reconciliation` —
    /// the ADR-0029 §D4 pull safety net) — held so tests can observe its
    /// pending-queue depth and holder index.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example() {
    /// use oceanfs_core::NodeConfig;
    /// use oceanfs_node::Node;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # fn storage_pools(tmp: &std::path::Path) -> oceanfs_core::StorageConfig {
    /// #     fn pool(name: &str, role: oceanfs_core::PoolRole, root: std::path::PathBuf) -> oceanfs_core::StoragePoolConfig {
    /// #         oceanfs_core::StoragePoolConfig {
    /// #             name: name.into(),
    /// #             role,
    /// #             root,
    /// #             weight: None,
    /// #             tech: Default::default(),
    /// #             health: Default::default(),
    /// #         }
    /// #     }
    /// #     oceanfs_core::StorageConfig {
    /// #         pools: vec![
    /// #             pool("data-0", oceanfs_core::PoolRole::Data, tmp.join("pool-data")),
    /// #             pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.join("pool-wal")),
    /// #             pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.join("pool-meta")),
    /// #             pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.join("pool-hints")),
    /// #         ],
    /// #         missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    /// #     }
    /// # }
    /// # let config = NodeConfig {
    /// #     data_dir: tmp.path().join("data"),
    /// #     listen_addr: "127.0.0.1:0".into(),
    /// #     grpc_listen_addr: "127.0.0.1:0".into(),
    /// #     membership_listen_addr: "127.0.0.1:0".into(),
    /// #     storage: storage_pools(&tmp.path()),
    /// #     ..NodeConfig::default()
    /// # };
    /// let node = Node::start(config).await.expect("node");
    /// assert_eq!(node.reconciliation().pending_len(), 0);
    /// node.shutdown().await.expect("shutdown");
    /// # }
    /// ```
    pub fn reconciliation(&self) -> Arc<oceanfs_durability::ReconciliationLoop> {
        self.durability.reconciliation.clone()
    }

    /// Removes one parked re-replication repair from the dispatcher's
    /// awaiting-target set (g5 observability / tests). Returns `None`
    /// when nothing is parked.
    pub fn try_recv_repair(&self) -> Option<RepairRequest> {
        self.durability.repair_dispatcher.parked_remove_one()
    }

    /// Returns the number of pending re-replication repair requests
    /// (g3/g4 observability — the dispatcher's awaiting-target set).
    pub fn pending_repairs(&self) -> usize {
        self.durability.repair_dispatcher.pending_len()
    }

    /// Returns the re-replication worker (g5, ADR-0030) — the
    /// acquiring-side executor (held for tests/observability).
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example() {
    /// use oceanfs_core::NodeConfig;
    /// use oceanfs_node::Node;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # fn storage_pools(tmp: &std::path::Path) -> oceanfs_core::StorageConfig {
    /// #     fn pool(name: &str, role: oceanfs_core::PoolRole, root: std::path::PathBuf) -> oceanfs_core::StoragePoolConfig {
    /// #         oceanfs_core::StoragePoolConfig {
    /// #             name: name.into(),
    /// #             role,
    /// #             root,
    /// #             weight: None,
    /// #             tech: Default::default(),
    /// #             health: Default::default(),
    /// #         }
    /// #     }
    /// #     oceanfs_core::StorageConfig {
    /// #         pools: vec![
    /// #             pool("data-0", oceanfs_core::PoolRole::Data, tmp.join("pool-data")),
    /// #             pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.join("pool-wal")),
    /// #             pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.join("pool-meta")),
    /// #             pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.join("pool-hints")),
    /// #         ],
    /// #         missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    /// #     }
    /// # }
    /// # let config = NodeConfig {
    /// #     data_dir: tmp.path().join("data"),
    /// #     listen_addr: "127.0.0.1:0".into(),
    /// #     grpc_listen_addr: "127.0.0.1:0".into(),
    /// #     membership_listen_addr: "127.0.0.1:0".into(),
    /// #     storage: storage_pools(&tmp.path()),
    /// #     ..NodeConfig::default()
    /// # };
    /// let node = Node::start(config).await.expect("node");
    /// let worker = node.rep_worker();
    /// node.shutdown().await.expect("shutdown");
    /// # }
    /// ```
    pub fn rep_worker(&self) -> Arc<oceanfs_durability::ReRepWorker> {
        self.durability.rep_worker.clone()
    }

    /// Returns the `storage_locations` holder set for a segment on THIS
    /// node (g5 observability / tests — the re-replication convergence
    /// check: the acquiring node's registry entry must list itself).
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example() {
    /// use oceanfs_core::{NodeConfig, SegmentId};
    /// use oceanfs_node::Node;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # fn storage_pools(tmp: &std::path::Path) -> oceanfs_core::StorageConfig {
    /// #     fn pool(name: &str, role: oceanfs_core::PoolRole, root: std::path::PathBuf) -> oceanfs_core::StoragePoolConfig {
    /// #         oceanfs_core::StoragePoolConfig {
    /// #             name: name.into(),
    /// #             role,
    /// #             root,
    /// #             weight: None,
    /// #             tech: Default::default(),
    /// #             health: Default::default(),
    /// #         }
    /// #     }
    /// #     oceanfs_core::StorageConfig {
    /// #         pools: vec![
    /// #             pool("data-0", oceanfs_core::PoolRole::Data, tmp.join("pool-data")),
    /// #             pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.join("pool-wal")),
    /// #             pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.join("pool-meta")),
    /// #             pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.join("pool-hints")),
    /// #         ],
    /// #         missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    /// #     }
    /// # }
    /// # let config = NodeConfig {
    /// #     data_dir: tmp.path().join("data"),
    /// #     listen_addr: "127.0.0.1:0".into(),
    /// #     grpc_listen_addr: "127.0.0.1:0".into(),
    /// #     membership_listen_addr: "127.0.0.1:0".into(),
    /// #     storage: storage_pools(&tmp.path()),
    /// #     ..NodeConfig::default()
    /// # };
    /// let node = Node::start(config).await.expect("node");
    /// // A segment this node does not hold → empty.
    /// assert!(node.segment_locations(&SegmentId::new()).is_none());
    /// node.shutdown().await.expect("shutdown");
    /// # }
    /// ```
    pub fn segment_locations(&self, segment_id: &SegmentId) -> Option<Vec<NodeId>> {
        self.storage
            .lifecycle
            .registry()
            .get(*segment_id)
            .map(|entry| entry.metadata.storage_locations.to_vec())
    }

    /// Returns this node's current `NodeManifest` (ADR-0029 D2), as
    /// gossiped to peers — `None` before the boot-time declaration.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example() {
    /// use oceanfs_core::NodeConfig;
    /// use oceanfs_node::Node;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # fn storage_pools(tmp: &std::path::Path) -> oceanfs_core::StorageConfig {
    /// #     fn pool(name: &str, role: oceanfs_core::PoolRole, root: std::path::PathBuf) -> oceanfs_core::StoragePoolConfig {
    /// #         oceanfs_core::StoragePoolConfig {
    /// #             name: name.into(),
    /// #             role,
    /// #             root,
    /// #             weight: None,
    /// #             tech: Default::default(),
    /// #             health: Default::default(),
    /// #         }
    /// #     }
    /// #     oceanfs_core::StorageConfig {
    /// #         pools: vec![
    /// #             pool("data-0", oceanfs_core::PoolRole::Data, tmp.join("pool-data")),
    /// #             pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.join("pool-wal")),
    /// #             pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.join("pool-meta")),
    /// #             pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.join("pool-hints")),
    /// #         ],
    /// #         missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    /// #     }
    /// # }
    /// # let config = NodeConfig {
    /// #     data_dir: tmp.path().join("data"),
    /// #     listen_addr: "127.0.0.1:0".into(),
    /// #     grpc_listen_addr: "127.0.0.1:0".into(),
    /// #     membership_listen_addr: "127.0.0.1:0".into(),
    /// #     storage: storage_pools(&tmp.path()),
    /// #     ..NodeConfig::default()
    /// # };
    /// let node = Node::start(config).await.expect("node");
    /// // The boot-time manifest declares the node's pools.
    /// assert!(node.self_manifest().is_some());
    /// node.shutdown().await.expect("shutdown");
    /// # }
    /// ```
    pub fn self_manifest(&self) -> Option<oceanfs_membership::manifest::NodeManifest> {
        self.membership.manifest_of(&self.node_id())
    }

    /// This node's id (from the config it booted with).
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example() {
    /// use oceanfs_core::NodeConfig;
    /// use oceanfs_node::Node;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # fn storage_pools(tmp: &std::path::Path) -> oceanfs_core::StorageConfig {
    /// #     fn pool(name: &str, role: oceanfs_core::PoolRole, root: std::path::PathBuf) -> oceanfs_core::StoragePoolConfig {
    /// #         oceanfs_core::StoragePoolConfig {
    /// #             name: name.into(),
    /// #             role,
    /// #             root,
    /// #             weight: None,
    /// #             tech: Default::default(),
    /// #             health: Default::default(),
    /// #         }
    /// #     }
    /// #     oceanfs_core::StorageConfig {
    /// #         pools: vec![
    /// #             pool("data-0", oceanfs_core::PoolRole::Data, tmp.join("pool-data")),
    /// #             pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.join("pool-wal")),
    /// #             pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.join("pool-meta")),
    /// #             pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.join("pool-hints")),
    /// #         ],
    /// #         missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
    /// #     }
    /// # }
    /// # let config = NodeConfig {
    /// #     data_dir: tmp.path().join("data"),
    /// #     listen_addr: "127.0.0.1:0".into(),
    /// #     grpc_listen_addr: "127.0.0.1:0".into(),
    /// #     membership_listen_addr: "127.0.0.1:0".into(),
    /// #     storage: storage_pools(&tmp.path()),
    /// #     ..NodeConfig::default()
    /// # };
    /// let node = Node::start(config).await.expect("node");
    /// assert!(!node.node_id().as_str().is_empty());
    /// node.shutdown().await.expect("shutdown");
    /// # }
    /// ```
    pub fn node_id(&self) -> oceanfs_core::NodeId {
        oceanfs_core::NodeId::new(&self.config.node_id)
    }

    /// Returns the acceleration dispatcher probed at startup (ADR-0006).
    ///
    /// Consumers (encoders, decoders, hash accelerators) acquire the
    /// dispatcher to submit work to the best available hardware tier.
    pub fn accel(&self) -> &Arc<oceanfs_accel::AccelDispatcher> {
        &self.storage.accel
    }

    /// Returns the bound HTTP server address.
    pub fn server_addr(&self) -> SocketAddr {
        self.server_addr
    }

    /// Returns the bound gRPC server address.
    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }

    /// Gracefully shuts down the node.
    ///
    /// Sequence: graceful leave → cancel every server + background task
    /// → wait for the task groups under their configured grace (aborting
    /// survivors so no task outlives the node) → stop the prefetch
    /// worker → flush WAL → stop the RocksDB metrics poller → close the
    /// metadata store → drop subsystems.
    ///
    /// Every background task is cancellable and awaited so that after
    /// this returns NO task holds the metadata store (`Arc<DB>`), the
    /// data-plane listeners or the membership-plane listener — an
    /// in-process restart on the same directories (same RocksDB LOCK,
    /// same fixed addresses) is therefore possible.
    ///
    /// # Errors
    ///
    /// Returns an error if any background task panicked or timed out.
    pub async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        info!(node_id = %self.config.node_id, "Shutting down OceanFS node");

        // ---- 1. Graceful leave ----
        // The NodeLeaveHandler (whole-datadir WAL+shard handoff to the ring
        // successor) was deleted in c1 (reviews #34/#35, B1): data is
        // replicated, so a leaving node drains in-flight work and announces
        // LEFT; replica holders detect under-replication and re-replicate
        // (ADR-0030). `leave(None)` runs the drain + announcement.
        let leave_result = self.membership.leave(None).await;
        if let Err(e) = leave_result {
            warn!(error = %e, "graceful leave failed; continuing shutdown");
        }

        // ---- 2. Cancel every server + background task ----
        // Data-plane servers: the tokens are passed to
        // `serve_with_incoming_shutdown`/`with_graceful_shutdown`, so on
        // cancellation the listeners stop accepting and the serve tasks
        // return once in-flight handlers complete (dropping the store
        // clones held by the HTTP router and the gRPC services).
        self.grpc_shutdown.cancel();
        self.http_shutdown.cancel();
        // Membership plane: the gossip/probe serve, routing-cache
        // subscriber, rejoin loop, ready gate and the membership
        // manager's internal tasks all stop on the membership shutdown
        // token (cancelled here so their handles can be awaited).
        self.membership.shutdown();

        // Signal all background loops to stop.
        let mut bg = self.background;
        bg.scheduler_cancel.cancel();
        bg.prefetch_cancel.cancel();
        bg.heal_cancel.cancel();
        bg.delivery_cancel.cancel();
        bg.hint_prune_cancel.cancel();
        bg.health_check_cancel.cancel();
        bg.health_cancel.cancel();
        bg.segment_replicator_cancel.cancel();
        bg.reconciliation_cancel.cancel();
        bg.rep_worker_cancel.cancel();
        bg.rep_dispatcher_cancel.cancel();
        bg.metric_poller_cancel.cancel();
        bg.health_consequences_cancel.cancel();

        // [review][config][high]
        // the shutdown grace period should be configurable, since it's dimensions is the product
        // of the queues sizes, and expected system load.
        // [end]
        // ---- 3. Wait for task groups under their grace ----
        // Grace is config-driven (review #71 — resolved in c5): the main
        // group waits `shutdown_grace_secs`; the transport/best-effort
        // handles (servers, membership plane, prefetch, hint delivery)
        // wait `shutdown_fast_grace_secs`. Every group is drained with an
        // ABORT backstop: a task that exceeds its grace is aborted rather
        // than detached, so no DB-holding or port-binding task can
        // outlive the node (the detach-on-timeout behaviour is what kept
        // the old data-plane gRPC serve task alive past shutdown).
        let grace = Duration::from_secs(self.config.shutdown_grace_secs.max(1));
        let fast = Duration::from_secs(self.config.shutdown_fast_grace_secs.max(1));

        // Housekeeping group (grace).
        let housekeeping = vec![
            bg.durability_scheduler.take(),
            bg.heal.take(),
            bg.hinted_handoff_prune.take(),
            // The segment replicator drains its bounded channel; if it is
            // mid-push the grace bounds the wait (its receiver is dropped
            // by the node drop anyway).
            bg.segment_replicator.take(),
            // g4: the reconciliation loop stops on its token.
            bg.reconciliation.take(),
            // g5: the worker drains its bounded queue; the dispatcher
            // stops its sweep. Both bound the wait via the grace.
            bg.rep_worker.take(),
            bg.rep_dispatcher.take(),
            // g2: the pool health monitor + consequence applier stop on
            // their tokens (drained before the stores close).
            bg.health_monitor.take(),
            bg.health_consequences.take(),
            bg.metric_poller.take(),
        ];
        Self::drain_tasks(housekeeping, grace, "housekeeping").await;

        // Transport group (fast grace): the server tasks + the
        // membership-plane tasks. These hold the DB/ports and MUST
        // finish (or be aborted) before the store close below.
        let transports = vec![
            bg.grpc_server.take(),
            bg.http_server.take(),
            bg.membership_grpc.take(),
            bg.membership_subscriber.take(),
            bg.membership_rejoin.take(),
            bg.ready_gate.take(),
        ];
        Self::drain_tasks(transports, fast, "transport").await;

        // Best-effort handles get the fast grace (they may be None).
        if let Some(pf) = bg.prefetch {
            let _ = tokio::time::timeout(fast, pf).await;
        }
        if let Some(dh) = bg.hinted_handoff_delivery {
            let _ = tokio::time::timeout(fast, dh).await;
        }

        // ---- 4. Stop the prefetch worker ----
        // Cancels the engine's worker + drains in-flight prefetch tasks
        // so their metadata-store clones are released. The engine Arc is
        // still held here (self.prefetch_engine) and drops with `self`
        // at the end of this method.
        if let Some(engine) = &self.prefetch_engine {
            engine.shutdown().await;
        }

        // ---- 5. Flush WAL writer to disk ----
        if let Err(e) = self.storage.wal_writer.sync().await {
            warn!(error = %e, "WAL sync failed during shutdown");
        }

        // ---- 6. Stop the RocksDB metrics poller ----
        // The task exits and drops its Arc<DB>, so the store close below
        // fully releases the RocksDB LOCK (a restarted node on the same
        // dir must be able to reopen it — every task is cancellable).
        self.storage.shutdown_metrics_task().await;

        // ---- 7. Close metadata store (flush RocksDB) ----
        if let Err(e) = self.storage.metadata_store.close() {
            warn!(error = %e, "metadata store close failed during shutdown");
        }

        // ---- 8. Drop subsystems ----
        // `self` (config, background handles, prefetch engine Arc,
        // storage, durability, membership) drops here, releasing the
        // last Arc<DB> holder (the storage module) AFTER every task that
        // held a clone has exited above.

        info!(node_id = %self.config.node_id, "OceanFS node shut down");
        Ok(())
    }

    /// Awaits a group of optional task handles under `grace`, aborting
    /// any task that has not finished when the grace elapses.
    ///
    /// Abort (rather than detach) is deliberate: a background task that
    /// survives shutdown can hold the RocksDB `Arc<DB>` (blocking a
    /// same-dir reopen) or a fixed TCP listener (blocking a same-address
    /// restart). Completed tasks are unaffected (abort is a no-op on a
    /// finished task).
    async fn drain_tasks(handles: Vec<Option<JoinHandle<()>>>, grace: Duration, group: &str) {
        // Capture abort handles up front — the JoinHandles themselves
        // are moved into the wait futures below.
        let aborts: Vec<tokio::task::AbortHandle> =
            handles.iter().flatten().map(|h| h.abort_handle()).collect();
        if aborts.is_empty() {
            return;
        }
        let mut waiter = tokio::task::JoinSet::new();
        for handle in handles.into_iter().flatten() {
            waiter.spawn(async move {
                let _ = handle.await;
            });
        }
        let timed_out =
            tokio::time::timeout(grace, async { while waiter.join_next().await.is_some() {} })
                .await
                .is_err();
        if timed_out {
            warn!(
                task_group = group,
                grace_secs = grace.as_secs(),
                "task group exceeded its shutdown grace; aborting survivors"
            );
            for abort in &aborts {
                abort.abort();
            }
            // Bounded re-reap: an aborted task is only stopped once the
            // runtime polls its cancellation, so join the wrappers (which
            // await the original JoinHandles) for a short bound. This
            // ensures the aborted tasks actually drop their Arc<DB> /
            // listener before the group returns on the abort path.
            let _ = tokio::time::timeout(Duration::from_millis(250), async {
                while waiter.join_next().await.is_some() {}
            })
            .await;
        }
    }
    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Validates and normalizes node configuration.
    fn validate_config(config: NodeConfig) -> Result<NodeConfig, Box<dyn std::error::Error>> {
        let mut cfg = config;

        std::fs::create_dir_all(&cfg.data_dir)
            .map_err(|e| format!("cannot create data directory {:?}: {e}", cfg.data_dir))?;

        if cfg.node_id.is_empty() {
            cfg.node_id = "oceanfs-node".to_string();
        }

        // Validate auth config at startup rather than at first request time.
        // If S3 auth is enabled but the key file is missing or invalid,
        // log a warning but don't block startup — the system will reject
        // all authenticated requests until valid keys are provided.
        if cfg.s3_auth_enabled {
            let keys_path = cfg.data_dir.join("access_keys.toml");
            if keys_path.exists() {
                match std::fs::read_to_string(&keys_path) {
                    Ok(content) => {
                        if let Err(e) = toml::from_str::<toml::Value>(&content) {
                            warn!(
                                path = %keys_path.display(),
                                error = %e,
                                "access_keys.toml is invalid TOML — S3 auth will reject all requests"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            path = %keys_path.display(),
                            error = %e,
                            "cannot read access_keys.toml — S3 auth will reject all requests"
                        );
                    }
                }
            } else {
                warn!("s3_auth_enabled but no access_keys.toml found at {}", keys_path.display());
            }
        }

        Ok(cfg)
    }
}

// ---------------------------------------------------------------------------
// Process metrics helpers
// ---------------------------------------------------------------------------

/// Reads the resident memory size from `/proc/self/statm`.
///
/// Returns the resident set size in pages multiplied by the page size
/// (typically 4096), yielding total resident memory in bytes.
///
/// # Errors
///
/// Returns an error if `/proc/self/statm` cannot be read or parsed.
pub(crate) fn read_process_memory_bytes() -> Result<u64, std::io::Error> {
    let statm = std::fs::read_to_string("/proc/self/statm")?;
    // Format: size resident shared text lib data dt (in pages)
    let parts: Vec<&str> = statm.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected statm format",
        ));
    }
    let resident_pages: u64 =
        parts[1].parse().map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let page_size = 4096u64; // Linux default page size
    Ok(resident_pages * page_size)
}

/// Counts the number of open file descriptors from `/proc/self/fd`.
///
/// # Errors
///
/// Returns an error if the directory cannot be read.
pub(crate) fn read_process_open_fds() -> Result<u64, std::io::Error> {
    let entries = std::fs::read_dir("/proc/self/fd")?;
    Ok(entries.count() as u64)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{net::TcpStream, time::Duration};

    use oceanfs_storage_api::MetadataStore;
    use tempfile::TempDir;

    use super::*;

    /// A role-complete `[storage]` topology for tests: one data, one wal,
    /// one metadata, one hints pool on sibling tempdir roots (ADR-0031
    /// mandatory roles; the data pool is id 0).
    fn test_storage_pools(tmp: &TempDir) -> oceanfs_core::StorageConfig {
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
        oceanfs_core::StorageConfig {
            pools: vec![
                pool("data-0", oceanfs_core::PoolRole::Data, tmp.path().join("pool-data")),
                pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.path().join("pool-wal")),
                pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.path().join("pool-meta")),
                pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.path().join("pool-hints")),
            ],
            missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
        }
    }

    fn test_config(tmp: &TempDir) -> NodeConfig {
        NodeConfig {
            // Pool roots are siblings under the tempdir (see
            // `test_storage_pools`), so `data_dir` is a subdir.
            data_dir: tmp.path().join("data"),
            listen_addr: "127.0.0.1:0".into(),
            grpc_listen_addr: "127.0.0.1:0".into(),
            // Ephemeral membership plane port — the default 0.0.0.0:9002
            // conflicts across parallel test nodes (ADR-0028 D1).
            membership_listen_addr: "127.0.0.1:0".into(),
            // ADR-0031: pools are mandatory in tests too.
            storage: test_storage_pools(tmp),
            // The event WAL lives under the temp data dir (the default
            // /var/lib/oceanfs/event-wal is not writable in tests).
            event_wal: oceanfs_core::EventWalConfig {
                event_wal_dir: tmp.path().join("event-wal"),
                ..Default::default()
            },
            ..NodeConfig::default()
        }
    }

    #[test]
    fn validate_config_creates_data_directory() {
        let tmp = TempDir::new().expect("tempdir");
        let config = test_config(&tmp);
        let validated = Node::validate_config(config).expect("validate");
        assert!(validated.data_dir.exists(), "data_dir should exist after validation");
    }

    #[test]
    fn validate_config_defaults_empty_node_id() {
        let tmp = TempDir::new().expect("tempdir");
        let mut config = test_config(&tmp);
        config.node_id = String::new();
        let validated = Node::validate_config(config).expect("validate");
        assert_eq!(validated.node_id, "oceanfs-node");
    }

    #[test]
    fn node_config_uses_segment_size_default() {
        let segment_size = SegmentSizeConfig::default();
        assert_eq!(segment_size.inline_threshold_bytes, 4096);
        assert_eq!(segment_size.small_threshold_bytes, 262144);
        assert_eq!(segment_size.default_target_size, 4 * 1024 * 1024);
    }

    #[tokio::test]
    async fn node_start_with_valid_config_succeeds() {
        let tmp = TempDir::new().expect("tempdir");
        let config = test_config(&tmp);
        let result = Node::start(config).await;
        assert!(
            result.is_ok(),
            "Node::start should succeed with valid config: {}",
            result.as_ref().err().map(|e| e.to_string()).unwrap_or_default()
        );
        // Clean shutdown.
        result.unwrap().shutdown().await.expect("shutdown");
    }

    /// Every background task is cancellable and awaited during shutdown,
    /// so a second node can be started IN-PROCESS on the same
    /// directories: the RocksDB LOCK is released (no task still holds an
    /// `Arc<DB>`) and no listener task survives. This is the prerequisite
    /// for the g7 boot-variant e2e (out-of-band wal replacement + restart
    /// in one test process).
    #[tokio::test]
    async fn node_in_process_restart_same_dirs_succeeds() {
        let tmp = TempDir::new().expect("tempdir");
        let first = Node::start(test_config(&tmp)).await.expect("first boot");
        first.shutdown().await.expect("first shutdown");

        // Give the OS a moment to release the ephemeral listeners.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let second = Node::start(test_config(&tmp)).await.unwrap_or_else(|e| {
            panic!("second in-process boot on the same dirs must succeed: {e}")
        });
        second.shutdown().await.expect("second shutdown");
    }

    /// ADR-0031 (f1): a node whose config has no `[storage.pools]` is
    /// refused at boot with the role-listing error.
    #[tokio::test]
    async fn node_start_without_pools_refuses_with_role_error() {
        let tmp = TempDir::new().expect("tempdir");
        let mut config = test_config(&tmp);
        config.storage = oceanfs_core::StorageConfig::default();
        let result = Node::start(config).await;
        assert!(result.is_err(), "boot without pools must fail");
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("'data'"), "message: {err_msg}");
        assert!(err_msg.contains("'wal'"), "message: {err_msg}");
        assert!(err_msg.contains("'metadata'"), "message: {err_msg}");
        assert!(err_msg.contains("'hints'"), "message: {err_msg}");
        assert!(err_msg.contains("mandatory"), "message: {err_msg}");
    }

    #[tokio::test]
    async fn node_start_with_invalid_addr_errors() {
        let tmp = TempDir::new().expect("tempdir");
        let config_invalid = NodeConfig {
            listen_addr: "not-a-valid-socket-addr".into(),
            grpc_listen_addr: "127.0.0.1:0".into(),
            ..test_config(&tmp)
        };
        let result = Node::start(config_invalid).await;
        assert!(result.is_err(), "invalid listen_addr should error");
        let err_msg = result.err().unwrap().to_string();
        assert!(
            err_msg.contains("HTTP server") || err_msg.contains("bind"),
            "error should mention bind failure: {err_msg}"
        );
    }

    #[tokio::test]
    async fn node_start_with_invalid_grpc_addr_errors() {
        // B2 (review #64): an unparseable `grpc_listen_addr` must halt
        // startup with an explicit error — no silent fallback to a
        // default self address.
        let tmp = TempDir::new().expect("tempdir");
        let config_invalid = NodeConfig {
            listen_addr: "127.0.0.1:0".into(),
            grpc_listen_addr: "not-a-valid-socket-addr".into(),
            ..test_config(&tmp)
        };
        let result = Node::start(config_invalid).await;
        assert!(result.is_err(), "invalid grpc_listen_addr should error");
        let err_msg = result.err().unwrap().to_string();
        assert!(
            err_msg.contains("grpc_listen_addr"),
            "error should mention the offending config key: {err_msg}"
        );
    }

    #[test]
    fn cluster_ready_gate_opens_at_configured_minimum_quorum() {
        // B6 (review #66/#69): the gate threshold derives from
        // `cluster_min_quorum_nodes`, not the hard-coded `ring >= 2`.
        // Default (2): a 2-node ring opens the gate, a 1-node ring
        // does not.
        assert!(cluster_ready_gate_opens(2, 2, false));
        assert!(!cluster_ready_gate_opens(1, 2, false));
        // A deployment requiring 3 nodes stays gated at 2 nodes — the
        // historical code would have opened here.
        assert!(!cluster_ready_gate_opens(2, 3, false));
        assert!(cluster_ready_gate_opens(3, 3, false));
        // The deadline bound still opens the gate regardless of ring
        // size (cluster_ready_timeout_sec semantics preserved).
        assert!(cluster_ready_gate_opens(1, 3, true));
        // A min-quorum <= 1 opens as soon as the node has a ring view.
        assert!(cluster_ready_gate_opens(1, 1, false));
    }

    #[tokio::test]
    async fn node_shutdown_releases_ports() {
        let tmp = TempDir::new().expect("tempdir");
        let config = test_config(&tmp);
        let node = Node::start(config).await.expect("start");
        let http_addr = node.server_addr();
        let grpc_addr_val = node.grpc_addr();

        // Shut down.
        node.shutdown().await.expect("shutdown");

        // Give the OS time to release ports.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify HTTP port is released.
        let http_reconnect = TcpStream::connect_timeout(&http_addr, Duration::from_secs(1));
        assert!(http_reconnect.is_err(), "HTTP port {http_addr} should be released after shutdown");

        // Verify gRPC port is released.
        let grpc_reconnect = TcpStream::connect_timeout(&grpc_addr_val, Duration::from_secs(1));
        assert!(
            grpc_reconnect.is_err(),
            "gRPC port {grpc_addr_val} should be released after shutdown"
        );
    }

    #[tokio::test]
    async fn node_config_uses_segment_size_config() {
        let tmp = TempDir::new().expect("tempdir");
        let config = test_config(&tmp);
        let node = Node::start(config).await.expect("start");
        // Verify the acceleration dispatcher is available and probed (ADR-0006).
        let _accel = node.accel();
        // Verify the bound HTTP address is non-zero.
        assert!(node.server_addr().port() > 0, "HTTP server port should be non-zero");
        node.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn background_tasks_spawns_all_handles() {
        // c5: every background loop is spawned through the module-owned
        // spawn methods (bundled by the background module); a started
        // node must hold a live handle for each loop, and shutdown must
        // drain them within the configured grace.
        let tmp = TempDir::new().expect("tempdir");
        let config = test_config(&tmp);
        let node = Node::start(config).await.expect("start");
        let handles = [
            node.background.durability_scheduler.as_ref(),
            node.background.heal.as_ref(),
            node.background.hinted_handoff_prune.as_ref(),
            node.background.hinted_handoff_delivery.as_ref(),
            node.background.health_monitor.as_ref(),
            node.background.health_consequences.as_ref(),
            node.background.segment_replicator.as_ref(),
            node.background.reconciliation.as_ref(),
            node.background.rep_worker.as_ref(),
            node.background.rep_dispatcher.as_ref(),
            node.background.metric_poller.as_ref(),
            node.background.grpc_server.as_ref(),
        ];
        for h in handles {
            let h = h.expect("loop handle present after start");
            assert!(!h.is_finished(), "loop must be running before shutdown");
        }
        node.shutdown().await.expect("shutdown");
    }

    #[test]
    fn prefetch_store_adapter_get_object_metadata_nonexistent() {
        let tmp = TempDir::new().expect("tempdir");
        let metadata_config =
            MetadataConfig { data_dir: tmp.path().join("metadata"), ..Default::default() };
        let store = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&metadata_config)
                .expect("open metadata store"),
        );
        let adapter = PrefetchStoreAdapter { store };
        let bucket = BucketId::new("test-bucket");
        let key = ObjectKey::new("nonexistent-key");
        let result = adapter.get_object_metadata(&bucket, &key).expect("get_object_metadata");
        assert!(result.is_none(), "nonexistent key should return None");
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn process_memory_bytes_returns_non_zero() {
        let mem = super::read_process_memory_bytes().expect("read memory");
        assert!(mem > 0, "resident memory should be > 0");
    }

    #[test]
    fn process_open_fds_returns_non_zero() {
        let fds = super::read_process_open_fds().expect("read fds");
        assert!(fds > 0, "open fds should be > 0");
    }

    // --- property_as_u64 tests ---

    #[test]
    fn property_as_u64_parses_rocksdb_integer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
            data_dir: dir.path().join("meta"),
            block_cache_size: 1024,
            memtable_size: 1024,
            ..Default::default()
        })
        .expect("open metadata store");

        // Estimate number of keys should parse to a valid u64.
        let val = super::property_as_u64(&store, "rocksdb.estimate-num-keys");
        assert!(val.is_some(), "estimate-num-keys should return a value");
        // New store should have few keys.
        assert!(val.unwrap() <= 10_000);
    }

    #[test]
    fn property_as_u64_unknown_property_returns_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
            data_dir: dir.path().join("meta"),
            block_cache_size: 1024,
            memtable_size: 1024,
            ..Default::default()
        })
        .expect("open metadata store");

        assert_eq!(super::property_as_u64(&store, "rocksdb.nonexistent"), None);
    }

    // ── Hinted Handoff Watcher (4.4) ──────────────────────────────

    /// Verifies that the membership event watcher correctly processes
    /// ALIVE events and calls `deliver_pending` on the hinted handoff.
    ///
    /// Uses an mpsc channel to confirm the watcher saw the event and
    /// attempted delivery.
    #[tokio::test]
    async fn hinted_handoff_watcher_delivers_on_alive_event() {
        use std::{net::SocketAddr, sync::Arc, time::Duration};

        use oceanfs_core::{Hlc, Incarnation, NodeId, NodeState, SegmentId};
        use oceanfs_durability::{HintRecord, HintedHandoff};
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;
        use tokio::sync::{broadcast, mpsc};
        use tokio_util::sync::CancellationToken;

        let addr: SocketAddr = "127.0.0.1:9100".parse().unwrap();
        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("watcher-test"),
            addr,
            addr,
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
        let handoff =
            Arc::new(HintedHandoff::new_with_pool(pool).with_membership(membership.clone()));

        // Store a hint for a returning node.
        let target = NodeId::new("returning-node");

        // Register the node as SUSPECT first so transitioning to ALIVE
        // triggers a state-change event.
        membership.upsert_node(
            target.clone(),
            NodeState::Suspect,
            Incarnation::new(1),
            Some("127.0.0.1:9200".parse().unwrap()),
        );

        let hint = HintRecord {
            intended_for: target.clone(),
            segment_id: SegmentId::new(),
            offset: 0,
            length: 42,
            timestamp: Hlc::zero(),
            data: vec![1, 2, 3].into(),
            stored_at_secs: 0,
        };
        handoff.handoff(target.clone(), hint).await.unwrap();
        assert_eq!(handoff.pending_count(&target), 1);

        // Channel to verify the watcher processes events.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let hh = handoff.clone();
        let token = cancel.clone();

        // Spawn the watcher — same logic as production.
        let mut events = membership.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    ev = events.recv() => {
                        match ev {
                            Ok(ev) if ev.new_state == NodeState::Alive &&
                                      ev.old_state != NodeState::Alive => {
                                let _ = tx.send(ev.node_id.clone());
                                let _ = hh.deliver_pending(ev.node_id).await;
                            }
                            Ok(_) => {}
                            Err(broadcast::error::RecvError::Closed) => break,
                            Err(broadcast::error::RecvError::Lagged(_)) => {}
                        }
                    }
                }
            }
        });

        // Mark the target node as returning to the cluster.
        membership.upsert_node(
            target.clone(),
            NodeState::Alive,
            Incarnation::new(2),
            Some("127.0.0.1:9200".parse().unwrap()),
        );

        // Wait for the watcher to process the event (with timeout).
        let received = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(received.is_ok(), "watcher should process ALIVE event within 2 seconds");
        assert_eq!(received.unwrap(), Some(target.clone()));

        // Delivery fails without a real gRPC server, but the watcher
        // should have attempted it (hints remain pending).
        assert_eq!(
            handoff.pending_count(&target),
            1,
            "hints retained after failed delivery attempt"
        );

        cancel.cancel();
    }

    /// Verifies the watcher ignores non-ALIVE state changes.
    #[tokio::test]
    async fn hinted_handoff_watcher_ignores_non_alive_events() {
        use std::{net::SocketAddr, sync::Arc};

        use oceanfs_core::{Incarnation, NodeId, NodeState};
        use oceanfs_membership::Membership;
        use tokio::sync::mpsc;
        use tokio_util::sync::CancellationToken;

        let addr: SocketAddr = "127.0.0.1:9100".parse().unwrap();
        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("ignorer"),
            addr,
            addr,
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        // hinted handoff not needed for this test — we verify watcher
        // event discrimination via the mpsc channel alone.

        let target = NodeId::new("suspect-node");
        // Pre-register the node in membership so state changes are detected.
        membership.upsert_node(
            target.clone(),
            NodeState::Alive,
            Incarnation::new(1),
            Some("127.0.0.1:9300".parse().unwrap()),
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let mut events = membership.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    ev = events.recv() => {
                        match ev {
                            Ok(ev) if ev.new_state == NodeState::Alive &&
                                      ev.old_state != NodeState::Alive => {
                                let _ = tx.send(ev.node_id.clone());
                            }
                            Ok(_) => {
                                // Non-ALIVE or same-state: send a dummy to confirm
                                // we saw it but didn't trigger delivery.
                                let _ = tx.send(NodeId::new("non-alive-seen"));
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        // Transition node to SUSPECT (not ALIVE).
        membership.upsert_node(
            target.clone(),
            NodeState::Suspect,
            Incarnation::new(2),
            Some("127.0.0.1:9300".parse().unwrap()),
        );

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv()).await;
        assert!(received.is_ok(), "watcher should process SUSPECT event");
        assert_eq!(
            received.unwrap(),
            Some(NodeId::new("non-alive-seen")),
            "non-ALIVE event should not trigger delivery"
        );

        cancel.cancel();
    }

    /// Verifies that `Membership::leave()` with a handler calls the handler
    /// instead of sleeping. Uses a mock handler that records calls.
    #[tokio::test]
    async fn membership_leave_calls_handler_instead_of_sleeping() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };

        use oceanfs_core::{NodeId, RingConfig};
        use oceanfs_membership::Membership;
        use oceanfs_routing::Ring;

        struct RecordingHandler {
            wal_called: AtomicBool,
            shard_called: AtomicBool,
        }

        #[async_trait::async_trait]
        impl oceanfs_membership::GracefulLeaveHandler for RecordingHandler {
            async fn handoff_wal_to(&self, _: &NodeId) -> oceanfs_core::Result<()> {
                self.wal_called.store(true, Ordering::SeqCst);
                Ok(())
            }
            async fn transfer_segment_shards_to(&self, _: &NodeId) -> oceanfs_core::Result<usize> {
                self.shard_called.store(true, Ordering::SeqCst);
                Ok(42)
            }
        }

        let ring = Ring::new(RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("leave-call-test"),
            "127.0.0.1:9400".parse().unwrap(),
            "127.0.0.1:9400".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache.clone(),
        ));

        // Add a successor node so handoff targets a real node.
        let mut write_ring = Ring::new(RingConfig::default());
        write_ring.add_node(NodeId::new("successor"));
        ring_cache.update(write_ring);
        // Register successor in membership for address resolution.
        membership.upsert_node(
            NodeId::new("successor"),
            oceanfs_core::NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            Some("127.0.0.1:9500".parse().unwrap()),
        );

        let handler = RecordingHandler {
            wal_called: AtomicBool::new(false),
            shard_called: AtomicBool::new(false),
        };

        // leave() requires started membership; start background tasks.
        membership.start().unwrap();
        membership.join(oceanfs_core::Incarnation::new(1), &[]).await.unwrap();

        let result = membership.leave(Some(&handler)).await;
        assert!(result.is_ok(), "leave should succeed");

        assert!(handler.wal_called.load(Ordering::SeqCst), "WAL handoff was called");
        assert!(handler.shard_called.load(Ordering::SeqCst), "shard transfer was called");
    }
}
