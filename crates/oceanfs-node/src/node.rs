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

use oceanfs_core::{
    AccelConfig, Incarnation, MetadataConfig, MetricRegistrar, NodeConfig, NodeId, RingConfig,
    RpcConfig, SegmentId, WalConfig,
};
#[cfg(test)]
use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata, SegmentSizeConfig};
/// A re-replication repair request (g5 ReRepWorker input).
pub use oceanfs_durability::healing_service::ReRepRequest as RepairRequest;
use oceanfs_durability::{
    GrpcHintDeliveryClient, HintedHandoff, HintedHandoffConfig, HintedHandoffManager,
};
use oceanfs_network::{apply_opts_to_fd, create_reuseport_listener};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::membership_state::{default_state_path, MembershipStateStore};
#[cfg(test)]
use crate::modules::server::PrefetchStoreAdapter;

// ---------------------------------------------------------------------------
// BackgroundTasks
// ---------------------------------------------------------------------------
/// Whether the cluster-readiness gate opens for the given ring view
/// (B6, review #66/#69).
///
/// The gate opens when the ring holds at least the configured minimum
/// quorum node count (`cluster_min_quorum_nodes`) or when the
/// configured deadline has elapsed (the bound keeps a node whose seeds
/// are unreachable from stalling writes forever). Single-node
/// deployments never consult this — they skip the gate entirely.
fn cluster_ready_gate_opens(
    ring_nodes: usize,
    min_quorum_nodes: u64,
    deadline_elapsed: bool,
) -> bool {
    ring_nodes as u64 >= min_quorum_nodes || deadline_elapsed
}
// [review][architecture][critical]
// we are running a lot of background tasks, each independently managing the following :
// - concurrency
// - scheduling
// - event binding
// this approach doesnt allow use to be able to manage the concurrency of background tasks at a global level.
// i think we need a global task scheduler approach, with a semaphore driven global concurrency for background tasks.
// this trully ensure the tasks cannot hurt the performance beyond a certain defined threshold.
// also, we could integrate a reactor approach for the event driven communication between subsystems. this would simplify a few of them
// i need an honest and torough discussion about this topic, since it is structurally very significant.
// [end]
/// Aggregated join handles and cancellation tokens for background loops.
pub struct BackgroundTasks {
    /// Gossip protocol task handle (Membership drives gossip internally;
    /// this handle waits for cancellation).
    pub(crate) gossip: JoinHandle<()>,
    /// Gossip cancellation token.
    pub(crate) gossip_cancel: CancellationToken,

    /// Garbage collector task.
    pub(crate) gc: JoinHandle<()>,
    /// GC cancellation token.
    pub(crate) gc_cancel: CancellationToken,

    /// Anti-entropy Merkle exchange task.
    pub(crate) anti_entropy: JoinHandle<()>,
    /// Anti-entropy cancellation token.
    pub(crate) ae_cancel: CancellationToken,

    /// Scrub scheduler task.
    pub(crate) scrub: JoinHandle<()>,
    /// Scrub cancellation token.
    pub(crate) scrub_cancel: CancellationToken,

    /// Orphan reaper task.
    pub(crate) orphan_reaper: JoinHandle<()>,
    /// Reaper cancellation token.
    pub(crate) reaper_cancel: CancellationToken,

    /// Prefetch engine background pre-warmer (only if prefetch is enabled).
    pub(crate) prefetch: Option<JoinHandle<()>>,
    /// Prefetch cancellation token.
    pub(crate) prefetch_cancel: CancellationToken,

    /// Failure detector task handle (Membership drives FD internally;
    /// this handle waits for cancellation).
    pub(crate) failure_detector: JoinHandle<()>,
    /// Failure detector cancellation token.
    pub(crate) fd_cancel: CancellationToken,

    /// EC Heal worker task.
    pub(crate) heal: JoinHandle<()>,
    /// Heal worker cancellation token.
    pub(crate) heal_cancel: CancellationToken,

    /// Hinted handoff delivery watcher task.
    pub(crate) hinted_handoff_delivery: Option<JoinHandle<()>>,
    /// Hinted handoff delivery cancellation token.
    pub(crate) delivery_cancel: CancellationToken,

    /// Hinted handoff WAL prune task.
    pub(crate) hinted_handoff_prune: JoinHandle<()>,
    /// Hinted handoff WAL prune cancellation token.
    pub(crate) hint_prune_cancel: CancellationToken,

    /// gRPC server task handle for graceful shutdown.
    pub(crate) grpc_server: Option<JoinHandle<()>>,
    /// gRPC server cancellation token.
    pub(crate) grpc_shutdown: CancellationToken,

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

        // [review][cleanup]
        // we ditched rayon in favor of a unified design around a unique tokio thread executor.
        // if this is never used anywhere, it should be discarded. otherwise, the consumer side should also be refactored out of rayon.
        // [end]
        // Configure the rayon global thread pool for EC encode/decode.
        // Leave 2 cores for tokio's async runtime to avoid CPU oversubscription
        // when both compute (EC) and I/O (gRPC, RocksDB) run concurrently.
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

        info!(
            node_id = %config.node_id,
            listen_addr = %config.listen_addr,
            grpc_addr = %config.grpc_listen_addr,
            "Starting OceanFS node"
        );

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
        let metadata_config =
            MetadataConfig { data_dir: paths.metadata.clone(), ..Default::default() };
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

        // ---- 4. Construct membership ----
        let grpc_addr: SocketAddr = config
            .grpc_listen_addr
            .parse()
            .map_err(|e| format!("invalid grpc_listen_addr: {e}"))?;
        // ADR-0028 D1: the announced membership address is the membership
        // plane's listen address with the data-plane's advertised IP
        // substituted for 0.0.0.0 (the gRPC address is already the
        // reachable IP — the deploy scripts write the node's IP there).
        let membership_addr: SocketAddr = config
            .membership_listen_addr
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 9002)));
        let membership_announce_addr = if membership_addr.ip().is_unspecified() {
            oceanfs_membership::plane::membership_address(
                &config.membership_listen_addr,
                Some(&grpc_addr.ip().to_string()),
            )
        } else {
            membership_addr
        };
        let gossip_config = config.gossip.clone();
        let membership = Arc::new(oceanfs_membership::Membership::new(
            NodeId::new(&config.node_id),
            membership_announce_addr,
            grpc_addr,
            gossip_config,
            ring_cache.clone(),
        ));

        // ---- 4a. Rejoin state (ADR-0022) ----
        // Load the persisted incarnation and fallback seeds so a restart
        // rejoins as the same identity with a bumped incarnation (D1) and
        // can re-contact the cluster when configured seeds are unreachable
        // or empty (D3).
        // [review][config][critical]
        // membership persistante across restart information follow the old one data dir approach.
        // this is incompatible with the pooled data dirs approach.
        // moreover, loosing the data drive means loosing the ability to rejoin at restart. this should not be possible.
        // a safer approach, using a foreign config store for cluster critical informations should be considered instead.
        // [end]
        let membership_state_store =
            MembershipStateStore::new(default_state_path(&config.data_dir));
        let durable_state = membership_state_store.load().map_err(|e| {
            format!(
                "failed to load membership state at {}: {e}",
                default_state_path(&config.data_dir).display()
            )
        })?;

        // Announce with persisted + 1; first boot keeps 1 (spec §13.1).
        let announce_incarnation = durable_state.self_incarnation.map_or(1, |p| p + 1);

        // Write-through the bump BEFORE announcing: if the process dies
        // after announcing but before persisting, the next restart would
        // re-announce the same incarnation and be rejected as stale.
        membership_state_store
            .save_incarnation(announce_incarnation)
            .map_err(|e| format!("failed to persist self incarnation: {e}"))?;
        info!(
            node_id = %config.node_id,
            incarnation = announce_incarnation,
            fallback_seeds = durable_state.fallback_seeds.len(),
            "rejoin state loaded: announcing with bumped incarnation"
        );

        // ---- 4b. Peer-side routing cache (ADR-0029 §D5) ----
        // The per-peer NodeManifest cache consulted as a routing hint by
        // the read/write coordinators (lock-free ArcSwap reads on the
        // hot path; populated from membership events below and seeded
        // with the self manifest at step 15d). Phase A: every manifest
        // is Healthy, so the exclusion filters are observationally
        // neutral — the structure and metrics land for Phase B.
        let manifest_cache = Arc::new(crate::routing_cache::ManifestCache::new());

        // [review][config][high]
        // no rpc config from config is operational, only the default values are used. rpc should be configurable
        // [end]
        // ---- 5. Construct connection pools ----
        let rpc_config = RpcConfig::default();
        let quickack = rpc_config.quickack;
        let busy_poll = rpc_config.busy_poll_us;
        // ADR-0028 D1: the membership plane has its own dedicated pool
        // (per-peer 2, probe-derived timeouts) so probe/gossip latency is
        // never coupled to the data plane's channel semaphore.
        let membership_pool = oceanfs_membership::plane::membership_pool(
            config.gossip.failure_timeout_ms / 3,
            rpc_config.tls_cert_path.clone(),
        );
        let pool = Arc::new(oceanfs_network::ConnectionPool::new(rpc_config));
        membership.set_pool(membership_pool.clone());

        // [review][config][critical]
        // segment tiers sizes should be configurable by the end user too
        // [end]
        // ---- 6. Construct storage subsystem (c1: modules/storage.rs) ----
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

        // ---- 7. Construct durability workers (c2: modules/durability.rs) ----
        // GC/AE/scrub/reaper/heal/reconciliation/re-rep + op timeouts are
        // built by the module against the c1 storage bundle's single shared
        // stores + lifecycle; metrics register through one module call at
        // §12. (Hinted handoff + its manager stay in §11 — c5 territory.)
        let durability = crate::modules::durability::DurabilityModule::build(
            &config,
            &storage,
            membership.clone(),
            pool.clone(),
        )
        .await?;

        // ---- 7b. Start the seal pipeline (storage-side) + 6a/6b
        // recovery — both run BEFORE any server construction. The seal
        // pipeline drains the pools' seal queues (relocated from the
        // write coordinator, c3-Option-A): startup recovery's replayed
        // re-seals complete asynchronously through it and recovery waits
        // on their `.dat` files — recovery must never depend on a server
        // object. The sealed-segment notifier (continuous anti-entropy +
        // seal-time replication fan-out) rides the pipeline.
        let ae_for_seal_notify = Arc::clone(&durability.ae);
        let replicator_for_seal_notify = Arc::clone(&storage.segment_replicator);
        let sealed_segment_notifier: oceanfs_storage::segment::seal_pipeline::SealedSegmentNotifier =
            Arc::new(move |segment_id, merkle_root| {
                ae_for_seal_notify.on_segment_sealed(segment_id, merkle_root);
                // Seal-time segment replication (sealed-segment-replication):
                // publish the sealed segment for the replicator — a single
                // non-blocking channel send, NO network on the seal path.
                replicator_for_seal_notify.enqueue(segment_id);
            });
        storage.start_seal_pipeline(Some(sealed_segment_notifier));
        // (c1: moved to modules/storage.rs — run_startup_recovery)
        storage.run_startup_recovery().await?;

        // ---- 11. Construct I/O infrastructure ----
        let hinted_handoff = Arc::new(
            HintedHandoff::new_with_pool(pool.clone())
                .with_membership(membership.clone())
                .with_timeouts(durability.op_timeouts.clone()),
        );

        // Construct the persistent per-node HintWAL directory and
        // HintedHandoffManager for durable hinted handoff (ADR-0018 Decision 2).
        // The hints WAL lives on the pinned hints pool root (resolved in
        // pool_paths; the legacy `hint_wal_dir` override was removed by
        // ADR-0031 D2).
        let hints_dir = paths.hints.clone();
        let hint_delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> = Arc::new(
            GrpcHintDeliveryClient::new(pool.clone())
                // The hint receiver fetches segment-ref data back
                // from THIS node's gRPC listener (remote_addr on the
                // receiver is the ephemeral source port — dead by
                // fetch time). The self address is the parsed
                // `grpc_listen_addr` from section 4 — startup already
                // failed if that address was unparseable (B2: no silent
                // default network address).
                .with_self_grpc_addr(grpc_addr),
        );
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
                .with_timeouts(durability.op_timeouts.clone()), // Delivery contract (ADR-0027 as amended): hints are
                                                                // NEVER dropped at the sender — deliver everything, the
                                                                // receiver's HLC-LWW apply is the single gate. The old
                                                                // obsolete pre-check dropped hints based on the sender's
                                                                // view of distributed state, which could diverge from
                                                                // the truth (the churn residual class).
        );

        // Replay existing hints from the WAL into in-memory queues.
        let _replayed = hinted_handoff_manager.replay_and_enqueue().await?;

        // Cluster-readiness gate (phase-3 churn fix): a node that just
        // (re)joined a cluster has a ring containing only itself until
        // its membership pull converges; with the adaptive quorum such a
        // window ACKs writes with a single durable copy (silent
        // under-replication). While the gate is closed the write path
        // returns 503 instead. Single-node deployments (no seeds, no
        // fallback seeds) never close the gate.
        let ready_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let is_cluster_node = !config.gossip.seed_nodes.is_empty()
            || membership_state_store
                .load()
                .map(|state| !state.fallback_seeds.is_empty())
                .unwrap_or(false);
        if is_cluster_node {
            let gate_membership = membership.clone();
            let gate = ready_gate.clone();
            let gate_timeout_secs = config.cluster_ready_timeout_sec.max(1);
            // B6 (review #66/#69): the minimum ring node count comes
            // from config (`cluster_min_quorum_nodes`), not a hard-coded
            // w=2 estimate. Derivation is documented on the field.
            let min_quorum_nodes = config.cluster_min_quorum_nodes;
            tokio::spawn(async move {
                // Open the gate when the ring reaches the configured
                // minimum quorum node count or after the configured
                // bound — the rejoin pull takes seconds; the bound
                // keeps a node whose seeds are unreachable from
                // stalling writes forever (it would serve stale data
                // anyway — the 503s it emits while gated are the safer
                // failure mode). The timeout is config
                // (`cluster_ready_timeout_sec`) because convergence
                // scales with the gossip profile.
                let deadline =
                    tokio::time::Instant::now() + std::time::Duration::from_secs(gate_timeout_secs);
                loop {
                    let ring_nodes = gate_membership.ring().snapshot().node_count();
                    if cluster_ready_gate_opens(
                        ring_nodes,
                        min_quorum_nodes,
                        tokio::time::Instant::now() >= deadline,
                    ) {
                        gate.store(true, std::sync::atomic::Ordering::Release);
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            });
        } else {
            ready_gate.store(true, std::sync::atomic::Ordering::Release);
        }

        // HERE
        // TODO : finish node startup sequence, then ocean durability, then server and quorum functionnalities
        // ---- 8-13. Construct the server subsystem (c3: modules/server.rs) ----
        // Caches + policies (§8), the prefetch engine (§9), the bridge
        // adapter (§10), the write/read coordinators + forwarding router
        // (§11 server parts), the S3 + admin handlers (§12) and the axum
        // router (§13) are built by the module. The central metrics
        // registry is created here FIRST: the module registers its own
        // series (caches, S3 handler, healing service) during build; the
        // node-side series (durability, hinted handoff, pools, storage
        // WAL/pools/replicator, RocksDB) register right below.
        let metrics = Arc::new(oceanfs_server::admin::MetricsRegistry::new());
        let metrics_for_late_registration = Arc::clone(&metrics);
        let server = crate::modules::server::ServerModule::build(
            &config,
            &storage,
            &durability,
            membership.clone(),
            pool.clone(),
            membership_pool,
            ring_cache.clone(),
            manifest_cache.clone(),
            hinted_handoff,
            hinted_handoff_manager.clone(),
            ready_gate,
            is_cluster_node,
            announce_incarnation,
            metrics.clone(),
        )?;

        // Register subsystem metrics into the central registry.
        metrics.register_gauge(storage.startup_rebuild_gauge.clone());
        storage.accel.register_metrics(&*metrics);
        storage.shard_buffer_pool.register_metrics(&*metrics);

        // Phase D: durability subsystem counters (c2: one module call).
        durability.register_metrics(&*metrics);
        // The manager is the component that actually stores and delivers
        // hints; its counters are the authoritative
        // hinted_handoff_hints_{stored,delivered,expired}_total series.
        // (The legacy HintedHandoff — the gRPC *receiver* — is not
        // registered: its counters had inverted semantics and stayed 0.)
        hinted_handoff_manager.register_metrics(&*metrics);
        pool.register_metrics(&*metrics);
        // Segment shard gauges (`segment_active_count` — Phase 2
        // asserts the segment pipeline is producing segments).
        storage.shard_small.register_metrics(&*metrics);
        storage.shard_standard.register_metrics(&*metrics);
        storage.wal_writer.register_metrics(&*metrics);
        storage.sealer.register_metrics(&*metrics);
        // Lifecycle registry-size gauges (ADR-0025 Decision 5 — the
        // registry's O(live segments) memory cost is metric-visible).
        storage.lifecycle.register_metrics(&*metrics);
        // Event WAL metrics (ADR-0024 — bytes, files, append count).
        storage.event_wal.register_metrics(&*metrics);
        // Checkpoint metrics (checkpoint bytes written, bytes truncated).
        storage.event_checkpoint.register_metrics(&*metrics);
        // Storage pool metrics (ADR-0029 — status, bytes free/total,
        // I/O error counter per pool).
        storage.registry.register_metrics(&*metrics);
        // Routing-cache metrics (ADR-0029 §D5 — cache misses,
        // error-driven failovers).
        manifest_cache.register_metrics(&*metrics);
        // Seal-time segment replication metrics (pushed/bytes/retries/
        // failures/needs gauge).
        storage.segment_replicator.register_metrics(&*metrics);

        // Register RocksDB property gauges into the central metrics registry.
        storage.metadata_store.metrics().register(&*metrics);
        // Start the background RocksDB metrics polling task (every 30s).
        storage.metadata_store.start_metrics_task();

        // Register process-level gauges.
        let proc_mem_gauge =
            metrics.gauge("process_resident_memory_bytes", "Resident memory in bytes");
        let proc_fd_gauge = metrics.gauge("process_open_fds", "Open file descriptors");
        // Storage WAL file count — the Phase 2 `wal_not_unbounded`
        // invariant (sealed segments must keep the WAL consumed).
        let wal_count_gauge = metrics.gauge("wal_file_count", "Storage WAL files present");
        // Live segment-pipeline gauge: the shard registration above sets
        // the initial value; this poller refreshes it from the pools'
        // Appending slots, which churn as segments fill and seal.
        let active_segments_gauge =
            metrics.gauge("segment_active_count", "Active segment groups in the sharded pool");

        // Spawn a background poller for process-level metrics (every 15s).
        // RocksDB metrics are polled separately by metadata_store.start_metrics_task().
        let wal_dir = paths.wal.clone();
        // Active pools snapshot for the poller — cloned before the spawn
        // (the module field stays owned by `storage`).
        let active_pools_for_metrics = storage.active_pools.clone();
        // [review][implementation][high]
        // the metric poller cannot be cancelled, unkink other background tasks
        // on an another topic : we seems to make the start function bear the initialisation logic of every module.
        // a good implementation approach would be to rather make dedicated modules hide the implementation behind a setup method
        // [end]
        let _process_poller = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            loop {
                interval.tick().await;
                if let Ok(mem) = read_process_memory_bytes() {
                    proc_mem_gauge.set(mem);
                }
                if let Ok(fds) = read_process_open_fds() {
                    proc_fd_gauge.set(fds);
                }
                let wal_config = WalConfig { data_dir: wal_dir.clone(), ..WalConfig::default() };
                wal_count_gauge.set(oceanfs_storage::count_wal_files(&wal_config) as u64);
                let live = active_pools_for_metrics.iter().map(|p| p.active_count()).sum::<usize>();
                active_segments_gauge.set(live as u64);
            }
        });
        // ---- 14. Bind HTTP server ----
        let http_listener = tokio::net::TcpListener::bind(&config.listen_addr)
            .await
            .map_err(|e| format!("failed to bind HTTP server on {}: {e}", config.listen_addr))?;
        let server_addr = http_listener.local_addr()?;

        let http_shutdown = CancellationToken::new();
        let http_shutdown_signal = http_shutdown.clone();

        tokio::spawn(async move {
            if let Err(e) = axum::serve(http_listener, server.router.into_make_service())
                .with_graceful_shutdown(http_shutdown_signal.cancelled_owned())
                .await
            {
                error!("HTTP server error: {e}");
            }
        });

        // ---- 15. Bind gRPC server ----
        let grpc_addr: SocketAddr = config
            .grpc_listen_addr
            .parse()
            .map_err(|e| format!("invalid grpc_listen_addr: {e}"))?;

        // The data-plane gRPC services are constructed inside the server
        // module (c3) with their decode caps baked into the wrapped RPC
        // servers; the tonic router assembly lives at the bind. The
        // membership gossip/probe services are returned unwrapped for
        // the membership-plane bind at §15b.
        let grpc_router = tonic::transport::Server::builder()
            .add_service(server.grpc.segment)
            .add_service(server.grpc.healing)
            .add_service(server.grpc.cache)
            .add_service(server.grpc.scrub);

        // Create gRPC shutdown token before spawning so it can be used
        // by both the gRPC server and BackgroundTasks.
        let grpc_shutdown = CancellationToken::new();
        let _grpc_shutdown_signal = grpc_shutdown.clone();

        let grpc_server_handle = tokio::spawn(async move {
            use std::os::unix::io::AsRawFd;

            use tokio_stream::StreamExt;

            let listener = match create_reuseport_listener(grpc_addr) {
                Ok(l) => l,
                Err(e) => {
                    error!("gRPC listener creation failed for {grpc_addr}: {e}");
                    return;
                }
            };

            let stream =
                tokio_stream::wrappers::TcpListenerStream::new(listener).map(move |conn| {
                    if let Ok(ref stream) = conn {
                        apply_opts_to_fd(stream.as_raw_fd(), quickack, busy_poll);
                    }
                    conn
                });

            if let Err(e) = grpc_router.serve_with_incoming(stream).await {
                error!("gRPC server error: {e}");
            }
        });

        // ---- 15b. Bind the membership plane (ADR-0028 D1) ----
        // A separate listener on membership_listen_addr hosting ONLY the
        // membership services: GossipRpc (push/pull) + ProbeRpc (SWIM).
        // Isolation from the data plane is the point — probe latency must
        // not inherit the data plane's tail (16 MiB streams, hint
        // batches). Bound BEFORE membership.start(): peers probe and
        // push to this listener immediately after the join announcement.
        let membership_router = tonic::transport::Server::builder()
            .add_service(oceanfs_network::GossipRpcServer::new(server.gossip_service))
            .add_service(oceanfs_network::gossip::probe_rpc_server::ProbeRpcServer::new(
                server.probe_service,
            ));

        let membership_listener = match create_reuseport_listener(membership_addr) {
            Ok(l) => l,
            Err(e) => {
                error!("membership plane listener creation failed for {membership_addr}: {e}");
                return Err(format!(
                    "membership plane listener creation failed for {membership_addr}: {e}"
                )
                .into());
            }
        };

        tokio::spawn(async move {
            // Same socket treatment as the data plane (perf 4.3):
            // quickack + busy-poll on accepted membership connections —
            // probe latency is the detection bound.
            use std::os::unix::io::AsRawFd;

            use tokio_stream::StreamExt;

            let stream = tokio_stream::wrappers::TcpListenerStream::new(membership_listener).map(
                move |conn| {
                    if let Ok(ref stream) = conn {
                        apply_opts_to_fd(stream.as_raw_fd(), quickack, busy_poll);
                    }
                    conn
                },
            );
            if let Err(e) = membership_router.serve_with_incoming(stream).await {
                error!("membership plane server error: {e}");
            }
        });

        // ---- 15c. Bootstrap membership: start failure detection +
        // gossip, then join the ring. MUST happen after the gRPC server
        // is bound: peers probe and deliver hinted handoffs to our gRPC
        // listener immediately after the join announcement, and a join
        // that precedes the bind produces join-time false Suspects and
        // refused hint deliveries (t5/t21).
        membership.start().map_err(|e| format!("failed to start membership: {e}"))?;
        // Register the gossip metrics AFTER start(): the gossip
        // protocol + its counters/histograms are created inside
        // start() — an earlier registration captured None and the
        // gossip series never appeared (the timing-metrics run
        // queried an empty metric).
        membership.register_membership_metrics(&*metrics_for_late_registration);

        // ---- 15d. Declare the storage-pool manifest (ADR-0029 D2) ----
        // Built once from the registry with the announce incarnation and
        // attached to the self membership entry: the version bump the
        // manifest triggers is all the gossip plane needs to propagate it
        // (a pool change is not a restart — the incarnation is untouched).
        // Phase A registers at boot only; f8 (runtime-attach) re-declares
        // on pool set changes. The join() below carries the manifest in
        // its self-announcement, so seeds learn it immediately.
        let node_manifest =
            crate::pool_manifest::build_node_manifest(announce_incarnation, &storage.registry);
        membership.set_self_manifest(node_manifest.clone());
        // Seed the routing cache with the self manifest so the node's
        // own pool state is visible to the exclusion filters (and the
        // peers' caches converge to include it via gossip).
        manifest_cache.update(NodeId::new(&config.node_id), Arc::new(node_manifest));

        // ---- 15e. Routing-cache event subscriber (ADR-0029 §D5) ----
        // Populates the per-peer manifest cache from membership events:
        // version-bumped entries carry the manifest (f6), Dead/Left
        // members are evicted. The cache is a hint — a stale-but-present
        // manifest beats absent, and the error path is the guarantee.
        let cache_events = membership.subscribe();
        let cache_for_events = manifest_cache.clone();
        let cache_shutdown = membership.shutdown_token();
        tokio::spawn(async move {
            let mut cache_events = cache_events;
            loop {
                tokio::select! {
                    event = cache_events.recv() => {
                        match event {
                            Ok(ev) => {
                                match ev.new_state {
                                    oceanfs_core::NodeState::Dead
                                    | oceanfs_core::NodeState::Left => {
                                        cache_for_events.remove(&ev.node_id);
                                    }
                                    _ => {
                                        if let Some(manifest) = ev.manifest {
                                            cache_for_events.update(ev.node_id, manifest);
                                        }
                                    }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!(skipped = n, "routing cache subscriber lagged");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    _ = cache_shutdown.cancelled() => break,
                }
            }
            tracing::debug!("routing cache subscriber shut down");
        });

        let join_incarnation = Incarnation::new(announce_incarnation);
        let join_fallback_seeds = durable_state.fallback_seeds.clone();
        if let Err(e) = membership.join(join_incarnation, &join_fallback_seeds).await {
            // A transient seed outage at boot must not isolate the node:
            // with configured seeds the old behavior ABORTED the process
            // (and the unit is Restart=no, so the node stayed down); with
            // empty configured seeds (restart path) it started as a
            // singleton with no retry. Instead, warn and rejoin in the
            // background — the cluster-readiness gate keeps writes
            // refused until the ring converges.
            warn!(error = %e, "initial cluster join failed; retrying in the background");
        }

        // Background rejoin: retry the (idempotent) join every 3s until
        // the ring reaches the configured minimum quorum node count
        // (`cluster_min_quorum_nodes`, B6 — review #66/#69). Covers the
        // seedless-restart path (fallback seeds) and fleet nodes that
        // boot before their seed comes up. Exits once joined.
        if is_cluster_node {
            let retry_membership = membership.clone();
            let retry_incarnation = join_incarnation;
            let retry_fallback = join_fallback_seeds.clone();
            let min_quorum_nodes = config.cluster_min_quorum_nodes;
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
                loop {
                    interval.tick().await;
                    let ring_nodes = retry_membership.ring().snapshot().node_count();
                    if cluster_ready_gate_opens(ring_nodes, min_quorum_nodes, false) {
                        return;
                    }
                    if let Err(e) = retry_membership.join(retry_incarnation, &retry_fallback).await
                    {
                        tracing::debug!(error = %e, "rejoin retry failed");
                    }
                }
            });
        }

        // After a successful join, snapshot the known member addresses as
        // fallback seeds. Events emitted during join are missed by the
        // watcher spawned later (broadcast channels do not replay), so
        // this write also captures members learned from the seed pull.
        // Self is excluded: its own old address is useless after a
        // restart (t43).
        //
        // A seedless singleton join (no configured seeds, all fallback
        // seeds down at restart time) must NOT wipe the persisted list:
        // the snapshot would contain only self → `save_fallback_seeds([])`
        // — and every later restart would then have no seeds at all,
        // stranding the node forever (observed in the churn run: node-0
        // restarted at inc 2 with fallback_seeds=2, then inc 3/4/5 with
        // fallback_seeds=0 after the wipe). The persisted list is the
        // last-known truth; only a join that actually learned peers may
        // replace it.
        {
            let self_id = NodeId::new(&config.node_id);
            let seeds: Vec<String> = membership
                .nodes_full()
                .iter()
                .filter(|(id, _, _, _, _, _, _, _)| *id != self_id)
                .map(|(_, _, _, _, membership_addr, _, _, _)| membership_addr.to_string())
                .collect();
            if seeds.is_empty() {
                tracing::debug!(
                    node_id = %config.node_id,
                    "join learned no peers — keeping the persisted fallback seeds"
                );
            } else if let Err(e) = membership_state_store.save_fallback_seeds(&seeds) {
                warn!(error = %e, "failed to persist fallback seeds after join");
            }
        }

        // ---- 16. Spawn background tasks ----
        let mut background = Self::spawn_background_tasks(
            durability.gc.clone(),
            storage.metadata_store.clone(),
            Arc::clone(&storage.lifecycle_registry),
            durability.ae.clone(),
            durability.scrub.clone(),
            durability.reaper.clone(),
            server.prefetch_engine,
            durability.heal.clone(),
            storage.data_store.clone(),
            hinted_handoff_manager.clone(),
            &config,
        );
        background.grpc_shutdown = grpc_shutdown;
        background.grpc_server = Some(grpc_server_handle);

        // ---- 16b. Spawn the pool health monitor + consequence applier ----
        // (g2 `failure-state-machine`, ADR-0029 §D3). The monitor ticks
        // each pool every `detection_window_secs` (f1 per-pool knobs),
        // drives registry status + wal write_degraded, and emits bounded
        // status events; the applier maps role → consequences
        // (metadata Dead → the node serves nothing, surfaced lazily via
        // PoolRegistry::node_serves_requests — g6's gates; data Dead →
        // affected segments) and re-declares the manifest so peers see
        // the change.
        let (health_monitor, health_events) = oceanfs_storage::pool::health::HealthMonitor::new(
            storage.registry.clone(),
            storage.io_observer.clone(),
            oceanfs_storage::pool::health::HealthMonitorConfig::default(),
        );
        let health_cancel = CancellationToken::new();
        let health_token = health_cancel.clone();
        let health_handle = tokio::spawn(async move {
            health_monitor.run(health_token).await;
            info!("Pool health monitor stopped");
        });
        background.health_monitor = Some(health_handle);
        background.health_cancel = health_cancel;
        // g3 `loss-announcement` fan-out (ADR-0029 §D4 fast path): when a
        // data pool is confirmed Dead, announce the affected segment set
        // to `union(storage_locations − self)` over the set — bounded
        // retries, then drop (g4 reconciliation is the failsafe).
        // `announcements_enabled=false` (tests) disables the push so the
        // g4 reconciliation loop is proven to be the independent safety
        // net.
        let loss_announcer: Option<crate::health::LossAnnouncer> = if config.announcements_enabled {
            let lifecycle_registry = Arc::clone(&storage.lifecycle_registry);
            let membership = Arc::clone(&membership);
            let pool = Arc::clone(&pool);
            let self_id = NodeId::new(&config.node_id);
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
                    let locations: Vec<(SegmentId, Vec<NodeId>)> = affected
                        .iter()
                        .filter_map(|segment_id| {
                            lifecycle_registry.get(*segment_id).map(|entry| {
                                (*segment_id, entry.metadata.storage_locations.to_vec())
                            })
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
        let consequences_handle = crate::health::spawn_health_consequences(
            health_events,
            storage.registry.clone(),
            membership.clone(),
            Arc::clone(&storage.lifecycle_registry),
            NodeId::new(&config.node_id),
            announce_incarnation,
            manifest_cache.clone(),
            loss_announcer,
        );
        background.health_consequences = Some(consequences_handle);

        // ---- 16c. Spawn the seal-time segment replicator ----
        // (sealed-segment-replication). The drain loop consumes
        // sealed-segment events (seal worker + compactor + startup pass)
        // and pushes each segment's data to its ring replicas; the sweep
        // retries the needs set. Runs until shutdown.
        let replicator_cancel = CancellationToken::new();
        let replicator_token = replicator_cancel.clone();
        let replicator_for_spawn = Arc::clone(&storage.segment_replicator);
        let replicator_handle = tokio::spawn(async move {
            replicator_for_spawn.run(replicator_token).await;
            info!("Segment replicator stopped");
        });
        background.segment_replicator = Some(replicator_handle);
        background.segment_replicator_cancel = replicator_cancel;

        // ---- 16d. Spawn the periodic reconciliation loop (g4) ----
        // (ADR-0029 §D4 pull safety net). Event-driven wake + bounded
        // risk-prioritized queue + hourly drift scan. Runs independently
        // of announcements — the complete safety net.
        let reconciliation_cancel = CancellationToken::new();
        let reconciliation_token = reconciliation_cancel.clone();
        let reconciliation_for_spawn = Arc::clone(&durability.reconciliation);
        let reconciliation_handle = tokio::spawn(async move {
            reconciliation_for_spawn.run(reconciliation_token).await;
            info!("Reconciliation loop stopped");
        });
        background.reconciliation = Some(reconciliation_handle);
        background.reconciliation_cancel = reconciliation_cancel;

        // ---- 16e. Spawn the re-replication worker + dispatcher (g5) ----
        // (ADR-0030 target-pull). The worker drains the acquiring-side
        // queue (fed by the request_re_replication RPC handler) and
        // pulls + writes + stamps; the dispatcher retries parked
        // requests that had no eligible target.
        let rep_worker_cancel = CancellationToken::new();
        let rep_worker_token = rep_worker_cancel.clone();
        let rep_worker_for_spawn = Arc::clone(&durability.rep_worker);
        let rep_worker_handle = tokio::spawn(async move {
            rep_worker_for_spawn.run(rep_worker_token).await;
            info!("Re-replication worker stopped");
        });
        background.rep_worker = Some(rep_worker_handle);
        background.rep_worker_cancel = rep_worker_cancel;

        let rep_dispatcher_cancel = CancellationToken::new();
        let rep_dispatcher_token = rep_dispatcher_cancel.clone();
        let rep_dispatcher_for_spawn = Arc::clone(&durability.repair_dispatcher);
        let rep_dispatcher_handle = tokio::spawn(async move {
            rep_dispatcher_for_spawn.run(rep_dispatcher_token).await;
            info!("Re-replication dispatcher stopped");
        });
        background.rep_dispatcher = Some(rep_dispatcher_handle);
        background.rep_dispatcher_cancel = rep_dispatcher_cancel;

        // HERE

        // ---- 17. Spawn hinted handoff delivery watcher ----
        // Watches for membership events and drains the handoff buffer
        // for nodes that are (or return to) ALIVE. Any Alive event —
        // including an Alive→Alive address update from a rejoin
        // (ADR-0022, t21) — triggers delivery: `deliver_pending` is a
        // no-op when nothing is buffered. On the same events it also
        // records the node's address in the persisted fallback-seed
        // list (ADR-0022 D3) — incrementally from the event itself, so
        // the write never races the membership manager's apply step.
        let hh = hinted_handoff_manager.clone();
        let seed_store = membership_state_store.clone();
        let self_node_id = NodeId::new(&config.node_id);
        let mut events = membership.subscribe();
        let delivery_token = background.delivery_cancel.clone();
        let mut sweep_interval = tokio::time::interval(std::time::Duration::from_secs(
            config.hint_delivery_sweep_sec.max(1),
        ));
        // [review][architecture][high]
        // at multiple points during the startup phase, we define submodules using inner function and spawn + handle pattern directly inside the
        // startup function. this bloats the function out, an blurs the responsibilities.
        // as a matter of principle, any submodules should have it's own dedicated file / module and expose
        // its startup sequence.
        // [end]
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
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
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
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                warn!(skipped = n, "hinted handoff watcher lagged");
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                info!("Membership event channel closed; stopping delivery watcher");
                                break;
                            }
                        }
                    }
                }
            }
        });
        background.hinted_handoff_delivery = Some(delivery_handle);

        // Start periodic connection pool health checks (perf rule §4.1).
        // The health check loop runs until cancelled during shutdown.
        let health_cancel = background.health_check_cancel.clone();
        pool.start_health_check_loop(health_cancel);

        info!(
            node_id = %config.node_id,
            http_addr = %server_addr,
            grpc_addr = %grpc_addr,
            "OceanFS node started"
        );

        Ok(Node {
            config,
            server_addr,
            grpc_addr,
            http_shutdown,
            grpc_shutdown: background.grpc_shutdown.clone(),
            background,
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
    /// Sequence: graceful leave → cancel gRPC → cancel HTTP → cancel background
    /// tasks → wait for tasks → flush WAL → close metadata → drop subsystems.
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

        // ---- 2. Cancel gRPC server (stop accepting new RPCs, drain in-flight) ----
        self.grpc_shutdown.cancel();

        // ---- 3. Signal the HTTP server to stop accepting connections and drain ----
        self.http_shutdown.cancel();

        // ---- 4. Signal all background tasks to stop ----
        self.background.gossip_cancel.cancel();
        self.background.gc_cancel.cancel();
        self.background.ae_cancel.cancel();
        self.background.scrub_cancel.cancel();
        self.background.reaper_cancel.cancel();
        self.background.prefetch_cancel.cancel();
        self.background.fd_cancel.cancel();
        self.background.heal_cancel.cancel();
        self.background.delivery_cancel.cancel();
        self.background.hint_prune_cancel.cancel();
        self.background.health_check_cancel.cancel();
        self.background.health_cancel.cancel();
        self.background.segment_replicator_cancel.cancel();
        // g5 re-replication (ADR-0030): stop the acquiring-side worker
        // and the holder-side dispatcher sweep.
        self.background.rep_worker_cancel.cancel();
        self.background.rep_dispatcher_cancel.cancel();

        // [review][config][high]
        // the shutdown grace period should be configurable, since it's dimensions is the product
        // of the queues sizes, and expected system load.
        // [end]
        // ---- 5. Wait for background tasks with a timeout ----
        let _ = tokio::time::timeout(Duration::from_secs(10), async {
            let _ = tokio::try_join!(
                async { self.background.gossip.await.map_err(|e| format!("{e}")) },
                async { self.background.gc.await.map_err(|e| format!("{e}")) },
                async { self.background.anti_entropy.await.map_err(|e| format!("{e}")) },
                async { self.background.scrub.await.map_err(|e| format!("{e}")) },
                async { self.background.orphan_reaper.await.map_err(|e| format!("{e}")) },
                async { self.background.failure_detector.await.map_err(|e| format!("{e}")) },
                async { self.background.heal.await.map_err(|e| format!("{e}")) },
                async { self.background.hinted_handoff_prune.await.map_err(|e| format!("{e}")) },
                // The segment replicator drains its bounded channel; if it
                // is mid-push the timeout below bounds the wait (its
                // receiver is dropped by the node drop anyway).
                async {
                    match self.background.segment_replicator {
                        Some(h) => h.await.map_err(|e| format!("{e}")),
                        None => Ok(()),
                    }
                },
                // g5: the worker drains its bounded queue; the dispatcher
                // stops its sweep. Both bound the wait via the timeout.
                async {
                    match self.background.rep_worker {
                        Some(h) => h.await.map_err(|e| format!("{e}")),
                        None => Ok(()),
                    }
                },
                async {
                    match self.background.rep_dispatcher {
                        Some(h) => h.await.map_err(|e| format!("{e}")),
                        None => Ok(()),
                    }
                },
            );
        })
        .await;

        // Wait for prefetch handle separately (it may be None).
        if let Some(pf) = self.background.prefetch {
            let _ = tokio::time::timeout(Duration::from_secs(5), pf).await;
        }

        // Wait for hinted handoff delivery handle (may be None).
        if let Some(dh) = self.background.hinted_handoff_delivery {
            let _ = tokio::time::timeout(Duration::from_secs(5), dh).await;
        }

        // Wait for gRPC server handle (may be None).
        if let Some(grpc_handle) = self.background.grpc_server {
            let _ = tokio::time::timeout(Duration::from_secs(5), grpc_handle).await;
        }

        // ---- 6. Flush WAL writer to disk ----
        if let Err(e) = self.storage.wal_writer.sync().await {
            warn!(error = %e, "WAL sync failed during shutdown");
        }

        // ---- 7. Close metadata store (flush RocksDB) ----
        if let Err(e) = self.storage.metadata_store.close() {
            warn!(error = %e, "metadata store close failed during shutdown");
        }

        // ---- 8. Membership shutdown (cancels internal gossip + FD) ----
        self.membership.shutdown();

        info!(node_id = %self.config.node_id, "OceanFS node shut down");
        Ok(())
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

    /// Spawns all background task loops.
    #[allow(clippy::too_many_arguments)]
    fn spawn_background_tasks(
        gc_worker: Arc<oceanfs_durability::GarbageCollector>,
        metadata_store: Arc<oceanfs_storage::RocksDbMetadataStore>,
        lifecycle_registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
        ae_worker: Arc<oceanfs_durability::AntiEntropy>,
        scrub_worker: Arc<oceanfs_durability::ScrubCoordinator>,
        reaper: Arc<oceanfs_durability::OrphanReaper>,
        prefetch_engine: Arc<oceanfs_cache::PrefetchEngine>,
        heal_worker: Arc<oceanfs_durability::HealWorker>,
        data_store: Arc<dyn oceanfs_durability::SegmentDataStore>,
        hinted_handoff_manager: Arc<HintedHandoffManager>,
        config: &oceanfs_core::NodeConfig,
    ) -> BackgroundTasks {
        // The background tasks hold the registry across 'static spawns.
        let gc_registry = Arc::clone(&lifecycle_registry);
        let scrub_registry = Arc::clone(&lifecycle_registry);

        // Gossip: Membership drives the gossip protocol internally via
        // Membership::start(). This task holds a cancellation-aware standby
        // so the shutdown sequence can await it cleanly.
        let gossip_cancel = CancellationToken::new();
        let gossip_token = gossip_cancel.clone();
        let gossip = tokio::spawn(async move {
            gossip_token.cancelled().await;
            info!("Gossip task cancelled");
        });

        // GC: runs every gc_interval_sec from config.
        let gc_cancel = CancellationToken::new();
        let gc_token = gc_cancel.clone();
        let gc_store = metadata_store.clone();
        let gc_interval = Duration::from_secs(config.gc_interval_sec);
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let gc = tokio::spawn(async move {
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
        });

        // Anti-entropy: runs every ae_interval_sec from config.
        let ae_cancel = CancellationToken::new();
        let ae_token = ae_cancel.clone();
        let ae_interval_secs = config.ae_interval_sec;
        // Continuous mode exchanges Merkle ROOTS with peers via the
        // incremental tree — it never reads segment data, so per-cycle
        // cost is O(sealed segments) metadata calls instead of reading
        // every segment file (GBs per cycle on the phase-2 SUT, which
        // stalled cycles for 90s+ under load and spiked RSS). The full
        // cycle (reads all data + rebuilds trees) stays available for
        // `continuous_enabled = false`.
        let ae_continuous = config.anti_entropy.continuous_enabled;
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let ae = tokio::spawn(async move {
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
        });

        // Scrub: runs every scrub_interval_sec from config.
        let scrub_cancel = CancellationToken::new();
        let scrub_token = scrub_cancel.clone();
        let _scrub_store = metadata_store.clone();
        let scrub_data = data_store;
        let scrub_interval_secs = config.scrub_interval_sec;
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let scrub = tokio::spawn(async move {
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
        });

        // Orphan reaper: runs every orphan_reaper_interval_sec from config.
        let reaper_cancel = CancellationToken::new();
        let reaper_token = reaper_cancel.clone();
        let reaper_interval = Duration::from_secs(config.orphan_reaper_interval_sec);
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let orphan_reaper = tokio::spawn(async move {
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
        });

        // Prefetch background pre-warmer: PrefetchEngine runs its own internal
        // worker (spawned in PrefetchEngine::new()). This task holds the engine
        // Arc alive and waits for cancellation. When prefetch is disabled, the
        // engine silently drops all queued tasks.
        let prefetch_cancel = CancellationToken::new();
        let prefetch_token = prefetch_cancel.clone();
        let prefetch = Some(tokio::spawn(async move {
            // Hold the engine alive for the lifetime of this task.
            let _engine = prefetch_engine;
            prefetch_token.cancelled().await;
            info!("Prefetch task cancelled");
        }));

        // SWIM failure detector: Membership drives the failure detector
        // internally via Membership::start(). This task holds a cancellation-
        // aware standby so the shutdown sequence can await it cleanly.
        let fd_cancel = CancellationToken::new();
        let fd_token = fd_cancel.clone();
        let failure_detector = tokio::spawn(async move {
            fd_token.cancelled().await;
            info!("Failure detector task cancelled");
        });

        // EC Heal worker: drains the HealQueue and repairs corrupt shards.
        let heal_cancel = CancellationToken::new();
        let heal_token = heal_cancel.clone();
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let heal = tokio::spawn(async move {
            if io_idle {
                oceanfs_storage::io::apply_background_io_class("heal");
            }
            if cpu_idle {
                oceanfs_storage::io::apply_background_cpu_sched("heal");
            }
            heal_worker.run(heal_token).await;
            info!("Heal worker task completed");
        });

        // Hinted handoff delivery watcher token — the watcher itself is
        // spawned after BackgroundTasks is constructed so we can store
        // the join handle retroactively.
        let delivery_cancel = CancellationToken::new();

        // Hinted handoff WAL periodic prune — removes expired entries
        // from all per-node WAL files to bound storage growth.
        let hint_prune_cancel = CancellationToken::new();
        let hint_prune_token = hint_prune_cancel.clone();
        let hint_ttl_secs = config.hint_ttl_sec;
        let hint_prune_interval = Duration::from_secs(config.hint_prune_interval_sec);
        let hinted_handoff_prune = tokio::spawn(async move {
            let mut interval = tokio::time::interval(hint_prune_interval);
            loop {
                tokio::select! {
                    _ = hint_prune_token.cancelled() => {
                        info!("Hinted handoff WAL prune task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        match hinted_handoff_manager.prune_all_expired(hint_ttl_secs).await {
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
        });

        BackgroundTasks {
            gossip,
            gossip_cancel,
            gc,
            gc_cancel,
            anti_entropy: ae,
            ae_cancel,
            scrub,
            scrub_cancel,
            orphan_reaper,
            reaper_cancel,
            prefetch,
            prefetch_cancel,
            failure_detector,
            fd_cancel,
            heal,
            heal_cancel,
            hinted_handoff_delivery: None,
            delivery_cancel,
            hinted_handoff_prune,
            hint_prune_cancel,
            grpc_server: None,
            grpc_shutdown: CancellationToken::new(),
            health_check_cancel: CancellationToken::new(),
            health_monitor: None,
            health_cancel: CancellationToken::new(),
            health_consequences: None,
            segment_replicator: None,
            segment_replicator_cancel: CancellationToken::new(),
            reconciliation: None,
            reconciliation_cancel: CancellationToken::new(),
            rep_worker: None,
            rep_worker_cancel: CancellationToken::new(),
            rep_dispatcher: None,
            rep_dispatcher_cancel: CancellationToken::new(),
        }
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
fn read_process_memory_bytes() -> Result<u64, std::io::Error> {
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
fn read_process_open_fds() -> Result<u64, std::io::Error> {
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
        let tmp = TempDir::new().expect("tempdir");
        let metadata_config =
            MetadataConfig { data_dir: tmp.path().join("metadata"), ..Default::default() };
        let metadata_store = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&metadata_config)
                .expect("open metadata store"),
        );
        let gc_config = oceanfs_durability::GcConfig::default();
        let gc_worker = Arc::new(oceanfs_durability::GarbageCollector::new(gc_config.clone()));

        // Wire minimal membership and connection pool for AntiEntropy construction
        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(oceanfs_membership::Membership::new(
            oceanfs_core::NodeId::new("test-node"),
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        let pool =
            Arc::new(oceanfs_network::ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let lifecycle_registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let ae_worker = Arc::new(oceanfs_durability::AntiEntropy::new(
            oceanfs_durability::AntiEntropyConfig::default(),
            membership,
            Arc::clone(&lifecycle_registry),
            pool,
            Arc::new(oceanfs_durability::InMemorySegmentStore::new()),
            Arc::new(oceanfs_durability::merkle::IncrementalMerkleTree::new(
                oceanfs_durability::merkle::MerkleTreeConfig::default(),
            )),
        ));
        let scrub_config = oceanfs_durability::ScrubConfig::default();
        let scrub_worker = Arc::new(oceanfs_durability::ScrubCoordinator::new(scrub_config));
        let reaper_shard_store: Arc<dyn oceanfs_durability::SegmentShardStore> =
            Arc::new(oceanfs_durability::InMemorySegmentShardStore::new(4194304));
        let lifecycle = Arc::new(oceanfs_storage::SegmentLifecycleCoordinator::with_registry(
            Arc::clone(&lifecycle_registry),
        ));
        let reaper = Arc::new(oceanfs_durability::OrphanReaper::new(
            metadata_store.clone(),
            lifecycle,
            reaper_shard_store.clone(),
            gc_config,
        ));

        let prefetch_config = oceanfs_cache::PrefetchConfig::default();
        let prefetch_store: Arc<dyn oceanfs_storage_api::MetadataStore> =
            Arc::new(PrefetchStoreAdapter { store: metadata_store.clone() });
        let _metadata_cache = Arc::new(oceanfs_cache::MetadataCache::new(
            oceanfs_cache::MetadataCacheConfig::default(),
            Box::new(oceanfs_cache::eviction::TtlLruPolicy::new(
                oceanfs_cache::eviction::TtlLruConfig::default(),
            )),
        ));
        let prefetch_engine = Arc::new(oceanfs_cache::PrefetchEngine::new(
            prefetch_config,
            _metadata_cache,
            None,
            prefetch_store,
        ));

        // Create minimal heal worker for testing
        let heal_config = oceanfs_durability::HealConfig::default();
        let heal_queue = Arc::new(oceanfs_durability::HealQueue::new(heal_config.queue_capacity()));
        let heal_decoder: Arc<dyn oceanfs_ec::Decoder> =
            Arc::new(oceanfs_ec::CauchyEncoder::new(oceanfs_core::CodecConfig::default()));
        let bg_data_store: Arc<dyn oceanfs_durability::SegmentDataStore> =
            Arc::new(oceanfs_durability::InMemorySegmentStore::new());
        let heal_data_store: Arc<dyn oceanfs_durability::SegmentDataStore> =
            Arc::new(oceanfs_durability::InMemorySegmentStore::new());
        let heal_lifecycle = Arc::new(oceanfs_storage::SegmentLifecycleCoordinator::new(
            &oceanfs_core::LifecycleConfig::default(),
        ));
        let heal_worker = Arc::new(oceanfs_durability::HealWorker::new(
            heal_config,
            heal_queue,
            heal_decoder,
            heal_lifecycle,
            heal_data_store,
        ));

        let hints_dir = tmp.path().join("bg_hints");
        let hint_delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> =
            Arc::new(GrpcHintDeliveryClient::new(Arc::new(oceanfs_network::ConnectionPool::new(
                oceanfs_core::RpcConfig::default(),
            ))));
        let hint_config =
            HintedHandoffConfig { wal_dir: hints_dir.clone(), ..HintedHandoffConfig::default() };
        let hinted_handoff_manager =
            Arc::new(HintedHandoffManager::new(hints_dir, hint_delivery_client, hint_config));

        let bg = Node::spawn_background_tasks(
            gc_worker,
            metadata_store.clone(),
            Arc::clone(&lifecycle_registry),
            ae_worker,
            scrub_worker,
            reaper,
            prefetch_engine,
            heal_worker,
            bg_data_store,
            hinted_handoff_manager,
            &NodeConfig::default(),
        );

        // Verify handles are not finished immediately (they are pending).
        assert!(!bg.gossip.is_finished());
        assert!(!bg.gc.is_finished());
        assert!(!bg.anti_entropy.is_finished());
        assert!(!bg.scrub.is_finished());
        assert!(!bg.orphan_reaper.is_finished());
        assert!(bg.prefetch.is_some());
        assert!(!bg.failure_detector.is_finished());
        assert!(!bg.heal.is_finished());
        assert!(!bg.hinted_handoff_prune.is_finished());

        // Cancel all and wait.
        bg.gossip_cancel.cancel();
        bg.gc_cancel.cancel();
        bg.ae_cancel.cancel();
        bg.scrub_cancel.cancel();
        bg.reaper_cancel.cancel();
        bg.prefetch_cancel.cancel();
        bg.fd_cancel.cancel();
        bg.heal_cancel.cancel();
        bg.hint_prune_cancel.cancel();
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
