//! Membership-plane bundle (c4 — planes split).
//!
//! Owns everything the *membership plane* (ADR-0028) needs on the node:
//! the [`Membership`] state machine + its durable rejoin state
//! (ADR-0022), the peer-side routing/manifest cache (ADR-0029 §D5), the
//! plane's dedicated connection pool, the gossip/probe gRPC services
//! (re-seated from the c3 server module — they wrap ONLY membership-plane
//! inputs), the plane listener bind, and the bootstrap sequence
//! (`membership.start()` → manifest declaration → join/rejoin →
//! fallback-seed snapshot).
//!
//! The data plane (HTTP + data-plane gRPC binds and the shared data
//! pool) lives in `modules/data_plane.rs`; `Node::start()` orders the
//! two: membership plane bind + start MUST follow the data-plane binds
//! (peers probe and deliver hinted handoffs to our gRPC listener
//! immediately after the join announcement).

use std::{net::SocketAddr, sync::Arc};

use oceanfs_core::{NodeConfig, NodeId, RpcConfig};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::{
    membership_state::{default_state_path, MembershipStateStore},
    routing_cache::ManifestCache,
};

/// The membership-plane bundle (c4).
///
/// Built early (before the storage/durability/server builders — they all
/// consume `membership`), bound and started late by
/// [`Self::start_plane_and_join`] after the data-plane binds.
pub(crate) struct MembershipModule {
    /// The cluster membership state machine (§4 move).
    pub(crate) membership: Arc<oceanfs_membership::Membership>,
    /// Peer-side NodeManifest cache (§4b move) — fed by the routing-cache
    /// subscriber (started in `start_plane_and_join`) and consumed by the
    /// c3 coordinators + the §16b health consequences.
    pub(crate) manifest_cache: Arc<ManifestCache>,
    /// Durable rejoin state store (§4a move) — node.rs §17's delivery
    /// watcher clones it to record fallback seeds on Alive events.
    pub(crate) membership_state_store: MembershipStateStore,
    /// The incarnation this boot announces with (§4a move).
    pub(crate) announce_incarnation: u64,
    /// Whether this node is a cluster member (seeds or persisted
    /// fallback seeds) — consumed by the ready gate.
    pub(crate) is_cluster_node: bool,
    /// The cluster-readiness gate (phase-3 churn fix): while closed the
    /// write path returns 503 (no silent under-replication during ring
    /// convergence). Opened immediately for single-node deployments;
    /// for cluster nodes [`Self::spawn_ready_gate`] opens it when the
    /// ring reaches the configured minimum quorum or the configured
    /// bound elapses. Consumed by the c3 write coordinator
    /// (`.with_ready_gate`).
    pub(crate) ready_gate: Arc<std::sync::atomic::AtomicBool>,
    /// Strictly-parsed data-plane gRPC address (§4 move) — consumed by
    /// the §11 hint-delivery self address.
    pub(crate) grpc_addr: SocketAddr,
    /// The membership plane's dedicated connection pool (ADR-0028 D1 —
    /// per-peer 2, probe-derived timeouts). Private: only the plane's own
    /// probe service and gossip set_pool consume it.
    membership_pool: Arc<oceanfs_network::ConnectionPool>,
    /// This node's id.
    node_id: NodeId,
    /// The membership plane's listen address (ADR-0028 D1).
    membership_addr: SocketAddr,
    /// Probe timeout budget (`gossip.failure_timeout_ms / 3`) — the plane
    /// pool and probe service derive from it.
    probe_timeout_ms: u64,
    /// B6: the configured minimum ring node count the rejoin loop waits
    /// for (review #66/#69).
    min_quorum_nodes: u64,
    /// The ready-gate bound (`cluster_ready_timeout_sec`).
    ready_timeout_secs: u64,
    /// Perf 4.3 socket options applied to accepted plane connections.
    quickack: bool,
    busy_poll: u32,
    /// The persisted fallback seeds the join retries with (ADR-0022 D3).
    fallback_seeds: Vec<String>,
}

impl MembershipModule {
    /// Builds the membership-plane bundle.
    ///
    /// Owns the construction previously inline in `Node::start()` §4
    /// (membership + strict address parsing — review #64: an unparseable
    /// `membership_listen_addr` is now a hard startup error instead of a
    /// silent `0.0.0.0:9002` fallback), §4a (rejoin state load +
    /// incarnation bump + write-through persist), §4b (peer manifest
    /// cache) and §5's membership-plane pool.
    ///
    /// # Parameters
    ///
    /// `config` is the validated node config; `ring_cache` is the §3
    /// routing cache the membership state machine mirrors.
    ///
    /// # Errors
    ///
    /// Returns an error when either listen address does not parse, the
    /// persisted membership state cannot be loaded, or the incarnation
    /// bump cannot be persisted.
    pub(crate) fn build(
        config: &NodeConfig,
        ring_cache: Arc<oceanfs_routing::RingCache>,
    ) -> Result<Self, String> {
        let node_id = NodeId::new(&config.node_id);

        // Strict address parses (review #64 — no silent default network
        // addresses; B2 already made the gRPC address strict, the
        // membership address had a silent 0.0.0.0:9002 fallback).
        let grpc_addr: SocketAddr = config
            .grpc_listen_addr
            .parse()
            .map_err(|e| format!("invalid grpc_listen_addr: {e}"))?;
        let membership_addr: SocketAddr = config
            .membership_listen_addr
            .parse()
            .map_err(|e| format!("invalid membership_listen_addr: {e}"))?;
        // ADR-0028 D1: the announced membership address is the membership
        // plane's listen address with the data-plane's advertised IP
        // substituted for 0.0.0.0 (the gRPC address is already the
        // reachable IP — the deploy scripts write the node's IP there).
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
            node_id.clone(),
            membership_announce_addr,
            grpc_addr,
            gossip_config,
            ring_cache,
        ));

        // ---- Rejoin state (ADR-0022) ----
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

        // ---- Peer-side routing cache (ADR-0029 §D5) ----
        // The per-peer NodeManifest cache consulted as a routing hint by
        // the read/write coordinators (lock-free ArcSwap reads on the
        // hot path; populated from membership events and seeded with the
        // self manifest at join). Phase A: every manifest is Healthy, so
        // the exclusion filters are observationally neutral — the
        // structure and metrics land for Phase B.
        let manifest_cache = Arc::new(ManifestCache::new());

        // The membership plane's dedicated pool (ADR-0028 D1: per-peer 2,
        // probe-derived timeouts) so probe/gossip latency is never
        // coupled to the data plane's channel semaphore. Rpc config is
        // not plumbed yet — both planes read the same defaults (see the
        // [review][config][high] marker in modules/data_plane.rs).
        let plane_cfg = RpcConfig::default();
        let probe_timeout_ms = config.gossip.failure_timeout_ms / 3;
        let membership_pool = oceanfs_membership::plane::membership_pool(
            probe_timeout_ms,
            plane_cfg.tls_cert_path.clone(),
        );
        membership.set_pool(membership_pool.clone());

        // B6 (review #66/#69): a cluster node has configured seeds or
        // persisted fallback seeds. Derived from the ALREADY-loaded
        // durable state — nothing writes the store between this load and
        // the ready gate (§11) that consumes the flag.
        let is_cluster_node =
            !config.gossip.seed_nodes.is_empty() || !durable_state.fallback_seeds.is_empty();

        // The cluster-readiness gate. Single-node deployments never
        // close it via the loop — it is open from boot.
        let ready_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if !is_cluster_node {
            ready_gate.store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(MembershipModule {
            membership,
            manifest_cache,
            membership_state_store,
            announce_incarnation,
            is_cluster_node,
            ready_gate,
            grpc_addr,
            membership_pool,
            node_id,
            membership_addr,
            probe_timeout_ms,
            min_quorum_nodes: config.cluster_min_quorum_nodes,
            ready_timeout_secs: config.cluster_ready_timeout_sec,
            quickack: plane_cfg.quickack,
            busy_poll: plane_cfg.busy_poll_us,
            fallback_seeds: durable_state.fallback_seeds,
        })
    }

    /// Binds the membership plane and bootstraps the node into the
    /// cluster (§15b–§15e).
    ///
    /// Owns the sequence previously inline in `Node::start()` §15b–§15e:
    /// the gossip/probe service construction (re-seated from the c3
    /// server module — they wrap only membership-plane inputs), the
    /// plane listener bind + serve spawn (ADR-0028 D1),
    /// `membership.start()` + the gossip metrics registration (which
    /// must stay AFTER start — the protocol + counters are created
    /// inside it), the storage-pool manifest declaration + routing-cache
    /// self-seed (ADR-0029 D2), the routing-cache event subscriber
    /// (§D5), the initial join + the background rejoin loop, and the
    /// post-join fallback-seed snapshot.
    ///
    /// MUST be called after [`crate::modules::data_plane::DataPlaneModule::serve`]:
    /// peers probe and deliver hinted handoffs to our gRPC listener
    /// immediately after the join announcement, and a join that precedes
    /// the data-plane bind produces join-time false Suspects and refused
    /// hint deliveries (t5/t21).
    ///
    /// # Parameters
    ///
    /// `metrics` is the node's central registry (the gossip series
    /// register here); `registry` is the c1 storage-pool registry the
    /// self manifest is built from.
    ///
    /// # Returns
    ///
    /// The spawned membership-plane task handles ([`MembershipPlane`]):
    /// the gossip/probe gRPC serve task, the routing-cache subscriber
    /// and (for cluster nodes) the background rejoin loop. Every task
    /// exits on the membership shutdown token
    /// ([`Membership::shutdown_token`]) — the node cancels that token
    /// during shutdown and awaits these handles so the plane's fixed
    /// listener is released before a same-address restart.
    ///
    /// # Errors
    ///
    /// Returns an error when the membership-plane listener cannot bind
    /// or `membership.start()` fails. A failed initial join is NOT an
    /// error — it is warned and retried in the background (the
    /// cluster-readiness gate keeps writes refused until the ring
    /// converges).
    pub(crate) async fn start_plane_and_join(
        &self,
        metrics: Arc<oceanfs_server::admin::MetricsRegistry>,
        registry: &oceanfs_storage::PoolRegistry,
    ) -> Result<MembershipPlane, String> {
        // The membership services (gossip + probe) are constructed here
        // with the plane's dedicated pool: the data-plane server hosts
        // only Segment/Healing/Cache/Scrub (ADR-0028 D1).
        let gossip_service = oceanfs_membership::grpc::gossip_service::GossipGrpcService::new(
            self.membership.clone(),
        );
        let probe_service = oceanfs_membership::grpc::probe_service::ProbeGrpcService::new(
            self.node_id.clone(),
            self.membership.clone(),
            self.membership_pool.clone(),
            self.probe_timeout_ms,
        );

        // A separate listener on membership_listen_addr hosting ONLY the
        // membership services: GossipRpc (push/pull) + ProbeRpc (SWIM).
        // Isolation from the data plane is the point — probe latency must
        // not inherit the data plane's tail (16 MiB streams, hint
        // batches).
        let membership_router = tonic::transport::Server::builder()
            .add_service(oceanfs_network::GossipRpcServer::new(gossip_service))
            .add_service(oceanfs_network::gossip::probe_rpc_server::ProbeRpcServer::new(
                probe_service,
            ));

        let membership_listener =
            match oceanfs_network::create_reuseport_listener(self.membership_addr) {
                Ok(l) => l,
                Err(e) => {
                    error!(
                        "membership plane listener creation failed for {}: {e}",
                        self.membership_addr
                    );
                    return Err(format!(
                        "membership plane listener creation failed for {}: {e}",
                        self.membership_addr
                    ));
                }
            };

        let quickack = self.quickack;
        let busy_poll = self.busy_poll;
        // The plane's tasks all stop on the membership shutdown token:
        // `Node::shutdown` cancels it and awaits the returned handles,
        // so the plane's listener is released before a same-address
        // restart (the boot-variant e2e restarts a node in-process).
        let plane_shutdown = self.membership.shutdown_token();
        let plane_grpc_handle = tokio::spawn(async move {
            // Same socket treatment as the data plane (perf 4.3):
            // quickack + busy-poll on accepted membership connections —
            // probe latency is the detection bound.
            use std::os::unix::io::AsRawFd;

            use tokio_stream::StreamExt;

            let stream = tokio_stream::wrappers::TcpListenerStream::new(membership_listener).map(
                move |conn| {
                    if let Ok(ref stream) = conn {
                        oceanfs_network::apply_opts_to_fd(stream.as_raw_fd(), quickack, busy_poll);
                    }
                    conn
                },
            );
            if let Err(e) = membership_router
                .serve_with_incoming_shutdown(stream, plane_shutdown.cancelled_owned())
                .await
            {
                error!("membership plane server error: {e}");
            }
        });

        // Start failure detection + gossip, then join the ring. MUST
        // happen after the data-plane gRPC server is bound (see the
        // method docs).
        self.membership.start().map_err(|e| format!("failed to start membership: {e}"))?;
        // Register the gossip metrics AFTER start(): the gossip
        // protocol + its counters/histograms are created inside
        // start() — an earlier registration captured None and the
        // gossip series never appeared (the timing-metrics run
        // queried an empty metric).
        self.membership.register_membership_metrics(&*metrics);

        // Declare the storage-pool manifest (ADR-0029 D2). Built once
        // from the registry with the announce incarnation and attached to
        // the self membership entry: the version bump the manifest
        // triggers is all the gossip plane needs to propagate it (a pool
        // change is not a restart — the incarnation is untouched). Phase
        // A registers at boot only; f8 (runtime-attach) re-declares on
        // pool set changes (the c3 server module's attach closure). The
        // join() below carries the manifest in its self-announcement, so
        // seeds learn it immediately.
        let node_manifest =
            crate::pool_manifest::build_node_manifest(self.announce_incarnation, registry);
        self.membership.set_self_manifest(node_manifest.clone());
        // Seed the routing cache with the self manifest so the node's
        // own pool state is visible to the exclusion filters (and the
        // peers' caches converge to include it via gossip).
        self.manifest_cache.update(self.node_id.clone(), Arc::new(node_manifest));

        // Routing-cache event subscriber (ADR-0029 §D5): populates the
        // per-peer manifest cache from membership events — version-bumped
        // entries carry the manifest (f6), Dead/Left members are evicted.
        // The cache is a hint — a stale-but-present manifest beats
        // absent, and the error path is the guarantee.
        let cache_events = self.membership.subscribe();
        let cache_for_events = Arc::clone(&self.manifest_cache);
        let cache_shutdown = self.membership.shutdown_token();
        let subscriber_handle = tokio::spawn(async move {
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

        let join_incarnation = oceanfs_core::Incarnation::new(self.announce_incarnation);
        let join_fallback_seeds = self.fallback_seeds.clone();
        if let Err(e) = self.membership.join(join_incarnation, &join_fallback_seeds).await {
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
        // boot before their seed comes up. Exits once joined (or when
        // the membership shutdown token fires).
        let rejoin_handle: Option<JoinHandle<()>> = if self.is_cluster_node {
            let retry_membership = Arc::clone(&self.membership);
            let retry_incarnation = join_incarnation;
            let retry_fallback = join_fallback_seeds.clone();
            let min_quorum_nodes = self.min_quorum_nodes;
            let rejoin_shutdown = self.membership.shutdown_token();
            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
                loop {
                    tokio::select! {
                        _ = rejoin_shutdown.cancelled() => {
                            tracing::debug!("background rejoin loop cancelled");
                            return;
                        }
                        _ = interval.tick() => {}
                    }
                    let ring_nodes = retry_membership.ring().snapshot().node_count();
                    if cluster_ready_gate_opens(ring_nodes, min_quorum_nodes, false) {
                        return;
                    }
                    if let Err(e) = retry_membership.join(retry_incarnation, &retry_fallback).await
                    {
                        tracing::debug!(error = %e, "rejoin retry failed");
                    }
                }
            }))
        } else {
            None
        };

        // After a successful join, snapshot the known member addresses as
        // fallback seeds. Events emitted during join are missed by the
        // subscriber spawned above (broadcast channels do not replay), so
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
            let seeds: Vec<String> = self
                .membership
                .nodes_full()
                .iter()
                .filter(|(id, _, _, _, _, _, _, _)| *id != self.node_id)
                .map(|(_, _, _, _, membership_addr, _, _, _)| membership_addr.to_string())
                .collect();
            if seeds.is_empty() {
                tracing::debug!(
                    node_id = %self.node_id,
                    "join learned no peers — keeping the persisted fallback seeds"
                );
            } else if let Err(e) = self.membership_state_store.save_fallback_seeds(&seeds) {
                warn!(error = %e, "failed to persist fallback seeds after join");
            }
        }

        Ok(MembershipPlane { grpc_handle: plane_grpc_handle, subscriber_handle, rejoin_handle })
    }
}

/// The spawned membership-plane task handles returned by
/// [`MembershipModule::start_plane_and_join`].
///
/// Every task exits on the membership shutdown token; `Node::shutdown`
/// cancels that token and awaits these handles (with an abort backstop)
/// so the plane's fixed listener is released before a same-address
/// restart.
pub(crate) struct MembershipPlane {
    /// The gossip/probe gRPC serve task handle.
    pub(crate) grpc_handle: JoinHandle<()>,
    /// The routing-cache event subscriber task handle.
    pub(crate) subscriber_handle: JoinHandle<()>,
    /// The background rejoin loop handle (`None` for single-node
    /// deployments — they never spawn the loop).
    pub(crate) rejoin_handle: Option<JoinHandle<()>>,
}

/// Whether the cluster-readiness gate opens for the given ring view
/// (B6, review #66/#69).
///
/// The gate opens when the ring holds at least the configured minimum
/// quorum node count (`cluster_min_quorum_nodes`) or when the
/// configured deadline has elapsed (the bound keeps a node whose seeds
/// are unreachable from stalling writes forever). Single-node
/// deployments never consult this — they skip the gate entirely.
///
/// Shared with `Node::start()`'s §11 ready-gate task and the module's
/// background rejoin loop (moved here by c4).
pub(crate) fn cluster_ready_gate_opens(
    ring_nodes: usize,
    min_quorum_nodes: u64,
    deadline_elapsed: bool,
) -> bool {
    ring_nodes as u64 >= min_quorum_nodes || deadline_elapsed
}

impl MembershipModule {
    /// Spawns the cluster-readiness gate loop (c5 — the gate is a
    /// membership-plane concern: it opens when the RING reaches the
    /// configured minimum quorum node count or the configured bound
    /// elapses). No-op for single-node deployments — their gate is
    /// already open (see [`ready_gate`](Self::ready_gate)). Returns the
    /// loop handle so shutdown can await it (the loop also exits on the
    /// membership shutdown token).
    pub(crate) fn spawn_ready_gate(&self) -> Option<JoinHandle<()>> {
        if !self.is_cluster_node {
            return None;
        }
        use tracing::info;
        let gate_membership = Arc::clone(&self.membership);
        let gate = Arc::clone(&self.ready_gate);
        let gate_timeout_secs = self.ready_timeout_secs.max(1);
        let min_quorum_nodes = self.min_quorum_nodes;
        let gate_shutdown = self.membership.shutdown_token();
        Some(tokio::spawn(async move {
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
                tokio::select! {
                    _ = gate_shutdown.cancelled() => return,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                }
                let ring_nodes = gate_membership.ring().snapshot().node_count();
                if cluster_ready_gate_opens(
                    ring_nodes,
                    min_quorum_nodes,
                    tokio::time::Instant::now() >= deadline,
                ) {
                    gate.store(true, std::sync::atomic::Ordering::Release);
                    break;
                }
            }
            info!("Cluster-readiness gate opened");
        }))
    }

    /// Registers the membership module's metric series (the peer
    /// routing cache — ADR-0029 §D5) with the node's central registry
    /// (c5 — replaces the §12 register line). The gossip series
    /// register separately inside `start_plane_and_join` (they must
    /// follow `membership.start()`).
    pub(crate) fn register_metrics(&self, registrar: &dyn oceanfs_core::MetricRegistrar) {
        self.manifest_cache.register_metrics(registrar);
    }
}
