//! Re-replication dispatch (g5 `re-replication-worker`, ADR-0030).
//!
//! Two node-side pieces:
//!
//! - [`ManifestRepairTargetSelector`] — the `RepairTargetSelector` impl
//!   over the manifest cache (f7): excludes candidates with
//!   `write_degraded` / no Healthy data pool and prefers the node with
//!   the most free data-pool capacity.
//! - [`RepairDispatcher`] — the `RepairSink` impl wired to g3's
//!   loss-announcement handler and g4's reconciliation loop. It
//!   filters the request's holders to LIVE holders, selects a target
//!   via the selector, and sends the `RequestReReplication` RPC to the
//!   acquiring node (ADR-0030 target-pull). Requests with no eligible
//!   target are **parked** (the honest cannot-reach-RF state, the
//!   backbone's `needs`-set pattern) and retried by the sweep.
//!
//! The dispatcher is the HOLDER side of the repair; the actual fetch +
//! write + stamp happens on the acquiring target's `ReRepWorker`
//! (oceanfs-durability::repair).

use std::{sync::Arc, time::Duration};

use oceanfs_core::{Counter, Gauge, LabelSet, MetricRegistrar, NodeId, NodeState, SegmentId};
use oceanfs_durability::{
    healing_rpc::{
        healing_rpc_client::HealingRpcClient, RepairReason as ProtoRepairReason,
        RequestReReplicationRequest,
    },
    healing_service::{ReRepRequest, RepairSink},
    RepairTargetSelector,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Per-RPC timeout for a re-replication dispatch.
pub(crate) const REPAIR_DISPATCH_TIMEOUT_MS: u64 = 2_000;

// ---------------------------------------------------------------------------
// ManifestRepairTargetSelector
// ---------------------------------------------------------------------------

/// `RepairTargetSelector` over the membership manifest cache (f7).
///
/// Filters candidates by manifest health — excludes the source holder
/// itself, nodes with `write_degraded` or no Healthy data pool — and
/// prefers the node with the most free data-pool capacity
/// (`capacity_free_bytes`). Ties break by node id (deterministic).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{GossipConfig, NodeId, RingConfig, SegmentId};
/// use oceanfs_durability::RepairTargetSelector;
/// use oceanfs_membership::Membership;
/// use oceanfs_node::repair::ManifestRepairTargetSelector;
/// use oceanfs_routing::{Ring, RingCache};
/// use std::sync::Arc;
///
/// let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
/// let membership = Arc::new(Membership::new(
///     NodeId::new("n1"), "127.0.0.1:9100".parse().unwrap(),
///     "127.0.0.1:9101".parse().unwrap(), GossipConfig::default(), ring,
/// ));
/// let selector = ManifestRepairTargetSelector::new(
///     membership,
///     NodeId::new("n1"),
/// );
/// // A membership with no manifests returns None (nothing eligible).
/// assert!(selector.pick_repair_target(&SegmentId::new(), &[NodeId::new("n2")]).is_none());
/// ```
pub struct ManifestRepairTargetSelector {
    membership: Arc<Membership>,
    self_id: NodeId,
}

impl ManifestRepairTargetSelector {
    /// Creates the selector over the node's membership/manifest view.
    pub fn new(membership: Arc<Membership>, self_id: NodeId) -> Self {
        Self { membership, self_id }
    }
}

impl RepairTargetSelector for ManifestRepairTargetSelector {
    fn pick_repair_target(&self, _source: &SegmentId, holders: &[NodeId]) -> Option<NodeId> {
        // Iterate the membership view once. A candidate is eligible when:
        // - alive (Alive | Suspect — Suspect is still servable);
        // - not self, not an existing holder;
        // - its manifest has at least one Healthy, non-write_degraded
        //   data pool (f7 — a node with no healthy data pool cannot
        //   hold a new copy).
        let holder_set: std::collections::HashSet<&NodeId> = holders.iter().collect();
        let mut best: Option<(u64, NodeId)> = None;

        for (node_id, state, _inc, _addr, _maddr, _v, _o, manifest) in self.membership.nodes_full()
        {
            if !matches!(state, NodeState::Alive | NodeState::Suspect) {
                continue;
            }
            if node_id == self.self_id || holder_set.contains(&node_id) {
                continue;
            }
            let Some(manifest) = manifest else { continue };
            let has_healthy_data_pool = manifest
                .pools()
                .iter()
                .any(|p| p.role() == "data" && p.status() == "healthy" && !p.write_degraded());
            if !has_healthy_data_pool {
                continue;
            }
            let capacity = manifest
                .pools()
                .iter()
                .filter(|p| p.role() == "data")
                .map(|p| p.capacity_free_bytes())
                .sum::<u64>();
            let replace = match &best {
                None => true,
                Some((best_capacity, best_id)) => {
                    capacity > *best_capacity || (capacity == *best_capacity && node_id < *best_id)
                }
            };
            if replace {
                best = Some((capacity, node_id));
            }
        }

        best.map(|(_, id)| id)
    }
}

// ---------------------------------------------------------------------------
// RepairDispatcher
// ---------------------------------------------------------------------------

/// Dispatch metrics (ADR-0029 §D6 observability).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{Counter, Gauge, LabelSet, MetricRegistrar};
/// use oceanfs_node::repair::RepairMetrics;
///
/// struct Registrar;
/// impl MetricRegistrar for Registrar {
///     fn register_counter(&self, _c: Counter) {}
///     fn register_gauge(&self, _g: Gauge) {}
///     fn register_histogram(&self, _h: std::sync::Arc<oceanfs_core::Histogram>) {}
/// }
///
/// let metrics = RepairMetrics::new();
/// metrics.register_metrics(&Registrar);
/// metrics.record_dispatch();
/// ```
#[derive(Debug, Clone)]
pub struct RepairMetrics {
    re_replicated_total: Counter,
    failures_total: Counter,
    queue_depth_announcement: Gauge,
    queue_depth_reconciliation: Gauge,
}

impl RepairMetrics {
    /// Creates unregistered metrics.
    pub fn new() -> Self {
        Self {
            re_replicated_total: Counter::new(
                "oceanfs_ranges_re_replicated_total".into(),
                "Re-replication repairs dispatched".into(),
                LabelSet::empty(),
            ),
            failures_total: Counter::new(
                "oceanfs_repair_failures_total".into(),
                "Re-replication dispatch failures".into(),
                LabelSet::empty(),
            ),
            // The queue-depth gauge carries the `{priority}` label
            // (ADR-0029 §D6 urgency — the feature doc's
            // `oceanfs_repair_queue_depth{priority}`): one series per
            // detector.
            queue_depth_announcement: Gauge::new(
                "oceanfs_repair_queue_depth".into(),
                "Re-replication repairs awaiting a target (announcement)".into(),
                LabelSet::new(&[("priority", "announcement")]),
            ),
            queue_depth_reconciliation: Gauge::new(
                "oceanfs_repair_queue_depth".into(),
                "Re-replication repairs awaiting a target (reconciliation)".into(),
                LabelSet::new(&[("priority", "reconciliation")]),
            ),
        }
    }

    /// Registers the metrics with a registrar.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.re_replicated_total.clone());
        registrar.register_counter(self.failures_total.clone());
        registrar.register_gauge(self.queue_depth_announcement.clone());
        registrar.register_gauge(self.queue_depth_reconciliation.clone());
    }

    /// Records one successful dispatch (the target accepted).
    pub fn record_dispatch(&self) {
        self.re_replicated_total.inc();
    }

    /// Records one dispatch failure.
    pub fn record_failure(&self) {
        self.failures_total.inc();
    }

    /// Updates the awaiting-target gauges — the parked set is counted
    /// per priority (a parked announcement is higher urgency than a
    /// parked reconciliation, ADR-0029 §D6).
    pub fn set_queue_depth(&self, parked: &dashmap::DashMap<SegmentId, ReRepRequest>) {
        let mut announcement = 0u64;
        let mut reconciliation = 0u64;
        for entry in parked.iter() {
            match entry.value().reason {
                oceanfs_durability::healing_service::RepairReason::Announcement => {
                    announcement += 1;
                }
                _ => reconciliation += 1,
            }
        }
        self.queue_depth_announcement.set(announcement);
        self.queue_depth_reconciliation.set(reconciliation);
    }
}

impl Default for RepairMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// The holder-side re-replication dispatcher (ADR-0030 target-pull).
///
/// Implements [`RepairSink`] — the same trait g3's `announce_loss`
/// handler and g4's reconciliation loop enqueue into. For each request:
///
/// 1. Filters the request's holders to LIVE holders (alive + not
///    data-dead).
/// 2. Selects a target via the injected [`RepairTargetSelector`].
/// 3. Sends `RequestReReplication` to the acquiring node.
///
/// A request with no eligible target (e.g. only the RF nodes remain
/// alive) is **parked** — the honest cannot-reach-RF state — and
/// retried by the sweep. This keeps the g3/g4 tests meaningful (the
/// holder has accepted the repair) while the actual copy lands on the
/// acquiring node.
pub struct RepairDispatcher {
    selector: Arc<dyn RepairTargetSelector>,
    pool: Arc<ConnectionPool>,
    membership: Arc<Membership>,
    /// The lifecycle coordinator — the dispatcher converges its OWN
    /// registry entry after a successful dispatch (ADR-0030 Decision 3:
    /// the holder records the acquiring target in `storage_locations`
    /// so its reconciliation loop stops re-dispatching).
    lifecycle: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
    /// Parked segments awaiting an eligible target.
    parked: dashmap::DashMap<SegmentId, ReRepRequest>,
    metrics: RepairMetrics,
}

impl RepairDispatcher {
    /// Creates the dispatcher with the injected target selector.
    pub fn new(
        selector: Arc<dyn RepairTargetSelector>,
        pool: Arc<ConnectionPool>,
        membership: Arc<Membership>,
        lifecycle: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
        _self_id: NodeId,
    ) -> Self {
        Self {
            selector,
            pool,
            membership,
            lifecycle,
            parked: dashmap::DashMap::new(),
            metrics: RepairMetrics::new(),
        }
    }

    /// Returns the number of parked repairs (awaiting a target).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_core::{GossipConfig, NodeId, RingConfig};
    /// use oceanfs_membership::Membership;
    /// use oceanfs_network::ConnectionPool;
    /// use oceanfs_node::repair::RepairDispatcher;
    /// use oceanfs_routing::{Ring, RingCache};
    ///
    /// let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
    /// let membership = Arc::new(Membership::new(
    ///     NodeId::new("n1"), "127.0.0.1:9100".parse().unwrap(),
    ///     "127.0.0.1:9101".parse().unwrap(), GossipConfig::default(), ring,
    /// ));
    /// let dispatcher = RepairDispatcher::new(
    ///     Arc::new(oceanfs_node::repair::ManifestRepairTargetSelector::new(
    ///         membership.clone(), NodeId::new("n1"),
    ///     )),
    ///     Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
    ///     membership,
    ///     Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
    ///         &oceanfs_core::LifecycleConfig::default(),
    ///     )),
    ///     NodeId::new("n1"),
    /// );
    /// assert_eq!(dispatcher.pending_len(), 0);
    /// ```
    pub fn pending_len(&self) -> usize {
        self.parked.len()
    }

    /// Returns `true` when `node`'s manifest reports it data-dead: it
    /// has data pools and every one is `dead`. A data-dead node cannot
    /// serve the fetch the acquiring worker will make, so it must not
    /// count as a live holder. A node with no manifest (unknown — the
    /// gossip view has not caught up) is NOT data-dead: excluding it
    /// could strand a repairable segment until the next sweep, while
    /// including it only costs a failed fetch attempt.
    fn is_data_dead(&self, node: &NodeId) -> bool {
        let Some(manifest) = self.membership.manifest_of(node) else {
            return false;
        };
        let data_pools: Vec<_> = manifest.pools().iter().filter(|p| p.role() == "data").collect();
        !data_pools.is_empty() && data_pools.iter().all(|p| p.status() == "dead")
    }

    /// Removes and returns one parked repair (the g5 observability /
    /// test drain). Returns `None` when nothing is parked.
    pub fn parked_remove_one(&self) -> Option<ReRepRequest> {
        let key = self.parked.iter().next().map(|e| *e.key())?;
        self.parked.remove(&key).map(|(_, req)| req)
    }

    /// Registers the dispatcher's metrics.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        self.metrics.register_metrics(registrar);
    }

    /// Retries the parked repairs in a bounded batch (the sweep). Each
    /// parked request is re-dispatched; a still-untargetable request
    /// stays parked for the next sweep.
    async fn sweep(&self) {
        if self.parked.is_empty() {
            return;
        }
        let requests: Vec<ReRepRequest> = self.parked.iter().map(|e| e.value().clone()).collect();
        for req in requests {
            let dispatched = self.try_dispatch(&req).await;
            if dispatched {
                self.parked.remove(&req.segment_id);
            }
            // Not dispatched → stays parked; the next sweep retries.
        }
        self.metrics.set_queue_depth(&self.parked);
    }

    /// Runs the dispatcher's retry sweep until shutdown.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use oceanfs_core::{GossipConfig, NodeId, RingConfig};
    /// # use oceanfs_membership::Membership;
    /// # use oceanfs_network::ConnectionPool;
    /// # use oceanfs_node::repair::RepairDispatcher;
    /// # use oceanfs_routing::{Ring, RingCache};
    /// # let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
    /// # let membership = Arc::new(Membership::new(
    /// #     NodeId::new("n1"), "127.0.0.1:9100".parse().unwrap(),
    /// #     "127.0.0.1:9101".parse().unwrap(), GossipConfig::default(), ring,
    /// # ));
    /// # let dispatcher = Arc::new(RepairDispatcher::new(
    /// #     Arc::new(oceanfs_node::repair::ManifestRepairTargetSelector::new(
    /// #         membership.clone(), NodeId::new("n1"),
    /// #     )),
    /// #     Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
    /// #     membership,
    /// #     Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
    /// #         &oceanfs_core::LifecycleConfig::default(),
    /// #     )),
    /// #     NodeId::new("n1"),
    /// # ));
    /// let shutdown = tokio_util::sync::CancellationToken::new();
    /// let token = shutdown.clone();
    /// let for_spawn = Arc::clone(&dispatcher);
    /// tokio::spawn(async move { for_spawn.run(token).await });
    /// shutdown.cancel();
    /// ```
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let mut sweep_interval = tokio::time::interval(Duration::from_secs(5));
        sweep_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        sweep_interval.tick().await; // first tick fires immediately — consume it
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("re-replication dispatcher shutting down");
                    break;
                }
                _ = sweep_interval.tick() => {
                    self.sweep().await;
                }
            }
        }
    }

    /// Attempts to dispatch one request to an acquiring node. Returns
    /// `true` when the target accepted (the repair will be executed
    /// there), `false` when no target is eligible or the RPC failed.
    async fn try_dispatch(&self, request: &ReRepRequest) -> bool {
        // Filter to LIVE holders (the request may carry a stale full
        // set — e.g. the dead origin is still listed). Live means:
        // node Alive/Suspect AND not data-dead (its manifest reports
        // every data pool Dead — it can no longer serve the fetch the
        // acquiring node will make). A node with a DEAD data pool but
        // an Alive node state stays in the holder set ONLY if it still
        // has a servable data pool; otherwise it is a location that
        // cannot serve bytes and must not count as a holder (the same
        // semantics reconcile.rs::membership_snapshot applies).
        let live_holders: Vec<NodeId> = request
            .holders
            .iter()
            .filter(|h| {
                let alive = matches!(
                    self.membership.state_of(h),
                    Some(NodeState::Alive | NodeState::Suspect)
                );
                alive && !self.is_data_dead(h)
            })
            .cloned()
            .collect();

        let Some(target) = self.selector.pick_repair_target(&request.segment_id, &live_holders)
        else {
            debug!(
                segment_id = %request.segment_id,
                holders = live_holders.len(),
                "re-replication: no eligible target; parked"
            );
            self.metrics.set_queue_depth(&self.parked);
            return false;
        };

        let addr = match self.membership.address_of(&target) {
            Some(a) => a,
            None => {
                warn!(target = %target, "re-replication: target has no address; parked");
                return false;
            }
        };
        let pooled = match self.pool.get_channel(addr).await {
            Ok(p) => p,
            Err(e) => {
                warn!(target = %target, error = %e, "re-replication: channel to target failed; parked");
                self.metrics.record_failure();
                return false;
            }
        };
        let channel = pooled.channel().clone();
        drop(pooled);

        let proto_sid: oceanfs_core::proto::common::SegmentId = request.segment_id.into();
        let proto_holders: Vec<oceanfs_core::proto::common::NodeId> =
            live_holders.iter().map(|n| n.clone().into()).collect();
        let proto_reason: i32 = match request.reason {
            oceanfs_durability::healing_service::RepairReason::Announcement => {
                ProtoRepairReason::Announcement as i32
            }
            oceanfs_durability::healing_service::RepairReason::Reconciliation => {
                ProtoRepairReason::Reconciliation as i32
            }
            // `#[non_exhaustive]` — future reasons degrade to the
            // reconciliation priority (a safety-net repair).
            _ => ProtoRepairReason::Reconciliation as i32,
        };
        let merkle_bytes: bytes::Bytes = request
            .merkle_root
            .map(|r| bytes::Bytes::copy_from_slice(r.as_bytes()))
            .unwrap_or_default();
        // The seal-time shape rides the request (ADR-0030): the
        // acquiring worker registers the pulled copy with the SOURCE's
        // tier/EC geometry. Tier encodes via the shared wire mapping
        // (the segment-push encoding; see
        // `segment_replicator::tier_to_u32`).
        let rpc_request = tonic::Request::new(RequestReReplicationRequest {
            segment_id: Some(proto_sid),
            holders: proto_holders,
            reason: proto_reason,
            merkle_root: merkle_bytes,
            tier: crate::segment_replicator::tier_to_u32(request.tier),
            ec_k: request.ec_k as u32,
            ec_m: request.ec_m as u32,
        });

        let mut client = HealingRpcClient::new(channel);
        let result = tokio::time::timeout(
            Duration::from_millis(REPAIR_DISPATCH_TIMEOUT_MS),
            client.request_re_replication(rpc_request),
        )
        .await;

        match result {
            Ok(Ok(response)) => {
                let accepted = response.into_inner().accepted;
                if accepted {
                    info!(
                        segment_id = %request.segment_id,
                        target = %target,
                        reason = ?request.reason,
                        "re-replication dispatched to acquiring node"
                    );
                    self.metrics.record_dispatch();
                    // ADR-0030 Decision 3: converge THIS holder's own
                    // registry entry — append the acquiring target to
                    // `storage_locations` through the durable refresh
                    // path so the g4 reconciler stops re-dispatching the
                    // same segment (its live-count now includes the new
                    // copy).
                    self.converge_holder_registry(request, &target).await;
                    true
                } else {
                    warn!(
                        segment_id = %request.segment_id,
                        target = %target,
                        "re-replication request not accepted; parked"
                    );
                    false
                }
            }
            Ok(Err(e)) => {
                warn!(segment_id = %request.segment_id, target = %target, error = %e, "re-replication dispatch failed; parked");
                self.metrics.record_failure();
                false
            }
            Err(_elapsed) => {
                warn!(segment_id = %request.segment_id, target = %target, "re-replication dispatch timed out; parked");
                self.metrics.record_failure();
                false
            }
        }
    }

    /// Converges THIS holder's registry entry after a successful
    /// dispatch (ADR-0030 Decision 3): appends the acquiring target to
    /// the segment's `storage_locations` through the durable refresh
    /// path, so the g4 reconciler's live-count includes the new copy
    /// and it stops re-dispatching the same segment.
    ///
    /// Best-effort: a stale/deleted entry is left untouched (the worker
    /// on the target stamps its OWN registry, so the copy exists
    /// regardless; this is the holder-side convergence only).
    async fn converge_holder_registry(&self, request: &ReRepRequest, target: &NodeId) {
        let Some(entry) = self.lifecycle.registry().get(request.segment_id) else {
            // The segment vanished (deleted between dispatch and here) —
            // nothing to converge.
            debug!(segment_id = %request.segment_id, "re-replication: holder registry entry gone; skip convergence");
            return;
        };
        // Already stamped (a duplicate dispatch raced us) — done.
        if entry.metadata.storage_locations.iter().any(|loc| loc == target) {
            return;
        }
        let mut locations = entry.metadata.storage_locations.clone();
        locations.push(target.clone());
        if let Err(e) = self
            .lifecycle
            .request_refresh_metadata(
                request.segment_id,
                entry.metadata.merkle_root,
                Some(locations),
            )
            .await
        {
            warn!(
                segment_id = %request.segment_id,
                target = %target,
                error = ?e,
                "re-replication: holder registry convergence failed; the g4 drift scan will re-converge"
            );
        }
    }
}

#[async_trait::async_trait]
impl RepairSink for RepairDispatcher {
    async fn enqueue(&self, request: ReRepRequest) -> Result<(), String> {
        // Try to dispatch immediately; park on failure. The enqueue
        // ALWAYS returns Ok — a parked request is a held obligation
        // (the g3/g4 tests observe it via `pending_repairs`), not a
        // delivery error.
        if !self.try_dispatch(&request).await {
            self.parked.insert(request.segment_id, request);
        }
        self.metrics.set_queue_depth(&self.parked);
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::{Incarnation, RingConfig};

    use super::*;

    fn make_membership(node_id: &str) -> Arc<Membership> {
        use oceanfs_routing::{Ring, RingCache};

        let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
        ring.add_node(NodeId::new(node_id));
        let ring = Arc::new(RingCache::new(ring));
        Arc::new(Membership::new(
            NodeId::new(node_id),
            "127.0.0.1:9100".parse().unwrap(),
            "127.0.0.1:9101".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring,
        ))
    }

    fn upsert(m: &Arc<Membership>, id: &str) {
        m.upsert_node(
            NodeId::new(id),
            NodeState::Alive,
            Incarnation::new(1),
            Some("127.0.0.1:9200".parse().unwrap()),
        );
    }

    fn request(segment_id: SegmentId, holders: Vec<NodeId>) -> ReRepRequest {
        ReRepRequest {
            origin: NodeId::new("origin"),
            segment_id,
            holders,
            reason: oceanfs_durability::healing_service::RepairReason::Reconciliation,
            retry_count: 0,
            merkle_root: None,
            tier: oceanfs_core::SizeTier::Standard,
            ec_k: 1,
            ec_m: 0,
        }
    }

    /// A selector that always returns the first NON-holder (deterministic
    /// test stub): picks the lexicographically-smallest candidate.
    #[derive(Clone)]
    struct SmallestId;

    impl RepairTargetSelector for SmallestId {
        fn pick_repair_target(&self, _source: &SegmentId, _holders: &[NodeId]) -> Option<NodeId> {
            // The dispatcher passes live holders; this stub needs
            // candidate discovery — we return None unless the test wires
            // a concrete target through the holder list semantics below.
            None
        }
    }

    /// The manifest selector excludes candidates with no healthy data
    /// pool manifest and prefers most free capacity.
    #[test]
    fn manifest_selector_prefers_most_free_capacity() {
        use oceanfs_membership::manifest::{NodeManifest, PoolManifest};

        let membership = make_membership("n1");
        upsert(&membership, "n2");
        upsert(&membership, "n3");
        // Both peers get manifests: n2 has 100 GiB free, n3 has 200 GiB.
        membership.set_peer_manifest(
            NodeId::new("n2"),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "data", "healthy", false, 100 << 30, 1)],
            ),
        );
        membership.set_peer_manifest(
            NodeId::new("n3"),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "data", "healthy", false, 200 << 30, 1)],
            ),
        );

        let selector = ManifestRepairTargetSelector::new(membership, NodeId::new("n1"));
        // Both are eligible (neither is a holder); n3 has more capacity.
        let pick = selector.pick_repair_target(&SegmentId::new(), &[NodeId::new("n4")]);
        assert_eq!(pick, Some(NodeId::new("n3")), "most free capacity wins");
    }

    /// The manifest selector excludes write_degraded / no-Healthy-pool
    /// candidates.
    #[test]
    fn manifest_selector_excludes_degraded_nodes() {
        use oceanfs_membership::manifest::{NodeManifest, PoolManifest};

        let membership = make_membership("n1");
        upsert(&membership, "n2");
        upsert(&membership, "n3");
        // n2: write_degraded → excluded. n3: no data pool → excluded.
        membership.set_peer_manifest(
            NodeId::new("n2"),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "data", "healthy", true, 999 << 30, 1)],
            ),
        );
        membership.set_peer_manifest(
            NodeId::new("n3"),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "wal", "healthy", false, 999 << 30, 1)],
            ),
        );

        let selector = ManifestRepairTargetSelector::new(membership, NodeId::new("n1"));
        assert_eq!(
            selector.pick_repair_target(&SegmentId::new(), &[NodeId::new("n4")]),
            None,
            "write_degraded + no-data-pool nodes are ineligible"
        );
    }

    /// The manifest selector never returns an existing holder or self.
    #[test]
    fn manifest_selector_excludes_holders_and_self() {
        use oceanfs_membership::manifest::{NodeManifest, PoolManifest};

        let membership = make_membership("n1");
        upsert(&membership, "n2");
        upsert(&membership, "n3");
        membership.set_peer_manifest(
            NodeId::new("n2"),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "data", "healthy", false, 100 << 30, 1)],
            ),
        );
        membership.set_peer_manifest(
            NodeId::new("n3"),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "data", "healthy", false, 200 << 30, 1)],
            ),
        );

        let selector = ManifestRepairTargetSelector::new(membership, NodeId::new("n1"));
        // n2 and n3 are both holders → nothing left.
        assert_eq!(
            selector.pick_repair_target(&SegmentId::new(), &[NodeId::new("n2"), NodeId::new("n3")]),
            None
        );
    }

    /// The dispatcher parks a request with no eligible target and
    /// reports it via `pending_len()`.
    #[tokio::test]
    async fn dispatcher_parks_no_target_request() {
        let membership = make_membership("n1");
        // No peers at all → no target.
        let dispatcher = RepairDispatcher::new(
            Arc::new(SmallestId),
            Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            membership,
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            NodeId::new("n1"),
        );
        let req = request(SegmentId::new(), vec![NodeId::new("n2")]);
        dispatcher.enqueue(req).await.unwrap();
        assert_eq!(dispatcher.pending_len(), 1, "no-target request is parked");
    }

    /// The dispatcher's enqueue always succeeds (a parked request is an
    /// accepted obligation, observable via `pending_len`).
    #[tokio::test]
    async fn dispatcher_enqueue_always_ok() {
        let membership = make_membership("n1");
        let dispatcher = RepairDispatcher::new(
            Arc::new(SmallestId),
            Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            membership,
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            NodeId::new("n1"),
        );
        assert!(dispatcher.enqueue(request(SegmentId::new(), vec![])).await.is_ok());
    }

    /// The dispatcher's holder filter treats a node whose manifest
    /// reports every data pool Dead as NOT a live holder (it cannot
    /// serve the acquiring node's fetch), while an unknown node (no
    /// manifest yet) stays eligible — excluding it could strand a
    /// repairable segment until the next sweep.
    #[test]
    fn data_dead_semantics_match_reconciler_snapshot() {
        use oceanfs_membership::manifest::{NodeManifest, PoolManifest};

        let membership = make_membership("n1");
        upsert(&membership, "n2");
        let dispatcher = RepairDispatcher::new(
            Arc::new(SmallestId),
            Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            membership.clone(),
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            NodeId::new("n1"),
        );

        // No manifest → not data-dead (the gossip view has not caught
        // up; the node may still serve).
        let n2 = NodeId::new("n2");
        assert!(!dispatcher.is_data_dead(&n2), "unknown node is not data-dead");

        // A manifest with a Healthy data pool → servable.
        membership.set_peer_manifest(
            n2.clone(),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "data", "healthy", false, 100 << 30, 1)],
            ),
        );
        assert!(!dispatcher.is_data_dead(&n2), "healthy data pool is servable");

        // All data pools Dead → data-dead (the pool-loss case: the node
        // is still Alive in membership but its bytes are gone).
        membership.set_peer_manifest(
            n2.clone(),
            NodeManifest::from_pools(1, &[PoolManifest::new(0, "data", "dead", false, 0, 1)]),
        );
        assert!(dispatcher.is_data_dead(&n2), "all data pools dead = data-dead");

        // No data pools at all (metadata/wal-only node) → not a data
        // holder, therefore not "data-dead" in the holder sense (it
        // would simply never appear in a holder set).
        membership.set_peer_manifest(
            n2.clone(),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "wal", "healthy", false, 10 << 30, 1)],
            ),
        );
        assert!(!dispatcher.is_data_dead(&n2), "wal-only node is not data-dead");
    }
}
