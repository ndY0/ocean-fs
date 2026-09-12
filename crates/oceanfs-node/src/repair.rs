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

use std::{collections::VecDeque, sync::Arc, time::Duration};

use oceanfs_core::{Counter, Gauge, LabelSet, MetricRegistrar, NodeId, NodeState, SegmentId};
use oceanfs_durability::{
    healing_rpc::{
        healing_rpc_client::HealingRpcClient, RepairReason as ProtoRepairReason,
        RequestReReplicationRequest,
    },
    healing_service::{ReRepRequest, RepairReason, RepairSink},
    hinted_handoff::{HintDropRecord, HintDropSink},
    RepairTargetSelector,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_server::CandidateClass;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Outcome classification of one [`RequestReReplication`] RPC (shared by
/// the repair `try_dispatch` and the drain `dispatch_drain` paths).
enum DispatchOutcome {
    /// The target accepted (repair: queued; drain: durable copy).
    Accepted,
    /// The target explicitly did not accept.
    NotAccepted,
    /// The target has no routable address.
    NoAddress,
    /// The channel / RPC transport failed.
    Channel,
    /// The target returned an RPC error.
    Rpc,
    /// The RPC timed out.
    TimedOut,
}

/// Per-RPC timeout for a re-replication dispatch.
pub(crate) const REPAIR_DISPATCH_TIMEOUT_MS: u64 = 2_000;

/// Per-RPC timeout for a **drain** re-replication dispatch (d4,
/// ADR-0036 D4). A drain dispatch must return only after the target's
/// copy is durable (the target handler polls its registry up to
/// `DRAIN_MATERIALIZE_TIMEOUT`); this client-side bound sits above that
/// deadline (150 s vs 120 s) so a slow-but-progressing copy is not
/// abandoned by the RPC.
pub(crate) const DRAIN_DISPATCH_TIMEOUT_MS: u64 = 150_000;

/// Failure taxonomy for a d4 cluster-drain dispatch
/// ([`RepairDispatcher::dispatch_drain`]).
///
/// Unlike a repair dispatch (which parks and is retried by the sweep), a
/// drain dispatch is a synchronous, per-segment operation whose outcome
/// the drain controller acts on: no eligible target ⇒ the source pool is
/// parked with a blocked reason; a target that did not materialize the
/// copy within the timeout is NOT released.
///
/// # Examples
///
/// ```
/// use oceanfs_node::repair::DrainDispatchError;
///
/// let err = DrainDispatchError::NoEligibleTarget;
/// assert_eq!(err.to_string(), "no eligible cluster target for the re-replication");
/// ```
#[derive(Debug, Clone, thiserror::Error)]
pub enum DrainDispatchError {
    /// The selector found no other node with a healthy data pool that is
    /// not already a holder (single-node cluster or no capacity).
    #[error("no eligible cluster target for the re-replication")]
    NoEligibleTarget,
    /// The chosen target has no routable address in the membership view.
    #[error("drain target {0} has no routable address")]
    NoAddress(NodeId),
    /// The channel to the target could not be established.
    #[error("drain channel to {target} failed: {detail}")]
    Channel {
        /// The target node.
        target: NodeId,
        /// The channel error text.
        detail: String,
    },
    /// The target accepted the request but reported it could not
    /// materialize a durable copy (its worker queue rejected the
    /// request or the copy did not become durable in time).
    #[error("drain target {0} did not confirm a durable copy")]
    NotDurable(NodeId),
    /// The re-replication RPC to the target failed.
    #[error("drain re-replication RPC to {0} failed: {1}")]
    Rpc(NodeId, String),
    /// The RPC timed out waiting for the target's durable confirmation.
    #[error("drain re-replication to {0} timed out waiting for a durable copy")]
    TimedOut(NodeId),
}

// ---------------------------------------------------------------------------
// ManifestRepairTargetSelector
// ---------------------------------------------------------------------------

/// `RepairTargetSelector` over the membership manifest cache (f7, f5).
///
/// Filters candidates by manifest class — excludes the source holder
/// itself, hard-excluded nodes (`write_degraded`, `node_unavailable`, no
/// writable data pool) — and prefers **Healthy** destinations over
/// Degraded fallbacks. Within a tier it prefers the node with the most
/// free data-pool capacity (`capacity_free_bytes`); ties break by node id
/// (deterministic). Degraded-fallback targets let repair land while the
/// fleet is degraded (f5 D2): over-replication is a lesser problem than
/// under-replication.
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
        // - NOT node_unavailable (g8: a metadata-dead node cannot
        //   persist the new copy's object row);
        // - written-class Preferred (≥1 Healthy, non-write_degraded data
        //   pool) or Fallback (Degraded data pool; f5 D2 — repair must be
        //   able to land while the fleet is degraded).
        // Preferred destinations always win over fallbacks; capacity breaks
        // ties inside a tier.
        let holder_set: std::collections::HashSet<&NodeId> = holders.iter().collect();
        let mut best: Option<(CandidateClass, u64, NodeId)> = None;

        for (node_id, state, _inc, _addr, _maddr, _v, _o, manifest) in self.membership.nodes_full()
        {
            if !matches!(state, NodeState::Alive | NodeState::Suspect) {
                continue;
            }
            if node_id == self.self_id || holder_set.contains(&node_id) {
                continue;
            }
            let Some(manifest) = manifest else { continue };
            let class = crate::routing_cache::manifest_write_class(&manifest);
            if class == CandidateClass::Excluded {
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
                Some((best_class, best_capacity, best_id)) => {
                    class_rank(class) < class_rank(*best_class)
                        || (class == *best_class
                            && (capacity > *best_capacity
                                || (capacity == *best_capacity && node_id < *best_id)))
                }
            };
            if replace {
                best = Some((class, capacity, node_id));
            }
        }

        best.map(|(_, _, id)| id)
    }
}

/// Orders the manifest classes for repair-target preference (lower wins).
fn class_rank(class: CandidateClass) -> u8 {
    match class {
        CandidateClass::Preferred => 0,
        CandidateClass::Fallback => 1,
        // Non-exhaustive: unknown classes rank last.
        _ => 2,
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
    /// ae1 S1: terminal no-live-holder classifications (once per
    /// segment; a resumed + re-classified segment counts again).
    unrecoverable_total: Counter,
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
            unrecoverable_total: Counter::new(
                "oceanfs_repair_unrecoverable_total".into(),
                "Repairs classified terminal: no live recorded holder".into(),
                LabelSet::new(&[("reason", "no_live_holder")]),
            ),
        }
    }

    /// Registers the metrics with a registrar.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.re_replicated_total.clone());
        registrar.register_counter(self.failures_total.clone());
        registrar.register_gauge(self.queue_depth_announcement.clone());
        registrar.register_gauge(self.queue_depth_reconciliation.clone());
        registrar.register_counter(self.unrecoverable_total.clone());
    }

    /// Records one terminal no-live-holder classification (ae1).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_node::repair::RepairMetrics;
    ///
    /// let metrics = RepairMetrics::new();
    /// metrics.record_unrecoverable();
    /// assert_eq!(metrics.unrecoverable_total_for_test(), 1);
    /// ```
    pub fn record_unrecoverable(&self) {
        self.unrecoverable_total.inc();
    }

    /// Returns the terminal-classification count (for tests).
    #[doc(hidden)]
    pub fn unrecoverable_total_for_test(&self) -> u64 {
        self.unrecoverable_total.get()
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

/// ae1 S1: hard cap on the terminal no-live-holder set (ADR-0034
/// bounded discipline). Oldest classifications are evicted first.
///
/// # Examples
///
/// ```
/// use oceanfs_node::repair::UNRECOVERABLE_SET_CAPACITY;
///
/// assert_eq!(UNRECOVERABLE_SET_CAPACITY, 10_000);
/// ```
pub const UNRECOVERABLE_SET_CAPACITY: usize = 10_000;

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
///
/// ae1 S1: a request whose recorded holder set has **no live member at
/// all** cannot ever be served. After `unrecoverable_sweeps` consecutive
/// sweeps it is classified terminal (see [`Self::unrecoverable_len`]),
/// counted once in `oceanfs_repair_unrecoverable_total`, and kept in a
/// bounded set so it is not re-enqueued; a recorded holder returning
/// clears the marker and resumes normal repair.
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
    /// ae1 S1: consecutive-sweep no-live-holder streaks per parked
    /// segment (reset by a live-holder dispatch).
    unrecoverable_misses: dashmap::DashMap<SegmentId, u32>,
    /// ae1 S1: terminal no-live-holder classifications. The request is
    /// retained so the clear/resume re-check can re-park it.
    unrecoverable: dashmap::DashMap<SegmentId, ReRepRequest>,
    /// ae1 S1: FIFO insertion order for bounded eviction of
    /// `unrecoverable` (ADR-0034 bounded discipline).
    unrecoverable_order: parking_lot::Mutex<VecDeque<SegmentId>>,
    /// ae1 S1: consecutive no-live-holder sweeps before terminal.
    unrecoverable_sweeps: u32,
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
            unrecoverable_misses: dashmap::DashMap::new(),
            unrecoverable: dashmap::DashMap::with_capacity(UNRECOVERABLE_SET_CAPACITY),
            unrecoverable_order: parking_lot::Mutex::new(VecDeque::with_capacity(
                UNRECOVERABLE_SET_CAPACITY,
            )),
            // Overridden by `with_unrecoverable_sweeps` at wiring time
            // (the config default is 3; see `DurabilityConfig`).
            unrecoverable_sweeps: 3,
            metrics: RepairMetrics::new(),
        }
    }

    /// ae1 S1: sets the consecutive-sweep bound before a parked repair
    /// with no live recorded holder becomes terminal. Clamped to at
    /// least 1 (a terminal classification must never be immediate).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Requires a constructed dispatcher; see the integration test
    /// // `tests/repair_unrecoverable.rs`.
    /// let dispatcher = RepairDispatcher::new(selector, pool, membership, lifecycle, self_id)
    ///     .with_unrecoverable_sweeps(3);
    /// ```
    #[must_use]
    pub fn with_unrecoverable_sweeps(mut self, sweeps: u32) -> Self {
        self.unrecoverable_sweeps = sweeps.max(1);
        self
    }

    /// ae1 S1: the number of segments currently classified unrecoverable
    /// (terminal: no live recorded holder).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_core::{GossipConfig, LifecycleConfig, NodeId, RingConfig};
    /// use oceanfs_membership::Membership;
    /// use oceanfs_network::ConnectionPool;
    /// use oceanfs_node::repair::RepairDispatcher;
    /// use oceanfs_routing::{Ring, RingCache};
    /// use oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator;
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
    ///     Arc::new(SegmentLifecycleCoordinator::new(&LifecycleConfig::default())),
    ///     NodeId::new("n1"),
    /// );
    /// assert_eq!(dispatcher.unrecoverable_len(), 0);
    /// ```
    pub fn unrecoverable_len(&self) -> usize {
        self.unrecoverable.len()
    }

    /// ae1 S1: whether `segment_id` is currently classified unrecoverable.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Requires a constructed dispatcher (see the integration test
    /// // `tests/repair_unrecoverable.rs`).
    /// assert!(!dispatcher.is_unrecoverable(&segment_id));
    /// ```
    pub fn is_unrecoverable(&self, segment_id: &SegmentId) -> bool {
        self.unrecoverable.contains_key(segment_id)
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

    /// Retries the parked repairs in a bounded batch (the sweep) and
    /// applies the ae1 S1 unrecoverable rule.
    ///
    /// A parked request whose recorded holder set has **no live member**
    /// increments its consecutive-miss streak; after
    /// `unrecoverable_sweeps` sweeps it becomes terminal (not
    /// re-enqueued, counted once). A request with live holders but no
    /// eligible target is the benign cannot-reach-RF state and stays
    /// parked indefinitely.
    async fn sweep(&self) {
        // ae1 S1: resume terminal repairs whose recorded holder set has a
        // live member again, BEFORE processing the parked set, so a
        // resumed request can dispatch on this same sweep.
        self.resume_recoverable();

        if !self.parked.is_empty() {
            let requests: Vec<ReRepRequest> =
                self.parked.iter().map(|e| e.value().clone()).collect();
            for req in requests {
                if self.live_holders_of(&req).is_empty() {
                    let misses = {
                        let mut entry =
                            self.unrecoverable_misses.entry(req.segment_id).or_insert(0);
                        *entry = entry.saturating_add(1);
                        *entry
                    };
                    if misses >= self.unrecoverable_sweeps {
                        self.classify_unrecoverable(req);
                    }
                    continue;
                }
                // A live holder exists: not an unrecoverable candidate;
                // reset the streak and dispatch normally.
                self.unrecoverable_misses.remove(&req.segment_id);
                if self.try_dispatch(&req).await {
                    self.parked.remove(&req.segment_id);
                }
                // Not dispatched → stays parked; the next sweep retries.
            }
        }
        self.metrics.set_queue_depth(&self.parked);
    }

    /// ae1 S1: clears terminal markers whose recorded holder set has a
    /// live member again and re-parks the request for normal dispatch.
    fn resume_recoverable(&self) {
        if self.unrecoverable.is_empty() {
            return;
        }
        let candidates: Vec<(SegmentId, ReRepRequest)> =
            self.unrecoverable.iter().map(|entry| (*entry.key(), entry.value().clone())).collect();
        for (segment_id, request) in candidates {
            if self.live_holders_of(&request).is_empty() {
                continue;
            }
            self.unrecoverable.remove(&segment_id);
            self.unrecoverable_order.lock().retain(|id| *id != segment_id);
            info!(
                segment_id = %segment_id,
                "unrecoverable repair resumed: a recorded holder is live again"
            );
            self.parked.insert(segment_id, request);
        }
    }

    /// ae1 S1: classifies a parked repair as terminal (no live recorded
    /// holder after N consecutive sweeps): stops re-enqueueing, counts it
    /// once, and keeps the request for the clear/resume re-check. The set
    /// is bounded (oldest evicted first) per ADR-0034.
    fn classify_unrecoverable(&self, request: ReRepRequest) {
        let segment_id = request.segment_id;
        self.parked.remove(&segment_id);
        self.unrecoverable_misses.remove(&segment_id);
        let newly = self.unrecoverable.insert(segment_id, request).is_none();
        if newly {
            let mut order = self.unrecoverable_order.lock();
            order.push_back(segment_id);
            while order.len() > UNRECOVERABLE_SET_CAPACITY {
                if let Some(evicted) = order.pop_front() {
                    self.unrecoverable.remove(&evicted);
                    debug!(
                        segment_id = %evicted,
                        "evicted oldest unrecoverable marker (bounded set)"
                    );
                }
            }
            drop(order);
            self.metrics.record_unrecoverable();
            warn!(
                segment_id = %segment_id,
                sweeps = self.unrecoverable_sweeps,
                "re-replication: no live recorded holder after consecutive sweeps; \
                 classified unrecoverable"
            );
        }
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
        let live_holders = self.live_holders_of(request);
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

        match self.dispatch_rpc(request, &target, &live_holders, REPAIR_DISPATCH_TIMEOUT_MS).await {
            DispatchOutcome::Accepted => {
                // ae1 S1: a live-holder dispatch ends any no-live-holder
                // streak for this segment.
                self.unrecoverable_misses.remove(&request.segment_id);
                true
            }
            DispatchOutcome::NotAccepted => {
                warn!(
                    segment_id = %request.segment_id,
                    target = %target,
                    "re-replication request not accepted; parked"
                );
                false
            }
            DispatchOutcome::NoAddress
            | DispatchOutcome::Channel
            | DispatchOutcome::Rpc
            | DispatchOutcome::TimedOut => {
                // The helper logged the specific reason (and recorded the
                // failure metric where the original path did); the caller
                // parks so the sweep retries.
                false
            }
        }
    }

    /// Dispatches one segment for the **d4 cluster drain** (ADR-0036
    /// D4): synchronously re-replicates to a non-holder target and
    /// returns only after the target confirms a **durable** copy.
    ///
    /// Reuses the repair target selection and RPC machinery: `holders`
    /// (the segment's current `storage_locations`) filters out nodes that
    /// already hold the segment, and the request rides `RepairReason::
    /// Drain` so the target's handler waits for its stamp before acking.
    /// On success the target is converged into THIS node's registry entry
    /// (ADR-0030 Decision 3), so a subsequent source-release refreshes to
    /// `holders − self` and drops this node cleanly.
    ///
    /// The drain controller paces calls to this method; failures do NOT
    /// park — the controller classifies them (blocked on no target,
    /// retry next cycle on a not-durable target).
    ///
    /// # Errors
    ///
    /// [`DrainDispatchError`]: no eligible target; an unroutable target;
    /// a channel/RPC failure; or a target that did not confirm a durable
    /// copy within the drain timeout.
    pub async fn dispatch_drain(
        &self,
        request: &ReRepRequest,
    ) -> Result<NodeId, DrainDispatchError> {
        let live_holders = self.live_holders_of(request);
        let target = self
            .selector
            .pick_repair_target(&request.segment_id, &live_holders)
            .ok_or(DrainDispatchError::NoEligibleTarget)?;

        match self.dispatch_rpc(request, &target, &live_holders, DRAIN_DISPATCH_TIMEOUT_MS).await {
            DispatchOutcome::Accepted => Ok(target),
            DispatchOutcome::NotAccepted => Err(DrainDispatchError::NotDurable(target)),
            DispatchOutcome::NoAddress => Err(DrainDispatchError::NoAddress(target)),
            DispatchOutcome::Channel => Err(DrainDispatchError::Channel {
                target,
                detail: "channel could not be established".into(),
            }),
            DispatchOutcome::Rpc => {
                Err(DrainDispatchError::Rpc(target, "re-replication RPC failed".into()))
            }
            DispatchOutcome::TimedOut => Err(DrainDispatchError::TimedOut(target)),
        }
    }

    /// The request's holder set filtered to LIVE holders (node
    /// Alive/Suspect AND not data-dead). A dead-or-unavailable node in a
    /// stale full set cannot serve the fetch the acquiring node will
    /// make and must not count as a holder.
    fn live_holders_of(&self, request: &ReRepRequest) -> Vec<NodeId> {
        request
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
            .collect()
    }

    /// Sends one `RequestReReplication` RPC to `target` and classifies
    /// the outcome. On `Accepted` the target has enqueued the repair
    /// (and, for a drain request, confirmed a durable copy) and THIS
    /// holder converges its own registry entry (ADR-0030 Decision 3).
    ///
    /// Metric recording mirrors the original inline path: `Accepted`
    /// counts a dispatch; channel/RPC/timeout failures count a failure;
    /// a missing address or a not-accepted ack count nothing.
    async fn dispatch_rpc(
        &self,
        request: &ReRepRequest,
        target: &NodeId,
        live_holders: &[NodeId],
        timeout_ms: u64,
    ) -> DispatchOutcome {
        let Some(addr) = self.membership.address_of(target) else {
            warn!(target = %target, "re-replication: target has no address; parked");
            return DispatchOutcome::NoAddress;
        };
        let pooled = match self.pool.get_channel(addr).await {
            Ok(p) => p,
            Err(e) => {
                warn!(target = %target, error = %e, "re-replication: channel to target failed; parked");
                self.metrics.record_failure();
                return DispatchOutcome::Channel;
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
            oceanfs_durability::healing_service::RepairReason::Drain => {
                ProtoRepairReason::Drain as i32
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
            Duration::from_millis(timeout_ms),
            client.request_re_replication(rpc_request),
        )
        .await;

        match result {
            Ok(Ok(response)) => {
                if response.into_inner().accepted {
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
                    self.converge_holder_registry(request, target).await;
                    DispatchOutcome::Accepted
                } else {
                    warn!(
                        segment_id = %request.segment_id,
                        target = %target,
                        "re-replication request not accepted; parked"
                    );
                    DispatchOutcome::NotAccepted
                }
            }
            Ok(Err(e)) => {
                warn!(segment_id = %request.segment_id, target = %target, error = %e, "re-replication dispatch failed; parked");
                self.metrics.record_failure();
                DispatchOutcome::Rpc
            }
            Err(_elapsed) => {
                warn!(segment_id = %request.segment_id, target = %target, "re-replication dispatch timed out; parked");
                self.metrics.record_failure();
                DispatchOutcome::TimedOut
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
                None, // no pool_id relocation (d2) on the holder-converge path
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
        // ae1 S1: a segment already classified unrecoverable is not
        // re-enqueued; the sweep's clear/resume re-check is the only way
        // out of the terminal state.
        if self.unrecoverable.contains_key(&request.segment_id) {
            debug!(
                segment_id = %request.segment_id,
                "re-replication: request for an unrecoverable segment ignored (terminal)"
            );
            return Ok(());
        }
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

/// f5 D3 bridge: converts exhausted hint debt into bounded ADR-0030
/// repair intent.
///
/// The hint manager calls this once per distinct dropped segment (within
/// a delivery cycle). The bridge resolves the segment's current holders
/// from the local lifecycle registry and spawns one
/// [`RepairDispatcher::enqueue`] per record — the dispatcher picks a
/// target (Healthy-preferred, Degraded-fallback) and the acquiring node
/// pulls the bytes. Segments this node does not hold are skipped: a
/// holder's own reconciliation owns them, and inventing a holder set
/// would violate the target-pull contract.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_core::{GossipConfig, NodeId, RingConfig};
/// use oceanfs_durability::hinted_handoff::HintDropSink;
/// use oceanfs_membership::Membership;
/// use oceanfs_network::ConnectionPool;
/// use oceanfs_node::repair::{HintDropRepairBridge, RepairDispatcher};
/// use oceanfs_routing::{Ring, RingCache};
///
/// let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
/// let membership = Arc::new(Membership::new(
///     NodeId::new("n1"),
///     "127.0.0.1:9100".parse().unwrap(),
///     "127.0.0.1:9101".parse().unwrap(),
///     GossipConfig::default(),
///     ring,
/// ));
/// let lifecycle =
///     Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
///         &oceanfs_core::LifecycleConfig::default(),
///     ));
/// let dispatcher = Arc::new(RepairDispatcher::new(
///     Arc::new(oceanfs_node::repair::ManifestRepairTargetSelector::new(
///         membership.clone(),
///         NodeId::new("n1"),
///     )),
///     Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
///     membership,
///     Arc::clone(&lifecycle),
///     NodeId::new("n1"),
/// ));
/// let bridge = HintDropRepairBridge::new(dispatcher, lifecycle, NodeId::new("n1"));
/// // No dropped hints: the sink is inert for an empty batch.
/// bridge.on_hints_dropped(&[]);
/// ```
pub struct HintDropRepairBridge {
    dispatcher: Arc<RepairDispatcher>,
    lifecycle: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
    self_id: NodeId,
    /// Shared `oceanfs_repair_enqueued_total` series (f5 D3): emitted
    /// hint-drop intents are counted next to the drift-scan repairs.
    repair_enqueued: Option<Counter>,
}

impl HintDropRepairBridge {
    /// Creates the bridge over the node's repair dispatcher + lifecycle
    /// registry.
    pub fn new(
        dispatcher: Arc<RepairDispatcher>,
        lifecycle: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
        self_id: NodeId,
    ) -> Self {
        Self { dispatcher, lifecycle, self_id, repair_enqueued: None }
    }

    /// Counts every emitted hint-drop intent on the supplied
    /// `oceanfs_repair_enqueued_total` handle, so the series covers the
    /// reconciliation drift scan AND the hint-drop bridge (f5 D3).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_core::{Counter, GossipConfig, LabelSet, NodeId, RingConfig};
    /// use oceanfs_membership::Membership;
    /// use oceanfs_network::ConnectionPool;
    /// use oceanfs_node::repair::{HintDropRepairBridge, RepairDispatcher};
    /// use oceanfs_routing::{Ring, RingCache};
    ///
    /// let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
    /// let membership = Arc::new(Membership::new(
    ///     NodeId::new("n1"),
    ///     "127.0.0.1:9100".parse().unwrap(),
    ///     "127.0.0.1:9101".parse().unwrap(),
    ///     GossipConfig::default(),
    ///     ring,
    /// ));
    /// let lifecycle =
    ///     Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
    ///         &oceanfs_core::LifecycleConfig::default(),
    ///     ));
    /// let dispatcher = Arc::new(RepairDispatcher::new(
    ///     Arc::new(oceanfs_node::repair::ManifestRepairTargetSelector::new(
    ///         membership.clone(),
    ///         NodeId::new("n1"),
    ///     )),
    ///     Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
    ///     membership,
    ///     Arc::clone(&lifecycle),
    ///     NodeId::new("n1"),
    /// ));
    /// let counter = Counter::new(
    ///     "oceanfs_repair_enqueued_total".into(),
    ///     "repair intents enqueued".into(),
    ///     LabelSet::empty(),
    /// );
    /// let _bridge = HintDropRepairBridge::new(dispatcher, lifecycle, NodeId::new("n1"))
    ///     .with_repair_enqueued_counter(counter.clone());
    /// assert_eq!(counter.get(), 0);
    /// ```
    pub fn with_repair_enqueued_counter(mut self, counter: Counter) -> Self {
        self.repair_enqueued = Some(counter);
        self
    }
}

impl HintDropSink for HintDropRepairBridge {
    fn on_hints_dropped(&self, dropped: &[HintDropRecord]) {
        for record in dropped {
            let Some(entry) = self.lifecycle.registry().get(record.segment_id) else {
                debug!(
                    segment_id = %record.segment_id,
                    intended_for = %record.intended_for,
                    "hint-drop repair skipped: segment not held locally"
                );
                continue;
            };
            let holders: Vec<NodeId> = entry.metadata.storage_locations.iter().cloned().collect();
            let request = ReRepRequest {
                origin: self.self_id.clone(),
                segment_id: record.segment_id,
                holders,
                reason: RepairReason::Reconciliation,
                retry_count: 0,
                merkle_root: entry.metadata.merkle_root,
                tier: entry.metadata.size_tier,
                ec_k: entry.metadata.ec_k,
                ec_m: entry.metadata.ec_m,
            };
            if let Some(counter) = &self.repair_enqueued {
                counter.inc();
            }
            let dispatcher = Arc::clone(&self.dispatcher);
            // Fire-and-forget: the dispatcher's bounded queue owns pacing;
            // a dispatch failure parks the request for the next sweep.
            tokio::spawn(async move {
                if let Err(error) = dispatcher.enqueue(request).await {
                    warn!(error = %error, "hint-drop repair intent rejected");
                }
            });
        }
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

    /// d1 (ADR-0036 D6): the manifest selector excludes a node whose only
    /// data pools are draining — `"draining"` flows through the existing
    /// `status() == "healthy"` seam, so a retiring node is never chosen to
    /// receive a new copy.
    #[test]
    fn manifest_selector_excludes_nodes_with_only_draining_data_pools() {
        use oceanfs_membership::manifest::{NodeManifest, PoolManifest};

        let membership = make_membership("n1");
        upsert(&membership, "n2");
        upsert(&membership, "n3");
        // n2: two data pools, both draining (the pool is retiring and must
        // not receive new copies). n3: healthy data pool.
        membership.set_peer_manifest(
            NodeId::new("n2"),
            NodeManifest::from_pools(
                1,
                &[
                    PoolManifest::new(0, "data", "draining", false, 999 << 30, 1),
                    PoolManifest::new(1, "data", "draining", false, 999 << 30, 1),
                ],
            ),
        );
        membership.set_peer_manifest(
            NodeId::new("n3"),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "data", "healthy", false, 100 << 30, 1)],
            ),
        );

        let selector = ManifestRepairTargetSelector::new(membership, NodeId::new("n1"));
        assert_eq!(
            selector.pick_repair_target(&SegmentId::new(), &[NodeId::new("n4")]),
            Some(NodeId::new("n3")),
            "a node whose data pools all drain is never a repair target"
        );
    }

    /// g8: the manifest selector excludes `node_unavailable` candidates
    /// even when they report a healthy data pool (they cannot persist the
    /// new copy's object row).
    #[test]
    fn manifest_selector_excludes_node_unavailable_despite_healthy_data() {
        use oceanfs_membership::manifest::{NodeManifest, PoolManifest};

        let membership = make_membership("n1");
        upsert(&membership, "n2");
        upsert(&membership, "n3");
        // n2: healthy data pool BUT node_unavailable → excluded despite
        // the most free capacity. n3: available with healthy data.
        let unavailable = NodeManifest::from_pools(
            1,
            &[PoolManifest::new(0, "data", "healthy", false, 999 << 30, 1)],
        )
        .with_node_unavailable(true);
        membership.set_peer_manifest(NodeId::new("n2"), unavailable);
        membership.set_peer_manifest(
            NodeId::new("n3"),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "data", "healthy", false, 100 << 30, 1)],
            ),
        );

        let selector = ManifestRepairTargetSelector::new(membership, NodeId::new("n1"));
        assert_eq!(
            selector.pick_repair_target(&SegmentId::new(), &[NodeId::new("n4")]),
            Some(NodeId::new("n3")),
            "the available node wins; the unavailable node is never a repair target"
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

    /// f5 D3: the hint-drop bridge emits exactly ONE ADR-0030 intent per
    /// dropped segment the node holds — carrying the segment's recorded
    /// holder set — and counts each emitted intent on the shared
    /// `oceanfs_repair_enqueued_total` handle. A dropped segment this
    /// node does not hold is skipped (its holders' own reconciliation
    /// owns it).
    #[tokio::test]
    async fn hint_drop_bridge_dispatches_one_intent_per_held_segment() {
        use oceanfs_durability::hinted_handoff::{HintDropRecord, HintDropSink};

        let membership = make_membership("n1");
        upsert(&membership, "n2");
        upsert(&membership, "n3");

        let lifecycle =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let held = SegmentId::new();
        let holders = vec![NodeId::new("n2"), NodeId::new("n3")];
        let meta = oceanfs_core::SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id: held,
            ec_k: 4,
            ec_m: 2,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: None,
            storage_locations: holders.iter().cloned().collect(),
            sealed_at: Some(0),
        };
        lifecycle.registry().reserve(held, meta.clone()).unwrap();
        lifecycle.registry().seal(held, meta).unwrap();

        let dispatcher = Arc::new(RepairDispatcher::new(
            Arc::new(SmallestId),
            Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            membership,
            Arc::clone(&lifecycle),
            NodeId::new("n1"),
        ));
        let enqueued = Counter::new(
            "test_repair_enqueued_total".into(),
            "test repair intents".into(),
            LabelSet::empty(),
        );
        let bridge = HintDropRepairBridge::new(
            Arc::clone(&dispatcher),
            Arc::clone(&lifecycle),
            NodeId::new("n1"),
        )
        .with_repair_enqueued_counter(enqueued.clone());

        let not_held = SegmentId::new();
        bridge.on_hints_dropped(&[
            HintDropRecord { segment_id: held, intended_for: NodeId::new("n2") },
            HintDropRecord { segment_id: not_held, intended_for: NodeId::new("n3") },
        ]);
        // The bridge spawns the bounded dispatch tasks; let them park.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            dispatcher.pending_len(),
            1,
            "one intent for the HELD segment; the unheld drop is skipped"
        );
        assert_eq!(
            enqueued.get(),
            1,
            "the emitted intent is counted on the shared repair-enqueued series"
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

    /// f5 D2: repair target selection prefers Healthy destinations over
    /// Degraded fallbacks (regardless of capacity) and never picks a
    /// hard-excluded candidate; when only Degraded candidates remain, a
    /// Degraded target is chosen (over-replication is a lesser problem
    /// than under-replication).
    #[test]
    fn repair_target_prefers_healthy_over_degraded_and_never_hard_excluded() {
        use oceanfs_membership::manifest::{NodeManifest, PoolManifest};

        let membership = make_membership("n1");
        upsert(&membership, "n2");
        upsert(&membership, "n3");
        upsert(&membership, "n4");

        // n2: Degraded with the most capacity; n3: Healthy with less.
        membership.set_peer_manifest(
            NodeId::new("n2"),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "data", "degraded", false, 500 << 30, 1)],
            ),
        );
        membership.set_peer_manifest(
            NodeId::new("n3"),
            NodeManifest::from_pools(
                1,
                &[PoolManifest::new(0, "data", "healthy", false, 100 << 30, 1)],
            ),
        );
        // n4: write_degraded (Dead WAL) → hard-excluded, despite the most
        // capacity.
        membership.set_peer_manifest(
            NodeId::new("n4"),
            NodeManifest::from_pools(
                1,
                &[
                    PoolManifest::new(0, "data", "healthy", false, 900 << 30, 1),
                    PoolManifest::new(1, "wal", "healthy", true, 1 << 30, 1),
                ],
            ),
        );

        let selector = ManifestRepairTargetSelector::new(membership.clone(), NodeId::new("n1"));
        assert_eq!(
            selector.pick_repair_target(&SegmentId::new(), &[]),
            Some(NodeId::new("n3")),
            "Healthy wins over the larger Degraded candidate; write_degraded never wins"
        );

        // With n3 already a holder, the Degraded n2 is the fallback.
        assert_eq!(
            selector.pick_repair_target(&SegmentId::new(), &[NodeId::new("n3")]),
            Some(NodeId::new("n2")),
            "a Degraded data pool is a valid repair fallback target"
        );
    }

    // ── ae1 S1: terminal no-live-holder classification ────────────────

    fn test_dispatcher(membership: Arc<Membership>, sweeps: u32) -> RepairDispatcher {
        RepairDispatcher::new(
            Arc::new(SmallestId),
            Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            membership,
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            NodeId::new("n1"),
        )
        .with_unrecoverable_sweeps(sweeps)
    }

    /// ae1 S1: a parked repair whose recorded holders are all absent from
    /// membership becomes terminal after N sweeps — not before — stops
    /// being re-enqueued, and counts exactly once.
    #[tokio::test]
    async fn terminal_classification_after_n_sweeps_and_not_before() {
        let membership = make_membership("n1");
        // No peers at all: the recorded holder "n2" is absent from membership.
        let dispatcher = test_dispatcher(membership, 3);
        let segment_id = SegmentId::new();
        dispatcher.enqueue(request(segment_id, vec![NodeId::new("n2")])).await.unwrap();
        assert_eq!(dispatcher.pending_len(), 1);

        for _ in 0..2 {
            dispatcher.sweep().await;
            assert!(!dispatcher.is_unrecoverable(&segment_id), "not terminal before N sweeps");
            assert_eq!(dispatcher.pending_len(), 1, "still parked before N sweeps");
        }
        dispatcher.sweep().await;
        assert!(dispatcher.is_unrecoverable(&segment_id), "terminal at N sweeps");
        assert_eq!(dispatcher.pending_len(), 0, "terminal requests are not parked");
        assert_eq!(dispatcher.metrics.unrecoverable_total_for_test(), 1, "counted once");

        // Repeated sweeps do not reclassify; re-enqueue is ignored.
        dispatcher.sweep().await;
        assert_eq!(dispatcher.metrics.unrecoverable_total_for_test(), 1, "no double count");
        assert_eq!(dispatcher.unrecoverable_len(), 1, "the dedupe set does not grow");
        dispatcher.enqueue(request(segment_id, vec![NodeId::new("n2")])).await.unwrap();
        assert_eq!(dispatcher.pending_len(), 0, "terminal requests are not re-enqueued");
    }

    /// ae1 S1: a recorded holder returning clears the terminal marker and
    /// resumes normal repair (re-parked, not terminal).
    #[tokio::test]
    async fn terminal_repair_resumes_when_a_recorded_holder_returns() {
        let membership = make_membership("n1");
        let dispatcher = test_dispatcher(membership.clone(), 2);
        let segment_id = SegmentId::new();
        dispatcher.enqueue(request(segment_id, vec![NodeId::new("n2")])).await.unwrap();
        dispatcher.sweep().await;
        dispatcher.sweep().await;
        assert!(dispatcher.is_unrecoverable(&segment_id), "terminal after two absent sweeps");

        // The recorded holder returns → the marker clears and the request
        // re-enters the parked set for normal dispatch.
        upsert(&membership, "n2");
        dispatcher.sweep().await;
        assert!(!dispatcher.is_unrecoverable(&segment_id), "marker cleared on holder return");
        assert_eq!(dispatcher.pending_len(), 1, "request resumes normal parking");
        assert_eq!(
            dispatcher.metrics.unrecoverable_total_for_test(),
            1,
            "resume does not decrement or recount"
        );
    }

    /// ae1 S1: the terminal set is bounded — more than `UNRECOVERABLE_SET_CAPACITY`
    /// classifications evict the oldest markers instead of growing
    /// without bound.
    #[tokio::test]
    async fn unrecoverable_set_evicts_oldest_beyond_capacity() {
        let membership = make_membership("n1");
        let dispatcher = test_dispatcher(membership, 1);
        let total = UNRECOVERABLE_SET_CAPACITY + 1;
        let mut segments = Vec::with_capacity(total);
        for _ in 0..total {
            let segment = SegmentId::new();
            segments.push(segment);
            dispatcher.enqueue(request(segment, vec![NodeId::new("ghost")])).await.unwrap();
        }
        assert_eq!(dispatcher.pending_len(), total);

        dispatcher.sweep().await;
        assert_eq!(dispatcher.metrics.unrecoverable_total_for_test(), total as u64);
        assert_eq!(
            dispatcher.unrecoverable_len(),
            UNRECOVERABLE_SET_CAPACITY,
            "the terminal set stays bounded"
        );
        let remaining = segments.iter().filter(|s| dispatcher.is_unrecoverable(s)).count();
        assert_eq!(
            remaining, UNRECOVERABLE_SET_CAPACITY,
            "exactly the excess classifications were evicted (classification order)"
        );
    }

    /// ae1 S1: an empty holder set is parked and classified terminal — no
    /// panic, and a live holder but no eligible target stays parked
    /// forever (the benign cannot-reach-RF state is NOT unrecoverable).
    #[tokio::test]
    async fn empty_holder_set_terminates_but_live_holders_do_not() {
        let membership = make_membership("n1");
        let dispatcher = test_dispatcher(membership.clone(), 2);
        let empty_segment = SegmentId::new();
        dispatcher.enqueue(request(empty_segment, vec![])).await.unwrap();
        for _ in 0..2 {
            dispatcher.sweep().await;
        }
        assert!(dispatcher.is_unrecoverable(&empty_segment), "no holders at all → terminal");

        // A live holder with no eligible target (no free peer) stays
        // parked — it is repairable, just not now.
        upsert(&membership, "n2");
        let live_segment = SegmentId::new();
        dispatcher.enqueue(request(live_segment, vec![NodeId::new("n2")])).await.unwrap();
        for _ in 0..5 {
            dispatcher.sweep().await;
        }
        assert!(
            !dispatcher.is_unrecoverable(&live_segment),
            "live holder + no target is the honest cannot-reach-RF state"
        );
        assert_eq!(dispatcher.pending_len(), 1);
    }
}
