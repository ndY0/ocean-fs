//! Periodic reconciliation (g4 `reconciliation`, ADR-0029 §D4 pull safety
//! net).
//!
//! The mandatory complement to g3's targeted push: a per-node loop that
//! restores RF **independently of any announcement having arrived**. It is
//! a repair loop, not a detection loop — failed repairs retry next tick.
//!
//! ## Design (user-approved, scan-free)
//!
//! - **Event-driven wake, not a full scan.** The 5s tick processes a
//!   bounded, risk-prioritized WORK QUEUE. The queue is populated by
//!   events — membership changes (a node died / a pool died) — which
//!   identify the affected segments through a [`HolderIndex`] maintained
//!   incrementally (O(RF) per `storage_locations` stamp), so a node dying
//!   touches exactly the segments that listed it, never a full scan of all
//!   segments (the "scanning terabytes every tick" anti-pattern the ADR
//!   warns against).
//! - **Completeness without sampling.** A slow drift scan (hourly,
//!   configurable) does a full pass as belt-and-suspenders — every segment
//!   is checked, just not every tick (ADR-0029 §D4: "healthy ranges at
//!   slow background cadence (drift detection)").
//! - **Risk-prioritized queue.** Single-copy segments (live=1) drain
//!   first, double-copy (live=2) next, healthy never enqueued.
//! - **Retry pacing.** A failed repair is retried at most once per
//!   `retry_after_ticks` (never hot-looped).
//!
//! The loop's live-copy math treats [`SegmentMetadata::storage_locations`]
//! as **intent, not truth** (GAP-5): the count is
//! `|storage_locations ∩ alive − unavailable|` where a node whose metadata
//! pool is Dead still counts (it HOLDS the data; it is merely unservable —
//! a routing concern, g6), and a node whose data pools are all Dead (or a
//! node that left) does not.

use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use oceanfs_core::{
    Counter, Gauge, LabelSet, MetricRegistrar, NodeId, NodeState, SegmentId, SegmentMetadata,
};
use oceanfs_membership::{manifest::NodeManifest, Membership};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::healing_service::{ReRepRequest, RepairReason, RepairSink};

// ---------------------------------------------------------------------------
// ReconcileConfig
// ---------------------------------------------------------------------------

/// Tuning for the reconciliation loop.
///
/// # Examples
///
/// ```
/// use oceanfs_durability::reconcile::ReconcileConfig;
///
/// let config = ReconcileConfig::default();
/// assert_eq!(config.tick_secs, 5);
/// assert_eq!(config.retry_after_ticks, 3);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileConfig {
    /// The work-queue processing cadence (seconds). Default 5 — matches
    /// `hint_delivery_sweep_sec`.
    pub tick_secs: u64,
    /// A segment whose repair failed is retried at most once per this
    /// many ticks (never hot-looped). Default 3.
    pub retry_after_ticks: u64,
    /// The full drift-scan cadence (seconds) — the completeness
    /// guarantee. Default 3600 (hourly).
    pub drift_scan_secs: u64,
    /// Maximum segments processed per tick from the work queue. A large
    /// event (a node with many segments dying) drains over multiple
    /// ticks; the queue is never processed in one unbounded burst.
    /// Default 256.
    pub max_batch_per_tick: usize,
    /// Bound on the pending work queue (distinct segments), f5 D3. When
    /// the queue is full, a new intent is not enqueued (drop-new) and
    /// `oceanfs_reconcile_queue_overflow_total` increments; overflow is
    /// safe because the drift scan and the holder's next event re-detect
    /// the segment. Bounds memory under intent storms (e.g. mass
    /// hint-drop conversion). Default 10_000.
    pub max_queue_depth: usize,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self {
            tick_secs: 5,
            retry_after_ticks: 3,
            drift_scan_secs: 3600,
            max_batch_per_tick: 256,
            max_queue_depth: 10_000,
        }
    }
}

// ---------------------------------------------------------------------------
// live_copy_count
// ---------------------------------------------------------------------------

/// Computes the live replica count for a segment: the number of
/// `storage_locations` holders that are alive AND not unavailable.
///
/// `alive` is the membership-alive node set. `unavailable` is the set of
/// nodes whose data is **not faithfully held**: no Healthy data pool on
/// their manifest (every data pool Degraded or Dead — f5 D2;
/// over-replication after a pool recovers is acceptable), or the node
/// Left/Dead. A node whose METADATA pool is Dead is NOT in `unavailable`
/// — it still holds the segment's data (data pools intact, g8), so it
/// counts as a live copy for RF purposes; it is merely unservable, which
/// is a routing concern (g6), not durability.
///
/// This is a *belief* (metadata says who holds S), not disk truth —
/// scrub/anti-entropy verify disk contents (out of scope).
///
/// # Examples
///
/// ```
/// use std::collections::HashSet;
/// use oceanfs_core::{NodeId, SegmentId, SegmentMetadata, SizeTier};
/// use oceanfs_durability::reconcile::live_copy_count;
///
/// let id = SegmentId::new();
/// let a = NodeId::new("a");
/// let b = NodeId::new("b");
/// let c = NodeId::new("c");
/// let mut locations = smallvec::SmallVec::new();
/// locations.push(a.clone());
/// locations.push(b.clone());
/// locations.push(c.clone());
/// let segment = SegmentMetadata {
///     pool_id: 0,
///     total_bytes: 0,
///     segment_id: id,
///     ec_k: 4,
///     ec_m: 2,
///     size_tier: SizeTier::Standard,
///     merkle_root: None,
///     storage_locations: locations,
///     sealed_at: None,
/// };
///
/// let alive: HashSet<NodeId> = [a.clone(), b.clone(), c.clone()].into_iter().collect();
/// // c is unavailable (no Healthy data pool left) → only a and b count.
/// let unavailable: HashSet<NodeId> = [c].into_iter().collect();
/// assert_eq!(live_copy_count(&segment, &alive, &unavailable), 2);
/// ```
pub fn live_copy_count(
    segment: &SegmentMetadata,
    alive: &HashSet<NodeId>,
    unavailable: &HashSet<NodeId>,
) -> usize {
    segment
        .storage_locations
        .iter()
        .filter(|loc| alive.contains(*loc) && !unavailable.contains(*loc))
        .count()
}

// [review][architecture][critical]
// this potentially holds every segment ids from the whole replication set within the cluster.
// this could become a very large list, shouldnt this better fit in the metadata store rather than in
// a plain in memory structure ? this remark holds for every plain in memory strcutures potentially holding large data sets.
// [end]
// ---------------------------------------------------------------------------
// HolderIndex
// ---------------------------------------------------------------------------

/// Reverse index: `node_id → {segment ids listing node_id in
/// storage_locations}`.
///
/// Enables the event-driven wake: when node X dies, the segments that
/// lost a copy are exactly the bucket `X` in the index — found in
/// O(|index bucket|), never by scanning all segments.
///
/// Maintained incrementally (O(RF) per stamp) from the single choke point
/// `oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::persist_storage_locations`
/// (plain code — the storage crate is not a rustdoc dependency here),
/// plus a boot build and a drift-scan rebuild (the completeness fallback
/// if a notifier was missed).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{NodeId, SegmentId};
/// use oceanfs_durability::reconcile::HolderIndex;
///
/// let index = HolderIndex::new();
/// let a = NodeId::new("a");
/// let seg = SegmentId::new();
/// let mut locations = smallvec::SmallVec::<[NodeId; 4]>::new();
/// locations.push(a.clone());
/// index.record(seg, &locations);
/// assert_eq!(index.segments_held_by(&a), vec![seg]);
/// ```
#[derive(Debug, Default)]
pub struct HolderIndex {
    /// `node_id → {segment ids listing node_id in storage_locations}`.
    inner: parking_lot::RwLock<HashMap<NodeId, HashSet<SegmentId>>>,
    /// `segment_id → {its current holders}` — enables the O(RF) remove
    /// (only the segment's OWN previous buckets are touched, never every
    /// bucket in the index).
    segment_holders: parking_lot::RwLock<HashMap<SegmentId, smallvec::SmallVec<[NodeId; 4]>>>,
}

impl HolderIndex {
    /// Creates an empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records (or replaces) a segment's holder set. The segment is
    /// removed from its PREVIOUS holder buckets (tracked per segment —
    /// O(RF), not O(distinct holders)), then added to the buckets named
    /// by the new `locations`.
    pub fn record(&self, segment_id: SegmentId, locations: &[NodeId]) {
        // Compute the diff under a single write lock for both maps
        // (consistent view, no torn updates).
        let mut holders = self.inner.write();
        let mut segment_map = self.segment_holders.write();
        // Remove from the segment's own previous buckets.
        if let Some(prev) = segment_map.get(&segment_id) {
            for old in prev.iter() {
                if let Some(bucket) = holders.get_mut(old) {
                    bucket.remove(&segment_id);
                    if bucket.is_empty() {
                        holders.remove(old);
                    }
                }
            }
        }
        // Add to the new buckets.
        let mut new_locs = smallvec::SmallVec::<[NodeId; 4]>::with_capacity(locations.len());
        for loc in locations {
            holders.entry(loc.clone()).or_default().insert(segment_id);
            if !new_locs.contains(loc) {
                new_locs.push(loc.clone());
            }
        }
        segment_map.insert(segment_id, new_locs);
    }

    /// The segments that list `node_id` in their storage_locations.
    pub fn segments_held_by(&self, node_id: &NodeId) -> Vec<SegmentId> {
        self.inner.read().get(node_id).iter().flat_map(|s| s.iter().copied()).collect()
    }

    /// The number of distinct holders indexed (observability / tests).
    pub fn holder_count(&self) -> usize {
        self.inner.read().len()
    }

    // [review][algorithmic][high]
    // if this a test only function, it should be conditionally compiled specifically for test modules, to not pollute the production binary
    // [end]
    /// The number of segments indexed across all holders (observability /
    /// tests).
    pub fn total_segments(&self) -> usize {
        self.inner.read().values().map(|s| s.len()).sum()
    }
}

// ---------------------------------------------------------------------------
// ReconciliationLoop
// ---------------------------------------------------------------------------

/// A single pending work item, risk-prioritized by live-copy count.
///
/// `BinaryHeap` is a max-heap, so the ordering is REVERSED: fewer live
/// copies = higher priority = pops first.
#[derive(Debug)]
struct WorkItem {
    segment_id: SegmentId,
    live: usize,
}

impl PartialEq for WorkItem {
    fn eq(&self, other: &Self) -> bool {
        self.live == other.live
    }
}
impl Eq for WorkItem {}
impl PartialOrd for WorkItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for WorkItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse: fewer live copies sorts higher.
        other.live.cmp(&self.live).then_with(|| {
            self.segment_id.as_uuid().as_u128().cmp(&other.segment_id.as_uuid().as_u128())
        })
    }
}

/// The periodic reconciliation loop (ADR-0029 §D4 pull safety net).
///
/// Event-driven + bounded queue + hourly drift scan. Consumes membership
/// events (a node died / its pools died) to enqueue exactly the affected
/// segments via the [`HolderIndex`], processes the queue in bounded
/// risk-prioritized batches per tick, and enqueues re-replication repair
/// requests (g5) for under-replicated segments.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use oceanfs_core::{GossipConfig, NodeId, RingConfig};
/// use oceanfs_durability::reconcile::{ReconcileConfig, ReconciliationLoop};
/// use oceanfs_membership::Membership;
/// use oceanfs_routing::{Ring, RingCache};
///
/// # let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
/// # let membership = Arc::new(Membership::new(
/// #     NodeId::new("n1"), "127.0.0.1:9200".parse().unwrap(),
/// #     "127.0.0.1:9201".parse().unwrap(), GossipConfig::default(), ring,
/// # ));
/// # let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
/// #     &oceanfs_core::LifecycleConfig::default(),
/// # ));
/// # let sink: Arc<dyn oceanfs_durability::healing_service::RepairSink> =
/// #     Arc::new(oceanfs_durability::reconcile::NoopRepairSink);
/// let loop_: Arc<ReconciliationLoop> = Arc::new(ReconciliationLoop::new(
///     registry,
///     membership,
///     sink,
///     NodeId::new("n1"),
///     3,
///     ReconcileConfig::default(),
/// ));
/// let shutdown = tokio_util::sync::CancellationToken::new();
/// let token = shutdown.clone();
/// let for_spawn = Arc::clone(&loop_);
/// tokio::spawn(async move { for_spawn.run(token).await });
/// shutdown.cancel();
/// ```
pub struct ReconciliationLoop {
    registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
    membership: Arc<Membership>,
    repair_sink: Arc<dyn RepairSink>,
    self_id: NodeId,
    rf: usize,
    config: ReconcileConfig,
    /// Reverse holder index for event-driven wake.
    index: Arc<HolderIndex>,
    /// The pending work queue (risk-prioritized).
    queue: parking_lot::Mutex<BinaryHeap<WorkItem>>,
    /// Dedup: segments already in the queue.
    in_queue: parking_lot::Mutex<HashSet<SegmentId>>,
    /// Per-segment tick-of-last-enqueue for retry pacing.
    last_enqueued_tick: parking_lot::Mutex<HashMap<SegmentId, u64>>,
    /// The current tick counter (increments per processing pass).
    tick: std::sync::atomic::AtomicU64,
    // Metrics
    ranges_under_replicated: Gauge,
    scan_ms: Gauge,
    repair_enqueued_total: Counter,
    /// Current pending-queue occupancy (distinct segments), f5 D3.
    queue_depth: Gauge,
    /// Intents dropped at `ReconcileConfig::max_queue_depth` (f5 D3).
    /// Overflow is observable, never silent; the drift scan re-detects
    /// the segment later.
    queue_overflow_total: Counter,
}

/// A repair sink that does nothing (tests / minimal embeddings).
///
/// # Examples
///
/// ```
/// use oceanfs_core::NodeId;
/// use oceanfs_durability::healing_service::{ReRepRequest, RepairReason, RepairSink};
/// use oceanfs_durability::reconcile::NoopRepairSink;
///
/// let sink = NoopRepairSink;
/// let req = ReRepRequest {
///     origin: NodeId::new("a"),
///     segment_id: oceanfs_core::SegmentId::new(),
///     holders: vec![NodeId::new("b")],
///     reason: RepairReason::Announcement,
///     retry_count: 0,
///     merkle_root: None,
///     tier: oceanfs_core::SizeTier::Standard,
///     ec_k: 1,
///     ec_m: 0,
/// };
/// let rt = tokio::runtime::Runtime::new().expect("runtime");
/// assert!(rt.block_on(sink.enqueue(req)).is_ok());
/// ```
#[derive(Debug, Clone, Copy)]
pub struct NoopRepairSink;

#[async_trait::async_trait]
impl RepairSink for NoopRepairSink {
    async fn enqueue(&self, _request: ReRepRequest) -> Result<(), String> {
        Ok(())
    }
}

impl ReconciliationLoop {
    /// Creates a new reconciliation loop.
    ///
    /// `rf` is the cluster replication factor (the node's
    /// `config.replication_factor`). `registry` is the lifecycle registry
    /// of segments THIS node holds; `membership` provides the alive-node
    /// set + manifests; `repair_sink` receives re-replication requests
    /// (g5).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
        membership: Arc<Membership>,
        repair_sink: Arc<dyn RepairSink>,
        self_id: NodeId,
        rf: usize,
        config: ReconcileConfig,
    ) -> Self {
        let index = Arc::new(HolderIndex::new());
        // Boot build: one full pass at startup (acceptable — startup
        // already rebuilds state; this is NOT a per-tick scan).
        build_index_from_registry(&registry, &index);
        Self {
            registry,
            membership,
            repair_sink,
            self_id,
            rf,
            config,
            index,
            queue: parking_lot::Mutex::new(BinaryHeap::new()),
            in_queue: parking_lot::Mutex::new(HashSet::new()),
            last_enqueued_tick: parking_lot::Mutex::new(HashMap::new()),
            tick: std::sync::atomic::AtomicU64::new(0),
            ranges_under_replicated: Gauge::new(
                "oceanfs_ranges_under_replicated".into(),
                "Segments below RF by live-copy count".into(),
                LabelSet::empty(),
            ),
            scan_ms: Gauge::new(
                "oceanfs_reconcile_scan_ms".into(),
                "Duration of the last reconciliation pass (ms)".into(),
                LabelSet::empty(),
            ),
            repair_enqueued_total: Counter::new(
                "oceanfs_repair_enqueued_total".into(),
                "Re-replication repair requests enqueued".into(),
                LabelSet::empty(),
            ),
            queue_depth: Gauge::new(
                "oceanfs_reconcile_queue_depth".into(),
                "Pending reconciliation work items (distinct segments)".into(),
                LabelSet::empty(),
            ),
            queue_overflow_total: Counter::new(
                "oceanfs_reconcile_queue_overflow_total".into(),
                "Repair intents dropped at the reconciliation queue bound".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Returns the holder index (tests/observability).
    pub fn holder_index(&self) -> Arc<HolderIndex> {
        Arc::clone(&self.index)
    }

    /// A handle to the `oceanfs_repair_enqueued_total` counter (f5 D3).
    ///
    /// Other repair producers increment the same series through this
    /// handle — the node's hint-drop bridge counts its exhausted-hint
    /// repair intents next to the reconciliation drift-scan enqueues.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use oceanfs_core::{GossipConfig, NodeId, RingConfig};
    /// use oceanfs_durability::reconcile::{ReconcileConfig, ReconciliationLoop};
    /// use oceanfs_membership::Membership;
    /// use oceanfs_routing::{Ring, RingCache};
    ///
    /// # let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
    /// # let membership = Arc::new(Membership::new(
    /// #     NodeId::new("n1"), "127.0.0.1:9200".parse().unwrap(),
    /// #     "127.0.0.1:9201".parse().unwrap(), GossipConfig::default(), ring,
    /// # ));
    /// # let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
    /// #     &oceanfs_core::LifecycleConfig::default(),
    /// # ));
    /// # let sink: Arc<dyn oceanfs_durability::healing_service::RepairSink> =
    /// #     Arc::new(oceanfs_durability::reconcile::NoopRepairSink);
    /// let loop_ = ReconciliationLoop::new(
    ///     registry, membership, sink, NodeId::new("n1"), 3, ReconcileConfig::default(),
    /// );
    /// // The handle the node's hint-drop bridge increments (f5 D3).
    /// assert_eq!(loop_.repair_enqueued_counter().get(), 0);
    /// ```
    pub fn repair_enqueued_counter(&self) -> Counter {
        self.repair_enqueued_total.clone()
    }

    /// Records a segment's holder set into the index (the composition
    /// root wires this to the lifecycle coordinator's
    /// `persist_storage_locations` notifier).
    pub fn on_storage_locations(&self, segment_id: SegmentId, locations: &[NodeId]) {
        self.index.record(segment_id, locations);
    }

    /// Registers the loop's metrics with a registrar.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_gauge(self.ranges_under_replicated.clone());
        registrar.register_gauge(self.scan_ms.clone());
        registrar.register_counter(self.repair_enqueued_total.clone());
        registrar.register_gauge(self.queue_depth.clone());
        registrar.register_counter(self.queue_overflow_total.clone());
    }

    /// The number of pending work items (tests/observability).
    pub fn pending_len(&self) -> usize {
        self.in_queue.lock().len()
    }

    /// Enqueues a segment for a live-count check (event wake).
    ///
    /// Dedup: a segment already in the queue is not re-enqueued; its
    /// priority is refreshed only when it is processed. Returns `true`
    /// when the segment was newly enqueued.
    pub fn enqueue(&self, segment_id: SegmentId) -> bool {
        let live = self.compute_live(segment_id);
        self.enqueue_with_live(segment_id, live)
    }

    /// Enqueues a segment with a CALLER-COMPUTED live count (the drift
    /// scan uses this to avoid re-entering the registry — `for_each`
    /// already holds the shard lock).
    ///
    /// f5 D3: bounded by [`ReconcileConfig::max_queue_depth`]. Overflow
    /// drops the NEW intent (the segment is not inserted), increments
    /// `oceanfs_reconcile_queue_overflow_total`, and leaves the segment
    /// for the drift scan / next holder event — bounded memory, never a
    /// silent loss.
    fn enqueue_with_live(&self, segment_id: SegmentId, live: usize) -> bool {
        let mut in_queue = self.in_queue.lock();
        if !in_queue.insert(segment_id) {
            return false;
        }
        if in_queue.len() > self.config.max_queue_depth {
            in_queue.remove(&segment_id);
            self.queue_overflow_total.inc();
            return false;
        }
        self.queue.lock().push(WorkItem { segment_id, live });
        self.queue_depth.set(in_queue.len() as u64);
        true
    }

    /// Computes the current live-copy count for a segment from the
    /// membership view + manifests.
    fn compute_live(&self, segment_id: SegmentId) -> usize {
        let Some(entry) = self.registry.get(segment_id) else {
            return usize::MAX; // unknown segment: not our problem
        };
        let (alive, unavailable) = self.membership_snapshot();
        live_copy_count(&entry.metadata, &alive, &unavailable)
    }

    /// Whether a manifest means its node is **not a faithful copy
    /// holder** (f5 D2): every data pool is non-Healthy (Degraded or
    /// Dead). A Degraded pool is treated as lost for RF accounting —
    /// over-replication after recovery is acceptable, and it is the only
    /// way repair can fire while a pool sits Degraded.
    ///
    /// The empty-data-pool guard is preserved: a manifest that declares
    /// no data rows is not confirmed loss (unknown pools), so it stays
    /// countable.
    fn manifest_data_unavailable(manifest: &NodeManifest) -> bool {
        let data_pools: Vec<_> = manifest.pools().iter().filter(|p| p.role() == "data").collect();
        !data_pools.is_empty() && data_pools.iter().all(|p| p.status() != "healthy")
    }

    /// Snapshots the membership view: alive nodes + unavailable nodes
    /// (Left/Dead members, or members with no Healthy data pool per
    /// their manifest — f5 D2).
    fn membership_snapshot(&self) -> (HashSet<NodeId>, HashSet<NodeId>) {
        let mut alive = HashSet::new();
        let mut unavailable = HashSet::new();
        for (node_id, state, _inc, _addr, _maddr, _v, _o, manifest) in self.membership.nodes_full()
        {
            match state {
                NodeState::Alive | NodeState::Suspect => {
                    alive.insert(node_id.clone());
                    // f5 (D2, ADR-0029 D3): a node with no Healthy data
                    // pool is not a faithful copy holder. A Degraded pool
                    // counts as lost for reconciliation — over-replication
                    // is a lesser problem than never repairing RF. The
                    // empty-data-pool guard is preserved: a manifest with
                    // no data rows stays countable (unknown pools are not
                    // confirmed loss).
                    let no_healthy_data = manifest
                        .as_ref()
                        .map(|m| Self::manifest_data_unavailable(m.as_ref()))
                        .unwrap_or(false);
                    if no_healthy_data {
                        unavailable.insert(node_id.clone());
                    }
                }
                NodeState::Dead | NodeState::Left | NodeState::Leaving => {
                    unavailable.insert(node_id);
                }
            }
        }
        (alive, unavailable)
    }

    /// The set of segments that need a live-count check after node `x`
    /// changed state: those that listed `x` as a holder.
    fn segments_affected_by(&self, x: &NodeId) -> Vec<SegmentId> {
        self.index.segments_held_by(x)
    }

    /// Runs the loop until shutdown: event-driven wake + bounded
    /// per-tick queue processing + hourly drift scan.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let mut events = self.membership.subscribe();
        let mut tick_interval =
            tokio::time::interval(std::time::Duration::from_secs(self.config.tick_secs.max(1)));
        let mut drift_interval = tokio::time::interval(std::time::Duration::from_secs(
            self.config.drift_scan_secs.max(1),
        ));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("reconciliation loop shutting down");
                    break;
                }
                event = events.recv() => {
                    match event {
                        Ok(ev) => {
                            // A node became unavailable (Dead/Left) or its
                            // manifest now reports all data pools Dead —
                            // wake the segments that listed it. The
                            // manifest change is the data-pool-death signal
                            // (the node itself stays Alive — only its pool
                            // died).
                            let changed = ev.node_id.clone();
                            let node_unavailable = matches!(
                                ev.new_state,
                                NodeState::Dead | NodeState::Left | NodeState::Leaving
                            );
                            let pool_dead = ev
                                .manifest
                                .as_ref()
                                .map(|m| {
                                    let data_pools: Vec<_> = m
                                        .pools()
                                        .iter()
                                        .filter(|p| p.role() == "data")
                                        .collect();
                                    !data_pools.is_empty()
                                        && data_pools.iter().all(|p| p.status() == "dead")
                                })
                                .unwrap_or(false);
                            if node_unavailable || pool_dead {
                                let affected = self.segments_affected_by(&changed);
                                debug!(
                                    node = %changed,
                                    node_unavailable,
                                    pool_dead,
                                    affected = affected.len(),
                                    "reconciliation event wake: node data unavailable"
                                );
                                for seg in affected {
                                    self.enqueue(seg);
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // Missed events — the next tick/drift covers them.
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = tick_interval.tick() => {
                    self.process_batch().await;
                }
                _ = drift_interval.tick() => {
                    // Completeness pass: rebuild the index from the
                    // registry and check EVERY segment (hourly — a full
                    // pass, not sampling). The membership view is
                    // snapshotted ONCE for the whole pass.
                    //
                    // CRITICAL: `for_each` holds a shard read lock and
                    // its doc forbids calling back into the registry.
                    // `enqueue` → `compute_live` → `registry.get` would
                    // re-acquire that lock → deadlock (parking_lot's
                    // writer-preference read lock blocks a recursive read
                    // behind a queued writer). So live counts are
                    // computed from the ALREADY-HELD entry and enqueued
                    // with the computed value (no registry re-entry).
                    let started = Instant::now();
                    build_index_from_registry(&self.registry, &self.index);
                    let (alive, unavailable) = self.membership_snapshot();
                    let mut checked = 0usize;
                    let mut under = 0usize;
                    self.registry.for_each(|id, entry| {
                        checked += 1;
                        let live = live_copy_count(&entry.metadata, &alive, &unavailable);
                        if live < self.rf {
                            under += 1;
                            self.enqueue_with_live(id, live);
                        }
                    });
                    self.scan_ms.set(started.elapsed().as_millis() as u64);
                    debug!(checked, under, "reconciliation drift scan complete");
                }
            }
        }
    }

    /// Processes one bounded batch of the work queue: computes live
    /// counts, enqueues repairs for under-replicated segments, and
    /// applies retry pacing.
    ///
    /// All lock acquisition happens in a SYNCHRONOUS pre-pass (no
    /// parking_lot guard is ever held across an await — the future must
    /// stay `Send`); the only `.await` is the repair-sink enqueue.
    async fn process_batch(&self) {
        let tick = self.tick.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut batch = Vec::with_capacity(self.config.max_batch_per_tick);
        {
            let mut queue = self.queue.lock();
            for _ in 0..self.config.max_batch_per_tick {
                match queue.pop() {
                    Some(item) => batch.push(item),
                    None => break,
                }
            }
        }
        if batch.is_empty() {
            return;
        }

        let (alive, unavailable) = self.membership_snapshot();
        let batch_len = batch.len();
        let mut under_count = 0usize;
        let mut repaired = 0usize;

        // Synchronous pre-pass: decide, for each popped item, whether to
        // enqueue a repair (live < RF AND the retry-pacing window has
        // elapsed) or drop/requeue it. NO locks are held when this
        // returns — the `last_enqueued_tick` guard is scoped here.
        let mut to_enqueue: Vec<SegmentId> = Vec::with_capacity(batch_len);
        {
            let mut last = self.last_enqueued_tick.lock();
            for item in batch {
                let Some(entry) = self.registry.get(item.segment_id) else {
                    // Segment vanished (deleted) — drop it.
                    self.in_queue.lock().remove(&item.segment_id);
                    continue;
                };
                let live = live_copy_count(&entry.metadata, &alive, &unavailable);
                if live >= self.rf {
                    // Healthy now (a repair landed / the node returned) —
                    // done.
                    self.in_queue.lock().remove(&item.segment_id);
                    continue;
                }
                // Retry pacing: a segment retried fewer than
                // retry_after_ticks ago is NOT re-enqueued (no hot-loop).
                // A segment with no recorded tick is enqueued immediately
                // (the first detection is not "too soon").
                match last.get(&item.segment_id) {
                    Some(last_tick)
                        if tick.saturating_sub(*last_tick) < self.config.retry_after_ticks =>
                    {
                        // Too soon — put it back for a later tick.
                        self.queue.lock().push(item);
                        continue;
                    }
                    _ => {}
                }
                last.insert(item.segment_id, tick);
                to_enqueue.push(item.segment_id);
            }
        }

        // Enqueue the repairs (g5 executes them). No locks held. The
        // request carries the FULL holder set from this node's registry
        // entry; the node-side dispatcher filters it to LIVE holders
        // before selecting a target and sending the RPC (ADR-0030).
        for segment_id in to_enqueue {
            let entry = self.registry.get(segment_id);
            let holders: Vec<NodeId> = entry
                .as_ref()
                .map(|entry| entry.metadata.storage_locations.to_vec())
                .unwrap_or_default();
            let merkle_root = entry.as_ref().and_then(|e| e.metadata.merkle_root);
            // The seal-time shape rides the request (ADR-0030): the
            // acquiring worker registers the pulled copy with the
            // SOURCE's tier/EC geometry.
            let (tier, ec_k, ec_m) = entry
                .as_ref()
                .map(|e| (e.metadata.size_tier, e.metadata.ec_k, e.metadata.ec_m))
                .unwrap_or((oceanfs_core::SizeTier::Standard, 1, 0));
            let request = ReRepRequest {
                origin: self.self_id.clone(),
                segment_id,
                holders,
                reason: RepairReason::Reconciliation,
                retry_count: 0,
                merkle_root,
                tier,
                ec_k,
                ec_m,
            };
            match self.repair_sink.enqueue(request).await {
                Ok(()) => {
                    repaired += 1;
                    self.repair_enqueued_total.inc();
                }
                Err(e) => {
                    warn!(
                        segment_id = %segment_id,
                        error = %e,
                        "reconciliation repair enqueue failed; retry next tick"
                    );
                }
            }
            // The segment stays in the queue for the next check after the
            // pacing window (if the repair did not restore RF, the next
            // pass re-enqueues it).
            self.queue.lock().push(WorkItem { segment_id, live: 0 });
            under_count += 1;
        }

        // The gauge reflects the CURRENT under-replicated population (the
        // segments still awaiting repair — the in_queue set) rather than
        // the last batch's transient count, so it is monotonic-ish and
        // meaningful across batches.
        self.ranges_under_replicated.set(self.pending_len() as u64);
        self.queue_depth.set(self.pending_len() as u64);
        debug!(processed = batch_len, under_count, repaired, "reconciliation batch processed");
    }
}

/// Builds the holder index from the registry (boot + drift rebuild).
fn build_index_from_registry(
    registry: &oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry,
    index: &HolderIndex,
) {
    registry.for_each(|id, entry| {
        let locations: Vec<NodeId> = entry.metadata.storage_locations.to_vec();
        index.record(id, &locations);
    });
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn segment_meta(id: SegmentId, locations: &[NodeId]) -> SegmentMetadata {
        let mut locs = smallvec::SmallVec::new();
        for l in locations {
            locs.push(l.clone());
        }
        SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: None,
            storage_locations: locs,
            sealed_at: None,
        }
    }

    #[test]
    fn live_copy_counts_intersection_minus_unavailable() {
        let id = SegmentId::new();
        let a = NodeId::new("a");
        let b = NodeId::new("b");
        let c = NodeId::new("c");
        let segment = segment_meta(id, &[a.clone(), b.clone(), c.clone()]);
        let alive: HashSet<NodeId> = [a.clone(), b.clone(), c.clone()].into_iter().collect();
        let unavailable: HashSet<NodeId> = [b.clone()].into_iter().collect();
        assert_eq!(live_copy_count(&segment, &alive, &unavailable), 2);

        // A metadata-Dead node is NOT unavailable (it still holds data).
        let unavailable: HashSet<NodeId> = HashSet::new();
        assert_eq!(live_copy_count(&segment, &alive, &unavailable), 3);

        // A node not in alive is not counted.
        let alive: HashSet<NodeId> = [a.clone()].into_iter().collect();
        assert_eq!(live_copy_count(&segment, &alive, &unavailable), 1);
    }

    /// f5 D2: all-Degraded data pools mean "not a faithful copy" — the
    /// same predicate the snapshot uses to build `unavailable`.
    #[test]
    fn manifest_data_unavailable_flags_all_degraded_and_all_dead() {
        use oceanfs_membership::manifest::PoolManifest;

        let degraded = NodeManifest::from_pools(
            1,
            &[PoolManifest::new(0, "data", "degraded", false, 1 << 40, 1)],
        );
        assert!(ReconciliationLoop::manifest_data_unavailable(&degraded));

        let dead =
            NodeManifest::from_pools(1, &[PoolManifest::new(0, "data", "dead", false, 0, 1)]);
        assert!(ReconciliationLoop::manifest_data_unavailable(&dead));

        let mixed = NodeManifest::from_pools(
            1,
            &[
                PoolManifest::new(0, "data", "degraded", false, 1 << 40, 1),
                PoolManifest::new(1, "data", "healthy", false, 1 << 40, 1),
            ],
        );
        assert!(
            !ReconciliationLoop::manifest_data_unavailable(&mixed),
            "one Healthy data pool is still a faithful copy"
        );

        // The empty-data-pool guard: unknown pools are not confirmed loss.
        let no_data = NodeManifest::from_pools(
            1,
            &[PoolManifest::new(0, "wal", "healthy", false, 1 << 30, 1)],
        );
        assert!(!ReconciliationLoop::manifest_data_unavailable(&no_data));
    }

    /// f5 D3: the reconciliation queue is bounded — an intent beyond
    /// `max_queue_depth` is not enqueued and the overflow counter moves;
    /// the queue never grows past the bound.
    #[test]
    fn bounded_queue_drops_new_intents_and_counts_overflow() {
        use oceanfs_membership::Membership;
        use oceanfs_routing::{Ring, RingCache};

        let ring = Arc::new(RingCache::new(Ring::new(oceanfs_core::RingConfig::default())));
        let membership = Arc::new(Membership::new(
            NodeId::new("n1"),
            "127.0.0.1:9300".parse().unwrap(),
            "127.0.0.1:9301".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring,
        ));
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let config = ReconcileConfig { max_queue_depth: 1, ..ReconcileConfig::default() };
        let sink: Arc<dyn RepairSink> = Arc::new(NoopRepairSink);
        let loop_ =
            ReconciliationLoop::new(registry, membership, sink, NodeId::new("n1"), 3, config);

        assert!(loop_.enqueue(SegmentId::new()), "first intent fits");
        assert!(!loop_.enqueue(SegmentId::new()), "second intent dropped at the bound");
        assert_eq!(loop_.pending_len(), 1, "queue never exceeds max_queue_depth");
        assert_eq!(loop_.queue_overflow_total.get(), 1, "overflow is observed");
        assert_eq!(loop_.queue_depth.get(), 1, "occupancy gauge reflects the queue");
    }

    #[test]
    fn holder_index_records_and_replaces() {
        let index = HolderIndex::new();
        let a = NodeId::new("a");
        let b = NodeId::new("b");
        let seg = SegmentId::new();
        index.record(seg, std::slice::from_ref(&a));
        assert_eq!(index.segments_held_by(&a), vec![seg]);
        assert!(index.segments_held_by(&b).is_empty());

        // A remap moves the segment from a to b.
        index.record(seg, std::slice::from_ref(&b));
        assert!(index.segments_held_by(&a).is_empty());
        assert_eq!(index.segments_held_by(&b), vec![seg]);
    }

    #[test]
    fn priority_orders_single_copy_first() {
        let mut heap = BinaryHeap::new();
        let s1 = SegmentId::new();
        let s2 = SegmentId::new();
        heap.push(WorkItem { segment_id: s2, live: 2 });
        heap.push(WorkItem { segment_id: s1, live: 1 });
        assert_eq!(heap.pop().unwrap().segment_id, s1, "live=1 pops first");
        assert_eq!(heap.pop().unwrap().segment_id, s2, "live=2 pops next");
    }

    /// NoopRepairSink is a valid RepairSink.
    #[tokio::test]
    async fn noop_sink_accepts_requests() {
        let sink = NoopRepairSink;
        let req = ReRepRequest {
            origin: NodeId::new("a"),
            segment_id: SegmentId::new(),
            holders: vec![NodeId::new("b")],
            reason: RepairReason::Reconciliation,
            retry_count: 0,
            merkle_root: None,
            tier: oceanfs_core::SizeTier::Standard,
            ec_k: 1,
            ec_m: 0,
        };
        assert!(sink.enqueue(req).await.is_ok());
    }

    /// A recording repair sink (shared counter).
    #[derive(Clone, Default)]
    struct CountingSink {
        count: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl RepairSink for CountingSink {
        async fn enqueue(&self, _request: ReRepRequest) -> Result<(), String> {
            self.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    /// Retry pacing: a segment under RF is enqueued for repair at most
    /// once per `retry_after_ticks` — a hot-looping segment must NOT be
    /// re-enqueued every tick.
    #[tokio::test]
    async fn retry_pacing_prevents_hot_loop() {
        // A fresh membership with NO nodes → every segment's live count
        // is 0 < RF → every enqueued segment would be "under-replicated".
        let ring = Arc::new(oceanfs_routing::RingCache::new(oceanfs_routing::Ring::new(
            oceanfs_core::RingConfig::default(),
        )));
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new("n1"),
            addr,
            addr,
            oceanfs_core::GossipConfig::default(),
            ring,
        ));

        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        // Seed a sealed segment with NO storage_locations → live 0.
        let seg = SegmentId::new();
        registry
            .reserve(
                seg,
                SegmentMetadata {
                    pool_id: 0,
                    total_bytes: 0,
                    segment_id: seg,
                    ec_k: 4,
                    ec_m: 2,
                    size_tier: oceanfs_core::SizeTier::Standard,
                    merkle_root: None,
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: None,
                },
            )
            .unwrap();
        registry
            .seal(
                seg,
                SegmentMetadata {
                    pool_id: 0,
                    total_bytes: 0,
                    segment_id: seg,
                    ec_k: 4,
                    ec_m: 2,
                    size_tier: oceanfs_core::SizeTier::Standard,
                    merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0x11; 32])),
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: Some(1),
                },
            )
            .unwrap();

        let sink = CountingSink::default();
        let loop_ = Arc::new(ReconciliationLoop::new(
            Arc::clone(&registry),
            membership,
            Arc::new(sink.clone()),
            NodeId::new("n1"),
            3,
            ReconcileConfig { retry_after_ticks: 3, ..Default::default() },
        ));

        // Enqueue once → the first process_batch enqueues a repair.
        assert!(loop_.enqueue(seg));
        loop_.process_batch().await;
        assert_eq!(sink.count.load(std::sync::atomic::Ordering::Relaxed), 1);

        // Immediately process again → pacing must suppress the repair
        // (fewer than retry_after_ticks have elapsed).
        loop_.process_batch().await;
        assert_eq!(
            sink.count.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "pacing must prevent a hot-loop re-enqueue within retry_after_ticks"
        );
    }

    /// The drift scan builds the holder index from the registry — a
    /// segment recorded via the index is found when scanned.
    #[test]
    fn drift_scan_builds_index_from_registry() {
        let registry = oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
            &oceanfs_core::LifecycleConfig::default(),
        );
        let a = NodeId::new("a");
        let seg = SegmentId::new();
        registry.reserve(seg, segment_meta(seg, std::slice::from_ref(&a))).unwrap();
        registry.seal(seg, segment_meta(seg, std::slice::from_ref(&a))).unwrap();

        let index = HolderIndex::new();
        build_index_from_registry(&registry, &index);
        assert_eq!(index.segments_held_by(&a), vec![seg]);
    }
}
