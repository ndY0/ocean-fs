//! Seal-time segment replication (sealed-segment-replication).
//!
//! The data-replication backbone: after a segment seals on this node (the
//! write-path seal worker or the GC compactor), its FULL data section is
//! pushed to the segment's ring replicas —
//! [`oceanfs_routing::segment_replica_set`] minus self — the exact set
//! the read path's gRPC fallback fetches from (`fetch.rs`). Without this,
//! object bytes live on exactly one node (the phase-2 replication defect
//! this module exists to fix).
//!
//! **The seal path never makes a network call.** Sealing is a critical hot
//! path: `enqueue` is a single non-blocking channel send (one atomic op —
//! perf 7.1). All network work happens in the decoupled `run` task.
//!
//! Robustness (the backbone contract):
//! - bounded channel (perf 2.6): a full channel routes the segment into
//!   `needs_replication` instead of dropping it or blocking the sealer;
//! - idempotent receiver: duplicate pushes converge to one copy, so
//!   retries and reconciliation re-pushes are always safe;
//! - per-target ack tracking: `storage_locations` is stamped on the
//!   registry entry ONLY after every intended holder acked, so a non-empty
//!   set means "all holders confirmed" (the g3/g4 holder set);
//! - the `needs_replication` set + the periodic sweep ARE the g4
//!   reconciliation skeleton: g4 extends the same primitive to a full
//!   5s-tick scan of the owned inventory.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use oceanfs_core::{
    proto::segment::PushSealedSegmentRequest, Counter, Gauge, LabelSet, MetricRegistrar, NodeId,
    SegmentId, SizeTier,
};
use oceanfs_durability::SegmentDataStore;
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::{segment_replica_set, RingCache};
use oceanfs_storage::SegmentRpcClient;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// Configuration for the segment replicator.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Bounded channel capacity for sealed-segment events (perf 2.6 —
    /// overflow routes to `needs_replication`, never blocks the sealer).
    pub channel_capacity: usize,
    /// Per-target push concurrency cap (perf 2.7 — bounded fan-out,
    /// decoupled from the seal worker's own drain).
    pub max_concurrent_pushes: usize,
    /// Periodic retry sweep interval for the `needs_replication` set.
    pub retry_sweep_secs: u64,
    /// Push throttle in bytes/sec (0 = unlimited). Mirrors the scrub/heal
    /// throttle pattern: seal pushes back off during write/read bursts.
    pub throttle_bytes_sec: u64,
    /// Per-push RPC timeout in milliseconds.
    pub push_timeout_ms: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 1024,
            max_concurrent_pushes: 2,
            retry_sweep_secs: 5,
            throttle_bytes_sec: 0,
            push_timeout_ms: 30_000,
        }
    }
}

/// Maps a `SizeTier` to its wire `u32` (must match the receiver's decode).
fn tier_to_u32(tier: SizeTier) -> u32 {
    match tier {
        SizeTier::Inline => 0,
        SizeTier::Small => 1,
        SizeTier::Standard => 2,
        SizeTier::Multi => 3,
        _ => 2,
    }
}

/// A fixed-window byte-rate limiter for the segment-replication push path.
///
/// `throttle_bytes_sec` bytes may be pushed per wall-clock second; a push
/// that would exceed the window's budget sleeps until the window rolls.
/// `0` = unlimited (the default — the limiter is a no-op). One instance
/// per replicator (shared across concurrent pushes), so the aggregate
/// replication rate is bounded, not just per-target.
struct ByteRateLimiter {
    bytes_per_sec: u64,
    state: parking_lot::Mutex<RateWindow>,
}

/// The limiter's window state: the current second's start instant and the
/// bytes already admitted into it.
struct RateWindow {
    window_start: std::time::Instant,
    admitted_bytes: u64,
}

impl ByteRateLimiter {
    /// Creates a limiter for `bytes_per_sec` (0 = unlimited).
    fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            state: parking_lot::Mutex::new(RateWindow {
                window_start: std::time::Instant::now(),
                admitted_bytes: 0,
            }),
        }
    }

    /// Admits `bytes` into the current window, sleeping (up to 1 s) until
    /// the next window when the budget is exhausted. Returns immediately
    /// for unlimited (0) or when the window has capacity.
    async fn acquire(&self, bytes: u64) {
        if self.bytes_per_sec == 0 {
            return;
        }
        loop {
            let sleep_for = {
                let mut state = self.state.lock();
                let now = std::time::Instant::now();
                // Window rolled: reset.
                if now.duration_since(state.window_start) >= std::time::Duration::from_secs(1) {
                    state.window_start = now;
                    state.admitted_bytes = 0;
                }
                if state.admitted_bytes.saturating_add(bytes) <= self.bytes_per_sec {
                    state.admitted_bytes += bytes;
                    return;
                }
                let remaining =
                    std::time::Duration::from_secs(1) - now.duration_since(state.window_start);
                // Bound the sleep so a huge backlog still yields to
                // the drain loop regularly.
                remaining.min(std::time::Duration::from_millis(100))
            };
            tokio::time::sleep(sleep_for).await;
        }
    }
}

/// Streams a sealed segment's data section to one target via
/// `PushSealedSegment` (64 KB chunks — perf 4.4).
#[allow(clippy::too_many_arguments)]
async fn push_to_target(
    pool: &Arc<ConnectionPool>,
    membership: &Arc<Membership>,
    target: &NodeId,
    segment_id: SegmentId,
    tier: SizeTier,
    ec_k: u8,
    ec_m: u8,
    merkle_root: Bytes,
    storage_locations: &[NodeId],
    data: &Bytes,
    timeout_ms: u64,
) -> Result<(), String> {
    let addr = membership
        .address_of(target)
        .ok_or_else(|| format!("node {target} not found in membership (replication push)"))?;
    let pooled = pool
        .get_channel(addr)
        .await
        .map_err(|e| format!("connection pool error for {target}: {e}"))?;
    let channel = pooled.channel().clone();
    drop(pooled);

    let mut client = SegmentRpcClient::new(channel);
    let proto_sid: oceanfs_core::proto::common::SegmentId = segment_id.into();
    let proto_locations: Vec<oceanfs_core::proto::common::NodeId> =
        storage_locations.iter().map(|n| n.clone().into()).collect();

    // Build the streaming request: the first chunk carries the metadata
    // + the first data slice; subsequent chunks carry data only.
    let chunk_size = 65536usize;
    let mut chunks: Vec<PushSealedSegmentRequest> =
        Vec::with_capacity(data.len().div_ceil(chunk_size).max(1));
    let mut offset = 0usize;
    let mut first = true;
    loop {
        let end = (offset + chunk_size).min(data.len());
        let slice = data.slice(offset..end);
        chunks.push(PushSealedSegmentRequest {
            segment_id: Some(proto_sid.clone()),
            tier: tier_to_u32(tier),
            ec_k: ec_k as u32,
            ec_m: ec_m as u32,
            // Metadata rides only the first chunk (the receiver captures
            // it once); later chunks repeat the id for robustness but
            // leave the metadata fields empty.
            merkle_root: if first { merkle_root.clone() } else { Bytes::new() },
            storage_locations: if first { proto_locations.clone() } else { Vec::new() },
            data: slice,
        });
        if end >= data.len() {
            break;
        }
        offset = end;
        first = false;
    }

    let deadline = Duration::from_millis(timeout_ms);
    let result =
        tokio::time::timeout(deadline, client.push_sealed_segment(tokio_stream::iter(chunks)))
            .await;

    match result {
        Err(_) => Err(format!("push to {target} timed out after {timeout_ms}ms")),
        Ok(Err(status)) => Err(format!("push to {target} failed: {status}")),
        Ok(Ok(resp)) => {
            if resp.into_inner().acked {
                debug!(segment_id = %segment_id, target = %target, "segment push acked");
                Ok(())
            } else {
                Err(format!("push to {target} not acked"))
            }
        }
    }
}

/// The seal-time segment replicator.
///
/// Drain loop: consume sealed-segment events → read the segment's data
/// section locally → push to `segment_replica_set(ring) − self` → stamp
/// `storage_locations` on the registry entry once every target acked →
/// sweep `needs_replication` on a fixed interval.
pub struct SegmentReplicator {
    ring: Arc<RingCache>,
    membership: Arc<Membership>,
    pool: Arc<ConnectionPool>,
    data_store: Arc<dyn SegmentDataStore>,
    lifecycle: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
    node_id: NodeId,
    config: ReplicationConfig,
    /// Sealed-segment event sender (the seal worker / compactor enqueue).
    tx: mpsc::Sender<SegmentId>,
    /// The drain loop's receiver end (taken by `run`; interior mutability
    /// so `run(Arc<Self>)` can take it once).
    rx: parking_lot::Mutex<Option<mpsc::Receiver<SegmentId>>>,
    /// Segments whose pushes did not fully ack — retried by the sweep.
    needs: Arc<dashmap::DashMap<SegmentId, ()>>,
    /// Shared byte-rate limiter for the push path (perf 2.6 — bounds the
    /// background replication rate so seal traffic backs off during
    /// write/read bursts, mirroring `heal_throttle_bytes_sec`).
    throttle: ByteRateLimiter,
    pushed_total: Counter,
    bytes_total: Counter,
    retries_total: Counter,
    failures_total: Counter,
    needs_gauge: Gauge,
}

impl SegmentReplicator {
    /// Creates the replicator with a bounded event channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::net::SocketAddr;
    /// use std::sync::Arc;
    /// use oceanfs_core::{GossipConfig, NodeId, RingConfig};
    /// use oceanfs_durability::InMemorySegmentStore;
    /// use oceanfs_membership::Membership;
    /// use oceanfs_network::ConnectionPool;
    /// use oceanfs_node::segment_replicator::{ReplicationConfig, SegmentReplicator};
    /// use oceanfs_routing::{Ring, RingCache};
    ///
    /// let mut ring = Ring::new(RingConfig::default());
    /// ring.add_node(NodeId::new("n1"));
    /// let ring_cache = Arc::new(RingCache::new(ring));
    /// let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    /// let membership = Arc::new(Membership::new(
    ///     NodeId::new("n1"),
    ///     addr,
    ///     addr,
    ///     GossipConfig::default(),
    ///     ring_cache.clone(),
    /// ));
    /// let lifecycle = Arc::new(
    ///     oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
    ///         &oceanfs_core::LifecycleConfig::default(),
    ///     ),
    /// );
    /// let replicator = SegmentReplicator::new(
    ///     ring_cache,
    ///     membership,
    ///     Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
    ///     Arc::new(InMemorySegmentStore::new()),
    ///     lifecycle,
    ///     NodeId::new("n1"),
    ///     ReplicationConfig::default(),
    /// );
    /// // The replicator is spawned via `run(shutdown_token)` by the node.
    /// ```
    pub fn new(
        ring: Arc<RingCache>,
        membership: Arc<Membership>,
        pool: Arc<ConnectionPool>,
        data_store: Arc<dyn SegmentDataStore>,
        lifecycle: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
        node_id: NodeId,
        config: ReplicationConfig,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let needs = Arc::new(dashmap::DashMap::new());
        let throttle = ByteRateLimiter::new(config.throttle_bytes_sec);
        Self {
            ring,
            membership,
            pool,
            data_store,
            lifecycle,
            node_id,
            config,
            tx,
            rx: parking_lot::Mutex::new(Some(rx)),
            needs,
            throttle,
            pushed_total: Counter::new(
                "oceanfs_segment_replication_pushed_total".into(),
                "Sealed segments fully replicated to all targets".into(),
                LabelSet::empty(),
            ),
            bytes_total: Counter::new(
                "oceanfs_segment_replication_bytes_total".into(),
                "Segment data bytes pushed to replicas".into(),
                LabelSet::empty(),
            ),
            retries_total: Counter::new(
                "oceanfs_segment_replication_retries_total".into(),
                "Segment replication retries (needs set sweep)".into(),
                LabelSet::empty(),
            ),
            failures_total: Counter::new(
                "oceanfs_segment_replication_failures_total".into(),
                "Segment replication failures (targets unacked)".into(),
                LabelSet::empty(),
            ),
            needs_gauge: Gauge::new(
                "oceanfs_segment_replication_needs_gauge".into(),
                "Segments currently awaiting replication (needs set)".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Returns the event sender for the seal worker / compactor
    /// (`enqueue` — non-blocking `try_send`; a full channel routes the
    /// segment into `needs_replication`, never blocks the seal path).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::SegmentId;
    /// # use oceanfs_node::segment_replicator::SegmentReplicator;
    /// # // Construction is exercised in `SegmentReplicator::new`'s
    /// # // example; here we only pin the call shape.
    /// # let _ = SegmentId::new();
    /// ```
    pub fn enqueue(&self, segment_id: SegmentId) {
        match self.tx.try_send(segment_id) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(segment_id)) => {
                // Backpressure: the channel is bounded (perf 2.6); the
                // segment must not be dropped — it lands in the needs set
                // the sweep retries.
                self.needs.insert(segment_id, ());
                self.needs_gauge.set(self.needs.len() as u64);
                debug!(
                    segment_id = %segment_id,
                    "replication channel full; segment routed to needs set"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!("replication channel closed; segment {segment_id} not enqueued");
            }
        }
    }

    /// Returns the current `needs_replication` segment count (metrics /
    /// tests).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::SegmentId;
    /// use oceanfs_node::segment_replicator::SegmentReplicator;
    /// # use oceanfs_node::segment_replicator::ReplicationConfig;
    /// # use std::sync::Arc;
    /// # use oceanfs_core::{GossipConfig, NodeId, RingConfig};
    /// # use oceanfs_routing::{Ring, RingCache};
    /// # let ring = Ring::new(RingConfig::default());
    /// # let replicator = SegmentReplicator::new(
    /// #     Arc::new(RingCache::new(ring)),
    /// #     Arc::new(oceanfs_membership::Membership::new(
    /// #         NodeId::new("n1"), "127.0.0.1:9001".parse().unwrap(),
    /// #         "127.0.0.1:9001".parse().unwrap(), GossipConfig::default(),
    /// #         Arc::new(RingCache::new(Ring::new(RingConfig::default()))),
    /// #     )),
    /// #     Arc::new(oceanfs_network::ConnectionPool::new(oceanfs_core::RpcConfig::default())),
    /// #     Arc::new(oceanfs_durability::InMemorySegmentStore::new()),
    /// #     Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
    /// #         &oceanfs_core::LifecycleConfig::default(),
    /// #     )),
    /// #     NodeId::new("n1"), ReplicationConfig::default(),
    /// # );
    /// // A fresh replicator has an empty needs set.
    /// assert_eq!(replicator.needs_len(), 0);
    /// ```
    pub fn needs_len(&self) -> usize {
        self.needs.len()
    }

    /// Returns the number of nodes in the current ring view (tests —
    /// convergence polling).
    pub fn ring_node_count(&self) -> usize {
        self.ring.snapshot().node_count()
    }

    /// Registers the replicator's metrics.
    ///
    /// # Examples
    ///
    /// ```
    /// # use oceanfs_node::segment_replicator::SegmentReplicator;
    /// # let replicator = {
    /// #     // A fresh replicator (see `SegmentReplicator::new`).
    /// #     use std::sync::Arc;
    /// #     use oceanfs_core::{GossipConfig, NodeId, RingConfig};
    /// #     use oceanfs_routing::{Ring, RingCache};
    /// #     SegmentReplicator::new(
    /// #         Arc::new(RingCache::new(Ring::new(RingConfig::default()))),
    /// #         Arc::new(oceanfs_membership::Membership::new(
    /// #             NodeId::new("n1"), "127.0.0.1:9001".parse().unwrap(),
    /// #             "127.0.0.1:9001".parse().unwrap(), GossipConfig::default(),
    /// #             Arc::new(RingCache::new(Ring::new(RingConfig::default()))),
    /// #         )),
    /// #         Arc::new(oceanfs_network::ConnectionPool::new(oceanfs_core::RpcConfig::default())),
    /// #         Arc::new(oceanfs_durability::InMemorySegmentStore::new()),
    /// #         Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
    /// #             &oceanfs_core::LifecycleConfig::default(),
    /// #         )),
    /// #         NodeId::new("n1"), oceanfs_node::segment_replicator::ReplicationConfig::default(),
    /// #     )
    /// # };
    /// use oceanfs_core::{Counter, Gauge, LabelSet, MetricRegistrar};
    ///
    /// // A minimal registrar that accepts every registration.
    /// struct NoopRegistrar;
    /// impl MetricRegistrar for NoopRegistrar {
    ///     fn register_counter(&self, _c: Counter) {}
    ///     fn register_gauge(&self, _g: Gauge) {}
    ///     fn register_histogram(&self, _h: std::sync::Arc<oceanfs_core::Histogram>) {}
    /// }
    /// let _ = LabelSet::empty();
    /// replicator.register_metrics(&NoopRegistrar);
    /// ```
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.pushed_total.clone());
        registrar.register_counter(self.bytes_total.clone());
        registrar.register_counter(self.retries_total.clone());
        registrar.register_counter(self.failures_total.clone());
        registrar.register_gauge(self.needs_gauge.clone());
    }

    /// Pushes one segment to all ring replicas (minus self).
    ///
    /// Returns `Ok(())` when every target acked; `Err` with the list of
    /// unacked targets otherwise.
    async fn replicate_segment(&self, segment_id: SegmentId) -> Result<(), Vec<NodeId>> {
        // Read the segment's metadata (tier/ec/merkle root) from the
        // registry — the replicator never invents storage shape.
        let Some(entry) = self.lifecycle.registry().get(segment_id) else {
            // The segment was deleted between enqueue and drain — nothing
            // to replicate.
            return Ok(());
        };
        // Only SEALED segments carry durable data worth replicating. A
        // Deleted entry (compaction repacked the segment away and unlinked
        // the `.dat`, or the orphan reaper reclaimed it) has nothing to
        // push — returning Ok lets `process` drop it from the needs set.
        // Without this guard, a compacted-away segment sitting in
        // `needs_replication` would fail its local read forever and be
        // retried on every sweep: a needs-set leak / hot-loop.
        if entry.state != oceanfs_storage::SegmentState::Sealed {
            debug!(
                segment_id = %segment_id,
                state = ?entry.state,
                "replication: segment no longer Sealed; nothing to replicate"
            );
            return Ok(());
        }
        let meta = &entry.metadata;
        let Some(merkle_root) = meta.merkle_root else {
            warn!(
                segment_id = %segment_id,
                "sealed segment has no merkle root; skipping replication"
            );
            return Ok(());
        };

        // Read the full data section locally (the `.dat` is durable by
        // the time the notifier fires).
        let data = match self.data_store.read_segment_data(&segment_id) {
            Ok(d) => d,
            Err(e) => {
                warn!(segment_id = %segment_id, error = %e, "replication: local read failed");
                return Err(vec![]);
            }
        };
        if data.is_empty() {
            warn!(segment_id = %segment_id, "replication: empty segment data");
            return Err(vec![]);
        }

        let targets: Vec<NodeId> = segment_replica_set(&self.ring, &segment_id)
            .into_iter()
            .filter(|n| n != &self.node_id)
            .collect();
        let locations: Vec<NodeId> = {
            let mut loc: Vec<NodeId> = vec![self.node_id.clone()];
            for t in &targets {
                if !loc.contains(t) {
                    loc.push(t.clone());
                }
            }
            loc
        };
        if targets.is_empty() {
            // No peer replicas: either a genuinely single-node ring
            // (nothing to push — the only holder is self) or the ring
            // has not converged yet (a seal that raced gossip). The
            // second case must NOT be stamped as replicated: park in
            // needs_replication so the sweep retries after the ring
            // grows. A true single-node deployment stays in the needs
            // set (the honest "cannot reach RF" state g4 will also
            // compute) — harmless, metric-visible, no network.
            debug!(
                segment_id = %segment_id,
                ring_nodes = self.ring.snapshot().node_count(),
                "replication: no peer targets; parking in needs set"
            );
            return Err(vec![]);
        }

        // Bounded background rate (perf 2.6): the aggregate push rate
        // across all targets of this segment is throttled so seal
        // replication backs off during write/read bursts. No-op when
        // `throttle_bytes_sec` is 0 (default).
        self.throttle.acquire(data.len() as u64).await;

        let merkle_bytes = Bytes::copy_from_slice(merkle_root.as_bytes());
        // Bound concurrent pushes (perf 2.7).
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrent_pushes));
        let mut handles = Vec::with_capacity(targets.len());
        for target in &targets {
            let semaphore = Arc::clone(&semaphore);
            let permit = semaphore.clone().acquire_owned().await.map_err(|_| targets.clone())?;
            let pool = Arc::clone(&self.pool);
            let membership = Arc::clone(&self.membership);
            let target = target.clone();
            let data = data.clone();
            let locations = locations.clone();
            let timeout = self.config.push_timeout_ms;
            let tier = meta.size_tier;
            let ec_k = meta.ec_k;
            let ec_m = meta.ec_m;
            let merkle_bytes = merkle_bytes.clone();
            handles.push(tokio::spawn(async move {
                let _permit = permit;
                push_to_target(
                    &pool,
                    &membership,
                    &target,
                    segment_id,
                    tier,
                    ec_k,
                    ec_m,
                    merkle_bytes,
                    &locations,
                    &data,
                    timeout,
                )
                .await
            }));
        }

        let mut unacked: Vec<NodeId> = Vec::new();
        for (target, handle) in targets.iter().zip(handles) {
            match handle.await {
                Ok(Ok(())) => {
                    self.bytes_total.add(data.len() as u64);
                }
                Ok(Err(e)) => {
                    warn!(
                        segment_id = %segment_id,
                        target = %target,
                        error = %e,
                        "replication push failed"
                    );
                    self.failures_total.add(1);
                    unacked.push(target.clone());
                }
                Err(e) => {
                    warn!(
                        segment_id = %segment_id,
                        target = %target,
                        error = %e,
                        "replication task failed"
                    );
                    self.failures_total.add(1);
                    unacked.push(target.clone());
                }
            }
        }

        if unacked.is_empty() {
            self.stamp_locations(segment_id, &locations);
            self.pushed_total.add(1);
            Ok(())
        } else {
            Err(unacked)
        }
    }

    /// Stamps the holder set on the registry entry (Sealed-only; a
    /// concurrently deleted segment is left untouched).
    fn stamp_locations(&self, segment_id: SegmentId, locations: &[NodeId]) {
        let mut set = smallvec::SmallVec::with_capacity(4);
        for loc in locations {
            set.push(loc.clone());
        }
        match self.lifecycle.set_storage_locations(segment_id, set) {
            Ok(()) => debug!(segment_id = %segment_id, "storage_locations stamped"),
            Err(e) => warn!(
                segment_id = %segment_id,
                error = ?e,
                "storage_locations stamp skipped (entry not live/sealed)"
            ),
        }
    }

    /// Runs the drain loop until shutdown: consumes sealed-segment
    /// events and periodically retries the `needs_replication` set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use oceanfs_node::segment_replicator::{ReplicationConfig, SegmentReplicator};
    /// # // A fresh replicator (see `SegmentReplicator::new` for the full
    /// # // construction) — here we only pin the run/shutdown shape.
    /// # let replicator: Arc<SegmentReplicator> = {
    /// #     use oceanfs_core::{GossipConfig, NodeId, RingConfig};
    /// #     use oceanfs_routing::{Ring, RingCache};
    /// #     Arc::new(SegmentReplicator::new(
    /// #         Arc::new(RingCache::new(Ring::new(RingConfig::default()))),
    /// #         Arc::new(oceanfs_membership::Membership::new(
    /// #             NodeId::new("n1"), "127.0.0.1:9001".parse().unwrap(),
    /// #             "127.0.0.1:9001".parse().unwrap(), GossipConfig::default(),
    /// #             Arc::new(RingCache::new(Ring::new(RingConfig::default()))),
    /// #         )),
    /// #         Arc::new(oceanfs_network::ConnectionPool::new(oceanfs_core::RpcConfig::default())),
    /// #         Arc::new(oceanfs_durability::InMemorySegmentStore::new()),
    /// #         Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
    /// #             &oceanfs_core::LifecycleConfig::default(),
    /// #         )),
    /// #         NodeId::new("n1"), ReplicationConfig::default(),
    /// #     ))
    /// # };
    /// let shutdown = tokio_util::sync::CancellationToken::new();
    /// let shutdown_for_task = shutdown.clone();
    /// tokio::spawn(async move { replicator.run(shutdown_for_task).await });
    /// shutdown.cancel();
    /// ```
    pub async fn run(self: Arc<Self>, shutdown: tokio_util::sync::CancellationToken) {
        let mut rx = match self.rx.lock().take() {
            Some(rx) => rx,
            None => {
                warn!("segment replicator run called twice; second run exits");
                return;
            }
        };
        let sweep_interval = Duration::from_secs(self.config.retry_sweep_secs);
        let mut sweep = tokio::time::interval(sweep_interval);
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The first tick fires immediately; delay it so the sweep does not
        // race the first batch of enqueues.
        sweep.tick().await;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("segment replicator shutting down");
                    break;
                }
                event = rx.recv() => {
                    match event {
                        Some(segment_id) => {
                            self.process(segment_id).await;
                        }
                        None => {
                            // All senders dropped — the replicator is done.
                            break;
                        }
                    }
                }
                _ = sweep.tick() => {
                    self.sweep_needs().await;
                }
            }
        }
    }

    /// Handles one sealed-segment event: replicate, and on partial ack
    /// park the segment in `needs_replication`.
    async fn process(&self, segment_id: SegmentId) {
        match self.replicate_segment(segment_id).await {
            Ok(()) => {
                self.needs.remove(&segment_id);
            }
            Err(_unacked) => {
                self.needs.insert(segment_id, ());
                self.retries_total.add(1);
            }
        }
        self.needs_gauge.set(self.needs.len() as u64);
    }

    /// Retries the `needs_replication` set in bounded batches.
    ///
    /// The needs set can grow large when a target is down for a while;
    /// retrying ALL of it in one sweep tick would monopolize the drain
    /// loop (the `select!` in `run` cannot consume new seal events while
    /// the sweep is running — a retry-storm that starves fresh seals).
    /// Process at most `max_retries_per_sweep` per tick; the rest wait
    /// for the next sweep. A failed segment simply stays in the set — it
    /// is retried until a sweep succeeds or g4 reconciliation re-homes it
    /// (never dropped, never hot-looped).
    async fn sweep_needs(&self) {
        if self.needs.is_empty() {
            return;
        }
        let ids: Vec<SegmentId> = self.needs.iter().map(|e| *e.key()).collect();
        for id in ids.into_iter().take(MAX_RETRIES_PER_SWEEP) {
            self.process(id).await;
        }
    }
}

/// Maximum number of `needs_replication` retries per sweep tick (perf
/// 2.6 — bounds the retry rate so a large needs set cannot starve the
/// seal-event channel drain).
const MAX_RETRIES_PER_SWEEP: usize = 16;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::net::SocketAddr;

    use oceanfs_core::{Incarnation, NodeState, RingConfig};
    use oceanfs_durability::InMemorySegmentStore;
    use oceanfs_routing::Ring;

    use super::*;

    /// Test environment: the replicator plus the handles the tests need
    /// to seed segments / observe the ring (avoids a 5-tuple — clippy
    /// `type_complexity`).
    struct TestEnv {
        replicator: Arc<SegmentReplicator>,
        ring: Arc<RingCache>,
        membership: Arc<Membership>,
        store: Arc<InMemorySegmentStore>,
        lifecycle: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
    }

    fn make_env(node_id: &str) -> TestEnv {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 16, replication_factor: 3 });
        ring.add_node(NodeId::new(node_id));
        ring.add_node(NodeId::new("n2"));
        ring.add_node(NodeId::new("n3"));
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new(node_id),
            addr,
            addr,
            oceanfs_core::GossipConfig::default(),
            ring_cache.clone(),
        ));
        membership.upsert_node(
            NodeId::new("n2"),
            NodeState::Alive,
            Incarnation::new(1),
            Some("127.0.0.1:9002".parse().unwrap()),
        );
        membership.upsert_node(
            NodeId::new("n3"),
            NodeState::Alive,
            Incarnation::new(1),
            Some("127.0.0.1:9003".parse().unwrap()),
        );
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
        let store = Arc::new(InMemorySegmentStore::new());
        let lifecycle =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let replicator = Arc::new(SegmentReplicator::new(
            ring_cache.clone(),
            membership.clone(),
            pool,
            store.clone(),
            lifecycle.clone(),
            NodeId::new(node_id),
            ReplicationConfig { retry_sweep_secs: 1, push_timeout_ms: 500, ..Default::default() },
        ));
        TestEnv { replicator, ring: ring_cache, membership, store, lifecycle }
    }

    /// The target derivation must be `segment_replica_set − self` and
    /// must EXCLUDE self (no push to the node that already holds the
    /// sealed data).
    #[test]
    fn targets_exclude_self_and_match_ring() {
        let env = make_env("n1");
        let id = SegmentId::new();
        let full = oceanfs_routing::segment_replica_set(&env.ring, &id);
        assert_eq!(full.len(), 3);
        assert!(full.contains(&NodeId::new("n1")));
        let targets: Vec<NodeId> =
            full.iter().filter(|n| *n != &env.replicator.node_id).cloned().collect();
        assert_eq!(targets.len(), 2);
        assert!(!targets.contains(&NodeId::new("n1")));
    }

    /// A segment with no registered lifecycle entry is skipped (deleted
    /// between enqueue and drain) — no error, no needs entry.
    #[tokio::test]
    async fn missing_entry_is_skipped() {
        let env = make_env("n1");
        let result = env.replicator.replicate_segment(SegmentId::new()).await;
        assert!(result.is_ok());
        assert_eq!(env.replicator.needs_len(), 0);
    }

    /// Seeds a Sealed segment (with data + merkle root) in the lifecycle
    /// and store so `replicate_segment` has something to push.
    fn seed_sealed_segment(
        lifecycle: &Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
        store: &Arc<InMemorySegmentStore>,
        id: SegmentId,
        data: &[u8],
    ) {
        store.write_segment_data(&id, data).expect("seed store");
        let root = oceanfs_durability::MerkleTree::build(data, 0).expect("merkle").root().hash();
        let meta = oceanfs_core::SegmentMetadata {
            pool_id: 0,
            segment_id: id,
            ec_k: 1,
            ec_m: 0,
            size_tier: oceanfs_core::SizeTier::Small,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes(*root.as_bytes())),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };
        lifecycle.registry().reserve(id, meta.clone()).expect("reserve");
        lifecycle.registry().seal(id, meta).expect("seal");
    }

    /// A push with an EMPTY target set (ring not yet converged, or a
    /// single-node cluster) parks the segment in `needs_replication`
    /// instead of stamping it replicated — the gossip-race guard: the
    /// ring may grow later, and a segment stamped as fully replicated
    /// with no peers would never be pushed once convergence arrives.
    #[tokio::test]
    async fn single_node_targets_empty_parks_in_needs() {
        // Build a replicator whose ring holds ONLY self: targets = {} →
        // replicate_segment returns Err(vec![]) (the gossip-race guard —
        // park in needs so a later ring convergence retries), and the
        // needs-set sweep then stamps [self] as the sole holder.
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 16, replication_factor: 3 });
        ring.add_node(NodeId::new("n1"));
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new("n1"),
            addr,
            addr,
            oceanfs_core::GossipConfig::default(),
            ring_cache.clone(),
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
        let store = Arc::new(InMemorySegmentStore::new());
        let lifecycle =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let replicator = Arc::new(SegmentReplicator::new(
            ring_cache,
            membership,
            pool,
            store.clone(),
            lifecycle.clone(),
            NodeId::new("n1"),
            ReplicationConfig::default(),
        ));

        let id = SegmentId::new();
        seed_sealed_segment(&lifecycle, &store, id, &vec![0xABu8; 4096]);

        let result = replicator.replicate_segment(id).await;
        assert!(result.is_err(), "empty targets → parked in needs (gossip-race guard)");
        // process() parks it; the holder set must NOT be stamped (a later
        // ring convergence must re-push, and stamping [self] alone would
        // falsely mark the segment replicated).
        replicator.process(id).await;
        assert_eq!(replicator.needs_len(), 1, "parked while the ring has no peers");
        let entry = lifecycle.registry().get(id).expect("entry");
        assert!(
            entry.metadata.storage_locations.is_empty(),
            "empty targets must NOT stamp storage_locations"
        );
    }

    /// A push that cannot reach its targets (2 peers with no gRPC server
    /// running) fails and parks the segment in `needs_replication`.
    #[tokio::test]
    async fn unacked_push_lands_in_needs_set() {
        let env = make_env("n1");
        let id = SegmentId::new();
        seed_sealed_segment(&env.lifecycle, &env.store, id, &vec![0xABu8; 4096]);

        let result = env.replicator.replicate_segment(id).await;
        // Targets = n2, n3 — neither has a gRPC server; connection fails.
        assert!(result.is_err(), "peers are unreachable → unacked");
        let entry = env.lifecycle.registry().get(id).expect("entry");
        assert!(
            entry.metadata.storage_locations.is_empty(),
            "full ack required before stamping locations"
        );
        // process() (not replicate_segment) parks it in the needs set.
        env.replicator.process(id).await;
        assert_eq!(env.replicator.needs_len(), 1, "unacked segment parked in needs set");
    }

    /// A channel-full enqueue routes the segment into `needs_replication`
    /// instead of dropping it (perf 2.6 backpressure). mpsc requires a
    /// capacity ≥ 1, so use capacity 1 and pre-fill it to force Full.
    #[test]
    fn channel_full_routes_to_needs_set() {
        let env = make_env("n1");
        let config = ReplicationConfig { channel_capacity: 1, ..Default::default() };
        let replicator = SegmentReplicator::new(
            env.ring,
            env.membership,
            Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            env.store,
            env.lifecycle,
            NodeId::new("n1"),
            config,
        );
        let id1 = SegmentId::new();
        let id2 = SegmentId::new();
        // First send fills the capacity-1 channel…
        replicator.enqueue(id1);
        // …the second is a Full → parked in needs, never dropped.
        replicator.enqueue(id2);
        assert_eq!(replicator.needs_len(), 1, "channel-full enqueue parks in needs set");
    }

    /// A segment DELETED by compaction (registry state Deleted + `.dat`
    /// unlinked) must be dropped from `needs_replication`, not retried
    /// forever: without the Sealed-only guard, the local read of the
    /// unlinked `.dat` fails on every sweep — a needs-set leak / hot-loop.
    #[tokio::test]
    async fn deleted_segment_is_dropped_from_needs_set() {
        let env = make_env("n1");
        let id = SegmentId::new();
        seed_sealed_segment(&env.lifecycle, &env.store, id, &vec![0xABu8; 4096]);
        // Simulate a compaction that repacked this segment away: the
        // durable delete folds the entry to Deleted and the compactor
        // unlinks the `.dat` (the in-memory store's data is irrelevant
        // once the guard fires — the point is the registry state).
        env.lifecycle.registry().delete(id).expect("delete");

        // Park it in needs (as if a sweep had found it unacked), then
        // process: the Deleted guard must return Ok and drop it.
        env.replicator.process(id).await;
        assert_eq!(
            env.replicator.needs_len(),
            0,
            "a Deleted segment must not linger in the needs set"
        );
    }
}
