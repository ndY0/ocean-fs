//! Re-replication worker (g5 `re-replication-worker`, ADR-0030).
//!
//! The target-side executor of ADR-0029 §D4/D5/D6: consumes
//! re-replication requests (routed from a holder-side dispatcher via
//! the `RequestReReplication` RPC), fetches the full segment data from
//! a live holder, writes it through the target's own pool-aware
//! `SegmentDataStore`, registers it in the lifecycle, and stamps
//! `storage_locations`.
//!
//! ## Placement (ADR-0030 Decision 1 — target-pull)
//!
//! The node that detects under-replication acts only as a dispatcher;
//! the node whose pool will hold the new copy materializes it through
//! its own store (PlacementPolicy picks the pool, ADR-0029 f3). The
//! worker therefore runs on the acquiring target.
//!
//! ## Concurrency control
//!
//! - **Perf rule 2.7/8.5:** a `tokio::sync::Semaphore` bounds the
//!   number of concurrent repair operations to `max_concurrent_repairs`
//!   (default 16).
//! - **Perf rule 2.6:** a bounded mpsc queue (backpressure).
//! - **Perf rule 8.1:** parallel holder fetch attempts use a bounded
//!   `tokio::task::JoinSet` with `abort_all` on the first success —
//!   the same "fastest holder wins" semantics as `FuturesUnordered`,
//!   without adding the `futures` dependency (the healing service's
//!   parallel fetch pass uses the identical `JoinSet` pattern), plus a
//!   concurrency cap (perf 8.5).
//! - **Perf rule 1.3:** the fetch buffer is pre-sized to the chunk size.

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_core::{
    proto::common::SegmentId as ProtoSegmentId, NodeId, OperationTimeouts, SegmentId,
    SegmentMetadata,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_storage_api::SegmentDataStore;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{Error, HealingRpcClient, Result};

// ---------------------------------------------------------------------------
// ReRepConfig
// ---------------------------------------------------------------------------

/// Tuning for the re-replication worker.
///
/// # Examples
///
/// ```
/// use oceanfs_durability::repair::ReRepConfig;
///
/// let config = ReRepConfig::default();
/// assert_eq!(config.max_concurrent_repairs, 16);
/// assert_eq!(config.queue_capacity, 1024);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReRepConfig {
    /// Bounded repair-request queue capacity (perf 2.6 backpressure).
    pub queue_capacity: usize,
    /// Maximum concurrent repair operations (perf 2.7/8.5).
    pub max_concurrent_repairs: usize,
    /// Maximum retry attempts for a single repair before giving up.
    pub retry_limit: u32,
}

impl Default for ReRepConfig {
    fn default() -> Self {
        Self { queue_capacity: 1024, max_concurrent_repairs: 16, retry_limit: 3 }
    }
}

// ---------------------------------------------------------------------------
// RepairTargetSelector
// ---------------------------------------------------------------------------

/// Selects the node a lost segment copy should be re-replicated onto.
///
/// Injected from the node layer (ADR-0030): the durability crate never
/// touches manifests directly (ring_cache is a dev-dependency here
/// today — same boundary, heal/worker.rs:86-88). The node's
/// implementation consults the manifest cache (f7): it excludes
/// candidates with `write_degraded` / no Healthy data pool and prefers
/// the node with the most free data-pool capacity.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{NodeId, SegmentId};
/// use oceanfs_durability::repair::RepairTargetSelector;
///
/// struct AlwaysFirst;
/// impl RepairTargetSelector for AlwaysFirst {
///     fn pick_repair_target(&self, _source: &SegmentId, holders: &[NodeId]) -> Option<NodeId> {
///         holders.first().cloned()
///     }
/// }
///
/// let selector = AlwaysFirst;
/// let holders = [NodeId::new("n1"), NodeId::new("n2")];
/// assert_eq!(
///     selector.pick_repair_target(&SegmentId::new(), &holders),
///     Some(NodeId::new("n1"))
/// );
/// ```
pub trait RepairTargetSelector: Send + Sync {
    /// Picks a target node for `source` among the (already live)
    /// `holders`. Returns `None` when no eligible candidate exists
    /// (the repair parks; g4's reconciliation retries later).
    fn pick_repair_target(&self, source: &SegmentId, holders: &[NodeId]) -> Option<NodeId>;
}

// ---------------------------------------------------------------------------
// ReRepWorker
// ---------------------------------------------------------------------------

/// Background task that executes re-replication repairs on the acquiring
/// target (ADR-0030).
///
/// Drains the bounded repair-request queue (fed by the
/// `RequestReReplication` RPC handler). Each request:
///
/// 1. Fetches the full segment data from a live holder (`holders −
///    self`) via `HealingRpcClient::fetch_shard` in full-segment mode.
/// 2. Writes it through the pool-aware `SegmentDataStore` (the target's
///    own store picks the pool via PlacementPolicy).
/// 3. Registers the segment in the lifecycle (reserve + seal).
/// 4. Stamps `storage_locations` via the durable refresh path.
///
/// Concurrency is bounded by a semaphore; failures with remaining
/// retries are re-enqueued.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_durability::repair::{ReRepConfig, ReRepWorker};
/// use oceanfs_durability::InMemorySegmentStore;
///
/// let config = ReRepConfig::default();
/// let worker = std::sync::Arc::new(ReRepWorker::new(
///     config,
///     data_store,
///     lifecycle,
///     pool,
///     membership,
///     timeouts,
/// ));
/// let shutdown = CancellationToken::new();
/// let worker_for_spawn = std::sync::Arc::clone(&worker);
/// tokio::spawn(async move { worker_for_spawn.run(shutdown).await });
/// ```
pub struct ReRepWorker {
    config: ReRepConfig,
    /// Bounded queue of pending repair requests.
    queue: parking_lot::Mutex<
        Option<tokio::sync::mpsc::Receiver<crate::healing_service::ReRepRequest>>,
    >,
    /// Data store for reading/writing segment data.
    data_store: Arc<dyn SegmentDataStore>,
    /// The lifecycle coordinator — the target registers the pulled
    /// segment through the machine (ADR-0025) and stamps
    /// `storage_locations` via the durable refresh path.
    lifecycle: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
    /// Connection pool for the healing gRPC fetch.
    pool: Arc<ConnectionPool>,
    /// Membership for holder address resolution.
    membership: Arc<Membership>,
    /// Semaphore bounding concurrent repair operations.
    semaphore: Arc<Semaphore>,
    /// Per-operation timeout configuration.
    timeouts: Arc<OperationTimeouts>,
    /// Repair queue sender (fed by the RPC handler; interior mutability
    /// so the node can wire `request_re_replication` to it).
    sender:
        parking_lot::Mutex<Option<tokio::sync::mpsc::Sender<crate::healing_service::ReRepRequest>>>,
}

impl ReRepWorker {
    /// Creates a new re-replication worker.
    ///
    /// The semaphore is initialized with
    /// `config.max_concurrent_repairs` permits (perf rules 2.7, 8.5).
    ///
    /// # Panics
    ///
    /// Panics if `config.max_concurrent_repairs` is zero.
    pub fn new(
        config: ReRepConfig,
        data_store: Arc<dyn SegmentDataStore>,
        lifecycle: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
        pool: Arc<ConnectionPool>,
        membership: Arc<Membership>,
        timeouts: Arc<OperationTimeouts>,
    ) -> Self {
        assert!(config.max_concurrent_repairs > 0, "max_concurrent_repairs must be > 0");
        let (tx, rx) = tokio::sync::mpsc::channel(config.queue_capacity);
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_repairs)),
            config,
            queue: parking_lot::Mutex::new(Some(rx)),
            data_store,
            lifecycle,
            pool,
            membership,
            timeouts,
            sender: parking_lot::Mutex::new(Some(tx)),
        }
    }

    /// Returns the bounded queue sender the `RequestReReplication` RPC
    /// handler enqueues into (the `ReRepRequest` path, ADR-0030).
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use oceanfs_core::{GossipConfig, NodeId, OperationTimeouts, RingConfig};
    /// # use oceanfs_durability::{InMemorySegmentStore, repair::{ReRepConfig, ReRepWorker}};
    /// # use oceanfs_membership::Membership;
    /// # use oceanfs_network::ConnectionPool;
    /// # use oceanfs_routing::{Ring, RingCache};
    /// # let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
    /// # let membership = Arc::new(Membership::new(
    /// #     NodeId::new("n1"), "127.0.0.1:9200".parse().unwrap(),
    /// #     "127.0.0.1:9201".parse().unwrap(), GossipConfig::default(), ring,
    /// # ));
    /// # let worker = ReRepWorker::new(
    /// #     ReRepConfig::default(),
    /// #     Arc::new(InMemorySegmentStore::new()),
    /// #     Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
    /// #         &oceanfs_core::LifecycleConfig::default(),
    /// #     )),
    /// #     Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
    /// #     membership,
    /// #     Arc::new(OperationTimeouts::default()),
    /// # );
    /// assert!(worker.sender().is_some());
    /// ```
    pub fn sender(
        &self,
    ) -> Option<tokio::sync::mpsc::Sender<crate::healing_service::ReRepRequest>> {
        self.sender.lock().clone()
    }

    /// Runs the worker loop until the shutdown token is cancelled.
    ///
    /// Continuously drains the bounded queue. Each request waits for a
    /// semaphore permit (perf rules 2.7/8.5), then performs the repair;
    /// on failure with remaining retries, re-enqueues with an
    /// incremented retry count.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let mut rx = match self.queue.lock().take() {
            Some(rx) => rx,
            None => {
                warn!("ReRepWorker: queue receiver already taken, exiting");
                return;
            }
        };
        // The worker's own retry re-enqueue goes back through the same
        // bounded channel (its sender is held internally).
        let sender = self.sender.lock().clone();
        let Some(sender) = sender else {
            warn!("ReRepWorker: no queue sender, exiting");
            return;
        };

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("ReRepWorker: shutdown signal received");
                    break;
                }
                request = rx.recv() => {
                    match request {
                        Some(req) => {
                            self.process_request(req, &sender).await;
                        }
                        None => {
                            info!("ReRepWorker: queue closed, exiting");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Process a single repair request with concurrency control.
    async fn process_request(
        &self,
        request: crate::healing_service::ReRepRequest,
        sender: &tokio::sync::mpsc::Sender<crate::healing_service::ReRepRequest>,
    ) {
        let data_store = self.data_store.clone();
        let lifecycle = self.lifecycle.clone();
        let pool = self.pool.clone();
        let membership = self.membership.clone();
        let semaphore = self.semaphore.clone();
        let timeouts = self.timeouts.clone();
        let retry_limit = self.config.retry_limit;
        let sender = sender.clone();

        tokio::spawn(async move {
            // Acquire semaphore permit (perf rules 2.7, 8.5).
            let _permit = match semaphore.acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };

            match Self::execute_repair(
                &request,
                Arc::as_ref(&data_store),
                Arc::as_ref(&lifecycle),
                &pool,
                &membership,
                &timeouts,
            )
            .await
            {
                Ok(()) => {
                    info!(
                        segment_id = %request.segment_id,
                        reason = ?request.reason,
                        "re-replication succeeded"
                    );
                }
                Err(e) => {
                    if request.retry_count < retry_limit {
                        let retry_req = crate::healing_service::ReRepRequest {
                            retry_count: request.retry_count + 1,
                            ..request
                        };
                        warn!(
                            segment_id = %request.segment_id,
                            retry = retry_req.retry_count,
                            error = %e,
                            "re-replication failed, retrying"
                        );
                        if sender.try_send(retry_req.clone()).is_err() {
                            // The bounded queue is full (perf 2.6
                            // backpressure). The retry is dropped here,
                            // but the repair is NOT lost: g4's
                            // reconciliation re-detects the segment on
                            // its next tick and re-enqueues.
                            warn!(
                                segment_id = %retry_req.segment_id,
                                retry = retry_req.retry_count,
                                "re-replication retry dropped: queue full (g4 will re-enqueue)"
                            );
                        }
                    } else {
                        warn!(
                            segment_id = %request.segment_id,
                            retries = request.retry_count,
                            error = %e,
                            "re-replication permanently failed after exhausting retries"
                        );
                    }
                }
            }
        });
    }

    /// Core repair logic: fetch the full segment from a live holder,
    /// write it locally, register it, and stamp `storage_locations`.
    ///
    /// ## Steps
    ///
    /// 1. Skip if the segment is already held locally (idempotent —
    ///    a duplicate `RequestReReplication` for an already-materialized
    ///    copy is a no-op).
    /// 2. Fetch the full segment data from a live holder (`holders −
    ///    self`), trying holders in parallel and taking the first
    ///    success (perf 8.1).
    /// 3. Write via the pool-aware `SegmentDataStore` (PlacementPolicy
    ///    picks the pool on THIS node).
    /// 4. Register in the lifecycle: `request_reserve` + `request_seal`
    ///    (the machine is the only writer of lifecycle state,
    ///    ADR-0025).
    /// 5. Stamp `storage_locations` through the durable refresh path
    ///    (`request_refresh_metadata` carrying the new location set).
    async fn execute_repair(
        request: &crate::healing_service::ReRepRequest,
        data_store: &dyn SegmentDataStore,
        lifecycle: &oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator,
        pool: &Arc<ConnectionPool>,
        membership: &Arc<Membership>,
        timeouts: &OperationTimeouts,
    ) -> Result<()> {
        let segment_id = request.segment_id;

        // Step 1: idempotency — already held? Nothing to do.
        if lifecycle.registry().get(segment_id).is_some() {
            debug!(segment_id = %segment_id, "re-replication: segment already held locally");
            return Ok(());
        }

        // Step 2: fetch the full segment data from a live holder.
        let data = Self::fetch_segment_from_holders(
            segment_id,
            &request.holders,
            pool,
            membership,
            timeouts,
        )
        .await?;
        if data.is_empty() {
            return Err(Error::Storage(format!(
                "re-replication: no live holder returned data for {segment_id}"
            )));
        }

        // Step 3: verify the fetched data against the holder's seal-time
        // merkle root when the request carries one (ADR-0030 — the same
        // integrity check `push_sealed_segment` performs on the owner
        // side). A truncated or corrupt transfer is rejected BEFORE any
        // write, never materialized as a self-consistent-but-wrong copy.
        let merkle_root = crate::MerkleTree::build(&data, 0)
            .ok_or_else(|| Error::Storage("re-replication: merkle build failed".into()))?
            .root()
            .hash();
        if let Some(expected) = request.merkle_root {
            if merkle_root != expected {
                return Err(Error::Storage(format!(
                    "re-replication: fetched data for {segment_id} fails merkle verification \
                     (expected {expected}, got {merkle_root})"
                )));
            }
        }

        // Step 4: register in the lifecycle FIRST (ADR-0032 D3 — the
        // reserve precedes the write so the unified store resolves the
        // segment's pool from its registry entry; the write-before-
        // register bridge is gone). The target's metadata mirrors the
        // holder's shape — the request CARRIES the source's seal-time
        // shape (tier + EC geometry), read by the dispatcher/enqueuer
        // from its own registry entry alongside the merkle root
        // (ADR-0030). The pulled copy is registered with that real
        // shape, never a hardcoded default.
        let tier = request.tier;
        let ec_k = request.ec_k;
        let ec_m = request.ec_m;

        lifecycle
            .request_reserve(segment_id, tier, ec_k, ec_m)
            .await
            .map_err(|e| Error::Storage(format!("re-replication reserve failed: {e}")))?;

        // Step 5: write through the pool-aware store. A failure cleans
        // up its own reservation (the compactor's cleanup precedent) —
        // a Reserved entry without data is dropped by the next recovery
        // anyway, but the clean delete lets an immediate retry re-reserve.
        if let Err(write_err) = data_store
            .write_segment_data(&segment_id, &data)
            .await
            .map_err(|e| Error::Storage(format!("re-replication write failed: {e}")))
        {
            if let Err(cleanup_err) = lifecycle.request_delete(segment_id).await {
                tracing::warn!(
                    segment_id = %segment_id,
                    cleanup_error = ?cleanup_err,
                    "re-replication: write failed; reservation cleanup delete failed"
                );
            }
            return Err(write_err);
        }

        let mut meta = SegmentMetadata {
            pool_id: 0,
            segment_id,
            ec_k,
            ec_m,
            size_tier: tier,
            merkle_root: Some(merkle_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            ),
        };
        if let Err(e) = lifecycle.request_seal(segment_id, meta.clone(), None).await {
            // Already sealed (a concurrent repair won) — treat as
            // success: the copy exists.
            if matches!(e, oceanfs_storage::segment::lifecycle::TransitionError::AlreadySealed) {
                return Ok(());
            }
            return Err(Error::Storage(format!("re-replication seal failed: {e}")));
        }

        // Step 6: stamp storage_locations durably. The new holder set
        // is the request's (already live-filtered) holders plus self.
        let mut locations = smallvec::SmallVec::with_capacity(request.holders.len() + 1);
        for h in &request.holders {
            if !locations.iter().any(|l: &NodeId| l == h) {
                locations.push(h.clone());
            }
        }
        if !locations.iter().any(|l: &NodeId| l == membership.node_id()) {
            locations.push(membership.node_id().clone());
        }
        meta.storage_locations = locations;
        lifecycle
            .request_refresh_metadata(segment_id, Some(merkle_root), Some(meta.storage_locations))
            .await
            .map_err(|e| Error::Storage(format!("re-replication location stamp failed: {e}")))?;

        Ok(())
    }

    /// Fetches the full data section of a segment from any live holder.
    ///
    /// Iterates `holders − self`; the first holder that serves the full
    /// data wins (parallel attempts via a bounded `JoinSet`, perf 8.1 —
    /// the same shape as the healing service's parallel hint-fetch pass).
    async fn fetch_segment_from_holders(
        segment_id: SegmentId,
        holders: &[NodeId],
        pool: &Arc<ConnectionPool>,
        membership: &Arc<Membership>,
        timeouts: &OperationTimeouts,
    ) -> Result<Bytes> {
        use crate::healing_rpc::FetchShardRequest as GprcFetchShardRequest;

        let candidates: Vec<NodeId> =
            holders.iter().filter(|id| *id != membership.node_id()).cloned().collect();
        if candidates.is_empty() {
            return Err(Error::Storage(format!(
                "re-replication: no live holder candidates for {segment_id}"
            )));
        }

        // Bound the number of in-flight attempts (perf 8.5 — a huge
        // holder set must not spawn unbounded tasks). Each attempt is
        // independent; the first success cancels the rest via JoinSet's
        // abort_all.
        const MAX_PARALLEL_FETCHES: usize = 16;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_PARALLEL_FETCHES));
        let mut attempts = tokio::task::JoinSet::new();
        for holder in candidates {
            let holder = holder.clone();
            let semaphore = Arc::clone(&semaphore);
            let pool = pool.clone();
            let membership = membership.clone();
            let timeout_ms = timeouts.shard_fetch_ms;
            attempts.spawn(async move {
                let _permit = match semaphore.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return (holder.clone(), None),
                };
                let addr = match membership.address_of(&holder) {
                    Some(a) => a,
                    None => return (holder.clone(), None),
                };
                let pooled = match pool.get_channel(addr).await {
                    Ok(p) => p,
                    Err(_) => return (holder.clone(), None),
                };
                let channel = pooled.channel().clone();
                drop(pooled);

                // ONE deadline bounds the whole attempt: the RPC's
                // initial response AND every stream message (a holder
                // that stalls mid-stream after sending headers must not
                // hang the attempt forever — same budget, single clock).
                let attempt_deadline =
                    tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
                let proto_sid: ProtoSegmentId = segment_id.into();
                let mut client = HealingRpcClient::new(channel);
                let request = tonic::Request::new(GprcFetchShardRequest {
                    segment_id: Some(proto_sid),
                    shard_index: 0,
                    offset: 0,
                    length: 0, // full-segment mode (ADR-0030)
                });
                let result =
                    tokio::time::timeout_at(attempt_deadline, client.fetch_shard(request)).await;
                match result {
                    Ok(Ok(response)) => {
                        let mut stream = response.into_inner();
                        // Pre-size the buffer to the first chunk size
                        // (perf 1.3); the stream grows it as needed.
                        let mut buf = bytes::BytesMut::with_capacity(64 * 1024);
                        // A mid-stream error must NOT be treated as
                        // end-of-stream: a truncated transfer must not
                        // materialize a partial copy (the merkle
                        // verification in `execute_repair` is the second
                        // line of defense). On Err we reject this
                        // holder's attempt (None → try the next).
                        let mut stream_ok = true;
                        loop {
                            // Each message read races the SAME attempt
                            // deadline — a stalled (headers-only) holder
                            // is rejected, never awaited forever.
                            let msg =
                                tokio::time::timeout_at(attempt_deadline, stream.message()).await;
                            match msg {
                                Ok(Ok(Some(chunk))) => {
                                    if chunk.data.is_empty() {
                                        break;
                                    }
                                    buf.extend_from_slice(&chunk.data);
                                }
                                Ok(Ok(None)) => break,
                                Ok(Err(_)) => {
                                    stream_ok = false;
                                    break;
                                }
                                Err(_elapsed) => {
                                    // The holder stalled mid-stream —
                                    // treat as a failed attempt.
                                    stream_ok = false;
                                    break;
                                }
                            }
                        }
                        if stream_ok && !buf.is_empty() {
                            (holder, Some(buf.freeze()))
                        } else {
                            (holder, None)
                        }
                    }
                    _ => (holder, None),
                }
            });
        }

        while let Some(joined) = attempts.join_next().await {
            match joined {
                Ok((holder, Some(data))) => {
                    // The first success wins — cancel the remaining
                    // attempts (the segment is fully fetched).
                    attempts.abort_all();
                    debug!(
                        segment_id = %segment_id,
                        holder = %holder,
                        bytes = data.len(),
                        "re-replication fetched segment from holder"
                    );
                    return Ok(data);
                }
                Ok((_holder, None)) => continue,
                Err(e) => {
                    debug!(segment_id = %segment_id, error = %e, "re-replication fetch attempt failed");
                }
            }
        }

        Err(Error::Storage(format!(
            "re-replication: no reachable holder served segment {segment_id}"
        )))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::needless_borrow,
    clippy::useless_conversion,
    clippy::needless_borrows_for_generic_args,
    clippy::explicit_auto_deref
)]
mod tests {
    use oceanfs_core::GossipConfig;

    use super::*;
    use crate::{
        anti_entropy::InMemorySegmentStore,
        healing_service::{ReRepRequest, RepairReason},
    };

    fn test_membership(node_id: &str) -> Arc<Membership> {
        let ring = Arc::new(oceanfs_routing::RingCache::new(oceanfs_routing::Ring::new(
            oceanfs_core::RingConfig::default(),
        )));
        Arc::new(Membership::new(
            NodeId::new(node_id),
            "127.0.0.1:9200".parse().unwrap(),
            "127.0.0.1:9201".parse().unwrap(),
            GossipConfig::default(),
            ring,
        ))
    }

    /// A RepairTargetSelector that always returns the first holder.
    struct FirstHolder;

    impl RepairTargetSelector for FirstHolder {
        fn pick_repair_target(&self, _source: &SegmentId, holders: &[NodeId]) -> Option<NodeId> {
            holders.first().cloned()
        }
    }

    #[test]
    fn rep_config_defaults_are_sane() {
        let config = ReRepConfig::default();
        assert_eq!(config.max_concurrent_repairs, 16);
        assert_eq!(config.queue_capacity, 1024);
        assert_eq!(config.retry_limit, 3);
    }

    #[test]
    fn repair_selector_trait_shape() {
        let selector = FirstHolder;
        let holders = [NodeId::new("n1"), NodeId::new("n2")];
        assert_eq!(
            selector.pick_repair_target(&SegmentId::new(), &holders),
            Some(NodeId::new("n1"))
        );
        assert_eq!(selector.pick_repair_target(&SegmentId::new(), &[]), None);
    }

    #[test]
    #[should_panic(expected = "max_concurrent_repairs must be > 0")]
    fn rep_worker_rejects_zero_concurrency() {
        let config = ReRepConfig { max_concurrent_repairs: 0, ..Default::default() };
        let lifecycle =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let _worker = ReRepWorker::new(
            config,
            Arc::new(InMemorySegmentStore::new()),
            lifecycle,
            Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            test_membership("n1"),
            Arc::new(OperationTimeouts::default()),
        );
    }

    /// The fetch returns an error when no holder is reachable.
    #[tokio::test]
    async fn fetch_segment_from_holders_errors_when_no_reachable() {
        // No gRPC server behind the members' addresses → every attempt
        // fails; the fetch must error, not hang or panic.
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
        let membership = test_membership("n1");
        // Add a holder with a bogus address (no server).
        membership.upsert_node(
            NodeId::new("n2"),
            oceanfs_core::NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            Some("127.0.0.1:1".parse().unwrap()),
        );
        let result = ReRepWorker::fetch_segment_from_holders(
            SegmentId::new(),
            &[NodeId::new("n2")],
            &pool,
            &membership,
            &OperationTimeouts { shard_fetch_ms: 100, ..Default::default() },
        )
        .await;
        assert!(result.is_err(), "no reachable holder → error");
    }

    /// The semaphore bounds concurrent repairs to
    /// `max_concurrent_repairs` permits (perf 2.7/8.5).
    #[tokio::test]
    async fn semaphore_bounds_concurrent_repairs() {
        let worker = ReRepWorker::new(
            ReRepConfig { max_concurrent_repairs: 3, ..Default::default() },
            Arc::new(InMemorySegmentStore::new()),
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            test_membership("n1"),
            Arc::new(OperationTimeouts::default()),
        );
        // All 3 permits are available on a fresh worker.
        assert_eq!(worker.semaphore.available_permits(), 3);
        // Acquiring all 3 exhausts the bound; a 4th acquisition waits.
        let _p1 = worker.semaphore.acquire().await.unwrap();
        let _p2 = worker.semaphore.acquire().await.unwrap();
        let _p3 = worker.semaphore.acquire().await.unwrap();
        assert_eq!(worker.semaphore.available_permits(), 0);
    }

    /// A lifecycle coordinator with a real event WAL (the durable writer
    /// `request_reserve`/`request_seal`/`request_refresh_metadata`
    /// require).
    async fn make_lifecycle(
    ) -> Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator> {
        let tmp = tempfile::TempDir::new().unwrap();
        let event_wal = Arc::new(
            oceanfs_storage::segment::event_wal::EventWal::open(
                tmp.path().join("event-wal"),
                &oceanfs_core::EventWalConfig {
                    event_wal_dir: tmp.path().join("event-wal"),
                    event_wal_file_size_bytes: 1024 * 1024,
                    event_wal_fsync_batch_timeout_ms: 10,
                    event_wal_checkpoint_bytes: 1024 * 1024,
                },
            )
            .await
            .unwrap(),
        );
        Arc::new(
            oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            )
            .with_event_wal(event_wal),
        )
    }

    /// End-to-end worker unit test (the feature doc's "worker processes a
    /// request end-to-end with an in-memory data store"): a real healing
    /// gRPC service on the holder serves the full segment via
    /// `fetch_shard`; the worker pulls it, verifies the merkle root,
    /// writes it through the in-memory store, registers it (reserve +
    /// seal), and stamps `storage_locations` with itself.
    #[tokio::test]
    async fn worker_pulls_writes_registers_and_stamps_end_to_end() {
        use oceanfs_core::{HlcClock, NodeState};
        use oceanfs_network::ConnectionPool as Pool;
        use tokio_stream::wrappers::TcpListenerStream;

        use crate::{
            healing_rpc::healing_rpc_server::HealingRpcServer, healing_service::HealingGrpcService,
        };

        // ---- Holder side: a real healing service serving the segment ----
        let segment_id = SegmentId::new();
        let data: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
        let expected_root = crate::MerkleTree::build(&data, 0).unwrap().root().hash();

        let holder_store = Arc::new(InMemorySegmentStore::new());
        holder_store.write_segment_data(&segment_id, &data).await.unwrap();
        let holder_service = HealingGrpcService::new(
            Arc::new(crate::HintedHandoff::new()),
            Arc::new(
                oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                    data_dir: std::env::temp_dir()
                        .join(format!("oceanfs-test-rep-holder-{}", std::process::id())),
                    ..Default::default()
                })
                .unwrap(),
            ),
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            holder_store,
            Arc::new(HlcClock::new()),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let holder_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(HealingRpcServer::new(holder_service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // ---- Target side: membership knows the holder; worker pulls ----
        let membership = test_membership("n1");
        membership.upsert_node(
            NodeId::new("holder"),
            NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            Some(holder_addr),
        );

        let target_store = Arc::new(InMemorySegmentStore::new());
        let lifecycle = make_lifecycle().await;
        let pool = Arc::new(Pool::new(oceanfs_core::RpcConfig::default()));

        // The request carries the SOURCE's real seal-time shape (a
        // Small-tier, EC k=4/m=2 segment) — the worker must register the
        // pulled copy with THIS shape, never a hardcoded default.
        let request = ReRepRequest {
            origin: NodeId::new("holder"),
            segment_id,
            holders: vec![NodeId::new("holder")],
            reason: RepairReason::Reconciliation,
            retry_count: 0,
            merkle_root: Some(expected_root),
            tier: oceanfs_core::SizeTier::Small,
            ec_k: 4,
            ec_m: 2,
        };
        let result = ReRepWorker::execute_repair(
            &request,
            &*target_store,
            &*lifecycle,
            &pool,
            &membership,
            &OperationTimeouts { shard_fetch_ms: 5_000, ..Default::default() },
        )
        .await;
        assert!(result.is_ok(), "worker must pull+write+register+stamp: {result:?}");

        // The target's store holds the byte-identical data.
        let got = target_store
            .read_segment_data(&segment_id)
            .await
            .unwrap()
            .expect("target store holds the pulled segment")
            .data;
        assert_eq!(&got[..], &data[..], "target store must hold the pulled segment");

        // The lifecycle entry exists and lists the target (self) in
        // storage_locations — the durable stamp — AND carries the
        // request's shape (the source's real tier/EC geometry, not the
        // pre-change hardcoded Standard/1/0).
        let entry = lifecycle.registry().get(segment_id).expect("registered");
        assert!(
            entry.metadata.storage_locations.iter().any(|loc| loc == membership.node_id()),
            "storage_locations must include the acquiring node (self)"
        );
        assert_eq!(
            entry.metadata.size_tier,
            oceanfs_core::SizeTier::Small,
            "the pulled copy must be registered with the source's tier"
        );
        assert_eq!(entry.metadata.ec_k, 4, "the pulled copy must carry the source's ec_k");
        assert_eq!(entry.metadata.ec_m, 2, "the pulled copy must carry the source's ec_m");
    }

    /// A corrupted/truncated fetch (merkle mismatch) is rejected — the
    /// worker must NOT materialize a self-consistent-but-wrong copy.
    /// Unlike the idempotency no-op above, this drives the REAL fetch
    /// path: a healing service serves the segment's bytes while the
    /// request carries a WRONG seal-time root; the worker must reject
    /// the transfer and leave the target store + lifecycle untouched.
    #[tokio::test]
    async fn worker_rejects_merkle_mismatch() {
        use oceanfs_core::{HlcClock, NodeState};
        use oceanfs_network::ConnectionPool as Pool;
        use tokio_stream::wrappers::TcpListenerStream;

        use crate::{
            healing_rpc::healing_rpc_server::HealingRpcServer, healing_service::HealingGrpcService,
        };

        // ---- Holder side: real service serving the REAL segment bytes ----
        let segment_id = SegmentId::new();
        let data: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
        // The request carries a WRONG root (the real root is derived
        // below and never sent) — a truncated/corrupt transfer proxy.
        let _real_root = crate::MerkleTree::build(&data, 0).unwrap().root().hash();
        let wrong_root = oceanfs_core::HashOutput::from_bytes([0xFF; 32]);
        assert_ne!(_real_root, wrong_root, "the wrong root must actually differ");

        let holder_store = Arc::new(InMemorySegmentStore::new());
        holder_store.write_segment_data(&segment_id, &data).await.unwrap();
        let holder_service = HealingGrpcService::new(
            Arc::new(crate::HintedHandoff::new()),
            Arc::new(
                oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                    data_dir: std::env::temp_dir()
                        .join(format!("oceanfs-test-rep-holder-mm-{}", std::process::id())),
                    ..Default::default()
                })
                .unwrap(),
            ),
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            holder_store,
            Arc::new(HlcClock::new()),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let holder_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(HealingRpcServer::new(holder_service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // ---- Target side: empty store + lifecycle, wrong-root request ----
        let membership = test_membership("n1");
        membership.upsert_node(
            NodeId::new("holder"),
            NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            Some(holder_addr),
        );

        let target_store = Arc::new(InMemorySegmentStore::new());
        let lifecycle = make_lifecycle().await;
        let pool = Arc::new(Pool::new(oceanfs_core::RpcConfig::default()));

        let request = ReRepRequest {
            origin: NodeId::new("holder"),
            segment_id,
            holders: vec![NodeId::new("holder")],
            reason: RepairReason::Announcement,
            retry_count: 0,
            merkle_root: Some(wrong_root),
            tier: oceanfs_core::SizeTier::Standard,
            ec_k: 1,
            ec_m: 0,
        };
        let result = ReRepWorker::execute_repair(
            &request,
            &*target_store,
            &*lifecycle,
            &pool,
            &membership,
            &OperationTimeouts { shard_fetch_ms: 5_000, ..Default::default() },
        )
        .await;
        assert!(result.is_err(), "the wrong-root fetch must be rejected, got: {result:?}");

        // No partial/wrong copy was materialized: the store is empty and
        // the lifecycle has no entry for the segment.
        assert!(
            target_store.read_segment_data(&segment_id).await.unwrap().is_none(),
            "target store must NOT hold a merkle-rejected segment"
        );
        assert!(
            lifecycle.registry().get(segment_id).is_none(),
            "lifecycle must NOT register a merkle-rejected segment"
        );
    }

    /// An already-held segment short-circuits BEFORE the fetch and the
    /// merkle verification (idempotent no-op — duplicate dispatches and
    /// the g4 re-enqueue overlap are safe).
    #[tokio::test]
    async fn worker_skips_fetch_when_segment_already_held() {
        let store = Arc::new(InMemorySegmentStore::new());
        let lifecycle =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        // An already-held segment (idempotent no-op) — the request
        // carries a WRONG root; even if the idempotency check were
        // bypassed, the verification would reject it. This asserts the
        // idempotency path: held → success, no re-write.
        let segment_id = SegmentId::new();
        let meta = SegmentMetadata {
            pool_id: 0,
            segment_id,
            ec_k: 1,
            ec_m: 0,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0x11; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1),
        };
        lifecycle.registry().reserve(segment_id, meta.clone()).unwrap();
        lifecycle.registry().seal(segment_id, meta).unwrap();

        let request = ReRepRequest {
            origin: NodeId::new("a"),
            segment_id,
            holders: vec![NodeId::new("a")],
            reason: RepairReason::Announcement,
            retry_count: 0,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xFF; 32])),
            tier: oceanfs_core::SizeTier::Standard,
            ec_k: 1,
            ec_m: 0,
        };
        let result = ReRepWorker::execute_repair(
            &request,
            &*store,
            &*lifecycle,
            &Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            &test_membership("n1"),
            &OperationTimeouts::default(),
        )
        .await;
        assert!(result.is_ok(), "already-held short-circuits before verification");
    }
}
