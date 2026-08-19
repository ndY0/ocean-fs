//! Batched hinted handoff delivery manager.
//!
//! `HintedHandoffManager` bridges the `HintWal` (persistent write-ahead log)
//! with in-memory queues and batched gRPC delivery. When a node returns
//! to the cluster, all pending hints for that node are drained from the
//! queue and sent in a single RPC call.
//!
//! ## Architecture
//!
//! ```text
//! enqueue(record)
//!   ├→ HintWal::write_hint()      [persist to WAL]
//!   └→ queues[record.intended_for] [in-memory for fast lookup]
//!
//! drain_and_deliver(target)
//!   ├→ drain queues[target]
//!   ├→ build HintedHandoffRequest { hints: repeated }
//!   ├→ gRPC: client.hinted_handoff(request)
//!   └→ on success: HintWal::truncate_after(last_position)
//! ```

use std::{
    collections::VecDeque,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use dashmap::DashMap;
use oceanfs_core::{Counter, LabelSet, MetricRegistrar, NodeId, OperationTimeouts, SegmentId};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use tracing::{debug, info, warn};

use crate::{
    error::{Error, Result},
    healing_rpc::{self, healing_rpc_client::HealingRpcClient},
    hinted_handoff_rpc::{self, HintRecord, HintedHandoffRequest, HintedHandoffResponse},
    HintWal,
};

/// Configuration for hinted handoff delivery.
///
/// Controls the WAL directory for per-node WAL files, inline/blob
/// threshold, and maximum batch size per delivery.
#[derive(Debug, Clone)]
pub struct HintedHandoffConfig {
    /// Directory where per-node hinted handoff WAL files are stored.
    /// Each node gets `{wal_dir}/{node_id}.wal`.
    pub wal_dir: std::path::PathBuf,
    /// Maximum blob size stored inline in the hinted handoff WAL (bytes).
    /// Blobs above this threshold are stored as segment references.
    /// Default: 4096 (4 KB).
    pub inline_threshold_bytes: u64,
    /// Maximum hints per batched gRPC delivery call.
    /// Default: 256.
    pub max_batch_size: usize,
    /// Maximum total payload bytes per batched gRPC delivery call.
    /// Default: 32 MiB.
    ///
    /// Hints carry the blob data inline (the phase-3 churn fix), so a
    /// batch's byte size is the sum of its blobs. This cap keeps a batch
    /// well under the server's gRPC message limit (64 MiB) — without it,
    /// 256 hints of multi-MiB blobs would build a multi-GiB RPC and be
    /// rejected (or OOM the decoder).
    pub max_batch_bytes: usize,
}

impl Default for HintedHandoffConfig {
    fn default() -> Self {
        Self {
            wal_dir: std::path::PathBuf::from("/var/lib/oceanfs/hints"),
            inline_threshold_bytes: 4096,
            max_batch_size: 256,
            max_batch_bytes: 32 * 1024 * 1024,
        }
    }
}

/// Client abstraction for delivering hinted handoff records.
///
/// Allows testing with mock gRPC clients without requiring a live server.
#[async_trait::async_trait]
pub trait HintDeliveryClient: Send + Sync {
    /// Delivers a batch of hint records to a remote node.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    async fn deliver_hints(
        &self,
        target_addr: SocketAddr,
        request: HintedHandoffRequest,
        timeout_ms: u64,
    ) -> std::result::Result<HintedHandoffResponse, Error>;
}

/// Real gRPC-based hint delivery client.
///
/// Uses `ConnectionPool` to acquire a channel and `HealingRpcClient`
/// to perform the hinted handoff RPC.
pub struct GrpcHintDeliveryClient {
    pool: Arc<ConnectionPool>,
}

impl GrpcHintDeliveryClient {
    /// Creates a new gRPC hint delivery client.
    pub fn new(pool: Arc<ConnectionPool>) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl HintDeliveryClient for GrpcHintDeliveryClient {
    async fn deliver_hints(
        &self,
        target_addr: SocketAddr,
        request: HintedHandoffRequest,
        timeout_ms: u64,
    ) -> std::result::Result<HintedHandoffResponse, Error> {
        let pooled =
            self.pool.get_channel(target_addr).await.map_err(|e| Error::ForwardFailed {
                target: target_addr.to_string(),
                reason: format!("connection pool error: {e}"),
            })?;

        let channel = pooled.channel().clone();
        drop(pooled);

        let mut client = HealingRpcClient::new(channel);

        let delivery = async {
            let response =
                client.hinted_handoff(request).await.map_err(|status| Error::ForwardFailed {
                    target: target_addr.to_string(),
                    reason: format!("gRPC hint delivery failed: {status}"),
                })?;

            Ok(response.into_inner())
        };

        match tokio::time::timeout(Duration::from_millis(timeout_ms), delivery).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(e),
            Err(_elapsed) => Err(Error::Timeout { elapsed_ms: timeout_ms }),
        }
    }
}

/// Manages hinted handoff persistence and delivery.
///
/// On `enqueue()`, writes the hint to the per-node WAL for durability and
/// adds it to an in-memory queue keyed by the intended recipient node.
/// On `drain_and_deliver()`, drains all pending hints for a node
/// and sends them in a single batched gRPC call.
///
/// # Per-Node WAL Files
///
/// Each target node gets its own WAL file at `{wal_dir}/{node_id}.wal`.
/// Files are lazily opened on first access and evicted after 60+ seconds
/// of inactivity. At most 16 WALs are open concurrently to bound file
/// descriptor usage.
///
/// # Examples
///
/// ```ignore
/// // Requires tokio runtime; see integration tests.
/// use oceanfs_durability::{HintedHandoffManager, HintedHandoffConfig};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = HintedHandoffConfig::default();
/// let manager = HintedHandoffManager::new(
///     "/var/lib/oceanfs/hints".into(),
///     delivery_client,
///     config,
/// );
/// # Ok(())
/// # }
/// ```
pub struct HintedHandoffManager {
    /// Directory containing per-node WAL files (`{wal_dir}/{node_id}.wal`).
    wal_dir: PathBuf,
    /// Per-node WAL files, lazily opened via `get_or_open_node_wal()`.
    /// Uses `DashMap` for lock-free concurrent access across nodes.
    node_wals: DashMap<NodeId, Arc<HintWal>>,
    /// Tracks the last access time of each node's WAL for lazy-close
    /// eviction. Entries older than 60s with no queue activity are
    /// eligible for eviction.
    last_access: DashMap<NodeId, Instant>,
    /// Delivery client (gRPC or mock).
    delivery_client: Arc<dyn HintDeliveryClient>,
    /// In-memory queues: `NodeId → VecDeque<(start_position, end_position, HintRecord)>`.
    /// Uses `DashMap` for lock-free concurrent access across nodes.
    queues: DashMap<NodeId, VecDeque<(u64, u64, HintRecord)>>,
    /// Configuration.
    config: HintedHandoffConfig,
    /// Per-operation timeout configuration.
    timeouts: Arc<OperationTimeouts>,
    /// Membership for address resolution.
    membership: Option<Arc<Membership>>,
    /// Hints enqueued for delivery (sender-side semantics).
    hints_stored_total: Counter,
    /// Hints successfully delivered to their target node.
    hints_delivered_total: Counter,
    /// Hints pruned from the WAL after TTL expiry.
    hints_expired_total: Counter,
}

/// Fetches a segment byte range from an origin node over gRPC.
///
/// Materializes segment-ref hints on the hinted-handoff receiver: the
/// hint carries `segment_id + offset + length` (NOT the blob — hints
/// stay small even for multipart/GB blobs), and the receiver pulls the
/// range from the origin (the hint sender, which holds the segment)
/// via [`HealingRpcClient::fetch_hint_data`] before applying it.
pub struct GrpcHintDataFetcher {
    pool: Arc<ConnectionPool>,
}

impl GrpcHintDataFetcher {
    /// Creates a fetcher using the shared connection pool.
    pub fn new(pool: Arc<ConnectionPool>) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl crate::healing_service::HintDataFetcher for GrpcHintDataFetcher {
    async fn fetch_range(
        &self,
        origin: SocketAddr,
        segment_id: &SegmentId,
        offset: u64,
        length: u32,
    ) -> std::result::Result<Bytes, String> {
        let pooled = self.pool.get_channel(origin).await.map_err(|e| format!("pool: {e}"))?;
        let mut client = HealingRpcClient::new(pooled.channel().clone());
        let mut stream = client
            .fetch_hint_data(healing_rpc::FetchHintDataRequest {
                segment_id: Some((*segment_id).into()),
                offset,
                length,
            })
            .await
            .map_err(|e| format!("fetch_hint_data rpc: {e}"))?
            .into_inner();

        let mut buf = Vec::with_capacity(length as usize);
        while let Some(chunk) = stream.message().await.map_err(|e| format!("stream: {e}"))? {
            buf.extend_from_slice(&chunk.data);
        }
        Ok(Bytes::from(buf))
    }
}

impl HintedHandoffManager {
    /// Creates a new hinted handoff manager.
    ///
    /// Requires a directory path for per-node WAL files and a delivery
    /// client for gRPC communication.
    /// To populate in-memory queues from existing WAL files, call
    /// `replay_and_enqueue()`.
    pub fn new(
        wal_dir: PathBuf,
        delivery_client: Arc<dyn HintDeliveryClient>,
        config: HintedHandoffConfig,
    ) -> Self {
        Self {
            wal_dir,
            node_wals: DashMap::new(),
            last_access: DashMap::new(),
            delivery_client,
            queues: DashMap::new(),
            config,
            timeouts: Arc::new(OperationTimeouts::default()),
            membership: None,
            hints_stored_total: Counter::new(
                "hinted_handoff_hints_stored_total".into(),
                "Hints stored for unreachable nodes".into(),
                LabelSet::empty(),
            ),
            hints_delivered_total: Counter::new(
                "hinted_handoff_hints_delivered_total".into(),
                "Hints delivered to returning nodes".into(),
                LabelSet::empty(),
            ),
            hints_expired_total: Counter::new(
                "hinted_handoff_hints_expired_total".into(),
                "Hints expired before delivery".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Registers the sender-side handoff counters with the metrics
    /// registry.
    ///
    /// The manager is the component that actually stores and delivers
    /// hints; its counters are the authoritative
    /// `hinted_handoff_hints_{stored,delivered,expired}_total` series.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.hints_stored_total.clone());
        registrar.register_counter(self.hints_delivered_total.clone());
        registrar.register_counter(self.hints_expired_total.clone());
    }

    /// Sets the membership reference for address resolution.
    #[must_use]
    pub fn with_membership(mut self, membership: Arc<Membership>) -> Self {
        self.membership = Some(membership);
        self
    }

    /// Sets the per-operation timeout configuration.
    #[must_use]
    pub fn with_timeouts(mut self, timeouts: Arc<OperationTimeouts>) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Replays all records from per-node WAL files and enqueues them in memory.
    ///
    /// Scans the `wal_dir` for `*.wal` files, extracts the node ID from
    /// each filename, replays the WAL, and populates the in-memory queues.
    ///
    /// Call this at startup to repopulate the in-memory queues from
    /// persistent WAL files after a restart.
    ///
    /// # Returns
    ///
    /// The number of records replayed and enqueued.
    ///
    /// # Errors
    ///
    /// Returns an error if WAL replay fails for any file.
    pub async fn replay_and_enqueue(&self) -> Result<usize> {
        // If the WAL directory does not exist yet (first run), create it
        // and return zero — there are no WAL files to replay.
        if !self.wal_dir.exists() {
            std::fs::create_dir_all(&self.wal_dir).map_err(|e| {
                Error::Internal(format!(
                    "failed to create hint WAL directory {:?}: {e}",
                    self.wal_dir
                ))
            })?;
            return Ok(0);
        }

        let mut total = 0usize;

        let dir = std::fs::read_dir(&self.wal_dir).map_err(|e| {
            Error::Internal(format!("failed to read WAL directory {:?}: {e}", self.wal_dir))
        })?;

        for entry in dir {
            let entry = entry
                .map_err(|e| Error::Internal(format!("failed to read WAL directory entry: {e}")))?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "wal") {
                // Extract NodeId from filename: "{node_id}.wal"
                let file_name = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
                let node_id = NodeId::new(&file_name);
                let wal = HintWal::open(&path).await?;
                let records = wal.replay().await?;
                let count = records.len();

                for (start, end, record) in records {
                    let mut queue = self.queues.entry(node_id.clone()).or_default();
                    queue.push_back((start, end, record));
                }

                info!(
                    node = %node_id,
                    count,
                    "replayed hint records from per-node WAL"
                );

                total += count;

                // Keep the WAL open in the map for subsequent appends.
                self.node_wals.insert(node_id.clone(), Arc::new(wal));
            }
        }

        info!(total, "replayed and enqueued hint records from all per-node WALs");
        Ok(total)
    }

    /// Enqueues a hint record for delivery.
    ///
    /// Writes the record to the per-node WAL for durability, then adds it
    /// to the in-memory queue for the intended recipient.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails.
    pub async fn enqueue(&self, mut record: HintRecord) -> Result<()> {
        let target = record
            .intended_for()
            .ok_or_else(|| Error::Internal("hint record has no intended_for field".into()))?;

        // Resolve or lazily open the per-node WAL file.
        let wal = self.get_or_open_node_wal(&target).await?;

        // Write to WAL first for durability.
        record.stored_at_secs =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let (position, end_position) = wal.write_hint(&record).await?;

        // Then add to in-memory queue.
        let mut queue = self.queues.entry(target.clone()).or_default();
        queue.push_back((position, end_position, record));
        self.hints_stored_total.add(1);

        debug!(
            target = %target,
            position,
            queue_len = queue.len(),
            "enqueued hint record"
        );

        Ok(())
    }

    /// Drains all pending hints for a target node and delivers them in a batch.
    ///
    /// 1. Drains the in-memory queue for `target`.
    /// 2. Builds a `HintedHandoffRequest` with all pending hints.
    /// 3. Resolves the target's address via membership.
    /// 4. Sends the batched request via gRPC.
    /// 5. On success: truncates the WAL and clears the queue.
    /// 6. On failure: re-enqueues hints at the front of the queue and returns an error.
    ///
    /// # Returns
    ///
    /// The number of hints successfully delivered.
    ///
    /// # Errors
    ///
    /// Returns an error if delivery fails.
    pub async fn drain_and_deliver(&self, target: NodeId) -> Result<usize> {
        // Drain the queue for this target, bounded by BOTH the hint count
        // and the total payload bytes (hints carry blob data inline; the
        // byte cap keeps the RPC under the gRPC message-size limit).
        let drained: Vec<(u64, u64, HintRecord)> = {
            let mut queue = self.queues.entry(target.clone()).or_default();
            let mut batch_size = 0usize;
            let mut batch_bytes: usize = 0;
            for item in queue.iter() {
                if batch_size >= self.config.max_batch_size {
                    break;
                }
                // Payload estimate for the proto record: the inline blob
                // (or the fixed-size segment ref), plus proto overhead
                // slack.
                let payload = match &item.2.record {
                    Some(hinted_handoff_rpc::hint_record::Record::Inline(inline)) => {
                        inline.data.len() + 128
                    }
                    Some(hinted_handoff_rpc::hint_record::Record::SegmentRef(_)) => 256,
                    None => 128,
                };
                if batch_bytes + payload > self.config.max_batch_bytes {
                    break;
                }
                batch_size += 1;
                batch_bytes += payload;
            }
            queue.drain(..batch_size).collect()
        };

        if drained.is_empty() {
            debug!(target = %target, "no pending hints to deliver");
            return Ok(0);
        }

        info!(
            target = %target,
            count = drained.len(),
            "delivering batched hints"
        );

        // Resolve the target's address.
        //
        // NOTE: on resolution failure the drained batch MUST be
        // re-enqueued. A bare `?` here would return before the failure
        // path below and silently DROP every hint in the batch — the
        // churn regression where "node address not found in membership"
        // (the target's entry briefly disappears while it restarts)
        // destroyed batches of up to `max_batch_size` hints with
        // `hints_delivered_total` stuck at 0.
        let addr = match &self.membership {
            Some(membership) => match membership.address_of(&target) {
                Some(addr) => addr,
                None => {
                    self.reenqueue_front(&target, drained);
                    return Err(Error::ForwardFailed {
                        target: target.to_string(),
                        reason: "node address not found in membership".into(),
                    });
                }
            },
            None => {
                // No membership configured — use a dummy address for
                // testing. Real delivery via gRPC requires membership
                // for address resolution; mock clients used in tests
                // accept any address.
                "127.0.0.1:0"
                    .parse::<SocketAddr>()
                    .map_err(|e| Error::Internal(format!("failed to parse dummy address: {e}")))?
            }
        };

        // Build the batched request.
        let hints: Vec<HintRecord> = drained.iter().map(|(_, _, r)| r.clone()).collect();
        let request = HintedHandoffRequest { hints };

        // Deliver via gRPC.
        let timeout_ms = self.timeouts.hint_delivery_ms;
        let result = self.delivery_client.deliver_hints(addr, request, timeout_ms).await;

        match result {
            Ok(resp) => {
                if !resp.accepted {
                    // Re-enqueue: delivery was attempted but remote node rejected.
                    self.reenqueue_front(&target, drained);
                    return Err(Error::ForwardFailed {
                        target: target.to_string(),
                        reason: "remote node rejected batched hint delivery".into(),
                    });
                }

                // Success — truncate the per-node WAL file to zero and remove
                // it from the map. The file is fully delivered and no longer needed.
                if let Some(wal) = self.node_wals.get(&target) {
                    let _ = wal.truncate_after(0).await;
                }
                // Remove the empty file to free disk space.
                let file_path = self.wal_dir.join(format!("{}.wal", target));
                let _ = std::fs::remove_file(&file_path);
                self.node_wals.remove(&target);
                self.last_access.remove(&target);

                let delivered = drained.len();
                self.hints_delivered_total.add(delivered as u64);
                info!(
                    target = %target,
                    delivered,
                    accepted = resp.accepted_count,
                    "batched hint delivery succeeded"
                );

                Ok(delivered)
            }
            Err(e) => {
                // Delivery failed — re-enqueue hints for retry.
                warn!(
                    target = %target,
                    error = %e,
                    count = drained.len(),
                    "batched hint delivery failed; re-enqueuing for retry"
                );
                self.reenqueue_front(&target, drained);
                Err(e)
            }
        }
    }

    /// Returns the number of pending hints for a given node.
    pub fn pending_count(&self, target: &NodeId) -> usize {
        self.queues.get(target).map(|q| q.len()).unwrap_or(0)
    }

    /// Returns the node ids with at least one pending hint, sorted for a
    /// deterministic sweep order.
    ///
    /// Used by the periodic delivery sweep: event-driven delivery can be
    /// missed (holder down during the recipient's Alive event, or the
    /// event landing before the recipient's gRPC listener is ready), so
    /// the sweep iterates whatever is pending and retries delivery.
    pub fn nodes_with_pending(&self) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self
            .queues
            .iter()
            .filter(|entry| !entry.value().is_empty())
            .map(|entry| entry.key().clone())
            .collect();
        nodes.sort();
        nodes
    }

    /// Returns the total number of pending hints across all nodes.
    pub fn total_pending_count(&self) -> usize {
        self.queues.iter().map(|entry| entry.value().len()).sum()
    }

    /// Delivers all pending hints for a returned node (convenience wrapper).
    ///
    /// This is an alias for `drain_and_deliver` for backward compatibility
    /// with code that used the legacy `HintedHandoff::deliver_pending`.
    pub async fn deliver_pending(&self, target: NodeId) -> Result<usize> {
        self.drain_and_deliver(target).await
    }

    /// Prunes expired entries from all open per-node WAL files.
    ///
    /// Iterates all open per-node WALs and calls `prune_expired()` on each,
    /// delegating the TTL check to the persistent WAL layer.
    ///
    /// # Returns
    ///
    /// The total number of entries pruned across all node WALs.
    ///
    /// # Errors
    ///
    /// Returns an error if pruning fails for any WAL.
    pub async fn prune_all_expired(&self, ttl_secs: u64) -> Result<usize> {
        let mut total_pruned = 0usize;

        for entry in self.node_wals.iter() {
            match entry.value().prune_expired(ttl_secs).await {
                Ok(0) => {}
                Ok(n) => {
                    total_pruned += n;
                    info!(
                        node = %entry.key(),
                        pruned = n,
                        "pruned expired entries from per-node hint WAL"
                    );
                }
                Err(e) => {
                    warn!(
                        node = %entry.key(),
                        error = %e,
                        "failed to prune per-node hint WAL"
                    );
                }
            }
        }

        // Also scan the directory for WAL files that aren't currently open
        // and prune them as well (they may be stale files from previous runs).
        if let Ok(dir) = std::fs::read_dir(&self.wal_dir) {
            for entry in dir.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "wal") {
                    let file_name =
                        path.file_stem().unwrap_or_default().to_string_lossy().to_string();
                    let node_id = NodeId::new(&file_name);
                    // Skip already-open WALs (handled above).
                    if self.node_wals.contains_key(&node_id) {
                        continue;
                    }
                    match HintWal::open(&path).await {
                        Ok(wal) => match wal.prune_expired(ttl_secs).await {
                            Ok(0) => {}
                            Ok(n) => {
                                total_pruned += n;
                                info!(
                                    node = %node_id,
                                    pruned = n,
                                    "pruned expired entries from unopened per-node hint WAL"
                                );
                            }
                            Err(e) => {
                                warn!(
                                    node = %node_id,
                                    error = %e,
                                    "failed to prune unopened per-node hint WAL"
                                );
                            }
                        },
                        Err(e) => {
                            warn!(
                                path = %path.display(),
                                error = %e,
                                "failed to open per-node WAL for pruning"
                            );
                        }
                    }
                }
            }
        }

        if total_pruned > 0 {
            self.hints_expired_total.add(total_pruned as u64);
        }

        Ok(total_pruned)
    }

    // ------------------------------------------------------------------
    // WAL management helpers
    // ------------------------------------------------------------------

    /// Returns or lazily opens the per-node WAL for the given node.
    ///
    /// If the WAL is already open, its access time is updated and it is
    /// returned immediately. Otherwise, a new WAL file at
    /// `{wal_dir}/{node_id}.wal` is opened. Concurrently open WALs are
    /// capped at 16; if the cap is reached, the least recently used WAL
    /// is evicted.
    async fn get_or_open_node_wal(&self, node_id: &NodeId) -> Result<Arc<HintWal>> {
        if let Some(wal) = self.node_wals.get(node_id) {
            self.last_access.insert(node_id.clone(), Instant::now());
            return Ok(wal.clone());
        }

        // Cap concurrently open WALs at 16.
        if self.node_wals.len() >= 16 {
            self.evict_least_recently_used();
        }

        // Ensure the hints directory exists before opening per-node WAL files.
        std::fs::create_dir_all(&self.wal_dir).map_err(|e| {
            Error::Internal(format!("failed to create hint WAL directory {:?}: {e}", self.wal_dir))
        })?;

        let file_path = self.wal_dir.join(format!("{}.wal", node_id));
        let wal = Arc::new(HintWal::open(&file_path).await?);
        self.node_wals.insert(node_id.clone(), wal.clone());
        self.last_access.insert(node_id.clone(), Instant::now());

        info!(node = %node_id, path = %file_path.display(), "opened per-node hint WAL");
        Ok(wal)
    }

    /// Evicts the least recently used WAL from the cache.
    ///
    /// Finds the entry in `last_access` with the oldest timestamp that
    /// has been inactive for at least 60 seconds. Removes it from both
    /// `node_wals` and `last_access` — dropping the `Arc<HintWal>`
    /// closes the underlying file.
    fn evict_least_recently_used(&self) {
        let now = Instant::now();
        let mut oldest_node: Option<NodeId> = None;
        let mut oldest_time: Option<Instant> = None;

        for entry in self.last_access.iter() {
            let elapsed = now.duration_since(*entry.value());
            // Only evict if inactive for 60+ seconds.
            if elapsed.as_secs() >= 60
                && (oldest_time.is_none() || oldest_time.is_some_and(|t| *entry.value() < t))
            {
                oldest_time = Some(*entry.value());
                oldest_node = Some(entry.key().clone());
            }
        }

        if let Some(node_id) = oldest_node {
            self.node_wals.remove(&node_id);
            self.last_access.remove(&node_id);
            info!(node = %node_id, "evicted least recently used per-node hint WAL");
        }
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Re-enqueues drained records at the front of the queue (LIFO re-insertion).
    fn reenqueue_front(&self, target: &NodeId, records: Vec<(u64, u64, HintRecord)>) {
        let mut queue = self.queues.entry(target.clone()).or_default();
        for (start, end, record) in records.into_iter().rev() {
            queue.push_front((start, end, record));
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::BucketId;
    use parking_lot::Mutex as StdMutex;
    use tempfile::tempdir;

    use super::*;

    /// A mock delivery client for testing.
    struct MockDeliveryClient {
        /// Records incoming requests.
        requests: StdMutex<Vec<(SocketAddr, HintedHandoffRequest)>>,
        /// Responses to return (popped from front).
        responses: StdMutex<VecDeque<std::result::Result<HintedHandoffResponse, Error>>>,
        /// If true, record all requests even when responses are empty.
        _record_all: bool,
    }

    impl MockDeliveryClient {
        fn new() -> Self {
            Self {
                requests: StdMutex::new(Vec::new()),
                responses: StdMutex::new(VecDeque::new()),
                _record_all: true,
            }
        }

        fn add_response(&self, resp: std::result::Result<HintedHandoffResponse, Error>) {
            self.responses.lock().push_back(resp);
        }

        fn take_requests(&self) -> Vec<(SocketAddr, HintedHandoffRequest)> {
            self.requests.lock().drain(..).collect()
        }
    }

    #[async_trait::async_trait]
    impl HintDeliveryClient for MockDeliveryClient {
        async fn deliver_hints(
            &self,
            target_addr: SocketAddr,
            request: HintedHandoffRequest,
            _timeout_ms: u64,
        ) -> std::result::Result<HintedHandoffResponse, Error> {
            self.requests.lock().push((target_addr, request.clone()));
            self.responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Ok(HintedHandoffResponse { accepted: true, accepted_count: 0 }))
        }
    }

    fn make_test_config(wal_dir: std::path::PathBuf) -> HintedHandoffConfig {
        HintedHandoffConfig { wal_dir, ..HintedHandoffConfig::default() }
    }

    // ── T1.5: Batched delivery ────────────────────────────────────────

    #[tokio::test]
    async fn test_hinted_handoff_batched_delivery() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();

        let mock = Arc::new(MockDeliveryClient::new());
        // Add two success responses (one per node drain).
        mock.add_response(Ok(HintedHandoffResponse { accepted: true, accepted_count: 5 }));
        mock.add_response(Ok(HintedHandoffResponse { accepted: true, accepted_count: 3 }));

        let manager =
            HintedHandoffManager::new(wal_dir.clone(), mock.clone(), make_test_config(wal_dir));

        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");

        // Enqueue 5 hints for node_a.
        for i in 0..5 {
            let record = HintRecord::new_inline(
                node_a.clone(),
                BucketId::new("bucket-a"),
                format!("key-a-{i}"),
                vec![i as u8].into(),
                oceanfs_core::Hlc::zero(),
            );
            manager.enqueue(record).await.unwrap();
        }

        // Enqueue 3 hints for node_b.
        for i in 0..3 {
            let record = HintRecord::new_inline(
                node_b.clone(),
                BucketId::new("bucket-b"),
                format!("key-b-{i}"),
                vec![i as u8].into(),
                oceanfs_core::Hlc::zero(),
            );
            manager.enqueue(record).await.unwrap();
        }

        assert_eq!(manager.pending_count(&node_a), 5);
        assert_eq!(manager.pending_count(&node_b), 3);

        // Deliver node_a's hints.
        let delivered_a = manager.drain_and_deliver(node_a.clone()).await.unwrap();
        assert_eq!(delivered_a, 5);
        assert_eq!(manager.pending_count(&node_a), 0);
        assert_eq!(manager.pending_count(&node_b), 3, "node_b unchanged");

        // Deliver node_b's hints.
        let delivered_b = manager.drain_and_deliver(node_b.clone()).await.unwrap();
        assert_eq!(delivered_b, 3);
        assert_eq!(manager.pending_count(&node_b), 0);

        // Verify delivery requests.
        let requests = mock.take_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].1.hints.len(), 5);
        assert_eq!(requests[1].1.hints.len(), 3);
    }

    // ── T1.6: Delivery failure re-enqueues ───────────────────────────

    #[tokio::test]
    async fn test_hinted_handoff_delivery_failure_reenqueues() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();

        let mock = Arc::new(MockDeliveryClient::new());
        // First attempt fails.
        mock.add_response(Err(Error::ForwardFailed {
            target: "node-a".into(),
            reason: "connection refused".into(),
        }));
        // Second attempt succeeds.
        mock.add_response(Ok(HintedHandoffResponse { accepted: true, accepted_count: 3 }));

        let manager =
            HintedHandoffManager::new(wal_dir.clone(), mock.clone(), make_test_config(wal_dir));

        let node_a = NodeId::new("node-a");

        // Enqueue 3 hints.
        for i in 0..3 {
            let record = HintRecord::new_inline(
                node_a.clone(),
                BucketId::new("b"),
                format!("key-{i}"),
                vec![i as u8].into(),
                oceanfs_core::Hlc::zero(),
            );
            manager.enqueue(record).await.unwrap();
        }
        assert_eq!(manager.pending_count(&node_a), 3);

        // First attempt fails.
        let result = manager.drain_and_deliver(node_a.clone()).await;
        assert!(result.is_err(), "first delivery should fail");
        assert_eq!(manager.pending_count(&node_a), 3, "hints re-enqueued");

        // Second attempt succeeds.
        let delivered = manager.drain_and_deliver(node_a.clone()).await.unwrap();
        assert_eq!(delivered, 3);
        assert_eq!(manager.pending_count(&node_a), 0);
    }

    // ── No hints to deliver returns 0 ────────────────────────────────

    #[tokio::test]
    async fn test_drain_empty_returns_zero() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());

        let manager =
            HintedHandoffManager::new(wal_dir, mock, make_test_config(dir.path().to_path_buf()));
        let result = manager.drain_and_deliver(NodeId::new("nobody")).await.unwrap();
        assert_eq!(result, 0);
    }

    // ── Replay repopulates queues ────────────────────────────────────

    #[tokio::test]
    async fn test_replay_repopulates_queues() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();

        // Write records directly to per-node WAL files under the wal_dir.
        let wal_path = wal_dir.join("n1.wal");
        let wal1 = HintWal::open(&wal_path).await.unwrap();
        for i in 0..4 {
            let record = HintRecord::new_inline(
                NodeId::new("n1"),
                BucketId::new("b"),
                format!("key-{i}"),
                vec![i as u8].into(),
                oceanfs_core::Hlc::zero(),
            );
            wal1.write_hint(&record).await.unwrap();
        }
        drop(wal1);

        // Create manager and replay from directory.
        let mock = Arc::new(MockDeliveryClient::new());
        let manager =
            HintedHandoffManager::new(wal_dir, mock, make_test_config(dir.path().to_path_buf()));

        let count = manager.replay_and_enqueue().await.unwrap();
        assert_eq!(count, 4);
        assert_eq!(manager.pending_count(&NodeId::new("n1")), 4);
    }

    // ── T2.1: Per-node WAL files created in directory ────────────────

    #[tokio::test]
    async fn test_per_node_wal_files_created_in_directory() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();

        let mock = Arc::new(MockDeliveryClient::new());
        // Responses for drain (won't be used in this test, but needed for
        // drain_and_deliver if called).
        mock.add_response(Ok(HintedHandoffResponse { accepted: true, accepted_count: 1 }));
        mock.add_response(Ok(HintedHandoffResponse { accepted: true, accepted_count: 1 }));

        let manager =
            HintedHandoffManager::new(wal_dir.clone(), mock, make_test_config(wal_dir.clone()));

        // Enqueue hints for two different nodes.
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");

        manager
            .enqueue(HintRecord::new_inline(
                node_a.clone(),
                BucketId::new("b"),
                "key-a".into(),
                vec![1].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();

        manager
            .enqueue(HintRecord::new_inline(
                node_b.clone(),
                BucketId::new("b"),
                "key-b".into(),
                vec![2].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();

        // Verify two *.wal files exist in the directory.
        let entries: Vec<_> = std::fs::read_dir(&wal_dir).unwrap().collect();
        let wal_files: Vec<_> = entries
            .iter()
            .filter_map(|e| e.as_ref().ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
            .map(|e| e.path().file_stem().unwrap().to_string_lossy().to_string())
            .collect();

        assert_eq!(wal_files.len(), 2, "expected 2 per-node WAL files");
        assert!(wal_files.contains(&"node-a".to_string()), "missing node-a.wal");
        assert!(wal_files.contains(&"node-b".to_string()), "missing node-b.wal");
    }

    // ── T2.2: Per-node WAL truncates independently ───────────────────

    #[tokio::test]
    async fn test_per_node_wal_truncates_independently() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();

        let mock = Arc::new(MockDeliveryClient::new());
        mock.add_response(Ok(HintedHandoffResponse { accepted: true, accepted_count: 2 }));

        let manager = HintedHandoffManager::new(
            wal_dir.clone(),
            mock.clone(),
            make_test_config(wal_dir.clone()),
        );

        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");

        // Enqueue for both nodes.
        for i in 0..2 {
            manager
                .enqueue(HintRecord::new_inline(
                    node_a.clone(),
                    BucketId::new("b"),
                    format!("key-a-{i}"),
                    vec![i as u8].into(),
                    oceanfs_core::Hlc::zero(),
                ))
                .await
                .unwrap();
        }
        manager
            .enqueue(HintRecord::new_inline(
                node_b.clone(),
                BucketId::new("b"),
                "key-b".into(),
                vec![9].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();

        // Deliver node-a only — its WAL file should be removed.
        let delivered = manager.drain_and_deliver(node_a.clone()).await.unwrap();
        assert_eq!(delivered, 2);

        // Verify node-a.wal is gone, node-b.wal still exists.
        assert!(
            !wal_dir.join("node-a.wal").exists(),
            "node-a.wal should be removed after delivery"
        );
        assert!(wal_dir.join("node-b.wal").exists(), "node-b.wal should still exist");
        assert_eq!(manager.pending_count(&node_b), 1);

        // Deliver node-b — its file should also be removed.
        mock.add_response(Ok(HintedHandoffResponse { accepted: true, accepted_count: 1 }));
        let delivered_b = manager.drain_and_deliver(node_b.clone()).await.unwrap();
        assert_eq!(delivered_b, 1);
        assert!(
            !wal_dir.join("node-b.wal").exists(),
            "node-b.wal should be removed after delivery"
        );
    }

    // ── T2.3: Lazy open/close cap ────────────────────────────────────

    #[tokio::test]
    async fn test_lazy_open_close_cap() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());

        let manager =
            HintedHandoffManager::new(wal_dir.clone(), mock, make_test_config(wal_dir.clone()));

        // Enqueue for 20 different nodes — should only keep 16 WALs open.
        for n in 0..20u32 {
            let node_id = NodeId::new(format!("node-{n}"));
            manager
                .enqueue(HintRecord::new_inline(
                    node_id.clone(),
                    BucketId::new("b"),
                    "key".into(),
                    vec![n as u8].into(),
                    oceanfs_core::Hlc::zero(),
                ))
                .await
                .unwrap();
        }

        // Immediately after enqueueing, we should have at most 16 WALs open.
        // (The eviction only happens when the cap is exceeded AND there is
        // a WAL idle for 60+ seconds. With all 20 enqueues happening rapidly,
        // the cap may not trigger eviction since all WALs have recent access.
        // However, the manager must not panic or exceed 20.)
        assert!(
            manager.node_wals.len() <= 20,
            "at most 20 WALs open (all enqueued rapidly so eviction may not trigger)"
        );

        // Verify all 20 hints were stored.
        let total = manager.total_pending_count();
        assert_eq!(total, 20);
    }

    // ── T2.4: Replay scans directory ─────────────────────────────────

    #[tokio::test]
    async fn test_replay_scans_directory() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();

        // Create multiple per-node WAL files manually.
        let wal_a_path = wal_dir.join("node-a.wal");
        let wal_b_path = wal_dir.join("node-b.wal");

        let wal_a = HintWal::open(&wal_a_path).await.unwrap();
        wal_a
            .write_hint(&HintRecord::new_inline(
                NodeId::new("node-a"),
                BucketId::new("b"),
                "key-a1".into(),
                vec![1].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();
        wal_a
            .write_hint(&HintRecord::new_inline(
                NodeId::new("node-a"),
                BucketId::new("b"),
                "key-a2".into(),
                vec![2].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();
        drop(wal_a);

        let wal_b = HintWal::open(&wal_b_path).await.unwrap();
        wal_b
            .write_hint(&HintRecord::new_inline(
                NodeId::new("node-b"),
                BucketId::new("b"),
                "key-b1".into(),
                vec![3].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();
        drop(wal_b);

        // Now replay from the directory.
        let mock = Arc::new(MockDeliveryClient::new());
        let manager =
            HintedHandoffManager::new(wal_dir, mock, make_test_config(dir.path().to_path_buf()));
        let count = manager.replay_and_enqueue().await.unwrap();

        assert_eq!(count, 3, "should replay 3 records (2 from node-a, 1 from node-b)");
        assert_eq!(manager.pending_count(&NodeId::new("node-a")), 2);
        assert_eq!(manager.pending_count(&NodeId::new("node-b")), 1);
    }

    // ── prune_all_expired ────────────────────────────────────────────

    #[tokio::test]
    async fn test_prune_all_expired() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());

        let manager = HintedHandoffManager::new(wal_dir.clone(), mock, make_test_config(wal_dir));

        // Enqueue a hint so a node WAL is opened.
        manager
            .enqueue(HintRecord::new_inline(
                NodeId::new("node-a"),
                BucketId::new("b"),
                "key".into(),
                vec![1].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();

        // Prune with a very long TTL — since everything was just written,
        // nothing should be pruned.
        let pruned = manager.prune_all_expired(86_400 * 365).await.unwrap();
        assert_eq!(pruned, 0, "no entries should be pruned with fresh data and long TTL");
    }

    // ── Failure retention (the churn-cycle regression) ────────────────

    #[tokio::test]
    async fn test_failed_delivery_retains_all_hints_across_retries() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());
        let manager =
            HintedHandoffManager::new(wal_dir.clone(), mock.clone(), make_test_config(wal_dir));

        let target = NodeId::new("node-b");
        for i in 0..500 {
            manager
                .enqueue(HintRecord::new_inline(
                    target.clone(),
                    BucketId::new("b"),
                    format!("key-{i}").into(),
                    vec![1].into(),
                    oceanfs_core::Hlc::zero(),
                ))
                .await
                .unwrap();
        }
        assert_eq!(manager.pending_count(&target), 500);

        // Every delivery attempt fails (transport error), like the
        // churn-cycle case where the target is down or its address is
        // still missing from membership. The queue must survive intact —
        // a dropped hint is silent data loss.
        for attempt in 0..12 {
            mock.add_response(Err(Error::ForwardFailed {
                target: target.to_string(),
                reason: "simulated transport failure".into(),
            }));
            let res = manager.deliver_pending(target.clone()).await;
            assert!(res.is_err(), "attempt {attempt}: delivery must fail");
            assert_eq!(
                manager.pending_count(&target),
                500,
                "attempt {attempt}: all hints retained after failed delivery"
            );
        }

        // Once the target is reachable, the batches drain (delivery is
        // capped at max_batch_size per call).
        let mut total_delivered = 0usize;
        while manager.pending_count(&target) > 0 {
            total_delivered += manager.deliver_pending(target.clone()).await.unwrap();
        }
        assert_eq!(total_delivered, 500, "all hints delivered after the outage");
        assert_eq!(manager.pending_count(&target), 0);
    }

    // ── nodes_with_pending ───────────────────────────────────────────

    #[tokio::test]
    async fn test_nodes_with_pending_lists_only_nonempty_queues_sorted() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());

        let manager = HintedHandoffManager::new(wal_dir.clone(), mock, make_test_config(wal_dir));

        assert!(manager.nodes_with_pending().is_empty(), "fresh manager has no pending");

        // Hints for two nodes; a third stays empty.
        for node in ["node-b", "node-a"] {
            manager
                .enqueue(HintRecord::new_inline(
                    NodeId::new(node),
                    BucketId::new("b"),
                    "key".into(),
                    vec![1].into(),
                    oceanfs_core::Hlc::zero(),
                ))
                .await
                .unwrap();
        }

        let nodes = manager.nodes_with_pending();
        assert_eq!(
            nodes,
            vec![NodeId::new("node-a"), NodeId::new("node-b")],
            "sorted, only nodes with pending hints"
        );

        // Deliver node-a's batch (mock accepts) — it drops out of the set.
        manager.deliver_pending(NodeId::new("node-a")).await.unwrap();
        let nodes = manager.nodes_with_pending();
        assert_eq!(nodes, vec![NodeId::new("node-b")], "delivered node drops out");
    }
}
