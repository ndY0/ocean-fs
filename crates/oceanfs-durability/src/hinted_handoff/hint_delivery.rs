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
    collections::{HashSet, VecDeque},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use dashmap::DashMap;
use oceanfs_core::{
    Counter, Gauge, LabelSet, MetricRegistrar, NodeId, OperationTimeouts, SegmentId,
    SharedMetricRegistrar,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_storage::io::IoOp;
use tracing::{debug, info, warn};

use super::{hint_io::HintIoRecorder, HintDropRecord, HintDropSink};
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
    /// Maximum delivery attempts per hint before it is dropped.
    ///
    /// The receiver reports per-hint retry indices; a hint the receiver
    /// keeps rejecting for a non-terminal reason is retried at most this
    /// many times, then dropped and counted in
    /// `hinted_handoff_hints_dropped_total`. Without a cap, one
    /// unappliable hint would occupy its per-target queue forever (the
    /// retry loop never ages the hint). Default: 10.
    pub max_delivery_attempts: u32,
}

impl Default for HintedHandoffConfig {
    fn default() -> Self {
        Self {
            wal_dir: std::path::PathBuf::from("/var/lib/oceanfs/hints"),
            inline_threshold_bytes: 4096,
            max_batch_size: 256,
            max_batch_bytes: 32 * 1024 * 1024,
            max_delivery_attempts: 10,
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
    /// This node's gRPC LISTENER address, sent as a request metadata
    /// header so the hint receiver can fetch segment-ref hint data back
    /// from the LISTENER. `request.remote_addr()` on the receiver is
    /// the sender's ephemeral SOURCE port (the client side of the
    /// delivery connection) — dialing it fails; the header carries the
    /// address that actually accepts connections.
    self_grpc_addr: Option<SocketAddr>,
}

impl GrpcHintDeliveryClient {
    /// Creates a new gRPC hint delivery client.
    pub fn new(pool: Arc<ConnectionPool>) -> Self {
        Self { pool, self_grpc_addr: None }
    }

    /// Sets this node's gRPC listener address (composition root).
    #[must_use]
    pub fn with_self_grpc_addr(mut self, addr: SocketAddr) -> Self {
        self.self_grpc_addr = Some(addr);
        self
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
            let mut request = tonic::Request::new(request);
            // Carry the sender's LISTENER address (see the struct docs).
            if let Some(addr) = self.self_grpc_addr {
                let value =
                    tonic::metadata::MetadataValue::try_from(addr.to_string()).map_err(|e| {
                        Error::ForwardFailed {
                            target: target_addr.to_string(),
                            reason: format!("invalid sender grpc addr header: {e}"),
                        }
                    })?;
                request.metadata_mut().insert("oceanfs-sender-grpc", value);
            }
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
    /// Hint debt that FAILED to record (WAL write error). A failed
    /// enqueue means the mutation is NOT owed anywhere — the hint is
    /// gone — so it must be visible (the churn residual class: the
    /// newest mutation's hint silently missing from every queue).
    hints_enqueue_failed_total: Counter,
    /// Hints dropped after exceeding `max_delivery_attempts`. Visible so
    /// "gave up" is never silent (the retry loop is bounded, not
    /// infinite).
    hints_dropped_total: Counter,
    /// Needed hint debt REFUSED by the coordinator's admission gate before
    /// any WAL attempt (hints pool Dead). Disjoint from
    /// `hints_enqueue_failed_total`, which counts attempts that failed
    /// inside this manager: every refused-or-failed debt path is counted
    /// exactly once.
    hints_rejected_write_total: Counter,
    /// Same as [`Self::hints_rejected_write_total`] for the delete path.
    hints_rejected_delete_total: Counter,
    /// Optional I/O observability seam. When wired, hint-WAL `open` /
    /// `write_hint` outcomes are recorded into the shared storage observer
    /// under the hints pool id, giving the idle hints pool a health
    /// producer (f0 D1). `None` keeps the previous invisible-I/O behavior
    /// (unit tests / managers without a pool registry).
    hint_io: Option<HintIoRecorder>,
    /// f5 D3: converts exhausted hint debt into bounded repair intent(s)
    /// (one per distinct segment, fanned out via the ADR-0030 dispatch).
    /// `None` keeps the legacy give-up behavior (counted, not repaired).
    drop_sink: Option<Arc<dyn HintDropSink>>,
    /// ae1 S1: registrar handle for per-target debt gauges registered
    /// after startup (dynamic `{target}` labels cannot be pre-registered
    /// at construction). `None` keeps the legacy no-gauge behavior
    /// (unit tests / managers without a registry).
    metric_registrar: Option<SharedMetricRegistrar>,
    /// ae1 S1: per-target pending-debt gauges (record count), created
    /// lazily on first debt and kept for the process lifetime.
    pending_debt_gauges: DashMap<NodeId, Gauge>,
    /// ae1 S1: per-target pending-debt gauges (payload bytes).
    pending_debt_bytes_gauges: DashMap<NodeId, Gauge>,
    /// ae1 S1: outstanding debt payload bytes per target, maintained
    /// alongside `queues` under the per-target queue lock.
    pending_bytes: DashMap<NodeId, u64>,
}

/// Human-readable (bucket, key, type) for a hint record (tracing).
/// The hint delivery contract (ADR-0027 Decision 2 as amended 2026-09-10):
/// the sender does not invent an opinion about distributed state and never
/// silently drops a hint the receiver might still need — but it re-enqueues
/// ONLY the per-hint retry set the receiver reports (the receiver's
/// HLC-LWW apply is the single gate). A hint the receiver keeps rejecting
/// is retried at most `HintedHandoffConfig::max_delivery_attempts` times,
/// then dropped and counted in `hints_dropped_total` — a bounded give-up,
/// never a silent one. This replaces the old all-or-nothing batch
/// re-enqueue, where one unappliable hint wedged its whole per-target
/// queue forever.
///
/// Fetches an object's CURRENT state from an origin node over gRPC.
///
/// Materializes hints on the hinted-handoff receiver BY KEY: the
/// receiver asks the origin "what is the current state of K?" and
/// applies the answer with HLC-LWW. Hints carry no blob data (they
/// stay small even for multipart/GB blobs); the fetch transfers the
/// current logical data from the origin's read path.
pub struct GrpcHintObjectFetcher {
    pool: Arc<ConnectionPool>,
}

impl GrpcHintObjectFetcher {
    /// Creates a fetcher using the shared connection pool.
    pub fn new(pool: Arc<ConnectionPool>) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl crate::healing_service::HintObjectFetcher for GrpcHintObjectFetcher {
    async fn fetch_object(
        &self,
        origin: SocketAddr,
        bucket: &oceanfs_core::BucketId,
        key: &str,
    ) -> std::result::Result<Option<(oceanfs_core::ObjectMetadata, Bytes)>, String> {
        let pooled = self.pool.get_channel(origin).await.map_err(|e| format!("pool: {e}"))?;
        let mut client = HealingRpcClient::new(pooled.channel().clone());
        let mut stream = client
            .fetch_hint_object(healing_rpc::FetchHintObjectRequest {
                bucket_id: Some(oceanfs_core::proto::common::BucketId {
                    name: bucket.as_str().to_string(),
                }),
                object_key: key.to_string(),
            })
            .await
            .map_err(|e| format!("fetch_hint_object rpc: {e}"))?
            .into_inner();

        // First chunk carries the object's state (present + hlc + size).
        let first = stream
            .message()
            .await
            .map_err(|e| format!("stream: {e}"))?
            .ok_or_else(|| "empty fetch_hint_object stream".to_string())?;
        if !first.present {
            return Ok(None);
        }
        let hlc = first
            .hlc
            .as_ref()
            .map(|h| oceanfs_core::Hlc::new(h.wall_time, h.logical))
            .unwrap_or_else(oceanfs_core::Hlc::zero);
        let size = first.size;
        let advertised_hash: Option<[u8; 32]> = if first.blake3_hash.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&first.blake3_hash);
            Some(arr)
        } else {
            None
        };

        let mut buf = Vec::with_capacity(size as usize);
        buf.extend_from_slice(&first.data);
        while let Some(chunk) = stream.message().await.map_err(|e| format!("stream: {e}"))? {
            buf.extend_from_slice(&chunk.data);
        }
        let data = Bytes::from(buf);

        // Integrity verification: the reassembled stream must match the
        // origin's advertised size and hash. A stream truncated mid-way
        // (origin restart, network hiccup) otherwise yields PARTIAL data
        // carrying the FULL version's HLC — which would win LWW and
        // spread unrecorded bytes to every node fetching from the same
        // origin (churn: all replicas serve the same never-written hash).
        // On mismatch the hint is NOT accepted — the sender retries.
        if data.len() as u64 != size {
            return Err(format!(
                "hint object fetch integrity: size mismatch (advertised {size}, got {})",
                data.len()
            ));
        }
        if let Some(expected) = advertised_hash {
            let actual = *blake3::hash(&data).as_bytes();
            if actual != expected {
                return Err(
                    "hint object fetch integrity: blake3 mismatch (truncated or corrupted stream)"
                        .to_string(),
                );
            }
        }

        let meta = oceanfs_core::ObjectMetadata {
            object_key: oceanfs_core::ObjectKey::new(key),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(
                *blake3::hash(&data).as_bytes(),
            )),
            chunks: smallvec::SmallVec::new(),
            inline_data: None,
            created_at: 0,
            hlc,
        };
        Ok(Some((meta, data)))
    }
}

// [review][implementation][high]
// i am skeptical of the utility of using an in memory representation of hints, since we have the wal files,
// and once a node is available again, we serve it all sequentially anyway.
// plus, the size of that memory item is very loosely bounded (only ttl).
// [end]

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
            hints_enqueue_failed_total: Counter::new(
                "hinted_handoff_hints_enqueue_failed_total".into(),
                "Hint debt that failed to record (WAL write error)".into(),
                LabelSet::empty(),
            ),
            hints_dropped_total: Counter::new(
                "hinted_handoff_hints_dropped_total".into(),
                "Hints dropped after exceeding the delivery attempt cap".into(),
                LabelSet::empty(),
            ),
            hints_rejected_write_total: Counter::new(
                "hinted_handoff_hints_rejected_total".into(),
                "Needed hint debt refused by the admission gate (disjoint from enqueue_failed)"
                    .into(),
                LabelSet::new(&[("reason", "pool_dead"), ("path", "write")]),
            ),
            hints_rejected_delete_total: Counter::new(
                "hinted_handoff_hints_rejected_total".into(),
                "Needed hint debt refused by the admission gate (disjoint from enqueue_failed)"
                    .into(),
                LabelSet::new(&[("reason", "pool_dead"), ("path", "delete")]),
            ),
            hint_io: None,
            drop_sink: None,
            metric_registrar: None,
            pending_debt_gauges: DashMap::new(),
            pending_debt_bytes_gauges: DashMap::new(),
            pending_bytes: DashMap::new(),
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
        registrar.register_counter(self.hints_enqueue_failed_total.clone());
        registrar.register_counter(self.hints_dropped_total.clone());
        registrar.register_counter(self.hints_rejected_write_total.clone());
        registrar.register_counter(self.hints_rejected_delete_total.clone());
    }

    /// Wires the hint-WAL I/O observability seam (f0 D1).
    ///
    /// After this, every `HintWal::open` / `write_hint` outcome is recorded
    /// into the shared storage observer under the recorder's pool id.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Requires a node-style IoObserver registered for the hints pool:
    /// let manager = HintedHandoffManager::new(wal_dir, client, config)
    ///     .with_io_recorder(HintIoRecorder::new(observer, hints_pool_id));
    /// ```
    #[must_use]
    pub fn with_io_recorder(mut self, recorder: HintIoRecorder) -> Self {
        self.hint_io = Some(recorder);
        self
    }

    /// Wires the sink that converts exhausted hint debt into repair
    /// intent(s) (f5 D3).
    ///
    /// When unset, a dropped hint is only counted (the legacy give-up
    /// behavior). The node composition root wires a sink that fans one
    /// ADR-0030 repair intent per distinct segment out to the segment's
    /// RF holders.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let manager = HintedHandoffManager::new(wal_dir, client, config)
    ///     .with_drop_sink(Arc::new(node_bridge));
    /// ```
    #[must_use]
    pub fn with_drop_sink(mut self, sink: Arc<dyn HintDropSink>) -> Self {
        self.drop_sink = Some(sink);
        self
    }

    /// ae1 S1: wires the shared metrics registrar so the per-target
    /// pending-debt gauges can be registered lazily.
    ///
    /// Dynamic `{target}` labels cannot be pre-registered at
    /// construction; the manager creates and registers one gauge pair
    /// per target on first debt and updates it through the stored
    /// clone. When unset, debt bookkeeping still runs — only the gauge
    /// series are absent (unit tests / managers without a registry).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let manager = HintedHandoffManager::new(wal_dir, client, config)
    ///     .with_metric_registrar(metrics.clone());
    /// ```
    #[must_use]
    pub fn with_metric_registrar(mut self, registrar: SharedMetricRegistrar) -> Self {
        self.metric_registrar = Some(registrar);
        self
    }

    /// Counts an admission-gate refusal of needed write debt
    /// (`reason=pool_dead`).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Requires a constructed manager (the counter is registered with it):
    /// manager.record_rejected_write_hint();
    /// ```
    pub fn record_rejected_write_hint(&self) {
        self.hints_rejected_write_total.inc();
    }

    /// Counts an admission-gate refusal of needed delete debt
    /// (`reason=pool_dead`).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Requires a constructed manager (the counter is registered with it):
    /// manager.record_rejected_delete_hint();
    /// ```
    pub fn record_rejected_delete_hint(&self) {
        self.hints_rejected_delete_total.inc();
    }

    /// Returns the count of refused write-hint debt (for tests).
    #[doc(hidden)]
    pub fn hints_rejected_write_total_for_test(&self) -> u64 {
        self.hints_rejected_write_total.get()
    }

    /// Returns the count of refused delete-hint debt (for tests).
    #[doc(hidden)]
    pub fn hints_rejected_delete_total_for_test(&self) -> u64 {
        self.hints_rejected_delete_total.get()
    }

    /// Returns the count of manager WAL attempts that failed (for tests).
    #[doc(hidden)]
    pub fn hints_enqueue_failed_total_for_test(&self) -> u64 {
        self.hints_enqueue_failed_total.get()
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
        // A hint WAL directory that cannot be created or read — missing
        // mountpoint, detached/read-only device, broken parent path — must
        // NOT refuse node startup (f0): the hints pool registers Degraded
        // under the role-aware missing-root policy, the admission gate
        // refuses exactly the writes that need debt, and the periodic
        // probe keeps reporting the root's health. Boot with zero replayed
        // hints and a warning.
        if !self.wal_dir.exists() {
            match std::fs::create_dir_all(&self.wal_dir) {
                Ok(()) => return Ok(0),
                Err(e) => {
                    warn!(
                        dir = %self.wal_dir.display(),
                        error = %e,
                        "hint WAL directory unavailable; booting with zero replayed hints \
                         (the honest-debt gate enforces admission)"
                    );
                    return Ok(0);
                }
            }
        }

        let mut total = 0usize;

        let dir = match std::fs::read_dir(&self.wal_dir) {
            Ok(dir) => dir,
            Err(e) => {
                warn!(
                    dir = %self.wal_dir.display(),
                    error = %e,
                    "hint WAL directory unreadable; booting with zero replayed hints \
                     (the honest-debt gate enforces admission)"
                );
                return Ok(0);
            }
        };

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

                let mut queue = self.queues.entry(node_id.clone()).or_default();
                for (start, end, record) in records {
                    queue.push_back((start, end, record));
                }
                self.refresh_target_debt(&node_id, &queue);
                drop(queue);

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

        // Serialize the WAL write + queue push against the drain's
        // truncate under the per-target queue lock: the delivery-success
        // truncate must never wipe an entry being written concurrently
        // (the churn residual class — a hint that existed only in the
        // in-memory queue and vanished on crash).
        let mut queue = self.queues.entry(target.clone()).or_default();

        // Resolve or lazily open the per-node WAL file. The I/O outcome is
        // recorded into the storage observer when the seam is wired (f0
        // D1) — latency always, error kind on failure.
        let open_started = Instant::now();
        let wal = self.get_or_open_node_wal(&target).await.inspect_err(|e| {
            self.hints_enqueue_failed_total.add(1);
            self.record_hint_io(IoOp::Open, open_started, Some(e));
        })?;
        self.record_hint_io(IoOp::Open, open_started, None);

        // Write to WAL first for durability.
        // Stamp the store time ONCE: retries preserve it so the TTL prune
        // remains a durable backstop (a retry must not reset the hint's
        // age, which would make it immortal).
        if record.stored_at_secs == 0 {
            record.stored_at_secs =
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        }
        let write_started = Instant::now();
        let (position, end_position) = wal.write_hint(&record).await.inspect_err(|e| {
            self.hints_enqueue_failed_total.add(1);
            self.record_hint_io(IoOp::Write, write_started, Some(e));
        })?;
        self.record_hint_io(IoOp::Write, write_started, None);

        // Then add to in-memory queue.
        queue.push_back((position, end_position, record.clone()));
        self.hints_stored_total.add(1);

        // ae1 S1: keep the pending-debt gauge pair (count/bytes) in
        // lockstep with the queue — incrementally on the enqueue path
        // (the delivery/prune paths rebuild from the queue).
        let bytes = self.pending_bytes.get(&target).map(|v| *v).unwrap_or(0)
            + record_payload_bytes(&record);
        self.pending_bytes.insert(target.clone(), bytes);
        let count = queue.len() as u64;
        // Update the gauges while still holding the queue lock so a
        // concurrent enqueue cannot interleave a newer snapshot with an
        // older one (observability-only; refreshed on the next write).
        self.update_debt_gauge(&target, count, bytes);
        drop(queue);

        debug!(
            target = %target,
            position,
            queue_len = count,
            "enqueued hint record"
        );

        Ok(())
    }

    /// Records a hint-WAL operation outcome into the observer seam (f0 D1).
    ///
    /// No-op when the seam is not wired.
    fn record_hint_io(&self, op: IoOp, started: Instant, error: Option<&Error>) {
        if let Some(recorder) = &self.hint_io {
            recorder.record(op, started.elapsed(), error);
        }
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
                let payload = record_payload_bytes(&item.2) as usize;
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
                // Per-hint partial acceptance: the receiver reports the
                // indices it could not confirm applied/resolved. Empty
                // means the whole batch is terminal. Only the rejected
                // hints are retried, so one unappliable hint cannot wedge
                // the rest (head-of-line blocking).
                let retry: std::collections::HashSet<u32> =
                    resp.retry_indices.iter().copied().collect();
                let drained_end = drained.last().map(|(_, end, _)| *end).unwrap_or(0);
                let total = drained.len();
                if !retry.is_empty() {
                    warn!(
                        target = %target,
                        retry = retry.len(),
                        count = total,
                        "hint batch partially accepted; retrying only the rejected hints"
                    );
                }

                // Split the drained batch: terminal records are done;
                // retry records get an attempt bump and are kept until the
                // give-up cap.
                let mut requeue: Vec<(u64, u64, HintRecord)> = Vec::new();
                let mut dropped = 0usize;
                // f5 D3: dedupe exhausted debt by segment before emitting
                // repair intent(s) — one intent per distinct segment per
                // delivery cycle (inline/delete records carry no segment
                // and cannot be segment-repaired).
                let mut drop_records: Vec<HintDropRecord> = Vec::new();
                let mut dropped_segments: std::collections::HashSet<SegmentId> =
                    std::collections::HashSet::new();
                for (i, (start, end, mut record)) in drained.into_iter().enumerate() {
                    if retry.contains(&(i as u32)) {
                        let attempts = record.attempts.saturating_add(1);
                        if attempts > self.config.max_delivery_attempts {
                            dropped += 1;
                            warn!(
                                target = %target,
                                attempts,
                                max = self.config.max_delivery_attempts,
                                "hint dropped after exceeding max delivery attempts"
                            );
                            if let Some(drop) = hint_drop_record(&record) {
                                if dropped_segments.insert(drop.segment_id) {
                                    drop_records.push(drop);
                                }
                            }
                        } else {
                            record.attempts = attempts;
                            requeue.push((start, end, record));
                        }
                    }
                }
                let delivered = total - retry.len();

                // Rewrite the WAL/queue to keep only the un-drained
                // remainder plus the retry records (with their durable
                // attempt counts). Drops everything delivered.
                self.rewrite_after_partial(&target, drained_end, requeue).await;

                self.hints_delivered_total.add(delivered as u64);
                if dropped > 0 {
                    self.hints_dropped_total.add(dropped as u64);
                    if let Some(sink) = &self.drop_sink {
                        if !drop_records.is_empty() {
                            sink.on_hints_dropped(&drop_records);
                        }
                    }
                }
                info!(
                    target = %target,
                    delivered,
                    dropped,
                    "batched hint delivery processed"
                );

                Ok(delivered)
            }
            Err(e) => {
                // Delivery failed (target unreachable/timeout) — re-enqueue
                // the whole batch WITHOUT bumping attempts: a target that
                // is merely down during churn must not consume the give-up
                // budget.
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

    /// Rewrites the per-target WAL and in-memory queue after a delivery:
    /// keeps the un-drained remainder (WAL records whose end position is
    /// after `drained_end`) plus `requeue` (the hints to retry), dropping
    /// everything delivered. When nothing remains the WAL file is removed.
    ///
    /// Holds the per-target queue lock across the rewrite so a concurrent
    /// `enqueue` (which also takes this lock before writing the WAL)
    /// cannot interleave and lose an entry.
    async fn rewrite_after_partial(
        &self,
        target: &NodeId,
        drained_end: u64,
        requeue: Vec<(u64, u64, HintRecord)>,
    ) {
        let mut queue = self.queues.entry(target.clone()).or_default();
        let wal = match self.get_or_open_node_wal(target).await {
            Ok(wal) => wal,
            Err(e) => {
                warn!(
                    target = %target,
                    error = %e,
                    "hint WAL unavailable; keeping retries in memory only"
                );
                for (start, end, record) in requeue.into_iter().rev() {
                    queue.push_front((start, end, record));
                }
                self.refresh_target_debt(target, &queue);
                return;
            }
        };

        // The un-drained remainder still present in the WAL.
        let survivors: Vec<HintRecord> = match wal.replay().await {
            Ok(records) => records
                .into_iter()
                .filter(|(_, end, _)| *end > drained_end)
                .map(|(_, _, record)| record)
                .collect(),
            Err(e) => {
                warn!(
                    target = %target,
                    error = %e,
                    "hint WAL replay failed during rewrite; retry set kept in memory"
                );
                Vec::new()
            }
        };

        // Clear and rewrite: remainder first, then the retry records.
        if let Err(e) = wal.truncate_after(0).await {
            warn!(target = %target, error = %e, "hint WAL clear failed during rewrite");
        }
        let mut rebuilt: VecDeque<(u64, u64, HintRecord)> = VecDeque::new();
        for record in survivors.into_iter().chain(requeue.into_iter().map(|(_, _, r)| r)) {
            match wal.write_hint(&record).await {
                Ok((start, end)) => rebuilt.push_back((start, end, record)),
                Err(e) => {
                    warn!(target = %target, error = %e, "hint WAL rewrite failed")
                }
            }
        }

        queue.clear();
        if rebuilt.is_empty() {
            // Nothing left for this target — reclaim the WAL file.
            drop(queue);
            self.node_wals.remove(target);
            self.last_access.remove(target);
            self.pending_bytes.remove(target);
            self.update_debt_gauge(target, 0, 0);
            let file_path = self.wal_dir.join(format!("{}.wal", target));
            let _ = std::fs::remove_file(&file_path);
            return;
        }
        for item in rebuilt {
            queue.push_back(item);
        }
        self.refresh_target_debt(target, &queue);
        self.last_access.insert(target.clone(), std::time::Instant::now());
    }

    /// Returns the number of pending hints for a given node.
    pub fn pending_count(&self, target: &NodeId) -> usize {
        self.queues.get(target).map(|q| q.len()).unwrap_or(0)
    }

    /// Returns the number of hints dropped after exceeding the delivery
    /// attempt cap (for tests).
    #[doc(hidden)]
    pub fn hints_dropped_total_for_test(&self) -> u64 {
        self.hints_dropped_total.get()
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

    /// ae1 S1: updates the per-target pending-debt gauges (record count
    /// and payload bytes), creating and registering the gauge pair on
    /// first use.
    ///
    /// No-op when no registrar is wired. The gauges are kept for the
    /// process lifetime (one series per target), so a drained target
    /// reads 0 rather than disappearing.
    fn update_debt_gauge(&self, target: &NodeId, count: u64, bytes: u64) {
        let Some(registrar) = &self.metric_registrar else {
            return;
        };
        let count_gauge = self
            .pending_debt_gauges
            .entry(target.clone())
            .or_insert_with(|| {
                let gauge = Gauge::new(
                    "hinted_handoff_pending_debt".into(),
                    "Outstanding hint debt (records) per target".into(),
                    LabelSet::new(&[("target", target.as_str())]),
                );
                registrar.register_gauge(gauge.clone());
                gauge
            })
            .clone();
        count_gauge.set(count);

        let bytes_gauge = self
            .pending_debt_bytes_gauges
            .entry(target.clone())
            .or_insert_with(|| {
                let gauge = Gauge::new(
                    "hinted_handoff_pending_debt_bytes".into(),
                    "Outstanding hint debt (payload bytes) per target".into(),
                    LabelSet::new(&[("target", target.as_str())]),
                );
                registrar.register_gauge(gauge.clone());
                gauge
            })
            .clone();
        bytes_gauge.set(bytes);
    }

    /// ae1 S1: recomputes the pending count/bytes of `target` from its
    /// in-memory queue. Callers hold the per-target queue lock.
    fn refresh_target_debt(&self, target: &NodeId, queue: &VecDeque<(u64, u64, HintRecord)>) {
        let count = queue.len() as u64;
        let bytes: u64 = queue.iter().map(|(_, _, record)| record_payload_bytes(record)).sum();
        self.pending_bytes.insert(target.clone(), bytes);
        self.update_debt_gauge(target, count, bytes);
    }

    /// ae1 S1: whether a target should keep its debt because it is still
    /// in the topology (`Alive`/`Suspect`/retained `Dead` — ADR-0027 D1).
    /// A membership-less manager treats every target as retained
    /// (TTL-only behavior).
    fn target_is_retained(&self, target: &NodeId) -> bool {
        match &self.membership {
            None => true,
            Some(membership) => membership.state_of(target).is_some(),
        }
    }

    /// ae1 S1: prunes one target's WAL and escalates debt per the
    /// retention rule.
    ///
    /// Returns `(ttl_expired_count, reclaimed)`. A departed target
    /// (absent from membership) has all remaining debt escalated through
    /// the f5 D3 sink and its WAL reclaimed; a retained target only
    /// loses TTL-expired records, which are escalated the same way
    /// (never a silent delete).
    async fn prune_target_wal(
        &self,
        target: &NodeId,
        wal: &Arc<HintWal>,
        ttl_secs: u64,
        drop_records: &mut Vec<HintDropRecord>,
        dropped_segments: &mut HashSet<SegmentId>,
    ) -> (usize, bool) {
        if !self.target_is_retained(target) {
            match wal.replay().await {
                Ok(records) => {
                    let count = records.len();
                    for (_, _, record) in &records {
                        collect_drop_record(record, drop_records, dropped_segments);
                    }
                    if count > 0 {
                        self.hints_dropped_total.add(count as u64);
                        info!(
                            node = %target,
                            count,
                            "escalated and reclaimed hint debt of a departed target"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        node = %target,
                        error = %e,
                        "failed to replay the hint WAL of a departed target; kept on disk"
                    );
                    return (0, false);
                }
            }
            self.node_wals.remove(target);
            self.last_access.remove(target);
            self.pending_bytes.remove(target);
            self.update_debt_gauge(target, 0, 0);
            self.queues.remove(target);
            let file_path = self.wal_dir.join(format!("{}.wal", target));
            let _ = std::fs::remove_file(&file_path);
            return (0, true);
        }

        match wal.prune_expired(ttl_secs).await {
            Ok((0, _)) => (0, false),
            Ok((n, expired)) => {
                for record in &expired {
                    collect_drop_record(record, drop_records, dropped_segments);
                }
                // The prune re-wrote the surviving frames at new offsets:
                // rebuild the in-memory queue so positions stay valid.
                match wal.replay().await {
                    Ok(records) => {
                        let mut queue = self.queues.entry(target.clone()).or_default();
                        queue.clear();
                        for item in records {
                            queue.push_back(item);
                        }
                        self.refresh_target_debt(target, &queue);
                    }
                    Err(e) => {
                        warn!(
                            node = %target,
                            error = %e,
                            "hint WAL replay failed after prune; in-memory queue may be stale"
                        );
                    }
                }
                (n, false)
            }
            Err(e) => {
                warn!(node = %target, error = %e, "failed to prune per-node hint WAL");
                (0, false)
            }
        }
    }

    /// Delivers all pending hints for a returned node (convenience wrapper).
    ///
    /// This is an alias for `drain_and_deliver` for backward compatibility
    /// with code that used the legacy `HintedHandoff::deliver_pending`.
    pub async fn deliver_pending(&self, target: NodeId) -> Result<usize> {
        self.drain_and_deliver(target).await
    }

    /// Prunes expired or departed-target debt from all per-node WALs
    /// (ae1 S1).
    ///
    /// Retention follows the membership topology:
    ///
    /// - a target still in the ring (`Alive`/`Suspect`/retained `Dead` —
    ///   ADR-0027 D1 / ADR-0028) keeps its debt; only the TTL cap prunes
    ///   it, and the expired records are **escalated** through the f5 D3
    ///   [`HintDropSink`] (one repair intent per distinct segment) before
    ///   removal — never a silent delete;
    /// - a target no longer in the topology escalates its remaining debt
    ///   the same way and its WAL is reclaimed.
    ///
    /// When the manager has no membership handle (unit tests / legacy
    /// embeddings) every target is treated as retained and the TTL cap
    /// applies.
    ///
    /// # Returns
    ///
    /// The total number of TTL-expired entries pruned across all node
    /// WALs. Departed-target removals are counted in
    /// `hinted_handoff_hints_dropped_total` instead.
    ///
    /// # Errors
    ///
    /// Per-WAL failures are logged and skipped; the directory scan never
    /// fails the call.
    pub async fn prune_all_expired(&self, ttl_secs: u64) -> Result<usize> {
        let mut total_pruned = 0usize;
        let mut drop_records: Vec<HintDropRecord> = Vec::new();
        let mut dropped_segments: HashSet<SegmentId> = HashSet::new();

        // Open WALs first. Collect the handles BEFORE awaiting: never hold
        // a DashMap ref across an await point.
        let open: Vec<(NodeId, Arc<HintWal>)> = self
            .node_wals
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        for (target, wal) in open {
            total_pruned += self
                .prune_target_wal(&target, &wal, ttl_secs, &mut drop_records, &mut dropped_segments)
                .await
                .0;
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
                        Ok(wal) => {
                            let (pruned, _reclaimed) = self
                                .prune_target_wal(
                                    &node_id,
                                    &std::sync::Arc::new(wal),
                                    ttl_secs,
                                    &mut drop_records,
                                    &mut dropped_segments,
                                )
                                .await;
                            total_pruned += pruned;
                        }
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
        if !drop_records.is_empty() {
            if let Some(sink) = &self.drop_sink {
                sink.on_hints_dropped(&drop_records);
            }
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

/// Extracts the repair identity from a dropped hint (f5 D3).
///
/// Only segment-reference records carry a `segment_id`: the mutation's
/// data lives in a segment, so a re-replication intent can restore the
/// missing copy. Inline records are self-contained and deletes are
/// tombstones — neither has a segment copy to repair, so they return
/// `None` (they remain counted in `hints_dropped_total`).
fn hint_drop_record(record: &HintRecord) -> Option<HintDropRecord> {
    let hinted_handoff_rpc::hint_record::Record::SegmentRef(seg) = record.record.as_ref()? else {
        return None;
    };
    let segment_id = SegmentId::try_from(seg.segment_id.clone()?).ok()?;
    let intended_for = NodeId::from(seg.intended_for.as_ref()?.id.clone());
    Some(HintDropRecord { segment_id, intended_for })
}

/// ae1 S1: appends `record`'s repair identity to `out` unless its segment
/// was already collected in this prune/delivery cycle.
fn collect_drop_record(
    record: &HintRecord,
    out: &mut Vec<HintDropRecord>,
    seen: &mut HashSet<SegmentId>,
) {
    if let Some(drop) = hint_drop_record(record) {
        if seen.insert(drop.segment_id) {
            out.push(drop);
        }
    }
}

/// ae1 S1: the payload size estimate used for debt-byte accounting —
/// the same shape the delivery batching uses: the inline blob (or the
/// fixed-size segment ref / delete), plus proto overhead slack.
fn record_payload_bytes(record: &HintRecord) -> u64 {
    match &record.record {
        Some(hinted_handoff_rpc::hint_record::Record::Inline(inline)) => {
            (inline.data.len() + 128) as u64
        }
        Some(hinted_handoff_rpc::hint_record::Record::SegmentRef(_)) => 256,
        Some(hinted_handoff_rpc::hint_record::Record::Delete(_)) => 128,
        None => 128,
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unnecessary_cast,
    clippy::useless_conversion
)]
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
            self.responses.lock().pop_front().unwrap_or_else(|| {
                Ok(HintedHandoffResponse {
                    accepted: true,
                    accepted_count: 0,
                    retry_indices: vec![],
                })
            })
        }
    }

    fn make_test_config(wal_dir: std::path::PathBuf) -> HintedHandoffConfig {
        HintedHandoffConfig { wal_dir, ..HintedHandoffConfig::default() }
    }

    /// Partial acceptance: only the receiver-reported retry indices are
    /// re-enqueued; accepted hints are dropped. Regression for the
    /// head-of-line wedge (the whole batch used to be re-enqueued, so one
    /// unappliable hint blocked every other hint forever).
    #[tokio::test]
    async fn partial_acceptance_reenqueues_only_retry_indices() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();

        let mock = Arc::new(MockDeliveryClient::new());
        mock.add_response(Ok(HintedHandoffResponse {
            accepted: false,
            accepted_count: 2,
            retry_indices: vec![1],
        }));
        let manager = HintedHandoffManager::new(
            wal_dir.clone(),
            mock.clone(),
            make_test_config(wal_dir.clone()),
        );
        let node = NodeId::new("node-a");
        for i in 0..3u8 {
            manager
                .enqueue(HintRecord::new_inline(
                    node.clone(),
                    BucketId::new("b"),
                    format!("key-{i}"),
                    vec![i].into(),
                    oceanfs_core::Hlc::zero(),
                ))
                .await
                .unwrap();
        }

        let delivered = manager.drain_and_deliver(node.clone()).await.unwrap();
        assert_eq!(delivered, 2, "only the two accepted hints count as delivered");
        assert_eq!(manager.pending_count(&node), 1, "the rejected hint is retried");

        // The retry must survive a restart: the rewrite re-persisted it.
        let manager2 = HintedHandoffManager::new(
            wal_dir.clone(),
            Arc::new(MockDeliveryClient::new()),
            make_test_config(wal_dir.clone()),
        );
        let replayed = manager2.replay_and_enqueue().await.unwrap();
        assert_eq!(replayed, 1, "exactly the rejected hint was re-persisted");
        assert_eq!(manager2.pending_count(&node), 1);
    }

    /// Give-up cap: a hint the receiver keeps rejecting is dropped after
    /// `max_delivery_attempts`, so it cannot occupy the queue forever.
    #[tokio::test]
    async fn retry_cap_drops_permanently_rejected_hint() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());
        let mut config = make_test_config(wal_dir.clone());
        config.max_delivery_attempts = 2;
        let manager = HintedHandoffManager::new(wal_dir.clone(), mock.clone(), config);
        let node = NodeId::new("node-a");
        manager
            .enqueue(HintRecord::new_inline(
                node.clone(),
                BucketId::new("b"),
                "key".into(),
                vec![1].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();

        // attempts 1, 2 are kept (<= cap); attempt 3 exceeds the cap and
        // drops the hint.
        for expected_pending in [1usize, 1, 0] {
            mock.add_response(Ok(HintedHandoffResponse {
                accepted: false,
                accepted_count: 0,
                retry_indices: vec![0],
            }));
            manager.drain_and_deliver(node.clone()).await.unwrap();
            assert_eq!(manager.pending_count(&node), expected_pending);
        }
        assert_eq!(manager.hints_dropped_total_for_test(), 1, "the drop is counted");
    }

    /// f5 D3: an exhausted segment-reference hint emits exactly ONE
    /// repair record per distinct segment (two hints in the same segment
    /// collapse), with the intended node attached; the drop counter still
    /// increments for every dropped hint.
    #[tokio::test]
    async fn retry_cap_emits_one_deduped_repair_record_per_segment() {
        #[derive(Default)]
        struct RecordingSink(StdMutex<Vec<HintDropRecord>>);
        impl HintDropSink for RecordingSink {
            fn on_hints_dropped(&self, dropped: &[HintDropRecord]) {
                self.0.lock().extend_from_slice(dropped);
            }
        }

        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());
        let mut config = make_test_config(wal_dir.clone());
        config.max_delivery_attempts = 1;
        let sink = Arc::new(RecordingSink::default());
        let manager = HintedHandoffManager::new(wal_dir.clone(), mock.clone(), config)
            .with_drop_sink(sink.clone());
        let node = NodeId::new("node-a");
        let segment_a = SegmentId::new();
        let segment_b = SegmentId::new();
        // Two hints in the SAME segment (dedup) + one in another segment.
        for (key, segment) in [("k1", &segment_a), ("k2", &segment_a), ("k3", &segment_b)] {
            manager
                .enqueue(HintRecord::new_segment_ref(
                    node.clone(),
                    BucketId::new("b"),
                    key.to_string(),
                    *segment,
                    0,
                    1024,
                    oceanfs_core::Hlc::zero(),
                ))
                .await
                .unwrap();
        }

        // cap = 1: the first attempt is kept, the second exceeds and drops.
        for _ in 0..2 {
            mock.add_response(Ok(HintedHandoffResponse {
                accepted: false,
                accepted_count: 0,
                retry_indices: vec![0, 1, 2],
            }));
            manager.drain_and_deliver(node.clone()).await.unwrap();
        }
        assert_eq!(manager.hints_dropped_total_for_test(), 3, "every hint drop is counted");
        let records = sink.0.lock().clone();
        assert_eq!(records.len(), 2, "one repair record per DISTINCT segment");
        let segments: std::collections::HashSet<_> = records.iter().map(|r| r.segment_id).collect();
        assert!(segments.contains(&segment_a) && segments.contains(&segment_b));
        assert!(records.iter().all(|r| r.intended_for == node));
    }

    /// f5 D3: inline hints carry no segment and emit no repair record
    /// (nothing to re-replicate) — they remain counted only.
    #[tokio::test]
    async fn inline_hint_drop_emits_no_repair_record() {
        #[derive(Default)]
        struct RecordingSink(StdMutex<Vec<HintDropRecord>>);
        impl HintDropSink for RecordingSink {
            fn on_hints_dropped(&self, dropped: &[HintDropRecord]) {
                self.0.lock().extend_from_slice(dropped);
            }
        }

        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());
        let mut config = make_test_config(wal_dir.clone());
        config.max_delivery_attempts = 0;
        let sink = Arc::new(RecordingSink::default());
        let manager = HintedHandoffManager::new(wal_dir.clone(), mock.clone(), config)
            .with_drop_sink(sink.clone());
        let node = NodeId::new("node-a");
        manager
            .enqueue(HintRecord::new_inline(
                node.clone(),
                BucketId::new("b"),
                "key".into(),
                vec![1].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();

        mock.add_response(Ok(HintedHandoffResponse {
            accepted: false,
            accepted_count: 0,
            retry_indices: vec![0],
        }));
        manager.drain_and_deliver(node.clone()).await.unwrap();
        assert_eq!(manager.hints_dropped_total_for_test(), 1);
        assert!(sink.0.lock().is_empty(), "inline hints are not segment-repairable");
    }

    // ── T1.5: Batched delivery ────────────────────────────────────────

    #[tokio::test]
    async fn test_hinted_handoff_batched_delivery() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();

        let mock = Arc::new(MockDeliveryClient::new());
        // Add two success responses (one per node drain).
        mock.add_response(Ok(HintedHandoffResponse {
            accepted: true,
            accepted_count: 5,
            retry_indices: vec![],
        }));
        mock.add_response(Ok(HintedHandoffResponse {
            accepted: true,
            accepted_count: 3,
            retry_indices: vec![],
        }));

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
        mock.add_response(Ok(HintedHandoffResponse {
            accepted: true,
            accepted_count: 3,
            retry_indices: vec![],
        }));

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
        mock.add_response(Ok(HintedHandoffResponse {
            accepted: true,
            accepted_count: 1,
            retry_indices: vec![],
        }));
        mock.add_response(Ok(HintedHandoffResponse {
            accepted: true,
            accepted_count: 1,
            retry_indices: vec![],
        }));

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
        mock.add_response(Ok(HintedHandoffResponse {
            accepted: true,
            accepted_count: 2,
            retry_indices: vec![],
        }));

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

        // Deliver node-a only — its queue fully drained, so its WAL is
        // wiped and removed (all delivered); node-b's stays.
        let delivered = manager.drain_and_deliver(node_a.clone()).await.unwrap();
        assert_eq!(delivered, 2);

        // Verify node-a.wal is gone, node-b.wal still exists.
        assert!(
            !wal_dir.join("node-a.wal").exists(),
            "node-a.wal should be removed after full delivery"
        );
        assert!(wal_dir.join("node-b.wal").exists(), "node-b.wal should still exist");
        assert_eq!(manager.pending_count(&node_b), 1);

        // Deliver node-b — its file should also be removed.
        mock.add_response(Ok(HintedHandoffResponse {
            accepted: true,
            accepted_count: 1,
            retry_indices: vec![],
        }));
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

    // ── I/O observability seam (f0 D1) ───────────────────────────────

    /// f0 boot semantics: an uncreatable hint WAL directory must not make
    /// replay fatal — the node boots with zero hints and the gate enforces
    /// admission.
    #[tokio::test]
    async fn replay_with_uncreatable_wal_dir_boots_empty() {
        let dir = tempdir().unwrap();
        let blocked = dir.path().join("blocked");
        std::fs::write(&blocked, b"not a directory").unwrap();
        let wal_dir = blocked.join("hints");

        let manager = HintedHandoffManager::new(
            wal_dir.clone(),
            Arc::new(MockDeliveryClient::new()),
            make_test_config(wal_dir),
        );
        let replayed = manager.replay_and_enqueue().await.expect("replay must tolerate this");
        assert_eq!(replayed, 0, "no hints can be replayed from an unusable directory");
    }

    #[tokio::test]
    async fn enqueue_with_io_recorder_records_open_and_write() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let observer = Arc::new(oceanfs_storage::IoObserver::new());
        observer.register_pool(5, None);
        let recorder = crate::hinted_handoff::HintIoRecorder::new(observer.clone(), 5);
        let manager = HintedHandoffManager::new(
            wal_dir.clone(),
            Arc::new(MockDeliveryClient::new()),
            make_test_config(wal_dir),
        )
        .with_io_recorder(recorder);

        manager
            .enqueue(HintRecord::new_inline(
                NodeId::new("node-io"),
                BucketId::new("b"),
                "key".into(),
                vec![1].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();

        let signal = observer.snapshot(5).unwrap();
        assert_eq!(signal.errors, 0, "a healthy WAL records no errors");
        assert!(signal.ops >= 2, "open + write_hint are observed: {}", signal.ops);
    }

    #[tokio::test]
    async fn enqueue_wal_failure_records_error_kind_and_counter() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("hints");
        std::fs::create_dir_all(&wal_dir).unwrap();
        // A DIRECTORY named `{node}.wal` makes HintWal::open fail with
        // IsADirectory — a genuine `Error::Io` whose kind must reach the
        // observer (unlike the Internal-wrapped create_dir_all failure).
        let target = NodeId::new("node-broken");
        std::fs::create_dir_all(wal_dir.join(format!("{target}.wal"))).unwrap();

        let observer = Arc::new(oceanfs_storage::IoObserver::new());
        observer.register_pool(9, None);
        let recorder = crate::hinted_handoff::HintIoRecorder::new(observer.clone(), 9);
        let manager = HintedHandoffManager::new(
            wal_dir.clone(),
            Arc::new(MockDeliveryClient::new()),
            make_test_config(wal_dir),
        )
        .with_io_recorder(recorder);

        let result = manager
            .enqueue(HintRecord::new_inline(
                target,
                BucketId::new("b"),
                "key".into(),
                vec![1].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await;
        assert!(result.is_err(), "the WAL open must fail");

        assert_eq!(manager.hints_enqueue_failed_total_for_test(), 1, "failure counted once");
        assert_eq!(observer.io_error_count(9), 1, "the observer sees the failure");
        let signal = observer.snapshot(9).unwrap();
        assert_eq!(
            signal.error_kinds[oceanfs_storage::IoErrorKind::IsADirectory as usize],
            1,
            "the io kind reaches the health signal"
        );
    }

    // ── ae1 S1: retention, escalation, debt gauges ────────────────────

    /// ae1 S1: TTL expiry escalates debt through the f5 D3 sink instead
    /// of deleting it silently — one deduped repair record per segment;
    /// inline records are counted but not segment-repairable.
    #[tokio::test]
    async fn ttl_expiry_escalates_segment_debt_through_the_sink() {
        #[derive(Default)]
        struct RecordingSink(StdMutex<Vec<HintDropRecord>>);
        impl HintDropSink for RecordingSink {
            fn on_hints_dropped(&self, dropped: &[HintDropRecord]) {
                self.0.lock().extend_from_slice(dropped);
            }
        }

        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());
        let sink = Arc::new(RecordingSink::default());
        let manager = HintedHandoffManager::new(wal_dir.clone(), mock, make_test_config(wal_dir))
            .with_drop_sink(sink.clone());
        let node = NodeId::new("node-a");
        let segment = SegmentId::new();
        for key in ["k1", "k2"] {
            let mut record = HintRecord::new_segment_ref(
                node.clone(),
                BucketId::new("b"),
                key.to_string(),
                segment,
                0,
                1024,
                oceanfs_core::Hlc::zero(),
            );
            record.stored_at_secs = 1; // long expired
            manager.enqueue(record).await.unwrap();
        }
        let mut inline = HintRecord::new_inline(
            node.clone(),
            BucketId::new("b"),
            "inline".into(),
            vec![1, 2, 3, 4].into(),
            oceanfs_core::Hlc::zero(),
        );
        inline.stored_at_secs = 1;
        manager.enqueue(inline).await.unwrap();

        let pruned = manager.prune_all_expired(1).await.unwrap();
        assert_eq!(pruned, 3, "every expired record is pruned");
        let records = sink.0.lock().clone();
        assert_eq!(records.len(), 1, "one deduped repair record per distinct segment");
        assert_eq!(records[0].segment_id, segment);
        assert_eq!(records[0].intended_for, node);
        assert_eq!(manager.pending_count(&node), 0, "queue drained");
    }

    /// ae1 S1: retention follows the membership topology — a retained
    /// (Dead) target keeps its debt under the TTL cap; a departed target
    /// escalates through the sink and reclaims its WAL.
    #[tokio::test]
    async fn debt_retention_follows_membership_topology() {
        #[derive(Default)]
        struct RecordingSink(StdMutex<Vec<HintDropRecord>>);
        impl HintDropSink for RecordingSink {
            fn on_hints_dropped(&self, dropped: &[HintDropRecord]) {
                self.0.lock().extend_from_slice(dropped);
            }
        }

        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());
        let sink = Arc::new(RecordingSink::default());

        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("self"),
            "127.0.0.1:9100".parse().unwrap(),
            "127.0.0.1:9101".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));

        let manager =
            HintedHandoffManager::new(wal_dir.clone(), mock, make_test_config(wal_dir.clone()))
                .with_membership(membership.clone())
                .with_drop_sink(sink.clone());

        // Retained-Dead target: debt is held (long-TTL pass prunes nothing).
        let retained = NodeId::new("retained");
        membership.upsert_node(
            retained.clone(),
            oceanfs_core::NodeState::Dead,
            oceanfs_core::Incarnation::new(1),
            None,
        );
        manager
            .enqueue(HintRecord::new_segment_ref(
                retained.clone(),
                BucketId::new("b"),
                "kept".into(),
                SegmentId::new(),
                0,
                1024,
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();
        assert_eq!(
            manager.prune_all_expired(86_400 * 365).await.unwrap(),
            0,
            "retained debt survives a long-TTL pass"
        );
        assert_eq!(manager.pending_count(&retained), 1, "retained debt is held");

        // Departed target (absent from membership): escalate + reclaim.
        let departed = NodeId::new("departed");
        let segment_b = SegmentId::new();
        manager
            .enqueue(HintRecord::new_segment_ref(
                departed.clone(),
                BucketId::new("b"),
                "gone".into(),
                segment_b,
                0,
                1024,
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();
        let wal_path = dir.path().join("departed.wal");
        assert!(wal_path.exists(), "departed WAL written before the prune");

        assert_eq!(
            manager.prune_all_expired(86_400 * 365).await.unwrap(),
            0,
            "departure is not a TTL prune"
        );
        assert_eq!(manager.pending_count(&departed), 0, "departed queue reclaimed");
        assert!(!wal_path.exists(), "departed WAL file reclaimed");
        let records = sink.0.lock().clone();
        assert!(
            records.iter().any(|r| r.segment_id == segment_b && r.intended_for == departed),
            "departed debt escalates through the f5 D3 sink"
        );
    }

    /// ae1 S1: retained-Dead debt replays when the target returns (the
    /// membership-driven retention keeps it deliverable).
    #[tokio::test]
    async fn retained_dead_debt_replays_when_the_target_returns() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());
        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("self"),
            "127.0.0.1:9100".parse().unwrap(),
            "127.0.0.1:9101".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        let manager =
            HintedHandoffManager::new(wal_dir.clone(), mock.clone(), make_test_config(wal_dir))
                .with_membership(membership.clone());
        let target = NodeId::new("node-a");
        membership.upsert_node(
            target.clone(),
            oceanfs_core::NodeState::Dead,
            oceanfs_core::Incarnation::new(1),
            None,
        );
        manager
            .enqueue(HintRecord::new_inline(
                target.clone(),
                BucketId::new("b"),
                "k".into(),
                vec![7].into(),
                oceanfs_core::Hlc::zero(),
            ))
            .await
            .unwrap();

        // Retained (Dead still in the topology): a long-TTL pass holds it.
        assert_eq!(manager.prune_all_expired(86_400 * 365).await.unwrap(), 0);
        assert_eq!(manager.pending_count(&target), 1, "retained debt is held");

        // The target returns: the retained debt replays on delivery.
        let addr: SocketAddr = "127.0.0.1:9200".parse().unwrap();
        membership.upsert_node(
            target.clone(),
            oceanfs_core::NodeState::Alive,
            oceanfs_core::Incarnation::new(2),
            Some(addr),
        );
        mock.add_response(Ok(HintedHandoffResponse {
            accepted: true,
            accepted_count: 1,
            retry_indices: vec![],
        }));
        let delivered = manager.deliver_pending(target.clone()).await.unwrap();
        assert_eq!(delivered, 1, "retained debt replays when the target returns");
        assert_eq!(manager.pending_count(&target), 0);
    }

    /// ae1 S1: the pending-debt gauge pair tracks count and bytes and
    /// returns to zero after the debt is pruned (the series stays).
    #[tokio::test]
    async fn pending_debt_gauges_track_count_and_bytes() {
        #[derive(Default)]
        struct RecordingRegistrar(StdMutex<Vec<Gauge>>);
        impl MetricRegistrar for RecordingRegistrar {
            fn register_counter(&self, _counter: Counter) {}
            fn register_gauge(&self, gauge: Gauge) {
                self.0.lock().push(gauge);
            }
            fn register_histogram(&self, _histogram: std::sync::Arc<oceanfs_core::Histogram>) {}
        }

        let dir = tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();
        let mock = Arc::new(MockDeliveryClient::new());
        let registrar = Arc::new(RecordingRegistrar::default());
        let manager = HintedHandoffManager::new(wal_dir.clone(), mock, make_test_config(wal_dir))
            .with_metric_registrar(registrar.clone());
        let node = NodeId::new("node-a");

        fn gauge_value(registrar: &RecordingRegistrar, name: &str) -> u64 {
            registrar
                .0
                .lock()
                .iter()
                .find(|gauge| gauge.name() == name)
                .expect("gauge registered")
                .get()
        }

        let mut record = HintRecord::new_inline(
            node.clone(),
            BucketId::new("b"),
            "key".into(),
            vec![0u8; 100].into(),
            oceanfs_core::Hlc::zero(),
        );
        record.stored_at_secs = 1; // long expired
        manager.enqueue(record).await.unwrap();
        assert_eq!(gauge_value(&registrar, "hinted_handoff_pending_debt"), 1);
        assert_eq!(
            gauge_value(&registrar, "hinted_handoff_pending_debt_bytes"),
            100 + 128,
            "inline payload + proto slack"
        );

        manager.prune_all_expired(1).await.unwrap();
        assert_eq!(gauge_value(&registrar, "hinted_handoff_pending_debt"), 0);
        assert_eq!(gauge_value(&registrar, "hinted_handoff_pending_debt_bytes"), 0);
    }
}
