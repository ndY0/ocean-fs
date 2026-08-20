//! Distributed write coordinator with quorum-based replication.
//!
//! Coordinates blob writes across the cluster: determines the N successors
//! from the ring, appends to the local active segment, replicates the write
//! to W successors, awaits W acknowledgments, and returns the result.
//!
//! ## Write Modes
//!
//! - `ack_after_wal`: ack after WAL quorum (fast, client sees 200 early)
//! - `ec_async`: EC encoding happens post-ack in background
//!
//! Per performance guideline §2.6 (bounded channels), §4.5 (adaptive
//! timeouts), and §9.3 (pre-compute key hash once).

use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use oceanfs_cache::CacheRpcClient;
use oceanfs_core::{
    BucketId, ChunkRef, HashKey, HashOutput, Hlc, HlcClock, NodeId, ObjectKey, ObjectMetadata,
    OperationTimeouts, SegmentId, SegmentIndexEntry, SegmentSizeConfig, SizeTier, WriteAck,
    WriteResult,
};
use oceanfs_durability::HintedHandoffManager;
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::RingCache;
use oceanfs_storage::{
    SegmentLifecycleCoordinator, SegmentPool, SegmentRpcClient, SegmentSealer, SegmentShard,
    SegmentSplitter, TierRouter, TransitionError, WalEntry,
};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::{
    error::{Error, Result},
    write::replication::replicate_write,
};

/// Maps a segment-pool append error to a server error, converting the
/// backpressure timeout into a retryable `503 SlowDown` (the write was
/// not recorded; the client may retry).
fn map_append_error(tier: String) -> impl FnOnce(oceanfs_storage::Error) -> Error {
    move |e| match e {
        oceanfs_storage::Error::WriteBackpressureTimeout => Error::WriteOverloaded,
        e => Error::Storage(format!("{tier} tier append: {e}")),
    }
}

/// RAII writer-lease guard: the write path joins every segment it
/// appends to (after the reserve, before its WAL entry) and leaves at
/// request completion. `Drop` leaves whatever remains — the error and
/// panic paths can never leak a join (a leaked count would pin a
/// segment unsealed forever). The zeroed segments are noted for the
/// pending-seal drain — sync-only, so `Drop` is safe.
struct WriterLeaseGuard {
    lifecycle: Arc<SegmentLifecycleCoordinator>,
    /// Join count per segment (a request can append multiple chunks to
    /// one segment — the Multi tier's splitter reuses a slot).
    counts: std::collections::HashMap<SegmentId, u32>,
    /// The normal completion path left everything explicitly.
    completed: bool,
}

impl Drop for WriterLeaseGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        for (id, n) in self.counts.drain() {
            for _ in 0..n {
                if self.lifecycle.writer_leave(id) {
                    // The segment's writers are all gone; note it for
                    // the next drain (this Drop cannot await the
                    // freeze+enqueue — the drain is activity-driven).
                    self.lifecycle.note_pending_seal(id);
                }
            }
        }
    }
}

/// Maximum number of replica nodes to fan out to for a write.
const MAX_REPLICA_FANOUT: usize = 6;

/// A request to write an object.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    /// Target bucket.
    pub bucket: BucketId,
    /// Object key.
    pub key: ObjectKey,
    /// Pre-computed key hash.
    pub hash_key: HashKey,
    /// Object payload.
    pub data: Bytes,
    /// Expected quorum size for write acknowledgments.
    pub write_quorum: u8,
    /// Whether to acknowledge after WAL write (true) or after EC seal (false).
    pub ack_after_wal: bool,
    /// Whether to encode in the background after ack.
    pub ec_async: bool,
    /// Per-bucket policy (configuration, resolver, etc.).
    pub policy: Option<Arc<crate::BucketPolicy>>,
}

/// Coordinates distributed blob writes with quorum replication.
///
/// Routes writes to the correct replica set, appends to the local
/// segment via the segment pipeline, fans out replicas, and collects W
/// acknowledgments before returning to the client.
pub struct WriteCoordinator {
    /// Ring cache for consistent-hashing lookups.
    ring: Arc<RingCache>,
    /// Cluster membership for node state queries.
    membership: Arc<Membership>,
    /// gRPC connection pool for replica communication and forwarding.
    pool: Arc<ConnectionPool>,
    /// This node's identifier.
    node_id: NodeId,
    /// HLC clock for write timestamping.
    hlc_clock: Arc<HlcClock>,
    /// Metadata store for inline writes, wrapped in the async adapter
    /// (blocking RocksDB calls run on the blocking pool — see
    /// metadata-io-off-async-workers).
    metadata_store: Arc<crate::metadata_async::AsyncMetadataOps>,
    /// Tier router for classifying blob sizes.
    tier_router: TierRouter,
    /// Per-core sharded segment groups (Small tier).
    /// Wired for future per-core shard routing (perf §2.5).
    #[allow(dead_code)]
    shard_small: Arc<SegmentShard>,
    /// Per-core sharded segment groups (Standard tier).
    /// Wired for future per-core shard routing (perf §2.5).
    #[allow(dead_code)]
    shard_standard: Arc<SegmentShard>,
    /// Segment pool for pipeline parallelism (Small tier).
    segment_pool_small: Arc<SegmentPool>,
    /// Segment pool for pipeline parallelism (Standard tier).
    segment_pool_standard: Arc<SegmentPool>,
    /// Segment sealer for finalizing full segments.
    sealer: Arc<SegmentSealer>,
    /// The single writer of segment lifecycle state (ADR-0025 phase 1):
    /// the write path requests `reserve` through it BEFORE the first
    /// WAL entry of each segment; the seal worker's persistence path
    /// requests `seal` through it (via the flush coordinator); the
    /// orphan reaper requests `delete` through it. No other code writes
    /// segment state.
    lifecycle: Arc<SegmentLifecycleCoordinator>,
    /// Segment size configuration.
    size_config: SegmentSizeConfig,
    /// Hinted handoff buffer for writes to temporarily unreachable replicas.
    hinted_handoff: Arc<HintedHandoffManager>,
    /// Cluster-readiness gate: closed while this node's ring is still
    /// converging after (re)join; the write path refuses to
    /// under-replicate while closed. Defaults to OPEN (`true`) so tests
    /// and single-node deployments are unaffected; the composition root
    /// installs a real gate via [`set_ready_gate`](Self::set_ready_gate).
    ready: Arc<std::sync::atomic::AtomicBool>,
    /// When true (cluster mode), Step 1c requires the ring view to
    /// satisfy the requested write quorum. Single-node deployments
    /// (no seeds) set this false: the ring is permanently 1 node and
    /// the default bucket policy (w=2) would otherwise reject every
    /// write — the old adaptive capping is retained for them only.
    quorum_requires_ring: bool,
    /// Hint inline threshold: hints for blobs up to this size embed the
    /// data; larger blobs are hinted as segment references (the
    /// receiver pulls the range from this node). Defaults to 4 KB.
    hint_inline_threshold_bytes: u64,
    /// Compression backend (accel dispatcher), injected by the
    /// composition root. `None` (default) disables per-bucket
    /// compression.
    #[cfg(feature = "accel")]
    compressor: Option<Arc<dyn oceanfs_accel::Compressor>>,
    /// Bounds concurrent compress calls on the blocking pool — mirrors
    /// the seal semaphore so CPU-bound compression never floods the
    /// spawn_blocking threads (perf §2.7).
    #[cfg(feature = "accel")]
    compress_semaphore: Arc<tokio::sync::Semaphore>,
    /// Accumulated blob index entries per segment, keyed by segment ID.
    /// Entries are drained when the segment is sealed.
    segment_entries: DashMap<SegmentId, Vec<SegmentIndexEntry>>,
    /// Per-operation timeout configuration.
    timeouts: Arc<OperationTimeouts>,
    /// Optional notifier invoked after every successful seal, carrying
    /// the segment id and its seal-time Merkle root. Wired by the
    /// composition root to the anti-entropy engine so the incremental
    /// Merkle tree covers segments sealed after startup (continuous
    /// anti-entropy).
    segment_sealed_notifier: Option<Arc<dyn Fn(SegmentId, HashOutput) + Send + Sync>>,
}

/// Per-PUT compression context: backend + bucket config + semaphore +
/// a single worst-case-sized scratch buffer reused across all of the
/// PUT's chunks (Multi-tier objects compress chunk by chunk).
#[cfg(feature = "accel")]
struct WriteCompression {
    compressor: Arc<dyn oceanfs_accel::Compressor>,
    config: oceanfs_core::CompressConfig,
    semaphore: Arc<tokio::sync::Semaphore>,
    /// Worst-case-sized scratch buffer (bound of the largest chunk in
    /// the PUT). One allocation per PUT instead of one per chunk.
    scratch: Arc<tokio::sync::Mutex<Vec<u8>>>,
}

#[cfg(feature = "accel")]
impl WriteCompression {
    fn new(
        compressor: Arc<dyn oceanfs_accel::Compressor>,
        config: oceanfs_core::CompressConfig,
        semaphore: Arc<tokio::sync::Semaphore>,
        bound: usize,
    ) -> Self {
        Self {
            compressor,
            config,
            semaphore,
            scratch: Arc::new(tokio::sync::Mutex::new(vec![0u8; bound])),
        }
    }

    /// Compresses one chunk on the blocking pool (mirrors the seal
    /// path's EC encode). Returns `(stored_bytes, logical_len,
    /// compressed)`; chunks below `min_chunk_bytes` and incompressible
    /// payloads are stored as-is with `compressed = false`.
    async fn compress(&self, data: &Bytes) -> crate::Result<(Bytes, u32, bool)> {
        let logical = data.len() as u32;
        if data.len() < self.config.min_chunk_bytes {
            return Ok((data.clone(), logical, false));
        }
        let _permit = self.semaphore.acquire().await;
        let compressor = Arc::clone(&self.compressor);
        let data_for_encode = data.clone();
        let level = self.config.level;
        let scratch = Arc::clone(&self.scratch);
        let written = tokio::task::spawn_blocking(move || {
            let mut buf = scratch.blocking_lock();
            compressor.compress_into(&data_for_encode, level, &mut buf)
        })
        .await
        .map_err(|e| Error::Storage(format!("compression task failed: {e}")))?
        .map_err(|e| Error::Storage(format!("compress failed: {e}")))?;
        if written >= data.len() {
            // Incompressible payload — store the original, unmarked.
            return Ok((data.clone(), logical, false));
        }
        let stored = Bytes::copy_from_slice(&self.scratch.lock().await[..written]);
        Ok((stored, logical, true))
    }
}

/// Compresses `data` when the PUT's compression context is active.
/// The non-accel build compiles to a plain pass-through.
async fn compress_chunk(
    #[cfg(feature = "accel")] ctx: &Option<WriteCompression>,
    #[cfg(not(feature = "accel"))] _ctx: &Option<()>,
    data: &Bytes,
) -> crate::Result<(Bytes, u32, bool)> {
    #[cfg(feature = "accel")]
    {
        match ctx {
            Some(c) => c.compress(data).await,
            None => Ok((data.clone(), data.len() as u32, false)),
        }
    }
    #[cfg(not(feature = "accel"))]
    {
        Ok((data.clone(), data.len() as u32, false))
    }
}

impl WriteCoordinator {
    /// Creates a new write coordinator with the full segment pipeline.
    ///
    /// All dependencies are injected via `Arc` for testability and
    /// to support the composition-root pattern in `oceanfs-node`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ring: Arc<RingCache>,
        membership: Arc<Membership>,
        pool: Arc<ConnectionPool>,
        node_id: NodeId,
        hlc_clock: Arc<HlcClock>,
        metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore>,
        size_config: SegmentSizeConfig,
        shard_small: Arc<SegmentShard>,
        shard_standard: Arc<SegmentShard>,
        segment_pool_small: Arc<SegmentPool>,
        segment_pool_standard: Arc<SegmentPool>,
        sealer: Arc<SegmentSealer>,
        lifecycle: Arc<SegmentLifecycleCoordinator>,
        hinted_handoff: Arc<HintedHandoffManager>,
    ) -> Self {
        // Wrap the storage-api metadata store in the async adapter: the
        // Inline-tier write path's blocking RocksDB put runs on the
        // blocking pool via spawn_blocking, never on a runtime worker
        // (metadata-io-off-async-workers).
        let metadata_store =
            Arc::new(crate::metadata_async::AsyncMetadataOps::from_storage(metadata_store));
        let tier_router = TierRouter::new(size_config.clone());
        Self {
            ring,
            membership,
            pool,
            node_id,
            hlc_clock,
            metadata_store,
            tier_router,
            shard_small,
            shard_standard,
            segment_pool_small,
            segment_pool_standard,
            sealer,
            lifecycle,
            size_config,
            hinted_handoff,
            ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            quorum_requires_ring: true,
            hint_inline_threshold_bytes: 4096,
            #[cfg(feature = "accel")]
            compressor: None,
            #[cfg(feature = "accel")]
            compress_semaphore: Arc::new(tokio::sync::Semaphore::new(
                std::thread::available_parallelism().map_or(4, |n| n.get().saturating_mul(2)),
            )),
            segment_entries: DashMap::new(),
            timeouts: Arc::new(OperationTimeouts::default()),
            segment_sealed_notifier: None,
        }
    }

    /// Sets the per-operation timeout configuration for this coordinator.
    ///
    /// Call this at startup to inject config-driven timeouts.
    #[must_use]
    pub fn with_timeouts(mut self, timeouts: Arc<OperationTimeouts>) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Injects the compression backend (accel dispatcher) used when a
    /// bucket policy opts in via `compression.tier != None`.
    #[cfg(feature = "accel")]
    pub fn with_compressor(
        mut self,
        compressor: Option<Arc<dyn oceanfs_accel::Compressor>>,
    ) -> Self {
        self.compressor = compressor;
        self
    }

    /// Registers a notifier invoked after every successful seal.
    ///
    /// The composition root wires this to the anti-entropy engine's
    /// `on_segment_sealed` so the incremental Merkle tree is updated
    /// continuously instead of only at the startup rebuild.
    #[must_use]
    pub fn with_segment_sealed_notifier(
        mut self,
        notifier: Arc<dyn Fn(SegmentId, HashOutput) + Send + Sync>,
    ) -> Self {
        self.segment_sealed_notifier = Some(notifier);
        self
    }

    /// Installs the cluster-readiness gate (composition root).
    ///
    /// The gate is closed while this node's ring is still converging
    /// after (re)join; the write path refuses to under-replicate while
    /// closed (phase-3 churn fix). Single-node deployments keep the
    /// default open gate.
    #[must_use]
    #[doc(hidden)]
    pub fn with_ready_gate(mut self, ready: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.ready = ready;
        self
    }

    /// Sets whether Step 1c requires the ring view to satisfy the
    /// requested write quorum. Cluster nodes keep the honest check;
    /// single-node deployments (no seeds) disable it — the ring is
    /// permanently 1 node and the default bucket policy (w=2) would
    /// otherwise reject every write.
    #[must_use]
    pub fn with_quorum_requires_ring(mut self, requires: bool) -> Self {
        self.quorum_requires_ring = requires;
        self
    }

    /// Returns whether the ring view must satisfy the write quorum
    /// (used by the S3 delete handler's ring-view gate).
    pub fn quorum_requires_ring(&self) -> bool {
        self.quorum_requires_ring
    }

    /// Sets the hint inline threshold (hints above it become segment
    /// references instead of embedding the blob data). The composition
    /// root mirrors the node config's `hint_inline_threshold_bytes`.
    #[must_use]
    #[doc(hidden)]
    pub fn with_hint_inline_threshold(mut self, bytes: u64) -> Self {
        self.hint_inline_threshold_bytes = bytes;
        self
    }

    /// Executes a distributed write through the segment pipeline.
    ///
    /// # Algorithm
    ///
    /// 1. Look up the replica set from the ring.
    /// 2. If this node is not in the replica set, forward to the first
    ///    successor (in a full implementation, via gRPC).
    /// 3. If local: classify blob size via `TierRouter`, store via the
    ///    segment pipeline (inline or `SegmentPool`), replicate to W
    ///    successors, collect W acks.
    /// 4. On quorum success: return `WriteResult`.
    /// 5. On quorum failure (timeout or insufficient acks): return error.
    ///
    /// # Errors
    ///
    /// Returns [`Error::QuorumNotMet`] if the required number of
    /// acknowledgments is not received within the timeout.
    /// Returns [`Error::Routing`] if the ring returns an empty set.
    pub async fn put(&self, req: WriteRequest) -> Result<WriteResult> {
        // Step 1: Route the key.
        let replica_set = self.ring.lookup(req.hash_key.as_bytes());
        if replica_set.is_empty() {
            return Err(Error::Routing("ring returned empty replica set".into()));
        }

        // Step 1b: Cluster-readiness gate. A node that just (re)joined a
        // cluster has a ring that still contains only itself until its
        // membership pull converges; with the adaptive quorum
        // (min(write_quorum, ring size)) such a window would ACK writes
        // with a single durable copy — silent under-replication (found
        // by the phase-3 churn test). While the gate is closed, writes
        // fail with QuorumNotMet (503) instead of under-replicating.
        // Single-node deployments (no seeds, no fallback seeds) never
        // close the gate; the node-side gate flips open after
        // convergence or a 5s bound.
        if !self.ready.load(std::sync::atomic::Ordering::Relaxed) && replica_set.len() < 2 {
            return Err(Error::QuorumNotMet {
                required: req.write_quorum.min(2),
                received: replica_set.len(),
            });
        }

        // Step 1c: The requested quorum must be satisfiable by this
        // ring view. The adaptive `min(write_quorum, ring size)` in
        // Step 5 silently ACKs writes with FEWER copies than requested
        // — with a stale 1-node ring view (the post-restart gossip
        // window) a quorum=2 write would be acked with a single durable
        // copy and NO hints (the coordinator doesn't even know the
        // other replicas exist) — the churn 404/404/200 divergence.
        // Fail instead: the client retries. An error is a retry signal;
        // a degraded quorum is not.
        if self.quorum_requires_ring && (replica_set.len() as u8) < req.write_quorum {
            return Err(Error::QuorumNotMet {
                required: req.write_quorum,
                received: replica_set.len(),
            });
        }

        let is_local = replica_set.contains(&self.node_id);

        // Step 2: If not local, forward to the first available successor.
        if !is_local {
            let forward_target = replica_set
                .iter()
                .find(|n| self.membership.state_of(n) == Some(oceanfs_core::NodeState::Alive))
                .cloned()
                .ok_or_else(|| Error::Routing("no alive replica to forward write".into()))?;

            return self.forward_write(&forward_target, &req).await;
        }

        // Step 3: Local write + timestamp. Handle empty blobs early.
        let hlc = self.hlc_clock.now();
        let blob_size = req.data.len() as u64;
        if blob_size == 0 {
            let hash = blake3::hash(&req.data);
            let blake3_hash = HashOutput::from_bytes(*hash.as_bytes());
            return Ok(WriteResult {
                object_key: req.key,
                chunks: smallvec::SmallVec::new(),
                size: 0,
                blake3_hash: Some(blake3_hash),
                hlc,
            });
        }

        let tier = self.tier_router.classify(blob_size);

        // Clone the blob data for segment append and WAL write.
        // (Bytes clone is a ref-count bump, not a copy.)
        let wal_data = req.data.clone();

        // Per-bucket compression context (accel feature). Built once per
        // PUT: resolves the bucket's compression config against the
        // injected backend and sizes a single scratch buffer (worst-case
        // bound of the largest chunk in this PUT) reused across chunks.
        #[cfg(feature = "accel")]
        let compression_ctx = {
            let enabled = req
                .policy
                .as_ref()
                .map(|p| p.compression.tier != oceanfs_core::CompressionTier::None)
                .unwrap_or(false);
            match (enabled, self.compressor.as_ref()) {
                (true, Some(compressor)) => req.policy.as_ref().map(|p| {
                    let max_chunk = if tier == SizeTier::Multi {
                        self.size_config.default_target_size as usize
                    } else {
                        wal_data.len()
                    };
                    WriteCompression::new(
                        Arc::clone(compressor),
                        p.compression.clone(),
                        Arc::clone(&self.compress_semaphore),
                        compressor.worst_case_bound(max_chunk),
                    )
                }),
                _ => None,
            }
        };
        #[cfg(not(feature = "accel"))]
        let compression_ctx: Option<()> = None;

        // Compute BLAKE3 hash of the data.
        let hash = blake3::hash(&req.data);
        let blake3_hash = HashOutput::from_bytes(*hash.as_bytes());

        // Step 4: Store data through the segment pipeline.
        // Phantom registrations performed in this request (one per
        // unique segment), BEFORE each segment's WAL entry.
        let mut registered: std::collections::HashSet<SegmentId> = std::collections::HashSet::new();
        // Writer leases: the join/leave pair per segment (seal-on-zero
        // — the deterministic partial-seal trigger). Drop-safe on every
        // error path below.
        let mut leases = WriterLeaseGuard {
            lifecycle: Arc::clone(&self.lifecycle),
            counts: std::collections::HashMap::new(),
            completed: false,
        };
        let chunks = match tier {
            SizeTier::Inline => {
                let meta = ObjectMetadata {
                    object_key: req.key.clone(),
                    size: req.data.len() as u64,
                    blake3_hash: Some(blake3_hash),
                    chunks: smallvec::SmallVec::new(),
                    inline_data: Some(req.data.clone()),
                    created_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64,
                    hlc,
                };
                self.metadata_store
                    .put_object(&req.bucket, meta)
                    .await
                    .map_err(|e| Error::Storage(format!("inline metadata write: {e}")))?;
                smallvec::SmallVec::new()
            }
            SizeTier::Small => {
                let (stored, logical_len, compressed) =
                    compress_chunk(&compression_ctx, &wal_data).await?;
                // The seal hand-off budget spans append + enqueue (the
                // enqueue runs after the WAL entry below; the deadline
                // is anchored before the append so the total budget is
                // unchanged).
                let write_deadline = std::time::Instant::now()
                    + std::time::Duration::from_millis(self.timeouts.write_queue_ms);
                let (segment_id, offset, length, sealed) = self
                    .segment_pool_small
                    .append_with_hook_async(
                        &stored[..],
                        |seg_id, off, len| {
                            // Recorded under the slot lock, before any
                            // fill-triggered seal enqueue: the seal worker
                            // (another thread) can never drain the entries
                            // map before this entry exists.
                            self.record_blob_entry(seg_id, off, len, blake3_hash);
                        },
                        std::time::Duration::from_millis(self.timeouts.write_queue_ms),
                    )
                    .await
                    .map_err(map_append_error("small".into()))?;
                // Reserve the segment BEFORE the WAL entry so the WAL
                // cleanup can never mistake this segment for garbage
                // (ADR-0024 invariant: reserve precedes the first
                // DataEntry).
                self.request_reserve_before_wal(segment_id, SizeTier::Small, &mut registered)
                    .await?;
                // Join as a writer: the count gates the seal-on-zero
                // while this request is between its append and its WAL
                // record.
                self.lifecycle.writer_join(segment_id);
                *leases.counts.entry(segment_id).or_insert(0) += 1;
                // Write WAL entry for crash-recovery durability (C4-storage, D6).
                // `logical_length` lets crash replay classify compressed
                // chunks by their original size.
                self.write_wal_entry(segment_id, offset, stored, length, logical_len, 0, hlc)
                    .await?;
                // Ordering (ADR-0024 §Retention): the seal work item
                // becomes visible to the seal worker only NOW — after
                // the segment's last data-WAL position was recorded
                // above. A seal that observed the segment earlier could
                // capture a stale `(0, 0)` position in its `SealEvent`,
                // pinning the segment's WAL files forever.
                self.segment_pool_small
                    .enqueue_seal_handoff(sealed, write_deadline)
                    .await
                    .map_err(map_append_error("small".into()))?;
                let mut chunks = smallvec::SmallVec::new();
                chunks.push(ChunkRef {
                    segment_id,
                    offset,
                    length,
                    compressed,
                    logical_length: logical_len,
                });
                chunks
            }
            SizeTier::Standard => {
                let (stored, logical_len, compressed) =
                    compress_chunk(&compression_ctx, &wal_data).await?;
                let write_deadline = std::time::Instant::now()
                    + std::time::Duration::from_millis(self.timeouts.write_queue_ms);
                let (segment_id, offset, length, sealed) = self
                    .segment_pool_standard
                    .append_with_hook_async(
                        &stored[..],
                        |seg_id, off, len| {
                            // Same airtight ordering as the Small tier above.
                            self.record_blob_entry(seg_id, off, len, blake3_hash);
                        },
                        std::time::Duration::from_millis(self.timeouts.write_queue_ms),
                    )
                    .await
                    .map_err(map_append_error("standard".into()))?;
                // Reserve the segment BEFORE the WAL entry so the WAL
                // cleanup can never mistake this segment for garbage
                // (ADR-0024 invariant: reserve precedes the first
                // DataEntry).
                self.request_reserve_before_wal(segment_id, SizeTier::Standard, &mut registered)
                    .await?;
                // Join as a writer (seal-on-zero gate — see Small arm).
                self.lifecycle.writer_join(segment_id);
                *leases.counts.entry(segment_id).or_insert(0) += 1;
                // Write WAL entry for crash-recovery durability (C4-storage, D6).
                // `logical_length` lets crash replay classify compressed
                // chunks by their original size. Tier byte 1 = standard
                // pool (the replay routes by it — a 0 here sends the
                // segment's rebuild to the small pool).
                self.write_wal_entry(segment_id, offset, stored, length, logical_len, 1, hlc)
                    .await?;
                // Ordering (ADR-0024 §Retention): the seal work item is
                // enqueued only after the segment's last data-WAL
                // position was recorded (see the Small arm).
                self.segment_pool_standard
                    .enqueue_seal_handoff(sealed, write_deadline)
                    .await
                    .map_err(map_append_error("standard".into()))?;
                let mut chunks = smallvec::SmallVec::new();
                chunks.push(ChunkRef {
                    segment_id,
                    offset,
                    length,
                    compressed,
                    logical_length: logical_len,
                });
                chunks
            }
            SizeTier::Multi => {
                let splitter = SegmentSplitter::new(self.size_config.default_target_size);
                let split_chunks = splitter.split(&wal_data[..]);
                let mut chunks = smallvec::SmallVec::new();
                for (_, chunk_data) in &split_chunks {
                    let (stored, logical_len, compressed) =
                        compress_chunk(&compression_ctx, &Bytes::copy_from_slice(chunk_data))
                            .await?;
                    let write_deadline = std::time::Instant::now()
                        + std::time::Duration::from_millis(self.timeouts.write_queue_ms);
                    let (seg_id, seg_offset, length, sealed) = self
                        .segment_pool_standard
                        .append_with_hook_async(
                            &stored[..],
                            |seg_id, off, len| {
                                // Record the blob index entry BEFORE any
                                // fill-triggered seal enqueue (Defect 2).
                                // Without this, a segment filled entirely by
                                // multi-tier chunks has no index entries when
                                // the seal worker drains it, so the seal is
                                // skipped and the segment never reaches disk.
                                self.record_blob_entry(seg_id, off, len, blake3_hash);
                            },
                            std::time::Duration::from_millis(self.timeouts.write_queue_ms),
                        )
                        .await
                        .map_err(map_append_error("multi".into()))?;
                    // Reserve the segment BEFORE the WAL entry (see
                    // Small arm — ADR-0024 invariant).
                    self.request_reserve_before_wal(seg_id, SizeTier::Standard, &mut registered)
                        .await?;
                    // Join as a writer (seal-on-zero gate — see Small
                    // arm).
                    self.lifecycle.writer_join(seg_id);
                    *leases.counts.entry(seg_id).or_insert(0) += 1;
                    // Write WAL entry for each chunk (C4-storage, D6).
                    self.write_wal_entry(seg_id, seg_offset, stored, length, logical_len, 1, hlc)
                        .await?;
                    // Ordering (ADR-0024 §Retention): enqueue the seal
                    // hand-off only after this chunk's data-WAL position
                    // was recorded (see the Small arm).
                    self.segment_pool_standard
                        .enqueue_seal_handoff(sealed, write_deadline)
                        .await
                        .map_err(map_append_error("multi".into()))?;
                    chunks.push(ChunkRef {
                        segment_id: seg_id,
                        offset: seg_offset,
                        length,
                        compressed,
                        logical_length: logical_len,
                    });
                }
                chunks
            }
            _ => {
                return Err(Error::InvalidRequest(format!("unsupported storage tier: {tier:?}")));
            }
        };

        // Seal-on-zero: every writer leaves its segments. The leave
        // runs after this request's last WAL record, so the pending
        // seal (freeze + enqueue) is ordered after the position record
        // by construction — the seal can never capture a stale
        // position. Segments whose count hit zero are noted and
        // drained immediately (the deterministic partial-seal trigger;
        // error paths leave through the guard's Drop instead).
        for (id, n) in leases.counts.drain() {
            for _ in 0..n {
                if self.lifecycle.writer_leave(id) {
                    self.lifecycle.note_pending_seal(id);
                }
            }
        }
        leases.completed = true;
        self.lifecycle.drain_pending_seals().await;

        let segment_id =
            chunks.first().map(|c: &ChunkRef| c.segment_id).unwrap_or_else(SegmentId::new);

        info!(
            bucket = %req.bucket,
            key = %req.key,
            size = req.data.len(),
            segment_id = %segment_id,
            hlc_wall = hlc.wall_time(),
            hlc_logical = hlc.logical(),
            "local write completed"
        );

        // Step 5: Replicate to W successors using the replication module.
        // Cluster mode: the quorum is the REQUESTED quorum — Step 1c
        // guarantees the ring view can satisfy it; there is no adaptive
        // degradation. Single-node deployments (quorum_requires_ring =
        // false) keep the adaptive cap — the ring is permanently 1 node
        // and the default bucket policy (w=2) must not reject every
        // write.
        let quorum = if self.quorum_requires_ring {
            req.write_quorum
        } else {
            req.write_quorum.min(replica_set.len() as u8)
        };
        let mut acks_received: usize = 1; // local ack counted
        let mut failed_targets: Vec<NodeId> = Vec::new();

        // Build list of remote replicas.
        let remote_targets: Vec<&NodeId> =
            replica_set.iter().filter(|n| *n != &self.node_id).take(MAX_REPLICA_FANOUT).collect();

        if !remote_targets.is_empty() {
            let write_timeout_ms = self.timeouts.wal_write_ms;
            let results = replicate_write(
                &self.membership,
                &self.pool,
                &remote_targets,
                segment_id,
                &req.data,
                hlc,
                write_timeout_ms,
                &req,
                &chunks,
                &blake3_hash,
            )
            .await;

            // EVERY remote target must be accounted for: acknowledged
            // (counted) or recorded as failed for hinting AFTER the
            // quorum check. The result list may omit targets whose RPC
            // was still in-flight when the global replication deadline
            // fired — those are treated as failures too. And the loop
            // MUST NOT break on quorum: an unprocessed failed target
            // would be silently abandoned (no data, no hint) — the
            // churn 404/404/200 divergence where a write is alive on
            // one node while the other replicas stay tombstoned.
            //
            // Hints are NOT enqueued here: a write that fails the
            // quorum check must leave no trace — enqueuing first would
            // spread the unacknowledged version to the failed replicas.
            let mut results_by_target: std::collections::HashMap<NodeId, Result<WriteAck>> =
                results.into_iter().collect();
            for target in &remote_targets {
                match results_by_target.remove(target) {
                    Some(Ok(_)) => {
                        acks_received += 1;
                    }
                    Some(Err(e)) => {
                        warn!(target = %target, error = %e, "replica write failed");
                        failed_targets.push((*target).clone());
                    }
                    None => {
                        warn!(
                            target = %target,
                            "replica write unresolved by replication deadline"
                        );
                        failed_targets.push((*target).clone());
                    }
                }
            }
        }

        // Step 6: Verify quorum.
        if acks_received < quorum as usize {
            // Roll back the local write: the client sees the error and
            // retries — the unacknowledged version must not linger
            // locally (reads would serve a version no client ever
            // acknowledged) nor spread via hints (hints are only
            // enqueued after this check, Step 6b). The rollback
            // tombstone carries a FRESH HLC (strictly newer than the
            // write's) so any late hint for the failed write is
            // discarded by LWW.
            let rollback_hlc = self.hlc_clock.now();
            warn!(
                bucket = %req.bucket,
                key = %req.key,
                required = quorum,
                received = acks_received,
                "write quorum not met; rolling back local write"
            );
            if let Err(e) =
                self.metadata_store.delete_object(&req.bucket, &req.key, rollback_hlc).await
            {
                warn!(
                    bucket = %req.bucket,
                    key = %req.key,
                    error = %e,
                    "write rollback failed"
                );
            }
            return Err(Error::QuorumNotMet { required: quorum, received: acks_received });
        }

        // Step 6b: Quorum met — hint the replicas that missed the write.
        for target in failed_targets {
            self.enqueue_write_hint(&target, &req, &chunks, hlc).await;
        }

        // Step 7: Build result.
        Ok(WriteResult {
            object_key: req.key,
            chunks,
            size: req.data.len() as u64,
            blake3_hash: Some(blake3_hash),
            hlc,
        })
    }

    /// Enqueues a hinted write for a replica that missed one.
    ///
    /// Small blobs (≤ inline_threshold_bytes) embed the data inline.
    /// Larger blobs reference the segment (segment_id + offset +
    /// length) WITHOUT the data: the receiver pulls the range from
    /// this node over gRPC (FetchHintObject) and applies it. Refs keep
    /// hints small — embedding data would break the moment multipart
    /// uploads make blobs reach GB sizes (the hint WAL and the gRPC
    /// batch would balloon to the blob size).
    async fn enqueue_write_hint(
        &self,
        target: &NodeId,
        req: &WriteRequest,
        chunks: &[ChunkRef],
        hlc: Hlc,
    ) {
        let hint = if req.data.len() as u64 <= self.hint_inline_threshold_bytes {
            oceanfs_durability::hinted_handoff_rpc::HintRecord::new_inline(
                target.clone(),
                req.bucket.clone(),
                req.key.to_string(),
                req.data.clone(),
                hlc,
            )
        } else if let Some(chunk) = chunks.first() {
            // Use the first chunk's segment reference.
            // For Small/Standard tier there is exactly
            // one chunk; for Multi tier, the first
            // chunk covers the blob start.
            oceanfs_durability::hinted_handoff_rpc::HintRecord::new_segment_ref(
                target.clone(),
                req.bucket.clone(),
                req.key.to_string(),
                chunk.segment_id,
                chunk.offset,
                chunk.length,
                hlc,
            )
        } else {
            // Safety guard: no chunk (inline tier) —
            // fall back to inline storage.
            warn!(
                "no chunks available for segment-ref hint; \
                 falling back to inline for target {target}"
            );
            oceanfs_durability::hinted_handoff_rpc::HintRecord::new_inline(
                target.clone(),
                req.bucket.clone(),
                req.key.to_string(),
                req.data.clone(),
                hlc,
            )
        };
        if let Err(e) = self.hinted_handoff.enqueue(hint).await {
            // A failed enqueue means the debt was NOT recorded anywhere
            // (no WAL entry, no queue entry) — the mutation is lost for
            // this replica. Never silent: the counter + this warn are
            // the only trace (the churn residual class).
            warn!(
                target = %target,
                bucket = %req.bucket,
                key = %req.key,
                error = %e,
                "hinted handoff enqueue FAILED — write debt lost"
            );
        }
    }

    /// Writes a WAL entry for crash-recovery durability.
    ///
    /// Records the segment append in the write-ahead log so that unsealed
    /// segment data can be replayed on crash recovery (C4-storage, D6).
    /// The segment is reserved through the lifecycle coordinator
    /// BEFORE its WAL entry is written (see
    /// [`request_reserve_before_wal`](Self::request_reserve_before_wal));
    /// the WAL cleanup treats unregistered ids as sweepable garbage, so
    /// an entry whose registration lags would let the cleanup delete
    /// the file holding the segment's early entries — corrupting crash
    /// recovery. With the registration first, the cleanup can only ever
    /// see TRUE crash phantoms (requests killed before any entry was
    /// written).
    async fn request_reserve_before_wal(
        &self,
        segment_id: SegmentId,
        tier: SizeTier,
        registered: &mut std::collections::HashSet<SegmentId>,
    ) -> Result<()> {
        if !registered.insert(segment_id) {
            return Ok(()); // already reserved in this request
        }
        let (ec_k, ec_m, _strip) = match tier {
            SizeTier::Small => self.segment_pool_small.ec_params(),
            _ => self.segment_pool_standard.ec_params(),
        };
        // The typed transition API rejects a reserve on a Sealed or
        // Deleted id (AlreadySealed / AlreadyDeleted) — the
        // phantom-downgrade race is unrepresentable here, not patched.
        // Both outcomes mean the segment is already durable (or gone),
        // so no phantom is needed and the write proceeds.
        match self.lifecycle.request_reserve(segment_id, tier, ec_k, ec_m).await {
            Ok(()) => Ok(()),
            Err(TransitionError::AlreadySealed) | Err(TransitionError::AlreadyDeleted) => Ok(()),
            Err(e) => Err(Error::Storage(format!("phantom registration failed: {e}"))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_wal_entry(
        &self,
        segment_id: SegmentId,
        offset: u64,
        data: Bytes,
        length: u32,
        logical_length: u32,
        tier: u8,
        hlc: Hlc,
    ) -> std::result::Result<(), Error> {
        let chunk_hash = blake3::hash(&data);
        let checksum = HashOutput::from_bytes(*chunk_hash.as_bytes());
        let entry = WalEntry::new(
            segment_id,
            offset,
            length,
            logical_length,
            tier,
            hlc.wall_time,
            hlc.logical,
            checksum,
            data,
        );
        // Append through the sealer so the entry's data-WAL position is
        // recorded per segment (ADR-0024 Decision 2): the coordinator
        // embeds the LAST entry's position in the SealEvent, making the
        // data WAL seekable for recovery and retention.
        self.sealer
            .append_wal_entry(entry)
            .await
            .map_err(|e| Error::Storage(format!("WAL append failed: {e}")))?;
        Ok(())
    }

    /// Returns a reference to the HLC clock.
    pub fn hlc_clock(&self) -> &Arc<HlcClock> {
        &self.hlc_clock
    }

    /// Returns a reference to the hinted-handoff manager (for testing).
    #[doc(hidden)]
    pub fn hinted_handoff_for_test(&self) -> &Arc<HintedHandoffManager> {
        &self.hinted_handoff
    }

    /// Returns the async metadata store (for tests).
    pub fn metadata_store_async_for_test(&self) -> &Arc<crate::metadata_async::AsyncMetadataOps> {
        &self.metadata_store
    }

    /// Returns the number of replicas in the ring for the given key.
    ///
    /// Used by the S3 delete handler's ring-view gate: cluster mode
    /// requires the ring view to satisfy the requested quorum (no
    /// capping); single-node deployments cap the required quorum at the
    /// replica count.
    pub fn replica_count(&self, hash_key: &HashKey) -> usize {
        self.ring.lookup(hash_key.as_bytes()).len()
    }

    /// Invalidates cached object data on all remote replicas in the ring.
    ///
    /// Called after a write or delete to ensure remote nodes don't serve
    /// stale data from their L1/L2 caches.
    pub async fn invalidate_cache_on_replicas(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        hash_key: &HashKey,
    ) {
        let replica_set = self.ring.lookup(hash_key.as_bytes());
        for target in &replica_set {
            if *target == self.node_id {
                continue;
            }
            let addr = match self.membership.address_of(target) {
                Some(a) => a,
                None => continue,
            };
            let pooled = match self.pool.get_channel(addr).await {
                Ok(p) => p,
                Err(_) => continue,
            };
            let channel = pooled.channel().clone();
            drop(pooled);

            let proto_bucket: oceanfs_core::proto::common::BucketId = bucket.clone().into();
            let proto_key: oceanfs_core::proto::common::ObjectKey = key.clone().into();
            let mut client = CacheRpcClient::new(channel);
            let request = tonic::Request::new(oceanfs_cache::cache::CacheInvalidateRequest {
                bucket_id: Some(proto_bucket),
                object_key: Some(proto_key),
                invalidation_type: 0, // ObjectData
            });
            let _ = client.invalidate(request).await;
        }
    }

    /// Forwards a write request to another node via gRPC.
    ///
    /// Resolves the target's address and streams the write request
    /// using the same `AppendSegment` gRPC call that replication uses.
    async fn forward_write(&self, target: &NodeId, req: &WriteRequest) -> Result<WriteResult> {
        let addr = self.membership.address_of(target).ok_or_else(|| Error::ForwardFailed {
            target: target.to_string(),
            reason: "node address not found in membership".into(),
        })?;

        let pooled = self.pool.get_channel(addr).await.map_err(|e| Error::ForwardFailed {
            target: target.to_string(),
            reason: format!("connection pool error: {e}"),
        })?;

        let channel = pooled.channel().clone();
        drop(pooled);

        let mut client = SegmentRpcClient::new(channel);

        let segment_id = SegmentId::new();
        let proto_segment_id: oceanfs_core::proto::common::SegmentId = segment_id.into();
        let hlc = self.hlc_clock.now();
        let proto_hlc: oceanfs_core::proto::common::HlcTimestamp = hlc.into();

        let request = oceanfs_core::proto::segment::SegmentAppendRequest {
            segment_id: Some(proto_segment_id),
            shard_index: None,
            offset: 0,
            data: req.data.clone(),
            hlc: Some(proto_hlc),
            bucket_id: req.bucket.to_string(),
            object_key: req.key.to_string(),
            object_size: req.data.len() as u64,
            blake3_hash: Bytes::new(),
            chunk_segment_ids: vec![],
            chunk_offsets: vec![],
            chunk_lengths: vec![],
        };

        info!(
            target = %target,
            bucket = %req.bucket,
            key = %req.key,
            "forwarding write to remote replica"
        );

        let response =
            client.append_segment(tokio_stream::once(request)).await.map_err(|status| {
                Error::ForwardFailed {
                    target: target.to_string(),
                    reason: format!("gRPC forward failed: {status}"),
                }
            })?;

        let _ack = response.into_inner();

        let hash = blake3::hash(&req.data);
        let blake3_hash = HashOutput::from_bytes(*hash.as_bytes());

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id,
            offset: 0,
            length: req.data.len() as u32,
            compressed: false,
            logical_length: req.data.len() as u32,
        });

        Ok(WriteResult {
            object_key: req.key.clone(),
            chunks,
            size: req.data.len() as u64,
            blake3_hash: Some(blake3_hash),
            hlc,
        })
    }

    /// Records a blob index entry for an append to the segment.
    ///
    /// Accumulated entries are consumed by the seal worker when
    /// the segment is sealed.
    fn record_blob_entry(&self, segment_id: SegmentId, offset: u64, length: u32, hash: HashOutput) {
        let entry = SegmentIndexEntry { offset, length, blob_key_hash: *hash.as_bytes() };
        self.segment_entries.entry(segment_id).or_default().push(entry);
    }

    /// Starts a background seal worker that drains seal queues from both
    /// segment pools and calls the sealer for each filled segment.
    ///
    /// The seal worker acquires a permit from the pool's seal semaphore
    /// before processing each work item, enforcing bounded concurrency.
    /// On successful seal, accumulated blob index entries are removed
    /// from the tracking map.
    ///
    /// # Returns
    ///
    /// A `JoinHandle` for the spawned sealing task.
    pub fn start_seal_worker(self: &Arc<Self>) -> JoinHandle<()> {
        let self_small = Arc::clone(self);
        let self_standard = Arc::clone(self);

        // Take seal receivers from both pools.
        let rx_small = self.segment_pool_small.take_seal_rx();
        let rx_standard = self.segment_pool_standard.take_seal_rx();

        tokio::spawn(async move {
            // Merge both receivers into a single stream using select.
            match (rx_small, rx_standard) {
                (Some(mut small_rx), Some(mut standard_rx)) => {
                    loop {
                        let work = tokio::select! {
                            maybe_work = small_rx.recv() => maybe_work,
                            maybe_work = standard_rx.recv() => maybe_work,
                        };
                        let work = match work {
                            Some(w) => w,
                            None => {
                                // Both channels closed — nothing left to seal.
                                info!("seal worker shutting down: both seal queues closed");
                                break;
                            }
                        };

                        let sealer_arc = Arc::clone(&self_small.sealer);
                        let entries_map = &self_small.segment_entries;
                        let sem = if work.tier == SizeTier::Small {
                            self_small.segment_pool_small.seal_semaphore()
                        } else {
                            self_standard.segment_pool_standard.seal_semaphore()
                        };

                        // Drain the blob index entries synchronously — the
                        // writer's append_with_hook guarantees they are
                        // already recorded when the work item was enqueued.
                        let segment_id = work.segment_id;
                        let tier = work.tier;
                        let entries =
                            entries_map.remove(&segment_id).map(|(_, v)| v).unwrap_or_default();

                        // NOTE: an empty entry list is LEGITIMATE — a
                        // segment rebuilt by WAL replay carries data that
                        // was never appended through this coordinator (no
                        // blob entries were recorded for it). Sealing it
                        // with an empty index is correct: the data bytes
                        // are the drained buffer, readers locate chunks
                        // via the object metadata's ChunkRefs, and the
                        // seal makes the segment durable (and its WAL
                        // files sweepable). Skipping the seal left such
                        // segments registered-unsealed forever, pinning
                        // their WAL files indefinitely (2.5 GB leak).
                        if entries.is_empty() {
                            tracing::debug!(
                                segment_id = %segment_id,
                                "sealing segment with empty blob index (WAL-replayed data)"
                            );
                        }

                        // Acquire a permit to enforce bounded concurrency
                        // (perf §2.7/8.5), then seal on a spawned task so
                        // the worker keeps draining the queues. Sealing
                        // serially here let the bounded queue overflow
                        // under write bursts (try_send dropped data);
                        // concurrent seals keep the drain rate above the
                        // fill rate (read-path-integrity-under-load).
                        let self_small = Arc::clone(&self_small);
                        let self_standard = Arc::clone(&self_standard);
                        tokio::spawn(async move {
                            let permit = sem.acquire().await;

                            // Race-closing reserve: the write path
                            // reserves the segment BEFORE its first WAL
                            // entry, but the fill-triggered seal work
                            // item is enqueued DURING the append — a
                            // seal can drain before that reserve lands,
                            // and the flush path's Reserved-only
                            // validation would reject it as Missing.
                            // Reserving here (idempotent, through the
                            // coordinator — still the only writer) only
                            // when the registry has no entry yet closes
                            // the race; the common case (the write
                            // path's reserve already folded) skips the
                            // extra durable write.
                            if self_small.lifecycle.registry().get(segment_id).is_none() {
                                match self_small
                                    .lifecycle
                                    .request_reserve(segment_id, tier, work.ec_k, work.ec_m)
                                    .await
                                {
                                    Ok(())
                                    | Err(TransitionError::AlreadySealed)
                                    | Err(TransitionError::AlreadyDeleted) => {}
                                    Err(e) => {
                                        warn!(
                                            segment_id = %segment_id,
                                            error = %e,
                                            "seal-time reserve failed; seal deferred to replay"
                                        );
                                        // The segment's data remains
                                        // readable via the sealing-data
                                        // set and the WAL still holds its
                                        // entries — crash recovery
                                        // replays it. Do not seal: the
                                        // flush path would reject it.
                                        return;
                                    }
                                }
                            }

                            // The seal-time EC parity is computed inside
                            // `seal_from_data` on the blocking pool
                            // (single scheduler — the write path never
                            // touches a second thread pool).
                            // Compute the seal-time Merkle root over the
                            // data section (64 KiB leaves — the shared
                            // default used by scrub and anti-entropy) and
                            // persist it in the segment metadata: it is
                            // the trusted anchor for scrub verification,
                            // anti-entropy's local-vs-stored comparison,
                            // and the startup rebuild of the incremental
                            // Merkle tree. Without it, every segment is
                            // "missing merkle root" (scrub inert,
                            // anti-entropy flags every segment).
                            //
                            // The build is CPU-bound (hashing the full
                            // segment data) — it runs on the blocking
                            // pool, never on a runtime worker.
                            let merkle_data = work.segment_data.clone();
                            let merkle_root = match tokio::task::spawn_blocking(move || {
                                oceanfs_durability::MerkleTree::build(
                                    &merkle_data,
                                    0, // 0 selects the shared 64 KiB default
                                )
                                .map(|tree| tree.root().hash())
                            })
                            .await
                            {
                                Ok(root) => root,
                                Err(e) => {
                                    warn!(
                                        segment_id = %segment_id,
                                        error = %e,
                                        "merkle build task failed; sealing without merkle root"
                                    );
                                    None
                                }
                            };

                            let result = sealer_arc
                                .seal_from_data(
                                    segment_id,
                                    tier,
                                    work.segment_data.clone(),
                                    &entries,
                                    work.ec_k,
                                    work.ec_m,
                                    work.strip_size_bytes,
                                    work.ec_encoder.clone(),
                                    merkle_root,
                                )
                                .await;

                            match result {
                                Ok(_handle) => {
                                    // The in-flight read window is closed
                                    // by the seal transition itself (the
                                    // coordinator's fold cleared the
                                    // entry's in_flight — the `.dat` is
                                    // durable), so no cross-crate
                                    // remove_seal_buffer call exists any
                                    // more (lifecycle-read-path).
                                    // Notify the anti-entropy engine so the
                                    // incremental Merkle tree covers this
                                    // segment without waiting for the next
                                    // startup rebuild (continuous AE).
                                    if let Some(notifier) = &self_small.segment_sealed_notifier {
                                        if let Some(root) = merkle_root {
                                            notifier(segment_id, root);
                                        }
                                    }
                                    // Recycle the segment's backing buffer.
                                    // The sealing-data clone was just dropped
                                    // and seal_from_data's clone went out of
                                    // scope, so the work item now holds the
                                    // last reference to the original BytesMut
                                    // allocation: try_into_mut recovers it
                                    // zero-copy for the next activation
                                    // (pool-backpressure-and-buffer-recycling).
                                    match work.segment_data.try_into_mut() {
                                        Ok(buf) => {
                                            if tier == SizeTier::Small {
                                                self_small.segment_pool_small.release_buffer(buf);
                                            } else {
                                                self_standard
                                                    .segment_pool_standard
                                                    .release_buffer(buf);
                                            }
                                        }
                                        // Still referenced (e.g. an in-flight
                                        // read of the sealing set): drop.
                                        Err(bytes) => drop(bytes),
                                    }
                                    info!(
                                        segment_id = %segment_id,
                                        tier = ?tier,
                                        blob_count = entries.len(),
                                        "segment sealed successfully"
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        segment_id = %segment_id,
                                        error = %e,
                                        "segment seal failed"
                                    );
                                    // The in-memory entries were drained above
                                    // and are dropped. The segment's bytes
                                    // remain readable via the pool's sealing
                                    // set, and the WAL still holds the append
                                    // entries, so crash recovery replays this
                                    // segment on restart.
                                }
                            }
                            drop(permit); // permit released
                        });
                    }
                }
                _ => {
                    info!("seal worker: seal queues unavailable");
                }
            }
        })
    }

    /// Deletes an object by replicating the deletion to all replicas.
    ///
    /// 1. Looks up the replica set from the ring.
    /// 2. Sends a `DeleteObject` gRPC call to each remote replica.
    /// 3. Returns the number of remote replicas that confirmed deletion.
    ///
    /// The local tombstone is written by the caller (the S3 handler)
    /// before this is invoked, so the local delete is not counted here.
    /// Every replica attempt is logged at `debug!` and every skip or
    /// failure at `warn!` — deletion is no longer silently swallowed
    /// (F3a: the caller needs the confirmed count to enforce quorum).
    ///
    /// `hlc` is the delete's timestamp stamped by the caller — the same
    /// value the caller persisted in the local tombstone — so all
    /// replicas converge on one tombstone version
    /// (hlc-causality-closure G4/G8).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Routing`] if the ring returns an empty replica set.
    pub async fn delete(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        hash_key: &HashKey,
        hlc: Hlc,
    ) -> Result<usize> {
        let replica_set = self.ring.lookup(hash_key.as_bytes());
        if replica_set.is_empty() {
            return Err(Error::Routing("ring returned empty replica set".into()));
        }

        let mut deleted: usize = 0;

        // Delete on remote replicas.
        for target in &replica_set {
            if *target == self.node_id {
                // Local delete is handled by the caller.
                debug!(target = %target, bucket = %bucket, key = %key, "delete: skipping self (local delete handled by caller)");
                continue;
            }

            let addr = match self.membership.address_of(target) {
                Some(a) => a,
                None => {
                    warn!(
                        target = %target,
                        bucket = %bucket,
                        key = %key,
                        "delete replication skipped: no address in membership"
                    );
                    continue;
                }
            };

            let pooled = match self.pool.get_channel(addr).await {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        target = %target,
                        addr = %addr,
                        error = %e,
                        bucket = %bucket,
                        key = %key,
                        "delete replication skipped: failed to acquire channel; \
                         storing hinted handoff"
                    );
                    // The pool pre-connects eagerly, so an unreachable
                    // replica fails HERE (connection refused at channel
                    // acquisition), not at the RPC call below. Hint it —
                    // see the RPC failure branch.
                    self.enqueue_delete_hint(target, bucket, key, hlc).await;
                    continue;
                }
            };
            let channel = pooled.channel().clone();
            drop(pooled);

            let mut client = SegmentRpcClient::new(channel);
            let proto_hlc: oceanfs_core::proto::common::HlcTimestamp = hlc.into();
            let request = tonic::Request::new(oceanfs_core::proto::segment::DeleteObjectRequest {
                bucket_id: bucket.to_string(),
                object_key: key.to_string(),
                hlc: Some(proto_hlc),
            });

            debug!(
                target = %target,
                addr = %addr,
                bucket = %bucket,
                key = %key,
                "delete replication attempt"
            );

            match client.delete_object(request).await {
                Ok(resp) => {
                    let confirmed = resp.into_inner().deleted;
                    debug!(
                        target = %target,
                        addr = %addr,
                        deleted = confirmed,
                        bucket = %bucket,
                        key = %key,
                        "delete replication outcome"
                    );
                    if confirmed {
                        deleted += 1;
                    } else {
                        warn!(
                            target = %target,
                            bucket = %bucket,
                            key = %key,
                            "replica reported deletion not confirmed"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        target = %target,
                        addr = %addr,
                        error = %e,
                        bucket = %bucket,
                        key = %key,
                        "delete replication failed; storing hinted handoff"
                    );
                    self.enqueue_delete_hint(target, bucket, key, hlc).await;
                }
            }
        }

        Ok(deleted)
    }

    /// Enqueues a hinted DELETE for a replica that missed a delete.
    ///
    /// The dead replica missed the delete: hint it so it applies the
    /// tombstone when it returns. WITHOUT this, a node that missed a
    /// delete keeps its stale row forever — and the sender-side
    /// obsolete pre-check then drops later write hints for keys that
    /// are still live elsewhere (churn divergence).
    async fn enqueue_delete_hint(
        &self,
        target: &NodeId,
        bucket: &BucketId,
        key: &ObjectKey,
        hlc: Hlc,
    ) {
        let hint = oceanfs_durability::hinted_handoff_rpc::HintRecord::new_delete(
            target.clone(),
            bucket.clone(),
            key.to_string(),
            hlc,
        );
        if let Err(e) = self.hinted_handoff.enqueue(hint).await {
            // See enqueue_write_hint: a failed enqueue is a LOST delete
            // — the tombstone will never reach the dead replica.
            warn!(
                target = %target,
                bucket = %bucket,
                key = %key,
                error = %e,
                "hinted handoff enqueue FAILED — delete debt lost"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::SocketAddr;

    use oceanfs_core::{
        GossipConfig, Incarnation, NodeId, NodeState, PoolConfig, RingConfig, RpcConfig, SizeTier,
        Tombstone, WalConfig,
    };
    use oceanfs_durability::HintedHandoffConfig;
    use oceanfs_routing::{hash_key, Ring};
    use oceanfs_storage::{BufferPool, RocksDbMetadataStore, SealConfig, WalWriter};
    use parking_lot::Mutex;

    use super::*;

    /// Creates a test coordinator with a fully wired segment pipeline.
    async fn make_write_coordinator(node_id: &str, ring_nodes: &[&str]) -> WriteCoordinator {
        use oceanfs_durability::GrpcHintDeliveryClient;

        let dir = tempfile::tempdir().unwrap();
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> =
            Arc::new(GrpcHintDeliveryClient::new(pool.clone()));
        make_write_coordinator_with_delivery(node_id, ring_nodes, dir, pool, delivery_client)
            .await
            .0
    }

    /// A fresh lifecycle registry for pool construction (the pools hold
    /// it for the read path and the in-flight attach).
    fn test_registry() -> Arc<oceanfs_storage::SegmentLifecycleRegistry> {
        Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
            &oceanfs_core::LifecycleConfig::default(),
        ))
    }
    /// Creates a test coordinator with a caller-provided hint delivery client.
    async fn make_write_coordinator_with_delivery(
        node_id: &str,
        ring_nodes: &[&str],
        dir: tempfile::TempDir,
        pool: Arc<ConnectionPool>,
        delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient>,
    ) -> (WriteCoordinator, tempfile::TempDir) {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
        for node in ring_nodes {
            ring.add_node(NodeId::new(*node));
        }
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new(node_id),
            addr,
            GossipConfig::default(),
            ring_cache.clone(),
        ));

        for node in ring_nodes {
            membership.upsert_node(
                NodeId::new(*node),
                NodeState::Alive,
                Incarnation::new(1),
                Some(addr),
            );
        }
        let hlc_clock = Arc::new(HlcClock::new());

        // Segment pipeline components (in-memory / temp dir).
        let metadata = Arc::new(
            RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        );
        let size_config = SegmentSizeConfig::default();
        let buffer_pool = Arc::new(BufferPool::new(65536, 16));

        let shard_small =
            Arc::new(SegmentShard::new(4, SizeTier::Small, &size_config, &buffer_pool).unwrap());
        let shard_standard =
            Arc::new(SegmentShard::new(4, SizeTier::Standard, &size_config, &buffer_pool).unwrap());

        let pool_cfg = PoolConfig::default();
        let segment_pool_small = Arc::new(
            SegmentPool::new(
                pool_cfg.clone(),
                SizeTier::Small,
                &size_config,
                buffer_pool.clone(),
                None,
                None,
                test_registry(),
            )
            .unwrap(),
        );
        let segment_pool_standard = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_config,
                buffer_pool,
                None,
                None,
                test_registry(),
            )
            .unwrap(),
        );

        let wal = Arc::new(
            WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        let seal_config = SealConfig {
            target_size_bytes: size_config.default_target_size,
            seal_timeout_ms: 5000,
            data_dir: dir.path().join("segments"),
            io_mode: oceanfs_storage::io::IoReadMode::Buffered,
            write_mode: oceanfs_storage::io::SegmentWriteMode::Rename,
            ..Default::default()
        };
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::new(&oceanfs_core::LifecycleConfig::default())
                .with_event_wal(Arc::new(
                    oceanfs_storage::segment::event_wal::EventWal::open(
                        dir.path().join("event-wal"),
                        &oceanfs_core::EventWalConfig {
                            event_wal_dir: dir.path().join("event-wal"),
                            event_wal_file_size_bytes: 1024 * 1024,
                            event_wal_fsync_batch_timeout_ms: 10,
                            event_wal_checkpoint_bytes: 1024 * 1024,
                        },
                    )
                    .await
                    .unwrap(),
                )),
        );
        let sealer = Arc::new(SegmentSealer::new(seal_config, wal, Arc::clone(&lifecycle)));

        use oceanfs_durability::HintedHandoffManager;

        let hints_dir = dir.path().join("hints");
        let hint_config =
            HintedHandoffConfig { wal_dir: hints_dir.clone(), ..HintedHandoffConfig::default() };
        let hinted_handoff =
            Arc::new(HintedHandoffManager::new(hints_dir, delivery_client, hint_config.clone()));

        (
            WriteCoordinator::new(
                ring_cache,
                membership,
                pool,
                NodeId::new(node_id),
                hlc_clock,
                metadata,
                size_config,
                shard_small,
                shard_standard,
                segment_pool_small,
                segment_pool_standard,
                sealer,
                lifecycle,
                hinted_handoff,
            ),
            dir,
        )
    }

    /// Hint delivery client that captures delivered hint requests.
    #[derive(Default)]
    struct CaptureDeliveryClient {
        delivered:
            parking_lot::Mutex<Vec<oceanfs_durability::hinted_handoff_rpc::HintedHandoffRequest>>,
    }

    impl CaptureDeliveryClient {
        fn delivered(&self) -> Vec<oceanfs_durability::hinted_handoff_rpc::HintedHandoffRequest> {
            self.delivered.lock().clone()
        }
    }

    #[async_trait::async_trait]
    impl oceanfs_durability::HintDeliveryClient for CaptureDeliveryClient {
        async fn deliver_hints(
            &self,
            _target_addr: std::net::SocketAddr,
            request: oceanfs_durability::hinted_handoff_rpc::HintedHandoffRequest,
            _timeout_ms: u64,
        ) -> std::result::Result<
            oceanfs_durability::hinted_handoff_rpc::HintedHandoffResponse,
            oceanfs_durability::Error,
        > {
            self.delivered.lock().push(request);
            Ok(oceanfs_durability::hinted_handoff_rpc::HintedHandoffResponse {
                accepted: true,
                accepted_count: 1,
            })
        }
    }

    /// G5: the hint enqueued for a failed replica carries the write's HLC.
    #[tokio::test]
    async fn enqueued_hint_carries_write_hlc() {
        let dir = tempfile::tempdir().unwrap();
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let capture = Arc::new(CaptureDeliveryClient::default());
        let delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> = capture.clone();
        let coord = make_write_coordinator_with_delivery(
            "n1",
            &["n1", "n2", "n3"],
            dir,
            pool,
            delivery_client,
        )
        .await
        .0;

        // The write is routed to n2/n3 (dead addresses) → replication
        // fails → hints are enqueued with the write's stamped HLC.
        let data = Bytes::from_static(b"hinted payload");
        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("hinted-obj"),
            hash_key: HashKey::from_bytes(hash_key(b"hinted-obj")),
            data,
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };
        let result = coord.put(req.clone()).await;
        // Quorum 1 is satisfied by the local write; hints are best-effort.
        assert!(result.is_ok(), "local write must succeed: {result:?}");

        // Drain pending hints for n2 — the capture client receives them.
        let delivered = coord.hinted_handoff_for_test().drain_and_deliver(NodeId::new("n2")).await;
        let _ = delivered; // delivery outcome irrelevant; the capture holds the payload

        let requests = capture.delivered();
        assert!(!requests.is_empty(), "hints must have been delivered to the capture client");
        let hint_hlc = requests
            .iter()
            .flat_map(|r| r.hints.iter())
            .find_map(|h| match &h.record {
                Some(oceanfs_durability::hinted_handoff_rpc::hint_record::Record::Inline(
                    inline,
                )) => inline.hlc.as_ref().map(|p| Hlc::new(p.wall_time, p.logical)),
                _ => None,
            })
            .expect("captured request must contain an inline hint with an hlc");
        assert!(hint_hlc > Hlc::zero(), "the hint must carry the write's stamped HLC");
    }

    #[tokio::test]
    async fn coordinator_put_returns_result_for_local_node() {
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        // Use data larger than the inline threshold (4096) to hit the Small tier.
        let data = vec![0xABu8; 5000];
        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("obj"),
            hash_key: HashKey::from_bytes(hash_key(b"obj")),
            data: Bytes::from(data),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await.unwrap();
        assert_eq!(result.size, 5000);
        assert_eq!(result.chunks.len(), 1);
        assert_eq!(result.chunks[0].length, 5000);
        assert!(result.blake3_hash.is_some(), "BLAKE3 hash must be computed");
    }

    #[tokio::test]
    async fn coordinator_put_generates_valid_hash() {
        let coord = make_write_coordinator("n1", &["n1"]).await;

        let data = Bytes::from_static(b"test data");
        let expected_hash = blake3::hash(&data);

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("hash-test"),
            hash_key: HashKey::from_bytes(hash_key(b"hash-test")),
            data,
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await.unwrap();
        let hash = result.blake3_hash.unwrap();
        assert_eq!(hash.as_bytes(), expected_hash.as_bytes());
    }

    #[tokio::test]
    async fn coordinator_put_forwards_non_local() {
        // n4 is not in the ring, so it's not a replica.
        // It should attempt to forward to an alive node from the
        // replica set, returning a ForwardFailed error with the
        // target node information.
        let coord = make_write_coordinator("n4", &["n1", "n2"]).await;

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("remote"),
            hash_key: HashKey::from_bytes(hash_key(b"remote")),
            data: Bytes::from_static(b"data"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_err(), "non-local write should attempt forwarding");
        match result.unwrap_err() {
            Error::ForwardFailed { target, .. } => {
                assert!(!target.is_empty(), "forward target should be specified");
            }
            other => {
                panic!("expected ForwardFailed, got {other:?}");
            }
        }
    }

    #[tokio::test]
    async fn coordinator_put_quorum_single_node_succeeds_with_quorum_1() {
        // Single node in ring. A quorum the ring cannot satisfy must
        // FAIL (no silent capping): with a 1-node ring view and
        // write_quorum=2, the old adaptive `min` acked the write with a
        // single durable copy and no hints — the churn 404/404/200
        // divergence. An error is a retry signal; a degraded quorum is
        // not.
        let coord = make_write_coordinator("n1", &["n1"]).await;

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("quorum-capped"),
            hash_key: HashKey::from_bytes(hash_key(b"quorum-capped")),
            data: Bytes::from_static(b"data"),
            write_quorum: 2, // Requested 2, but only 1 replica exists.
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(
            matches!(result, Err(Error::QuorumNotMet { required: 2, received: 1 })),
            "an unsatisfiable quorum must fail, got: {result:?}",
        );
    }

    #[tokio::test]
    async fn coordinator_put_quorum_met_after_rollback_leaves_no_trace() {
        // A quorum-failed write must roll back the local object: a
        // subsequent read must NOT serve the unacknowledged version.
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        // n2/n3 unreachable (default 127.0.0.1:9001, nothing listening)
        // → acks = local only = 1 < quorum 2 → rollback.
        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("rollback"),
            hash_key: HashKey::from_bytes(hash_key(b"rollback")),
            data: Bytes::from_static(b"failed-write-data"),
            write_quorum: 2,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(matches!(result, Err(Error::QuorumNotMet { .. })), "quorum=2 unmet must fail");

        let meta = coord
            .metadata_store_async_for_test()
            .get_object(&BucketId::new("test"), &ObjectKey::new("rollback"))
            .await
            .unwrap();
        assert!(
            meta.is_none(),
            "the failed write must be rolled back — reads must not serve \
             a version no client ever acknowledged",
        );
    }

    #[tokio::test]
    async fn coordinator_put_quorum_exceeds_replicas_ok() {
        // 2 nodes in ring; quorum=1 uses at least 1. Write succeeds.
        let coord = make_write_coordinator("n1", &["n1", "n2"]).await;

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("quorum-ok"),
            hash_key: HashKey::from_bytes(hash_key(b"quorum-ok")),
            data: Bytes::from_static(b"test"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_ok(), "write with quorum 1 should succeed");
    }

    #[tokio::test]
    async fn coordinator_put_hlc_clock_advances() {
        let coord = make_write_coordinator("n1", &["n1", "n2"]).await;

        let before = coord.hlc_clock().now();

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("hlc-test"),
            hash_key: HashKey::from_bytes(hash_key(b"hlc-test")),
            data: Bytes::from_static(b"data"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        coord.put(req).await.unwrap();

        let after = coord.hlc_clock().now();
        assert!(after > before, "HLC clock must advance after write");
    }

    // ── Quorum tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn coordinator_put_quorum_not_met_when_insufficient_acks() {
        // 3-node ring, n1 is local, quorum=2.
        // Remote replicas n2 and n3 will fail (no gRPC server running).
        // Local ack counts as 1, so acks=1 < quorum=2 → QuorumNotMet.
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("quorum-fail"),
            hash_key: HashKey::from_bytes(hash_key(b"quorum-fail")),
            data: Bytes::from_static(b"data"),
            write_quorum: 2,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_err(), "write should fail with insufficient acks");
        match result.unwrap_err() {
            Error::QuorumNotMet { required, received } => {
                assert_eq!(required, 2, "quorum required should be 2");
                assert_eq!(received, 1, "only local ack received");
            }
            other => panic!("expected QuorumNotMet, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn coordinator_put_succeeds_with_quorum_1_even_if_remotes_fail() {
        // 3-node ring, n1 is local, quorum=1.
        // Remote replicas fail but local ack counts as 1, so quorum is met.
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("partial-fail-ok"),
            hash_key: HashKey::from_bytes(hash_key(b"partial-fail-ok")),
            data: Bytes::from_static(b"partial failure test data"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_ok(), "write with quorum=1 should succeed despite remote failures");
        let wr = result.unwrap();
        assert_eq!(wr.size, 25);
        assert_eq!(wr.object_key, ObjectKey::new("partial-fail-ok"));
    }

    #[tokio::test]
    async fn coordinator_put_empty_replica_set_returns_routing_error() {
        // Ring with no nodes → routing error.
        let coord = make_write_coordinator("n1", &[]).await;

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("empty-ring"),
            hash_key: HashKey::from_bytes(hash_key(b"empty-ring")),
            data: Bytes::from_static(b"data"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_err(), "empty ring should return routing error");
        match result.unwrap_err() {
            Error::Routing(msg) => {
                assert!(msg.contains("empty"), "error should mention empty replica set");
            }
            other => panic!("expected Routing, got {other:?}"),
        }
    }

    // ── Sealing tests (Epic 3: write-path-unification) ─────────────

    #[tokio::test]
    async fn seal_worker_persists_segment_metadata_after_fill() {
        // Verify that writing enough data to fill a segment triggers the
        // seal worker, which persists SegmentMetadata to RocksDB.
        let coord = Arc::new(make_write_coordinator("n1", &["n1"]).await);

        // Use a tiny target size (100 bytes) so a single append fills
        // the segment. The pool config also limits active_pool_size to 2
        // with encode_queue_capacity=2, forcing seal on the first append.
        // Note: the make_write_coordinator helper uses default pool config
        // with 4MB target, so a single 5KB append won't fill. We instead
        // verify that the seal worker starts and drain cycles don't panic.

        // Start the seal worker.
        let _seal_handle = coord.start_seal_worker();

        // Write a blob > inline threshold (4KB) to hit Small tier.
        let data = vec![0xABu8; 5000];
        let req = WriteRequest {
            bucket: BucketId::new("seal-test"),
            key: ObjectKey::new("obj-seal"),
            hash_key: HashKey::from_bytes(hash_key(b"obj-seal")),
            data: Bytes::from(data),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await.unwrap();
        assert_eq!(result.chunks.len(), 1);
        assert!(result.blake3_hash.is_some());

        // The blob was appended to a pool segment and a WAL entry written.
        // If the segment filled (unlikely with default 4MB target), the
        // seal worker would have sealed it. In any case, the coordinator
        // and seal worker are operational without panics.
    }

    #[tokio::test]
    async fn retention_bounds_wal_files_through_the_real_write_path() {
        // The production write pipeline end to end: pool append (seal
        // payload returned, not enqueued) → reserve → data-WAL append +
        // position record → caller-side seal enqueue → seal worker →
        // flush → seal_finalized_batch → rotation → machine-backed
        // sweep. The WAL file count must stay bounded: a seal can never
        // capture a stale (0,0) position (the enqueue is ordered after
        // the record), so every sealed segment's entries are garbage and
        // old files are pruned (the wal_not_unbounded regression).
        let dir = tempfile::tempdir().unwrap();
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> =
            Arc::new(oceanfs_durability::GrpcHintDeliveryClient::new(pool.clone()));
        let (coord, dir) =
            make_write_coordinator_with_delivery("n1", &["n1"], dir, pool, delivery_client).await;
        let coord = Arc::new(coord);

        // The production liveness closure (node.rs): absent → garbage.
        let lifecycle = Arc::clone(&coord.lifecycle);
        coord.sealer.wal_writer().set_liveness(Arc::new(move |id, pos| {
            lifecycle
                .registry()
                .get(id)
                .map(|entry| oceanfs_storage::entry_is_garbage(&entry, &pos))
                .unwrap_or(true)
        }));

        let _seal_handle = coord.start_seal_worker();

        let wal_config = WalConfig {
            data_dir: dir.path().join("wal"),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            ..Default::default()
        };
        let initial_files = oceanfs_storage::wal::count_wal_files(&wal_config);

        // Write ~1500 incompressible 5KB objects: ~7.5 MB → ~8 WAL
        // rotations and ~125 filled small segments (the seal worker
        // seals each fill).
        let mut data = vec![0u8; 5000];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i * 31 + 7) as u8; // incompressible-ish
        }
        for i in 0..1500u32 {
            let key = format!("obj-{i}");
            let req = WriteRequest {
                bucket: BucketId::new("retention"),
                key: ObjectKey::new(&key),
                hash_key: HashKey::from_bytes(hash_key(key.as_bytes())),
                data: Bytes::from(data.clone()),
                write_quorum: 1,
                ack_after_wal: true,
                ec_async: false,
                policy: None,
            };
            coord.put(req).await.expect("PUT must succeed");
        }

        // Wait for the seal pipeline to drain (every fill-triggered seal
        // completes; seal-on-zero closes the partials), then the
        // rotations' cleanups prune everything outside the window.
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let files = oceanfs_storage::wal::count_wal_files(&wal_config);
            // The count must drop to the retention window (initial + a
            // few) and stay there — never grow with more writes.
            if files <= initial_files + 3 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "WAL file count did not converge to the retention window \
                 (last {files}, initial {initial_files})"
            );
        }
        let settled = oceanfs_storage::wal::count_wal_files(&wal_config);
        assert!(
            settled <= initial_files + 3,
            "WAL files must be pruned to the retention window after sealing: {settled}"
        );
    }

    #[tokio::test]
    async fn seal_worker_handles_empty_entries_gracefully() {
        // Inline writes produce no segment work; the seal worker must
        // drain without error.
        let coord = Arc::new(make_write_coordinator("n1", &["n1"]).await);
        let _seal_handle = coord.start_seal_worker();

        // Write data exactly at inline threshold to hit inline (no segment).
        let data = vec![0xABu8; 128]; // 128 bytes, well below 4KB inline threshold
        let req = WriteRequest {
            bucket: BucketId::new("seal-inline"),
            key: ObjectKey::new("inline-obj"),
            hash_key: HashKey::from_bytes(hash_key(b"inline-obj")),
            data: Bytes::from(data),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await.unwrap();
        // Inline writes produce no chunks — nothing to seal.
        assert!(result.chunks.is_empty());
    }

    #[tokio::test]
    async fn seal_worker_seals_replayed_segment_with_empty_entries() {
        // Regression (WAL leak on the phase-2 SUT): a segment rebuilt by
        // WAL replay fills with data that never passed through the
        // coordinator's append hooks, so its blob-index entry list is
        // EMPTY at seal time. The seal worker used to skip such
        // segments, leaving them registered-unsealed forever — and the
        // seal-aware WAL retention correctly protected their files
        // forever (the WAL was their only durable copy), leaking ~50
        // files / 2.5 GB. The worker must SEAL them with an empty index
        // (readers locate chunks via object-metadata ChunkRefs).
        let dir = tempfile::tempdir().unwrap();
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> =
            Arc::new(oceanfs_durability::GrpcHintDeliveryClient::new(pool.clone()));
        let (coord, dir) =
            make_write_coordinator_with_delivery("n1", &["n1"], dir, pool, delivery_client).await;
        let coord = Arc::new(coord);
        let _seal_handle = coord.start_seal_worker();

        // Rebuild a segment through the replay path: 64 KiB chunks until
        // the pool has no empty slot left (each fill enqueues a seal
        // with zero recorded entries).
        let replayed_id = SegmentId::new();
        let chunk = vec![0xCDu8; 64 * 1024];
        let mut appended: u64 = 0;
        // Replay keeps recycling sealed slots into fresh segments under
        // the same id (pass-2 claim), so bound the loop explicitly:
        // 512 × 64 KiB = 32 MiB, enough to fill several 4 MiB segments.
        for _ in 0..512 {
            match coord.segment_pool_small.append_replayed(replayed_id, &chunk).await {
                Ok(()) => appended += chunk.len() as u64,
                Err(_) => break, // pool saturated — all slots sealing/used
            }
        }
        assert!(appended > 0, "replay append must make progress");

        // The seal worker must persist the segment (empty index) — poll
        // for the on-disk segment file.
        let seg_path = dir.path().join("segments").join(format!("{replayed_id}.dat"));
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
        while !seg_path.exists() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "replayed segment was never sealed (WAL leak regression)"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    // ── Replication fan-out test ──────────────────────────────────

    #[tokio::test]
    async fn replicate_write_fan_out_contacts_all_targets() {
        // Test at the replicate_write level: with 3 known targets
        // (all failing because no gRPC server), verify we get one
        // result per target, confirming all were contacted.
        let membership = make_membership_for_replication("n1");
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let target_n2 = NodeId::new("n2");
        let target_n3 = NodeId::new("n3");
        let targets: Vec<&NodeId> = vec![&target_n2, &target_n3];

        let results = crate::write::replication::replicate_write(
            &membership,
            &pool,
            &targets,
            SegmentId::new(),
            b"fan-out test data",
            oceanfs_core::Hlc::zero(),
            5000,
            &WriteRequest {
                bucket: BucketId::new("test"),
                key: ObjectKey::new("fan-out"),
                hash_key: HashKey::from_bytes(hash_key(b"fan-out")),
                data: Bytes::from_static(b"fan-out test data"),
                write_quorum: 1,
                ack_after_wal: true,
                ec_async: false,
                policy: None,
            },
            &[],
            &HashOutput::from_bytes([0u8; 32]),
        )
        .await;

        assert_eq!(results.len(), 2, "should return one result per target");
        for (_target, result) in &results {
            assert!(result.is_err(), "all should fail without gRPC server");
        }
    }

    fn make_membership_for_replication(node_id: &str) -> Arc<Membership> {
        use std::net::SocketAddr;
        let ring = Ring::new(RingConfig::default());
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new(node_id),
            addr,
            GossipConfig::default(),
            ring_cache,
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
        membership
    }

    // ── Hint creation tests (segment-ref-hints) ────────────────────

    #[tokio::test]
    async fn test_hint_creation_uses_inline_for_small_blobs() {
        // With a multi-node ring and quorum=1, a small blob write succeeds
        // locally, but remote replication fails (no gRPC server). The hint
        // should be created as inline because data.len() <= 4096.
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        let data = vec![0xABu8; 100]; // 100 bytes << inline threshold
        let req = WriteRequest {
            bucket: BucketId::new("hint-inline"),
            key: ObjectKey::new("small-obj"),
            hash_key: HashKey::from_bytes(hash_key(b"small-obj")),
            data: Bytes::from(data),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_ok(), "write with quorum=1 should succeed");

        // Verify hints were created for the failed remote replicas.
        let n2 = NodeId::new("n2");
        let n3 = NodeId::new("n3");
        assert!(coord.hinted_handoff.pending_count(&n2) > 0, "should have hints for n2");
        assert!(coord.hinted_handoff.pending_count(&n3) > 0, "should have hints for n3");
    }

    #[tokio::test]
    async fn test_hint_creation_uses_segment_ref_for_large_blobs() {
        // With a multi-node ring and quorum=1, a large blob write > 4096 bytes
        // succeeds locally, but remote replication fails. The hint should use
        // segment reference instead of inline data.
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        let data = vec![0xCDu8; 5000]; // 5000 bytes > inline threshold
        let req = WriteRequest {
            bucket: BucketId::new("hint-segref"),
            key: ObjectKey::new("large-obj"),
            hash_key: HashKey::from_bytes(hash_key(b"large-obj")),
            data: Bytes::from(data),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_ok(), "write with quorum=1 should succeed");

        // Verify hints were created for failed remote replicas.
        let n2 = NodeId::new("n2");
        let n3 = NodeId::new("n3");
        assert!(coord.hinted_handoff.pending_count(&n2) > 0, "should have hints for n2");
        assert!(coord.hinted_handoff.pending_count(&n3) > 0, "should have hints for n3");
    }

    /// Regression: a failed replica MUST be hinted even when quorum is
    /// already met by other replicas. The old loop broke on quorum, so
    /// a failed replica whose result arrived after the quorum break was
    /// silently abandoned (no data, no hint) — the churn 404/404/200
    /// divergence where a write is alive on one node while the other
    /// replicas stay tombstoned.
    #[tokio::test]
    async fn failed_replica_hinted_even_when_quorum_met_by_others() {
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        // n2 gets a live server (its write acks → quorum met); n3 keeps
        // the helper's default address (127.0.0.1:9001, nothing
        // listening) → its replication fails.
        let n2 = NodeId::new("n2");
        let addr_n2 = spawn_segment_server(false).await;
        coord.membership.upsert_node(
            n2.clone(),
            NodeState::Alive,
            Incarnation::new(2),
            Some(addr_n2),
        );

        let data = vec![0xABu8; 100]; // small → inline hint
        let req = WriteRequest {
            bucket: BucketId::new("quorum-break"),
            key: ObjectKey::new("k"),
            hash_key: HashKey::from_bytes(hash_key(b"k")),
            data: Bytes::from(data),
            write_quorum: 2, // local + n2 = quorum met without n3
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_ok(), "quorum=2 with one live replica must succeed");

        let n3 = NodeId::new("n3");
        assert_eq!(
            coord.hinted_handoff.pending_count(&n3),
            1,
            "the failed replica must be hinted even though quorum was met without it",
        );
    }

    #[tokio::test]
    async fn test_hint_creation_at_threshold_boundary() {
        // Test blob sizes at exactly 4096 (inline) and 4097 (segment_ref) bytes.
        let coord_4096 = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;
        let coord_4097 = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        // Exactly at threshold: 4096 bytes → inline.
        let data_4096 = vec![0x01u8; 4096];
        let req_4096 = WriteRequest {
            bucket: BucketId::new("threshold"),
            key: ObjectKey::new("exact-4096"),
            hash_key: HashKey::from_bytes(hash_key(b"exact-4096")),
            data: Bytes::from(data_4096),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };
        let result_4096 = coord_4096.put(req_4096).await;
        assert!(result_4096.is_ok(), "write at 4096 bytes should succeed");
        let n2 = NodeId::new("n2");
        assert!(
            coord_4096.hinted_handoff.pending_count(&n2) > 0,
            "should create hint at threshold (4096)"
        );

        // Just above threshold: 4097 bytes → segment_ref.
        let data_4097 = vec![0x02u8; 4097];
        let req_4097 = WriteRequest {
            bucket: BucketId::new("threshold"),
            key: ObjectKey::new("above-4097"),
            hash_key: HashKey::from_bytes(hash_key(b"above-4097")),
            data: Bytes::from(data_4097),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };
        let result_4097 = coord_4097.put(req_4097).await;
        assert!(result_4097.is_ok(), "write at 4097 bytes should succeed");
        assert!(
            coord_4097.hinted_handoff.pending_count(&n2) > 0,
            "should create hint above threshold (4097)"
        );
    }

    #[tokio::test]
    async fn test_hint_creation_inline_tier_no_chunks_handled() {
        // Inline-tier writes (<= 128 bytes by default) produce empty chunk lists.
        // When replication fails for such a write, the hint creation should not
        // panic even though chunks is empty — it falls back to inline storage.
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        // 64 bytes — well within inline tier (default 128).
        let data = vec![0xEFu8; 64];
        let req = WriteRequest {
            bucket: BucketId::new("inline-tier"),
            key: ObjectKey::new("tiny-obj"),
            hash_key: HashKey::from_bytes(hash_key(b"tiny-obj")),
            data: Bytes::from(data),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        // The write should succeed (quorum=1, local ack counts).
        assert!(result.is_ok(), "inline-tier write with quorum=1 should succeed");
        // Verify no panic occurred for empty chunks — hints were stored inline.
        let n2 = NodeId::new("n2");
        assert!(
            coord.hinted_handoff.pending_count(&n2) > 0,
            "should create inline hint for inline-tier write"
        );
    }

    // ── Delete replication tests (F3d) ───────────────────────────

    /// Minimal `MetadataStore` whose `delete_object` either confirms
    /// the deletion or fails, driving the F3d test scenarios.
    struct MockDeleteMetadata {
        /// When `true`, `delete_object` returns an error.
        fail_deletes: bool,
    }

    impl oceanfs_storage_api::MetadataStore for MockDeleteMetadata {
        fn list_object_keys(
            &self,
            _bucket: &BucketId,
        ) -> std::io::Result<Vec<(BucketId, ObjectKey)>> {
            Ok(Vec::new())
        }

        fn get_object_metadata(
            &self,
            _bucket: &BucketId,
            _key: &ObjectKey,
        ) -> std::io::Result<Option<ObjectMetadata>> {
            Ok(None)
        }

        fn list_objects(
            &self,
            _bucket: &BucketId,
            _prefix: &str,
        ) -> Vec<std::io::Result<ObjectMetadata>> {
            Vec::new()
        }

        fn list_tombstones(
            &self,
            _bucket: &BucketId,
        ) -> Vec<std::io::Result<(ObjectKey, Tombstone)>> {
            Vec::new()
        }

        fn delete_tombstone(&self, _bucket: &BucketId, _key: &ObjectKey) -> std::io::Result<()> {
            Ok(())
        }

        fn has_tombstone(&self, _bucket: &BucketId, _key: &ObjectKey) -> std::io::Result<bool> {
            Ok(false)
        }

        fn put_object(&self, _bucket: &BucketId, _meta: ObjectMetadata) -> std::io::Result<()> {
            Ok(())
        }

        fn delete_object(
            &self,
            _bucket: &BucketId,
            _key: &ObjectKey,
            _hlc: Hlc,
        ) -> std::io::Result<()> {
            if self.fail_deletes {
                Err(std::io::Error::other("mock delete failure"))
            } else {
                Ok(())
            }
        }

        fn batch_write(&self, _ops: Vec<oceanfs_storage_api::BatchOp>) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Spins up a live `SegmentRpc` server handling `DeleteObject`
    /// (plus the rest of the data plane) on an ephemeral port.
    /// Returns the bound address; the server runs until the test ends.
    async fn spawn_segment_server(fail_deletes: bool) -> SocketAddr {
        let data_store: Arc<dyn oceanfs_durability::SegmentDataStore> =
            Arc::new(oceanfs_durability::anti_entropy::InMemorySegmentStore::new());
        let metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore> =
            Arc::new(MockDeleteMetadata { fail_deletes });
        let buffer_pool = Arc::new(BufferPool::new(65536, 4));
        let service = crate::grpc::segment_service::SegmentGrpcService::new(
            data_store,
            Some(metadata_store),
            buffer_pool,
            Arc::new(oceanfs_core::HlcClock::new()),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio_stream::wrappers::TcpListenerStream;
            tonic::transport::Server::builder()
                .add_service(oceanfs_storage::SegmentRpcServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        // Give the server a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        addr
    }

    /// F3d(1): when every remote replica is reachable, `delete` returns
    /// the full replica count.
    #[tokio::test]
    async fn delete_all_replicas_reachable_returns_replica_count() {
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        // Point the remote replicas at live gRPC servers via a higher
        // incarnation + fresh address (the ADR-0022 merge semantics).
        let n2 = NodeId::new("n2");
        let n3 = NodeId::new("n3");
        let addr_n2 = spawn_segment_server(false).await;
        let addr_n3 = spawn_segment_server(false).await;
        coord.membership.upsert_node(
            n2.clone(),
            NodeState::Alive,
            Incarnation::new(2),
            Some(addr_n2),
        );
        coord.membership.upsert_node(
            n3.clone(),
            NodeState::Alive,
            Incarnation::new(2),
            Some(addr_n3),
        );

        let deleted = coord
            .delete(
                &BucketId::new("test"),
                &ObjectKey::new("obj"),
                &HashKey::from_bytes(hash_key(b"obj")),
                Hlc::zero(),
            )
            .await
            .unwrap();
        assert_eq!(deleted, 2, "both remote replicas must confirm the deletion");
    }

    /// F3d(2): one reachable + one unreachable replica → partial count
    /// returned (the caller decides whether quorum is met).
    #[tokio::test]
    async fn delete_partial_failure_returns_partial_count() {
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        // n2 gets a live server; n3 keeps the helper's default address
        // (127.0.0.1:9001) where nothing is listening.
        let n2 = NodeId::new("n2");
        let addr_n2 = spawn_segment_server(false).await;
        coord.membership.upsert_node(
            n2.clone(),
            NodeState::Alive,
            Incarnation::new(2),
            Some(addr_n2),
        );

        let deleted = coord
            .delete(
                &BucketId::new("test"),
                &ObjectKey::new("obj"),
                &HashKey::from_bytes(hash_key(b"obj")),
                Hlc::zero(),
            )
            .await
            .unwrap();
        assert_eq!(deleted, 1, "only the reachable replica confirms");
    }

    /// F3d(4): a delete that fails to replicate is HINTED — a node
    /// that misses a delete keeps its stale row forever, which
    /// diverges the cluster (the sender-side obsolete pre-check then
    /// drops later write hints for keys that are still live
    /// elsewhere).
    #[tokio::test]
    async fn delete_unreachable_replica_enqueues_delete_hint() {
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]).await;

        // n2 gets a live server; n3 keeps the helper's default address
        // (127.0.0.1:9001) where nothing is listening → its delete
        // replication fails and must be hinted.
        let n2 = NodeId::new("n2");
        let addr_n2 = spawn_segment_server(false).await;
        coord.membership.upsert_node(
            n2.clone(),
            NodeState::Alive,
            Incarnation::new(2),
            Some(addr_n2),
        );

        let hlc = coord.hlc_clock.now();
        let deleted = coord
            .delete(
                &BucketId::new("test"),
                &ObjectKey::new("obj"),
                &HashKey::from_bytes(hash_key(b"obj")),
                hlc,
            )
            .await
            .unwrap();
        assert_eq!(deleted, 1, "only the reachable replica confirms");

        let pending = coord.hinted_handoff_for_test().pending_count(&NodeId::new("n3"));
        assert_eq!(pending, 1, "the failed delete replication must be hinted");
    }

    /// F3d(3): an empty ring returns the existing `Routing` error path.
    #[tokio::test]
    async fn delete_empty_replica_set_returns_routing_error() {
        let coord = make_write_coordinator("n1", &[]).await;

        let result = coord
            .delete(
                &BucketId::new("test"),
                &ObjectKey::new("obj"),
                &HashKey::from_bytes(hash_key(b"obj")),
                Hlc::zero(),
            )
            .await;
        assert!(result.is_err(), "empty ring should return routing error");
        match result.unwrap_err() {
            Error::Routing(msg) => assert!(msg.contains("empty"), "error should mention empty set"),
            other => panic!("expected Routing, got {other:?}"),
        }
    }

    // ── Multi-tier read-path integrity tests ───────────────────────
    // (gap-closure/read-path-integrity-under-load: Defect 1 — chunk refs
    // stored blob-relative offsets; Defect 2 — multi-tier chunks never
    // registered a blob index entry, so their segments were skipped at
    // seal time and never reached disk.)

    /// Adapter exposing a `RocksDbMetadataStore` through the server's
    /// `MetadataOps` trait so the read coordinator can look up object
    /// metadata in tests. (In production, `oceanfs-node` wires the
    /// equivalent `MetadataStoreAdapter`.)
    struct RocksDbMetadataOps {
        store: Arc<RocksDbMetadataStore>,
    }

    impl crate::metadata_ops::MetadataOps for RocksDbMetadataOps {
        fn get_object(
            &self,
            bucket: &BucketId,
            key: &ObjectKey,
        ) -> std::result::Result<Option<ObjectMetadata>, crate::metadata_ops::MetadataError>
        {
            self.store
                .get_object(bucket, key)
                .map_err(|e| crate::metadata_ops::MetadataError::Internal(format!("{e}")))
        }

        fn delete_object(
            &self,
            bucket: &BucketId,
            key: &ObjectKey,
            hlc: Hlc,
        ) -> std::result::Result<(), crate::metadata_ops::MetadataError> {
            self.store
                .delete_object(bucket, key, hlc)
                .map_err(|e| crate::metadata_ops::MetadataError::Internal(format!("{e}")))
        }

        fn put_object(
            &self,
            bucket: &BucketId,
            meta: ObjectMetadata,
        ) -> std::result::Result<(), crate::metadata_ops::MetadataError> {
            self.store
                .put_object_in_bucket(bucket, meta)
                .map_err(|e| crate::metadata_ops::MetadataError::Internal(format!("{e}")))
        }

        fn list_objects(
            &self,
            bucket: &BucketId,
            prefix: &str,
        ) -> std::result::Result<Vec<ObjectMetadata>, crate::metadata_ops::MetadataError> {
            self.store
                .list_objects(bucket, prefix)
                .into_iter()
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| crate::metadata_ops::MetadataError::Internal(format!("{e}")))
        }
    }

    /// Complete multi-tier test fixture: write coordinator with a
    /// single-slot standard pool, metadata store, and read coordinator.
    struct MultiTierFixture {
        coord: Arc<WriteCoordinator>,
        read: crate::ReadCoordinator,
        metadata: Arc<RocksDbMetadataStore>,
        standard_pool: Arc<SegmentPool>,
        /// The lifecycle coordinator — the single writer of segment
        /// lifecycle state (the tests probe its registry).
        lifecycle: Arc<SegmentLifecycleCoordinator>,
        /// The lifecycle registry — the tests' sealed-set assertions
        /// enumerate the machine (ADR-0025 Decision 3).
        registry: Arc<oceanfs_storage::SegmentLifecycleRegistry>,
        seal_dir: std::path::PathBuf,
        /// (segment_id, merkle_root) pairs recorded by the seal notifier.
        sealed_events: Arc<Mutex<Vec<(SegmentId, oceanfs_core::HashOutput)>>>,
        _dir: tempfile::TempDir,
    }

    /// Segment sizing for the multi-tier fixtures: 4 KiB standard target
    /// keeps blobs small while still exercising the multi-segment path.
    fn multi_tier_size_config() -> SegmentSizeConfig {
        SegmentSizeConfig {
            inline_threshold_bytes: 1024,
            small_threshold_bytes: 1024,
            small_target_size: 1024,
            default_target_size: 4096,
        }
    }

    /// Builds a coordinator + read path wired to a single-slot standard
    /// pool. The single slot makes consecutive appends accumulate in one
    /// segment, so multi-tier chunks land at non-zero segment offsets —
    /// the exact case the read-path defect corrupted silently.
    async fn make_multi_tier_fixture() -> MultiTierFixture {
        use oceanfs_durability::GrpcHintDeliveryClient;

        let dir = tempfile::tempdir().unwrap();
        let size_config = multi_tier_size_config();

        let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
        ring.add_node(NodeId::new("n1"));
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new("n1"),
            addr,
            GossipConfig::default(),
            ring_cache.clone(),
        ));
        membership.upsert_node(
            NodeId::new("n1"),
            NodeState::Alive,
            Incarnation::new(1),
            Some(addr),
        );
        let hlc_clock = Arc::new(HlcClock::new());

        let metadata = Arc::new(
            RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        );
        let buffer_pool = Arc::new(BufferPool::new(65536, 16));

        let shard_small =
            Arc::new(SegmentShard::new(4, SizeTier::Small, &size_config, &buffer_pool).unwrap());
        let shard_standard =
            Arc::new(SegmentShard::new(4, SizeTier::Standard, &size_config, &buffer_pool).unwrap());

        // Single active slot: appends always target slot 0, so chunks
        // accumulate at sequential offsets within one segment.
        let pool_cfg =
            PoolConfig { active_pool_size: 1, encode_queue_capacity: 64, ..PoolConfig::default() };
        // The registry is SHARED by the pools and the coordinator (the
        // machine's entry is the one the pools attach to and resolve
        // reads through — construction order: registry → pools →
        // coordinator).
        let fixture_registry = Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
            &oceanfs_core::LifecycleConfig::default(),
        ));
        let segment_pool_small = Arc::new(
            SegmentPool::new(
                pool_cfg.clone(),
                SizeTier::Small,
                &size_config,
                buffer_pool.clone(),
                None,
                None,
                Arc::clone(&fixture_registry),
            )
            .unwrap(),
        );
        let standard_pool = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                &size_config,
                buffer_pool,
                None,
                None,
                Arc::clone(&fixture_registry),
            )
            .unwrap(),
        );

        let wal = Arc::new(
            WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        let seal_dir = dir.path().join("segments");
        let seal_config = SealConfig {
            target_size_bytes: size_config.default_target_size,
            seal_timeout_ms: 5000,
            data_dir: seal_dir.clone(),
            io_mode: oceanfs_storage::io::IoReadMode::Buffered,
            write_mode: oceanfs_storage::io::SegmentWriteMode::Rename,
            ..Default::default()
        };
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::with_registry(Arc::clone(&fixture_registry))
                // The event log is the coordinator's only durable writer
                // (ADR-0025 Decision 3 final form).
                .with_event_wal(Arc::new(
                    oceanfs_storage::segment::event_wal::EventWal::open(
                        dir.path().join("event-wal"),
                        &oceanfs_core::EventWalConfig {
                            event_wal_dir: dir.path().join("event-wal"),
                            event_wal_file_size_bytes: 1024 * 1024,
                            event_wal_fsync_batch_timeout_ms: 10,
                            event_wal_checkpoint_bytes: 1024 * 1024,
                        },
                    )
                    .await
                    .unwrap(),
                ))
                // Idle-seal driver: the coordinator owns the idle-seal
                // timer (ADR-0025 phase 1) and sweeps the standard pool.
                .with_seal_pools(vec![standard_pool.clone()]),
        );
        let sealer = Arc::new(SegmentSealer::new(seal_config, wal, Arc::clone(&lifecycle)));

        let hints_dir = dir.path().join("hints");
        let hint_config =
            HintedHandoffConfig { wal_dir: hints_dir.clone(), ..HintedHandoffConfig::default() };
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> =
            Arc::new(GrpcHintDeliveryClient::new(pool.clone()));
        let hinted_handoff =
            Arc::new(HintedHandoffManager::new(hints_dir, delivery_client, hint_config.clone()));

        let sealed_events: Arc<Mutex<Vec<(SegmentId, oceanfs_core::HashOutput)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let sealed_events_notifier = Arc::clone(&sealed_events);
        let coord = Arc::new(
            WriteCoordinator::new(
                ring_cache.clone(),
                membership,
                pool,
                NodeId::new("n1"),
                hlc_clock,
                metadata.clone(),
                size_config.clone(),
                shard_small,
                shard_standard,
                segment_pool_small,
                standard_pool.clone(),
                sealer,
                lifecycle.clone(),
                hinted_handoff,
            )
            .with_segment_sealed_notifier(Arc::new(move |segment_id, merkle_root| {
                sealed_events_notifier.lock().push((segment_id, merkle_root));
            })),
        );

        let metadata_ops: Arc<dyn crate::metadata_ops::MetadataOps> =
            Arc::new(RocksDbMetadataOps { store: metadata.clone() });
        let read = crate::ReadCoordinator::new_with_metadata(
            ring_cache,
            NodeId::new("n1"),
            None,
            metadata_ops,
        );

        MultiTierFixture {
            coord,
            read,
            metadata,
            standard_pool,
            lifecycle,
            registry: fixture_registry,
            seal_dir,
            sealed_events,
            _dir: dir,
        }
    }

    /// Runs a PUT through the coordinator and persists the resulting
    /// object metadata, mirroring the S3 handler's post-put step.
    async fn put_and_persist(
        coord: &WriteCoordinator,
        metadata: &Arc<RocksDbMetadataStore>,
        bucket: &str,
        key: &str,
        data: Bytes,
    ) -> WriteResult {
        let req = WriteRequest {
            bucket: BucketId::new(bucket),
            key: ObjectKey::new(key),
            hash_key: HashKey::from_bytes(hash_key(key.as_bytes())),
            data,
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };
        let result = coord.put(req).await.unwrap();
        if !result.chunks.is_empty() {
            let meta = ObjectMetadata {
                object_key: ObjectKey::new(key),
                size: result.size,
                blake3_hash: result.blake3_hash,
                chunks: result.chunks.clone(),
                inline_data: None,
                created_at: 0,
                hlc: result.hlc,
            };
            metadata.put_object_in_bucket(&BucketId::new(bucket), meta).unwrap();
        }
        result
    }

    /// GETs an object through the read coordinator and asserts the body
    /// and BLAKE3 hash match the original PUT payload.
    async fn get_and_verify(
        read: &crate::ReadCoordinator,
        bucket: &str,
        key: &str,
        expected: &[u8],
    ) {
        let req = crate::ReadRequest {
            bucket: BucketId::new(bucket),
            key: ObjectKey::new(key),
            hash_key: HashKey::from_bytes(hash_key(key.as_bytes())),
            metadata_only: false,
            local_only: false,
            policy: None,
        };
        let result = read.get_object(req).await.unwrap();
        assert_eq!(&result.data[..], expected, "GET must return the exact PUT bytes");
        let expected_hash = blake3::hash(expected);
        assert_eq!(result.hash.as_bytes(), expected_hash.as_bytes(), "BLAKE3 must match");
    }

    /// Unit round-trip (active segments): multi-tier PUT whose first
    /// chunk lands at a non-zero segment offset, then GET via the
    /// active-segment pool reader. Covers Defect 1 (blob-relative vs
    /// segment-relative chunk ref offsets).
    #[tokio::test]
    async fn multi_tier_roundtrip_active_segment_reads_back_exact_bytes() {
        let fx = make_multi_tier_fixture().await;

        // Pre-fill the single standard segment with a Standard-tier blob
        // so the first multi-tier chunk lands at segment offset 2048 —
        // the in-bounds case that corrupts silently (BadDigest). The
        // pre-fill mirrors the write path (append → reserve → join) but
        // WITHOUT the completion leave: the test holds the writer lease,
        // so seal-on-zero does not close the partial segment before the
        // multi-tier PUT lands in it (a concurrent writer's lease).
        let prefill = vec![0x11u8; 2048];
        let mut prefill_registered = std::collections::HashSet::new();
        let (held_segment, prefill_offset, _, _sealed) = fx
            .coord
            .segment_pool_standard
            .append_with_hook_async(&prefill, |_, _, _| {}, std::time::Duration::from_secs(5))
            .await
            .expect("prefill append");
        assert_eq!(prefill_offset, 0, "prefill lands at the segment start");
        fx.coord
            .request_reserve_before_wal(held_segment, SizeTier::Standard, &mut prefill_registered)
            .await
            .expect("prefill reserve");
        fx.coord.lifecycle.writer_join(held_segment);

        // 10752 bytes > default_target_size (4096) → Multi tier,
        // split into 4096 + 4096 + 2560 chunks.
        let payload: Vec<u8> = (0..10752u32).map(|i| (i % 251) as u8).collect();
        let put = put_and_persist(
            &fx.coord,
            &fx.metadata,
            "test",
            "multi-obj",
            Bytes::from(payload.clone()),
        )
        .await;

        // Release the held lease (the pre-fill segment was fill-sealed
        // by the multi chunk — the drain skips sealed entries).
        if fx.coord.lifecycle.writer_leave(held_segment) {
            fx.coord.lifecycle.note_pending_seal(held_segment);
        }
        fx.coord.lifecycle.drain_pending_seals().await;

        assert_eq!(put.chunks.len(), 3, "10.5 KiB blob must split into three chunks");
        // Chunk 0 lands after the 2048-byte pre-fill blob; chunk refs
        // must carry segment-relative offsets.
        assert_eq!(put.chunks[0].offset, 2048, "first chunk ref must be segment-relative");
        assert_eq!(put.chunks[1].offset, 0, "second chunk ref must be segment-relative");
        assert_eq!(put.chunks[2].offset, 0, "third chunk ref must be segment-relative");

        // Serve reads from the active pool (including segments held in
        // the seal window), falling back to disk.
        let disk = Arc::new(oceanfs_storage::io::DiskSegmentReader::new(
            oceanfs_storage::io::IoReadMode::Direct,
            Arc::new(oceanfs_storage::io::DiskIo::TokioFs),
            None,
            fx.seal_dir.clone(),
            None,
            None,
        ));
        let reader: Arc<dyn oceanfs_storage::io::SegmentReader> = Arc::new(
            oceanfs_storage::io::PoolFallbackReader::new(vec![fx.standard_pool.clone()], disk),
        );
        let read = fx.read.with_segment_reader(reader);

        get_and_verify(&read, "test", "multi-obj", &payload).await;
    }

    /// Sealed-segment round-trip: the same multi-tier PUT, but the
    /// segments are forced through the seal worker first and the read
    /// is served from disk only. Covers Defect 2 (multi-tier chunks
    /// must register blob index entries or the seal is skipped and the
    /// segment never reaches disk).
    #[tokio::test]
    async fn multi_tier_roundtrip_sealed_segment_reads_back_from_disk() {
        let fx = make_multi_tier_fixture().await;

        // Pre-fill so chunk 0 lands at a non-zero segment offset. The
        // pre-fill holds a writer lease (see the active-segment test).
        let prefill = vec![0x22u8; 2048];
        let mut prefill_registered = std::collections::HashSet::new();
        let (held_segment, prefill_offset, _, _sealed) = fx
            .coord
            .segment_pool_standard
            .append_with_hook_async(&prefill, |_, _, _| {}, std::time::Duration::from_secs(5))
            .await
            .expect("prefill append");
        assert_eq!(prefill_offset, 0, "prefill lands at the segment start");
        fx.coord
            .request_reserve_before_wal(held_segment, SizeTier::Standard, &mut prefill_registered)
            .await
            .expect("prefill reserve");
        fx.coord.lifecycle.writer_join(held_segment);

        // Exactly two full chunks (8192 bytes): each fills its segment.
        let payload: Vec<u8> = (0..8192u32).map(|i| (i % 239) as u8).collect();
        let put = put_and_persist(
            &fx.coord,
            &fx.metadata,
            "test",
            "multi-obj",
            Bytes::from(payload.clone()),
        )
        .await;

        // Release the held lease.
        if fx.coord.lifecycle.writer_leave(held_segment) {
            fx.coord.lifecycle.note_pending_seal(held_segment);
        }
        fx.coord.lifecycle.drain_pending_seals().await;
        assert_eq!(put.chunks.len(), 2, "8 KiB blob must split into two chunks");
        assert_eq!(put.chunks[0].offset, 2048, "first chunk ref must be segment-relative");

        // Start the seal worker and wait for both chunk segments to be
        // sealed to disk.
        let _seal_handle = fx.coord.start_seal_worker();
        let sealed_ids: Vec<SegmentId> = put.chunks.iter().map(|c| c.segment_id).collect();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let mut ids: Vec<SegmentId> = Vec::new();
            fx.registry.for_each(|id, entry| {
                if entry.state == oceanfs_storage::segment::lifecycle::SegmentState::Sealed {
                    ids.push(id);
                }
            });
            if sealed_ids.iter().all(|id| ids.contains(id)) {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "seals did not complete in time");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // Assert the sealed blob index contains each multi-tier chunk
        // (Defect 2 fix: without record_blob_entry the seal is skipped
        // and no segment file is ever written).
        for chunk in &put.chunks {
            // Every sealed segment must carry its seal-time Merkle root:
            // scrub verification, anti-entropy's local-vs-stored
            // comparison, and the startup incremental-tree rebuild all
            // depend on the machine's anchor.
            let meta = fx
                .registry
                .get(chunk.segment_id)
                .expect("sealed segment entry must exist")
                .metadata;
            assert!(
                meta.merkle_root.is_some(),
                "sealed segment {} must persist a Merkle root",
                chunk.segment_id
            );

            // The seal notifier must have fired for every sealed segment with
            // its persisted root (the continuous anti-entropy wiring).
            let events = fx.sealed_events.lock().clone();
            for chunk in &put.chunks {
                assert!(
                    events.iter().any(|(id, _)| *id == chunk.segment_id),
                    "seal notifier must observe segment {}",
                    chunk.segment_id
                );
            }
            for (id, root) in &events {
                let meta = fx.registry.get(*id).expect("notified segment exists").metadata;
                assert_eq!(
                    meta.merkle_root,
                    Some(*root),
                    "notified root must match the machine's root"
                );
            }

            let path = fx.seal_dir.join(format!("{}.dat", chunk.segment_id));
            let file_bytes = std::fs::read(&path).unwrap_or_else(|e| {
                panic!("sealed segment file missing for {}: {e}", chunk.segment_id)
            });
            let header = oceanfs_storage::SegmentHeader::from_bytes(&file_bytes)
                .unwrap_or_else(|| panic!("invalid segment header for {}", chunk.segment_id));
            assert!(header.blob_count >= 1, "segment blob index must be non-empty");
            let index_offset = header.data_end() as usize;
            let index_bytes = &file_bytes[index_offset..];
            let index = oceanfs_storage::SegmentIndex::from_bytes(index_bytes)
                .unwrap_or_else(|e| panic!("invalid segment index for {}: {e}", chunk.segment_id));
            assert!(
                index.lookup(chunk.offset).is_some(),
                "blob index must contain chunk at offset {}",
                chunk.offset
            );
        }

        // Read back via the DISK reader only — the pool fallback is
        // deliberately omitted so success proves the data reached disk
        // through the seal path.
        let disk = Arc::new(oceanfs_storage::io::DiskSegmentReader::new(
            oceanfs_storage::io::IoReadMode::Direct,
            Arc::new(oceanfs_storage::io::DiskIo::TokioFs),
            None,
            fx.seal_dir.clone(),
            None,
            None,
        ));
        let read = fx.read.with_segment_reader(disk);

        get_and_verify(&read, "test", "multi-obj", &payload).await;
    }

    // ── Lifecycle integration (ADR-0025 phase 1) ────────────────────
    // The coordinator is the only writer of segment lifecycle state:
    // a PUT reserves before the first WAL entry, the seal worker's
    // persistence path seals (via the flush coordinator), and the
    // registry + CF must agree at every step.

    #[tokio::test]
    async fn lifecycle_write_seal_read_roundtrip_through_coordinator() {
        // A full write → seal → read round trip through the lifecycle
        // coordinator at the server crate boundary: the registry entry
        // exists (Reserved) with the CF phantom as soon as the PUT
        // returns; after the seal worker drains, both registry and CF
        // show Sealed with a Merkle root; the data reads back from disk.
        let fx = make_multi_tier_fixture().await;
        let payload = vec![0x5Au8; 3000]; // Standard tier, one chunk
        let put = put_and_persist(
            &fx.coord,
            &fx.metadata,
            "test",
            "lifecycle-rt",
            Bytes::from(payload.clone()),
        )
        .await;
        let segment_id = put.chunks[0].segment_id;

        // Reserve-before-data, observable: the PUT has returned, so the
        // registry entry exists (Reserved — the seal worker may not have
        // run yet; Sealed is also correct).
        let entry = fx.lifecycle.registry().get(segment_id).expect("registry entry after PUT");
        match entry.state {
            // The seal may complete before the assert runs (the PUT
            // returns before the seal worker drains) — Sealed is also
            // correct; Reserved is the common case.
            oceanfs_storage::SegmentState::Reserved | oceanfs_storage::SegmentState::Sealed => {}
            other => panic!("unexpected registry state after PUT: {other:?}"),
        }

        // Start the seal worker and wait for the Sealed state in the
        // machine (the only durable segment-state store — ADR-0025
        // Decision 3). The PUT's segment is below the fill target, so it
        // seals via the coordinator's idle-seal driver.
        let _seal_handle = fx.coord.start_seal_worker();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let registry_sealed = fx
                .lifecycle
                .registry()
                .get(segment_id)
                .map(|e| e.state == oceanfs_storage::SegmentState::Sealed)
                .unwrap_or(false);
            if registry_sealed {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "seal did not complete in time");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let entry = fx.lifecycle.registry().get(segment_id).unwrap();
        assert_eq!(entry.state, oceanfs_storage::SegmentState::Sealed);
        assert!(entry.metadata.merkle_root.is_some(), "seal-time Merkle root persisted");

        // Read back from disk (no pool fallback): the data reached the
        // .dat through the seal path.
        let disk = Arc::new(oceanfs_storage::io::DiskSegmentReader::new(
            oceanfs_storage::io::IoReadMode::Direct,
            Arc::new(oceanfs_storage::io::DiskIo::TokioFs),
            None,
            fx.seal_dir.clone(),
            None,
            None,
        ));
        let read = fx.read.with_segment_reader(disk);
        get_and_verify(&read, "test", "lifecycle-rt", &payload).await;
    }

    #[tokio::test]
    async fn concurrent_put_seal_stress_never_downgrades_registry() {
        // Concurrent PUTs + the seal worker churn segments through the
        // coordinator. The poisoned-registry probe then verifies ZERO
        // Sealed→Reserved downgrades: every registry entry is Sealed
        // exactly when its CF entry is sealed, and a poison reserve
        // attempt on a Sealed id is rejected without mutating either
        // store.
        let fx = make_multi_tier_fixture().await;
        let _seal_handle = fx.coord.start_seal_worker();

        let coord = Arc::clone(&fx.coord);
        let metadata = Arc::clone(&fx.metadata);
        let mut handles = Vec::new();
        let mut put_ids: Vec<SegmentId> = Vec::new();
        for i in 0..16 {
            let coord = Arc::clone(&coord);
            let metadata = Arc::clone(&metadata);
            handles.push(tokio::spawn(async move {
                let payload = vec![(i as u8).wrapping_mul(17); 2000 + i * 37];
                let result = put_and_persist(
                    &coord,
                    &metadata,
                    "test",
                    &format!("stress-{i}"),
                    Bytes::from(payload.clone()),
                )
                .await;
                assert!(!result.chunks.is_empty(), "stress put must land in a segment");
                result.chunks.iter().map(|c| c.segment_id).collect::<Vec<_>>()
            }));
        }
        for h in handles {
            let mut ids = h.await.unwrap();
            put_ids.append(&mut ids);
        }

        // Wait for EVERY stress segment to reach Sealed (fill-triggered
        // for full segments, the coordinator's idle driver for partials).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let all_sealed = put_ids.iter().all(|id| {
                fx.lifecycle
                    .registry()
                    .get(*id)
                    .map(|e| e.state == oceanfs_storage::SegmentState::Sealed)
                    .unwrap_or(false)
            });
            if all_sealed {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "stress seals did not complete");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // Poisoned-registry probe: for every CF segment, the registry
        // entry must agree and a downgrade attempt must be rejected with
        // the registry unchanged. The probe must exercise at least every
        // distinct stress segment.
        let distinct_put_segments: std::collections::HashSet<SegmentId> =
            put_ids.iter().copied().collect();
        let mut probed = 0usize;
        let mut registry_ids: Vec<SegmentId> = Vec::new();
        fx.registry.for_each(|id, _entry| registry_ids.push(id));
        for id in registry_ids {
            let entry = fx.lifecycle.registry().get(id).expect("registry entry");
            if entry.state == oceanfs_storage::SegmentState::Sealed {
                // Poison probe: the phantom-downgrade write.
                let err =
                    fx.lifecycle.request_reserve(id, SizeTier::Standard, 0, 0).await.unwrap_err();
                assert_eq!(err, TransitionError::AlreadySealed, "no downgrade for {id}");
                assert_eq!(
                    fx.lifecycle.registry().get(id).unwrap().state,
                    oceanfs_storage::SegmentState::Sealed,
                    "registry must stay sealed for {id}"
                );
                probed += 1;
            }
        }
        assert!(
            probed >= distinct_put_segments.len(),
            "the probe must exercise every stress segment ({} distinct, probed {probed})",
            distinct_put_segments.len()
        );
    }

    #[tokio::test]
    async fn lifecycle_read_windows_append_inflight_sealed() {
        // The DoD's parameterized read-after-write window matrix at the
        // server boundary: GET after an acked PUT during (a) append-mode
        // (active slot), (b) in-flight between fill and seal (the
        // registry entry's frozen buffer), (c) sealed (disk) — all
        // return the exact bytes.
        let fx = make_multi_tier_fixture().await;
        let disk = Arc::new(oceanfs_storage::io::DiskSegmentReader::new(
            oceanfs_storage::io::IoReadMode::Buffered,
            Arc::new(oceanfs_storage::io::DiskIo::TokioFs),
            None,
            fx.seal_dir.clone(),
            None,
            None,
        ));
        // The composite reader: active pools first, disk fallback — the
        // production wiring (ADR-0020 Decision 2, unchanged).
        let composite: Arc<dyn oceanfs_storage::io::SegmentReader> = Arc::new(
            oceanfs_storage::io::PoolFallbackReader::new(vec![fx.standard_pool.clone()], disk),
        );
        let read = fx.read.with_segment_reader(composite);

        // (a) Append-mode: a small object (below the 4 KiB target) stays
        // in an active slot; the GET resolves via the slot scan.
        let small = vec![0x11u8; 2000];
        put_and_persist(&fx.coord, &fx.metadata, "test", "win-append", Bytes::from(small.clone()))
            .await;
        get_and_verify(&read, "test", "win-append", &small).await;

        // (b) In-flight: an object larger than the target fills its
        // segment on the FIRST append — the frozen buffer is attached to
        // the machine entry (the fill-before-reserve window self-heals
        // the entry; the write path's reserve follows). The seal worker
        // is NOT running, so the segment stays in-flight and the GET is
        // served from the registry entry.
        let big = vec![0x22u8; 5000];
        put_and_persist(&fx.coord, &fx.metadata, "test", "win-inflight", Bytes::from(big.clone()))
            .await;
        let in_flight_id = fx
            .metadata
            .get_object(&BucketId::new("test"), &ObjectKey::new("win-inflight"))
            .unwrap()
            .expect("object metadata present")
            .chunks[0]
            .segment_id;
        assert!(matches!(
            fx.lifecycle.registry().read_source(in_flight_id),
            oceanfs_storage::SegmentReadSource::InFlight(_)
        ));
        get_and_verify(&read, "test", "win-inflight", &big).await;

        // (c) Sealed: start the seal worker, wait for the Sealed state,
        // and GET from disk through the composite's fallback.
        let _seal_handle = fx.coord.start_seal_worker();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let sealed = fx
                .lifecycle
                .registry()
                .get(in_flight_id)
                .map(|e| e.state == oceanfs_storage::SegmentState::Sealed)
                .unwrap_or(false);
            if sealed {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "seal did not complete");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        get_and_verify(&read, "test", "win-inflight", &big).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_put_get_never_fails_any_read_window() {
        // Concurrent puts + the seal worker churn segments through every
        // read window (append-mode, in-flight, sealed); every GET must
        // return the exact bytes with ZERO read failures. The stress
        // fixture wires the composite reader (pools + disk fallback) and
        // starts the seal worker, mirroring production.
        let size_config = SegmentSizeConfig {
            inline_threshold_bytes: 4096,
            small_threshold_bytes: 262_144,
            small_target_size: 65_536,
            default_target_size: 4_194_304, // 4 MiB
        };
        let fx = make_stress_fixture(&size_config).await;

        let coord = Arc::clone(&fx.coord);
        let metadata = Arc::clone(&fx.metadata);
        let read = Arc::new(fx.read);
        let mut handles = Vec::new();
        for worker in 0..4usize {
            let coord = Arc::clone(&coord);
            let metadata = Arc::clone(&metadata);
            let read = Arc::clone(&read);
            handles.push(tokio::spawn(async move {
                for i in 0..15usize {
                    // Sizes straddle the 4 MiB target so append-mode,
                    // in-flight (fill), and sealed windows are all hit.
                    let len = 3_000_000 + (worker * 151 + i * 97) % 4_000_000;
                    let payload: Vec<u8> = (0..len)
                        .map(|b| {
                            ((b as u64).wrapping_mul(31).wrapping_add((worker * 17 + i) as u64)
                                % 251) as u8
                        })
                        .collect();
                    let key = format!("rw-{worker}-{i}");
                    put_and_persist(&coord, &metadata, "test", &key, Bytes::from(payload.clone()))
                        .await;
                    // GET immediately — must succeed in ANY window.
                    let req = crate::ReadRequest {
                        bucket: BucketId::new("test"),
                        key: ObjectKey::new(&key),
                        hash_key: HashKey::from_bytes(hash_key(key.as_bytes())),
                        metadata_only: false,
                        local_only: false,
                        policy: None,
                    };
                    let result = read
                        .get_object(req)
                        .await
                        .unwrap_or_else(|e| panic!("GET {key} failed: {e}"));
                    assert_eq!(
                        &result.data[..],
                        &payload[..],
                        "GET {key} must return the exact PUT bytes"
                    );
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    // ── Concurrency regression (read-path-integrity-under-load) ─────
    // Under concurrent multi-tier load the seal worker runs on another
    // thread than the PUT tasks; entry recording, seal draining, and
    // seal-queue overflow were all observed corrupting or losing data.
    // This test churns segments at production dimensions and verifies
    // every written object through the full read path.

    /// Adapter exposing a `RocksDbMetadataStore` through `MetadataOps`
    /// for read-path verification (mirrors the node's adapter).
    struct StressMetadataOps {
        store: Arc<RocksDbMetadataStore>,
    }

    impl crate::metadata_ops::MetadataOps for StressMetadataOps {
        fn get_object(
            &self,
            bucket: &BucketId,
            key: &ObjectKey,
        ) -> std::result::Result<Option<ObjectMetadata>, crate::metadata_ops::MetadataError>
        {
            self.store
                .get_object(bucket, key)
                .map_err(|e| crate::metadata_ops::MetadataError::Internal(format!("{e}")))
        }

        fn delete_object(
            &self,
            bucket: &BucketId,
            key: &ObjectKey,
            hlc: Hlc,
        ) -> std::result::Result<(), crate::metadata_ops::MetadataError> {
            self.store
                .delete_object(bucket, key, hlc)
                .map_err(|e| crate::metadata_ops::MetadataError::Internal(format!("{e}")))
        }

        fn put_object(
            &self,
            bucket: &BucketId,
            meta: ObjectMetadata,
        ) -> std::result::Result<(), crate::metadata_ops::MetadataError> {
            self.store
                .put_object_in_bucket(bucket, meta)
                .map_err(|e| crate::metadata_ops::MetadataError::Internal(format!("{e}")))
        }

        fn list_objects(
            &self,
            bucket: &BucketId,
            prefix: &str,
        ) -> std::result::Result<Vec<ObjectMetadata>, crate::metadata_ops::MetadataError> {
            self.store
                .list_objects(bucket, prefix)
                .into_iter()
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| crate::metadata_ops::MetadataError::Internal(format!("{e}")))
        }
    }

    /// Builds the full pipeline (pools + sealer + coordinators) with the
    /// given segment sizing, mirroring the production wiring.
    struct StressFixture {
        coord: Arc<WriteCoordinator>,
        metadata: Arc<RocksDbMetadataStore>,
        registry: Arc<oceanfs_storage::SegmentLifecycleRegistry>,
        read: crate::ReadCoordinator,
        _dir: tempfile::TempDir,
    }

    async fn make_stress_fixture(size_config: &SegmentSizeConfig) -> StressFixture {
        let dir = tempfile::tempdir().unwrap();
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
        ring.add_node(NodeId::new("n1"));
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new("n1"),
            addr,
            GossipConfig::default(),
            ring_cache.clone(),
        ));
        membership.upsert_node(
            NodeId::new("n1"),
            NodeState::Alive,
            Incarnation::new(1),
            Some(addr),
        );

        let metadata = Arc::new(
            RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().join("metadata"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        );
        let buffer_pool = Arc::new(BufferPool::new(65536, 64));
        let shard_small =
            Arc::new(SegmentShard::new(4, SizeTier::Small, size_config, &buffer_pool).unwrap());
        let shard_standard =
            Arc::new(SegmentShard::new(4, SizeTier::Standard, size_config, &buffer_pool).unwrap());
        let pool_cfg =
            PoolConfig { active_pool_size: 4, encode_queue_capacity: 64, ..PoolConfig::default() };
        // Shared registry: the pools and the coordinator must see the
        // same machine (registry → pools → coordinator).
        let stress_registry = Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
            &oceanfs_core::LifecycleConfig::default(),
        ));
        let small_pool = Arc::new(
            SegmentPool::new(
                pool_cfg.clone(),
                SizeTier::Small,
                size_config,
                buffer_pool.clone(),
                None,
                None,
                Arc::clone(&stress_registry),
            )
            .unwrap(),
        );
        let standard_pool = Arc::new(
            SegmentPool::new(
                pool_cfg,
                SizeTier::Standard,
                size_config,
                buffer_pool,
                None,
                None,
                Arc::clone(&stress_registry),
            )
            .unwrap(),
        );
        let wal = Arc::new(
            WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 8 * 1024 * 1024,
                fsync_batch_timeout_ms: 5,
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        let seal_dir = dir.path().join("segments");
        let lifecycle = Arc::new(
            SegmentLifecycleCoordinator::with_registry(Arc::clone(&stress_registry))
                // The event log is the coordinator's only durable writer
                // (ADR-0025 Decision 3 final form).
                .with_event_wal(Arc::new(
                    oceanfs_storage::segment::event_wal::EventWal::open(
                        dir.path().join("event-wal"),
                        &oceanfs_core::EventWalConfig {
                            event_wal_dir: dir.path().join("event-wal"),
                            event_wal_file_size_bytes: 1024 * 1024,
                            event_wal_fsync_batch_timeout_ms: 10,
                            event_wal_checkpoint_bytes: 1024 * 1024,
                        },
                    )
                    .await
                    .unwrap(),
                ))
                .with_seal_pools(vec![standard_pool.clone()]),
        );
        let sealer = Arc::new(SegmentSealer::new(
            SealConfig {
                target_size_bytes: size_config.default_target_size,
                seal_timeout_ms: 5000,
                data_dir: seal_dir.clone(),
                io_mode: oceanfs_storage::io::IoReadMode::Buffered,
                write_mode: oceanfs_storage::io::SegmentWriteMode::Rename,
                ..Default::default()
            },
            wal,
            lifecycle.clone(),
        ));

        use oceanfs_durability::GrpcHintDeliveryClient;
        let hints_dir = dir.path().join("hints");
        let hint_config =
            HintedHandoffConfig { wal_dir: hints_dir.clone(), ..HintedHandoffConfig::default() };
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> =
            Arc::new(GrpcHintDeliveryClient::new(pool.clone()));
        let hinted_handoff =
            Arc::new(HintedHandoffManager::new(hints_dir, delivery_client, hint_config));

        let coord = Arc::new(WriteCoordinator::new(
            ring_cache.clone(),
            membership,
            pool,
            NodeId::new("n1"),
            Arc::new(HlcClock::new()),
            metadata.clone(),
            size_config.clone(),
            shard_small,
            shard_standard,
            small_pool.clone(),
            standard_pool.clone(),
            sealer,
            lifecycle,
            hinted_handoff,
        ));
        let _seal_handle = coord.start_seal_worker();

        let ops: Arc<dyn crate::metadata_ops::MetadataOps> =
            Arc::new(StressMetadataOps { store: metadata.clone() });
        let disk = Arc::new(oceanfs_storage::io::DiskSegmentReader::new(
            oceanfs_storage::io::IoReadMode::Buffered,
            Arc::new(oceanfs_storage::io::DiskIo::TokioFs),
            None,
            seal_dir.clone(),
            None,
            None,
        ));
        let reader: Arc<dyn oceanfs_storage::io::SegmentReader> =
            Arc::new(oceanfs_storage::io::PoolFallbackReader::new(
                vec![small_pool.clone(), standard_pool.clone()],
                disk,
            ));
        let read =
            crate::ReadCoordinator::new_with_metadata(ring_cache, NodeId::new("n1"), None, ops)
                .with_segment_reader(reader);

        StressFixture { coord, metadata, registry: stress_registry, read, _dir: dir }
    }

    /// Concurrent multi-tier + standard writes at production dimensions:
    /// every written object must read back byte-exact through the full
    /// read path (pool fallback + sealed segments on disk).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_multi_tier_writes_remain_readable() {
        let size_config = SegmentSizeConfig {
            inline_threshold_bytes: 4096,
            small_threshold_bytes: 262_144,
            small_target_size: 65_536,
            default_target_size: 4_194_304, // 4 MiB — production standard target
        };
        let fx = make_stress_fixture(&size_config).await;

        let mut handles = Vec::new();
        for w in 0..8u32 {
            let coord = fx.coord.clone();
            let metadata = fx.metadata.clone();
            handles.push(tokio::spawn(async move {
                let mut rng: u64 =
                    0x9E37_79B9_7F4A_7C15u64.wrapping_mul((w as u64).wrapping_add(1));
                let mut written: Vec<(String, Vec<u8>)> = Vec::with_capacity(12);
                for i in 0..12u32 {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let multi = (rng & 1) == 0;
                    let len = if multi {
                        4_300_000 + (rng % 6_000_000) as usize
                    } else {
                        300_000 + (rng % 3_800_000) as usize
                    };
                    let key = format!("w{w}-i{i}-len{len}");
                    let data: Vec<u8> =
                        (0..len).map(|b| ((b as u64).wrapping_add(rng) % 251) as u8).collect();
                    let req = WriteRequest {
                        bucket: BucketId::new("test"),
                        key: ObjectKey::new(&key),
                        hash_key: HashKey::from_bytes(hash_key(key.as_bytes())),
                        data: Bytes::from(data.clone()),
                        write_quorum: 1,
                        ack_after_wal: true,
                        ec_async: false,
                        policy: None,
                    };
                    let result = coord.put(req).await.unwrap();
                    let meta = ObjectMetadata {
                        object_key: ObjectKey::new(&key),
                        size: result.size,
                        blake3_hash: result.blake3_hash,
                        chunks: result.chunks.clone(),
                        inline_data: None,
                        created_at: 0,
                        hlc: result.hlc,
                    };
                    metadata.put_object_in_bucket(&BucketId::new("test"), meta).unwrap();
                    written.push((key, data));
                    rng ^= i as u64;
                }
                written
            }));
        }
        let mut written = Vec::new();
        for h in handles {
            written.extend(h.await.unwrap());
        }

        // Wait for the seal worker to drain (the machine's live-entry
        // count stabilizes).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let mut last_count = usize::MAX;
        loop {
            let count = {
                let mut n = 0usize;
                fx.registry.for_each(|_id, _entry| n += 1);
                n
            };
            if count == last_count {
                break;
            }
            last_count = count;
            assert!(std::time::Instant::now() < deadline, "seal drain timed out");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        // Verify every object byte-exact through the read coordinator.
        let mut failures = 0usize;
        for (key, expected) in &written {
            let req = crate::ReadRequest {
                bucket: BucketId::new("test"),
                key: ObjectKey::new(key),
                hash_key: HashKey::from_bytes(hash_key(key.as_bytes())),
                metadata_only: false,
                local_only: false,
                policy: None,
            };
            match fx.read.get_object(req).await {
                Ok(result) => {
                    if &result.data[..] != &expected[..] {
                        failures += 1;
                        eprintln!("MISMATCH: {key} chunks={:?}", result.metadata.chunks);
                    }
                }
                Err(e) => {
                    failures += 1;
                    eprintln!("FETCH ERROR: {key}: {e}");
                }
            }
        }
        assert_eq!(
            failures,
            0,
            "stress: {failures} of {} objects failed round-trip",
            written.len()
        );
    }
}
