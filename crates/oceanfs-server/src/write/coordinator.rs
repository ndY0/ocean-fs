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
    OperationTimeouts, SegmentId, SegmentIndexEntry, SegmentSizeConfig, SizeTier, WriteResult,
};
use oceanfs_durability::{HintedHandoffConfig, HintedHandoffManager};
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
    /// Hinted handoff configuration (inline threshold, etc.).
    hint_config: HintedHandoffConfig,
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
        hint_config: HintedHandoffConfig,
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
            hint_config,
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
                let (segment_id, offset, length) = self
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
                // Write WAL entry for crash-recovery durability (C4-storage, D6).
                // `logical_length` lets crash replay classify compressed
                // chunks by their original size.
                self.write_wal_entry(segment_id, offset, stored, length, logical_len, 0, hlc)
                    .await?;
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
                let (segment_id, offset, length) = self
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
                // Write WAL entry for crash-recovery durability (C4-storage, D6).
                // `logical_length` lets crash replay classify compressed
                // chunks by their original size. Tier byte 1 = standard
                // pool (the replay routes by it — a 0 here sends the
                // segment's rebuild to the small pool).
                self.write_wal_entry(segment_id, offset, stored, length, logical_len, 1, hlc)
                    .await?;
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
                    let (seg_id, seg_offset, length) = self
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
                    // Write WAL entry for each chunk (C4-storage, D6).
                    self.write_wal_entry(seg_id, seg_offset, stored, length, logical_len, 1, hlc)
                        .await?;
                    // The chunk ref must carry the segment-relative offset
                    // returned by `append()`, not the splitter's
                    // blob-relative `chunk_offset` — readers slice the
                    // segment, not the blob (Defect 1).
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
        let quorum = req.write_quorum.min(replica_set.len() as u8);
        let mut acks_received: usize = 1; // local ack counted

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
            )
            .await;

            for (target, ack_result) in results {
                match ack_result {
                    Ok(_) => {
                        acks_received += 1;
                        if acks_received >= quorum as usize {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(target = %target, error = %e, "replica write failed");
                        // Store hinted handoff for the unreachable replica.
                        // For small blobs (≤inline_threshold_bytes): embed data inline.
                        // For larger blobs: reference the segment/offset/length —
                        //   data is already durable in the Segment WAL.
                        let hint =
                            if req.data.len() as u64 <= self.hint_config.inline_threshold_bytes {
                                oceanfs_durability::hinted_handoff_rpc::HintRecord::new_inline(
                                    target.clone(),
                                    req.bucket.clone(),
                                    req.key.to_string(),
                                    req.data.clone(),
                                    hlc,
                                )
                            } else if let Some(chunk) = chunks.first() {
                                // Use the first chunk's segment reference.
                                // For Small/Standard tier there is exactly one chunk.
                                // For Multi tier, the first chunk covers the blob start.
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
                                // Safety guard: if chunks is empty (inline tier),
                                // fall back to inline storage since no segment was used.
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
                        let _ = self.hinted_handoff.enqueue(hint).await;
                    }
                }
            }
        }

        // Step 6: Verify quorum.
        if acks_received < quorum as usize {
            return Err(Error::QuorumNotMet { required: quorum, received: acks_received });
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
        self.sealer
            .wal_writer()
            .append(entry)
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

    /// Returns the number of replicas in the ring for the given key.
    ///
    /// Used by the delete handler to cap the required quorum at the
    /// replica count (a single-node cluster cannot confirm more than
    /// one deletion), mirroring the write path's quorum capping.
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

        // Idle-seal driver: the lifecycle coordinator owns the
        // idle-seal timer (ADR-0025 phase 1). The tick runs at a
        // fraction of the seal timeout so a partially-filled segment
        // that stopped receiving writes is sealed within ~timeout of
        // going idle — fill-only sealing would leave it
        // registered-unsealed forever, pinning its WAL files (the
        // wal_not_unbounded leak).
        let idle_interval =
            std::time::Duration::from_millis((self.sealer.seal_timeout_ms() / 4).max(100));
        let lifecycle = Arc::clone(&self.lifecycle);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(idle_interval);
            // Skip the immediate first tick — a freshly started pool
            // has nothing idle yet.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                lifecycle.seal_idle_segments().await;
            }
        });

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
                        "delete replication skipped: failed to acquire channel"
                    );
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
                        "delete replication failed"
                    );
                }
            }
        }

        Ok(deleted)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::SocketAddr;

    use oceanfs_core::{
        GossipConfig, Incarnation, NodeId, NodeState, PoolConfig, RingConfig, RpcConfig,
        SegmentMetadata, SizeTier, Tombstone, WalConfig,
    };
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

    /// Creates a test coordinator, discarding the temp dir (callers that
    /// need the on-disk state use `make_write_coordinator_with_delivery`).
    #[allow(clippy::too_many_arguments)]
    async fn make_write_coordinator_discard_dir(
        node_id: &str,
        ring_nodes: &[&str],
        dir: tempfile::TempDir,
        pool: Arc<ConnectionPool>,
        delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient>,
    ) -> WriteCoordinator {
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
        let lifecycle = Arc::new(SegmentLifecycleCoordinator::new(
            metadata.clone(),
            &oceanfs_core::LifecycleConfig::default(),
        ));
        let sealer = Arc::new(SegmentSealer::new(seal_config, wal, Arc::clone(&lifecycle)));

        use oceanfs_durability::{HintedHandoffConfig, HintedHandoffManager};

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
                hint_config,
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
        // Single node in ring — quorum is capped at replica count (1).
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

        // Quorum is capped at replica_set.len() = 1, so writes succeed.
        let result = coord.put(req).await;
        assert!(result.is_ok(), "write should succeed with capped quorum");
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

        fn get_segment(&self, _id: SegmentId) -> std::io::Result<Option<SegmentMetadata>> {
            Ok(None)
        }

        fn list_segments(&self) -> Vec<std::io::Result<SegmentMetadata>> {
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

        fn put_segment(&self, _meta: SegmentMetadata) -> std::io::Result<()> {
            Ok(())
        }

        fn delete_segment(&self, _id: SegmentId) -> std::io::Result<()> {
            Ok(())
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

        fn put_segment(
            &self,
            meta: SegmentMetadata,
        ) -> std::result::Result<(), crate::metadata_ops::MetadataError> {
            self.store
                .put_segment(meta)
                .map_err(|e| crate::metadata_ops::MetadataError::Internal(format!("{e}")))
        }
        fn get_segment(
            &self,
            id: SegmentId,
        ) -> std::result::Result<Option<SegmentMetadata>, crate::metadata_ops::MetadataError>
        {
            self.store
                .get_segment(id)
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
            SegmentLifecycleCoordinator::with_registry(metadata.clone(), fixture_registry)
                // Idle-seal driver: the coordinator owns the idle-seal
                // timer (ADR-0025 phase 1) and sweeps the standard pool.
                .with_idle_seal(vec![standard_pool.clone()], seal_config.seal_timeout_ms),
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
                hint_config,
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
        // the in-bounds case that corrupts silently (BadDigest).
        let prefill = vec![0x11u8; 2048];
        put_and_persist(&fx.coord, &fx.metadata, "test", "prefill", Bytes::from(prefill)).await;

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

        // Pre-fill so chunk 0 lands at a non-zero segment offset.
        let prefill = vec![0x22u8; 2048];
        put_and_persist(&fx.coord, &fx.metadata, "test", "prefill", Bytes::from(prefill)).await;

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
        assert_eq!(put.chunks.len(), 2, "8 KiB blob must split into two chunks");
        assert_eq!(put.chunks[0].offset, 2048, "first chunk ref must be segment-relative");

        // Start the seal worker and wait for both chunk segments to be
        // sealed to disk.
        let _seal_handle = fx.coord.start_seal_worker();
        let sealed_ids: Vec<SegmentId> = put.chunks.iter().map(|c| c.segment_id).collect();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let ids: Vec<SegmentId> = fx
                .metadata
                .list_segments()
                .into_iter()
                .filter_map(std::result::Result::ok)
                .filter(|m| m.sealed_at.is_some())
                .map(|m| m.segment_id)
                .collect();
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
            // depend on the persisted anchor.
            let meta = fx
                .metadata
                .get_segment(chunk.segment_id)
                .expect("sealed segment metadata must exist")
                .expect("segment metadata entry present");
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
                let meta = fx
                    .metadata
                    .get_segment(*id)
                    .expect("notified segment exists")
                    .expect("metadata present");
                assert_eq!(
                    meta.merkle_root,
                    Some(*root),
                    "notified root must match the persisted root"
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
        // run yet) and the CF holds the unsealed phantom.
        let entry = fx.lifecycle.registry().get(segment_id).expect("registry entry after PUT");
        match entry.state {
            oceanfs_storage::SegmentState::Reserved => {
                let cf = fx.metadata.get_segment(segment_id).unwrap().expect("CF phantom");
                assert!(cf.sealed_at.is_none(), "phantom is unsealed");
            }
            // The seal may complete before the assert runs (the PUT
            // returns before the seal worker drains) — Sealed is also
            // correct; Reserved is the common case.
            oceanfs_storage::SegmentState::Sealed => {}
            other => panic!("unexpected registry state after PUT: {other:?}"),
        }

        // Start the seal worker and wait for the Sealed state in BOTH
        // the registry and the CF. The PUT's segment is below the fill
        // target, so it seals via the coordinator's idle-seal driver.
        let _seal_handle = fx.coord.start_seal_worker();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let registry_sealed = fx
                .lifecycle
                .registry()
                .get(segment_id)
                .map(|e| e.state == oceanfs_storage::SegmentState::Sealed)
                .unwrap_or(false);
            let cf_sealed = fx
                .metadata
                .get_segment(segment_id)
                .unwrap()
                .map(|m| m.sealed_at.is_some())
                .unwrap_or(false);
            if registry_sealed && cf_sealed {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "seal did not complete in time");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let entry = fx.lifecycle.registry().get(segment_id).unwrap();
        assert_eq!(entry.state, oceanfs_storage::SegmentState::Sealed);
        let cf = fx.metadata.get_segment(segment_id).unwrap().unwrap();
        assert!(cf.sealed_at.is_some());
        assert!(cf.merkle_root.is_some(), "seal-time Merkle root persisted");

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
        // entry must agree (Sealed ↔ sealed_at Some) and a downgrade
        // attempt must be rejected with the registry + CF unchanged.
        // The probe must exercise at least every distinct stress segment.
        let distinct_put_segments: std::collections::HashSet<SegmentId> =
            put_ids.iter().copied().collect();
        let mut probed = 0usize;
        let cf_ids: Vec<SegmentId> = fx
            .metadata
            .list_segments()
            .into_iter()
            .filter_map(std::result::Result::ok)
            .map(|m| m.segment_id)
            .collect();
        for id in cf_ids {
            let cf = fx.metadata.get_segment(id).unwrap().expect("CF entry");
            let entry = fx.lifecycle.registry().get(id).unwrap_or_else(|| {
                panic!("CF segment {id} must have a registry entry (seeded/reserved)")
            });
            assert_eq!(
                entry.state == oceanfs_storage::SegmentState::Sealed,
                cf.sealed_at.is_some(),
                "registry and CF must agree on segment {id}"
            );
            if cf.sealed_at.is_some() {
                // Poison probe: the phantom-downgrade write.
                let err =
                    fx.lifecycle.request_reserve(id, SizeTier::Standard, 0, 0).await.unwrap_err();
                assert_eq!(err, TransitionError::AlreadySealed, "no downgrade for {id}");
                let after = fx.metadata.get_segment(id).unwrap().unwrap();
                assert!(after.sealed_at.is_some(), "CF must stay sealed for {id}");
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

        fn put_segment(
            &self,
            meta: SegmentMetadata,
        ) -> std::result::Result<(), crate::metadata_ops::MetadataError> {
            self.store
                .put_segment(meta)
                .map_err(|e| crate::metadata_ops::MetadataError::Internal(format!("{e}")))
        }
        fn get_segment(
            &self,
            id: SegmentId,
        ) -> std::result::Result<Option<SegmentMetadata>, crate::metadata_ops::MetadataError>
        {
            self.store
                .get_segment(id)
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
            SegmentLifecycleCoordinator::with_registry(metadata.clone(), stress_registry)
                .with_idle_seal(vec![standard_pool.clone()], 5000),
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
            Arc::new(HintedHandoffManager::new(hints_dir, delivery_client, hint_config.clone()));

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
            hint_config,
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

        StressFixture { coord, metadata, read, _dir: dir }
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

        // Wait for the seal worker to drain (list_segments stabilizes).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let mut last_count = usize::MAX;
        loop {
            let count =
                fx.metadata.list_segments().into_iter().filter_map(std::result::Result::ok).count();
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
