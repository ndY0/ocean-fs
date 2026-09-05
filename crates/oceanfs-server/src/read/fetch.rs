//! Parallel shard fetch for blob reads.
//!
//! Fetches segment shards from k+m nodes in parallel using
//! `FuturesUnordered`. The fastest k responses are used to reconstruct
//! the blob data.
//!
//! When gRPC is not available (single-node mode), falls back to
//! reading from the local [`SegmentReader`].
//!
//! Per performance guideline §8.1 (FuturesUnordered) and §8.2
//! (timeout branches via `tokio::time::timeout`).
//!
//! EC recovery (when enabled via the `ec` feature) is performed
//! by `try_ec_recovery_for_chunk()`, which splits the segment into
//! data+parity shards and reconstructs missing data via
//! `EcRecoveryParams::decode_shards()`.

// [review][structure][high]
// this modules is called fetch, and yet half of it is about segment recovery with EC decoding.
// a split should be considered zfor better clarity
// [end]

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::{stream::FuturesUnordered, StreamExt};
use oceanfs_core::{
    proto::segment::FetchShardRequest as GprcFetchShardRequest, ChunkRef, NodeId, ObjectMetadata,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::{segment_replica_set, shard_batch, RingCache};
use oceanfs_storage::SegmentRpcClient;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::{
    error::{Error, Result},
    read::coordinator::SegmentReader,
    routing_hint::RoutingHint,
};

/// Parameters for EC recovery during chunk fetch.
///
/// When set, the fetch path will attempt EC-based reconstruction
/// if the normal chunk fetch fails. Only available when the `ec`
/// feature is enabled.
#[cfg(feature = "ec")]
pub(crate) struct EcRecoveryParams {
    /// The EC decoder for reconstructing missing data shards.
    pub decoder: Arc<dyn oceanfs_ec::Decoder>,
    /// Number of data shards (k).
    pub data_shards: u8,
    /// Number of parity shards (m).
    pub parity_shards: u8,
}

#[cfg(feature = "ec")]
impl EcRecoveryParams {
    /// Decodes available shards to recover missing data shards.
    ///
    /// This is the fetch-module counterpart to
    /// [`ReadCoordinator::decode_ec_shards`] — it uses the same
    /// underlying decoder but is accessible from the fetch pipeline
    /// without a coordinator reference.
    ///
    /// `available_shards` must have length `data_shards + parity_shards`.
    /// `None` entries indicate missing shards. At least `data_shards`
    /// entries must be `Some`.
    pub(crate) fn decode_shards(&self, available_shards: &[Option<&[u8]>]) -> Result<Vec<Bytes>> {
        self.decoder
            .decode(available_shards, self.data_shards, self.parity_shards)
            .map_err(|e| Error::Internal(format!("EC decode failed: {e}")))
    }
}

/// Fetches blob data from segments identified by chunk references.
///
/// Each chunk is fetched in parallel using `FuturesUnordered`. When a
/// `segment_reader` is provided, local reads are used as a fast path.
/// When the local reader is absent or fails, the function falls back
/// to gRPC `FetchShard` calls to remote replicas (if `pool` and
/// `membership` are provided).
///
/// If `ec_params` is provided and a chunk fetch fails, the function
/// will attempt EC-based reconstruction using parity shards.
///
/// # Errors
///
/// Returns an error if no replica can serve a chunk, or if the
/// operation exceeds the timeout.
/// Read-path decompression context (accel feature): the compressor
/// (accel dispatcher) plus the semaphore bounding concurrent
/// decompression on the blocking pool. The non-accel build defines an
/// empty alias so signatures stay uniform.
#[cfg(feature = "accel")]
pub(crate) type DecompressCtx<'a> =
    (&'a Arc<dyn oceanfs_accel::Compressor>, &'a Arc<tokio::sync::Semaphore>);
#[cfg(not(feature = "accel"))]
pub(crate) type DecompressCtx<'a> = ();

/// Decompresses a stored chunk when its metadata says it is compressed.
/// Runs on the blocking pool (mirrors the seal path's EC encode),
/// bounded by the same semaphore pattern; `expected_len` is the exact
/// logical size, so the backend allocates exactly once.
pub(crate) async fn maybe_decompress(
    ctx: Option<DecompressCtx<'_>>,
    chunk: &ChunkRef,
    data: Bytes,
) -> Result<Bytes> {
    #[cfg(feature = "accel")]
    {
        if !chunk.compressed {
            return Ok(data);
        }
        let (compressor, semaphore) = ctx.ok_or_else(|| {
            Error::Storage("compressed chunk read without a decompression backend".into())
        })?;
        let _permit = semaphore.acquire().await;
        let compressor = Arc::clone(compressor);
        let data = data.clone();
        let expected = chunk.logical_length as usize;
        tokio::task::spawn_blocking(move || compressor.decompress_exact(&data, expected))
            .await
            .map_err(|e| Error::Storage(format!("decompression task failed: {e}")))?
            .map_err(|e| Error::Storage(format!("decompress failed: {e}")))
    }
    #[cfg(not(feature = "accel"))]
    {
        let _ = (ctx, chunk);
        Ok(data)
    }
}

pub(crate) async fn fetch_chunks(
    ring: &Arc<RingCache>,
    metadata: &ObjectMetadata,
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    decompress_ctx: Option<DecompressCtx<'_>>,
) -> Result<Vec<Bytes>> {
    fetch_chunks_inner(
        ring,
        metadata,
        timeout_ms,
        segment_reader,
        None,
        None,
        None,
        None,
        true,
        true,
        None,
        decompress_ctx,
    )
    .await
}

/// Internal version that accepts optional gRPC dependencies.
/// - `use_fastest_k`: when `true`, `FuturesUnordered` may return early
///   once k data shards arrive (enabled when EC parity is available).
///   Currently a passthrough — full k-of-m termination is implemented
///   in the EC integration epic.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_chunks_with_grpc(
    ring: &Arc<RingCache>,
    metadata: &ObjectMetadata,
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
    routing_hint: Option<&Arc<dyn RoutingHint>>,
    parallel_fetch: bool,
    use_fastest_k: bool,
    stripe_semaphore: Option<&Arc<Semaphore>>,
    decompress_ctx: Option<DecompressCtx<'_>>,
) -> Result<Vec<Bytes>> {
    fetch_chunks_inner(
        ring,
        metadata,
        timeout_ms,
        segment_reader,
        pool,
        membership,
        routing_hint,
        None,
        parallel_fetch,
        use_fastest_k,
        stripe_semaphore,
        decompress_ctx,
    )
    .await
}

/// Fetches chunk data with optional EC recovery support.
///
/// When `ec_params` is provided and the EC feature is enabled,
/// the fetch path will attempt EC-based shard reconstruction
/// as a fallback if normal chunk fetch fails.
#[cfg(feature = "ec")]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_chunks_with_ec(
    ring: &Arc<RingCache>,
    metadata: &ObjectMetadata,
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
    routing_hint: Option<&Arc<dyn RoutingHint>>,
    ec_params: &EcRecoveryParams,
    parallel_fetch: bool,
    use_fastest_k: bool,
    stripe_semaphore: Option<&Arc<Semaphore>>,
    decompress_ctx: Option<DecompressCtx<'_>>,
) -> Result<Vec<Bytes>> {
    fetch_chunks_inner(
        ring,
        metadata,
        timeout_ms,
        segment_reader,
        pool,
        membership,
        routing_hint,
        Some(ec_params),
        parallel_fetch,
        use_fastest_k,
        stripe_semaphore,
        decompress_ctx,
    )
    .await
}

/// Internal implementation that supports both local and gRPC fetch,
/// plus optional EC recovery, concurrency control, and fetch strategy.
///
/// - `parallel_fetch`: when `false`, chunks are fetched sequentially
///   (one at a time) instead of in parallel via `FuturesUnordered`.
/// - `use_fastest_k`: when `true` and EC params are available,
///   `FuturesUnordered` may exit early once k data shards arrive.
/// - `stripe_semaphore`: when set, EC recovery tasks acquire a permit
///   before decoding, bounding concurrent EC work per perf §2.7/8.5.
#[allow(clippy::too_many_arguments)]
async fn fetch_chunks_inner(
    ring: &Arc<RingCache>,
    metadata: &ObjectMetadata,
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
    routing_hint: Option<&Arc<dyn RoutingHint>>,
    #[cfg_attr(not(feature = "ec"), allow(unused_variables))] ec_params: Option<&EcRecoveryParams>,
    parallel_fetch: bool,
    #[cfg_attr(not(feature = "ec"), allow(unused_variables))] use_fastest_k: bool,
    stripe_semaphore: Option<&Arc<Semaphore>>,
    decompress_ctx: Option<DecompressCtx<'_>>,
) -> Result<Vec<Bytes>> {
    if metadata.is_inline() {
        if let Some(ref data) = metadata.inline_data {
            return Ok(vec![data.clone()]);
        }
        return Err(Error::NotFound("inline metadata has no data".into()));
    }

    if metadata.chunks.is_empty() {
        return Ok(vec![]);
    }

    if parallel_fetch {
        fetch_all_chunks_parallel(
            ring,
            &metadata.chunks,
            timeout_ms,
            segment_reader,
            pool,
            membership,
            routing_hint,
            ec_params,
            use_fastest_k,
            stripe_semaphore,
            decompress_ctx,
        )
        .await
    } else {
        fetch_all_chunks_serial(
            ring,
            &metadata.chunks,
            timeout_ms,
            segment_reader,
            pool,
            membership,
            routing_hint,
            ec_params,
            use_fastest_k,
            stripe_semaphore,
            decompress_ctx,
        )
        .await
    }
}

/// Fetches all chunk data in parallel using `FuturesUnordered`.
///
/// Each chunk is fetched independently with its own timeout. Results are
/// collected as they complete and ordered by chunk index.
///
/// When `ec_params` is provided, each chunk fetch will attempt EC recovery
/// as a fallback if the normal fetch path fails.
///
/// When `use_fastest_k` is `true` and EC params are available, the loop
/// exits early once at least `k` data shards have been fetched successfully.
#[allow(clippy::too_many_arguments)]
async fn fetch_all_chunks_parallel(
    ring: &Arc<RingCache>,
    chunks: &[ChunkRef],
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
    routing_hint: Option<&Arc<dyn RoutingHint>>,
    ec_params: Option<&EcRecoveryParams>,
    use_fastest_k: bool,
    stripe_semaphore: Option<&Arc<Semaphore>>,
    decompress_ctx: Option<DecompressCtx<'_>>,
) -> Result<Vec<Bytes>> {
    let chunk_count = chunks.len();

    // Spawn a fetch future per chunk in FuturesUnordered.
    let ec_params_arc = ec_params.map(|p| {
        Arc::new(EcRecoveryParams {
            decoder: Arc::clone(&p.decoder),
            data_shards: p.data_shards,
            parity_shards: p.parity_shards,
        })
    });
    let sem = stripe_semaphore.cloned();

    // When use_fastest_k is enabled with EC, we only need k data shards
    // (the remaining m parity shards can be used for reconstruction).
    // Without EC, we need all chunks and fastest-k is a no-op.
    let required = if use_fastest_k {
        ec_params.map(|p| p.data_shards as usize).unwrap_or(chunk_count)
    } else {
        chunk_count
    };

    let mut futs: FuturesUnordered<_> = chunks
        .iter()
        .enumerate()
        .map(|(idx, chunk)| {
            let ring = Arc::clone(ring);
            let chunk = *chunk;
            let segment_reader = segment_reader.cloned();
            let pool = pool.cloned();
            let membership = membership.cloned();
            let routing_hint = routing_hint.cloned();
            let ec = ec_params_arc.clone();
            let sem = sem.clone();
            async move {
                let result = fetch_single_chunk(
                    decompress_ctx,
                    &ring,
                    &chunk,
                    timeout_ms,
                    segment_reader.as_ref(),
                    pool.as_ref(),
                    membership.as_ref(),
                    routing_hint.as_ref(),
                    ec.as_ref(),
                    sem.as_ref(),
                )
                .await;
                (idx, result)
            }
        })
        .collect();

    // Collect results, preserving chunk order.
    let mut chunk_data: Vec<Option<Bytes>> = vec![None; chunk_count];
    let mut errors = Vec::with_capacity(4);
    let mut successes: usize = 0;

    while let Some((idx, result)) = futs.next().await {
        match result {
            Ok(data) => {
                chunk_data[idx] = Some(data);
                successes += 1;
                // k-of-m early termination: stop once we have enough
                // successful fetches. Remaining futures are dropped.
                if successes >= required && required < chunk_count {
                    tracing::debug!(
                        successes,
                        required,
                        chunk_count,
                        "use_fastest_k: early termination after k shards"
                    );
                    break;
                }
            }
            Err(e) => {
                errors.push((idx, e));
            }
        }
    }

    // If any chunk failed and we have no fallback, return the first error.
    if chunk_data.iter().any(|d| d.is_none()) {
        if let Some((_idx, e)) = errors.into_iter().next() {
            return Err(e);
        }
        return Err(Error::Internal("all chunk fetches failed".into()));
    }

    // Safety: we checked above that no entry is None.
    #[allow(clippy::unwrap_used)]
    Ok(chunk_data.into_iter().map(|d| d.unwrap()).collect())
}

/// Fetches all chunks sequentially (one at a time).
///
/// Used when `ReadTuningConfig::parallel_fetch = false`. Each chunk is
/// fetched, then the next one starts. The semaphore (if set) bounds EC
/// recovery concurrency.
#[allow(clippy::too_many_arguments)]
async fn fetch_all_chunks_serial(
    ring: &Arc<RingCache>,
    chunks: &[ChunkRef],
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
    routing_hint: Option<&Arc<dyn RoutingHint>>,
    ec_params: Option<&EcRecoveryParams>,
    #[allow(unused_variables)] use_fastest_k: bool,
    stripe_semaphore: Option<&Arc<Semaphore>>,
    decompress_ctx: Option<DecompressCtx<'_>>,
) -> Result<Vec<Bytes>> {
    let ec_params_arc = ec_params.map(|p| {
        Arc::new(EcRecoveryParams {
            decoder: Arc::clone(&p.decoder),
            data_shards: p.data_shards,
            parity_shards: p.parity_shards,
        })
    });
    let sem = stripe_semaphore.cloned();

    let mut results = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let result = fetch_single_chunk(
            decompress_ctx,
            ring,
            chunk,
            timeout_ms,
            segment_reader,
            pool,
            membership,
            routing_hint,
            ec_params_arc.as_ref(),
            sem.as_ref(),
        )
        .await?;
        results.push(result);
    }
    Ok(results)
}

/// Fetches a single chunk from the local segment reader or via gRPC fallback.
///
/// When `ec_params` is set and the EC feature is enabled, attempts
/// EC-based shard reconstruction if the normal fetch path fails.
#[allow(clippy::too_many_arguments)]
async fn fetch_single_chunk(
    decompress_ctx: Option<DecompressCtx<'_>>,
    ring: &Arc<RingCache>,
    chunk: &ChunkRef,
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
    routing_hint: Option<&Arc<dyn RoutingHint>>,
    #[cfg_attr(not(feature = "ec"), allow(unused_variables))] ec_params: Option<
        &Arc<EcRecoveryParams>,
    >,
    stripe_semaphore: Option<&Arc<Semaphore>>,
) -> Result<Bytes> {
    let data = fetch_single_chunk_raw(
        ring,
        chunk,
        timeout_ms,
        segment_reader,
        pool,
        membership,
        routing_hint,
        ec_params,
        stripe_semaphore,
    )
    .await?;
    maybe_decompress(decompress_ctx, chunk, data).await
}

/// Raw stored-byte fetch: local segment reader, gRPC replicas, or EC
/// recovery — no decompression. Callers use [`fetch_single_chunk`] so
/// compressed chunks are transparently expanded.
#[allow(clippy::too_many_arguments)]
async fn fetch_single_chunk_raw(
    ring: &Arc<RingCache>,
    chunk: &ChunkRef,
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
    routing_hint: Option<&Arc<dyn RoutingHint>>,
    #[cfg_attr(not(feature = "ec"), allow(unused_variables))] ec_params: Option<
        &Arc<EcRecoveryParams>,
    >,
    stripe_semaphore: Option<&Arc<Semaphore>>,
) -> Result<Bytes> {
    // Fast path: local segment reader.
    if let Some(reader) = segment_reader {
        match reader.read_chunk(&chunk.segment_id, chunk.offset, chunk.length).await {
            Ok(data) => {
                debug!(
                    segment_id = %chunk.segment_id,
                    offset = chunk.offset,
                    length = chunk.length,
                    "chunk fetched from local segment reader"
                );
                return Ok(data);
            }
            Err(e) => {
                debug!(
                    segment_id = %chunk.segment_id,
                    error = %e,
                    "local segment read failed, trying replicas"
                );
            }
        }
    }

    // Determine replica set for this chunk's segment.
    // The shared segment-replica derivation: the seal-time replicator
    // pushes to exactly this set, so a local miss falls through to nodes
    // that actually hold the data (sealed-segment-replication).
    let replica_set = segment_replica_set(ring, &chunk.segment_id);

    // ADR-0029 §D5: exclude candidates whose manifest reports zero
    // Healthy data pools (the node cannot serve segment reads). The
    // manifest is a HINT — an unknown node stays eligible and the
    // error-driven fallback below is the guarantee.
    let replica_set: Vec<NodeId> = if let Some(hint) = routing_hint {
        replica_set.iter().filter(|n| !hint.exclude_read_candidate(n)).cloned().collect()
    } else {
        replica_set
    };

    if replica_set.is_empty() {
        // If no replicas but EC recovery is available, try it before giving up.
        #[cfg(feature = "ec")]
        {
            if let Some(params) = ec_params {
                if let Some(reader) = segment_reader {
                    if let Ok(data) = try_ec_recovery_for_chunk(
                        reader,
                        chunk,
                        params,
                        ring,
                        pool,
                        membership,
                        routing_hint,
                        timeout_ms,
                        stripe_semaphore,
                    )
                    .await
                    {
                        return Ok(data);
                    }
                }
            }
        }
        return Err(Error::Routing(format!(
            "no replicas for segment {} and no local reader",
            chunk.segment_id
        )));
    }

    // gRPC fallback: try to fetch from remote replicas via FetchShard.
    // Item 9: group shard requests by target node for batched per-node RPCs.
    if let (Some(pool), Some(membership)) = (pool, membership) {
        use oceanfs_core::proto::segment::ShardRange;

        // Build one ShardRequest per replica. All entries are value-identical
        // (same chunk) but occupy distinct positions in the vec for ptr::eq
        // identity comparison when grouping by target node.
        let shard_requests: Vec<shard_batch::ShardRequest> = replica_set
            .iter()
            .map(|_| shard_batch::ShardRequest {
                segment_id: chunk.segment_id,
                shard_index: 0,
                offset: chunk.offset,
                length: chunk.length as u64,
            })
            .collect();
        // Side-car mapping from shard-request index to the owning NodeId.
        let replica_index: Vec<_> = replica_set.to_vec();

        let node_groups = shard_batch::group_by_node(&shard_requests, |req| {
            // Use ptr::eq for identity comparison — all ShardRequest values are
            // identical for a single chunk, so PartialEq-based position() would
            // always return index 0 (Review Gap Item 9).
            let idx = shard_requests.iter().position(|r| std::ptr::eq(r, req))?;
            replica_index.get(idx).cloned()
        });

        for (replica, node_shards) in node_groups {
            let addr = match membership.address_of(&replica) {
                Some(a) => a,
                None => continue,
            };

            let pooled = match pool.get_channel(addr).await {
                Ok(p) => p,
                Err(e) => {
                    debug!(replica = %replica, error = %e, "failed to acquire channel for fetch");
                    if let Some(hint) = routing_hint {
                        hint.on_failover();
                    }
                    continue;
                }
            };

            let channel = pooled.channel().clone();
            drop(pooled);

            let proto_sid = chunk.segment_id.into();
            let mut client = SegmentRpcClient::new(channel);

            // Build batched shard ranges from the node's group.
            let shard_ranges: Vec<ShardRange> = node_shards
                .iter()
                .map(|s| ShardRange {
                    shard_index: s.shard_index,
                    offset: s.offset,
                    length: s.length,
                })
                .collect();

            let request = tonic::Request::new(GprcFetchShardRequest {
                segment_id: Some(proto_sid),
                shard_index: 0,
                offset: 0,
                length: 0,
                shards: shard_ranges,
            });

            let fetch_result = tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                client.fetch_shard(request),
            )
            .await;

            match fetch_result {
                Ok(Ok(response)) => {
                    let mut stream = response.into_inner();
                    let mut data = BytesMut::with_capacity(chunk.length as usize);
                    while let Some(chunk_result) = stream.message().await.unwrap_or(None) {
                        if chunk_result.data.is_empty() {
                            break; // EOF sentinel
                        }
                        data.extend_from_slice(&chunk_result.data);
                    }
                    if !data.is_empty() {
                        debug!(
                            segment_id = %chunk.segment_id,
                            replica = %replica,
                            bytes = data.len(),
                            "chunk fetched via gRPC from replica"
                        );
                        return Ok(data.freeze());
                    }
                    // Empty response: the replica served nothing — an
                    // error-driven fall-through to the next replica.
                    if let Some(hint) = routing_hint {
                        hint.on_failover();
                    }
                }
                Ok(Err(status)) => {
                    debug!(
                        replica = %replica,
                        error = %status,
                        "gRPC fetch failed for replica"
                    );
                    if let Some(hint) = routing_hint {
                        hint.on_failover();
                    }
                }
                Err(_elapsed) => {
                    debug!(
                        replica = %replica,
                        timeout_ms,
                        "gRPC fetch timed out for replica"
                    );
                    if let Some(hint) = routing_hint {
                        hint.on_failover();
                    }
                }
            }
        }
    }

    // EC recovery fallback: attempt shard-level reconstruction.
    #[cfg(feature = "ec")]
    {
        if let Some(params) = ec_params {
            if let Some(reader) = segment_reader {
                match try_ec_recovery_for_chunk(
                    reader,
                    chunk,
                    params,
                    ring,
                    pool,
                    membership,
                    routing_hint,
                    timeout_ms,
                    stripe_semaphore,
                )
                .await
                {
                    Ok(data) => {
                        debug!(
                            segment_id = %chunk.segment_id,
                            bytes = data.len(),
                            "chunk recovered via EC decode"
                        );
                        return Ok(data);
                    }
                    Err(e) => {
                        warn!(
                            segment_id = %chunk.segment_id,
                            error = %e,
                            "EC recovery attempt failed"
                        );
                    }
                }
            }
        }
    }

    // Neither local reader nor gRPC succeeded. This is the g4 Job B
    // trigger: the segment exists on no live holder — the object's
    // metadata references a compacted-away segment the remap missed
    // (GAP-1 failsafe). The read coordinator catches this variant and
    // attempts a one-shot dangling-metadata repair before surfacing it.
    Err(Error::SegmentUnavailable(chunk.segment_id))
}

/// Fetches a single parity shard from a remote replica via gRPC
/// `FetchShard`.
///
/// Uses the ring to find replicas for the segment and tries each
/// replica in order. The `shard_index` parameter must be in the
/// parity range (k..k+m-1) to request a parity shard.
///
/// Each gRPC call is wrapped in a timeout of `timeout_ms`
/// milliseconds to satisfy perf §8.2.
///
/// Only compiled when the `ec` feature is enabled.
#[cfg(feature = "ec")]
#[allow(clippy::too_many_arguments)]
async fn fetch_parity_shard_via_grpc(
    ring: &Arc<RingCache>,
    chunk: &ChunkRef,
    shard_index: u32,
    shard_size: u64,
    pool: &Arc<ConnectionPool>,
    membership: &Arc<Membership>,
    routing_hint: Option<&Arc<dyn RoutingHint>>,
    timeout_ms: u64,
) -> Result<Bytes> {
    // The shared segment-replica derivation (the seal-time replicator's
    // target set — see fetch_single_chunk_raw).
    let replica_set = segment_replica_set(ring, &chunk.segment_id);

    // ADR-0029 §D5: same read-candidate exclusion as the data-shard
    // path — a node with zero Healthy data pools cannot serve reads.
    let replica_set: Vec<NodeId> = if let Some(hint) = routing_hint {
        replica_set.iter().filter(|n| !hint.exclude_read_candidate(n)).cloned().collect()
    } else {
        replica_set
    };

    for replica in &replica_set {
        let addr = match membership.address_of(replica) {
            Some(a) => a,
            None => continue,
        };
        let pooled = match pool.get_channel(addr).await {
            Ok(p) => p,
            Err(_) => {
                if let Some(hint) = routing_hint {
                    hint.on_failover();
                }
                continue;
            }
        };
        let channel = pooled.channel().clone();
        drop(pooled);

        let proto_sid = chunk.segment_id.into();
        let mut client = SegmentRpcClient::new(channel);
        let request = tonic::Request::new(GprcFetchShardRequest {
            segment_id: Some(proto_sid),
            shard_index,
            offset: 0,
            length: shard_size,
            shards: vec![],
        });

        let fetch_result = tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            client.fetch_shard(request),
        )
        .await;

        match fetch_result {
            Ok(Ok(response)) => {
                let mut stream = response.into_inner();
                let mut data = BytesMut::with_capacity(shard_size as usize);
                while let Some(chunk_result) = stream.message().await.unwrap_or(None) {
                    if chunk_result.data.is_empty() {
                        break;
                    }
                    data.extend_from_slice(&chunk_result.data);
                }
                if !data.is_empty() {
                    debug!(
                        segment_id = %chunk.segment_id,
                        shard_index,
                        bytes = data.len(),
                        "parity shard fetched via gRPC"
                    );
                    return Ok(data.freeze());
                }
            }
            Ok(Err(status)) => {
                debug!(
                    replica = %replica,
                    shard_index,
                    error = %status,
                    "gRPC parity shard fetch failed"
                );
                if let Some(hint) = routing_hint {
                    hint.on_failover();
                }
            }
            Err(_elapsed) => {
                debug!(
                    replica = %replica,
                    shard_index,
                    timeout_ms,
                    "gRPC parity shard fetch timed out"
                );
                if let Some(hint) = routing_hint {
                    hint.on_failover();
                }
            }
        }
    }

    Err(Error::Internal(format!(
        "cannot fetch parity shard {shard_index} for segment {} — no replica responded",
        chunk.segment_id
    )))
}

/// Attempts EC-based reconstruction for a chunk when the normal fetch
/// path fails.
///
/// First tries to read the full segment from the local segment reader.
/// When `pool` and `membership` are provided, additionally attempts to
/// fetch individual parity shards from remote replicas via per-shard
/// gRPC `FetchShard` calls (using `shard_index` values k..k+m-1).
///
/// Splits the segment into `k` data shards and `m` parity shards, and
/// uses EC decoding to reconstruct any data shard that appears to be
/// missing or corrupted.
///
/// Only compiled when the `ec` feature is enabled.
#[cfg(feature = "ec")]
#[allow(clippy::too_many_arguments)]
async fn try_ec_recovery_for_chunk(
    reader: &Arc<dyn SegmentReader>,
    chunk: &ChunkRef,
    params: &Arc<EcRecoveryParams>,
    ring: &Arc<RingCache>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
    routing_hint: Option<&Arc<dyn RoutingHint>>,
    timeout_ms: u64,
    stripe_semaphore: Option<&Arc<Semaphore>>,
) -> Result<Bytes> {
    let k = params.data_shards as usize;
    let m = params.parity_shards as usize;
    let total_shards = k + m;

    if total_shards == 0 {
        return Err(Error::Internal("EC codec shard count is zero".into()));
    }

    // Read the full segment (offset 0, length = max). The
    // SegmentReader implementation is expected to return the full
    // segment data.
    let segment_data = reader.read_chunk(&chunk.segment_id, 0, u32::MAX).await.map_err(|e| {
        Error::Internal(format!(
            "EC recovery: failed to read full segment {}: {e}",
            chunk.segment_id
        ))
    })?;

    if segment_data.len() < total_shards {
        return Err(Error::Internal(format!(
            "EC recovery: segment {} too small ({} bytes, need {} shards)",
            chunk.segment_id,
            segment_data.len(),
            total_shards
        )));
    }

    let shard_size = segment_data.len() / total_shards;
    if shard_size == 0 {
        return Err(Error::Internal("EC recovery: computed shard size is zero".into()));
    }

    // Determine which data shards the chunk spans.
    let chunk_start = chunk.offset as usize;
    let chunk_len = chunk.length as usize;
    let chunk_end = chunk_start.saturating_add(chunk_len);
    let first_shard = chunk_start / shard_size;
    let last_shard = (chunk_end.saturating_sub(1)) / shard_size;

    if first_shard >= k || last_shard >= k {
        return Err(Error::Internal(format!(
            "EC recovery: chunk spans parity shards (shards {first_shard}..{last_shard})"
        )));
    }

    // Check if the first shard of the chunk is corrupted (all zeros).
    let first_shard_start = first_shard * shard_size;
    let first_shard_slice = &segment_data[first_shard_start..first_shard_start + shard_size];
    let is_shard_corrupted = first_shard_slice.iter().all(|&b| b == 0);

    // If the target shard is intact, read directly from the segment.
    if !is_shard_corrupted {
        let mut result = BytesMut::with_capacity(chunk_len);
        for shard_idx in first_shard..=last_shard {
            let s_start = shard_idx * shard_size;
            let slice_start = if shard_idx == first_shard { chunk_start - s_start } else { 0 };
            let slice_end = if shard_idx == last_shard {
                (chunk_end - s_start).min(shard_size)
            } else {
                shard_size
            };
            result.extend_from_slice(&segment_data[s_start + slice_start..s_start + slice_end]);
        }
        return Ok(result.freeze());
    }

    // Data shard appears corrupted — use EC decode to reconstruct.
    let mut available: Vec<Option<&[u8]>> = Vec::with_capacity(total_shards);

    // Data shards (0..k): mark the corrupted shards as missing.
    // We only know that the first shard we checked is corrupted; if the
    // chunk spans multiple shards, mark only those as missing.
    for i in 0..k {
        let start = i * shard_size;
        let end = start + shard_size;
        if i >= first_shard && i <= last_shard && segment_data[start..end].iter().all(|&b| b == 0) {
            available.push(None);
        } else {
            available.push(Some(&segment_data[start..end]));
        }
    }

    // Parity shards (k..k+m): try gRPC fetch first, fall back to local.
    // We hold fetched parity data in an owned Vec so it outlives the
    // borrows passed to decode_shards.
    let mut fetched_parity: Vec<Bytes> = Vec::with_capacity(m);
    if let (Some(pool), Some(membership)) = (pool, membership) {
        for i in 0..m {
            let shard_idx = (k + i) as u32;
            let parity_data = fetch_parity_shard_via_grpc(
                ring,
                chunk,
                shard_idx,
                shard_size as u64,
                pool,
                membership,
                routing_hint,
                timeout_ms,
            )
            .await?;
            fetched_parity.push(parity_data);
        }
        for p in &fetched_parity {
            available.push(Some(p.as_ref()));
        }
    } else {
        for i in 0..m {
            let start = (k + i) * shard_size;
            let end = start + shard_size;
            available.push(Some(&segment_data[start..end]));
        }
    }

    // Acquire the stripe-parallelism semaphore before the CPU-intensive
    // EC decode, bounding concurrent decode tasks (perf §2.7, §8.5).
    let _decode_permit = if let Some(sem) = stripe_semaphore {
        Some(Arc::clone(sem).acquire_owned().await)
    } else {
        None
    };

    let recovered = params.decode_shards(&available)?;

    // Extract the chunk from the recovered data shards (may span multiple).
    let mut result = BytesMut::with_capacity(chunk_len);
    for shard_idx in first_shard..=last_shard {
        let rec = recovered.get(shard_idx).ok_or_else(|| {
            Error::Internal("EC decode: recovered shard index out of bounds".into())
        })?;
        let slice_start =
            if shard_idx == first_shard { chunk_start - (shard_idx * shard_size) } else { 0 };
        let slice_end = if shard_idx == last_shard {
            (chunk_end - (shard_idx * shard_size)).min(shard_size)
        } else {
            shard_size
        };
        result.extend_from_slice(rec.get(slice_start..slice_end).ok_or_else(|| {
            Error::Internal("EC recovery: chunk range out of recovered bounds".into())
        })?);
    }
    Ok(result.freeze())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{net::SocketAddr, sync::atomic::AtomicU64};

    use oceanfs_core::{GossipConfig, HlcClock, NodeId, RingConfig, SegmentId};
    use oceanfs_membership::Membership;
    use oceanfs_network::ConnectionPool;
    use oceanfs_routing::{Ring, RingCache};
    use oceanfs_storage::{BufferPool, SegmentRpcServer};
    use oceanfs_storage_api::SegmentDataStore;
    use parking_lot::Mutex;
    use tonic::transport::Server;

    use super::*;
    use crate::{
        grpc::segment_service::SegmentGrpcService, read::coordinator::InMemorySegmentReader,
        routing_hint::RoutingHint,
    };

    /// An in-memory segment store for the failover test's gRPC servers.
    struct TestSegmentStore {
        data: Mutex<std::collections::HashMap<SegmentId, Bytes>>,
    }

    impl TestSegmentStore {
        fn new() -> Self {
            Self { data: Mutex::new(std::collections::HashMap::new()) }
        }
    }

    #[async_trait::async_trait]
    impl SegmentDataStore for TestSegmentStore {
        async fn write_segment_data(
            &self,
            segment_id: &SegmentId,
            data: &[u8],
        ) -> Result<(), oceanfs_storage_api::error::Error> {
            self.data.lock().insert(*segment_id, Bytes::copy_from_slice(data));
            Ok(())
        }
        async fn read_segment_data(
            &self,
            segment_id: &SegmentId,
        ) -> Result<Option<oceanfs_storage_api::SegmentFile>, oceanfs_storage_api::error::Error>
        {
            Ok(self.data.lock().get(segment_id).cloned().map(|data| {
                oceanfs_storage_api::SegmentFile {
                    segment_id: *segment_id,
                    version: 1,
                    header_len: 76,
                    data_end: (76 + data.len()) as u64,
                    data,
                }
            }))
        }
        async fn delete_shards(
            &self,
            segment_id: &SegmentId,
        ) -> Result<u64, oceanfs_storage_api::error::Error> {
            Ok(self.data.lock().remove(segment_id).map(|removed| removed.len() as u64).unwrap_or(0))
        }
        async fn delete_shards_with_pool(
            &self,
            segment_id: &SegmentId,
            _pool_id: u32,
        ) -> Result<u64, oceanfs_storage_api::error::Error> {
            self.delete_shards(segment_id).await
        }
        fn list_segment_files(
            &self,
            _root: &std::path::Path,
        ) -> Result<Vec<std::path::PathBuf>, oceanfs_storage_api::error::Error> {
            Ok(Vec::new())
        }
    }

    /// Starts a segment gRPC server over `store` with a lifecycle
    /// coordinator attached (the production shape — B3: fetch resolves
    /// shard geometry from the registry); returns its address.
    async fn serve_segment(
        store: Arc<dyn SegmentDataStore>,
    ) -> (SocketAddr, Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let lifecycle =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let service = SegmentGrpcService::new(
            store,
            None,
            Arc::new(BufferPool::new(65536, 1024)),
            Arc::new(HlcClock::new()),
        )
        .with_lifecycle(Arc::clone(&lifecycle));
        tokio::spawn(async move {
            Server::builder()
                .add_service(SegmentRpcServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (addr, lifecycle)
    }

    /// A counting routing hint: never excludes, counts failovers.
    struct CountingHint(Arc<AtomicU64>);

    impl RoutingHint for CountingHint {
        fn exclude_read_candidate(&self, _: &NodeId) -> bool {
            false
        }
        fn exclude_write_target(&self, _: &NodeId) -> bool {
            false
        }
        fn on_failover(&self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// ADR-0029 §D5 failover test: an error on the first replica falls
    /// through to the next replica, and the failover counter records the
    /// event — the cache was a hint, the I/O error was the truth.
    #[tokio::test]
    async fn fetch_falls_through_on_replica_error_and_counts_failover() {
        // Two replicas. The first has NO data for the segment (its
        // FetchShard errors), the second serves it.
        let test_data = Bytes::from_static(b"failover test data");
        let failing_store = Arc::new(TestSegmentStore::new()) as Arc<dyn SegmentDataStore>;
        let serving_store = Arc::new(TestSegmentStore::new()) as Arc<dyn SegmentDataStore>;

        // Ring with two replicas (RF 2 so both are in the lookup set).
        let mut ring = Ring::new(RingConfig { replication_factor: 2, ..RingConfig::default() });
        ring.add_node(NodeId::new("n1"));
        ring.add_node(NodeId::new("n2"));

        // The replica ORDER depends on the segment id's hash — pick a
        // segment id whose lookup puts the FAILING replica (n1) first, so
        // the fall-through is exercised deterministically.
        let seg_id = loop {
            let candidate = SegmentId::new();
            let hash = blake3::hash(candidate.to_string().as_bytes());
            if ring.lookup(hash.as_bytes()).first() == Some(&NodeId::new("n1")) {
                break candidate;
            }
        };
        serving_store.write_segment_data(&seg_id, &test_data).await.unwrap();

        let ring_cache = Arc::new(RingCache::new(ring));

        // The FAILING replica holds no lifecycle entry for the segment
        // (its FetchShard errors NotFound); the SERVING replica has the
        // data AND a registered entry so fetch resolves its geometry.
        let (failing_addr, _failing_lifecycle) = serve_segment(failing_store).await;
        let (serving_addr, serving_lifecycle) = serve_segment(serving_store).await;

        // Register the segment on the serving replica (in-memory pure
        // transitions — geometry k=4/m=2, the fetch path reads it to
        // compute shard boundaries).
        let root = oceanfs_durability::MerkleTree::build(&test_data, 0).unwrap().root().hash();
        let mut meta = oceanfs_core::SegmentMetadata {
            pool_id: 0,
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(0),
        };
        serving_lifecycle.registry().reserve(seg_id, meta.clone()).expect("reserve succeeds");
        meta.merkle_root = Some(root);
        serving_lifecycle.registry().seal(seg_id, meta).expect("seal succeeds");

        // Membership resolving both replicas to the test servers.
        let membership = Arc::new(Membership::new(
            NodeId::new("reader"),
            "127.0.0.1:9300".parse().unwrap(),
            "127.0.0.1:9300".parse().unwrap(),
            GossipConfig::default(),
            ring_cache.clone(),
        ));
        membership.upsert_node(
            NodeId::new("n1"),
            oceanfs_core::NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            Some(failing_addr),
        );
        membership.upsert_node(
            NodeId::new("n2"),
            oceanfs_core::NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            Some(serving_addr),
        );

        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
        let hint = Arc::new(CountingHint(Arc::new(AtomicU64::new(0))));
        let hint_dyn: Arc<dyn RoutingHint> = hint.clone();

        let chunk = ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: test_data.len() as u32,
            compressed: false,
            logical_length: test_data.len() as u32,
        };

        let data = fetch_single_chunk_raw(
            &ring_cache,
            &chunk,
            5000,
            None,
            Some(&pool),
            Some(&membership),
            Some(&hint_dyn),
            None,
            None,
        )
        .await
        .expect("the second replica must serve the chunk");

        assert_eq!(&data[..], &test_data[..], "data must come from the healthy replica");
        assert_eq!(
            hint.0.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the failed first replica must count one failover"
        );
    }

    #[tokio::test]
    async fn fetch_inline_metadata_returns_inline_data() {
        let meta = ObjectMetadata {
            object_key: oceanfs_core::ObjectKey::new("test"),
            size: 5,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(Bytes::from_static(b"hello")),
            created_at: 0,
            hlc: oceanfs_core::Hlc::zero(),
        };

        let ring = make_ring();
        let result = fetch_chunks(&ring, &meta, 1000, None, None).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(&result[0][..], b"hello");
    }

    #[tokio::test]
    async fn fetch_empty_chunks_returns_empty() {
        let meta = ObjectMetadata {
            object_key: oceanfs_core::ObjectKey::new("empty"),
            size: 0,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: None,
            created_at: 0,
            hlc: oceanfs_core::Hlc::zero(),
        };

        let ring = make_ring();
        let result = fetch_chunks(&ring, &meta, 1000, None, None).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn fetch_chunks_with_segment_reader_returns_real_data() {
        let seg_id = SegmentId::new();
        let test_data = b"real segment data for fetch test";
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: test_data.len() as u32,
            compressed: false,
            logical_length: test_data.len() as u32,
        });

        let meta = ObjectMetadata {
            object_key: oceanfs_core::ObjectKey::new("fetch-test"),
            size: test_data.len() as u64,
            blake3_hash: None,
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: oceanfs_core::Hlc::zero(),
        };

        let reader = Arc::new(InMemorySegmentReader::new());
        reader.put(seg_id, Bytes::from_static(test_data));
        let reader: Arc<dyn SegmentReader> = reader;

        let ring = make_ring();
        let result = fetch_chunks(&ring, &meta, 1000, Some(&reader), None).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(&result[0][..], test_data);
    }

    #[tokio::test]
    async fn fetch_chunks_without_reader_returns_error() {
        let seg_id = SegmentId::new();
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: 100,
            compressed: false,
            logical_length: 100,
        });

        let meta = ObjectMetadata {
            object_key: oceanfs_core::ObjectKey::new("no-reader"),
            size: 100,
            blake3_hash: None,
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: oceanfs_core::Hlc::zero(),
        };

        let ring = make_ring();
        let result = fetch_chunks(&ring, &meta, 5000, None, None).await;
        assert!(result.is_err(), "should fail without segment reader");
    }

    fn make_ring() -> Arc<RingCache> {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("n1"));
        Arc::new(RingCache::new(ring))
    }

    // ── EC Decode Tests ───────────────────────────────────────────

    #[cfg(feature = "ec")]
    mod ec_tests {
        use oceanfs_core::CodecConfig;
        use oceanfs_ec::{CauchyEncoder, Decoder, Encoder};

        use super::*;
        use crate::read::{coordinator::InMemorySegmentReader, fetch::EcRecoveryParams};

        fn make_ec_params() -> Arc<EcRecoveryParams> {
            let decoder: Arc<dyn Decoder> = Arc::new(CauchyEncoder::new(CodecConfig {
                data_shards: 4,
                parity_shards: 2,
                ..Default::default()
            }));
            Arc::new(EcRecoveryParams { decoder, data_shards: 4, parity_shards: 2 })
        }

        fn make_ring_for_ec() -> Arc<RingCache> {
            let mut ring = Ring::new(RingConfig::default());
            ring.add_node(NodeId::new("n1"));
            Arc::new(RingCache::new(ring))
        }

        /// Convenience: build an EC(4,2)-encoded segment from 4 equal-size
        /// data shards, returning (data_shards, parity_shards, full_segment).
        fn encode_ec_segment(data: &[Vec<u8>; 4]) -> (Vec<Vec<u8>>, Vec<Bytes>, Vec<u8>) {
            let data_refs: [&[u8]; 4] = [&data[0], &data[1], &data[2], &data[3]];
            let shard_size = data[0].len();
            let encoder = CauchyEncoder::new(CodecConfig {
                data_shards: 4,
                parity_shards: 2,
                strip_size_bytes: shard_size,
                ..Default::default()
            });
            let parity = encoder.encode(&data_refs, 2).unwrap();
            let mut segment = Vec::with_capacity(6 * shard_size);
            for s in data {
                segment.extend_from_slice(s);
            }
            for p in &parity {
                segment.extend_from_slice(p);
            }
            (data.to_vec(), parity, segment)
        }

        /// `EcRecoveryParams::decode_shards` recovers missing shards.
        #[test]
        fn ec_params_decode_shards_recovers_missing_data() {
            let params = make_ec_params();
            let shard = vec![0xAAu8; 16];
            let available: Vec<Option<&[u8]>> = vec![
                None,         // shard 0 missing
                Some(&shard), // shard 1
                Some(&shard), // shard 2
                Some(&shard), // shard 3
                Some(&shard), // parity 0
                Some(&shard), // parity 1
            ];
            let recovered = params.decode_shards(&available).unwrap();
            assert_eq!(recovered.len(), 4);
            assert_eq!(recovered[0].len(), 16);
        }

        /// `EcRecoveryParams::decode_shards` errors with too few shards.
        #[test]
        fn ec_params_decode_shards_errors_on_too_few_shards() {
            let params = make_ec_params();
            let shard = vec![0xBBu8; 16];
            let available: Vec<Option<&[u8]>> = vec![
                None,
                None,
                None, // 3 missing → only 3 available
                Some(&shard),
                Some(&shard),
                Some(&shard),
            ];
            let result = params.decode_shards(&available);
            assert!(result.is_err());
        }

        /// `try_ec_recovery_for_chunk` recovers a corrupted data shard
        /// by reading the full segment, detecting the all-zeros shard,
        /// and reconstructing via EC decode.
        #[tokio::test]
        async fn try_ec_recovery_recovers_corrupted_shard() {
            let data: [Vec<u8>; 4] = [
                b"DATA_SHARD_0____".to_vec(),
                b"DATA_SHARD_1____".to_vec(),
                b"DATA_SHARD_2____".to_vec(),
                b"DATA_SHARD_3____".to_vec(),
            ];
            let (original_data, _parity, mut segment) = encode_ec_segment(&data);

            // Zero out shard 0 to simulate corruption.
            let shard_len = data[0].len();
            for b in &mut segment[0..shard_len] {
                *b = 0;
            }

            let seg_id = SegmentId::new();
            let reader = Arc::new(InMemorySegmentReader::new());
            reader.put(seg_id, Bytes::from(segment));
            let reader: Arc<dyn SegmentReader> = reader;
            let ring = make_ring_for_ec();
            let params = make_ec_params();

            // Chunk is the first 8 bytes of shard 0 (offset 0, length 8).
            let chunk = ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 8,
                compressed: false,
                logical_length: 8,
            };

            let recovered = try_ec_recovery_for_chunk(
                &reader, &chunk, &params, &ring, None, None, None, 5000, None,
            )
            .await
            .unwrap();

            // Should recover the original shard 0 data.
            assert_eq!(&recovered[..], &original_data[0][0..8]);
        }

        /// `try_ec_recovery_for_chunk` succeeds when the target shard
        /// is intact (no EC decode needed) — fast path.
        #[tokio::test]
        async fn try_ec_recovery_intact_shard_reads_directly() {
            let data: [Vec<u8>; 4] = [
                b"INTACT_SHARD_0__".to_vec(),
                b"INTACT_SHARD_1__".to_vec(),
                b"INTACT_SHARD_2__".to_vec(),
                b"INTACT_SHARD_3__".to_vec(),
            ];
            let (original_data, _parity, segment) = encode_ec_segment(&data);

            let seg_id = SegmentId::new();
            let reader = Arc::new(InMemorySegmentReader::new());
            reader.put(seg_id, Bytes::from(segment));
            let reader: Arc<dyn SegmentReader> = reader;
            let ring = make_ring_for_ec();
            let params = make_ec_params();

            // Chunk at offset 0, length 8 (normal, no corruption).
            let chunk = ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 8,
                compressed: false,
                logical_length: 8,
            };

            let recovered = try_ec_recovery_for_chunk(
                &reader, &chunk, &params, &ring, None, None, None, 5000, None,
            )
            .await
            .unwrap();

            assert_eq!(&recovered[..], &original_data[0][0..8]);
        }

        /// `try_ec_recovery_for_chunk` errors when segment is too small.
        #[tokio::test]
        async fn try_ec_recovery_errors_on_too_small_segment() {
            let seg_id = SegmentId::new();
            let reader = Arc::new(InMemorySegmentReader::new());
            reader.put(seg_id, Bytes::from_static(b"tiny"));
            let reader: Arc<dyn SegmentReader> = reader;
            let ring = make_ring_for_ec();
            let params = make_ec_params();
            let chunk = ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 8,
                compressed: false,
                logical_length: 8,
            };

            let result = try_ec_recovery_for_chunk(
                &reader, &chunk, &params, &ring, None, None, None, 5000, None,
            )
            .await;
            assert!(result.is_err());
        }
    }
}
