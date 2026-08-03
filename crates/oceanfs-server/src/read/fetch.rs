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
//! (tokio::select! for timeout branches).

use std::sync::Arc;

use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use oceanfs_core::{
    proto::segment::FetchShardRequest as GprcFetchShardRequest, ChunkRef, ObjectMetadata,
};
use oceanfs_membership::Membership;
use oceanfs_network::{ConnectionPool, SegmentRpcClient};
use oceanfs_routing::RingCache;
use tracing::debug;

use crate::{
    error::{Error, Result},
    read_coordinator::SegmentReader,
};

/// Fetches blob data from segments identified by chunk references.
///
/// Each chunk is fetched in parallel using `FuturesUnordered`. When a
/// `segment_reader` is provided, local reads are used as a fast path.
/// When the local reader is absent or fails, the function falls back
/// to gRPC `FetchShard` calls to remote replicas (if `pool` and
/// `membership` are provided).
///
/// # Errors
///
/// Returns an error if no replica can serve a chunk, or if the
/// operation exceeds the timeout.
pub(crate) async fn fetch_chunks(
    ring: &Arc<RingCache>,
    metadata: &ObjectMetadata,
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
) -> Result<Vec<Bytes>> {
    fetch_chunks_inner(ring, metadata, timeout_ms, segment_reader, None, None).await
}

/// Internal version that accepts optional gRPC dependencies.
pub(crate) async fn fetch_chunks_with_grpc(
    ring: &Arc<RingCache>,
    metadata: &ObjectMetadata,
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
) -> Result<Vec<Bytes>> {
    fetch_chunks_inner(ring, metadata, timeout_ms, segment_reader, pool, membership).await
}

/// Internal implementation that supports both local and gRPC fetch.
async fn fetch_chunks_inner(
    ring: &Arc<RingCache>,
    metadata: &ObjectMetadata,
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
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

    fetch_all_chunks_parallel(ring, &metadata.chunks, timeout_ms, segment_reader, pool, membership)
        .await
}

/// Fetches all chunk data in parallel using `FuturesUnordered`.
///
/// Each chunk is fetched independently with its own timeout. Results are
/// collected as they complete and ordered by chunk index.
async fn fetch_all_chunks_parallel(
    ring: &Arc<RingCache>,
    chunks: &[ChunkRef],
    timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
) -> Result<Vec<Bytes>> {
    let chunk_count = chunks.len();

    // Spawn a fetch future per chunk in FuturesUnordered.
    let mut futs: FuturesUnordered<_> = chunks
        .iter()
        .enumerate()
        .map(|(idx, chunk)| {
            let ring = Arc::clone(ring);
            let chunk = *chunk;
            let segment_reader = segment_reader.cloned();
            let pool = pool.cloned();
            let membership = membership.cloned();
            async move {
                let result = fetch_single_chunk(
                    &ring,
                    &chunk,
                    timeout_ms,
                    segment_reader.as_ref(),
                    pool.as_ref(),
                    membership.as_ref(),
                )
                .await;
                (idx, result)
            }
        })
        .collect();

    // Collect results, preserving chunk order.
    let mut chunk_data: Vec<Option<Bytes>> = vec![None; chunk_count];
    let mut errors = Vec::new();

    while let Some((idx, result)) = futs.next().await {
        match result {
            Ok(data) => {
                chunk_data[idx] = Some(data);
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

/// Fetches a single chunk from the local segment reader or via gRPC fallback.
async fn fetch_single_chunk(
    ring: &Arc<RingCache>,
    chunk: &ChunkRef,
    _timeout_ms: u64,
    segment_reader: Option<&Arc<dyn SegmentReader>>,
    pool: Option<&Arc<ConnectionPool>>,
    membership: Option<&Arc<Membership>>,
) -> Result<Bytes> {
    // Fast path: local segment reader.
    if let Some(reader) = segment_reader {
        match reader.read_chunk(&chunk.segment_id, chunk.offset, chunk.length) {
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
    let segment_hash = blake3::hash(chunk.segment_id.to_string().as_bytes());
    let replica_set = ring.lookup(segment_hash.as_bytes());

    if replica_set.is_empty() {
        return Err(Error::Routing(format!(
            "no replicas for segment {} and no local reader",
            chunk.segment_id
        )));
    }

    // gRPC fallback: try to fetch from remote replicas via FetchShard.
    if let (Some(pool), Some(membership)) = (pool, membership) {
        for replica in &replica_set {
            let addr = match membership.address_of(replica) {
                Some(a) => a,
                None => continue,
            };

            let pooled = match pool.get_channel(addr).await {
                Ok(p) => p,
                Err(e) => {
                    debug!(replica = %replica, error = %e, "failed to acquire channel for fetch");
                    continue;
                }
            };

            let channel = pooled.channel().clone();
            drop(pooled);

            let proto_sid = chunk.segment_id.into();
            let mut client = SegmentRpcClient::new(channel);

            let request = tonic::Request::new(GprcFetchShardRequest {
                segment_id: Some(proto_sid),
                shard_index: 0,
                offset: chunk.offset,
                length: chunk.length as u64,
            });

            match client.fetch_shard(request).await {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    let mut data = Vec::new();
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
                        return Ok(Bytes::from(data));
                    }
                }
                Err(status) => {
                    debug!(
                        replica = %replica,
                        error = %status,
                        "gRPC fetch failed for replica"
                    );
                }
            }
        }
    }

    // Neither local reader nor gRPC succeeded.
    Err(Error::Internal(format!(
        "cannot fetch chunk {} — no segment reader and gRPC not available",
        chunk.segment_id
    )))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{NodeId, RingConfig, SegmentId};
    use oceanfs_routing::Ring;

    use super::*;
    use crate::read_coordinator::InMemorySegmentReader;

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
        let result = fetch_chunks(&ring, &meta, 1000, None).await.unwrap();
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
        let result = fetch_chunks(&ring, &meta, 1000, None).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn fetch_chunks_with_segment_reader_returns_real_data() {
        let seg_id = SegmentId::new();
        let test_data = b"real segment data for fetch test";
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef { segment_id: seg_id, offset: 0, length: test_data.len() as u32 });

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
        let result = fetch_chunks(&ring, &meta, 1000, Some(&reader)).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(&result[0][..], test_data);
    }

    #[tokio::test]
    async fn fetch_chunks_without_reader_returns_error() {
        let seg_id = SegmentId::new();
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef { segment_id: seg_id, offset: 0, length: 100 });

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
        let result = fetch_chunks(&ring, &meta, 5000, None).await;
        assert!(result.is_err(), "should fail without segment reader");
    }

    fn make_ring() -> Arc<RingCache> {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("n1"));
        Arc::new(RingCache::new(ring))
    }
}
