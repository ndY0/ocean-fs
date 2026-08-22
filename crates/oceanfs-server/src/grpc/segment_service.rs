//! Segment gRPC service.
//!
//! Handles `SegmentRpc::AppendSegment` (client-streaming append) and
//! `SegmentRpc::FetchShard` (server-streaming fetch) for node-to-node
//! data transfer.
//!
//! ## Wire Protocol
//!
//! **AppendSegment:** The remote coordinator streams `SegmentAppendRequest`
//! chunks. Once the final chunk is received, the service writes the
//! accumulated data to the local segment data store and returns an ack
//! with the write position.
//!
//! **FetchShard:** The caller requests a specific shard range (segment_id,
//! shard_index, offset, length). The service reads the segment data from
//! the local store, extracts the requested shard slice, and streams it
//! back in 64 KB chunks (per perf §4.4), ending with an empty-data sentinel.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use oceanfs_core::{
    proto::segment::{
        AckStatus, DeleteObjectRequest, DeleteObjectResponse, FetchShardRequest,
        GetObjectMetadataRequest, GetObjectMetadataResponse, PutObjectMetadataRequest,
        PutObjectMetadataResponse, SegmentAppendRequest, SegmentAppendResponse, ShardResponse,
    },
    BucketId, ChunkRef, Hlc, HlcClock, ObjectKey, ObjectMetadata, SegmentId,
};
use oceanfs_durability::SegmentDataStore;
use oceanfs_storage::{BufferPool, SegmentRpc};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

/// gRPC service for segment append and shard fetch.
///
/// Handles the node-to-node data plane: receiving segment data from
/// a remote writer coordinator, and serving shard data to a remote
/// reader.
pub struct SegmentGrpcService {
    /// Segment data store for reading and writing segment data.
    data_store: Arc<dyn SegmentDataStore>,
    /// Optional metadata store for persisting object metadata
    /// replicated alongside segment data.
    metadata_store: Option<Arc<dyn oceanfs_storage_api::MetadataStore>>,
    /// Optional async adapter over the metadata store: blocking RocksDB
    /// calls (DELETE replication, read-repair pushes) run on the
    /// blocking pool, never on a runtime worker
    /// (metadata-io-off-async-workers).
    metadata_async: Option<Arc<crate::metadata_async::AsyncMetadataOps>>,
    /// Buffer pool for segment data buffers (perf rule §1.2).
    buffer_pool: Arc<BufferPool>,
    /// HLC clock for receive-merge (hlc-causality-closure G2). Remote
    /// timestamps arriving on this service are merged via
    /// [`HlcClock::update`] so the local clock never lags replicas.
    hlc_clock: Arc<HlcClock>,
    /// Lifecycle coordinator (wired by the composition root). Replica
    /// appends REGISTER their segments here — an unregistered `.dat`
    /// is invisible to the GC and the orphan reaper (the fleet
    /// disk-fill root cause).
    lifecycle: Option<Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>>,
}

impl SegmentGrpcService {
    /// Creates a new segment gRPC service.
    ///
    /// # Arguments
    ///
    /// * `data_store` - The segment data store backing append and fetch operations.
    /// * `metadata_store` - Optional metadata store for cross-node metadata replication.
    /// * `buffer_pool` - Buffer pool for pre-allocated segment data buffers.
    /// * `hlc_clock` - HLC clock for receive-merge of remote timestamps.
    pub fn new(
        data_store: Arc<dyn SegmentDataStore>,
        metadata_store: Option<Arc<dyn oceanfs_storage_api::MetadataStore>>,
        buffer_pool: Arc<BufferPool>,
        hlc_clock: Arc<HlcClock>,
    ) -> Self {
        let metadata_async = metadata_store
            .clone()
            .map(|s| Arc::new(crate::metadata_async::AsyncMetadataOps::from_storage(s)));
        Self { data_store, metadata_store, metadata_async, buffer_pool, hlc_clock, lifecycle: None }
    }

    /// Wires the lifecycle coordinator so replicated appends register
    /// their segments (composition root).
    #[must_use]
    pub fn with_lifecycle(
        mut self,
        lifecycle: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
    ) -> Self {
        self.lifecycle = Some(lifecycle);
        self
    }

    /// Returns a reference to the underlying data store (for testing).
    #[doc(hidden)]
    pub fn data_store(&self) -> &Arc<dyn SegmentDataStore> {
        &self.data_store
    }

    /// Returns a reference to the HLC clock (for testing).
    #[doc(hidden)]
    pub fn hlc_clock(&self) -> &Arc<HlcClock> {
        &self.hlc_clock
    }
}

/// Stream bytes in 64 KB chunks with BLAKE3 checksums, ending with EOF sentinel.
///
/// Shared by single-shard and batched fetch_shard paths (Item 9).
fn stream_shard_bytes(shard_bytes: Bytes) -> ReceiverStream<Result<ShardResponse, Status>> {
    let (tx, rx) = mpsc::channel(16);
    let chunk_size = 65536; // 64 KB chunks

    tokio::spawn(async move {
        let mut chunk_idx = 0u32;
        let mut offset = 0usize;
        while offset < shard_bytes.len() {
            let end = (offset + chunk_size).min(shard_bytes.len());
            let chunk = shard_bytes.slice(offset..end);
            let checksum = blake3::hash(&chunk[..]);

            if tx
                .send(Ok(ShardResponse {
                    data: chunk,
                    checksum: Bytes::copy_from_slice(checksum.as_bytes()),
                    chunk_index: chunk_idx,
                }))
                .await
                .is_err()
            {
                break;
            }
            offset = end;
            chunk_idx += 1;
        }

        let _ = tx
            .send(Ok(ShardResponse {
                data: Bytes::new(),
                checksum: Bytes::from_static(&[0u8; 32]),
                chunk_index: u32::MAX,
            }))
            .await;
    });

    ReceiverStream::new(rx)
}

#[tonic::async_trait]
impl SegmentRpc for SegmentGrpcService {
    /// Handles a client-streaming append request.
    ///
    /// Accepts a stream of `SegmentAppendRequest` chunks from a remote
    /// writer coordinator, accumulates the full segment data, and writes
    /// it to the local segment data store.
    ///
    /// Returns a `SegmentAppendResponse` with the total bytes written
    /// and `AckStatus::Ok` on success. Returns `Internal` on I/O error.
    async fn append_segment(
        &self,
        request: Request<Streaming<SegmentAppendRequest>>,
    ) -> Result<Response<SegmentAppendResponse>, Status> {
        let mut stream = request.into_inner();
        let mut total_bytes: u64 = 0;
        let mut segment_data = self.buffer_pool.acquire();
        let mut segment_id = SegmentId::default();
        // Collect metadata from the first chunk that carries it.
        let mut bucket_id: Option<String> = None;
        let mut object_key: Option<String> = None;
        let mut object_size: u64 = 0;
        let mut blake3_hash = Bytes::new();
        let mut chunk_segment_ids: Vec<Bytes> = Vec::with_capacity(64);
        let mut chunk_offsets: Vec<u64> = Vec::with_capacity(64);
        let mut chunk_lengths: Vec<u32> = Vec::with_capacity(64);
        // The coordinator's HLC for the object, carried on the first
        // metadata-bearing chunk (hlc-causality-closure G3).
        let mut first_hlc: Option<Hlc> = None;

        // Collect all chunks from the stream.
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("append stream error: {e}")))?
        {
            // Extract segment_id from the first chunk.
            if let Some(ref proto_sid) = chunk.segment_id {
                if let Ok(sid) = SegmentId::try_from(proto_sid.clone()) {
                    segment_id = sid;
                }
            }
            // Capture metadata from the first chunk that carries it.
            if bucket_id.is_none() && !chunk.bucket_id.is_empty() {
                bucket_id = Some(chunk.bucket_id.clone());
                object_key = Some(chunk.object_key.clone());
                object_size = chunk.object_size;
                blake3_hash = chunk.blake3_hash.clone();
                chunk_segment_ids = chunk.chunk_segment_ids.clone();
                chunk_offsets = chunk.chunk_offsets.clone();
                chunk_lengths = chunk.chunk_lengths.clone();
                first_hlc = chunk.hlc.as_ref().map(|p| Hlc::new(p.wall_time, p.logical));
                if first_hlc.is_none() {
                    // A sender that omits the HLC is a bug (every fixed
                    // coordinator stamps it); make it visible.
                    tracing::warn!(
                        object_key = %chunk.object_key,
                        "append_segment: metadata-bearing chunk has no HLC — \
                         persisting zero timestamp (legacy sender?)"
                    );
                }
            }
            let chunk_len = chunk.data.len() as u64;
            segment_data.extend_from_slice(&chunk.data);
            total_bytes += chunk_len;
        }

        // Handle empty stream — nothing to write.
        if total_bytes == 0 {
            return Ok(Response::new(SegmentAppendResponse {
                wal_position: 0,
                ack: AckStatus::Error as i32,
            }));
        }

        // Persist object metadata if this append carried it (cross-node replication).
        if let (Some(ref md_store), Some(bucket), Some(key)) =
            (&self.metadata_store, bucket_id, object_key)
        {
            // G3: the coordinator's HLC travels with the request and is
            // persisted — replicated metadata must carry the original
            // version, not zero. Zero only for legacy senders.
            let hlc = first_hlc.unwrap_or_else(Hlc::zero);
            // Receive rule (G2): merge the remote timestamp into the
            // local clock before persisting.
            self.hlc_clock.update(hlc);

            let mut chunks = smallvec::SmallVec::new();
            for i in 0..chunk_segment_ids.len().min(chunk_offsets.len()).min(chunk_lengths.len()) {
                let seg_bytes: [u8; 16] =
                    chunk_segment_ids[i].as_ref().try_into().unwrap_or([0u8; 16]);
                chunks.push(ChunkRef {
                    segment_id: SegmentId::from_uuid_bytes(seg_bytes),
                    offset: chunk_offsets[i],
                    length: chunk_lengths[i],
                    compressed: false,
                    logical_length: chunk_lengths[i],
                });
            }
            // Inline objects (SizeTier::Inline on the coordinator)
            // carry no chunk references — the metadata must embed the
            // payload, or reads return EMPTY bytes (chunks empty +
            // inline_data None → the fetch path yields nothing). The
            // append stream's data IS the full blob, so store it inline
            // for chunkless metadata.
            let meta_chunks = chunks.clone();
            let meta = ObjectMetadata {
                object_key: ObjectKey::new(&key),
                size: object_size,
                blake3_hash: if blake3_hash.len() == 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&blake3_hash);
                    Some(oceanfs_core::HashOutput::from_bytes(arr))
                } else {
                    None
                },
                chunks: meta_chunks.clone(),
                inline_data: if meta_chunks.is_empty() {
                    Some(Bytes::copy_from_slice(&segment_data))
                } else {
                    None
                },
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
                hlc,
            };
            if let Err(e) = {
                let bucket_id = oceanfs_core::BucketId::new(&bucket);
                oceanfs_storage_api::MetadataStore::put_object(md_store.as_ref(), &bucket_id, meta)
            } {
                // FAIL the append: the coordinator must see the ack as
                // an error and HINT the target — a silent Ok (with only
                // a warn) makes the coordinator count the ack, the
                // write succeeds with quorum, and the receiver never
                // gets the object (no data, no hint) — the churn
                // 404/404/200 divergence.
                tracing::warn!(
                    bucket = %bucket,
                    key = %key,
                    error = %e,
                    "append_segment: failed to persist replicated metadata; \
                     failing the append so the coordinator hints"
                );
                return Err(Status::internal(format!(
                    "failed to persist replicated metadata: {e}"
                )));
            }

            // Write the local segment copy ONLY when it can serve the
            // object exactly. The copy starts at offset 0 of the
            // segment — for an object placed mid-segment on the
            // coordinator (offset > 0), the copy would shadow owner
            // fetches with CLAMPED garbage (the segment reader clamps
            // out-of-range reads to the file length instead of
            // erroring): reads would return wrong bytes and fail hash
            // verification instead of falling back to the segment
            // owner. With no copy, the local read misses and the
            // gRPC fallback serves the owner's correct bytes.
            let first_chunk_offset = meta_chunks.first().map(|c| c.offset).unwrap_or(0);
            if first_chunk_offset == 0 {
                self.data_store
                    .write_segment_data(&segment_id, &segment_data)
                    .map_err(|e| Status::internal(format!("segment write failed: {e}")))?;
            }
        } else {
            // No metadata carried: keep the legacy behavior of writing
            // the accumulated data to the segment store.
            self.data_store
                .write_segment_data(&segment_id, &segment_data)
                .map_err(|e| Status::internal(format!("segment write failed: {e}")))?;
        }

        tracing::debug!(
            segment_id = %segment_id,
            total_bytes = total_bytes,
            "append_segment: wrote {} bytes to store",
            total_bytes
        );

        // Register the replica segment in the lifecycle machine. The
        // registry drives the GC and the orphan reaper — a raw `.dat`
        // without an entry is INVISIBLE to both: the GC never
        // attributes its dead bytes, the reaper never sweeps it, and
        // the disk fills with replica copies (the fleet disk-fill root
        // cause: ~10k unregistered files vs 32 registered; phase 2 has
        // no replication, so every segment was a registered local
        // append and stayed bounded). The segment is complete on
        // arrival, so it is reserved and immediately sealed; the
        // durable ReserveEvent precedes the data (ADR-0024).
        if let Some(lifecycle) = &self.lifecycle {
            let tier = oceanfs_storage::TierRouter::new(oceanfs_core::SegmentSizeConfig::default())
                .classify(total_bytes);
            let sealed_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let merkle_root =
                oceanfs_durability::MerkleTree::build(&segment_data, 0).map(|t| t.root().hash());
            let meta = oceanfs_core::SegmentMetadata {
                pool_id: 0,
                segment_id,
                ec_k: 1,
                ec_m: 0,
                size_tier: tier,
                merkle_root,
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(sealed_at),
            };
            match lifecycle.request_reserve(segment_id, tier, 1, 0).await {
                Ok(()) => {
                    if let Err(e) = lifecycle.request_seal(segment_id, meta, None).await {
                        tracing::warn!(
                            segment_id = %segment_id,
                            error = ?e,
                            "append_segment: failed to seal replica segment"
                        );
                    }
                }
                Err(e) => {
                    // Already registered (replayed or duplicate) —
                    // nothing to do.
                    tracing::debug!(
                        segment_id = %segment_id,
                        error = ?e,
                        "append_segment: replica segment already registered"
                    );
                }
            }
        }

        Ok(Response::new(SegmentAppendResponse {
            wal_position: total_bytes,
            ack: AckStatus::Ok as i32,
        }))
    }

    /// Handles a delete-object request from the write coordinator.
    ///
    /// Removes object metadata from the local metadata store so that
    /// subsequent reads return 404. The tombstone carries the delete's
    /// HLC from the request (G4/G8) — the same timestamp the originating
    /// node stamped locally — so all replicas converge on one tombstone
    /// version.
    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(&req.bucket_id);
        let key = ObjectKey::new(&req.object_key);

        // Parse the delete's HLC (zero for legacy senders) and merge it
        // into the local clock (receive rule, G2).
        let hlc = match req.hlc {
            Some(ref hlc_proto) => Hlc::new(hlc_proto.wall_time, hlc_proto.logical),
            None => Hlc::zero(),
        };
        self.hlc_clock.update(hlc);

        if let Some(ref md_store) = self.metadata_async {
            md_store
                .delete_object(&bucket, &key, hlc)
                .await
                .map_err(|e| Status::internal(format!("metadata delete failed: {e}")))?;
        }

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            has_metadata_store = self.metadata_async.is_some(),
            "delete_object: tombstone applied"
        );

        Ok(Response::new(DeleteObjectResponse { deleted: true }))
    }

    type FetchShardStream = ReceiverStream<Result<ShardResponse, Status>>;

    /// Handles a server-streaming fetch request.
    ///
    /// Reads the requested segment data from the local data store,
    /// extracts the shard slice specified by `shard_index`, `offset`,
    /// and `length`, and streams the data back in 64 KB chunks.
    ///
    /// Each chunk carries a BLAKE3 checksum of the chunk data.
    /// The stream ends with a final empty-data chunk as EOF sentinel
    /// (per spec §12.3).
    ///
    /// Returns `NotFound` if the segment does not exist.
    /// Returns `Internal` on I/O error.
    async fn fetch_shard(
        &self,
        request: Request<FetchShardRequest>,
    ) -> Result<Response<Self::FetchShardStream>, Status> {
        let req = request.into_inner();

        let segment_id = req
            .segment_id
            .and_then(|sid| SegmentId::try_from(sid).ok())
            .ok_or_else(|| Status::invalid_argument("missing or invalid segment_id"))?;

        // Read the full segment data from the store once.
        let segment_data = self
            .data_store
            .read_segment_data(&segment_id)
            .map_err(|e| Status::not_found(format!("segment {} not found: {}", segment_id, e)))?;

        if segment_data.is_empty() {
            return Err(Status::not_found(format!("segment {} has no data", segment_id)));
        }

        let segment_bytes = segment_data;

        // Determine total shards from known configuration (k+m).
        let total_shards = 6; // default k=4, m=2

        // Batched mode (Item 9): iterate over repeated shard ranges.
        if !req.shards.is_empty() {
            let shard_size =
                if segment_bytes.is_empty() { 0 } else { segment_bytes.len() / total_shards };
            let mut all_data = BytesMut::new();
            for range in &req.shards {
                let si = range.shard_index as usize;
                let start = range.offset as usize;
                let len = if range.length == 0 {
                    shard_size.saturating_sub(start)
                } else {
                    range.length as usize
                };
                let shard_start = (si * shard_size) + start;
                let shard_end = (shard_start + len).min(segment_bytes.len());
                if shard_start < segment_bytes.len() {
                    all_data.extend_from_slice(&segment_bytes[shard_start..shard_end]);
                }
            }
            tracing::debug!(
                segment_id = %segment_id,
                shard_count = req.shards.len(),
                total_bytes = all_data.len(),
                "fetch_shard batched response"
            );
            return Ok(Response::new(stream_shard_bytes(all_data.freeze())));
        }

        // Single-shard mode (existing behavior).
        let shard_size =
            if segment_bytes.is_empty() { 0 } else { segment_bytes.len() / total_shards };
        let si = req.shard_index as usize;
        let start = req.offset as usize;
        let len =
            if req.length == 0 { shard_size.saturating_sub(start) } else { req.length as usize };
        let shard_start = (si * shard_size) + start;
        let shard_end = (shard_start + len).min(segment_bytes.len());

        if shard_start >= segment_bytes.len() {
            return Err(Status::out_of_range(format!(
                "shard index {} out of range for segment of {} bytes",
                si,
                segment_bytes.len()
            )));
        }

        let shard_bytes = segment_bytes.slice(shard_start..shard_end);
        Ok(Response::new(stream_shard_bytes(shard_bytes)))
    }

    /// Handles a metadata fetch request for read repair (4.2).
    ///
    /// Queries the local metadata store for the given object and returns
    /// its HLC timestamp, size, hash, and chunk references so the caller
    /// can compare versions across replicas.
    async fn get_object_metadata(
        &self,
        request: Request<GetObjectMetadataRequest>,
    ) -> Result<Response<GetObjectMetadataResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(&req.bucket_id);
        let key = ObjectKey::new(&req.object_key);

        let md_store = self
            .metadata_store
            .as_ref()
            .ok_or_else(|| Status::unimplemented("no metadata store configured"))?;

        match oceanfs_storage_api::MetadataStore::get_object_metadata(
            md_store.as_ref(),
            &bucket,
            &key,
        )
        .map_err(|e| Status::internal(format!("metadata lookup: {e}")))?
        {
            Some(meta) => {
                let mut chunk_segment_ids: Vec<oceanfs_core::proto::common::SegmentId> =
                    Vec::with_capacity(meta.chunks.len());
                let mut chunk_offsets: Vec<u64> = Vec::with_capacity(meta.chunks.len());
                let mut chunk_lengths: Vec<u32> = Vec::with_capacity(meta.chunks.len());
                let mut chunk_logical_lengths: Vec<u32> = Vec::with_capacity(meta.chunks.len());
                let mut chunk_compressed: Vec<bool> = Vec::with_capacity(meta.chunks.len());
                for chunk in &meta.chunks {
                    chunk_segment_ids.push(chunk.segment_id.into());
                    chunk_offsets.push(chunk.offset);
                    chunk_lengths.push(chunk.length);
                    chunk_logical_lengths.push(if chunk.compressed {
                        chunk.logical_length
                    } else {
                        chunk.length
                    });
                    chunk_compressed.push(chunk.compressed);
                }

                let hlc_proto = oceanfs_core::proto::common::HlcTimestamp {
                    wall_time: meta.hlc.wall_time,
                    logical: meta.hlc.logical,
                };

                Ok(Response::new(GetObjectMetadataResponse {
                    found: true,
                    size: meta.size,
                    blake3_hash: meta
                        .blake3_hash
                        .map(|h| Bytes::copy_from_slice(h.as_bytes()))
                        .unwrap_or_default(),
                    hlc: Some(hlc_proto),
                    inline_data: meta.inline_data.unwrap_or_default(),
                    chunk_segment_ids,
                    chunk_offsets,
                    chunk_lengths,
                    chunk_logical_lengths,
                    chunk_compressed,
                }))
            }
            None => Ok(Response::new(GetObjectMetadataResponse {
                found: false,
                size: 0,
                blake3_hash: Bytes::new(),
                hlc: None,
                inline_data: Bytes::new(),
                chunk_segment_ids: vec![],
                chunk_offsets: vec![],
                chunk_lengths: vec![],
                chunk_logical_lengths: vec![],
                chunk_compressed: vec![],
            })),
        }
    }

    /// Handles a metadata push request from read repair (4.2).
    ///
    /// Receives corrected object metadata + inline data from a peer
    /// and writes it to the local metadata store, overwriting any
    /// stale entry.
    ///
    /// A tombstoned key is authoritative and MUST NOT be resurrected:
    /// a read repair fired by a pre-delete GET could otherwise re-write
    /// the object after the tombstone landed (t19). Only a genuine new
    /// write (which clears the tombstone via `put_object`) may replace a
    /// tombstoned key, so the push is rejected with `failed_precondition`.
    async fn put_object_metadata(
        &self,
        request: Request<PutObjectMetadataRequest>,
    ) -> Result<Response<PutObjectMetadataResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(&req.bucket_id);
        let key = ObjectKey::new(&req.object_key);

        let md_store = self
            .metadata_store
            .as_ref()
            .ok_or_else(|| Status::unimplemented("no metadata store configured"))?;

        let hlc = match req.hlc {
            Some(ref hlc_proto) => Hlc::new(hlc_proto.wall_time, hlc_proto.logical),
            None => Hlc::zero(),
        };

        // Receive rule (G2): merge the remote timestamp into the local
        // clock before acting on it.
        self.hlc_clock.update(hlc);

        // G6: order-aware delete-vs-write resolution. A tombstone
        // rejects the repair push only when the incoming write did NOT
        // happen after the delete. A strictly newer HLC legitimately
        // resurrects the object (the write happened after the delete,
        // on some node); a zero HLC (legacy sender, un-migrated hint)
        // never resurrects. The lookup and the write are not atomic,
        // but the residual window is a delete racing a repair push on
        // the same key — the sender-side re-validation in
        // `run_read_repair` shrinks it to near zero.
        if let Some(tombstone) =
            oceanfs_storage_api::MetadataStore::get_tombstone(md_store.as_ref(), &bucket, &key)
                .map_err(|e| Status::internal(format!("tombstone lookup failed: {e}")))?
        {
            if hlc == Hlc::zero() || hlc <= tombstone.hlc {
                tracing::warn!(
                    bucket = %bucket,
                    key = %key,
                    push_wall = hlc.wall_time(),
                    push_logical = hlc.logical(),
                    tombstone_wall = tombstone.hlc.wall_time(),
                    "rejecting read-repair metadata push: object is tombstoned \
                     and the incoming HLC is not newer"
                );
                return Err(Status::failed_precondition("object is tombstoned"));
            }
            // The write happened after the delete: legitimate
            // resurrection. Clear the tombstone so the object is live.
            oceanfs_storage_api::MetadataStore::delete_tombstone(md_store.as_ref(), &bucket, &key)
                .map_err(|e| Status::internal(format!("tombstone clear failed: {e}")))?;
            tracing::info!(
                bucket = %bucket,
                key = %key,
                push_wall = hlc.wall_time(),
                "read-repair push newer than tombstone; clearing tombstone (legitimate resurrection)"
            );
        }

        let mut chunks = smallvec::SmallVec::new();
        let count =
            req.chunk_segment_ids.len().min(req.chunk_offsets.len()).min(req.chunk_lengths.len());
        for i in 0..count {
            let seg_id = SegmentId::try_from(req.chunk_segment_ids[i].clone())
                .unwrap_or_else(|_| SegmentId::default());
            chunks.push(ChunkRef {
                segment_id: seg_id,
                offset: req.chunk_offsets[i],
                length: req.chunk_lengths[i],
                compressed: false,
                logical_length: req.chunk_lengths[i],
            });
        }

        let blake3_hash = if req.blake3_hash.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&req.blake3_hash);
            Some(oceanfs_core::HashOutput::from_bytes(arr))
        } else {
            None
        };

        let inline_data = if req.inline_data.is_empty() { None } else { Some(req.inline_data) };

        // LWW gate: a push OLDER than the local row must not regress
        // it. The pusher compared versions at ITS read time — by
        // delivery time the receiver may have applied a newer write or
        // hint; overwriting it would regress the newer version, after
        // which an older delete hint could tombstone the key (the churn
        // 404/404/200 divergence: one node serves the newest write,
        // the other two tombstoned by stale pushes + older deletes).
        if let Some(local) = oceanfs_storage_api::MetadataStore::get_object_metadata(
            md_store.as_ref(),
            &bucket,
            &key,
        )
        .map_err(|e| Status::internal(format!("metadata lookup during read repair: {e}")))?
        {
            if hlc < local.hlc {
                tracing::warn!(
                    bucket = %bucket,
                    key = %key,
                    push_wall = hlc.wall_time(),
                    local_wall = local.hlc.wall_time(),
                    "rejecting read-repair metadata push: local version is newer (LWW)"
                );
                return Err(Status::failed_precondition("push is older than local version"));
            }
        }

        let meta = ObjectMetadata {
            object_key: key,
            size: req.size,
            blake3_hash,
            chunks,
            inline_data,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            hlc,
        };

        oceanfs_storage_api::MetadataStore::put_object(md_store.as_ref(), &bucket, meta)
            .map_err(|e| Status::internal(format!("metadata write during read repair: {e}")))?;

        Ok(Response::new(PutObjectMetadataResponse { written: true }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{collections::HashMap, net::SocketAddr};

    use oceanfs_core::{proto::common::SegmentId as ProtoSegmentId, SegmentId, Tombstone};
    use oceanfs_durability::SegmentDataStore;
    use oceanfs_storage::{SegmentRpcClient, SegmentRpcServer};
    use oceanfs_storage_api::MetadataStore;
    use parking_lot::Mutex;
    use tonic::transport::Server;

    use super::*;

    /// In-memory segment data store for testing.
    struct TestSegmentStore {
        data: Mutex<HashMap<SegmentId, Bytes>>,
    }

    impl TestSegmentStore {
        fn new() -> Self {
            Self { data: Mutex::new(HashMap::new()) }
        }
    }

    impl SegmentDataStore for TestSegmentStore {
        fn write_segment_data(
            &self,
            segment_id: &SegmentId,
            data: &[u8],
        ) -> Result<(), oceanfs_storage::Error> {
            self.data.lock().insert(*segment_id, Bytes::copy_from_slice(data));
            Ok(())
        }

        fn read_segment_data(
            &self,
            segment_id: &SegmentId,
        ) -> Result<Bytes, oceanfs_storage::Error> {
            self.data
                .lock()
                .get(segment_id)
                .cloned()
                .ok_or(oceanfs_storage::Error::SegmentNotFound(*segment_id))
        }
    }

    /// Helper to start a test gRPC server with the segment service and return a client.
    async fn test_server(
        store: Arc<dyn SegmentDataStore>,
    ) -> SegmentRpcClient<tonic::transport::Channel> {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let service = SegmentGrpcService::new(
            store,
            None,
            Arc::new(BufferPool::new(65536, 1024)),
            Arc::new(HlcClock::new()),
        );
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            Server::builder()
                .add_service(SegmentRpcServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        // Give the server a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        SegmentRpcClient::connect(format!("http://{addr}")).await.unwrap()
    }

    /// Test server variant with the lifecycle coordinator wired (the
    /// composition-root shape) — the replica appends must register
    /// their segments.
    async fn test_server_with_lifecycle(
        store: Arc<dyn SegmentDataStore>,
        metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
    ) -> (
        SegmentRpcClient<tonic::transport::Channel>,
        Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
    ) {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let lifecycle = Arc::new(
            oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                &oceanfs_core::LifecycleConfig::default(),
            )
            .with_event_wal(Arc::new(
                oceanfs_storage::segment::event_wal::EventWal::open(
                    tempfile::tempdir().unwrap().path().join("event-wal"),
                    &oceanfs_core::EventWalConfig {
                        event_wal_dir: tempfile::tempdir().unwrap().path().join("event-wal"),
                        event_wal_file_size_bytes: 1024 * 1024,
                        event_wal_fsync_batch_timeout_ms: 10,
                        event_wal_checkpoint_bytes: 1024 * 1024,
                    },
                )
                .await
                .unwrap(),
            )),
        );
        let service = SegmentGrpcService::new(
            store,
            Some(metadata),
            Arc::new(BufferPool::new(65536, 1024)),
            Arc::new(HlcClock::new()),
        )
        .with_lifecycle(Arc::clone(&lifecycle));
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            Server::builder()
                .add_service(SegmentRpcServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = SegmentRpcClient::connect(format!("http://{addr}")).await.unwrap();
        (client, lifecycle)
    }

    /// The replication receiver must REGISTER the replica segment in
    /// the lifecycle machine: an unregistered `.dat` is invisible to
    /// the GC and the orphan reaper — the fleet disk-fill root cause
    /// (the registry knew 32 segments while the disk held ~10k files).
    #[tokio::test]
    async fn append_segment_registers_replica_segment_in_lifecycle() {
        let store: Arc<dyn SegmentDataStore> = Arc::new(TestSegmentStore::new());
        let metadata: Arc<dyn oceanfs_storage_api::MetadataStore> = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: tempfile::tempdir().unwrap().path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        );

        let (mut client, lifecycle) = test_server_with_lifecycle(store, metadata).await;

        let segment_id = SegmentId::new();
        let chunk = make_append_chunk(Some(oceanfs_core::proto::common::HlcTimestamp {
            wall_time: 1_700_000_000_000,
            logical: 1,
        }));
        // The first chunk carries the segment id + the object metadata.
        let mut request = oceanfs_core::proto::segment::SegmentAppendRequest {
            segment_id: Some(ProtoSegmentId::from(segment_id)),
            shard_index: None,
            offset: 0,
            data: Bytes::from(vec![0xAB; 1024]),
            hlc: Some(oceanfs_core::proto::common::HlcTimestamp {
                wall_time: 1_700_000_000_000,
                logical: 1,
            }),
            bucket_id: "test".into(),
            object_key: "obj".into(),
            object_size: 1024,
            blake3_hash: Bytes::new(),
            chunk_segment_ids: vec![Bytes::copy_from_slice(segment_id.as_uuid().as_bytes())],
            chunk_offsets: vec![0],
            chunk_lengths: vec![1024],
        };
        request.data = chunk.data;

        let response = client.append_segment(tokio_stream::iter(vec![request])).await.unwrap();
        assert_eq!(response.into_inner().ack as i32, 0, "ack must be Ok");

        // The segment must now be registered AND sealed in the lifecycle.
        let entry = lifecycle.registry().get(segment_id).expect("segment registered");
        assert_eq!(entry.state, oceanfs_storage::SegmentState::Sealed);
        assert!(entry.metadata.sealed_at.is_some(), "sealed_at must be set (reaper TTL gate)");
    }

    #[tokio::test]
    async fn append_empty_stream_returns_error_ack() {
        let store = Arc::new(TestSegmentStore::new());
        let mut client = test_server(store).await;

        let stream = tokio_stream::iter(vec![]);
        let request = tonic::Request::new(stream);
        let response = client.append_segment(request).await.unwrap();
        let resp = response.into_inner();
        assert_eq!(resp.ack, AckStatus::Error as i32);
    }

    #[tokio::test]
    async fn append_valid_stream_persists_data() {
        let store = Arc::new(TestSegmentStore::new());
        let mut client = test_server(store.clone()).await;

        let seg_id = SegmentId::new();
        let proto_sid: ProtoSegmentId = seg_id.into();
        let test_data = Bytes::from_static(b"hello world append test data");

        let chunk = SegmentAppendRequest {
            segment_id: Some(proto_sid),
            shard_index: None,
            offset: 0,
            data: test_data.clone(),
            hlc: None,
            bucket_id: String::new(),
            object_key: String::new(),
            object_size: 0,
            blake3_hash: Bytes::new(),
            chunk_segment_ids: vec![],
            chunk_offsets: vec![],
            chunk_lengths: vec![],
        };

        let stream = tokio_stream::iter(vec![chunk]);
        let request = tonic::Request::new(stream);
        let response = client.append_segment(request).await.unwrap();
        let resp = response.into_inner();

        assert_eq!(resp.ack, AckStatus::Ok as i32);
        assert_eq!(resp.wal_position as usize, test_data.len());

        // Verify data was actually stored.
        let stored = store.read_segment_data(&seg_id).unwrap();
        assert_eq!(stored, test_data);
    }

    #[tokio::test]
    async fn fetch_nonexistent_segment_returns_not_found() {
        let store = Arc::new(TestSegmentStore::new());
        let mut client = test_server(store).await;

        let seg_id = SegmentId::new();
        let proto_sid: ProtoSegmentId = seg_id.into();

        let request = tonic::Request::new(FetchShardRequest {
            segment_id: Some(proto_sid),
            shard_index: 0,
            offset: 0,
            length: 0,
            shards: vec![],
        });

        let result = client.fetch_shard(request).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn fetch_existing_segment_returns_data() {
        let store = Arc::new(TestSegmentStore::new());
        let seg_id = SegmentId::new();

        // Write test data where total_shards=6, so each shard is data.len()/6.
        // (The previous version cast the byte count to u8, which truncated
        // 6144 to 0 and produced an empty segment — the server correctly
        // answered NotFound, masking the test's own bug.)
        let total_shards = 6;
        let shard_size = 1024;
        let test_data: Vec<u8> = (0..shard_size * total_shards).map(|v| (v % 256) as u8).collect();
        store.write_segment_data(&seg_id, &test_data).unwrap();

        let mut client = test_server(store).await;
        let proto_sid: ProtoSegmentId = seg_id.into();

        let request = tonic::Request::new(FetchShardRequest {
            segment_id: Some(proto_sid),
            shard_index: 0,
            offset: 0,
            length: 0,
            shards: vec![],
        });

        let mut response_stream = client.fetch_shard(request).await.unwrap().into_inner();

        let mut received_bytes: Vec<u8> = Vec::new();
        while let Some(chunk_result) = response_stream.message().await.unwrap() {
            if chunk_result.data.is_empty() {
                break; // EOF sentinel
            }
            // Verify checksum
            let computed = blake3::hash(&chunk_result.data);
            assert_eq!(
                computed.as_bytes(),
                chunk_result.checksum.as_ref(),
                "checksum mismatch in chunk {}",
                chunk_result.chunk_index
            );
            received_bytes.extend_from_slice(&chunk_result.data);
        }

        assert!(!received_bytes.is_empty(), "should have received shard data");
        assert_eq!(received_bytes.len(), shard_size);
        assert_eq!(&received_bytes[..], &test_data[..shard_size]);
    }

    #[tokio::test]
    async fn fetch_shard_with_offset_returns_correct_slice() {
        let store = Arc::new(TestSegmentStore::new());
        let seg_id = SegmentId::new();
        let total_shards = 6;
        let shard_size = 500;
        // Same u8-truncation bug class as `fetch_existing_segment_returns_data`:
        // 3000 as u8 would truncate to 184. Generate the full 3000 bytes.
        let test_data: Vec<u8> = (0..shard_size * total_shards).map(|v| (v % 256) as u8).collect();
        store.write_segment_data(&seg_id, &test_data).unwrap();

        let mut client = test_server(store).await;
        let proto_sid: ProtoSegmentId = seg_id.into();

        // Fetch shard 0 with offset 100 and explicit length 50.
        let request = tonic::Request::new(FetchShardRequest {
            segment_id: Some(proto_sid),
            shard_index: 0,
            offset: 100,
            length: 50,
            shards: vec![],
        });

        let mut response_stream = client.fetch_shard(request).await.unwrap().into_inner();

        let mut received_bytes: Vec<u8> = Vec::new();
        while let Some(chunk_result) = response_stream.message().await.unwrap() {
            if chunk_result.data.is_empty() {
                break;
            }
            received_bytes.extend_from_slice(&chunk_result.data);
        }

        assert_eq!(received_bytes.len(), 50);
        assert_eq!(&received_bytes[..], &test_data[100..150]);
    }

    // ── Tombstone gate tests (F3/t19 + hlc-causality-closure G6) ─────

    /// Metadata mock with configurable tombstone state (storing real
    /// [`Tombstone`] values so `get_tombstone` returns stamped HLCs).
    struct TombstoneMockMetadata {
        tombstoned: Mutex<HashMap<(String, String), Tombstone>>,
        last_put: Mutex<Option<ObjectMetadata>>,
        local_row: Mutex<Option<ObjectMetadata>>,
    }

    impl TombstoneMockMetadata {
        fn new() -> Self {
            Self {
                tombstoned: Mutex::new(HashMap::new()),
                last_put: Mutex::new(None),
                local_row: Mutex::new(None),
            }
        }

        fn get_tombstone_value(&self, key: &str) -> Option<Tombstone> {
            self.tombstoned.lock().get(&("b".to_string(), key.to_string())).cloned()
        }

        fn last_put(&self) -> Option<ObjectMetadata> {
            self.last_put.lock().clone()
        }

        /// Seeds a local object row (the receiver's current version).
        fn seed_local_row(&self, meta: ObjectMetadata) {
            *self.local_row.lock() = Some(meta);
        }
    }

    impl oceanfs_storage_api::MetadataStore for TombstoneMockMetadata {
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
            Ok(self.local_row.lock().clone())
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

        fn delete_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> std::io::Result<()> {
            self.tombstoned.lock().remove(&(bucket.as_str().into(), key.as_str().into()));
            Ok(())
        }

        fn has_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> std::io::Result<bool> {
            Ok(self.tombstoned.lock().contains_key(&(bucket.as_str().into(), key.as_str().into())))
        }

        fn get_tombstone(
            &self,
            bucket: &BucketId,
            key: &ObjectKey,
        ) -> std::io::Result<Option<Tombstone>> {
            Ok(self.tombstoned.lock().get(&(bucket.as_str().into(), key.as_str().into())).cloned())
        }

        fn put_object(&self, bucket: &BucketId, meta: ObjectMetadata) -> std::io::Result<()> {
            // Mirror the real store: a genuine write clears the tombstone.
            self.delete_tombstone(bucket, &meta.object_key)?;
            *self.last_put.lock() = Some(meta);
            Ok(())
        }

        fn delete_object(
            &self,
            bucket: &BucketId,
            key: &ObjectKey,
            hlc: Hlc,
        ) -> std::io::Result<()> {
            self.tombstoned.lock().insert(
                (bucket.as_str().into(), key.as_str().into()),
                Tombstone {
                    deletion_time: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64,
                    hlc,
                    chunks: smallvec::SmallVec::new(),
                },
            );
            Ok(())
        }

        fn batch_write(&self, _ops: Vec<oceanfs_storage_api::BatchOp>) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn make_put_metadata_request(bucket: &str, key: &str) -> PutObjectMetadataRequest {
        make_put_metadata_request_with_hlc(bucket, key, None)
    }

    fn make_put_metadata_request_with_hlc(
        bucket: &str,
        key: &str,
        hlc: Option<oceanfs_core::proto::common::HlcTimestamp>,
    ) -> PutObjectMetadataRequest {
        PutObjectMetadataRequest {
            bucket_id: bucket.to_string(),
            object_key: key.to_string(),
            size: 5,
            blake3_hash: Bytes::new(),
            hlc,
            inline_data: Bytes::from_static(b"hello"),
            chunk_segment_ids: vec![],
            chunk_offsets: vec![],
            chunk_lengths: vec![],
            chunk_logical_lengths: vec![],
            chunk_compressed: vec![],
        }
    }

    /// F3/t19: a read-repair push for a tombstoned key is rejected —
    /// a deleted object must never be resurrected by read repair.
    #[tokio::test]
    async fn put_object_metadata_rejects_tombstoned_key() {
        let metadata: Arc<dyn oceanfs_storage_api::MetadataStore> =
            Arc::new(TombstoneMockMetadata::new());
        // Tombstone the key first.
        metadata.delete_object(&BucketId::new("b"), &ObjectKey::new("k"), Hlc::zero()).unwrap();

        let service = SegmentGrpcService::new(
            Arc::new(TestSegmentStore::new()),
            Some(metadata),
            Arc::new(BufferPool::new(65536, 4)),
            Arc::new(HlcClock::new()),
        );

        let result = service
            .put_object_metadata(tonic::Request::new(make_put_metadata_request("b", "k")))
            .await;
        assert!(result.is_err(), "tombstoned key must reject read-repair pushes");
        assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
    }

    /// The tombstone gate must not block a legitimate repair push for a
    /// key that was never deleted.
    #[tokio::test]
    async fn put_object_metadata_accepts_clean_key() {
        let metadata: Arc<dyn oceanfs_storage_api::MetadataStore> =
            Arc::new(TombstoneMockMetadata::new());

        let service = SegmentGrpcService::new(
            Arc::new(TestSegmentStore::new()),
            Some(metadata),
            Arc::new(BufferPool::new(65536, 4)),
            Arc::new(HlcClock::new()),
        );

        let result = service
            .put_object_metadata(tonic::Request::new(make_put_metadata_request("b", "k")))
            .await;
        assert!(result.is_ok(), "clean key must accept read-repair push");
    }

    /// G6: a repair push newer than the tombstone is a legitimate
    /// resurrection — it succeeds and clears the tombstone.
    #[tokio::test]
    async fn put_object_metadata_newer_than_tombstone_succeeds_and_clears() {
        let metadata = Arc::new(TombstoneMockMetadata::new());
        // Delete stamped at (1000, 0).
        metadata
            .delete_object(&BucketId::new("b"), &ObjectKey::new("k"), Hlc::new(1000, 0))
            .unwrap();

        let service = SegmentGrpcService::new(
            Arc::new(TestSegmentStore::new()),
            Some(metadata.clone() as Arc<dyn oceanfs_storage_api::MetadataStore>),
            Arc::new(BufferPool::new(65536, 4)),
            Arc::new(HlcClock::new()),
        );

        // Push a write stamped at (2000, 0) — after the delete.
        let request = make_put_metadata_request_with_hlc(
            "b",
            "k",
            Some(oceanfs_core::proto::common::HlcTimestamp { wall_time: 2000, logical: 0 }),
        );
        let result = service.put_object_metadata(tonic::Request::new(request)).await;
        assert!(result.is_ok(), "newer push must resurrect the object: {result:?}");

        assert!(metadata.get_tombstone_value("k").is_none(), "tombstone must be cleared");
        let put = metadata.last_put().expect("push must persist object metadata");
        assert_eq!(put.hlc, Hlc::new(2000, 0), "persisted metadata carries the push's HLC");
    }

    /// LWW: a repair push OLDER than the local row is rejected — a
    /// stale pusher must not regress a newer version (the churn
    /// 404/404/200 divergence: a stale push regresses a node, after
    /// which an older delete hint can tombstone the key).
    #[tokio::test]
    async fn put_object_metadata_older_than_local_row_rejected() {
        let metadata = Arc::new(TombstoneMockMetadata::new());
        // The receiver already has a NEWER version (2000, 0).
        metadata.seed_local_row(oceanfs_core::ObjectMetadata {
            object_key: ObjectKey::new("k"),
            size: 5,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: None,
            created_at: 0,
            hlc: Hlc::new(2000, 0),
        });

        let service = SegmentGrpcService::new(
            Arc::new(TestSegmentStore::new()),
            Some(metadata.clone() as Arc<dyn oceanfs_storage_api::MetadataStore>),
            Arc::new(BufferPool::new(65536, 4)),
            Arc::new(HlcClock::new()),
        );

        // A stale push (1000, 0) must be rejected.
        let request = make_put_metadata_request_with_hlc(
            "b",
            "k",
            Some(oceanfs_core::proto::common::HlcTimestamp { wall_time: 1000, logical: 0 }),
        );
        let result = service.put_object_metadata(tonic::Request::new(request)).await;
        assert_eq!(
            result.unwrap_err().code(),
            tonic::Code::FailedPrecondition,
            "a push older than the local row must be rejected (LWW)",
        );
        assert!(metadata.last_put().is_none(), "the stale push must not persist");

        // An equal-HLC push is idempotent and accepted.
        let request = make_put_metadata_request_with_hlc(
            "b",
            "k",
            Some(oceanfs_core::proto::common::HlcTimestamp { wall_time: 2000, logical: 0 }),
        );
        let result = service.put_object_metadata(tonic::Request::new(request)).await;
        assert!(result.is_ok(), "an equal-HLC push is idempotent");

        // A NEWER push is accepted.
        let request = make_put_metadata_request_with_hlc(
            "b",
            "k",
            Some(oceanfs_core::proto::common::HlcTimestamp { wall_time: 3000, logical: 0 }),
        );
        let result = service.put_object_metadata(tonic::Request::new(request)).await;
        assert!(result.is_ok(), "a newer push must be accepted");
        let put = metadata.last_put().expect("newer push must persist");
        assert_eq!(put.hlc, Hlc::new(3000, 0));
    }

    /// G6: a repair push older than the tombstone is rejected.
    #[tokio::test]
    async fn put_object_metadata_older_than_tombstone_rejected() {
        let metadata = Arc::new(TombstoneMockMetadata::new());
        metadata
            .delete_object(&BucketId::new("b"), &ObjectKey::new("k"), Hlc::new(2000, 0))
            .unwrap();

        let service = SegmentGrpcService::new(
            Arc::new(TestSegmentStore::new()),
            Some(metadata.clone() as Arc<dyn oceanfs_storage_api::MetadataStore>),
            Arc::new(BufferPool::new(65536, 4)),
            Arc::new(HlcClock::new()),
        );

        let request = make_put_metadata_request_with_hlc(
            "b",
            "k",
            Some(oceanfs_core::proto::common::HlcTimestamp { wall_time: 1000, logical: 0 }),
        );
        let result = service.put_object_metadata(tonic::Request::new(request)).await;
        assert_eq!(
            result.unwrap_err().code(),
            tonic::Code::FailedPrecondition,
            "older push must be rejected",
        );
        assert!(metadata.get_tombstone_value("k").is_some(), "tombstone must remain");
    }

    /// G6: a repair push with an HLC equal to the tombstone is rejected.
    #[tokio::test]
    async fn put_object_metadata_equal_to_tombstone_rejected() {
        let metadata = Arc::new(TombstoneMockMetadata::new());
        metadata
            .delete_object(&BucketId::new("b"), &ObjectKey::new("k"), Hlc::new(1000, 5))
            .unwrap();

        let service = SegmentGrpcService::new(
            Arc::new(TestSegmentStore::new()),
            Some(metadata.clone() as Arc<dyn oceanfs_storage_api::MetadataStore>),
            Arc::new(BufferPool::new(65536, 4)),
            Arc::new(HlcClock::new()),
        );

        let request = make_put_metadata_request_with_hlc(
            "b",
            "k",
            Some(oceanfs_core::proto::common::HlcTimestamp { wall_time: 1000, logical: 5 }),
        );
        let result = service.put_object_metadata(tonic::Request::new(request)).await;
        assert_eq!(
            result.unwrap_err().code(),
            tonic::Code::FailedPrecondition,
            "equal-HLC push must be rejected",
        );
        assert!(metadata.get_tombstone_value("k").is_some(), "tombstone must remain");
    }

    // ── Replicated metadata HLC tests (hlc-causality-closure G3) ────

    /// Metadata mock that records the last `put_object` call.
    struct RecordingMetadata {
        last_put: Mutex<Option<ObjectMetadata>>,
        last_delete: Mutex<Option<Hlc>>,
    }

    impl RecordingMetadata {
        fn new() -> Self {
            Self { last_put: Mutex::new(None), last_delete: Mutex::new(None) }
        }

        fn last_put(&self) -> Option<ObjectMetadata> {
            self.last_put.lock().clone()
        }

        fn last_delete(&self) -> Option<Hlc> {
            *self.last_delete.lock()
        }
    }

    impl oceanfs_storage_api::MetadataStore for RecordingMetadata {
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

        fn put_object(&self, _bucket: &BucketId, meta: ObjectMetadata) -> std::io::Result<()> {
            *self.last_put.lock() = Some(meta);
            Ok(())
        }

        fn delete_object(
            &self,
            _bucket: &BucketId,
            _key: &ObjectKey,
            hlc: Hlc,
        ) -> std::io::Result<()> {
            *self.last_delete.lock() = Some(hlc);
            Ok(())
        }

        fn batch_write(&self, _ops: Vec<oceanfs_storage_api::BatchOp>) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Builds a metadata-bearing append chunk for the G3 tests.
    fn make_append_chunk(
        hlc: Option<oceanfs_core::proto::common::HlcTimestamp>,
    ) -> SegmentAppendRequest {
        SegmentAppendRequest {
            segment_id: Some(SegmentId::new().into()),
            shard_index: None,
            offset: 0,
            data: Bytes::from_static(b"hello"),
            hlc,
            bucket_id: "b".to_string(),
            object_key: "k".to_string(),
            object_size: 5,
            blake3_hash: Bytes::new(),
            chunk_segment_ids: vec![],
            chunk_offsets: vec![],
            chunk_lengths: vec![],
        }
    }

    /// Starts a gRPC server with the given service and returns a client.
    async fn start_server_with(
        service: SegmentGrpcService,
    ) -> (SocketAddr, SegmentRpcClient<tonic::transport::Channel>) {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(SegmentRpcServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let client = SegmentRpcClient::connect(format!("http://{addr}")).await.unwrap();
        (addr, client)
    }

    /// G3: a replicated append persists the coordinator's HLC, not zero.
    #[tokio::test]
    async fn append_segment_persists_coordinator_hlc() {
        let recorded = Arc::new(RecordingMetadata::new());
        let metadata: Arc<dyn oceanfs_storage_api::MetadataStore> = recorded.clone();
        let clock = Arc::new(HlcClock::new());
        let service = SegmentGrpcService::new(
            Arc::new(TestSegmentStore::new()),
            Some(metadata),
            Arc::new(BufferPool::new(65536, 4)),
            Arc::clone(&clock),
        );
        let (_addr, mut client) = start_server_with(service).await;

        let chunk = make_append_chunk(Some(oceanfs_core::proto::common::HlcTimestamp {
            wall_time: 1_234_567,
            logical: 89,
        }));
        let response = client
            .append_segment(tonic::Request::new(tokio_stream::iter(vec![chunk])))
            .await
            .unwrap();
        assert_eq!(response.into_inner().ack, AckStatus::Ok as i32);

        let put = recorded.last_put().expect("replicated metadata must be persisted");
        assert_eq!(put.hlc, Hlc::new(1_234_567, 89), "persisted hlc must equal the coordinator's");
        // The service clock must have merged the remote timestamp (G2).
        assert!(
            clock.now().wall_time() >= 1_234_567,
            "service clock wall must reach the remote wall",
        );
    }

    /// G3: a legacy append without an HLC degrades to zero with a warning.
    #[test]
    fn append_segment_without_hlc_persists_zero_and_warns() {
        // Capture WARN+ events emitted by the service into a buffer.
        struct CaptureWriter(Arc<parking_lot::Mutex<Vec<u8>>>);
        impl std::io::Write for CaptureWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
            type Writer = CaptureWriter;
            fn make_writer(&'a self) -> Self::Writer {
                CaptureWriter(Arc::clone(&self.0))
            }
        }

        let log_buf = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(CaptureWriter(Arc::clone(&log_buf)))
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();

        let recorded = Arc::new(RecordingMetadata::new());
        let metadata: Arc<dyn oceanfs_storage_api::MetadataStore> = recorded.clone();

        tracing::subscriber::with_default(subscriber, || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let service = SegmentGrpcService::new(
                    Arc::new(TestSegmentStore::new()),
                    Some(metadata),
                    Arc::new(BufferPool::new(65536, 4)),
                    Arc::new(HlcClock::new()),
                );
                let (_addr, mut client) = start_server_with(service).await;
                let response = client
                    .append_segment(tonic::Request::new(tokio_stream::iter(vec![
                        make_append_chunk(None),
                    ])))
                    .await
                    .unwrap();
                assert_eq!(response.into_inner().ack, AckStatus::Ok as i32);
            });
        });

        let put = recorded.last_put().expect("replicated metadata must be persisted");
        assert_eq!(put.hlc, Hlc::zero(), "missing HLC must degrade to zero");
        let logs = String::from_utf8(log_buf.lock().clone()).unwrap();
        assert!(logs.contains("has no HLC"), "expected a warning about the missing HLC: {logs}");
    }

    /// G4/G8: a remote delete carries the coordinator's HLC to the store.
    #[tokio::test]
    async fn delete_object_handler_passes_hlc_to_store() {
        let recorded = Arc::new(RecordingMetadata::new());
        let metadata: Arc<dyn oceanfs_storage_api::MetadataStore> = recorded.clone();
        let clock = Arc::new(HlcClock::new());
        let service = SegmentGrpcService::new(
            Arc::new(TestSegmentStore::new()),
            Some(metadata),
            Arc::new(BufferPool::new(65536, 4)),
            Arc::clone(&clock),
        );

        let request = tonic::Request::new(DeleteObjectRequest {
            bucket_id: "b".to_string(),
            object_key: "k".to_string(),
            hlc: Some(oceanfs_core::proto::common::HlcTimestamp {
                wall_time: 2_222_222,
                logical: 9,
            }),
        });
        let response = service.delete_object(request).await.unwrap();
        assert!(response.into_inner().deleted);

        assert_eq!(
            recorded.last_delete(),
            Some(Hlc::new(2_222_222, 9)),
            "the store must receive the delete's HLC from the request",
        );
        // The service clock must have merged the remote timestamp (G2).
        assert!(clock.now().wall_time() >= 2_222_222, "clock must merge the remote delete HLC");
    }
}
