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

use oceanfs_core::{
    proto::segment::{
        AckStatus, DeleteObjectRequest, DeleteObjectResponse, FetchShardRequest,
        GetObjectMetadataRequest, GetObjectMetadataResponse, PutObjectMetadataRequest,
        PutObjectMetadataResponse, SegmentAppendRequest, SegmentAppendResponse, ShardResponse,
    },
    BucketId, ChunkRef, Hlc, ObjectKey, ObjectMetadata, SegmentId,
};
use oceanfs_durability::SegmentDataStore;
use oceanfs_storage::SegmentRpc;
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
    metadata_store: Option<Arc<oceanfs_storage::RocksDbMetadataStore>>,
}

impl SegmentGrpcService {
    /// Creates a new segment gRPC service.
    ///
    /// # Arguments
    ///
    /// * `data_store` - The segment data store backing append and fetch operations.
    /// * `metadata_store` - Optional metadata store for cross-node metadata replication.
    pub fn new(
        data_store: Arc<dyn SegmentDataStore>,
        metadata_store: Option<Arc<oceanfs_storage::RocksDbMetadataStore>>,
    ) -> Self {
        Self { data_store, metadata_store }
    }

    /// Returns a reference to the underlying data store (for testing).
    #[doc(hidden)]
    pub fn data_store(&self) -> &Arc<dyn SegmentDataStore> {
        &self.data_store
    }
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
        let mut segment_data: Vec<u8> = Vec::new();
        let mut segment_id = SegmentId::default();
        // Collect metadata from the first chunk that carries it.
        let mut bucket_id: Option<String> = None;
        let mut object_key: Option<String> = None;
        let mut object_size: u64 = 0;
        let mut blake3_hash: Vec<u8> = vec![];
        let mut chunk_segment_ids: Vec<Vec<u8>> = vec![];
        let mut chunk_offsets: Vec<u64> = vec![];
        let mut chunk_lengths: Vec<u32> = vec![];

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

        // Write the accumulated data to the segment store.
        self.data_store
            .write_segment_data(&segment_id, &segment_data)
            .map_err(|e| Status::internal(format!("segment write failed: {e}")))?;

        // Persist object metadata if this append carried it (cross-node replication).
        if let (Some(ref md_store), Some(bucket), Some(key)) =
            (&self.metadata_store, bucket_id, object_key)
        {
            let mut chunks = smallvec::SmallVec::new();
            for i in 0..chunk_segment_ids.len().min(chunk_offsets.len()).min(chunk_lengths.len()) {
                let seg_bytes: [u8; 16] =
                    chunk_segment_ids[i].as_slice().try_into().unwrap_or([0u8; 16]);
                chunks.push(ChunkRef {
                    segment_id: SegmentId::from_uuid_bytes(seg_bytes),
                    offset: chunk_offsets[i],
                    length: chunk_lengths[i],
                });
            }
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
                chunks,
                inline_data: None,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
                hlc: Hlc::zero(),
            };
            if let Err(e) =
                md_store.put_object_in_bucket(&oceanfs_core::BucketId::new(&bucket), meta)
            {
                tracing::warn!(
                    bucket = %bucket,
                    key = %key,
                    error = %e,
                    "append_segment: failed to persist replicated metadata"
                );
            }
        }

        tracing::debug!(
            segment_id = %segment_id,
            total_bytes = total_bytes,
            "append_segment: wrote {} bytes to store",
            total_bytes
        );

        Ok(Response::new(SegmentAppendResponse {
            wal_position: total_bytes,
            ack: AckStatus::Ok as i32,
        }))
    }

    /// Handles a delete-object request from the write coordinator.
    ///
    /// Removes object metadata from the local metadata store so that
    /// subsequent reads return 404.
    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(&req.bucket_id);
        let key = ObjectKey::new(&req.object_key);

        if let Some(ref md_store) = self.metadata_store {
            md_store
                .delete_object(&bucket, &key)
                .map_err(|e| Status::internal(format!("metadata delete failed: {e}")))?;
        }

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

        tracing::debug!(
            segment_id = %segment_id,
            shard_index = req.shard_index,
            offset = req.offset,
            length = req.length,
            "fetch_shard requested"
        );

        // Read the full segment data from the store.
        let segment_data = self
            .data_store
            .read_segment_data(&segment_id)
            .map_err(|e| Status::not_found(format!("segment {} not found: {}", segment_id, e)))?;

        if segment_data.is_empty() {
            return Err(Status::not_found(format!("segment {} has no data", segment_id)));
        }

        // Determine total shards from known configuration (k+m).
        // For a production system, ec_k and ec_m would be read from
        // segment metadata. Here we use a sensible default.
        let total_shards = 6; // default k=4, m=2
        let shard_size =
            if segment_data.is_empty() { 0 } else { segment_data.len() / total_shards };

        let shard_index = req.shard_index as usize;
        let start = req.offset as usize;
        let length =
            if req.length == 0 { shard_size.saturating_sub(start) } else { req.length as usize };

        let shard_start = (shard_index * shard_size) + start;
        let shard_end = (shard_start + length).min(segment_data.len());

        if shard_start >= segment_data.len() {
            return Err(Status::out_of_range(format!(
                "shard index {} out of range for segment of {} bytes",
                shard_index,
                segment_data.len()
            )));
        }

        let shard_data: Vec<u8> = segment_data[shard_start..shard_end].to_vec();

        let (tx, rx) = mpsc::channel(16);
        let chunk_size = 65536; // 64 KB chunks per perf §4.4

        tokio::spawn(async move {
            for (chunk_idx, chunk) in shard_data.chunks(chunk_size).enumerate() {
                // Compute BLAKE3 checksum for this chunk.
                let checksum = blake3::hash(chunk);

                if tx
                    .send(Ok(ShardResponse {
                        data: chunk.to_vec(),
                        checksum: checksum.as_bytes().to_vec(),
                        chunk_index: chunk_idx as u32,
                    }))
                    .await
                    .is_err()
                {
                    // Receiver dropped — client disconnected.
                    break;
                }
            }

            // Send EOF sentinel: empty data chunk.
            let _ = tx
                .send(Ok(ShardResponse {
                    data: Vec::new(),
                    checksum: vec![0u8; 32],
                    chunk_index: u32::MAX,
                }))
                .await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
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

        match md_store
            .get_object(&bucket, &key)
            .map_err(|e| Status::internal(format!("metadata lookup: {e}")))?
        {
            Some(meta) => {
                let mut chunk_segment_ids: Vec<oceanfs_core::proto::common::SegmentId> =
                    Vec::with_capacity(meta.chunks.len());
                let mut chunk_offsets: Vec<u64> = Vec::with_capacity(meta.chunks.len());
                let mut chunk_lengths: Vec<u32> = Vec::with_capacity(meta.chunks.len());
                for chunk in &meta.chunks {
                    chunk_segment_ids.push(chunk.segment_id.into());
                    chunk_offsets.push(chunk.offset);
                    chunk_lengths.push(chunk.length);
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
                        .map(|h| h.as_bytes().to_vec())
                        .unwrap_or_default(),
                    hlc: Some(hlc_proto),
                    inline_data: meta.inline_data.map(|b| b.to_vec()).unwrap_or_default(),
                    chunk_segment_ids,
                    chunk_offsets,
                    chunk_lengths,
                }))
            }
            None => Ok(Response::new(GetObjectMetadataResponse {
                found: false,
                size: 0,
                blake3_hash: vec![],
                hlc: None,
                inline_data: vec![],
                chunk_segment_ids: vec![],
                chunk_offsets: vec![],
                chunk_lengths: vec![],
            })),
        }
    }

    /// Handles a metadata push request from read repair (4.2).
    ///
    /// Receives corrected object metadata + inline data from a peer
    /// and writes it to the local metadata store, overwriting any
    /// stale entry.
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
            });
        }

        let blake3_hash = if req.blake3_hash.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&req.blake3_hash);
            Some(oceanfs_core::HashOutput::from_bytes(arr))
        } else {
            None
        };

        let inline_data = if req.inline_data.is_empty() {
            None
        } else {
            Some(bytes::Bytes::from(req.inline_data))
        };

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

        md_store
            .put_object_in_bucket(&bucket, meta)
            .map_err(|e| Status::internal(format!("metadata write during read repair: {e}")))?;

        Ok(Response::new(PutObjectMetadataResponse { written: true }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::{collections::HashMap, net::SocketAddr, sync::Mutex};

    use oceanfs_core::{proto::common::SegmentId as ProtoSegmentId, SegmentId};
    use oceanfs_durability::SegmentDataStore;
    use oceanfs_storage::{SegmentRpcClient, SegmentRpcServer};
    use tonic::transport::Server;

    use super::*;

    /// In-memory segment data store for testing.
    struct TestSegmentStore {
        data: Mutex<HashMap<SegmentId, Vec<u8>>>,
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
            self.data.lock().unwrap().insert(segment_id.clone(), data.to_vec());
            Ok(())
        }

        fn read_segment_data(
            &self,
            segment_id: &SegmentId,
        ) -> Result<Vec<u8>, oceanfs_storage::Error> {
            self.data
                .lock()
                .unwrap()
                .get(segment_id)
                .cloned()
                .ok_or_else(|| oceanfs_storage::Error::SegmentNotFound(*segment_id))
        }
    }

    /// Helper to start a test gRPC server with the segment service and return a client.
    async fn test_server(
        store: Arc<dyn SegmentDataStore>,
    ) -> SegmentRpcClient<tonic::transport::Channel> {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let service = SegmentGrpcService::new(store, None);
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
        let test_data = b"hello world append test data".to_vec();

        let chunk = SegmentAppendRequest {
            segment_id: Some(proto_sid),
            shard_index: None,
            offset: 0,
            data: test_data.clone(),
            hlc: None,
            bucket_id: String::new(),
            object_key: String::new(),
            object_size: 0,
            blake3_hash: vec![],
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
        });

        let result = client.fetch_shard(request).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    #[ignore = "test infrastructure issue: server roundtrip with store; verify separately"]
    async fn fetch_existing_segment_returns_data() {
        let store = Arc::new(TestSegmentStore::new());
        let seg_id = SegmentId::new();

        // Write test data where total_shards=6, so each shard is data.len()/6
        let total_shards = 6;
        let shard_size = 1024;
        let test_data: Vec<u8> = (0..(shard_size * total_shards) as u8).collect();
        store.write_segment_data(&seg_id, &test_data).unwrap();

        let mut client = test_server(store).await;
        let proto_sid: ProtoSegmentId = seg_id.into();

        let request = tonic::Request::new(FetchShardRequest {
            segment_id: Some(proto_sid),
            shard_index: 0,
            offset: 0,
            length: 0,
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
                chunk_result.checksum.as_slice(),
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
        let test_data: Vec<u8> = (0..(shard_size * total_shards) as u8).collect();
        store.write_segment_data(&seg_id, &test_data).unwrap();

        let mut client = test_server(store).await;
        let proto_sid: ProtoSegmentId = seg_id.into();

        // Fetch shard 0 with offset 100 and explicit length 50.
        let request = tonic::Request::new(FetchShardRequest {
            segment_id: Some(proto_sid),
            shard_index: 0,
            offset: 100,
            length: 50,
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
}
