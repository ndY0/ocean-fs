//! Integration test: read repair — gRPC metadata push + fetch round-trip.
//!
//! Verifies §4.2: the `PutObjectMetadata` and `GetObjectMetadata` RPCs
//! work end-to-end through a real gRPC server backed by RocksDB. This
//! validates the data-plane of read repair: corrected metadata pushed
//! to a stale replica is persisted and subsequently readable.
//!
//! ## Test Flow
//!
//! 1. Start a gRPC server with SegmentGrpcService + RocksDB metadata store.
//! 2. Push object metadata via gRPC PutObjectMetadata.
//! 3. Fetch the same object via gRPC GetObjectMetadata.
//! 4. Verify the HLC and inline data match the pushed values.
//! 5. Push a newer version, fetch again, verify the newer version wins.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use oceanfs_core::{
    proto::segment::{
        GetObjectMetadataRequest, GetObjectMetadataResponse, PutObjectMetadataRequest,
    },
    BucketId, ObjectKey, SegmentId,
};
use oceanfs_durability::SegmentDataStore;
use oceanfs_server::grpc::segment_service::SegmentGrpcService;
use oceanfs_storage::{
    BufferPool, Error as StorageError, RocksDbMetadataStore, SegmentRpcClient, SegmentRpcServer,
};
use tonic::transport::Server;

// In-memory segment data store for testing.
struct InMemorySegments {
    data: parking_lot::Mutex<std::collections::HashMap<SegmentId, Bytes>>,
}

impl InMemorySegments {
    fn new() -> Self {
        Self { data: parking_lot::Mutex::new(std::collections::HashMap::new()) }
    }
}

impl SegmentDataStore for InMemorySegments {
    fn write_segment_data(&self, segment_id: &SegmentId, data: &[u8]) -> Result<(), StorageError> {
        self.data.lock().insert(*segment_id, Bytes::from(data.to_vec()));
        Ok(())
    }

    fn read_segment_data(&self, segment_id: &SegmentId) -> Result<Bytes, StorageError> {
        self.data
            .lock()
            .get(segment_id)
            .cloned()
            .ok_or_else(|| StorageError::SegmentNotFound(*segment_id))
    }
}

/// Starts a gRPC server with SegmentGrpcService and returns a client.
async fn start_server(
    metadata: Arc<RocksDbMetadataStore>,
    data_store: Arc<dyn SegmentDataStore>,
) -> (SocketAddr, SegmentRpcClient<tonic::transport::Channel>) {
    let svc =
        SegmentGrpcService::new(data_store, Some(metadata), Arc::new(BufferPool::new(65536, 1024)));
    let router = Server::builder().add_service(SegmentRpcServer::new(svc));

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let bound_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        router
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{bound_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    (bound_addr, SegmentRpcClient::new(channel))
}

fn make_rocks_metadata() -> Arc<RocksDbMetadataStore> {
    let tmp = tempfile::tempdir().unwrap();
    Arc::new(
        RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
            data_dir: tmp.path().join("meta"),
            block_cache_size: 1024,
            memtable_size: 1024,
            ..Default::default()
        })
        .unwrap(),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_get_metadata_roundtrip() {
    let metadata = make_rocks_metadata();
    let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegments::new());
    let (_addr, mut client) = start_server(metadata.clone(), data_store.clone()).await;

    let bucket = BucketId::new("test");
    let key = ObjectKey::new("obj1");

    // Push object metadata via gRPC.
    let hlc_proto = oceanfs_core::proto::common::HlcTimestamp { wall_time: 1000, logical: 5 };
    let push_req = tonic::Request::new(PutObjectMetadataRequest {
        bucket_id: bucket.as_str().to_string(),
        object_key: key.as_str().to_string(),
        size: 5,
        blake3_hash: vec![].into(),
        hlc: Some(hlc_proto),
        inline_data: b"hello".to_vec().into(),
        chunk_segment_ids: vec![].into(),
        chunk_offsets: vec![].into(),
        chunk_lengths: vec![].into(),
    });
    let push_resp = client.put_object_metadata(push_req).await.unwrap();
    assert!(push_resp.into_inner().written);

    // Fetch the same object via gRPC.
    let fetch_req = tonic::Request::new(GetObjectMetadataRequest {
        bucket_id: bucket.as_str().to_string(),
        object_key: key.as_str().to_string(),
    });
    let fetch_resp = client.get_object_metadata(fetch_req).await.unwrap();
    let meta: GetObjectMetadataResponse = fetch_resp.into_inner();

    assert!(meta.found);
    assert_eq!(meta.size, 5);
    assert_eq!(meta.inline_data.as_ref(), b"hello" as &[u8]);
    assert_eq!(meta.hlc.unwrap().wall_time, 1000);
    assert_eq!(meta.hlc.unwrap().logical, 5);
}

#[tokio::test]
async fn put_overwrites_stale_version() {
    let metadata = make_rocks_metadata();
    let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegments::new());
    let (_addr, mut client) = start_server(metadata.clone(), data_store.clone()).await;

    let bucket = BucketId::new("test");
    let key = ObjectKey::new("obj2");

    // Push v1 (HLC=500).
    let hlc1 = oceanfs_core::proto::common::HlcTimestamp { wall_time: 500, logical: 0 };
    client
        .put_object_metadata(tonic::Request::new(PutObjectMetadataRequest {
            bucket_id: bucket.as_str().to_string(),
            object_key: key.as_str().to_string(),
            size: 5,
            blake3_hash: vec![].into(),
            hlc: Some(hlc1),
            inline_data: vec![].into(),
            chunk_segment_ids: vec![].into(),
            chunk_offsets: vec![].into(),
            chunk_lengths: vec![].into(),
        }))
        .await
        .unwrap();

    // Push v2 (HLC=2000) — should overwrite v1.
    let hlc2 = oceanfs_core::proto::common::HlcTimestamp { wall_time: 2000, logical: 1 };
    client
        .put_object_metadata(tonic::Request::new(PutObjectMetadataRequest {
            bucket_id: bucket.as_str().to_string(),
            object_key: key.as_str().to_string(),
            size: 5,
            blake3_hash: vec![].into(),
            hlc: Some(hlc2),
            inline_data: b"v2000".to_vec().into(),
            chunk_segment_ids: vec![].into(),
            chunk_offsets: vec![].into(),
            chunk_lengths: vec![].into(),
        }))
        .await
        .unwrap();

    // Fetch — should return v2 (the last write).
    let fetch_req = tonic::Request::new(GetObjectMetadataRequest {
        bucket_id: bucket.as_str().to_string(),
        object_key: key.as_str().to_string(),
    });
    let meta = client.get_object_metadata(fetch_req).await.unwrap().into_inner();

    assert!(meta.found);
    assert_eq!(meta.inline_data.as_ref(), b"v2000" as &[u8]);
    assert_eq!(meta.hlc.unwrap().wall_time, 2000);
    assert_eq!(meta.hlc.unwrap().logical, 1);
}

#[tokio::test]
async fn get_nonexistent_object_returns_not_found() {
    let metadata = make_rocks_metadata();
    let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegments::new());
    let (_addr, mut client) = start_server(metadata.clone(), data_store.clone()).await;

    let fetch_req = tonic::Request::new(GetObjectMetadataRequest {
        bucket_id: "nonexistent".to_string(),
        object_key: "nope".to_string(),
    });
    let meta = client.get_object_metadata(fetch_req).await.unwrap().into_inner();
    assert!(!meta.found);
}

#[tokio::test]
async fn put_object_with_chunks_roundtrip() {
    let metadata = make_rocks_metadata();
    let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegments::new());
    let (_addr, mut client) = start_server(metadata.clone(), data_store.clone()).await;

    let bucket = BucketId::new("test");
    let key = ObjectKey::new("chunked");
    let seg_id = SegmentId::new();
    let proto_sid: oceanfs_core::proto::common::SegmentId = seg_id.into();

    let hlc = oceanfs_core::proto::common::HlcTimestamp { wall_time: 3000, logical: 2 };

    client
        .put_object_metadata(tonic::Request::new(PutObjectMetadataRequest {
            bucket_id: bucket.as_str().to_string(),
            object_key: key.as_str().to_string(),
            size: 200,
            blake3_hash: vec![0xABu8; 32].into(),
            hlc: Some(hlc),
            inline_data: vec![].into(),
            chunk_segment_ids: vec![proto_sid],
            chunk_offsets: vec![100],
            chunk_lengths: vec![200],
        }))
        .await
        .unwrap();

    let meta = client
        .get_object_metadata(tonic::Request::new(GetObjectMetadataRequest {
            bucket_id: bucket.as_str().to_string(),
            object_key: key.as_str().to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(meta.found);
    assert_eq!(meta.size, 200);
    assert_eq!(meta.blake3_hash, vec![0xABu8; 32]);
    assert_eq!(meta.chunk_offsets, vec![100]);
    assert_eq!(meta.chunk_lengths, vec![200]);
    assert_eq!(meta.hlc.unwrap().wall_time, 3000);
}
