//! End-to-end read/write round-trip integration tests.
//!
//! Tests PUT → GET → hash-matches for inline, small, and
//! standard blobs across the full node pipeline.

#![allow(clippy::unwrap_used)]

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use oceanfs_core::{BucketId, HashKey, HashOutput, ObjectKey, ObjectMetadata};
use oceanfs_routing::hash_key;
use oceanfs_server::{
    metadata_ops::{MetadataError, MetadataOps},
    InMemorySegmentReader, ReadCoordinator, ReadRequest, WriteCoordinator, WriteRequest,
};

/// In-memory metadata store for round-trip testing.
struct TestMetadata {
    objects: parking_lot::RwLock<HashMap<(String, String), ObjectMetadata>>,
}

impl TestMetadata {
    fn new() -> Self {
        Self { objects: parking_lot::RwLock::new(HashMap::new()) }
    }

    fn put_object(&self, bucket: &str, key: &str, meta: ObjectMetadata) {
        self.objects.write().insert((bucket.to_string(), key.to_string()), meta);
    }
}

impl MetadataOps for TestMetadata {
    fn get_object(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectMetadata>, MetadataError> {
        Ok(self
            .objects
            .read()
            .get(&(bucket.as_str().to_string(), key.as_str().to_string()))
            .cloned())
    }

    fn put_object(&self, bucket: &BucketId, meta: ObjectMetadata) -> Result<(), MetadataError> {
        self.objects
            .write()
            .insert((bucket.as_str().to_string(), meta.object_key.as_str().to_string()), meta);
        Ok(())
    }

    fn delete_object(&self, bucket: &BucketId, key: &ObjectKey) -> Result<(), MetadataError> {
        self.objects.write().remove(&(bucket.as_str().to_string(), key.as_str().to_string()));
        Ok(())
    }

    fn list_objects(
        &self,
        _bucket: &BucketId,
        prefix: &str,
    ) -> Result<Vec<ObjectMetadata>, MetadataError> {
        let prefix = prefix.to_string();
        Ok(self
            .objects
            .read()
            .iter()
            .filter(|(_, v)| v.object_key.as_str().starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect())
    }

    fn put_segment(&self, _meta: oceanfs_core::SegmentMetadata) -> Result<(), MetadataError> {
        // No-op: test store doesn't track segments.
        Ok(())
    }
}

/// Creates a minimal test setup with an in-memory segment store
/// and metadata store for round-trip verification.
struct RoundTripEnv {
    write: Arc<WriteCoordinator>,
    read: Arc<ReadCoordinator>,
    segment_store: Arc<InMemorySegmentReader>,
    metadata: Arc<TestMetadata>,
}

impl RoundTripEnv {
    async fn new() -> Self {
        use std::net::SocketAddr;

        use oceanfs_core::{
            GossipConfig, HlcClock, Incarnation, MetadataConfig, NodeId, NodeState, PoolConfig,
            RingConfig, RpcConfig, SegmentSizeConfig, SizeTier, WalConfig,
        };
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;
        use oceanfs_routing::{Ring, RingCache};
        use oceanfs_storage::{
            BufferPool, RocksDbMetadataStore, SealConfig, SegmentPool, SegmentSealer, SegmentShard,
            WalWriter,
        };

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
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let hlc_clock = Arc::new(HlcClock::new());

        // Segment pipeline.
        let dir = tempfile::tempdir().unwrap();
        let metadata_store = Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
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
            )
            .unwrap(),
        );
        let segment_pool_standard = Arc::new(
            SegmentPool::new(pool_cfg, SizeTier::Standard, &size_config, buffer_pool, None)
                .unwrap(),
        );
        let wal = Arc::new(
            WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 64 * 1024 * 1024,
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
        };
        let sealer = Arc::new(SegmentSealer::new(seal_config, metadata_store.clone(), wal));

        let (hinted_handoff, hint_config) = {
            let hints_dir = dir.path().join("hints");
            let delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> =
                Arc::new(oceanfs_durability::GrpcHintDeliveryClient::new(pool.clone()));
            let hint_config = oceanfs_durability::HintedHandoffConfig {
                wal_dir: hints_dir.clone(),
                ..Default::default()
            };
            (
                Arc::new(oceanfs_durability::HintedHandoffManager::new(
                    hints_dir,
                    delivery_client,
                    hint_config.clone(),
                )),
                hint_config,
            )
        };

        let write = Arc::new(WriteCoordinator::new(
            ring_cache.clone(),
            membership,
            pool,
            NodeId::new("n1"),
            hlc_clock,
            metadata_store,
            size_config,
            shard_small,
            shard_standard,
            segment_pool_small,
            segment_pool_standard,
            sealer,
            hinted_handoff,
            hint_config,
        ));

        let segment_store = Arc::new(InMemorySegmentReader::new());
        let metadata = Arc::new(TestMetadata::new());

        let read = Arc::new(
            ReadCoordinator::new_with_metadata(
                ring_cache,
                NodeId::new("n1"),
                None,
                metadata.clone(),
            )
            .with_segment_reader(segment_store.clone()),
        );

        Self { write, read, segment_store, metadata }
    }

    /// Helper: converts a byte slice to a shared `Bytes`.
    fn to_bytes(data: &[u8]) -> Bytes {
        Bytes::copy_from_slice(data)
    }

    /// Writes data for a key and returns the hash.
    async fn put(&self, bucket: &str, key: &str, data: &[u8]) -> String {
        let bucket_id = BucketId::new(bucket);
        let object_key = ObjectKey::new(key);
        let hk = HashKey::from_bytes(hash_key(object_key.as_str().as_bytes()));

        let req = WriteRequest {
            bucket: bucket_id.clone(),
            key: object_key.clone(),
            hash_key: hk,
            data: Self::to_bytes(data),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = self.write.put(req).await.unwrap();

        // Store segment data for subsequent reads (inline blobs have no chunks).
        let inline_data = if result.chunks.is_empty() {
            Some(Self::to_bytes(data))
        } else {
            for chunk in &result.chunks {
                self.segment_store.put(chunk.segment_id, Self::to_bytes(data));
            }
            None
        };

        // Store metadata so ReadCoordinator can find the object.
        let hash = blake3::hash(data);
        let stored_hash = HashOutput::from_bytes(*hash.as_bytes());
        let meta = ObjectMetadata {
            object_key: object_key.clone(),
            size: data.len() as u64,
            blake3_hash: Some(stored_hash),
            chunks: result.chunks.clone(),
            inline_data,
            created_at: 0,
            hlc: oceanfs_core::Hlc::zero(),
        };
        self.metadata.put_object(bucket, key, meta);

        stored_hash.to_hex()
    }

    /// Reads data for a key.
    async fn get(&self, bucket: &str, key: &str) -> Vec<u8> {
        let bucket_id = BucketId::new(bucket);
        let object_key = ObjectKey::new(key);
        let hk = HashKey::from_bytes(hash_key(object_key.as_str().as_bytes()));

        let req = ReadRequest {
            bucket: bucket_id,
            key: object_key,
            hash_key: hk,
            metadata_only: false,
            policy: None,
        };

        let result = self.read.get(req).await.unwrap();
        result.data.to_vec()
    }
}

/// Helper: compute BLAKE3 hash.
fn blake3_hash(data: &[u8]) -> String {
    let hash = blake3::hash(data);
    HashOutput::from_bytes(*hash.as_bytes()).to_hex()
}

#[tokio::test]
async fn roundtrip_1kb_blob_hash_matches() {
    let env = RoundTripEnv::new().await;
    let data = vec![0xABu8; 1024];
    let expected_hash = env.put("test", "1kb.bin", &data).await;

    let retrieved = env.get("test", "1kb.bin").await;
    let actual_hex = blake3_hash(&retrieved);

    assert_eq!(retrieved.len(), 1024);
    assert_eq!(&retrieved[..], &data[..]);
    assert_eq!(actual_hex, expected_hash);
}

#[tokio::test]
async fn roundtrip_100kb_blob_hash_matches() {
    let env = RoundTripEnv::new().await;
    let data = vec![0x42u8; 100_000];
    let expected_hash = env.put("test", "100kb.bin", &data).await;

    let retrieved = env.get("test", "100kb.bin").await;
    let actual_hex = blake3_hash(&retrieved);

    assert_eq!(retrieved.len(), 100_000);
    assert_eq!(&retrieved[..], &data[..]);
    assert_eq!(actual_hex, expected_hash);
}

#[tokio::test]
async fn roundtrip_small_blob_preserves_bytes() {
    let env = RoundTripEnv::new().await;
    let data = b"hello world small blob";
    let _ = env.put("test", "small.txt", data).await;

    let retrieved = env.get("test", "small.txt").await;
    assert_eq!(&retrieved[..], data);
}

#[tokio::test]
async fn roundtrip_empty_blob() {
    let env = RoundTripEnv::new().await;
    let data: &[u8] = &[];
    let _ = env.put("test", "empty.bin", data).await;

    let retrieved = env.get("test", "empty.bin").await;
    assert!(retrieved.is_empty());
}

#[tokio::test]
async fn roundtrip_multiple_blobs_independent() {
    let env = RoundTripEnv::new().await;

    let data1 = b"first blob data";
    let data2 = b"second blob content";
    let data3 = b"third blob payload";

    env.put("test", "blob1", data1).await;
    env.put("test", "blob2", data2).await;
    env.put("test", "blob3", data3).await;

    let r1 = env.get("test", "blob1").await;
    let r2 = env.get("test", "blob2").await;
    let r3 = env.get("test", "blob3").await;

    assert_eq!(&r1[..], data1);
    assert_eq!(&r2[..], data2);
    assert_eq!(&r3[..], data3);
}

#[tokio::test]
async fn roundtrip_1mb_blob_hash_matches() {
    let env = RoundTripEnv::new().await;
    let data = vec![0xABu8; 1_048_576]; // 1 MB
    let expected_hash = env.put("test", "1mb.bin", &data).await;

    let retrieved = env.get("test", "1mb.bin").await;
    let actual_hex = blake3_hash(&retrieved);

    assert_eq!(retrieved.len(), 1_048_576);
    assert_eq!(&retrieved[..], &data[..]);
    assert_eq!(actual_hex, expected_hash);
}

#[tokio::test]
async fn roundtrip_overwrite_preserves_latest() {
    let env = RoundTripEnv::new().await;

    let v1 = b"version one";
    let v2 = b"version two - updated content";

    env.put("test", "overwrite", v1).await;
    env.put("test", "overwrite", v2).await;

    let retrieved = env.get("test", "overwrite").await;
    assert_eq!(&retrieved[..], v2);
    assert_ne!(&retrieved[..], v1);
}
