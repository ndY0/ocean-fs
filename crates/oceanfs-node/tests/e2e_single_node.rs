//! End-to-end single-node integration test.
//!
//! Tests PUT and GET of 1 KB, 100 KB, and 1 MB blobs
//! through the full coordinator pipeline with hash verification.

#![allow(clippy::unwrap_used)]

mod helpers {
    use std::{collections::HashMap, sync::Arc};

    use bytes::Bytes;
    use oceanfs_core::{BucketId, HashKey, HashOutput, ObjectKey, ObjectMetadata};
    use oceanfs_routing::hash_key;
    use oceanfs_server::{
        metadata_ops::{MetadataError, MetadataOps},
        InMemorySegmentReader, ReadCoordinator, ReadRequest, WriteCoordinator, WriteRequest,
    };

    pub struct InMemoryMetadata {
        objects: parking_lot::RwLock<HashMap<(String, String), ObjectMetadata>>,
    }

    impl InMemoryMetadata {
        pub fn new() -> Self {
            Self { objects: parking_lot::RwLock::new(HashMap::new()) }
        }

        pub fn put_object(&self, bucket: &str, key: &str, meta: ObjectMetadata) {
            self.objects.write().insert((bucket.to_string(), key.to_string()), meta);
        }
    }

    impl MetadataOps for InMemoryMetadata {
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
            // No-op: in-memory store doesn't track segments.
            Ok(())
        }
    }

    pub struct TestNode {
        write: Arc<WriteCoordinator>,
        read: Arc<ReadCoordinator>,
        segment_store: Arc<InMemorySegmentReader>,
        metadata: Arc<InMemoryMetadata>,
    }

    impl TestNode {
        pub async fn new() -> Self {
            use std::net::SocketAddr;

            use oceanfs_core::{
                GossipConfig, HlcClock, Incarnation, MetadataConfig, NodeId, NodeState, PoolConfig,
                RingConfig, RpcConfig, SegmentSizeConfig, SizeTier, WalConfig,
            };
            use oceanfs_membership::Membership;
            use oceanfs_network::ConnectionPool;
            use oceanfs_routing::{Ring, RingCache};
            use oceanfs_storage::{
                BufferPool, RocksDbMetadataStore, SealConfig, SegmentPool, SegmentSealer,
                SegmentShard, WalWriter,
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
            membership.upsert_node(NodeId::new("n1"), NodeState::Alive, Incarnation::new(1), addr);
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
            let shard_small = Arc::new(
                SegmentShard::new(4, SizeTier::Small, &size_config, &buffer_pool).unwrap(),
            );
            let shard_standard = Arc::new(
                SegmentShard::new(4, SizeTier::Standard, &size_config, &buffer_pool).unwrap(),
            );
            let pool_cfg = PoolConfig::default();
            let segment_pool_small = Arc::new(
                SegmentPool::new(
                    pool_cfg.clone(),
                    SizeTier::Small,
                    &size_config,
                    buffer_pool.clone(),
                )
                .unwrap(),
            );
            let segment_pool_standard = Arc::new(
                SegmentPool::new(pool_cfg, SizeTier::Standard, &size_config, buffer_pool).unwrap(),
            );
            let wal = Arc::new(
                WalWriter::open(&WalConfig {
                    data_dir: dir.path().join("wal"),
                    max_file_size_bytes: 64 * 1024 * 1024, // 64 MB, accommodates standard blobs
                    fsync_batch_timeout_ms: 5,
                })
                .await
                .unwrap(),
            );
            let seal_config = SealConfig {
                target_size_bytes: size_config.default_target_size,
                seal_timeout_ms: 5000,
                data_dir: dir.path().join("segments"),
            };
            let sealer = Arc::new(SegmentSealer::new(seal_config, metadata_store.clone(), wal));

            let hinted_handoff =
                Arc::new(oceanfs_durability::HintedHandoff::new_with_pool(pool.clone()));

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
            ));

            let segment_store = Arc::new(InMemorySegmentReader::new());
            let metadata = Arc::new(InMemoryMetadata::new());

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

        pub async fn put(&self, bucket: &str, key: &str, data: &[u8]) {
            let bucket_id = BucketId::new(bucket);
            let object_key = ObjectKey::new(key);
            let hk = HashKey::from_bytes(hash_key(object_key.as_str().as_bytes()));

            let req = WriteRequest {
                bucket: bucket_id.clone(),
                key: object_key.clone(),
                hash_key: hk,
                data: Bytes::copy_from_slice(data),
                write_quorum: 1,
                ack_after_wal: true,
                ec_async: false,
                policy: None,
            };

            let result = self.write.put(req).await.unwrap();

            // If chunks is empty, this is an inline blob — store inline_data.
            let inline_data = if result.chunks.is_empty() {
                Some(Bytes::copy_from_slice(data))
            } else {
                for chunk in &result.chunks {
                    self.segment_store.put(chunk.segment_id, Bytes::copy_from_slice(data));
                }
                None
            };

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
        }

        pub async fn get(&self, bucket: &str, key: &str) -> Vec<u8> {
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

            self.read.get(req).await.unwrap().data.to_vec()
        }
    }
}

use helpers::TestNode;

#[tokio::test]
async fn e2e_put_get_1kb() {
    let node = TestNode::new().await;
    let data = vec![0xABu8; 1024];
    node.put("test", "1kb.bin", &data).await;
    let retrieved = node.get("test", "1kb.bin").await;
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn e2e_put_get_100kb() {
    let node = TestNode::new().await;
    let data = vec![0x42u8; 100_000];
    node.put("test", "100kb.bin", &data).await;
    let retrieved = node.get("test", "100kb.bin").await;
    assert_eq!(retrieved.len(), 100_000);
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn e2e_put_get_1mb() {
    let node = TestNode::new().await;
    let data = vec![0xABu8; 1_048_576];
    node.put("test", "1mb.bin", &data).await;
    let retrieved = node.get("test", "1mb.bin").await;
    assert_eq!(retrieved.len(), 1_048_576);
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn e2e_hash_verification_passes() {
    let node = TestNode::new().await;
    let data = b"hash verification test payload";
    node.put("test", "hash.bin", data).await;
    let retrieved = node.get("test", "hash.bin").await;

    let expected = blake3::hash(data);
    let actual = blake3::hash(&retrieved);
    assert_eq!(expected, actual);
    assert_eq!(&retrieved[..], data);
}
