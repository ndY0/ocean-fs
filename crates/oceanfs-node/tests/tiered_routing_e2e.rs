//! Integration test: Tiered blob size classification.
//!
//! Verifies H7-storage: blobs of different sizes are routed to the correct
//! storage tier (inline ≤4KB, small 4KB-256KB, standard 256KB-4MB, multi >4MB).
//!
//! ## Tests
//! - `inline_blob_1kb_has_no_chunk_refs`
//! - `small_blob_128kb_has_chunk_refs`
//! - `standard_blob_1mb_has_chunk_refs`

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use oceanfs_core::{
    BucketId, GossipConfig, HashKey, HlcClock, Incarnation, MetadataConfig, NodeId, NodeState,
    ObjectKey, PoolConfig, RingConfig, RpcConfig, SegmentSizeConfig, SizeTier, WalConfig,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::{hash_key, Ring, RingCache};
use oceanfs_server::{WriteCoordinator, WriteRequest};
use oceanfs_storage::{
    BufferPool, RocksDbMetadataStore, SealConfig, SegmentPool, SegmentSealer, SegmentShard,
    WalWriter,
};

fn test_registry() -> Arc<oceanfs_storage::SegmentLifecycleRegistry> {
    Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ))
}
async fn make_coordinator(node_id: &str, nodes: &[&str]) -> WriteCoordinator {
    let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
    for n in nodes {
        ring.add_node(NodeId::new(*n));
    }
    let ring_cache = Arc::new(RingCache::new(ring));
    let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let membership = Arc::new(Membership::new(
        NodeId::new(node_id),
        addr,
        GossipConfig::default(),
        ring_cache.clone(),
    ));
    for n in nodes {
        membership.upsert_node(NodeId::new(*n), NodeState::Alive, Incarnation::new(1), Some(addr));
    }
    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    let hlc_clock = Arc::new(HlcClock::new());

    let dir = tempfile::tempdir().unwrap();
    let metadata = Arc::new(
        RocksDbMetadataStore::open(&MetadataConfig {
            data_dir: dir.path().join("meta"),
            block_cache_size: 1024,
            memtable_size: 1024,
            ..Default::default()
        })
        .unwrap(),
    );
    let size_config = SegmentSizeConfig::default();
    let buffer_pool = Arc::new(BufferPool::new(65536, 32));
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
        ..Default::default()
    };
    let lifecycle = Arc::new(
        oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
            &oceanfs_core::LifecycleConfig::default(),
        )
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
    )
}

fn write_request(key: &str, data: Vec<u8>) -> WriteRequest {
    WriteRequest {
        bucket: BucketId::new("test"),
        key: ObjectKey::new(key),
        hash_key: HashKey::from_bytes(hash_key(key.as_bytes())),
        data: Bytes::from(data),
        write_quorum: 1,
        ack_after_wal: true,
        ec_async: false,
        policy: None,
    }
}

// ---------------------------------------------------------------------------
// Inline tier (≤ 4096 bytes)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inline_blob_1kb_has_no_chunk_refs() {
    let coord = make_coordinator("n1", &["n1"]).await;

    // 1 KB ≤ 4096 → Inline tier.
    let data = vec![0x11u8; 1024];
    let req = write_request("inline-1kb", data);
    let result = coord.put(req).await.unwrap();

    assert_eq!(result.size, 1024);
    assert!(result.chunks.is_empty(), "inline blobs should have zero chunk refs");
    assert!(result.blake3_hash.is_some(), "BLAKE3 hash should always be computed");
}

// ---------------------------------------------------------------------------
// Small tier (4 KB – 256 KB)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn small_blob_128kb_has_chunk_refs() {
    let coord = make_coordinator("n1", &["n1"]).await;

    // 128 KB → between 4KB and 256KB → Small tier.
    let data = vec![0x22u8; 131_072];
    let req = write_request("small-128kb", data);
    let result = coord.put(req).await.unwrap();

    assert_eq!(result.size, 131_072);
    assert!(!result.chunks.is_empty(), "small tier blobs should have at least one chunk ref");
    assert_eq!(result.chunks[0].offset, 0);
    assert_eq!(result.chunks[0].length as u64, 131_072);
    assert_ne!(result.chunks[0].segment_id, oceanfs_core::SegmentId::default());
}

// ---------------------------------------------------------------------------
// Standard tier (256 KB – 4 MB)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn standard_blob_1mb_has_chunk_refs() {
    let coord = make_coordinator("n1", &["n1"]).await;

    // 1 MB → between 256KB and 4MB → Standard tier.
    let data = vec![0x33u8; 1_048_576];
    let req = write_request("standard-1mb", data);
    let result = coord.put(req).await.unwrap();

    assert_eq!(result.size, 1_048_576);
    assert!(!result.chunks.is_empty(), "standard tier blobs should have at least one chunk ref");
    assert_eq!(result.chunks[0].offset, 0);
    assert_eq!(result.chunks[0].length as u64, 1_048_576);
}

// ---------------------------------------------------------------------------
// Edge: empty blob
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_blob_has_zero_chunks_and_size() {
    let coord = make_coordinator("n1", &["n1"]).await;

    let req = write_request("empty", vec![]);
    let result = coord.put(req).await.unwrap();

    assert_eq!(result.size, 0);
    assert!(result.chunks.is_empty());
    assert!(result.blake3_hash.is_some());
}
