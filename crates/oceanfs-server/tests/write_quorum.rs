//! Integration test: write coordinator and quorum replication.
//!
//! Tests quorum fan-out, ack collection, and error handling.

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

    // Segment pipeline.
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
        )
        .unwrap(),
    );
    let segment_pool_standard = Arc::new(
        SegmentPool::new(pool_cfg, SizeTier::Standard, &size_config, buffer_pool, None, None)
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
    let sealer = Arc::new(SegmentSealer::new(seal_config, metadata.clone(), wal));

    let hinted_handoff = {
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

    let (hinted_handoff, hint_config) = (hinted_handoff.0, hinted_handoff.1);
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
        hinted_handoff,
        hint_config,
    )
}

fn write_request(key: &str, data: &[u8], quorum: u8) -> WriteRequest {
    WriteRequest {
        bucket: BucketId::new("test"),
        key: ObjectKey::new(key),
        hash_key: HashKey::from_bytes(hash_key(key.as_bytes())),
        data: Bytes::copy_from_slice(data),
        write_quorum: quorum,
        ack_after_wal: true,
        ec_async: false,
        policy: None,
    }
}

#[tokio::test]
async fn write_quorum_1_with_local_node_succeeds() {
    let coord = make_coordinator("n1", &["n1", "n2", "n3"]).await;
    let req = write_request("obj-1", b"hello", 1);
    let result = coord.put(req).await;
    assert!(result.is_ok(), "write with quorum 1 should succeed");
    let wr = result.unwrap();
    assert_eq!(wr.size, 5);
    assert_eq!(wr.object_key.as_str(), "obj-1");
}

#[tokio::test]
async fn write_triggers_hlc_advance() {
    let coord = make_coordinator("n1", &["n1"]).await;
    let before = coord.hlc_clock().now();
    let req = write_request("hlc-test", b"data", 1);
    coord.put(req).await.unwrap();
    let after = coord.hlc_clock().now();
    assert!(after > before, "HLC must advance after write");
}

#[tokio::test]
async fn write_with_quorum_capped_to_replica_count() {
    // 1 node in ring, but requested quorum of 3 — capped to 1, succeeds.
    let coord = make_coordinator("n1", &["n1"]).await;
    let mut req = write_request("capped", b"x", 3);
    req.write_quorum = 3;
    let result = coord.put(req).await;
    assert!(result.is_ok(), "quorum capped to 1 should succeed");
}
