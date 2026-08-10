//! Integration test: hinted handoff lifecycle.
//!
//! Tests hint creation, storage lifecycle, and capacity enforcement.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use oceanfs_core::{Hlc, NodeId, SegmentId};
use oceanfs_durability::{HintRecord, HintedHandoff};

#[tokio::test]
async fn handoff_create_deliver_cleanup() {
    let hh = HintedHandoff::new();
    let target = NodeId::new("node-b");

    let hint = HintRecord {
        intended_for: target.clone(),
        segment_id: SegmentId::new(),
        offset: 0,
        length: 100,
        timestamp: Hlc::zero(),
        data: vec![1, 2, 3].into(),
        stored_at_secs: 0,
    };
    hh.handoff(target.clone(), hint).await.unwrap();
    assert_eq!(hh.pending_count(&target), 1);

    // Without a membership reference, gRPC delivery fails and hints are preserved.
    let delivered = hh.deliver_pending(target.clone()).await.unwrap();
    assert_eq!(delivered, 0, "no membership means delivery cannot succeed");
    assert_eq!(hh.pending_count(&target), 1, "failed delivery preserves hints");
}

#[tokio::test]
async fn handoff_multiple_hints_stored_and_counted() {
    let hh = HintedHandoff::new();
    let target = NodeId::new("node-c");

    for i in 0..5 {
        hh.handoff(
            target.clone(),
            HintRecord {
                intended_for: target.clone(),
                segment_id: SegmentId::new(),
                offset: i * 64,
                length: 64,
                timestamp: Hlc::zero(),
                data: vec![i as u8; 64].into(),
                stored_at_secs: 0,
            },
        )
        .await
        .unwrap();
    }
    assert_eq!(hh.pending_count(&target), 5);
    assert_eq!(hh.total_pending_count(), 5);
}

#[tokio::test]
async fn handoff_unknown_node_has_zero_pending() {
    let hh = HintedHandoff::new();
    assert_eq!(hh.pending_count(&NodeId::new("ghost")), 0);
}

#[tokio::test]
async fn deliver_to_node_with_no_hints_returns_zero() {
    let hh = HintedHandoff::new();
    let result = hh.deliver_pending(NodeId::new("empty")).await.unwrap();
    assert_eq!(result, 0);
}

#[tokio::test]
async fn handoff_bounded_capacity_rejects_excess() {
    let hh = HintedHandoff::new();
    let node = NodeId::new("full");

    // Fill to per-node limit (MAX_HINTS_PER_NODE = 1_000).
    for i in 0..1000 {
        hh.handoff(
            node.clone(),
            HintRecord {
                intended_for: node.clone(),
                segment_id: SegmentId::new(),
                offset: i as u64,
                length: 10,
                timestamp: Hlc::zero(),
                data: vec![i as u8].into(),
                stored_at_secs: 0,
            },
        )
        .await
        .unwrap();
    }
    assert_eq!(hh.pending_count(&node), 1000);

    let result = hh
        .handoff(
            node.clone(),
            HintRecord {
                intended_for: node.clone(),
                segment_id: SegmentId::new(),
                offset: 1000,
                length: 10,
                timestamp: Hlc::zero(),
                data: vec![0].into(),
                stored_at_secs: 0,
            },
        )
        .await;
    assert!(result.is_err(), "should reject above per-node capacity");
}

// ── Replica failure triggers hinted handoff (4.4, T21) ──────────

/// Verifies that when a replica write fails during quorum replication,
/// the `WriteCoordinator` stores a hinted handoff entry for the
/// unreachable node.
#[tokio::test]
async fn write_coordinator_handoff_on_replica_failure() {
    use std::{net::SocketAddr, sync::Arc};

    use bytes::Bytes;
    use oceanfs_core::{
        BucketId, GossipConfig, HashKey, HlcClock, Incarnation, NodeId, NodeState, ObjectKey,
        RingConfig, RpcConfig, SegmentSizeConfig,
    };
    use oceanfs_membership::Membership;
    use oceanfs_network::ConnectionPool;
    use oceanfs_routing::{hash_key, Ring, RingCache};
    use oceanfs_server::{WriteCoordinator, WriteRequest};
    use oceanfs_storage::{
        BufferPool, RocksDbMetadataStore, SegmentPool, SegmentSealer, SegmentShard, WalWriter,
    };

    // Set up ring with 3 nodes: n1 (local), n2, n3 (remote).
    let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
    ring.add_node(NodeId::new("n1"));
    ring.add_node(NodeId::new("n2"));
    ring.add_node(NodeId::new("n3"));
    let ring_cache = Arc::new(RingCache::new(ring));

    let addr: SocketAddr = "127.0.0.1:9100".parse().unwrap();
    let membership = Arc::new(Membership::new(
        NodeId::new("n1"),
        addr,
        GossipConfig::default(),
        ring_cache.clone(),
    ));
    membership.upsert_node(NodeId::new("n1"), NodeState::Alive, Incarnation::new(1), addr);
    membership.upsert_node(
        NodeId::new("n2"),
        NodeState::Alive,
        Incarnation::new(1),
        "127.0.0.1:9101".parse().unwrap(),
    );
    membership.upsert_node(
        NodeId::new("n3"),
        NodeState::Alive,
        Incarnation::new(1),
        "127.0.0.1:9102".parse().unwrap(),
    );

    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    let dir = tempfile::tempdir().unwrap();

    let hinted_handoff = {
        let hints_dir = dir.path().join("hints");
        let delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> =
            Arc::new(oceanfs_durability::GrpcHintDeliveryClient::new(pool.clone()));
        let hint_config = oceanfs_durability::HintedHandoffConfig {
            wal_dir: hints_dir.clone(),
            ..Default::default()
        };
        (
            Arc::new(
                oceanfs_durability::HintedHandoffManager::new(
                    hints_dir,
                    delivery_client,
                    hint_config.clone(),
                )
                .with_membership(membership.clone()),
            ),
            hint_config,
        )
    };
    let (hinted_handoff, hint_config) = (hinted_handoff.0, hinted_handoff.1);
    let hlc_clock = Arc::new(HlcClock::new());

    // Segment pipeline.
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
    let shard_small = Arc::new(
        SegmentShard::new(4, oceanfs_core::SizeTier::Small, &size_config, &buffer_pool).unwrap(),
    );
    let shard_standard = Arc::new(
        SegmentShard::new(4, oceanfs_core::SizeTier::Standard, &size_config, &buffer_pool).unwrap(),
    );
    let pool_cfg = oceanfs_core::PoolConfig::default();
    let segment_pool_small = Arc::new(
        SegmentPool::new(
            pool_cfg.clone(),
            oceanfs_core::SizeTier::Small,
            &size_config,
            buffer_pool.clone(),
            None,
        )
        .unwrap(),
    );
    let segment_pool_standard = Arc::new(
        SegmentPool::new(
            pool_cfg,
            oceanfs_core::SizeTier::Standard,
            &size_config,
            buffer_pool,
            None,
        )
        .unwrap(),
    );
    let wal = Arc::new(
        WalWriter::open(&oceanfs_core::WalConfig {
            data_dir: dir.path().join("wal"),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            ..Default::default()
        })
        .await
        .unwrap(),
    );
    let seal_config = oceanfs_storage::SealConfig {
        target_size_bytes: size_config.default_target_size,
        seal_timeout_ms: 5000,
        data_dir: dir.path().join("segments"),
        io_mode: oceanfs_storage::io::IoReadMode::Buffered,
        write_mode: oceanfs_storage::io::SegmentWriteMode::Rename,
    };
    let sealer = Arc::new(SegmentSealer::new(seal_config, metadata.clone(), wal));

    let coordinator = WriteCoordinator::new(
        ring_cache,
        membership,
        pool,
        NodeId::new("n1"),
        hlc_clock,
        metadata,
        size_config,
        shard_small,
        shard_standard,
        segment_pool_small,
        segment_pool_standard,
        sealer,
        hinted_handoff.clone(),
        hint_config,
    );

    // Write with quorum=1: local ack is sufficient, but we still
    // attempt replication to n2 and n3. Since no gRPC servers are
    // running, both replicas will fail, triggering hinted handoff.
    let req = WriteRequest {
        bucket: BucketId::new("test"),
        key: ObjectKey::new("handoff-test"),
        hash_key: HashKey::from_bytes(hash_key(b"handoff-test")),
        data: Bytes::from_static(b"hinted handoff test data"),
        write_quorum: 1,
        ack_after_wal: true,
        ec_async: false,
        policy: None,
    };

    let result = coordinator.put(req).await;
    assert!(result.is_ok(), "write with quorum=1 should succeed despite remote failures");
    let wr = result.unwrap();
    assert_eq!(wr.size, 24, "write result should reflect original data size");
    assert!(wr.blake3_hash.is_some(), "BLAKE3 hash must be computed");

    // Verify that hints were stored for the failed remote replicas.
    let n2 = NodeId::new("n2");
    let n3 = NodeId::new("n3");
    let pending_n2 = hinted_handoff.pending_count(&n2);
    let pending_n3 = hinted_handoff.pending_count(&n3);
    let total = hinted_handoff.total_pending_count();

    assert_eq!(pending_n2, 1, "should have exactly 1 hint for n2");
    assert_eq!(pending_n3, 1, "should have exactly 1 hint for n3");
    assert_eq!(total, 2, "should have exactly 2 hints total");

    // Verify delivery attempt with membership:
    // Without a real gRPC server, delivery fails and returns an error.
    let result = hinted_handoff.deliver_pending(n2.clone()).await;
    assert!(result.is_err(), "delivery should fail without gRPC server");
    // Hints retained after failed delivery (retry semantics).
    assert_eq!(hinted_handoff.pending_count(&n2), 1, "hints retained for retry");
    assert_eq!(hinted_handoff.total_pending_count(), 2, "total hints unchanged");
}
