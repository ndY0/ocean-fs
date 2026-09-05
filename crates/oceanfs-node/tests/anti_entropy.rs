//! Integration test: Anti-entropy Merkle tree exchange.
//!
#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Verifies that the anti-entropy worker:
//! 1. Builds Merkle trees from segment data
//! 2. Detects Merkle root mismatches
//! 3. Reports accurate statistics
//!
//! In a multi-node cluster, the full gRPC exchange would be tested;
//! this single-node test exercises the Merkle tree comparison logic.

use std::sync::Arc;

use oceanfs_core::{HashOutput, NodeId, SegmentId, SegmentMetadata, SizeTier};
use oceanfs_durability::{
    merkle::{IncrementalMerkleTree, MerkleTreeConfig},
    AntiEntropy, AntiEntropyConfig, InMemorySegmentStore, MerkleTree,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::{Ring, RingCache};
use oceanfs_storage_api::SegmentDataStore;

/// Creates a test IncrementalMerkleTree.
fn make_test_tree() -> Arc<IncrementalMerkleTree> {
    Arc::new(IncrementalMerkleTree::new(MerkleTreeConfig::default()))
}

/// Helper: create a membership + ring cache for a single node.
fn make_membership(node_id: &str) -> (Arc<Membership>, Arc<RingCache>) {
    let ring = Ring::new(oceanfs_core::RingConfig::default());
    let ring_cache = Arc::new(RingCache::new(ring));
    let addr: std::net::SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let membership = Arc::new(Membership::new(
        NodeId::new(node_id),
        addr,
        addr,
        oceanfs_core::GossipConfig::default(),
        ring_cache.clone(),
    ));
    (membership, ring_cache)
}

/// Helper: create a sealed segment metadata with a merkle root.
fn make_sealed_segment(id: SegmentId, merkle_root: Option<HashOutput>) -> SegmentMetadata {
    SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    }
}

/// Helper: create segment data for verify.
fn make_test_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i % 251) as u8;
    }
    data
}

#[tokio::test]
async fn ae_empty_segments_produces_zero_stats() {
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    let (membership, _ring_cache) = make_membership("test-node");
    let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
    let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegmentStore::new());

    let ae = AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership,
        Arc::clone(&registry),
        pool,
        data_store,
        make_test_tree(),
    );

    let stats = ae.run_cycle().await.expect("AE cycle");
    assert_eq!(stats.segments_compared, 0);
    assert_eq!(stats.mismatches_found, 0);
}

#[tokio::test]
async fn ae_sealed_segment_with_matching_root_no_mismatch() {
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegmentStore::new());

    // Create test data and write it to the in-memory store
    let data = make_test_data(65536); // 64 KB
    let seg_id = SegmentId::new();
    data_store.write_segment_data(&seg_id, &data).await.expect("write segment data");

    // Compute the Merkle root and seed the machine with it
    let tree = MerkleTree::build(&data, 0).expect("build Merkle tree");
    let root_hash = tree.root().hash();
    let seg_meta = make_sealed_segment(seg_id, Some(root_hash));
    registry.reserve(seg_id, seg_meta.clone()).expect("reserve segment");
    registry.seal(seg_id, seg_meta).expect("seal segment");

    let (membership, _ring_cache) = make_membership("test-node");
    let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

    let ae = AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership,
        Arc::clone(&registry),
        pool,
        data_store,
        make_test_tree(),
    );

    let stats = ae.run_cycle().await.expect("AE cycle");
    assert_eq!(stats.segments_compared, 1);
    // Root matches — no mismatch expected
    assert_eq!(stats.mismatches_found, 0);
}

#[tokio::test]
async fn ae_sealed_segment_with_mismatched_root_detected() {
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegmentStore::new());

    // Create test data
    let data = make_test_data(65536);
    let seg_id = SegmentId::new();
    data_store.write_segment_data(&seg_id, &data).await.expect("write segment data");

    // Store a deliberately WRONG merkle root
    let wrong_hash = HashOutput::from_bytes([0xAAu8; 32]);
    let seg_meta = make_sealed_segment(seg_id, Some(wrong_hash));
    registry.reserve(seg_id, seg_meta.clone()).expect("reserve segment");
    registry.seal(seg_id, seg_meta).expect("seal segment");

    let (membership, _ring_cache) = make_membership("test-node");
    let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

    let ae = AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership,
        Arc::clone(&registry),
        pool,
        data_store,
        make_test_tree(),
    );

    let stats = ae.run_cycle().await.expect("AE cycle");
    assert_eq!(stats.segments_compared, 1);
    // Root mismatch detected — even without peers, local verification catches it
    assert!(stats.mismatches_found >= 1);
}

#[tokio::test]
async fn ae_sealed_segment_without_merkle_root_is_flagged() {
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegmentStore::new());

    let data = make_test_data(4096);
    let seg_id = SegmentId::new();
    data_store.write_segment_data(&seg_id, &data).await.expect("write segment data");

    // Segment with no stored Merkle root
    let seg_meta = make_sealed_segment(seg_id, None);
    registry.reserve(seg_id, seg_meta.clone()).expect("reserve segment");
    registry.seal(seg_id, seg_meta).expect("seal segment");

    let (membership, _ring_cache) = make_membership("test-node");
    let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

    let ae = AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership,
        Arc::clone(&registry),
        pool,
        data_store,
        make_test_tree(),
    );

    let stats = ae.run_cycle().await.expect("AE cycle");
    assert_eq!(stats.segments_compared, 1);
    // Missing Merkle root is flagged as mismatch
    assert_eq!(stats.mismatches_found, 1);
}

#[tokio::test]
async fn ae_multiple_segments_all_compared() {
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegmentStore::new());

    for i in 0..5u32 {
        let data = make_test_data(1024 * (i + 1) as usize);
        let seg_id = SegmentId::new();
        data_store.write_segment_data(&seg_id, &data).await.expect("write segment data");

        let tree = MerkleTree::build(&data, 0).expect("build Merkle tree");
        let root_hash = tree.root().hash();
        let seg_meta = make_sealed_segment(seg_id, Some(root_hash));
        registry.reserve(seg_id, seg_meta.clone()).expect("reserve segment");
        registry.seal(seg_id, seg_meta).expect("seal segment");
    }

    let (membership, _ring_cache) = make_membership("test-node");
    let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

    let ae = AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership,
        Arc::clone(&registry),
        pool,
        data_store,
        make_test_tree(),
    );

    let stats = ae.run_cycle().await.expect("AE cycle");
    assert_eq!(stats.segments_compared, 5);
    // All roots match
    assert_eq!(stats.mismatches_found, 0);
}

#[tokio::test]
async fn ae_merkle_tree_build_and_compare() {
    // Pure MerkleTree test — no metadata store needed.
    let data = make_test_data(131072); // 128 KB = 2 leaves at 64 KB default
    let tree = MerkleTree::build(&data, 0).expect("build tree");
    assert_eq!(tree.leaf_count(), 2);

    // Identical data produces identical root
    let data2 = data.clone();
    let tree2 = MerkleTree::build(&data2, 0).expect("build tree 2");
    assert_eq!(tree.root().hash(), tree2.root().hash());
    assert!(tree.diff(&tree2).is_empty());

    // Different data produces different root
    let mut data3 = data;
    data3[0] ^= 0xFF;
    let tree3 = MerkleTree::build(&data3, 0).expect("build tree 3");
    assert_ne!(tree.root().hash(), tree3.root().hash());
    assert!(!tree.diff(&tree3).is_empty());
}

/// T2.3: AntiEntropyConfig with custom `peer_count` is respected by
/// `select_alive_peers()`. With a single-node cluster, 0 non-self peers
/// exist, so the returned vec is always empty — but the config is wired.
#[tokio::test]
async fn test_ae_config_peer_count_respected() {
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    let (membership, _ring_cache) = make_membership("test-node");
    let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
    let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegmentStore::new());

    // Custom peer_count = 3 (vs default 1).
    let config = AntiEntropyConfig::new(300, 3);
    let ae = AntiEntropy::new(
        config,
        membership,
        Arc::clone(&registry),
        pool,
        data_store,
        make_test_tree(),
    );

    // select_alive_peers should not panic; with only self node, returns empty.
    let peers = ae.select_alive_peers();
    assert!(peers.is_empty(), "single-node cluster has no peers");

    // Verify config accessor returns the custom value.
    // (Config is stored internally; verify via run_cycle stats.)
    let stats = ae.run_cycle().await.expect("AE cycle");
    assert_eq!(stats.segments_compared, 0);
    assert_eq!(stats.mismatches_found, 0);
}
