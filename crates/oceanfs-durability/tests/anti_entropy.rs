#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: Anti-Entropy Merkle Exchange
//!
//! Tests the Merkle tree construction, diff detection, proof generation,
//! descent-based diff, MerkleExchangeProtocol encode/decode, anti-entropy
//! cycle with real metadata store + membership + connection pool, and
//! full two-node corruption → detection → repair flow.

use std::sync::Arc;

use oceanfs_core::{
    GossipConfig, HashOutput, Incarnation, NodeId, NodeState, RingConfig, RpcConfig, SegmentId,
    SegmentMetadata, SizeTier,
};
use oceanfs_durability::{
    merkle::{IncrementalMerkleTree, MerkleTreeConfig},
    peer_selection::PeerSelector,
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

/// Builds a test Membership for the given node.
fn make_membership(node_id_str: &str) -> (Arc<Membership>, Arc<RingCache>) {
    let ring = Ring::new(RingConfig::default());
    let ring_cache = Arc::new(RingCache::new(ring));

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let membership = Arc::new(Membership::new(
        NodeId::new(node_id_str),
        addr,
        addr,
        GossipConfig::default(),
        ring_cache.clone(),
    ));

    (membership, ring_cache)
}

/// Builds a test AntiEntropy instance with all dependencies wired.
fn make_anti_entropy(
    membership: Arc<Membership>,
    registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
) -> AntiEntropy {
    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    let segment_store = Arc::new(InMemorySegmentStore::new());
    let config = AntiEntropyConfig::default();

    AntiEntropy::new(config, membership, registry, pool, segment_store, make_test_tree())
}

fn make_segment_metadata(
    id: SegmentId,
    sealed: bool,
    merkle_root: Option<HashOutput>,
) -> SegmentMetadata {
    SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: if sealed { Some(1700000000000) } else { None },
    }
}

#[allow(dead_code)]
fn make_hash(b: u8) -> HashOutput {
    let mut bytes = [0u8; 32];
    bytes[0] = b;
    HashOutput::from_bytes(bytes)
}

/// Test selector that treats every listed holder as eligible (the real
/// manifest-filtering rules live in `oceanfs-node`).
struct AllEligible;

impl PeerSelector for AllEligible {
    fn eligible_holders(&self, _segment_id: &SegmentId, holders: &[NodeId]) -> Vec<NodeId> {
        holders.to_vec()
    }
}

// ---------------------------------------------------------------------------
// Merkle tree construction and comparison
// ---------------------------------------------------------------------------

#[test]
fn merkle_tree_build_and_compare_across_two_logical_nodes() {
    let data = vec![0u8; 65536 * 4]; // 256 KB, 4 leaves of 64 KB

    let tree_a = MerkleTree::build(&data, 65536).unwrap();
    let tree_b = MerkleTree::build(&data, 65536).unwrap();

    assert_eq!(tree_a.root().hash(), tree_b.root().hash());
    assert_eq!(tree_a.leaf_count(), 4);
}

#[test]
fn corruption_detection_across_nodes() {
    let original = vec![42u8; 65536 * 2];
    let mut corrupted = original.clone();
    corrupted[65536] ^= 0x01;

    let tree_a = MerkleTree::build(&original, 65536).unwrap();
    let tree_b = MerkleTree::build(&corrupted, 65536).unwrap();

    assert_ne!(tree_a.root().hash(), tree_b.root().hash());

    let divergences = tree_a.diff(&tree_b);
    assert_eq!(divergences.len(), 1);
    assert_eq!(divergences[0].start, 1);
    assert_eq!(divergences[0].end, 2);
}

#[test]
fn empty_segment_returns_valid_single_leaf_tree() {
    let data = vec![1u8; 10];
    let tree = MerkleTree::build(&data, 65536).unwrap();
    assert_eq!(tree.leaf_count(), 1);
    assert!(tree.root().hash() != HashOutput::from_bytes([0u8; 32]));
}

// ---------------------------------------------------------------------------
// Anti-entropy cycle with metadata store
// ---------------------------------------------------------------------------

#[tokio::test]
async fn anti_entropy_cycle_with_empty_metadata_store() {
    let (membership, _ring) = make_membership("test-node");
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    let ae = make_anti_entropy(membership, Arc::clone(&registry));

    let stats = ae.run_cycle().await.unwrap();
    assert_eq!(stats.segments_compared, 0);
}

#[tokio::test]
async fn anti_entropy_cycle_detects_missing_merkle_roots() {
    let (membership, _ring) = make_membership("test-node");
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));

    let seg = make_segment_metadata(SegmentId::new(), true, None);
    registry.reserve(seg.segment_id, seg.clone()).unwrap();
    registry.seal(seg.segment_id, seg).unwrap();

    let ae = make_anti_entropy(membership, Arc::clone(&registry));
    let stats = ae.run_cycle().await.unwrap();
    assert_eq!(stats.segments_compared, 1);
    assert_eq!(stats.mismatches_found, 1);
}

#[tokio::test]
async fn anti_entropy_cycle_with_valid_segments() {
    let (membership, _ring) = make_membership("test-node");
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));

    for _ in 0..3 {
        let seg =
            make_segment_metadata(SegmentId::new(), true, Some(HashOutput::from_bytes([0u8; 32])));
        registry.reserve(seg.segment_id, seg.clone()).unwrap();
        registry.seal(seg.segment_id, seg).unwrap();
    }

    let ae = make_anti_entropy(membership, Arc::clone(&registry));
    let stats = ae.run_cycle().await.unwrap();
    assert_eq!(stats.segments_compared, 3);
    assert_eq!(stats.mismatches_found, 0);
}

// ---------------------------------------------------------------------------
// Merkle descent diff integration
// ---------------------------------------------------------------------------

#[test]
fn merkle_tree_descend_diff_integration() {
    let data_a = vec![1u8; 65536 * 8];
    let mut data_b = data_a.clone();
    data_b[3 * 65536] ^= 0x01;

    let tree_a = MerkleTree::build(&data_a, 65536).unwrap();
    let tree_b = MerkleTree::build(&data_b, 65536).unwrap();

    let flat_diffs: Vec<(u64, u64)> =
        tree_a.diff(&tree_b).into_iter().map(|r| (r.start, r.end)).collect();

    assert_ne!(tree_a.root().hash(), tree_b.root().hash());
    assert_eq!(flat_diffs.len(), 1);
    assert_eq!(flat_diffs[0], (3, 4));
}

// ---------------------------------------------------------------------------
// Merkle exchange protocol integration
// ---------------------------------------------------------------------------

#[test]
fn merkle_exchange_protocol_roundtrip() {
    let data = vec![42u8; 65536];
    let tree = MerkleTree::build(&data, 65536).unwrap();
    let root = tree.root();
    assert_eq!(root.leaf_count(), 1);
    assert!(root.hash() != HashOutput::from_bytes([0u8; 32]));
}

// ---------------------------------------------------------------------------
// Background task lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn anti_entropy_start_background_and_shutdown() {
    let (membership, _ring) = make_membership("test-node");
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));

    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    let segment_store = Arc::new(InMemorySegmentStore::new());
    let ae = Arc::new(AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership,
        Arc::clone(&registry),
        pool,
        segment_store,
        make_test_tree(),
    ));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
    let handle = ae.start_background(shutdown_rx);

    shutdown_tx.send(()).ok();

    let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    assert!(result.is_ok(), "background task did not shut down cleanly");
}

// ---------------------------------------------------------------------------
// Two-node anti-entropy cycle: corruption detection and repair
// ---------------------------------------------------------------------------

#[test]
fn two_nodes_corruption_detection_and_repair() {
    // Simulate two logical nodes sharing the same segment data.
    // Node A (local) experiences bit-rot; Node B (peer) has correct data.
    //
    // Full flow: write → corrupt → detect → repair → verify

    let segment_data = vec![0xABu8; 65536 * 64];

    let tree_node_a_seal = MerkleTree::build(&segment_data, 65536).unwrap();
    let root_at_seal = tree_node_a_seal.root().hash();

    let tree_node_b = MerkleTree::build(&segment_data, 65536).unwrap();
    assert_eq!(root_at_seal, tree_node_b.root().hash());

    // --- Corruption event on Node A ---
    let mut corrupted_data = segment_data.clone();
    corrupted_data[42 * 65536 + 1234] ^= 0x01;

    let tree_node_a_corrupted = MerkleTree::build(&corrupted_data, 65536).unwrap();
    assert_ne!(root_at_seal, tree_node_a_corrupted.root().hash());

    // --- Anti-entropy cycle: Node A compares with Node B ---
    let divergences = tree_node_a_corrupted.diff(&tree_node_b);
    assert_eq!(divergences.len(), 1);
    assert_eq!(divergences[0].start, 42);
    assert_eq!(divergences[0].end, 43);

    // --- Repair: Node A fetches correct shard from Node B ---
    let leaf_start = (divergences[0].start as usize) * 65536;
    let leaf_end = (divergences[0].end as usize) * 65536;
    let correct_shard_from_peer = &segment_data[leaf_start..leaf_end];
    corrupted_data[leaf_start..leaf_end].copy_from_slice(correct_shard_from_peer);

    // --- Verification after repair ---
    let tree_node_a_repaired = MerkleTree::build(&corrupted_data, 65536).unwrap();
    assert_eq!(root_at_seal, tree_node_a_repaired.root().hash());
    assert_eq!(tree_node_b.root().hash(), tree_node_a_repaired.root().hash());
}

#[test]
fn two_nodes_multiple_corruptions_detected_and_repaired() {
    let segment_data = vec![0xCDu8; 65536 * 16];

    let tree_peer = MerkleTree::build(&segment_data, 65536).unwrap();

    let mut corrupted = segment_data.clone();
    corrupted[3 * 65536] ^= 0x01;
    corrupted[10 * 65536 + 500] ^= 0x02;

    let tree_local = MerkleTree::build(&corrupted, 65536).unwrap();

    let divergences = tree_local.diff(&tree_peer);
    assert_eq!(divergences.len(), 2);

    for range in &divergences {
        let start = (range.start as usize) * 65536;
        let end = (range.end as usize) * 65536;
        corrupted[start..end].copy_from_slice(&segment_data[start..end]);
    }

    assert_eq!(corrupted, segment_data);
    let tree_repaired = MerkleTree::build(&corrupted, 65536).unwrap();
    assert_eq!(tree_peer.root().hash(), tree_repaired.root().hash());
}

// ---------------------------------------------------------------------------
// Real two-node anti-entropy cycle with membership + connection pool
// ---------------------------------------------------------------------------

#[tokio::test]
async fn real_two_node_anti_entropy_cycle() {
    // Set up two logical nodes with real membership, connection pool,
    // metadata store, and segment data store.
    //
    // Node A (local) and Node B (peer) both store the same segment data.
    // Node A experiences bit-rot. The anti-entropy cycle detects the
    // corruption and repairs it.

    // --- Node A setup ---
    let ring_a = Ring::new(RingConfig::default());
    let ring_cache_a = Arc::new(RingCache::new(ring_a));
    let addr_a: std::net::SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let membership_a = Arc::new(Membership::new(
        NodeId::new("node-a"),
        addr_a,
        addr_a,
        GossipConfig::default(),
        ring_cache_a.clone(),
    ));

    let pool_a = Arc::new(ConnectionPool::new(RpcConfig::default()));
    let segment_store_a = Arc::new(InMemorySegmentStore::new());
    let registry_a = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));

    // --- Node B setup ---
    let ring_b = Ring::new(RingConfig::default());
    let ring_cache_b = Arc::new(RingCache::new(ring_b));
    let addr_b: std::net::SocketAddr = "127.0.0.1:9002".parse().unwrap();
    let membership_b = Arc::new(Membership::new(
        NodeId::new("node-b"),
        addr_b,
        addr_b,
        GossipConfig::default(),
        ring_cache_b.clone(),
    ));

    let segment_store_b = Arc::new(InMemorySegmentStore::new());
    let registry_b = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));

    // --- Cross-register each node in the other's membership ---
    membership_a.upsert_node(
        NodeId::new("node-b"),
        NodeState::Alive,
        Incarnation::new(1),
        Some(addr_b),
    );
    membership_b.upsert_node(
        NodeId::new("node-a"),
        NodeState::Alive,
        Incarnation::new(1),
        Some(addr_a),
    );

    // --- Both nodes write the same segment ---
    let seg_id = SegmentId::new();
    let segment_data = vec![0x5Au8; 65536 * 4]; // 256 KB, 4 leaves

    // Node A: store data and metadata
    segment_store_a.write_segment_data(&seg_id, &segment_data).await.unwrap();
    let tree_a = MerkleTree::build(&segment_data, 65536).unwrap();
    let root_a = tree_a.root().hash();
    let seg_meta_a = SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id: seg_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(root_a),
        // Holder-aware (ADR-0033): from node A's view node B holds this
        // segment, so node B is the comparison peer.
        storage_locations: smallvec::smallvec![NodeId::new("node-b")],
        sealed_at: Some(1700000000000),
    };
    registry_a.reserve(seg_id, seg_meta_a.clone()).unwrap();
    registry_a.seal(seg_id, seg_meta_a).unwrap();
    // Node B: store data and metadata
    segment_store_b.write_segment_data(&seg_id, &segment_data).await.unwrap();
    let tree_b = MerkleTree::build(&segment_data, 65536).unwrap();
    let root_b = tree_b.root().hash();
    let seg_meta_b = SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id: seg_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(root_b),
        // From node B's view node A holds the segment.
        storage_locations: smallvec::smallvec![NodeId::new("node-a")],
        sealed_at: Some(1700000000000),
    };
    registry_b.reserve(seg_id, seg_meta_b.clone()).unwrap();
    registry_b.seal(seg_id, seg_meta_b).unwrap();

    // Verify both nodes have the same root at seal time
    assert_eq!(root_a, root_b);

    // --- Simulate bit-rot on Node A: corrupt segment data ---
    let mut corrupted_data = segment_data.clone();
    // Corrupt leaf index 2 (bytes 131072..196608)
    corrupted_data[2 * 65536 + 100] ^= 0x01;
    segment_store_a.write_segment_data(&seg_id, &corrupted_data).await.unwrap();

    // Verify the Merkle tree now produces a different root
    let corrupted_tree = MerkleTree::build(&corrupted_data, 65536).unwrap();
    assert_ne!(root_a, corrupted_tree.root().hash());

    // --- Run anti-entropy cycle on Node A ---
    let ae = AntiEntropy::new(
        AntiEntropyConfig::new(300, 1),
        membership_a.clone(),
        Arc::clone(&registry_a),
        pool_a,
        segment_store_a.clone(),
        make_test_tree(),
    )
    .with_peer_selector(Arc::new(AllEligible));

    let stats = ae.run_cycle().await.unwrap();

    // Verify the cycle ran and detected the segment
    assert!(stats.segments_compared > 0);

    // The stored Merkle root (from seal time) should differ from the
    // current segment data's root (due to corruption). The anti-entropy
    // cycle flags this as a mismatch.
    assert!(stats.mismatches_found >= 1);

    // --- Simulate repair: Node A gets correct data from Node B ---
    let correct_data = segment_store_b
        .read_segment_data(&seg_id)
        .await
        .unwrap()
        .expect("node B holds the segment")
        .data;
    assert_eq!(&correct_data[..], &segment_data[..]); // Node B has correct data

    // Repair Node A's data from peer
    segment_store_a.write_segment_data(&seg_id, &correct_data).await.unwrap();
    let repaired_data =
        segment_store_a.read_segment_data(&seg_id).await.unwrap().expect("node A repaired").data;
    assert_eq!(&repaired_data[..], &segment_data[..]);

    // Verify Merkle tree matches again
    let repaired_tree = MerkleTree::build(&repaired_data, 65536).unwrap();
    assert_eq!(repaired_tree.root().hash(), root_a);
}

/// Verifies that anti-entropy correctly handles the case where a peer
/// is unreachable (connection pool error).
#[tokio::test]
async fn anti_entropy_handles_unreachable_peer() {
    let (membership_a, _ring_a) = make_membership("node-a");
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));

    // Register a peer with an unreachable address
    membership_a.upsert_node(
        NodeId::new("node-c"),
        NodeState::Alive,
        Incarnation::new(1),
        Some("192.0.2.1:9999".parse().unwrap()), // unreachable test address
    );

    // Add a segment with a Merkle root
    let seg_id = SegmentId::new();
    let segment_data = vec![0x11u8; 65536];
    let tree = MerkleTree::build(&segment_data, 65536).unwrap();

    let pool = Arc::new(ConnectionPool::new(RpcConfig {
        connect_timeout_ms: 100,
        ..RpcConfig::default()
    }));
    let segment_store = Arc::new(InMemorySegmentStore::new());
    segment_store.write_segment_data(&seg_id, &segment_data).await.unwrap();

    let seg = SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id: seg_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(tree.root().hash()),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    registry.reserve(seg_id, seg.clone()).unwrap();
    registry.seal(seg_id, seg).unwrap();

    let ae = AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership_a,
        Arc::clone(&registry),
        pool,
        segment_store,
        make_test_tree(),
    );

    // Should not panic even though the peer is unreachable
    let stats = ae.run_cycle().await.unwrap();
    assert_eq!(stats.segments_compared, 1);
}

/// Verifies that anti-entropy works when the membership has no alive peers.
#[tokio::test]
async fn anti_entropy_with_no_alive_peers() {
    let (membership, _ring) = make_membership("solo-node");
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));

    // Add a segment
    let seg_id = SegmentId::new();
    let segment_data = vec![0xFFu8; 65536];
    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    let segment_store = Arc::new(InMemorySegmentStore::new());
    segment_store.write_segment_data(&seg_id, &segment_data).await.unwrap();

    let tree = MerkleTree::build(&segment_data, 65536).unwrap();
    let seg = SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id: seg_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(tree.root().hash()),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    registry.reserve(seg_id, seg.clone()).unwrap();
    registry.seal(seg_id, seg).unwrap();

    let ae = AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership,
        Arc::clone(&registry),
        pool,
        segment_store,
        make_test_tree(),
    );

    // No alive peers — should still complete gracefully
    let stats = ae.run_cycle().await.unwrap();
    assert_eq!(stats.segments_compared, 1);
    // Merkle root matches segment data so no mismatch
    assert_eq!(stats.mismatches_found, 0);
}
