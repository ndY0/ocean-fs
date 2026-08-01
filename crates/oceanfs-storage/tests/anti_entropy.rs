//! Integration test: Anti-Entropy Merkle Exchange
//!
//! Tests the Merkle tree construction, diff detection, proof generation,
//! and anti-entropy cycle across two logical nodes sharing a metadata store.

#![allow(clippy::unwrap_used)]

use oceanfs_core::HashOutput;
use oceanfs_storage::{AntiEntropy, AntiEntropyConfig, MerkleTree};

#[test]
fn merkle_tree_build_and_compare_across_two_logical_nodes() {
    // Node A and Node B both build trees over the same data
    let data = vec![0u8; 65536 * 4]; // 256 KB, 4 leaves of 64 KB

    let tree_a = MerkleTree::build(&data, 65536).unwrap();
    let tree_b = MerkleTree::build(&data, 65536).unwrap();

    assert_eq!(tree_a.root().hash(), tree_b.root().hash());
    assert_eq!(tree_a.leaf_count(), 4);
}

#[test]
fn corruption_detection_across_nodes() {
    // Node A has correct data, Node B has a corrupted shard
    let original = vec![42u8; 65536 * 2];
    let mut corrupted = original.clone();
    // Flip a single bit in the second leaf
    corrupted[65536] ^= 0x01;

    let tree_a = MerkleTree::build(&original, 65536).unwrap();
    let tree_b = MerkleTree::build(&corrupted, 65536).unwrap();

    // Roots should differ
    assert_ne!(tree_a.root().hash(), tree_b.root().hash());

    // Diff should identify the corrupted leaf
    let divergences = tree_a.diff(&tree_b);
    assert_eq!(divergences.len(), 1);
    assert_eq!(divergences[0].start, 1);
    assert_eq!(divergences[0].end, 2);
}

#[test]
fn empty_segment_returns_valid_single_leaf_tree() {
    let data = vec![1u8; 10]; // Small data, one leaf
    let tree = MerkleTree::build(&data, 65536).unwrap();
    assert_eq!(tree.leaf_count(), 1);
    assert!(tree.root().hash() != HashOutput::from_bytes([0u8; 32]));
}

#[tokio::test]
async fn anti_entropy_cycle_completes_without_peers() {
    let config = AntiEntropyConfig::default();
    let ae = AntiEntropy::new(config);
    let stats = ae.run_cycle().await.unwrap();
    assert_eq!(stats.segments_compared, 0);
}
