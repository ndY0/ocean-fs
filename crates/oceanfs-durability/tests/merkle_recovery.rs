#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: IncrementalMerkleTree rebuild from the machine.
//!
//! T2.4: Verify that the incremental Merkle tree can be rebuilt from
//! the lifecycle registry's sealed segments. On node restart, the tree
//! is reconstructed by scanning the machine — supersedes ADR-0018
//! Decision 1's segments-CF scan (ADR-0025 Decision 3).

use oceanfs_core::{SegmentId, SegmentMetadata, SizeTier};
use oceanfs_durability::merkle::{IncrementalMerkleTree, MerkleTreeConfig};
use oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry;

#[test]
fn test_rebuild_from_segment_scan_populates_tree() {
    // Step 1: Prepare the machine with sealed segments.
    let registry = SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default());

    // Insert sealed segments with merkle roots.
    let seg_id1 = SegmentId::new();
    let merkle_root1 = oceanfs_core::HashOutput::from_bytes([0x11u8; 32]);
    let meta1 = SegmentMetadata {
        segment_id: seg_id1,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(merkle_root1),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    registry.reserve(seg_id1, meta1.clone()).unwrap();
    registry.seal(seg_id1, meta1).unwrap();

    let seg_id2 = SegmentId::new();
    let merkle_root2 = oceanfs_core::HashOutput::from_bytes([0x22u8; 32]);
    let meta2 = SegmentMetadata {
        segment_id: seg_id2,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(merkle_root2),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    registry.reserve(seg_id2, meta2.clone()).unwrap();
    registry.seal(seg_id2, meta2).unwrap();

    // Step 2: Rebuild from the machine scan.
    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&registry, &MerkleTreeConfig::default())
            .unwrap();

    // Step 3: Verify both segments appear in the tree.
    assert_eq!(tree.segment_count(), 2);

    let root1 = tree.root(seg_id1);
    assert!(root1.is_some(), "tree should have a root for seg_id1");
    assert_ne!(root1.unwrap(), [0u8; 32], "root should not be all zeros");

    let root2 = tree.root(seg_id2);
    assert!(root2.is_some(), "tree should have a root for seg_id2");
    assert_ne!(root2.unwrap(), [0u8; 32], "root should not be all zeros");
}

#[test]
fn test_rebuild_from_segment_scan_ignores_unsealed() {
    let registry = SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default());

    // Insert an unsealed segment (should be skipped).
    let seg_unsealed = SegmentId::new();
    registry
        .reserve(
            seg_unsealed,
            SegmentMetadata {
                segment_id: seg_unsealed,
                ec_k: 4,
                ec_m: 2,
                size_tier: SizeTier::Standard,
                merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xAAu8; 32])),
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: None,
            },
        )
        .unwrap();

    // Insert a sealed segment with no merkle root (should be skipped).
    let seg_no_root = SegmentId::new();
    registry
        .reserve(
            seg_no_root,
            SegmentMetadata {
                segment_id: seg_no_root,
                ec_k: 4,
                ec_m: 2,
                size_tier: SizeTier::Standard,
                merkle_root: None,
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(1700000000000),
            },
        )
        .unwrap();
    registry
        .seal(
            seg_no_root,
            SegmentMetadata {
                segment_id: seg_no_root,
                ec_k: 4,
                ec_m: 2,
                size_tier: SizeTier::Standard,
                merkle_root: None,
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(1700000000000),
            },
        )
        .unwrap();

    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&registry, &MerkleTreeConfig::default())
            .unwrap();

    // Neither segment should appear: one is unsealed, the other has no merkle root.
    assert_eq!(tree.segment_count(), 0);
}

#[test]
fn test_rebuild_from_empty_metadata_store() {
    let registry = SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default());

    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&registry, &MerkleTreeConfig::default())
            .unwrap();

    assert_eq!(tree.segment_count(), 0);
}
