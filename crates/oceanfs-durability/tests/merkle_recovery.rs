#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: IncrementalMerkleTree rebuild from segment scan.
//!
//! T2.4: Verify that the incremental Merkle tree can be rebuilt from
//! a metadata store containing sealed segments. On node restart, the
//! tree is reconstructed by scanning the `segments` column family —
//! no MerkleWal is required (ADR-0018 Decision 1).

use oceanfs_core::{SegmentId, SegmentMetadata, SizeTier};
use oceanfs_durability::merkle::{IncrementalMerkleTree, MerkleTreeConfig};
use oceanfs_storage::RocksDbMetadataStore;

#[test]
fn test_rebuild_from_segment_scan_populates_tree() {
    let dir = tempfile::tempdir().unwrap();
    let metadata_dir = dir.path().join("meta");

    // Step 1: Prepare a metadata store with sealed segments.
    let metadata_config = oceanfs_core::MetadataConfig {
        data_dir: metadata_dir,
        block_cache_size: 1024,
        memtable_size: 1024,
        ..Default::default()
    };
    let metadata = RocksDbMetadataStore::open(&metadata_config).unwrap();

    // Insert sealed segments with merkle roots.
    let seg_id1 = SegmentId::new();
    let merkle_root1 = oceanfs_core::HashOutput::from_bytes([0x11u8; 32]);
    metadata
        .put_segment(SegmentMetadata {
            segment_id: seg_id1,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(merkle_root1),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        })
        .unwrap();

    let seg_id2 = SegmentId::new();
    let merkle_root2 = oceanfs_core::HashOutput::from_bytes([0x22u8; 32]);
    metadata
        .put_segment(SegmentMetadata {
            segment_id: seg_id2,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(merkle_root2),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        })
        .unwrap();

    // Step 2: Rebuild from segment scan.
    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&metadata, &MerkleTreeConfig::default())
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
    let dir = tempfile::tempdir().unwrap();
    let metadata_dir = dir.path().join("meta");

    let metadata_config = oceanfs_core::MetadataConfig {
        data_dir: metadata_dir,
        block_cache_size: 1024,
        memtable_size: 1024,
        ..Default::default()
    };
    let metadata = RocksDbMetadataStore::open(&metadata_config).unwrap();

    // Insert an unsealed segment (should be skipped).
    let seg_unsealed = SegmentId::new();
    metadata
        .put_segment(SegmentMetadata {
            segment_id: seg_unsealed,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xAAu8; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: None,
        })
        .unwrap();

    // Insert a sealed segment with no merkle root (should be skipped).
    let seg_no_root = SegmentId::new();
    metadata
        .put_segment(SegmentMetadata {
            segment_id: seg_no_root,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        })
        .unwrap();

    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&metadata, &MerkleTreeConfig::default())
            .unwrap();

    // Neither segment should appear: one is unsealed, the other has no merkle root.
    assert_eq!(tree.segment_count(), 0);
}

#[test]
fn test_rebuild_from_empty_metadata_store() {
    let dir = tempfile::tempdir().unwrap();
    let metadata_dir = dir.path().join("meta");

    let metadata_config = oceanfs_core::MetadataConfig {
        data_dir: metadata_dir,
        block_cache_size: 1024,
        memtable_size: 1024,
        ..Default::default()
    };
    let metadata = RocksDbMetadataStore::open(&metadata_config).unwrap();

    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&metadata, &MerkleTreeConfig::default())
            .unwrap();

    assert_eq!(tree.segment_count(), 0);
}
