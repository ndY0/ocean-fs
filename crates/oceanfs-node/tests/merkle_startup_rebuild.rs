//! Integration test: Merkle tree rebuilt from segments CF on startup.
//!
//! Verifies ADR-0018 Decision 1: when a node starts, the incremental Merkle
//! tree is rebuilt from the authoritative `segments` column family in RocksDB.
//! No MerkleWal is involved — the tree is pure in-memory, derived state.
//!
//! This test exercises the exact code path that `Node::start()` uses:
//! `IncrementalMerkleTree::rebuild_from_segment_scan(metadata_store, config)`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use oceanfs_core::{HashOutput, MetadataConfig, SegmentId, SegmentMetadata, SizeTier};
use oceanfs_durability::merkle::{IncrementalMerkleTree, MerkleTreeConfig};
use oceanfs_storage::RocksDbMetadataStore;

fn make_hash(byte: u8) -> HashOutput {
    let mut bytes = [0u8; 32];
    bytes[0] = byte;
    HashOutput::from_bytes(bytes)
}

fn make_sealed_segment(id: SegmentId, merkle_root: HashOutput) -> SegmentMetadata {
    SegmentMetadata {
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(merkle_root),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    }
}

#[test]
fn rebuild_tree_from_existing_segments_cf() {
    let dir = tempfile::tempdir().unwrap();
    let metadata = RocksDbMetadataStore::open(&MetadataConfig {
        data_dir: dir.path().join("meta"),
        block_cache_size: 1024,
        memtable_size: 1024,
        ..Default::default()
    })
    .unwrap();

    // Populate the segments CF with 5 sealed segments, each with a unique
    // Populate with 3 sealed segments with non-zero merkle roots.
    let mut expected_segments: Vec<(SegmentId, [u8; 32])> = Vec::new();
    for b in 1..=3u8 {
        let seg_id = SegmentId::new();
        let root = make_hash(b);
        metadata.put_segment(make_sealed_segment(seg_id, root)).unwrap();
        expected_segments.push((seg_id, *root.as_bytes()));
    }

    // Rebuild the tree — this is the same call Node::start() makes.
    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&metadata, &MerkleTreeConfig::default())
            .unwrap();

    // Every sealed segment with a merkle root must appear in the tree.
    assert_eq!(
        tree.segment_count(),
        expected_segments.len(),
        "tree should contain all sealed segments with merkle roots"
    );

    for (seg_id, expected_root) in &expected_segments {
        let root = tree.root(*seg_id);
        assert!(root.is_some(), "segment {seg_id} should have a root in the tree");
        assert_ne!(root.unwrap(), [0u8; 32], "root for {seg_id} should not be all zeros");
        // Single-leaf Merkle tree: the root IS the leaf hash.
        assert_eq!(
            root.unwrap(),
            *expected_root,
            "single-leaf tree root should equal the leaf hash for {seg_id}"
        );
    }
}

#[test]
fn rebuild_tree_skips_unsealed_segments() {
    let dir = tempfile::tempdir().unwrap();
    let metadata = RocksDbMetadataStore::open(&MetadataConfig {
        data_dir: dir.path().join("meta"),
        block_cache_size: 1024,
        memtable_size: 1024,
        ..Default::default()
    })
    .unwrap();

    // Unsealed segment — should not appear in the tree.
    let unsealed = SegmentMetadata {
        segment_id: SegmentId::new(),
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(make_hash(0xAA)),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: None,
    };
    metadata.put_segment(unsealed).unwrap();

    // Sealed but no merkle root — should not appear.
    let sealed_no_root = SegmentMetadata {
        segment_id: SegmentId::new(),
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    metadata.put_segment(sealed_no_root).unwrap();

    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&metadata, &MerkleTreeConfig::default())
            .unwrap();

    assert_eq!(
        tree.segment_count(),
        0,
        "tree should be empty — unsealed and no-root segments are skipped"
    );
}

#[test]
fn rebuild_tree_with_mixed_segments() {
    let dir = tempfile::tempdir().unwrap();
    let metadata = RocksDbMetadataStore::open(&MetadataConfig {
        data_dir: dir.path().join("meta"),
        block_cache_size: 1024,
        memtable_size: 1024,
        ..Default::default()
    })
    .unwrap();

    // Mix of sealed + merkle root, sealed + no root, unsealed.
    let good1 = SegmentId::new();
    metadata.put_segment(make_sealed_segment(good1, make_hash(0x11))).unwrap();

    // Sealed with no merkle root (skipped).
    metadata
        .put_segment(SegmentMetadata {
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        })
        .unwrap();

    let good2 = SegmentId::new();
    metadata.put_segment(make_sealed_segment(good2, make_hash(0x22))).unwrap();

    // Unsealed (skipped).
    metadata
        .put_segment(SegmentMetadata {
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(make_hash(0x99)),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: None,
        })
        .unwrap();

    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&metadata, &MerkleTreeConfig::default())
            .unwrap();

    // Only the two valid sealed + rooted segments should appear.
    assert_eq!(tree.segment_count(), 2);
    assert!(tree.root(good1).is_some());
    assert!(tree.root(good2).is_some());
}
