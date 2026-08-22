//! Integration test: Merkle tree rebuilt from the machine on startup.
//!
//! Verifies ADR-0025 Decision 3 (superseding ADR-0018 Decision 1): when a
//! node starts, the incremental Merkle tree is rebuilt from the lifecycle
//! registry's Sealed entries — the `segments` CF is removed. No MerkleWal
//! is involved — the tree is pure in-memory, derived state.
//!
//! This test exercises the exact code path that `Node::start()` uses:
//! `IncrementalMerkleTree::rebuild_from_segment_scan(&lifecycle_registry, config)`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use oceanfs_core::{HashOutput, SegmentId, SegmentMetadata, SizeTier};
use oceanfs_durability::merkle::{IncrementalMerkleTree, MerkleTreeConfig};
use oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry;

fn make_hash(byte: u8) -> HashOutput {
    let mut bytes = [0u8; 32];
    bytes[0] = byte;
    HashOutput::from_bytes(bytes)
}

fn make_sealed_segment(id: SegmentId, merkle_root: HashOutput) -> SegmentMetadata {
    SegmentMetadata {
        pool_id: 0,
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(merkle_root),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    }
}

fn make_registry() -> SegmentLifecycleRegistry {
    SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default())
}

fn seed_sealed(registry: &SegmentLifecycleRegistry, seg: SegmentMetadata) {
    registry.reserve(seg.segment_id, seg.clone()).unwrap();
    registry.seal(seg.segment_id, seg).unwrap();
}

#[test]
fn rebuild_tree_from_existing_segments() {
    let registry = make_registry();

    // Populate the machine with 3 sealed segments with non-zero merkle roots.
    let mut expected_segments: Vec<(SegmentId, [u8; 32])> = Vec::new();
    for b in 1..=3u8 {
        let seg_id = SegmentId::new();
        let root = make_hash(b);
        seed_sealed(&registry, make_sealed_segment(seg_id, root));
        expected_segments.push((seg_id, *root.as_bytes()));
    }

    // Rebuild the tree — this is the same call Node::start() makes.
    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&registry, &MerkleTreeConfig::default())
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
    let registry = make_registry();

    // Unsealed segment — should not appear in the tree.
    let unsealed = SegmentMetadata {
        pool_id: 0,
        segment_id: SegmentId::new(),
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(make_hash(0xAA)),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: None,
    };
    registry.reserve(unsealed.segment_id, unsealed).unwrap();

    // Sealed but no merkle root — should not appear.
    let sealed_no_root = SegmentMetadata {
        pool_id: 0,
        segment_id: SegmentId::new(),
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    seed_sealed(&registry, sealed_no_root);

    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&registry, &MerkleTreeConfig::default())
            .unwrap();

    assert_eq!(
        tree.segment_count(),
        0,
        "tree should be empty — unsealed and no-root segments are skipped"
    );
}

#[test]
fn rebuild_tree_with_mixed_segments() {
    let registry = make_registry();

    // Mix of sealed + merkle root, sealed + no root, unsealed.
    let good1 = SegmentId::new();
    seed_sealed(&registry, make_sealed_segment(good1, make_hash(0x11)));

    // Sealed with no merkle root (skipped).
    let no_root_id = SegmentId::new();
    seed_sealed(
        &registry,
        SegmentMetadata {
            pool_id: 0,
            segment_id: no_root_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        },
    );

    let good2 = SegmentId::new();
    seed_sealed(&registry, make_sealed_segment(good2, make_hash(0x22)));

    // Unsealed (skipped).
    let unsealed_id = SegmentId::new();
    registry
        .reserve(
            unsealed_id,
            SegmentMetadata {
                pool_id: 0,
                segment_id: unsealed_id,
                ec_k: 4,
                ec_m: 2,
                size_tier: SizeTier::Standard,
                merkle_root: Some(make_hash(0x99)),
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: None,
            },
        )
        .unwrap();

    let tree =
        IncrementalMerkleTree::rebuild_from_segment_scan(&registry, &MerkleTreeConfig::default())
            .unwrap();

    // Only the two valid sealed + rooted segments should appear.
    assert_eq!(tree.segment_count(), 2);
    assert!(tree.root(good1).is_some());
    assert!(tree.root(good2).is_some());
}
