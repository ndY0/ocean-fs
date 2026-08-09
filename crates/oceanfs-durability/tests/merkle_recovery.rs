#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: MerkleWal recovery fallback to segment scan.
//!
//! T2.4: Verify that when the Merkle WAL is corrupted, the system falls back
//! to rebuilding the incremental Merkle tree from a full segment scan.

use std::sync::Arc;

use oceanfs_core::{SegmentId, SegmentMetadata, SizeTier};
use oceanfs_durability::merkle::{
    IncrementalMerkleTree, MerkleTreeConfig, MerkleWal, MerkleWalEntry,
};
use oceanfs_storage::RocksDbMetadataStore;

#[test]
fn test_merkle_wal_corruption_falls_back_to_segment_scan() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("merkle.wal");
    let metadata_dir = dir.path().join("meta");

    // Step 1: Create a MerkleWal and log 5 mutations.
    let wal = MerkleWal::open(&wal_path).unwrap();
    for i in 0..5u32 {
        let entry = MerkleWalEntry::NodeInsert {
            segment_id: SegmentId::new(),
            node_index: i,
            hash: [i as u8; 32],
        };
        wal.log_mutation(&entry).unwrap();
    }
    drop(wal);

    // Step 2: Corrupt the CRC32 of entry 3 (the 4th entry, 0-indexed).
    {
        let mut data = std::fs::read(&wal_path).unwrap();
        // Parse and find the 4th entry's CRC bytes.
        let mut cursor = 0usize;
        for entry_idx in 0..4 {
            if cursor + 4 > data.len() {
                break;
            }
            let len = u32::from_le_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
            ]) as usize;
            let frame_len = 4 + len + 4;
            if cursor + frame_len > data.len() {
                break;
            }
            if entry_idx == 3 {
                // Corrupt CRC of the 4th entry.
                let crc_start = cursor + 4 + len;
                data[crc_start] ^= 0xFF;
                data[crc_start + 1] ^= 0xFF;
                data[crc_start + 2] ^= 0xFF;
                data[crc_start + 3] ^= 0xFF;
            }
            cursor += frame_len;
        }
        std::fs::write(&wal_path, &data).unwrap();
    }

    // Step 3: Replay should fail due to CRC corruption.
    let wal2 = MerkleWal::open(&wal_path).unwrap();
    let replay_result = wal2.replay_mutations();
    assert!(replay_result.is_err(), "replay must fail on corrupted CRC");
    drop(wal2);

    // Step 4: Prepare a metadata store with sealed segments for scan rebuild.
    let metadata_config = oceanfs_core::MetadataConfig {
        data_dir: metadata_dir,
        block_cache_size: 1024,
        memtable_size: 1024,
        ..Default::default()
    };
    let metadata = RocksDbMetadataStore::open(&metadata_config).unwrap();

    // Insert a sealed segment so the scan has something to rebuild from.
    let seg_id = SegmentId::new();
    let merkle_root = oceanfs_core::HashOutput::from_bytes([0x42u8; 32]);
    metadata
        .put_segment(SegmentMetadata {
            segment_id: seg_id,
            ec_k: 0,
            ec_m: 0,
            size_tier: SizeTier::Standard,
            merkle_root: Some(merkle_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        })
        .unwrap();

    // Step 5: Rebuild from segment scan — should succeed with correct root.
    let wal3 = Arc::new(MerkleWal::open(&wal_path).unwrap());
    let tree = IncrementalMerkleTree::rebuild_from_segment_scan(
        &metadata,
        wal3,
        &MerkleTreeConfig::default(),
    )
    .unwrap();

    // Step 6: Verify the tree has the correct root from the scan-rebuilt data.
    let root = tree.root(seg_id);
    assert!(root.is_some(), "tree should have a root for the scanned segment");
    assert_ne!(root.unwrap(), [0u8; 32], "root should not be all zeros");
}
