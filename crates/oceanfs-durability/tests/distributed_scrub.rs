//! Integration test: Distributed Scrubbing
#![allow(clippy::unwrap_used)]
//!
//! Tests scrub cycle verification, corruption detection,
//! and manual trigger functionality with real Merkle tree verification.
//! (Partition assignment is tested in the unit test module.)

use std::sync::Arc;

use oceanfs_core::{MetadataConfig, SegmentId, SegmentMetadata, SizeTier};
use oceanfs_durability::{
    InMemorySegmentStore, MerkleTree, ScrubConfig, ScrubCoordinator, SegmentDataStore,
};
use oceanfs_storage::RocksDbMetadataStore;

fn test_config() -> MetadataConfig {
    let dir = tempfile::tempdir().unwrap();
    MetadataConfig {
        data_dir: dir.path().to_path_buf(),
        block_cache_size: 8 * 1024 * 1024,
        memtable_size: 8 * 1024 * 1024,
    }
}

fn segment_store_with_data(entries: Vec<(SegmentId, Vec<u8>)>) -> Arc<InMemorySegmentStore> {
    let store = Arc::new(InMemorySegmentStore::new());
    for (id, data) in entries {
        store.write_segment_data(&id, &data).unwrap();
    }
    store
}

#[tokio::test]
async fn scrub_cycle_on_empty_store() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
    let data_store = segment_store_with_data(vec![]);
    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(metadata, data_store).await.unwrap();
    assert_eq!(report.segments_total(), 0);
    assert_eq!(report.segments_healthy(), 0);
}

#[tokio::test]
async fn scrub_cycle_verifies_healthy_segments() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
    let mut stored_data = Vec::new();

    // Put 3 segments with known data and correct Merkle roots
    for _ in 0..3 {
        let seg_id = SegmentId::new();
        let data = vec![0xAB; 2048];
        let merkle_root = MerkleTree::build(&data, 0).unwrap().root().hash();

        stored_data.push((seg_id, data));

        let seg = SegmentMetadata {
            segment_id: seg_id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(merkle_root),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };
        metadata.put_segment(seg).unwrap();
    }

    let data_store = segment_store_with_data(stored_data);
    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(metadata, data_store).await.unwrap();
    assert_eq!(report.segments_total(), 3);
    assert_eq!(report.segments_healthy(), 3);
    assert_eq!(report.segments_corrupt(), 0);
    assert!(report.bytes_scanned() > 0);
    assert!(report.duration_sec() >= 0.0);
}

#[tokio::test]
async fn scrub_cycle_detects_corruption() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

    let seg_id = SegmentId::new();
    let correct_data = vec![0x55; 4096];
    let mut bad_data = correct_data.clone();
    bad_data[777] ^= 0x01; // Inject a bit-flip

    let correct_root = MerkleTree::build(&correct_data, 0).unwrap().root().hash();

    let seg = SegmentMetadata {
        segment_id: seg_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(correct_root),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    metadata.put_segment(seg).unwrap();

    // Store the corrupt data
    let data_store = segment_store_with_data(vec![(seg_id, bad_data)]);

    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(metadata, data_store).await.unwrap();

    assert_eq!(report.segments_total(), 1);
    assert_eq!(report.segments_healthy(), 0);
    assert_eq!(report.segments_corrupt(), 1);
}

#[tokio::test]
async fn manual_scrub_trigger_does_not_error() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
    let data_store = segment_store_with_data(vec![]);
    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let result = coord.trigger_manual(metadata, data_store).await;
    assert!(result.is_ok());
    // Give the spawned task a moment
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
}

#[tokio::test]
async fn run_cycle_with_missing_data_detects_corruption() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

    let seg_id = SegmentId::new();
    let data = vec![0xBE; 1024];
    let merkle_root = MerkleTree::build(&data, 0).unwrap().root().hash();

    // Put segment metadata with a Merkle root
    let seg = SegmentMetadata {
        segment_id: seg_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(merkle_root),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    metadata.put_segment(seg).unwrap();

    // Empty data store — data is missing (simulates shard loss)
    let data_store = segment_store_with_data(vec![]);

    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(metadata, data_store).await.unwrap();

    assert_eq!(report.segments_total(), 1);
    assert_eq!(report.segments_healthy(), 0);
    assert_eq!(report.segments_corrupt(), 1);
}
