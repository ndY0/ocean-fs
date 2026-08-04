//! Integration test: Scrub cycle verification.
//!
//! Verifies that the scrub coordinator:
//! 1. Scans all segments from the metadata store
//! 2. Verifies Merkle root integrity for each segment
//! 3. Builds an accurate scrub report
//! 4. Detects corrupt/healthy segments correctly

use std::sync::Arc;

use oceanfs_core::{MetadataConfig, SegmentId, SegmentMetadata, SizeTier};
use oceanfs_durability::{
    InMemorySegmentStore, MerkleTree, ScrubConfig, ScrubCoordinator, SegmentDataStore,
};
use oceanfs_storage::RocksDbMetadataStore;

/// Helper: create a temporary metadata store.
fn open_temp_metadata() -> Arc<RocksDbMetadataStore> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = MetadataConfig {
        data_dir: dir.path().to_path_buf(),
        block_cache_size: 8 * 1024 * 1024,
        memtable_size: 8 * 1024 * 1024,
    };
    let _dir_leaked = Box::leak(Box::new(dir));
    Arc::new(RocksDbMetadataStore::open(&config).expect("open metadata store"))
}

/// Helper: create test data with the given size.
fn make_test_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i % 251) as u8;
    }
    data
}

/// Helper: create a data store with pre-populated segment data.
fn make_data_store(entries: Vec<(SegmentId, Vec<u8>)>) -> Arc<dyn SegmentDataStore> {
    let store = Arc::new(InMemorySegmentStore::new());
    for (id, data) in entries {
        store.write_segment_data(&id, &data).expect("write segment data");
    }
    store
}

/// Helper: create a sealed segment metadata with computed Merkle root.
fn make_sealed_segment(id: SegmentId, data: &[u8]) -> SegmentMetadata {
    let merkle_root = MerkleTree::build(data, 0).map(|t| t.root().hash());
    SegmentMetadata {
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    }
}

#[tokio::test]
async fn scrub_empty_store_produces_zero_report() {
    let metadata = open_temp_metadata();
    let data_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegmentStore::new());
    let coord = ScrubCoordinator::new(ScrubConfig::default());

    let report = coord.run_cycle(metadata, data_store).await.expect("scrub cycle");
    assert_eq!(report.segments_total(), 0);
    assert_eq!(report.segments_healthy(), 0);
    assert_eq!(report.segments_corrupt(), 0);
}

#[tokio::test]
async fn scrub_single_healthy_segment_report() {
    let metadata = open_temp_metadata();
    let data = make_test_data(65536);
    let seg_id = SegmentId::new();

    let seg_meta = make_sealed_segment(seg_id, &data);
    metadata.put_segment(seg_meta).expect("put segment");

    let data_store = make_data_store(vec![(seg_id, data)]);
    let coord = ScrubCoordinator::new(ScrubConfig::default());

    let report = coord.run_cycle(metadata, data_store).await.expect("scrub cycle");
    assert_eq!(report.segments_total(), 1);
    assert_eq!(report.segments_healthy(), 1);
    assert_eq!(report.segments_corrupt(), 0);
    assert!(report.bytes_scanned() > 0);
}

#[tokio::test]
async fn scrub_corrupt_segment_detected() {
    let metadata = open_temp_metadata();
    let original_data = make_test_data(65536);
    let seg_id = SegmentId::new();

    // Store metadata with the correct Merkle root
    let seg_meta = make_sealed_segment(seg_id, &original_data);
    metadata.put_segment(seg_meta).expect("put segment");

    // But put CORRUPT data in the data store (flip a byte)
    let mut corrupt_data = original_data.clone();
    corrupt_data[100] ^= 0xFF;
    let data_store = make_data_store(vec![(seg_id, corrupt_data)]);

    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(metadata, data_store).await.expect("scrub cycle");
    assert_eq!(report.segments_total(), 1);
    assert_eq!(report.segments_healthy(), 0);
    assert_eq!(report.segments_corrupt(), 1);
}

#[tokio::test]
async fn scrub_multiple_segments_mixed_health() {
    let metadata = open_temp_metadata();
    let mut entries = Vec::new();

    // 3 healthy segments
    for _ in 0..3u32 {
        let data = make_test_data(32768);
        let seg_id = SegmentId::new();
        let seg_meta = make_sealed_segment(seg_id, &data);
        metadata.put_segment(seg_meta).expect("put segment");
        entries.push((seg_id, data));
    }

    // 1 corrupt segment
    let corrupt_original = make_test_data(65536);
    let corrupt_id = SegmentId::new();
    let corrupt_meta = make_sealed_segment(corrupt_id, &corrupt_original);
    metadata.put_segment(corrupt_meta).expect("put segment");
    let mut corrupt_data = corrupt_original.clone();
    corrupt_data[0] ^= 0xFF;
    entries.push((corrupt_id, corrupt_data));

    let data_store = make_data_store(entries);
    let coord = ScrubCoordinator::new(ScrubConfig::default());

    let report = coord.run_cycle(metadata, data_store).await.expect("scrub cycle");
    assert_eq!(report.segments_total(), 4);
    assert_eq!(report.segments_healthy(), 3);
    assert_eq!(report.segments_corrupt(), 1);
}

#[tokio::test]
async fn scrub_report_includes_duration_and_bytes() {
    let metadata = open_temp_metadata();
    let data = make_test_data(65536);
    let seg_id = SegmentId::new();

    let seg_meta = make_sealed_segment(seg_id, &data);
    metadata.put_segment(seg_meta).expect("put segment");

    let data_store = make_data_store(vec![(seg_id, data)]);
    let coord = ScrubCoordinator::new(ScrubConfig::default());

    let report = coord.run_cycle(metadata, data_store).await.expect("scrub cycle");
    assert!(report.duration_sec() > 0.0);
    assert!(report.bytes_scanned() >= 65536);
    assert_eq!(report.nodes_participated(), 1);
}

#[tokio::test]
async fn scrub_without_merkle_root_still_scans_bytes() {
    let metadata = open_temp_metadata();
    let data = make_test_data(4096);
    let seg_id = SegmentId::new();

    // Segment WITHOUT a stored Merkle root
    let seg_meta = SegmentMetadata {
        segment_id: seg_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    metadata.put_segment(seg_meta).expect("put segment");

    let data_store = make_data_store(vec![(seg_id, data)]);
    let coord = ScrubCoordinator::new(ScrubConfig::default());

    let report = coord.run_cycle(metadata, data_store).await.expect("scrub cycle");
    assert_eq!(report.segments_total(), 1);
    // Without Merkle root the segment cannot be verified,
    // but it's still scanned and counted
    assert_eq!(report.segments_healthy(), 1);
    assert!(report.bytes_scanned() > 0);
}
