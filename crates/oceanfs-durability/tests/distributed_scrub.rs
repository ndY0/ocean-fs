//! Integration test: Distributed Scrubbing
#![allow(clippy::unwrap_used)]
//!
//! Tests scrub cycle verification, corruption detection,
//! and manual trigger functionality with real Merkle tree verification.
//! (Partition assignment is tested in the unit test module.)

use std::sync::Arc;

use oceanfs_core::{SegmentId, SegmentMetadata, SizeTier};
use oceanfs_durability::{
    InMemorySegmentStore, MerkleTree, ScrubConfig, ScrubCoordinator, SegmentDataStore,
};
use oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry;

fn make_registry() -> Arc<SegmentLifecycleRegistry> {
    Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()))
}

fn segment_store_with_data(entries: Vec<(SegmentId, Vec<u8>)>) -> Arc<InMemorySegmentStore> {
    let store = Arc::new(InMemorySegmentStore::new());
    for (id, data) in entries {
        store.write_segment_data(&id, &data).unwrap();
    }
    store
}

fn seed_sealed(registry: &SegmentLifecycleRegistry, seg: SegmentMetadata) {
    registry.reserve(seg.segment_id, seg.clone()).unwrap();
    registry.seal(seg.segment_id, seg).unwrap();
}

#[tokio::test]
async fn scrub_cycle_on_empty_store() {
    let registry = make_registry();
    let data_store = segment_store_with_data(vec![]);
    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(Arc::clone(&registry), data_store).await.unwrap();
    assert_eq!(report.segments_total(), 0);
    assert_eq!(report.segments_healthy(), 0);
}

#[tokio::test]
async fn scrub_cycle_verifies_healthy_segments() {
    let registry = make_registry();
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
        seed_sealed(&registry, seg);
    }

    let data_store = segment_store_with_data(stored_data);
    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(Arc::clone(&registry), data_store).await.unwrap();
    assert_eq!(report.segments_total(), 3);
    assert_eq!(report.segments_healthy(), 3);
    assert_eq!(report.segments_corrupt(), 0);
    assert!(report.bytes_scanned() > 0);
    assert!(report.duration_sec() >= 0.0);
}

#[tokio::test]
async fn scrub_cycle_detects_corruption() {
    let registry = make_registry();

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
    seed_sealed(&registry, seg);

    // Store the corrupt data
    let data_store = segment_store_with_data(vec![(seg_id, bad_data)]);

    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(Arc::clone(&registry), data_store).await.unwrap();

    assert_eq!(report.segments_total(), 1);
    assert_eq!(report.segments_healthy(), 0);
    assert_eq!(report.segments_corrupt(), 1);
}

#[tokio::test]
async fn manual_scrub_trigger_does_not_error() {
    let registry = make_registry();
    let data_store = segment_store_with_data(vec![]);
    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let result = coord.trigger_manual(Arc::clone(&registry), data_store).await;
    assert!(result.is_ok());
    // Give the spawned task a moment
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
}

#[tokio::test]
async fn run_cycle_with_missing_data_skips_not_corrupt() {
    let registry = make_registry();

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
    seed_sealed(&registry, seg);

    // Empty data store — data is missing (simulates a seal/GC race where
    // the shard is not yet on disk, or was reclaimed between the metadata
    // scan and the read). Missing shards are SKIPPED, not counted corrupt:
    // counting them corrupt produced false corruption alarms + spurious
    // heal requests for segments that were never corrupt.
    let data_store = segment_store_with_data(vec![]);

    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(Arc::clone(&registry), data_store).await.unwrap();

    assert_eq!(report.segments_total(), 1);
    assert_eq!(report.segments_healthy(), 0, "skipped segments are excluded from healthy");
    assert_eq!(report.segments_corrupt(), 0, "missing shard is not corruption");
}
