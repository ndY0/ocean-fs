//! Integration test: Segment metadata lifecycle.
//!
//! Verifies C3-storage and D1: segment metadata is created on seal
//! and is visible to the admin segments endpoint via `list_segments`.
//!
//! ## Tests
//! - `put_and_list_segment_metadata_roundtrip`
//! - `sealed_segment_produces_metadata_in_rocksdb`
//! - `list_segments_returns_empty_for_new_store`
//! - `multiple_segments_all_listed`

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::{
    MetadataConfig, SegmentId, SegmentMetadata, SegmentSizeConfig, SizeTier, WalConfig,
};
use oceanfs_storage::{
    segment::buffer::ActiveSegment, BufferPool, RocksDbMetadataStore, SealConfig, SegmentSealer,
    WalWriter,
};

// ---------------------------------------------------------------------------
// Test 1: Segment metadata round-trip (put → get → list)
// ---------------------------------------------------------------------------

#[test]
fn put_and_list_segment_metadata_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);
    let segment_id = SegmentId::new();

    let meta = SegmentMetadata {
        segment_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1_000_000),
    };

    // Initially empty.
    let initial = store.list_segments();
    assert!(initial.iter().all(|r| r.is_ok()), "all entries should be valid");
    let initial_count: Vec<_> = initial.into_iter().filter_map(|r| r.ok()).collect();
    assert_eq!(initial_count.len(), 0, "new store should have zero segments");

    // Store a segment.
    store.put_segment(meta.clone()).expect("put_segment should succeed");

    // Get it back.
    let fetched =
        store.get_segment(segment_id).expect("get_segment should succeed").expect("should exist");
    assert_eq!(fetched.segment_id, segment_id);
    assert_eq!(fetched.ec_k, 4);
    assert_eq!(fetched.ec_m, 2);
    assert_eq!(fetched.size_tier, SizeTier::Standard);
    assert!(fetched.is_sealed());

    // List should now include it.
    let listed: Vec<SegmentMetadata> =
        store.list_segments().into_iter().filter_map(|r| r.ok()).collect();
    assert_eq!(listed.len(), 1, "should have exactly one segment after put");
    assert_eq!(listed[0].segment_id, segment_id);
}

// ---------------------------------------------------------------------------
// Test 3: Filling a segment → seal → SegmentMetadata in RocksDB
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sealed_segment_produces_metadata_in_rocksdb() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    let size_config = SegmentSizeConfig {
        // Tiny target so we can fill it quickly.
        default_target_size: 100,
        small_target_size: 100,
        ..SegmentSizeConfig::default()
    };
    let pool = BufferPool::new(65536, 4);

    let wal = Arc::new(
        WalWriter::open(&WalConfig {
            data_dir: dir.path().join("wal"),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            ..Default::default()
        })
        .await
        .unwrap(),
    );

    let seal_config = SealConfig {
        target_size_bytes: size_config.default_target_size,
        seal_timeout_ms: 5000,
        data_dir: dir.path().join("segments"),
        io_mode: oceanfs_storage::io::IoReadMode::Buffered,
        write_mode: oceanfs_storage::io::SegmentWriteMode::Rename,
        ..Default::default()
    };
    let lifecycle =
        Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
            store.clone(),
            &oceanfs_core::LifecycleConfig::default(),
        ));
    let sealer = SegmentSealer::new(seal_config, wal, lifecycle.clone());

    // Create an active segment and fill it.
    let mut active =
        ActiveSegment::new(SizeTier::Standard, &size_config, &pool).expect("create active segment");
    assert!(!active.is_full(), "segment should not be full initially");

    // Append data larger than target (100 bytes).
    active.append(&[0xAAu8; 120]).expect("append should succeed");
    assert!(active.is_full(), "segment should be full after 120 byte append");

    // Seal it — this should write SegmentMetadata to RocksDB.
    let segment_id = active.id();
    let entries =
        vec![oceanfs_core::SegmentIndexEntry { offset: 0, length: 120, blob_key_hash: [0xBB; 32] }];
    // The flush path seals Reserved-only — reserve before sealing.
    lifecycle
        .request_reserve(segment_id, SizeTier::Standard, 0, 0)
        .await
        .expect("reserve should succeed");
    let handle = sealer.try_seal(&mut active, 0, &entries).await.expect("seal should succeed");
    assert!(handle.is_some(), "seal should return a handle for full segment");

    // Verify the metadata was persisted.
    let fetched = store
        .get_segment(segment_id)
        .expect("get_segment should succeed")
        .expect("segment metadata should exist after seal");
    assert_eq!(fetched.segment_id, segment_id);
    assert_eq!(fetched.size_tier, SizeTier::Standard);
    assert!(fetched.is_sealed(), "segment should be marked as sealed");

    // List should include it.
    let listed: Vec<SegmentMetadata> =
        store.list_segments().into_iter().filter_map(|r| r.ok()).collect();
    assert!(!listed.is_empty(), "list_segments should return at least one entry after seal");

    let found = listed.iter().find(|m| m.segment_id == segment_id);
    assert!(found.is_some(), "sealed segment should appear in list_segments output");

    // Verify the admin endpoint would return segment_count > 0 (D1).
    let total: u64 = listed.len() as u64;
    let sealed: u64 = listed.iter().filter(|m| m.is_sealed()).count() as u64;
    assert!(total > 0, "D1: total segment count should be > 0 after seal");
    assert!(sealed > 0, "D1: sealed segment count should be > 0 after seal");
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[test]
fn list_segments_returns_empty_for_new_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);
    let list = store.list_segments();
    let ok: Vec<_> = list.into_iter().filter_map(|r| r.ok()).collect();
    assert!(ok.is_empty(), "new store should have no segments");
}

#[test]
fn multiple_segments_all_listed() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir);

    for _ in 0..5 {
        let segment_id = SegmentId::new();
        store
            .put_segment(SegmentMetadata {
                segment_id,
                ec_k: 0,
                ec_m: 0,
                size_tier: SizeTier::Small,
                merkle_root: None,
                storage_locations: smallvec::SmallVec::new(),
                sealed_at: Some(1),
            })
            .expect("put_segment should succeed");
    }

    let listed: Vec<SegmentMetadata> =
        store.list_segments().into_iter().filter_map(|r| r.ok()).collect();
    assert_eq!(listed.len(), 5, "should list all 5 segments");

    // Verify each has a unique ID.
    let ids: Vec<SegmentId> = listed.iter().map(|m| m.segment_id).collect();
    let mut unique = ids.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 5, "all segment IDs should be unique");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_store(dir: &tempfile::TempDir) -> Arc<RocksDbMetadataStore> {
    let config = MetadataConfig {
        data_dir: dir.path().join("meta"),
        block_cache_size: 8 * 1024 * 1024,
        memtable_size: 64 * 1024 * 1024,
        ..Default::default()
    };
    Arc::new(RocksDbMetadataStore::open(&config).expect("failed to open store"))
}
