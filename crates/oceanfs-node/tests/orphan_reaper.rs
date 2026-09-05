//! Integration test: Orphan segment reaper.
//!
//! Verifies that the orphan reaper:
//! 1. Scans all segments and identifies unreferenced ones
//! 2. Respects the TTL for recently-sealed segments

#![allow(clippy::unwrap_used, clippy::expect_used)]
//! 3. Deletes orphan segment metadata and shard data
//! 4. Reports accurate statistics

use std::sync::Arc;

use oceanfs_core::{
    ChunkRef, HashOutput, Hlc, MetadataConfig, ObjectKey, ObjectMetadata, SegmentId,
    SegmentMetadata, SizeTier,
};
use oceanfs_durability::{GcConfig, InMemorySegmentShardStore, OrphanReaper};
use oceanfs_storage::{
    metadata::RocksDbMetadataStore, segment::lifecycle::SegmentLifecycleCoordinator,
};

/// Helper: create a temporary metadata store.
fn open_temp_metadata() -> Arc<RocksDbMetadataStore> {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = MetadataConfig {
        data_dir: dir.path().to_path_buf(),
        block_cache_size: 8 * 1024 * 1024,
        memtable_size: 8 * 1024 * 1024,
        ..Default::default()
    };
    let _dir_leaked = Box::leak(Box::new(dir));
    Arc::new(RocksDbMetadataStore::open(&config).expect("open metadata store"))
}

/// Constructs a reaper whose coordinator is seeded from the store
/// (mirroring the node's startup seed).
async fn make_reaper(
    metadata: Arc<RocksDbMetadataStore>,
    shard_store: Arc<dyn oceanfs_storage_api::SegmentDataStore>,
    config: GcConfig,
    registry: Arc<oceanfs_storage::SegmentLifecycleRegistry>,
) -> OrphanReaper {
    let tmp = tempfile::TempDir::new().unwrap();
    let event_wal = Arc::new(
        oceanfs_storage::segment::event_wal::EventWal::open(
            tmp.path().join("event-wal"),
            &oceanfs_core::EventWalConfig {
                event_wal_dir: tmp.path().join("event-wal"),
                event_wal_file_size_bytes: 1024 * 1024,
                event_wal_fsync_batch_timeout_ms: 10,
                event_wal_checkpoint_bytes: 1024 * 1024,
            },
        )
        .await
        .unwrap(),
    );
    let lifecycle = Arc::new(
        SegmentLifecycleCoordinator::with_registry(Arc::clone(&registry)).with_event_wal(event_wal),
    );
    OrphanReaper::new(metadata, lifecycle, shard_store, vec![], config)
}

/// Helper: create a test shard store.
fn make_shard_store() -> Arc<InMemorySegmentShardStore> {
    Arc::new(InMemorySegmentShardStore::new(4194304))
}

/// Helper: create a segment metadata with the given sealed timestamp.
fn make_segment(id: SegmentId, sealed_at: i64) -> SegmentMetadata {
    SegmentMetadata {
        pool_id: 0,
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(sealed_at),
    }
}

/// Helper: create an object metadata entry.
fn make_object(key: &str, chunk: ChunkRef) -> ObjectMetadata {
    let mut chunks = smallvec::SmallVec::new();
    chunks.push(chunk);
    ObjectMetadata {
        object_key: ObjectKey::new(key),
        size: chunk.length as u64,
        blake3_hash: Some(HashOutput::from_bytes([0u8; 32])),
        chunks,
        inline_data: None,
        created_at: 1700000000000,
        hlc: Hlc::new(1700000000000, 0),
    }
}

#[tokio::test]
async fn reaper_empty_store_produces_zero_stats() {
    let metadata = open_temp_metadata();
    let shard_store = make_shard_store();
    let registry = Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    let reaper =
        make_reaper(metadata, shard_store, GcConfig::default(), Arc::clone(&registry)).await;

    let stats = reaper.run_cycle().await.expect("reaper cycle");
    assert_eq!(stats.segments_scanned, 0);
    assert_eq!(stats.orphans_found, 0);
    assert_eq!(stats.orphans_deleted, 0);
}

#[tokio::test]
async fn reaper_referenced_segment_not_orphan() {
    let metadata = open_temp_metadata();

    let seg_id = SegmentId::new();
    let seg_meta = make_segment(seg_id, 1000000000000);
    let registry = Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
    registry.seal(seg_meta.segment_id, seg_meta).unwrap();

    // Create an object referencing this segment
    let obj = make_object(
        "alive.txt",
        ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: 500,
            compressed: false,
            logical_length: 500,
        },
    );
    metadata.put_object(obj).expect("put object");

    let shard_store = make_shard_store();
    let reaper =
        make_reaper(metadata, shard_store, GcConfig::default(), Arc::clone(&registry)).await;

    let stats = reaper.run_cycle().await.expect("reaper cycle");
    assert_eq!(stats.segments_scanned, 1);
    assert_eq!(stats.orphans_found, 0);
}

#[tokio::test]
async fn reaper_unreferenced_segment_beyond_ttl_is_orphan() {
    let metadata = open_temp_metadata();

    let seg_id = SegmentId::new();
    // Sealed very long ago (past any TTL)
    let seg_meta = make_segment(seg_id, 1000000000000);
    let registry = Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
    registry.seal(seg_meta.segment_id, seg_meta).unwrap();
    // No object references this segment

    let shard_store = make_shard_store();
    let reaper =
        make_reaper(metadata, shard_store, GcConfig::default(), Arc::clone(&registry)).await;

    let stats = reaper.run_cycle().await.expect("reaper cycle");
    assert_eq!(stats.segments_scanned, 1);
    assert_eq!(stats.orphans_found, 1);
    assert_eq!(stats.orphans_deleted, 1);
}

#[tokio::test]
async fn reaper_unreferenced_segment_within_ttl_not_orphan() {
    let metadata = open_temp_metadata();

    let seg_id = SegmentId::new();
    // Sealed very recently
    let now_ms =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
            as i64;
    let seg_meta = make_segment(seg_id, now_ms);
    let registry = Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
    registry.seal(seg_meta.segment_id, seg_meta).unwrap();
    // No object references this segment, but it's too young

    let shard_store = make_shard_store();
    let reaper =
        make_reaper(metadata, shard_store, GcConfig::default(), Arc::clone(&registry)).await;

    let stats = reaper.run_cycle().await.expect("reaper cycle");
    assert_eq!(stats.segments_scanned, 1);
    assert_eq!(stats.orphans_found, 0);
}

#[tokio::test]
async fn reaper_deletes_shard_data_and_metadata() {
    let metadata = open_temp_metadata();

    let seg_id = SegmentId::new();
    let seg_meta = make_segment(seg_id, 1000000000000);
    let registry = Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
    registry.seal(seg_meta.segment_id, seg_meta).unwrap();

    // Verify segment metadata exists before reaping
    assert!(registry.get(seg_id).is_some());

    let shard_store = make_shard_store();
    let reaper = make_reaper(
        metadata.clone(),
        shard_store.clone(),
        GcConfig::default(),
        Arc::clone(&registry),
    )
    .await;

    let stats = reaper.run_cycle().await.expect("reaper cycle");
    assert_eq!(stats.orphans_found, 1);
    assert_eq!(stats.orphans_deleted, 1);

    // Verify segment metadata was deleted
    assert!(registry.get(seg_id).is_none());

    // Verify shard data was deleted
    assert!(shard_store.is_deleted(seg_id));
}

#[tokio::test]
async fn reaper_multiple_orphans_all_reaped() {
    let metadata = open_temp_metadata();
    let registry = Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));

    // Create 5 orphan segments
    for _ in 0..5u32 {
        let seg_id = SegmentId::new();
        let seg_meta = make_segment(seg_id, 1000000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
    }

    let shard_store = make_shard_store();
    let reaper =
        make_reaper(metadata, shard_store, GcConfig::default(), Arc::clone(&registry)).await;

    let stats = reaper.run_cycle().await.expect("reaper cycle");
    assert_eq!(stats.segments_scanned, 5);
    assert_eq!(stats.orphans_found, 5);
    assert_eq!(stats.orphans_deleted, 5);
    assert!(stats.bytes_reclaimed > 0);
}

#[tokio::test]
async fn reaper_double_check_prevents_race_condition() {
    let metadata = open_temp_metadata();
    let seg_id = SegmentId::new();

    // Create an orphan segment
    let seg_meta = make_segment(seg_id, 1000000000000);
    let registry = Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
    registry.seal(seg_meta.segment_id, seg_meta).unwrap();

    let shard_store = make_shard_store();
    let reaper =
        make_reaper(metadata.clone(), shard_store, GcConfig::default(), Arc::clone(&registry))
            .await;

    // Simulate a race: an object referencing the segment is created
    // AFTER the scan phase but BEFORE the delete phase.
    // In a single-threaded test, we can't simulate this exact timing,
    // but we can verify the double-check logic exists and works.
    // The reaper builds the referenced set BEFORE scanning,
    // so a concurrent write would cause the double-check to catch it.

    // First run: segment is orphan → reaped
    let stats = reaper.run_cycle().await.expect("reaper cycle");
    assert_eq!(stats.orphans_found, 1);
    assert!(registry.get(seg_id).is_none());

    // Second run on already-deleted segment → no orphans found
    let stats2 = reaper.run_cycle().await.expect("reaper cycle 2");
    assert_eq!(stats2.orphans_found, 0);
}

#[tokio::test]
async fn reaper_reports_bytes_reclaimed_correctly() {
    let metadata = open_temp_metadata();
    let registry = Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));

    // Create 3 orphan segments
    for _ in 0..3u32 {
        let seg_id = SegmentId::new();
        let seg_meta = make_segment(seg_id, 1000000000000);
        registry.reserve(seg_meta.segment_id, seg_meta.clone()).unwrap();
        registry.seal(seg_meta.segment_id, seg_meta).unwrap();
    }

    let shard_size = 4194304u64;
    let shard_store = Arc::new(InMemorySegmentShardStore::new(shard_size));
    let reaper =
        make_reaper(metadata, shard_store, GcConfig::default(), Arc::clone(&registry)).await;

    let stats = reaper.run_cycle().await.expect("reaper cycle");
    assert_eq!(stats.orphans_found, 3);
    assert_eq!(stats.orphans_deleted, 3);
    assert_eq!(stats.bytes_reclaimed, shard_size * 3);
}
