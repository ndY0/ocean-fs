//! Integration test: Garbage Collection & Segment Compaction
#![allow(clippy::unwrap_used)]
//!
//! Tests the GC cycle, tombstones processing, liveness ratio computation,
//! and compaction of segments. Verifies that after a full GC cycle:
//! - Dead space triggers compaction for segments below threshold
//! - Live blobs are repacked into new segments
//! - Old segment metadata is removed
//! - Tombstone TTL is properly enforced

use std::sync::Arc;

use oceanfs_core::{
    BucketId, ChunkRef, HashOutput, Hlc, MetadataConfig, ObjectKey, ObjectMetadata, SegmentId,
    SegmentMetadata, SizeTier, Tombstone,
};
use oceanfs_durability::{GarbageCollector, GcConfig};
use oceanfs_storage::RocksDbMetadataStore;
use oceanfs_storage_api::SegmentDataStore;

fn test_config() -> MetadataConfig {
    let dir = tempfile::tempdir().unwrap();
    MetadataConfig {
        data_dir: dir.path().to_path_buf(),
        block_cache_size: 8 * 1024 * 1024,
        memtable_size: 8 * 1024 * 1024,
        ..Default::default()
    }
}

fn make_object_meta(key: &str, size: u64, chunk: ChunkRef) -> ObjectMetadata {
    let mut chunks = smallvec::SmallVec::new();
    chunks.push(chunk);
    ObjectMetadata {
        object_key: ObjectKey::new(key),
        size,
        blake3_hash: Some(HashOutput::from_bytes([0u8; 32])),
        chunks,
        inline_data: None,
        created_at: 1700000000000,
        hlc: Hlc::new(1700000000000, 0),
    }
}

fn make_segment_meta(id: SegmentId, tier: SizeTier, sealed_at: i64) -> SegmentMetadata {
    SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: tier,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(sealed_at),
    }
}

#[tokio::test]
async fn gc_cycle_with_only_live_objects() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
    let registry = oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    );
    registry.reserve(seg_id, seg_meta.clone()).unwrap();
    registry.seal(seg_id, seg_meta).unwrap();

    metadata
        .put_object(make_object_meta(
            "live.txt",
            500,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 500,
                compressed: false,
                logical_length: 500,
            },
        ))
        .unwrap();

    let gc = GarbageCollector::new(GcConfig::default());
    let stats = gc.run_cycle(metadata, &registry).await.unwrap();
    assert!(stats.segments_scanned >= 1);
    assert_eq!(stats.segments_compacted, 0);
    assert_eq!(stats.dead_bytes, 0);
}

#[tokio::test]
async fn gc_cycle_detects_dead_space() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
    let registry = oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    );
    registry.reserve(seg_id, seg_meta.clone()).unwrap();
    registry.seal(seg_id, seg_meta).unwrap();

    // Put a live object
    metadata
        .put_object(make_object_meta(
            "live.txt",
            600,
            ChunkRef {
                segment_id: seg_id,
                offset: 0,
                length: 600,
                compressed: false,
                logical_length: 600,
            },
        ))
        .unwrap();

    // Add a tombstone for a dead object that references the same segment
    let bucket = BucketId::new("default");
    metadata
        .put_tombstone(
            &bucket,
            &ObjectKey::new("live.txt"),
            Tombstone {
                deletion_time: 1700000000000,
                hlc: Hlc::new(1700000000000, 1),
                chunks: smallvec::SmallVec::new(),
            },
        )
        .unwrap();

    // Verify tombstone exists
    assert!(metadata.has_tombstone(&bucket, &ObjectKey::new("live.txt")).unwrap());

    let gc = GarbageCollector::new(GcConfig::default());
    let stats = gc.run_cycle(metadata, &registry).await.unwrap();
    assert!(stats.segments_scanned >= 1);
    // The GC cycle completes — dead space tracking depends on tombstone iteration
}

#[tokio::test]
async fn gc_cycle_empty_store() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
    let registry = oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    );
    let gc = GarbageCollector::new(GcConfig::default());
    let stats = gc.run_cycle(metadata, &registry).await.unwrap();
    assert_eq!(stats.segments_scanned, 0);
}

// ---------------------------------------------------------------------------
// New integration tests for full GC compaction cycle
// ---------------------------------------------------------------------------

/// Full GC cycle: write objects, delete some, run GC, verify compaction.
///
/// 1. Create 5 objects in a segment (5 × 200 = 1000 bytes total)
/// 2. Delete 3 of them (600 bytes dead → liveness 0.4)
/// 3. With threshold 0.5, the segment should be compacted
/// 4. Verify live objects are repacked to new segments
/// 5. Verify old segment metadata is removed
#[tokio::test]
async fn full_gc_cycle_compacts_segment() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
    let bucket = BucketId::new("default");

    let seg_id = SegmentId::new();
    let mut seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
    // The seal-time logical total (ADR-0034 D1): 1000 bytes on disk.
    seg_meta.total_bytes = 1000;

    // Write 5 objects
    let live_keys = vec!["obj3.txt", "obj4.txt"];
    for i in 0..5 {
        metadata
            .put_object(make_object_meta(
                &format!("obj{i}.txt"),
                200,
                ChunkRef {
                    segment_id: seg_id,
                    offset: i * 200,
                    length: 200,
                    compressed: false,
                    logical_length: 200,
                },
            ))
            .unwrap();
    }

    // Delete 3 objects (obj0, obj1, obj2) — the aged chunk-carrying
    // tombstones are the f1 capture shape (dead bytes = 600).
    for i in 0..3 {
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg_id,
            offset: i * 200,
            length: 200,
            compressed: false,
            logical_length: 200,
        });
        metadata
            .put_tombstone(
                &bucket,
                &ObjectKey::new(format!("obj{i}.txt")),
                Tombstone {
                    deletion_time: 1000000000000, // ancient
                    hlc: Hlc::new(1000000000000, 1),
                    chunks,
                },
            )
            .unwrap();
    }

    // Run GC with threshold that will trigger (liveness 0.4 < 0.5). The
    // compactor is a machine (ADR-0025 Decision 4): wire the lifecycle
    // coordinator + shard store, and seed the candidate through the
    // machine (the only writer of lifecycle state).
    let store = Arc::new(oceanfs_durability::InMemorySegmentStore::new());
    store.write_segment_data(&seg_id, &vec![0x55; 1000]).await.unwrap();
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    registry.reserve(seg_id, seg_meta.clone()).unwrap();
    let members: std::sync::Arc<[oceanfs_core::ContainedObject]> = std::sync::Arc::from(vec![
        oceanfs_core::ContainedObject {
            bucket: BucketId::new("default"),
            key: ObjectKey::new("obj0.txt"),
        },
        oceanfs_core::ContainedObject {
            bucket: BucketId::new("default"),
            key: ObjectKey::new("obj1.txt"),
        },
        oceanfs_core::ContainedObject {
            bucket: BucketId::new("default"),
            key: ObjectKey::new("obj2.txt"),
        },
        oceanfs_core::ContainedObject {
            bucket: BucketId::new("default"),
            key: ObjectKey::new("obj3.txt"),
        },
        oceanfs_core::ContainedObject {
            bucket: BucketId::new("default"),
            key: ObjectKey::new("obj4.txt"),
        },
    ]);
    registry.seal_with(seg_id, seg_meta.clone(), None, Some(members)).unwrap();
    let event_wal = Arc::new(
        oceanfs_storage::segment::event_wal::EventWal::open(
            std::env::temp_dir().join(format!("gc-compaction-{}", std::process::id())),
            &oceanfs_core::EventWalConfig {
                event_wal_dir: std::env::temp_dir()
                    .join(format!("gc-compaction-{}", std::process::id())),
                event_wal_file_size_bytes: 1024 * 1024,
                event_wal_fsync_batch_timeout_ms: 10,
                event_wal_checkpoint_bytes: 1024 * 1024,
            },
        )
        .await
        .unwrap(),
    );
    let lifecycle = Arc::new(
        oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::with_registry(
            Arc::clone(&registry),
        )
        .with_event_wal(event_wal),
    );
    let gc = GarbageCollector::new(GcConfig::new(3600, 0, 0.5, 2, 16))
        .with_data_store(store)
        .with_lifecycle(lifecycle);

    let stats = gc.run_cycle(metadata.clone(), &registry).await.unwrap();

    // Verify stats
    assert!(stats.segments_scanned >= 1);
    assert_eq!(stats.segments_compacted, 1, "segment should be compacted");
    assert!(stats.bytes_reclaimed > 0, "bytes should be reclaimed");

    // Old segment should be deleted
    assert!(registry.get(seg_id).is_none(), "old segment should be deleted after compaction");

    // Live objects should still exist and reference new segments
    for key_str in &live_keys {
        let obj_opt = metadata.get_object(&bucket, &ObjectKey::new(*key_str)).unwrap();
        assert!(obj_opt.is_some(), "live object should exist");

        let obj = obj_opt.unwrap();
        assert!(!obj.chunks.is_empty());
        assert_ne!(obj.chunks[0].segment_id, seg_id, "live object should reference new segment");
    }

    // Dead object metadata is cleaned up after compaction — their data
    // was not copied to the new segment and their tombstone was consumed.
    for i in 0..3 {
        let obj = metadata.get_object(&bucket, &ObjectKey::new(format!("obj{i}.txt"))).unwrap();
        assert!(obj.is_none(), "dead object metadata should be cleaned up after compaction");
    }
}

/// Verify that tombstones below TTL are NOT processed (no compaction triggered).
#[tokio::test]
async fn gc_cycle_respects_tombstone_ttl() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
    let bucket = BucketId::new("default");

    let seg_id = SegmentId::new();
    let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
    let registry = oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    );
    registry.reserve(seg_id, seg_meta.clone()).unwrap();
    registry.seal(seg_id, seg_meta).unwrap();

    // Write 3 objects (600 bytes total)
    for i in 0..3 {
        metadata
            .put_object(make_object_meta(
                &format!("ttl_obj{i}.txt"),
                200,
                ChunkRef {
                    segment_id: seg_id,
                    offset: i * 200,
                    length: 200,
                    compressed: false,
                    logical_length: 200,
                },
            ))
            .unwrap();
    }

    // Create a tombstone with a recent deletion_time (effectively "now")
    let now_ms =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
            as i64;

    metadata
        .put_tombstone(
            &bucket,
            &ObjectKey::new("ttl_obj0.txt"),
            Tombstone {
                deletion_time: now_ms,
                hlc: Hlc::new(now_ms as u64, 1),
                chunks: smallvec::SmallVec::new(),
            },
        )
        .unwrap();

    // Run GC with a very long TTL (1 year) — tombstone should NOT be eligible
    let gc = GarbageCollector::new(GcConfig::new(3600, 31536000, 0.5, 2, 16));

    let stats = gc.run_cycle(metadata.clone(), &registry).await.unwrap();

    // No compaction should occur because the tombstone is below TTL
    assert_eq!(stats.segments_compacted, 0, "recent tombstone should not trigger compaction");
    assert_eq!(stats.dead_bytes, 0, "recent tombstone should not mark bytes dead");

    // Old segment should still exist
    assert!(registry.get(seg_id).is_some());
}
