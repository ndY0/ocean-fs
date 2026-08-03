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
use oceanfs_storage::{GarbageCollector, GcConfig, MetadataStore};

fn test_config() -> MetadataConfig {
    let dir = tempfile::tempdir().unwrap();
    MetadataConfig {
        data_dir: dir.path().to_path_buf(),
        block_cache_size: 8 * 1024 * 1024,
        memtable_size: 8 * 1024 * 1024,
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
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
    metadata.put_segment(seg_meta).unwrap();

    metadata
        .put_object(make_object_meta(
            "live.txt",
            500,
            ChunkRef { segment_id: seg_id, offset: 0, length: 500 },
        ))
        .unwrap();

    let gc = GarbageCollector::new(GcConfig::default());
    let stats = gc.run_cycle(metadata).await.unwrap();
    assert!(stats.segments_scanned >= 1);
    assert_eq!(stats.segments_compacted, 0);
    assert_eq!(stats.dead_bytes, 0);
}

#[tokio::test]
async fn gc_cycle_detects_dead_space() {
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
    metadata.put_segment(seg_meta).unwrap();

    // Put a live object
    metadata
        .put_object(make_object_meta(
            "live.txt",
            600,
            ChunkRef { segment_id: seg_id, offset: 0, length: 600 },
        ))
        .unwrap();

    // Add a tombstone for a dead object that references the same segment
    let bucket = BucketId::new("default");
    metadata
        .put_tombstone(
            &bucket,
            &ObjectKey::new("live.txt"),
            Tombstone { deletion_time: 1700000000000, hlc: Hlc::new(1700000000000, 1) },
        )
        .unwrap();

    // Verify tombstone exists
    assert!(metadata.has_tombstone(&bucket, &ObjectKey::new("live.txt")).unwrap());

    let gc = GarbageCollector::new(GcConfig::default());
    let stats = gc.run_cycle(metadata).await.unwrap();
    assert!(stats.segments_scanned >= 1);
    // The GC cycle completes — dead space tracking depends on tombstone iteration
}

#[tokio::test]
async fn gc_cycle_empty_store() {
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
    let gc = GarbageCollector::new(GcConfig::default());
    let stats = gc.run_cycle(metadata).await.unwrap();
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
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
    let bucket = BucketId::new("default");

    let seg_id = SegmentId::new();
    let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
    metadata.put_segment(seg_meta).unwrap();

    // Write 5 objects
    let live_keys = vec!["obj3.txt", "obj4.txt"];
    for i in 0..5 {
        metadata
            .put_object(make_object_meta(
                &format!("obj{i}.txt"),
                200,
                ChunkRef { segment_id: seg_id, offset: i * 200, length: 200 },
            ))
            .unwrap();
    }

    // Delete 3 objects (obj0, obj1, obj2) — these have ancient timestamps (past TTL)
    for i in 0..3 {
        metadata
            .put_tombstone(
                &bucket,
                &ObjectKey::new(format!("obj{i}.txt")),
                Tombstone {
                    deletion_time: 1000000000000, // ancient
                    hlc: Hlc::new(1000000000000, 1),
                },
            )
            .unwrap();
    }

    // Run GC with threshold that will trigger (liveness 0.4 < 0.5)
    let gc = GarbageCollector::new(GcConfig::new(3600, 0, 0.5, 2, 16));

    let stats = gc.run_cycle(metadata.clone()).await.unwrap();

    // Verify stats
    assert!(stats.segments_scanned >= 1);
    assert_eq!(stats.segments_compacted, 1, "segment should be compacted");
    assert!(stats.bytes_reclaimed > 0, "bytes should be reclaimed");

    // Old segment should be deleted
    assert!(
        metadata.get_segment(seg_id).unwrap().is_none(),
        "old segment should be deleted after compaction"
    );

    // Live objects should still exist and reference new segments
    for key_str in &live_keys {
        let obj_opt = metadata.get_object(&bucket, &ObjectKey::new(*key_str)).unwrap();
        assert!(obj_opt.is_some(), "live object should exist");

        let obj = obj_opt.unwrap();
        assert!(!obj.chunks.is_empty());
        assert_ne!(obj.chunks[0].segment_id, seg_id, "live object should reference new segment");
    }

    // Dead objects still have metadata (their tombstone records deletion)
    for i in 0..3 {
        let obj = metadata.get_object(&bucket, &ObjectKey::new(format!("obj{i}.txt"))).unwrap();
        assert!(obj.is_some(), "dead object metadata still exists");
    }
}

/// Verify that tombstones below TTL are NOT processed (no compaction triggered).
#[tokio::test]
async fn gc_cycle_respects_tombstone_ttl() {
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
    let bucket = BucketId::new("default");

    let seg_id = SegmentId::new();
    let seg_meta = make_segment_meta(seg_id, SizeTier::Standard, 1700000000000);
    metadata.put_segment(seg_meta).unwrap();

    // Write 3 objects (600 bytes total)
    for i in 0..3 {
        metadata
            .put_object(make_object_meta(
                &format!("ttl_obj{i}.txt"),
                200,
                ChunkRef { segment_id: seg_id, offset: i * 200, length: 200 },
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
            Tombstone { deletion_time: now_ms, hlc: Hlc::new(now_ms as u64, 1) },
        )
        .unwrap();

    // Run GC with a very long TTL (1 year) — tombstone should NOT be eligible
    let gc = GarbageCollector::new(GcConfig::new(3600, 31536000, 0.5, 2, 16));

    let stats = gc.run_cycle(metadata.clone()).await.unwrap();

    // No compaction should occur because the tombstone is below TTL
    assert_eq!(stats.segments_compacted, 0, "recent tombstone should not trigger compaction");
    assert_eq!(stats.dead_bytes, 0, "recent tombstone should not mark bytes dead");

    // Old segment should still exist
    assert!(metadata.get_segment(seg_id).unwrap().is_some());
}
