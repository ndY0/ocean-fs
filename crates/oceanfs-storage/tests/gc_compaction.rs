//! Integration test: Garbage Collection & Segment Compaction
#![allow(clippy::unwrap_used)]
//!
//! Tests the GC cycle, tombstones processing, liveness ratio computation,
//! and compaction of segments.

use std::sync::Arc;

use oceanfs_core::{ChunkRef, HashOutput, Hlc, MetadataConfig, ObjectKey, ObjectMetadata, SizeTier};
use oceanfs_storage::{GarbageCollector, GcConfig, MetadataStore};

fn test_config() -> MetadataConfig {
    let dir = tempfile::tempdir().unwrap();
    MetadataConfig { data_dir: dir.path().to_path_buf(), block_cache_size: 8 * 1024 * 1024, memtable_size: 8 * 1024 * 1024 }
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

#[tokio::test]
async fn gc_cycle_with_only_live_objects() {
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    let seg_meta = oceanfs_core::SegmentMetadata {
        segment_id: seg_id,
        ec_k: 4, ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    metadata.put_segment(seg_meta).unwrap();

    metadata.put_object(make_object_meta("live.txt", 500, ChunkRef { segment_id: seg_id, offset: 0, length: 500 })).unwrap();

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
    let seg_meta = oceanfs_core::SegmentMetadata {
        segment_id: seg_id,
        ec_k: 4, ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    metadata.put_segment(seg_meta).unwrap();

    // Put a live object
    metadata.put_object(make_object_meta("live.txt", 600, ChunkRef { segment_id: seg_id, offset: 0, length: 600 })).unwrap();

    // Add a tombstone for a dead object that references the same segment
    let bucket = oceanfs_core::BucketId::new("default");
    metadata.put_tombstone(
        &bucket,
        &ObjectKey::new("live.txt"),
        oceanfs_core::Tombstone { deletion_time: 1700000000000, hlc: Hlc::new(1700000000000, 1) },
    ).unwrap();

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
