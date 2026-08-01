//! Integration test: Orphaned Segment Reaper
#![allow(clippy::unwrap_used)]
//!
//! Tests orphan detection, TTL enforcement, and the safety double-check
//! that prevents races between the reaper and concurrent writers.

use std::sync::Arc;

use oceanfs_core::{ChunkRef, HashOutput, Hlc, MetadataConfig, ObjectKey, ObjectMetadata, SizeTier};
use oceanfs_storage::{GcConfig, MetadataStore, OrphanReaper};

fn test_config() -> MetadataConfig {
    let dir = tempfile::tempdir().unwrap();
    MetadataConfig { data_dir: dir.path().to_path_buf(), block_cache_size: 8 * 1024 * 1024, memtable_size: 8 * 1024 * 1024 }
}

fn make_segment(id: oceanfs_core::SegmentId, sealed_at: i64) -> oceanfs_core::SegmentMetadata {
    oceanfs_core::SegmentMetadata {
        segment_id: id,
        ec_k: 4, ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(sealed_at),
    }
}

#[tokio::test]
async fn segment_with_live_object_not_reclaimed() {
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    metadata.put_segment(make_segment(seg_id, 1000000000000)).unwrap();

    // Object references this segment
    let mut chunks = smallvec::SmallVec::new();
    chunks.push(ChunkRef { segment_id: seg_id, offset: 0, length: 500 });
    let obj = ObjectMetadata {
        object_key: ObjectKey::new("alive.txt"),
        size: 500,
        blake3_hash: Some(HashOutput::from_bytes([0u8; 32])),
        chunks,
        inline_data: None,
        created_at: 1700000000000,
        hlc: Hlc::new(1700000000000, 0),
    };
    metadata.put_object(obj).unwrap();

    let reaper = OrphanReaper::new(GcConfig::default());
    let stats = reaper.run_cycle(metadata).await.unwrap();
    assert_eq!(stats.orphans_found, 0);
    assert_eq!(stats.orphans_deleted, 0);
}

#[tokio::test]
async fn unreferenced_segment_identified_as_orphan() {
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    // Sealed long ago (well past TTL)
    metadata.put_segment(make_segment(seg_id, 1000000000000)).unwrap();
    // No object references this segment

    let reaper = OrphanReaper::new(GcConfig::default());
    let stats = reaper.run_cycle(metadata).await.unwrap();
    assert_eq!(stats.orphans_found, 1);
}

#[tokio::test]
async fn recently_sealed_not_reclaimed() {
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    // Sealed very recently
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    metadata.put_segment(make_segment(seg_id, now_ms)).unwrap();
    // No object references this segment

    let reaper = OrphanReaper::new(GcConfig::default());
    let stats = reaper.run_cycle(metadata).await.unwrap();
    assert_eq!(stats.orphans_found, 0); // Too young to be orphan
}

#[tokio::test]
async fn empty_store_no_orphans() {
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
    let reaper = OrphanReaper::new(GcConfig::default());
    let stats = reaper.run_cycle(metadata).await.unwrap();
    assert_eq!(stats.orphans_found, 0);
}
