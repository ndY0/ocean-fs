//! Integration test: Distributed Scrubbing
#![allow(clippy::unwrap_used)]
//!
//! Tests partition assignment, segment verification, scrub cycle,
//! and manual trigger functionality.

use std::sync::Arc;

use oceanfs_core::{HashOutput, MetadataConfig, NodeId, SegmentId, SegmentMetadata, SizeTier};
use oceanfs_storage::{MetadataStore, ScrubConfig, ScrubCoordinator};

fn test_config() -> MetadataConfig {
    let dir = tempfile::tempdir().unwrap();
    MetadataConfig {
        data_dir: dir.path().to_path_buf(),
        block_cache_size: 8 * 1024 * 1024,
        memtable_size: 8 * 1024 * 1024,
    }
}

#[test]
fn partition_assignment_covers_all_segments() {
    let seg_ids: Vec<SegmentId> = (0..7).map(|_| SegmentId::new()).collect();
    let node_ids: Vec<NodeId> = (0..3).map(|i| NodeId::new(format!("n{i}"))).collect();

    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let partitions = coord.partition_segments(&seg_ids, &node_ids);

    assert_eq!(partitions.len(), 3);
    let total: usize = partitions.iter().map(|p| p.segment_ids.len()).sum();
    assert_eq!(total, 7);
}

#[test]
fn partition_assignment_no_overlap() {
    let seg_ids: Vec<SegmentId> = (0..5).map(|_| SegmentId::new()).collect();
    let node_ids: Vec<NodeId> = (0..2).map(|i| NodeId::new(format!("n{i}"))).collect();

    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let partitions = coord.partition_segments(&seg_ids, &node_ids);

    let mut seen = std::collections::HashSet::new();
    for p in &partitions {
        for id in &p.segment_ids {
            assert!(seen.insert(*id), "segment appears in multiple partitions");
        }
    }
}

#[tokio::test]
async fn scrub_cycle_on_empty_store() {
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(metadata).await.unwrap();
    assert_eq!(report.segments_total, 0);
    assert_eq!(report.segments_healthy, 0);
}

#[tokio::test]
async fn scrub_cycle_verifies_segments() {
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());

    // Put 3 segments
    for _ in 0..3 {
        let seg = SegmentMetadata {
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(HashOutput::from_bytes([0u8; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };
        metadata.put_segment(seg).unwrap();
    }

    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(metadata).await.unwrap();
    assert_eq!(report.segments_total, 3);
    assert_eq!(report.segments_healthy, 3);
    assert_eq!(report.segments_corrupt, 0);
    assert!(report.duration_sec >= 0.0);
}

#[tokio::test]
async fn manual_scrub_trigger_does_not_error() {
    let metadata = Arc::new(MetadataStore::open(&test_config()).unwrap());
    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let result = coord.trigger_manual(metadata).await;
    assert!(result.is_ok());
}
