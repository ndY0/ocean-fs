//! Integration test: Segment lifecycle through the machine.
//!
//! Verifies C3-storage and D1 with the `segments` CF removed (ADR-0025
//! Decision 3): segment state is created on seal, lives in the lifecycle
//! registry (the event log is the only durable writer), and is
//! enumerable for the admin segments endpoint via the registry — never
//! through RocksDB.
//!
//! ## Tests
//! - `machine_segment_lifecycle_roundtrip`
//! - `sealed_segment_produces_a_machine_entry`
//! - `registry_returns_empty_for_new_store`
//! - `multiple_segments_all_enumerated`

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::{
    EventWalConfig, LifecycleConfig, SegmentId, SegmentMetadata, SegmentSizeConfig, SizeTier,
    WalConfig,
};
use oceanfs_storage::{
    segment::{
        buffer::ActiveSegment,
        event_wal::EventWal,
        lifecycle::{SegmentLifecycleCoordinator, SegmentLifecycleRegistry, SegmentState},
    },
    BufferPool, RocksDbMetadataStore, SealConfig, SegmentSealer, WalWriter,
};

fn make_store(dir: &tempfile::TempDir) -> RocksDbMetadataStore {
    RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
        data_dir: dir.path().join("meta"),
        block_cache_size: 1024,
        memtable_size: 1024,
        ..Default::default()
    })
    .unwrap()
}

async fn make_machine(
    dir: &tempfile::TempDir,
) -> (Arc<SegmentLifecycleRegistry>, Arc<SegmentLifecycleCoordinator>) {
    let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
    let event_wal = Arc::new(
        EventWal::open(
            dir.path().join("event-wal"),
            &EventWalConfig {
                event_wal_dir: dir.path().join("event-wal"),
                event_wal_file_size_bytes: 1024 * 1024,
                event_wal_fsync_batch_timeout_ms: 10,
                event_wal_checkpoint_bytes: 1024 * 1024,
            },
        )
        .await
        .unwrap(),
    );
    let coordinator = Arc::new(
        SegmentLifecycleCoordinator::with_registry(Arc::clone(&registry)).with_event_wal(event_wal),
    );
    (registry, coordinator)
}

// ---------------------------------------------------------------------------
// Test 1: Lifecycle round-trip through the machine (reserve → seal → enumerate)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn machine_segment_lifecycle_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (_registry, coordinator) = make_machine(&dir).await;

    let segment_id = SegmentId::new();
    coordinator.request_reserve(segment_id, SizeTier::Standard, 4, 2).await.unwrap();

    // Reserved: entry present, enumerable, not sealed.
    let mut live: Vec<SegmentId> = Vec::new();
    coordinator.registry().for_each(|id, _entry| live.push(id));
    assert_eq!(live, vec![segment_id], "the reserve is enumerable");
    assert_eq!(coordinator.registry().get(segment_id).unwrap().state, SegmentState::Reserved);

    // Seal with full metadata.
    let meta = SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xAB; 32])),
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1_700_000_000_000),
    };
    coordinator.request_seal(segment_id, meta.clone(), None).await.unwrap();

    let entry = coordinator.registry().get(segment_id).unwrap();
    assert_eq!(entry.state, SegmentState::Sealed);
    assert_eq!(entry.metadata.merkle_root, meta.merkle_root, "the machine holds the full metadata");
    assert_eq!(coordinator.registry().len(), 1);
}

// ---------------------------------------------------------------------------
// Test 2: Filling a segment → seal → machine entry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sealed_segment_produces_a_machine_entry() {
    let dir = tempfile::tempdir().unwrap();
    let _store = make_store(&dir);

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
    let (_registry, lifecycle) = make_machine(&dir).await;
    let sealer = SegmentSealer::new(seal_config, wal, lifecycle.clone());

    // Create an active segment and fill it.
    let mut active =
        ActiveSegment::new(SizeTier::Standard, &size_config, &pool).expect("create active segment");
    assert!(!active.is_full(), "segment should not be full initially");

    // Append data larger than target (100 bytes).
    active.append(&[0xAAu8; 120]).expect("append should succeed");
    assert!(active.is_full(), "segment should be full after 120 byte append");

    // Seal it — this should create the machine's Sealed entry.
    let segment_id = active.id();
    let entries =
        vec![oceanfs_core::SegmentIndexEntry { offset: 0, length: 120, blob_key_hash: [0xBB; 32] }];
    // The flush path seals Reserved-only — reserve before sealing.
    lifecycle
        .request_reserve(segment_id, SizeTier::Standard, 0, 0)
        .await
        .expect("reserve should succeed");
    let handle = sealer
        .try_seal(&mut active, 0, &entries, Some(oceanfs_core::HashOutput::from_bytes([0xAB; 32])))
        .await
        .expect("seal should succeed");
    assert!(handle.is_some(), "seal should return a handle for full segment");

    // The machine holds the sealed entry (the event log is the only
    // durable segment-state store — RocksDB has no segments CF).
    let entry =
        lifecycle.registry().get(segment_id).expect("machine entry should exist after seal");
    assert_eq!(entry.state, SegmentState::Sealed);
    assert_eq!(entry.metadata.size_tier, SizeTier::Standard);
    assert!(entry.metadata.is_sealed(), "segment should be marked as sealed");

    // The admin endpoint's source: registry enumeration reports it.
    let mut total: u64 = 0;
    let mut sealed: u64 = 0;
    lifecycle.registry().for_each(|_id, entry| {
        total += 1;
        if entry.state == SegmentState::Sealed {
            sealed += 1;
        }
    });
    assert!(total > 0, "D1: total segment count should be > 0 after seal");
    assert!(sealed > 0, "D1: sealed segment count should be > 0 after seal");
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[test]
fn registry_returns_empty_for_new_store() {
    let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
    assert!(registry.is_empty(), "new registry should have no segments");
    assert_eq!(registry.len(), 0);
}

#[test]
fn multiple_segments_all_enumerated() {
    let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
    let mut ids: Vec<SegmentId> = Vec::new();
    for _ in 0..5 {
        let segment_id = SegmentId::new();
        let meta = SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id,
            ec_k: 0,
            ec_m: 0,
            size_tier: SizeTier::Small,
            merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0xAB; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1),
        };
        registry.reserve(segment_id, meta).unwrap();
        ids.push(segment_id);
    }

    let mut enumerated: Vec<SegmentId> = Vec::new();
    registry.for_each(|id, _entry| enumerated.push(id));
    assert_eq!(enumerated.len(), 5, "should enumerate all 5 segments");

    let mut unique = enumerated.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 5, "all segment IDs should be unique");
}
