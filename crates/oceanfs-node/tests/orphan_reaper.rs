//! Integration test: Orphan segment reaper (fully-dead accounting,
//! ADR-0034 D4/f2) — node-level fixture wiring.
//!
//! Verifies that the orphan reaper:
//! 1. Identifies fully-dead segments (aged dead-captures >= total) from
//!    byte accounting — no objects-CF reference scan
//! 2. Respects the seal TTL for recently-sealed segments
//! 3. Deletes orphan segment metadata and shard data
//! 4. Reports accurate statistics

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::{
    ChunkRef, Hlc, MetadataConfig, ObjectKey, SegmentId, SegmentMetadata, SizeTier, Tombstone,
};
use oceanfs_durability::{GcConfig, InMemoryShardStore, OrphanReaper};
use oceanfs_storage::{
    metadata::RocksDbMetadataStore,
    segment::lifecycle::{SegmentLifecycleCoordinator, SegmentLifecycleRegistry},
};
use oceanfs_storage_api::SegmentDataStore;

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
    store: Arc<dyn SegmentDataStore>,
    config: GcConfig,
    registry: Arc<SegmentLifecycleRegistry>,
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
    OrphanReaper::new(metadata, lifecycle, store, config)
}

/// Helper: create a test shard store.
fn make_shard_store() -> Arc<InMemoryShardStore> {
    Arc::new(InMemoryShardStore::new(4194304))
}

/// Helper: create a sealed segment with a known logical total and an
/// ancient seal time (the grace has elapsed).
fn seed_sealed(registry: &SegmentLifecycleRegistry, id: SegmentId, total_bytes: u64) {
    let meta = SegmentMetadata {
        pool_id: 0,
        total_bytes,
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1_000_000_000_000),
    };
    registry.reserve(id, meta.clone()).unwrap();
    registry.seal(id, meta).unwrap();
}

/// Plants an AGED chunk-carrying delete capture (the `delete_object`
/// shape) referencing `segment_id` for `length` bytes.
fn plant_aged_delete(metadata: &RocksDbMetadataStore, segment_id: SegmentId, length: u32) {
    let mut chunks = smallvec::SmallVec::new();
    chunks.push(ChunkRef {
        segment_id,
        offset: 0,
        length,
        compressed: false,
        logical_length: length,
    });
    metadata
        .put_tombstone(
            &oceanfs_core::BucketId::new("default"),
            &ObjectKey::new("gone.txt"),
            Tombstone { deletion_time: 1_000_000_000_000, hlc: Hlc::zero(), chunks },
        )
        .unwrap();
}

#[tokio::test]
async fn reaper_empty_store_produces_zero_stats() {
    let metadata = open_temp_metadata();
    let store = make_shard_store();
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    let reaper = make_reaper(metadata, store, GcConfig::default(), registry).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.segments_scanned, 0);
    assert_eq!(stats.orphans_found, 0);
    assert_eq!(stats.orphans_deleted, 0);
}

#[tokio::test]
async fn reaper_referenced_segment_not_orphan() {
    let metadata = open_temp_metadata();
    let seg_id = SegmentId::new();
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    seed_sealed(&registry, seg_id, 1000);

    // A live object row references the segment → no dead capture → kept.
    let mut chunks = smallvec::SmallVec::new();
    chunks.push(ChunkRef {
        segment_id: seg_id,
        offset: 0,
        length: 500,
        compressed: false,
        logical_length: 500,
    });
    metadata
        .put_object(oceanfs_core::ObjectMetadata {
            object_key: ObjectKey::new("alive.txt"),
            size: 500,
            blake3_hash: None,
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        })
        .unwrap();

    let reaper =
        make_reaper(metadata, make_shard_store(), GcConfig::default(), Arc::clone(&registry)).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.segments_scanned, 1);
    assert_eq!(stats.orphans_found, 0);
}

#[tokio::test]
async fn reaper_fully_dead_segment_beyond_ttl_is_orphan() {
    let metadata = open_temp_metadata();
    let seg_id = SegmentId::new();
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    seed_sealed(&registry, seg_id, 1000);
    plant_aged_delete(&metadata, seg_id, 1000);

    let store = make_shard_store();
    let reaper =
        make_reaper(metadata, store.clone(), GcConfig::default(), Arc::clone(&registry)).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.segments_scanned, 1);
    assert_eq!(stats.orphans_found, 1);
    assert_eq!(stats.orphans_deleted, 1);
    assert!(registry.get(seg_id).is_none(), "orphan metadata deleted");
    assert!(store.is_deleted(seg_id), "orphan shard data deleted");
}

#[tokio::test]
async fn reaper_partially_dead_segment_not_orphan() {
    let metadata = open_temp_metadata();
    let seg_id = SegmentId::new();
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    seed_sealed(&registry, seg_id, 2000);
    plant_aged_delete(&metadata, seg_id, 1000);

    let reaper =
        make_reaper(metadata, make_shard_store(), GcConfig::default(), Arc::clone(&registry)).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.orphans_found, 0, "dead < total is never an orphan");
    assert!(registry.get(seg_id).is_some());
}

#[tokio::test]
async fn reaper_unknown_total_segment_not_orphan() {
    let metadata = open_temp_metadata();
    let seg_id = SegmentId::new();
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    seed_sealed(&registry, seg_id, 0); // unknown total (row-3 adopt shape)
    plant_aged_delete(&metadata, seg_id, 1000);

    let reaper =
        make_reaper(metadata, make_shard_store(), GcConfig::default(), Arc::clone(&registry)).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.segments_scanned, 1);
    assert_eq!(stats.orphans_found, 0, "an unknown-total segment is never classified fully dead");
    assert!(registry.get(seg_id).is_some());
}

#[tokio::test]
async fn reaper_recently_sealed_not_orphan() {
    let metadata = open_temp_metadata();
    let seg_id = SegmentId::new();
    let now_ms =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
            as i64;
    let meta = SegmentMetadata {
        pool_id: 0,
        total_bytes: 1000,
        segment_id: seg_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(now_ms),
    };
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    registry.reserve(seg_id, meta.clone()).unwrap();
    registry.seal(seg_id, meta).unwrap();
    plant_aged_delete(&metadata, seg_id, 1000);

    let reaper =
        make_reaper(metadata, make_shard_store(), GcConfig::default(), Arc::clone(&registry)).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.segments_scanned, 1);
    assert_eq!(stats.orphans_found, 0, "a too-young segment keeps the TTL grace");
}

#[tokio::test]
async fn reaper_multiple_orphans_all_reaped() {
    let metadata = open_temp_metadata();
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    let mut ids = Vec::new();
    for i in 0..5u64 {
        let seg_id = SegmentId::new();
        seed_sealed(&registry, seg_id, 1000);
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: 1000,
            compressed: false,
            logical_length: 1000,
        });
        metadata
            .put_tombstone(
                &oceanfs_core::BucketId::new("default"),
                &ObjectKey::new(format!("gone{i}.txt")),
                Tombstone { deletion_time: 1_000_000_000_000, hlc: Hlc::zero(), chunks },
            )
            .unwrap();
        ids.push(seg_id);
    }

    let store = make_shard_store();
    let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.segments_scanned, 5);
    assert_eq!(stats.orphans_found, 5);
    assert_eq!(stats.orphans_deleted, 5);
    assert!(stats.bytes_reclaimed > 0);
    for id in ids {
        assert!(registry.get(id).is_none());
    }
}

#[tokio::test]
async fn reaper_reports_bytes_reclaimed_correctly() {
    let metadata = open_temp_metadata();
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    let mut ids = Vec::new();
    for i in 0..3u64 {
        let seg_id = SegmentId::new();
        seed_sealed(&registry, seg_id, 1000);
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: 1000,
            compressed: false,
            logical_length: 1000,
        });
        metadata
            .put_tombstone(
                &oceanfs_core::BucketId::new("default"),
                &ObjectKey::new(format!("gone{i}.txt")),
                Tombstone { deletion_time: 1_000_000_000_000, hlc: Hlc::zero(), chunks },
            )
            .unwrap();
        ids.push(seg_id);
    }

    let store = Arc::new(InMemoryShardStore::new(4194304));
    let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.orphans_found, 3);
    assert_eq!(stats.orphans_deleted, 3);
    assert_eq!(stats.bytes_reclaimed, 4194304 * 3);
    for id in ids {
        assert!(registry.get(id).is_none());
    }
}
