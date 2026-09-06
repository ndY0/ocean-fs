//! Integration test: Orphaned Segment Reaper (fully-dead accounting,
//! ADR-0034 D4/f2).
#![allow(clippy::unwrap_used)]
//!
//! Tests orphan detection from byte accounting: a segment is an orphan iff
//! its aged dead-chunk captures reached its seal-time total AND it is past
//! the TTL grace. Shard deletion, metadata removal, and the bounded
//! snapshot double-check are exercised with the accounting fixtures.

use std::sync::Arc;

use oceanfs_core::{ChunkRef, Hlc, MetadataConfig, ObjectKey, SizeTier, Tombstone};
use oceanfs_durability::{GcConfig, InMemoryShardStore, OrphanReaper};
use oceanfs_storage::{
    metadata::RocksDbMetadataStore,
    segment::lifecycle::{SegmentLifecycleCoordinator, SegmentLifecycleRegistry},
};
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

/// Constructs a reaper over the machine: the coordinator owns the
/// registry (ADR-0025 Decision 3 — the `segments` CF is removed), and
/// the event WAL is its only durable writer.
async fn make_reaper(
    metadata: Arc<RocksDbMetadataStore>,
    store: Arc<dyn SegmentDataStore>,
    config: GcConfig,
    registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
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

/// Seeds a sealed segment through the machine with a known logical total.
fn seed_sealed(registry: &SegmentLifecycleRegistry, id: oceanfs_core::SegmentId, total_bytes: u64) {
    let meta = oceanfs_core::SegmentMetadata {
        pool_id: 0,
        total_bytes,
        segment_id: id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1_000_000_000_000), // ancient — the seal grace has elapsed
    };
    registry.reserve(id, meta.clone()).unwrap();
    registry.seal(id, meta).unwrap();
}

/// Plants an AGED chunk-carrying tombstone — the shape `delete_object`
/// leaves (the row removed + the deleted chunks captured), with a
/// deterministic ancient `deletion_time`.
fn plant_aged_delete(metadata: &RocksDbMetadataStore, segment_id: oceanfs_core::SegmentId) {
    let mut chunks = smallvec::SmallVec::new();
    chunks.push(ChunkRef {
        segment_id,
        offset: 0,
        length: 1000,
        compressed: false,
        logical_length: 1000,
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
async fn segment_with_live_object_not_reclaimed() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    seed_sealed(&registry, seg_id, 1000);

    // A live object references this segment → its bytes are never
    // captured → dead (0) < total (1000).
    let mut chunks = smallvec::SmallVec::new();
    chunks.push(ChunkRef {
        segment_id: seg_id,
        offset: 0,
        length: 500,
        compressed: false,
        logical_length: 500,
    });
    let obj = oceanfs_core::ObjectMetadata {
        object_key: ObjectKey::new("alive.txt"),
        size: 500,
        blake3_hash: None,
        chunks,
        inline_data: None,
        created_at: 0,
        hlc: Hlc::zero(),
    };
    metadata.put_object(obj).unwrap();

    let store = Arc::new(InMemoryShardStore::new(4194304));
    let reaper = make_reaper(metadata, store, GcConfig::default(), registry).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.segments_scanned, 1);
    assert_eq!(stats.orphans_found, 0);
    assert_eq!(stats.orphans_deleted, 0);
}

#[tokio::test]
async fn fully_dead_segment_is_reclaimed_from_metadata_and_disk() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    seed_sealed(&registry, seg_id, 1000);
    plant_aged_delete(&metadata, seg_id);

    let store = Arc::new(InMemoryShardStore::new(4194304));
    let reaper =
        make_reaper(metadata, store.clone(), GcConfig::default(), Arc::clone(&registry)).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.segments_scanned, 1);
    assert_eq!(stats.orphans_found, 1);
    assert_eq!(stats.orphans_deleted, 1);
    assert_eq!(stats.bytes_reclaimed, 4194304);
    assert!(registry.get(seg_id).is_none(), "orphan metadata deleted through the machine");
    assert!(store.is_deleted(seg_id), "orphan shards unlinked after the durable delete");
}

#[tokio::test]
async fn partially_dead_segment_not_reclaimed() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    seed_sealed(&registry, seg_id, 2000);
    plant_aged_delete(&metadata, seg_id); // only 1000 of 2000 bytes dead

    let store = Arc::new(InMemoryShardStore::new(4194304));
    let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.orphans_found, 0, "dead (1000) < total (2000) is never an orphan");
    assert!(registry.get(seg_id).is_some());
}

#[tokio::test]
async fn recently_sealed_not_reclaimed() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    let now_ms =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
            as i64;
    let meta = oceanfs_core::SegmentMetadata {
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
    plant_aged_delete(&metadata, seg_id);

    let store = Arc::new(InMemoryShardStore::new(4194304));
    let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.orphans_found, 0, "a too-young segment keeps the TTL grace");
}

#[tokio::test]
async fn empty_store_no_orphans() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());
    let store = Arc::new(InMemoryShardStore::new(4194304));
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    let reaper = make_reaper(metadata, store, GcConfig::default(), registry).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.segments_scanned, 0);
    assert_eq!(stats.orphans_found, 0);
}

#[tokio::test]
async fn unknown_total_segment_not_reclaimed() {
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

    let seg_id = oceanfs_core::SegmentId::new();
    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    seed_sealed(&registry, seg_id, 0); // unknown total (row-3 adopt shape)
    plant_aged_delete(&metadata, seg_id);

    let store = Arc::new(InMemoryShardStore::new(4194304));
    let reaper = make_reaper(metadata, store, GcConfig::default(), Arc::clone(&registry)).await;
    let stats = reaper.run_cycle().await.unwrap();
    assert_eq!(stats.segments_scanned, 1);
    assert_eq!(stats.orphans_found, 0, "an unknown-total segment is never classified fully dead");
    assert!(registry.get(seg_id).is_some());
}

#[tokio::test]
async fn snapshot_double_check_reclaims_multiple_orphans() {
    // The bounded snapshot double-check (no store rescan) reclaims every
    // fully-dead candidate in one cycle.
    let metadata = Arc::new(RocksDbMetadataStore::open(&test_config()).unwrap());

    let registry =
        Arc::new(SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default()));
    let mut ids = Vec::new();
    for i in 0..3u64 {
        let seg_id = oceanfs_core::SegmentId::new();
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
    for id in ids {
        assert!(registry.get(id).is_none());
    }
}
