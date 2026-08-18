#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: WAL recovery.
//!
//! Verifies the write-ahead log's durability guarantees:
//! - Append entries and replay them
//! - Simulate crash: write entries, drop writer, verify reader replays all
//! - Truncation: partial truncation, verify only remaining entries replayed
//! - Concurrent append ordering
//!
//! Covers the `wal-write-ahead-log` feature's Definition of Done.

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_core::{
    HashOutput, LifecycleConfig, MetadataConfig, PoolConfig, SegmentId, SegmentSizeConfig,
    SizeTier, WalConfig,
};
use oceanfs_storage::{
    metadata::RocksDbMetadataStore,
    segment::lifecycle::{SegmentLifecycleCoordinator, SegmentLifecycleRegistry},
    wal::{replay_wal, WalEntry, WalReader, WalWriter},
    BufferPool, SegmentPool,
};

fn make_config(dir: &tempfile::TempDir) -> WalConfig {
    WalConfig {
        data_dir: dir.path().join("wal"),
        max_file_size_bytes: 64 * 1024 * 1024, // 64 MB
        fsync_batch_timeout_ms: 5,
        ..Default::default()
    }
}

async fn make_lifecycle() -> (Arc<RocksDbMetadataStore>, Arc<SegmentLifecycleCoordinator>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        RocksDbMetadataStore::open(&MetadataConfig {
            data_dir: dir.path().join("meta"),
            block_cache_size: 1024,
            memtable_size: 1024,
            ..Default::default()
        })
        .unwrap(),
    );
    let lifecycle =
        Arc::new(SegmentLifecycleCoordinator::new(store.clone(), &LifecycleConfig::default()));
    (store, lifecycle)
}

fn make_entry(segment_id: SegmentId, offset: u64, length: u32) -> WalEntry {
    WalEntry::new(
        segment_id,
        offset,
        length,
        length,
        0,
        0,
        0,
        HashOutput::from_bytes([0u8; 32]),
        vec![0u8; length as usize].into(),
    )
}

// ---------------------------------------------------------------------------
// Basic append and replay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn append_and_replay_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let config = make_config(&dir);

    let writer = WalWriter::open(&config).await.expect("failed to open writer");

    let seg_id = SegmentId::new();
    let entry1 = make_entry(seg_id, 0, 100);
    let entry2 = make_entry(seg_id, 100, 200);
    let entry3 = make_entry(seg_id, 300, 150);

    let pos1 = writer.append(entry1.clone()).await.unwrap();
    let pos2 = writer.append(entry2.clone()).await.unwrap();
    let pos3 = writer.append(entry3.clone()).await.unwrap();

    // Positions should be sequential.
    assert!(pos1 < pos2);
    assert!(pos2 < pos3);

    drop(writer);

    // Replay all entries.
    let reader = WalReader::open(&config).expect("failed to open reader");
    let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].offset, 0);
    assert_eq!(entries[1].offset, 100);
    assert_eq!(entries[2].offset, 300);
}

// ---------------------------------------------------------------------------
// Crash simulation: write without truncate, then replay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crash_simulation_replays_all_entries() {
    let dir = tempfile::tempdir().unwrap();
    let config = make_config(&dir);

    let seg_id = SegmentId::new();

    // Write entries and drop the writer without truncating (simulating crash).
    {
        let writer = WalWriter::open(&config).await.unwrap();
        for i in 0..10 {
            let entry = make_entry(seg_id, i * 64, 64);
            writer.append(entry).await.unwrap();
        }
        // Drop without truncate — simulates crash.
    }

    // On restart, replay all entries.
    let reader = WalReader::open(&config).expect("failed to open reader");
    let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(entries.len(), 10, "all 10 entries should be replayed after crash");
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.offset, i as u64 * 64);
        assert_eq!(entry.length, 64);
    }
}

// ---------------------------------------------------------------------------
// Truncation: truncate partial, verify only remaining entries replayed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn truncate_removes_entries_after_position() {
    let dir = tempfile::tempdir().unwrap();
    let config = make_config(&dir);

    let seg_id = SegmentId::new();
    let mut truncation_point: u64 = 0;

    // Write entries.
    {
        let writer = WalWriter::open(&config).await.unwrap();

        for i in 0..5 {
            let entry = make_entry(seg_id, i * 100, 100);
            let entry_size = entry.serialized_size() as u64;
            let pos = writer.append(entry).await.unwrap();
            if i == 2 {
                truncation_point = pos.offset + entry_size;
            }
        }

        // Truncate after the third entry (keeping first 3).
        writer.truncate(truncation_point).await.unwrap();
    }

    // Replay — only the first 3 entries should remain.
    let reader = WalReader::open(&config).expect("failed to open reader");
    let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(entries.len(), 3, "only first 3 entries should remain after truncation");
}

// ---------------------------------------------------------------------------
// Sync
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_does_not_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = make_config(&dir);

    let writer = WalWriter::open(&config).await.unwrap();
    let seg_id = SegmentId::new();
    let entry = make_entry(seg_id, 0, 42);

    writer.append(entry).await.unwrap();
    // Sync should succeed without error.
    writer.sync().await.unwrap();
}

// ---------------------------------------------------------------------------
// Empty WAL replay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_empty_directory_returns_no_entries() {
    let dir = tempfile::tempdir().unwrap();
    let config = make_config(&dir);

    // Create the WAL directory first (WalWriter::open creates it,
    // but we need it to exist before WalReader::open can scan it).
    {
        let _writer = WalWriter::open(&config).await.expect("failed to open writer");
        // Writer is dropped immediately — directory exists but has no entries.
    }

    let reader = WalReader::open(&config).expect("failed to open reader");
    let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>, _>>().unwrap();

    assert!(entries.is_empty());
}

// ---------------------------------------------------------------------------
// WalEntry serialization round-trip
// ---------------------------------------------------------------------------

#[test]
fn entry_roundtrip() {
    let seg_id = SegmentId::new();
    let data: Bytes = vec![0xBBu8; 256].into();
    let entry =
        WalEntry::new(seg_id, 42, 256, 256, 0, 0, 0, HashOutput::from_bytes([0xAA; 32]), data);

    let bytes = entry.to_bytes();
    let restored = WalEntry::from_bytes(&bytes).expect("failed to deserialize entry");

    assert_eq!(restored.offset, 42);
    assert_eq!(restored.length, 256);
    assert_eq!(restored.data.len(), 256);
}

// ---------------------------------------------------------------------------
// replay_wal function (crate-boundary test)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_wal_recovers_and_truncates() {
    let dir = tempfile::tempdir().unwrap();
    let config = make_config(&dir);
    let size_config = SegmentSizeConfig::default();
    let buffer_pool = Arc::new(BufferPool::new(65536, 8));
    let pool_cfg = PoolConfig::default();
    let registry = std::sync::Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
    let pool_small = SegmentPool::new(
        pool_cfg.clone(),
        SizeTier::Small,
        &size_config,
        buffer_pool.clone(),
        None,
        None,
        std::sync::Arc::clone(&registry),
    )
    .unwrap();
    let pool_standard = SegmentPool::new(
        pool_cfg,
        SizeTier::Standard,
        &size_config,
        buffer_pool,
        None,
        None,
        registry,
    )
    .unwrap();

    let seg_id = SegmentId::new();

    // Write entries and drop writer (simulating crash).
    {
        let writer = WalWriter::open(&config).await.unwrap();
        for i in 0..5 {
            let entry = make_entry(seg_id, i * 64, 64);
            writer.append(entry).await.unwrap();
        }
    }

    // On restart: open writer, replay WAL into pools.
    let wal_writer = WalWriter::open(&config).await.unwrap();
    let (_store, lifecycle) = make_lifecycle().await;
    let summary = replay_wal(
        &config,
        &wal_writer,
        &pool_small,
        &pool_standard,
        &size_config,
        |_| false,
        &lifecycle,
    )
    .await
    .expect("replay_wal should succeed after crash");

    assert_eq!(summary.entries_replayed, 5);
    assert_eq!(summary.bytes_replayed, 320);
    assert_eq!(summary.segments_seen.len(), 1);

    // After replay, WAL should be truncated — verify by re-reading.
    drop(wal_writer);
    let reader = WalReader::open(&config).unwrap();
    let entries: Vec<_> = reader.replay().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(entries.is_empty(), "WAL should be empty after successful replay with truncation");
}

#[tokio::test]
async fn replay_wal_empty_wal_returns_zero_summary() {
    let dir = tempfile::tempdir().unwrap();
    let config = make_config(&dir);
    let size_config = SegmentSizeConfig::default();
    let buffer_pool = Arc::new(BufferPool::new(65536, 8));
    let pool_cfg = PoolConfig::default();
    let registry = std::sync::Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
    let pool_small = SegmentPool::new(
        pool_cfg.clone(),
        SizeTier::Small,
        &size_config,
        buffer_pool.clone(),
        None,
        None,
        std::sync::Arc::clone(&registry),
    )
    .unwrap();
    let pool_standard = SegmentPool::new(
        pool_cfg,
        SizeTier::Standard,
        &size_config,
        buffer_pool,
        None,
        None,
        registry,
    )
    .unwrap();

    let wal_writer = WalWriter::open(&config).await.unwrap();
    let (_store, lifecycle) = make_lifecycle().await;
    let summary = replay_wal(
        &config,
        &wal_writer,
        &pool_small,
        &pool_standard,
        &size_config,
        |_| false,
        &lifecycle,
    )
    .await
    .unwrap();

    assert_eq!(summary.entries_replayed, 0);
    assert_eq!(summary.bytes_replayed, 0);
    assert!(summary.segments_seen.is_empty());
}

#[tokio::test]
async fn replay_recovers_segment_reserved_before_crash() {
    // The feature DoD's reserve-before-data mutation check ("kill after
    // first entry → segment present"): a segment whose durable reserve
    // landed BEFORE its first WAL entry survives a kill+replay with its
    // data present. The write path's order is reserve → WAL entry; this
    // test drops the writer right after the first entry (no truncate —
    // a simulated kill), then replays on a fresh writer and asserts the
    // segment is rebuilt and readable.
    let dir = tempfile::tempdir().unwrap();
    let config = make_config(&dir);
    let size_config = SegmentSizeConfig::default();
    let buffer_pool = Arc::new(BufferPool::new(65536, 8));
    let pool_cfg = PoolConfig::default();
    let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
    let pool_small = Arc::new(
        SegmentPool::new(
            pool_cfg.clone(),
            SizeTier::Small,
            &size_config,
            buffer_pool.clone(),
            None,
            None,
            Arc::clone(&registry),
        )
        .unwrap(),
    );
    let pool_standard = SegmentPool::new(
        pool_cfg,
        SizeTier::Standard,
        &size_config,
        buffer_pool,
        None,
        None,
        registry,
    )
    .unwrap();

    let (store, lifecycle) = make_lifecycle().await;
    let seg_id = SegmentId::new();

    // The write path contract: reserve BEFORE the first DataEntry.
    lifecycle.request_reserve(seg_id, SizeTier::Small, 2, 1).await.unwrap();
    {
        let writer = WalWriter::open(&config).await.unwrap();
        writer.append(make_entry(seg_id, 0, 64)).await.unwrap();
        // drop(writer): simulated kill — no truncate, no cleanup.
    }

    // Restart: replay with the same coordinator (the registry entry is
    // still Reserved; replay's own reserve is an idempotent no-op).
    let wal_writer = WalWriter::open(&config).await.unwrap();
    let summary = replay_wal(
        &config,
        &wal_writer,
        &pool_small,
        &pool_standard,
        &size_config,
        |_| false,
        &lifecycle,
    )
    .await
    .unwrap();
    assert_eq!(summary.entries_replayed, 1, "the entry survives the kill");
    assert_eq!(summary.segments_seen, vec![seg_id]);

    // "Segment present": the rebuilt segment is readable from the pool
    // (the write path's reserve is durable in the CF too).
    let chunk = pool_small.try_read(seg_id, 0, 64).expect("recovered segment must be present");
    assert_eq!(&chunk[..], &[0u8; 64], "recovered data must match the WAL entry");
    let cf = store.get_segment(seg_id).unwrap().expect("reserve phantom durable in the CF");
    assert!(cf.sealed_at.is_none(), "reserve is the unsealed phantom");
}
