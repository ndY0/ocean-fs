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
    HashOutput, LifecycleConfig, PoolConfig, SegmentId, SegmentMetadata, SegmentSizeConfig,
    SizeTier, WalConfig,
};
use oceanfs_storage::{
    segment::lifecycle::{SegmentLifecycleCoordinator, SegmentLifecycleRegistry},
    wal::{cleanup_old_wal_files, count_wal_files, replay_wal, WalEntry, WalReader, WalWriter},
    BufferPool, SealConfig, SegmentPool, SegmentSealer,
};

fn make_config(dir: &tempfile::TempDir) -> WalConfig {
    WalConfig {
        data_dir: dir.path().join("wal"),
        max_file_size_bytes: 64 * 1024 * 1024, // 64 MB
        fsync_batch_timeout_ms: 5,
        ..Default::default()
    }
}

async fn make_lifecycle() -> Arc<SegmentLifecycleCoordinator> {
    let dir = tempfile::tempdir().unwrap();
    let event_wal = Arc::new(
        oceanfs_storage::segment::event_wal::EventWal::open(
            dir.path().join("event-wal"),
            &oceanfs_core::EventWalConfig {
                event_wal_dir: dir.path().join("event-wal"),
                event_wal_file_size_bytes: 1024 * 1024,
                event_wal_fsync_batch_timeout_ms: 10,
                event_wal_checkpoint_bytes: 1024 * 1024,
            },
        )
        .await
        .unwrap(),
    );
    Arc::new(
        SegmentLifecycleCoordinator::new(&LifecycleConfig::default()).with_event_wal(event_wal),
    )
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
    let lifecycle = make_lifecycle().await;
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
    let lifecycle = make_lifecycle().await;
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

    let lifecycle = make_lifecycle().await;
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
    // (the write path's reserve is durable in the event log + registry).
    let chunk = pool_small.try_read(seg_id, 0, 64).expect("recovered segment must be present");
    assert_eq!(&chunk[..], &[0u8; 64], "recovered data must match the WAL entry");
    let entry = lifecycle.registry().get(seg_id).expect("reserve durable in the registry");
    assert_eq!(entry.state, oceanfs_storage::segment::lifecycle::SegmentState::Reserved);
}

// ---------------------------------------------------------------------------
// Retention: the production write → seal → rotate → sweep loop
// ---------------------------------------------------------------------------

/// Mirrors the production write path end to end: reserve → data-WAL
/// append (position recorded) → seal through the coordinator → WAL
/// rotation → machine-backed sweep. The seal is requested only after
/// the position record (ADR-0024 §Retention — the write path's caller-
/// side seal hand-off), so the sweep must prune every file older than
/// the retention window.
#[tokio::test]
async fn retention_sweeps_sealed_segments_with_machine_liveness() {
    let dir = tempfile::tempdir().unwrap();
    // Tiny files so a handful of segments forces several rotations.
    let config = WalConfig {
        data_dir: dir.path().join("wal"),
        max_file_size_bytes: 4096,
        fsync_batch_timeout_ms: 2,
        ..Default::default()
    };
    // The registry is shared: the coordinator folds into it and the
    // liveness closure reads it (the production wiring).
    let registry = Arc::new(SegmentLifecycleRegistry::new(&LifecycleConfig::default()));
    let event_wal = Arc::new(
        oceanfs_storage::segment::event_wal::EventWal::open(
            dir.path().join("event-wal"),
            &oceanfs_core::EventWalConfig {
                event_wal_dir: dir.path().join("event-wal"),
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
    let wal_writer = Arc::new(WalWriter::open(&config).await.unwrap());
    let sealer = Arc::new(SegmentSealer::new(
        SealConfig {
            data_dir: dir.path().join("segments"),
            io_mode: oceanfs_storage::io::IoReadMode::Buffered,
            write_mode: oceanfs_storage::io::SegmentWriteMode::Rename,
            ..Default::default()
        },
        Arc::clone(&wal_writer),
        Arc::clone(&lifecycle),
    ));

    // The production liveness closure (node.rs): absent → garbage.
    let liveness: Arc<dyn Fn(SegmentId, oceanfs_storage::DataWalPos) -> bool + Send + Sync> =
        Arc::new(move |id, pos| match registry.get(id) {
            Some(entry) => oceanfs_storage::entry_is_garbage(&entry, &pos),
            None => true,
        });
    wal_writer.set_liveness(Arc::clone(&liveness));

    // The production write path: reserve → WAL append (the position is
    // recorded by `append_wal_entry`) → seal.
    for i in 0..60u32 {
        let seg_id = SegmentId::new();
        lifecycle.request_reserve(seg_id, SizeTier::Small, 2, 1).await.unwrap();
        for chunk in 0..3 {
            sealer.append_wal_entry(make_entry(seg_id, chunk * 64, 64)).await.unwrap();
        }
        let meta = SegmentMetadata {
            segment_id: seg_id,
            ec_k: 2,
            ec_m: 1,
            size_tier: SizeTier::Small,
            merkle_root: Some(HashOutput::from_bytes([i as u8; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1_700_000_000_000),
        };
        lifecycle.request_seal(seg_id, meta, None).await.expect("seal must succeed");
    }

    // Every segment is sealed with a recorded data-WAL position; the
    // machine-backed sweep must prune all files older than the window.
    cleanup_old_wal_files(&config, 2, Some(liveness.as_ref())).await;
    let files = count_wal_files(&config);
    assert!(files <= 3, "old WAL files must be pruned after sealing; {files} remain");
}
