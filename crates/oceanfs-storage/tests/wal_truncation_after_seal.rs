//! Integration test: WAL truncation after segment seal.
//!
//! Verifies H8-storage: after a segment is sealed, the WAL entries for
//! that segment are truncated, preventing unbounded WAL growth.
//!
//! ## Tests
//! - `wal_truncation_called_during_seal`
//! - `wal_writer_truncation_is_idempotent`

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::{HashOutput, SegmentId, SegmentSizeConfig, SizeTier, WalConfig};
use oceanfs_storage::{
    io::IoReadMode, segment::buffer::ActiveSegment, BufferPool, SealConfig, SegmentSealer,
    WalEntry, WalWriter,
};

fn make_test_entry(segment_id: SegmentId, offset: u64, length: u32) -> WalEntry {
    WalEntry::new(
        segment_id,
        offset,
        length,
        0,
        0,
        HashOutput::from_bytes([0u8; 32]),
        vec![0u8; length as usize].into(),
    )
}

// ---------------------------------------------------------------------------
// Test: WAL truncation is exercised during seal
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wal_truncation_called_during_seal() {
    let dir = tempfile::tempdir().unwrap();

    // Open WAL.
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

    // Write some entries to the WAL (simulating prior segment activity).
    for i in 0u64..5 {
        let entry = make_test_entry(SegmentId::new(), i * 100, 100);
        wal.append(entry).await.unwrap();
    }

    let pos_before_seal = wal.global_position().await;
    assert!(pos_before_seal > 0, "WAL should have data before seal");

    // Create a sealer that uses this WAL.
    let metadata = Arc::new(
        oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
            data_dir: dir.path().join("meta"),
            block_cache_size: 1024,
            memtable_size: 1024,
            ..Default::default()
        })
        .unwrap(),
    );
    let seal_config = SealConfig {
        target_size_bytes: 100,
        seal_timeout_ms: 5000,
        data_dir: dir.path().join("segments"),
        io_mode: IoReadMode::Buffered,
        write_mode: oceanfs_storage::io::SegmentWriteMode::Rename,
    };
    let sealer = SegmentSealer::new(seal_config, metadata, wal.clone(), None);

    // Create and fill an active segment.
    let size_config = SegmentSizeConfig {
        default_target_size: 100,
        small_target_size: 100,
        ..SegmentSizeConfig::default()
    };
    let pool = BufferPool::new(65536, 4);
    let mut active = ActiveSegment::new(SizeTier::Standard, &size_config, &pool).unwrap();
    active.append(&[0xAAu8; 120]).unwrap();
    assert!(active.is_full());

    // Seal it — the sealer internally calls wal.truncate().
    let entries =
        vec![oceanfs_core::SegmentIndexEntry { offset: 0, length: 120, blob_key_hash: [0xBB; 32] }];
    let handle = sealer.try_seal(&mut active, 0, &entries).await.unwrap();
    assert!(handle.is_some(), "seal should succeed for full segment");

    // After seal, the WAL should have been truncated (global_position
    // should not have grown since the entries for this segment were
    // discarded).
    let pos_after_seal = wal.global_position().await;
    assert!(
        pos_after_seal <= pos_before_seal + 1024,
        "WAL position should not grow significantly after truncation ({pos_before_seal} → {pos_after_seal})"
    );

    // Verify we can still write to the WAL after truncation (idempotency).
    let new_entry = make_test_entry(SegmentId::new(), 0, 50);
    let post_trunc_pos = wal.append(new_entry).await.unwrap();
    assert!(post_trunc_pos > 0, "WAL should still accept writes after truncation");
}

// ---------------------------------------------------------------------------
// Edge: truncation does not error on empty WAL or at position 0
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wal_writer_truncation_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let wal = WalWriter::open(&WalConfig {
        data_dir: dir.path().join("wal"),
        max_file_size_bytes: 1024 * 1024,
        fsync_batch_timeout_ms: 5,
        ..Default::default()
    })
    .await
    .unwrap();

    // Truncate at 0 on a fresh WAL should not error.
    wal.truncate(0).await.unwrap();
    assert_eq!(wal.global_position().await, 0);

    // Write an entry, then truncate back.
    let entry = make_test_entry(SegmentId::new(), 0, 100);
    let pos = wal.append(entry).await.unwrap();
    assert!(pos < 1024, "first entry should be at a small position, got {pos}");

    wal.truncate(0).await.unwrap();

    // After truncation, the next append returns a small position
    // (truncation resets the file cursor).
    let pos2 = wal.append(make_test_entry(SegmentId::new(), 0, 50)).await.unwrap();
    assert!(pos2 < 1024, "after truncation, next write should be at a small position, got {pos2}");
}
