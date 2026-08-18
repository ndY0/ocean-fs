//! Integration tests for the disk-backed segment reader.
//!
//! Exercises the full read path: write segment to disk → read via
//! DiskSegmentReader → verify data and source metadata.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_core::SegmentId;
use oceanfs_storage::{
    io::{
        DiskIo, DiskSegmentReader, InMemorySegmentReader, IoReadMode, SegmentFileCache,
        SegmentReadSource, SegmentReader,
    },
    segment::header::SEGMENT_HEADER_SIZE_V1 as V1_HEADER_SIZE,
};

/// Writes a segment file with a 76-byte zeroed header followed by `data`.
/// Writes a valid v1 segment file (76-byte header with a real checksum,
/// version 1, no parity) followed by `data`.
fn write_segment_file(path: &std::path::Path, data: &[u8]) {
    let mut file_data = vec![0u8; V1_HEADER_SIZE];
    file_data[0..4].copy_from_slice(b"OFSG");
    file_data[4..6].copy_from_slice(&1u16.to_le_bytes());
    file_data[22..30].copy_from_slice(&(data.len() as u64).to_le_bytes());
    file_data[34..42].copy_from_slice(&((V1_HEADER_SIZE + data.len()) as u64).to_le_bytes());
    let checksum = *blake3::hash(data).as_bytes();
    file_data[42..74].copy_from_slice(&checksum);
    file_data.extend_from_slice(data);
    std::fs::write(path, &file_data).unwrap();
}

#[tokio::test]
async fn disk_reader_buffered_read_write_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let segment_id = SegmentId::new();
    let path = dir.path().join(format!("{segment_id}.dat"));

    // Write a segment file to disk.
    let test_data: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
    write_segment_file(&path, &test_data);

    // Create a disk reader in buffered mode.
    let reader = DiskSegmentReader::new(
        IoReadMode::Buffered,
        Arc::new(DiskIo::TokioFs),
        None,
        dir.path().to_path_buf(),
        None,
        None,
    );

    // Read a chunk and verify data matches.
    let chunk = reader.read_chunk(&segment_id, 1024, 4096).await.unwrap();
    assert_eq!(chunk.len(), 4096);
    assert_eq!(&chunk[..], &test_data[1024..5120]);

    // Verify source metadata.
    let source = reader.last_read_source(&segment_id);
    assert!(
        matches!(source, SegmentReadSource::DirectIo { .. }),
        "expected DirectIo source, got {source:?}"
    );
}

#[tokio::test]
async fn disk_reader_mmap_read_write_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let segment_id = SegmentId::new();
    let path = dir.path().join(format!("{segment_id}.dat"));

    let test_data = vec![0x5Au8; 16384];
    write_segment_file(&path, &test_data);

    let cache = Arc::new(SegmentFileCache::new(8));
    let reader = DiskSegmentReader::new(
        IoReadMode::Mmap,
        Arc::new(DiskIo::TokioFs),
        Some(cache.clone()),
        dir.path().to_path_buf(),
        None,
        None,
    );

    // First read: cache miss, maps the file.
    let chunk = reader.read_chunk(&segment_id, 0, 4096).await.unwrap();
    assert_eq!(chunk.len(), 4096);
    assert!(chunk.iter().all(|&b| b == 0x5A));
    assert_eq!(cache.len(), 1, "mmap cache should have one entry after miss");

    // Source should be mmap-backed.
    let source = reader.last_read_source(&segment_id);
    assert!(
        matches!(source, SegmentReadSource::MmapBacked { .. }),
        "expected MmapBacked source, got {source:?}"
    );

    // Second read: cache hit.
    let chunk2 = reader.read_chunk(&segment_id, 8192, 4096).await.unwrap();
    assert_eq!(chunk2.len(), 4096);
    assert_eq!(cache.len(), 1, "cache should still have one entry (hit)");
}

#[tokio::test]
async fn disk_reader_multiple_segments() {
    let dir = tempfile::tempdir().unwrap();

    let cache = Arc::new(SegmentFileCache::new(4));
    let reader = DiskSegmentReader::new(
        IoReadMode::Mmap,
        Arc::new(DiskIo::TokioFs),
        Some(cache),
        dir.path().to_path_buf(),
        None,
        None,
    );

    // Write and read 3 different segments.
    for i in 0..3 {
        let id = SegmentId::new();
        let path = dir.path().join(format!("{id}.dat"));
        let data = vec![i as u8; 1024];
        write_segment_file(&path, &data);

        let chunk = reader.read_chunk(&id, 0, 512).await.unwrap();
        assert_eq!(chunk.len(), 512);
        assert!(chunk.iter().all(|&b| b == i as u8));
    }
}

#[tokio::test]
async fn in_memory_reader_put_and_read_roundtrip() {
    let reader = InMemorySegmentReader::new();
    let id = SegmentId::new();

    // Put data into the in-memory store.
    reader.put(id, Bytes::from_static(&[10, 20, 30, 40, 50, 60]));

    // Read back a chunk.
    let chunk = reader.read_chunk(&id, 1, 3).await.unwrap();
    assert_eq!(&chunk[..], &[20, 30, 40]);

    // In-memory reader always returns Memory source.
    let source = reader.last_read_source(&id);
    assert_eq!(source, SegmentReadSource::Memory);
}

#[tokio::test]
async fn disk_reader_error_on_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let reader = DiskSegmentReader::new(
        IoReadMode::Buffered,
        Arc::new(DiskIo::TokioFs),
        None,
        dir.path().to_path_buf(),
        None,
        None,
    );

    let result = reader.read_chunk(&SegmentId::new(), 0, 100).await;
    assert!(result.is_err(), "missing file should return error");
    assert!(
        result.unwrap_err().contains("integrity check failed"),
        "error should mention file open failure"
    );
}

#[tokio::test]
async fn disk_reader_respects_io_mode_buffered() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    let path = dir.path().join(format!("{id}.dat"));
    write_segment_file(&path, b"buffered test data here");

    let reader = DiskSegmentReader::new(
        IoReadMode::Buffered,
        Arc::new(DiskIo::TokioFs),
        None,
        dir.path().to_path_buf(),
        None,
        None,
    );

    let chunk = reader.read_chunk(&id, 9, 4).await.unwrap();
    assert_eq!(&chunk[..], b"test");
    assert!(matches!(reader.last_read_source(&id), SegmentReadSource::DirectIo { .. }));
}

#[tokio::test]
async fn segment_cache_eviction_preserves_mmap_data() {
    let dir = tempfile::tempdir().unwrap();

    // Cache with capacity 1 — each new segment evicts the previous.
    let cache = Arc::new(SegmentFileCache::new(1));

    let id1 = SegmentId::new();
    let id2 = SegmentId::new();

    let path1 = dir.path().join(format!("{id1}.dat"));
    let path2 = dir.path().join(format!("{id2}.dat"));
    write_segment_file(&path1, b"first segment data payload here");
    write_segment_file(&path2, b"second segment different payload!");

    let reader = DiskSegmentReader::new(
        IoReadMode::Mmap,
        Arc::new(DiskIo::TokioFs),
        Some(cache.clone()),
        dir.path().to_path_buf(),
        None,
        None,
    );

    // Read id1 → maps and caches it.
    let _chunk1 = reader.read_chunk(&id1, 0, 10).await.unwrap();
    assert_eq!(cache.len(), 1);

    // Read id2 → evicts id1, maps id2.
    let chunk2 = reader.read_chunk(&id2, 0, 7).await.unwrap();
    assert_eq!(&chunk2[..], b"second ");
    assert_eq!(cache.len(), 1);

    // Read id1 again → cache miss, re-maps. Data is still correct because
    // the file on disk is immutable.
    let chunk1_again = reader.read_chunk(&id1, 0, 5).await.unwrap();
    assert_eq!(&chunk1_again[..], b"first");
}

#[tokio::test]
async fn segment_cache_invalidation_allows_re_read() {
    let dir = tempfile::tempdir().unwrap();
    let id = SegmentId::new();
    let path = dir.path().join(format!("{id}.dat"));
    write_segment_file(&path, b"invalidation test data");

    let cache = Arc::new(SegmentFileCache::new(4));
    let reader = DiskSegmentReader::new(
        IoReadMode::Mmap,
        Arc::new(DiskIo::TokioFs),
        Some(cache.clone()),
        dir.path().to_path_buf(),
        None,
        None,
    );

    // Read → cache hit.
    let _chunk1 = reader.read_chunk(&id, 0, 10).await.unwrap();
    assert_eq!(cache.len(), 1);

    // Simulate GC: invalidate the cache entry.
    cache.invalidate(id);
    assert_eq!(cache.len(), 0);

    // Next read → cache miss, re-maps. Data still correct.
    let chunk2 = reader.read_chunk(&id, 0, 10).await.unwrap();
    assert_eq!(&chunk2[..], b"invalidati");
    assert_eq!(cache.len(), 1);
}

// ---------------------------------------------------------------------------
// Write-path round-trip: seal via SegmentSealer → read via DiskSegmentReader
// ---------------------------------------------------------------------------

mod write_read_roundtrip {
    use std::sync::Arc;

    use oceanfs_core::{MetadataConfig, SegmentId, SegmentIndexEntry, SizeTier, WalConfig};
    use oceanfs_storage::{
        io::{DiskIo, DiskSegmentReader, IoReadMode, SegmentReader},
        metadata::RocksDbMetadataStore,
        segment::{
            lifecycle::SegmentLifecycleCoordinator,
            sealer::{SealConfig, SegmentSealer},
        },
        wal::WalWriter,
    };

    async fn setup(
        dir: &tempfile::TempDir,
        io_mode: IoReadMode,
    ) -> (Arc<SegmentSealer>, Arc<SegmentLifecycleCoordinator>, Arc<DiskSegmentReader>) {
        let metadata = Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: dir.path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        );

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

        let segments_dir = dir.path().join("segments");
        let lifecycle =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
                metadata,
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let sealer = Arc::new(SegmentSealer::new(
            SealConfig {
                target_size_bytes: 65536,
                seal_timeout_ms: 5000,
                data_dir: segments_dir.clone(),
                io_mode,
                write_mode: oceanfs_storage::io::SegmentWriteMode::Rename,
                ..Default::default()
            },
            wal,
            lifecycle.clone(),
        ));

        let reader = Arc::new(DiskSegmentReader::new(
            io_mode,
            Arc::new(DiskIo::TokioFs),
            None,
            segments_dir,
            None,
            None,
        ));

        (sealer, lifecycle.clone(), reader)
    }

    #[tokio::test]
    async fn seal_buffered_then_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let (sealer, lifecycle, reader) = setup(&dir, IoReadMode::Buffered).await;

        let segment_id = SegmentId::new();
        let data = b"write path integration test data payload";

        // The flush path seals Reserved-only — reserve first.
        lifecycle.request_reserve(segment_id, SizeTier::Standard, 0, 0).await.unwrap();
        sealer
            .seal_from_data(
                segment_id,
                SizeTier::Standard,
                bytes::Bytes::from_static(data),
                &[SegmentIndexEntry {
                    offset: 0,
                    length: data.len() as u32,
                    blob_key_hash: [0xAA; 32],
                }],
                0,
                0,
                0,
                None,
                None,
            )
            .await
            .unwrap();

        // Read back via DiskSegmentReader.
        let chunk = reader.read_chunk(&segment_id, 0, data.len() as u32).await.unwrap();
        assert_eq!(&chunk[..], data);
    }

    #[tokio::test]
    async fn seal_direct_then_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let (sealer, lifecycle, reader) = setup(&dir, IoReadMode::Direct).await;

        let segment_id = SegmentId::new();
        let data = b"O_DIRECT write then read from disk";

        // The flush path seals Reserved-only — reserve first.
        lifecycle.request_reserve(segment_id, SizeTier::Standard, 0, 0).await.unwrap();
        sealer
            .seal_from_data(
                segment_id,
                SizeTier::Standard,
                bytes::Bytes::from_static(data),
                &[SegmentIndexEntry {
                    offset: 0,
                    length: data.len() as u32,
                    blob_key_hash: [0xBB; 32],
                }],
                0,
                0,
                0,
                None,
                None,
            )
            .await
            .unwrap();

        let chunk = reader.read_chunk(&segment_id, 0, data.len() as u32).await.unwrap();
        assert_eq!(&chunk[..], data);
        assert!(matches!(
            reader.last_read_source(&segment_id),
            oceanfs_storage::io::SegmentReadSource::DirectIo { .. }
        ));
    }
}
