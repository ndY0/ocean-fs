//! Integration test: seal-time EC parity through the segment pool and sealer.
//!
//! Verifies the single-scheduler parity path end-to-end: a pool configured
//! with EC parameters seals segments whose work items carry (k, m, strip);
//! `seal_from_data` computes the parity on the blocking pool (the parallel
//! encoder — no second scheduler on the write path) and persists a v2
//! parity section whose shard-hash table verifies against the data.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::{CodecConfig, PoolConfig, SegmentSizeConfig, SizeTier};
use oceanfs_storage::{
    BufferPool, RocksDbMetadataStore, SealConfig, SegmentHeader, SegmentPool, SegmentSealer,
    WalWriter,
};

/// Creates a segment pool with EC parameters configured (k=4, m=2, strip=64).
fn make_ec_pool() -> SegmentPool {
    let ec_config = CodecConfig {
        data_shards: 4,
        parity_shards: 2,
        strip_size_bytes: 64,
        ..Default::default()
    };
    let pool_config = PoolConfig {
        ec_streaming_encode: true,
        active_pool_size: 1,
        shard_count: 1,
        max_inflight_encodes: 1,
        encode_queue_capacity: 4,
    };
    let size_config =
        SegmentSizeConfig { default_target_size: 1024, ..SegmentSizeConfig::default() };
    let buffer_pool = Arc::new(BufferPool::new(65536, 4));

    SegmentPool::new(pool_config, SizeTier::Standard, &size_config, buffer_pool, Some(ec_config))
        .unwrap()
}

/// Builds a sealer writing into `dir/segments`.
async fn make_sealer(dir: &tempfile::TempDir) -> (SegmentSealer, std::path::PathBuf) {
    let metadata = Arc::new(
        RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
            data_dir: dir.path().join("meta"),
            block_cache_size: 1024,
            memtable_size: 1024,
            ..Default::default()
        })
        .unwrap(),
    );
    let wal = Arc::new(
        WalWriter::open(&oceanfs_core::WalConfig {
            data_dir: dir.path().join("wal"),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            ..Default::default()
        })
        .await
        .unwrap(),
    );
    let seal_dir = dir.path().join("segments");
    let sealer = SegmentSealer::new(
        SealConfig {
            target_size_bytes: 1024,
            seal_timeout_ms: 5000,
            data_dir: seal_dir.clone(),
            io_mode: oceanfs_storage::io::IoReadMode::Buffered,
            write_mode: oceanfs_storage::io::SegmentWriteMode::Rename,
            ..Default::default()
        },
        metadata,
        wal,
    );
    (sealer, seal_dir)
}

#[test]
fn ec_pool_work_items_carry_ec_params() {
    let pool = make_ec_pool();

    // 1024 bytes = 4 complete stripes (k=4, strip=64 → 256 B per stripe).
    let data = vec![0xABu8; 1024];
    let (_seg_id, _offset, length) = pool.append(&data).unwrap();
    assert_eq!(length, 1024);

    let mut rx = pool.take_seal_rx().expect("seal rx");
    let work = rx.blocking_recv().expect("seal work item");

    assert_eq!(&work.segment_data[..], &data[..], "seal data must be intact");
    assert_eq!(work.ec_k, 4);
    assert_eq!(work.ec_m, 2);
    assert_eq!(work.strip_size_bytes, 64);
}

#[tokio::test]
async fn seal_from_data_persists_ec_parity_section() {
    let dir = tempfile::tempdir().unwrap();
    let (sealer, seal_dir) = make_sealer(&dir).await;

    let segment_id = oceanfs_core::SegmentId::new();
    let data = bytes::Bytes::from(vec![0xCDu8; 1024]); // 4 complete stripes

    let _handle = sealer
        .seal_from_data(segment_id, SizeTier::Standard, data.clone(), &[], 4, 2, 64, None)
        .await
        .unwrap();

    let path = seal_dir.join(format!("{segment_id}.dat"));
    let file = std::fs::read(&path).unwrap();
    let hdr = SegmentHeader::from_bytes(&file).expect("valid header");
    assert!(hdr.parity_offset > 0, "v2 file must carry the parity section");
    assert!(hdr.parity_size > 0, "parity section must be non-empty for 4 complete stripes");
    // The section contents (shard order, hash table) are verified at the
    // sealer unit level; the repair path is covered by the repair tests.
}

#[tokio::test]
async fn seal_without_ec_params_has_no_parity_section() {
    let dir = tempfile::tempdir().unwrap();
    let (sealer, seal_dir) = make_sealer(&dir).await;

    let segment_id = oceanfs_core::SegmentId::new();
    let data = bytes::Bytes::from(vec![0xEEu8; 512]);

    let _handle = sealer
        .seal_from_data(segment_id, SizeTier::Standard, data.clone(), &[], 0, 0, 0, None)
        .await
        .unwrap();

    let path = seal_dir.join(format!("{segment_id}.dat"));
    let file = std::fs::read(&path).unwrap();
    let hdr = SegmentHeader::from_bytes(&file).expect("valid header");
    assert_eq!(hdr.parity_offset, 0, "no EC → no parity section");
}
