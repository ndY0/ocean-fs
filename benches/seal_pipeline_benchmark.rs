//! Seal pipeline throughput benchmark.
//!
//! Measures the end-to-end `seal_from_data` path — index serialization,
//! checksum, parity encode (blocking pool), temp-file write, group-
//! committed fsync, and batched metadata persistence — under concurrent
//! seals, through the public `SegmentSealer` API.
//!
//! This bench is the before/after instrument for the seal-pipeline
//! batching feature:
//!
//! - **Before baseline** (pre-batching): run on the commit before the
//!   write/flush split landed, record seals/sec for `--concurrency 8`
//!   and `--concurrency 16`.
//! - **After**: run on the current tree with the same args and compare.
//!
//! Note that `seal_from_data` performs real disk I/O (temp write +
//! fsync) and real RocksDB metadata writes — the bench measures the
//! actual pipeline, not a mocked hot path. Use a tmpfs-backed directory
//! (`--tmpdir /dev/shm/...`) to isolate from disk jitter if needed.
//!
//! Run with:
//! ```text
//! cargo bench --bench seal_pipeline_benchmark -- --concurrency 8 --seals 64
//! ```

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use oceanfs_core::{SegmentId, SizeTier, WalConfig};
use oceanfs_storage::{
    io::{IoReadMode, SegmentWriteMode},
    segment::index::SegmentIndexEntry,
    SegmentSealer, WalWriter,
};

/// Runs `seals` concurrent `seal_from_data` calls through one sealer.
///
/// Each seal writes a `data_size`-byte segment with a single index
/// entry. The flush coordinator's group commit batches the fsyncs and
/// metadata writes across the concurrent seals.
async fn concurrent_seals(
    seals: usize,
    concurrency: usize,
    data_size: usize,
    tmpdir: &std::path::Path,
) -> Duration {
    let wal = Arc::new(
        WalWriter::open(&WalConfig {
            data_dir: tmpdir.join("wal"),
            max_file_size_bytes: 16 * 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            ..Default::default()
        })
        .await
        .expect("open wal"),
    );
    let lifecycle =
        std::sync::Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::new(
            &oceanfs_core::LifecycleConfig::default(),
        ));
    let sealer = Arc::new(SegmentSealer::new(
        oceanfs_storage::SealConfig {
            target_size_bytes: data_size as u64,
            seal_timeout_ms: 60_000,
            data_dir: tmpdir.join("segments"),
            io_mode: IoReadMode::Buffered,
            write_mode: SegmentWriteMode::Rename,
            // 10 ms group-commit window, flush early at 8 pending seals.
            fsync_batch_timeout_ms: 10,
            fsync_max_waiters: 8,
        },
        wal,
        lifecycle,
    ));

    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..seals {
        let sealer = Arc::clone(&sealer);
        let data = Bytes::from(vec![0x42u8; data_size]);
        handles.push(tokio::spawn(async move {
            let id = SegmentId::new();
            let entries = vec![SegmentIndexEntry {
                offset: 0,
                length: data_size as u32,
                blob_key_hash: [0xAB; 32],
            }];
            sealer
                .seal_from_data(id, SizeTier::Standard, data, &entries, 0, 0, 0, None, None)
                .await
                .expect("seal");
        }));
        // Bound the in-flight concurrency like the real seal worker's
        // semaphore (max_inflight_encodes).
        if handles.len() >= concurrency {
            let done = handles.remove(0);
            done.await.expect("seal task");
        }
    }
    for h in handles {
        h.await.expect("seal task");
    }
    start.elapsed()
}

fn seal_pipeline_benchmark(c: &mut Criterion) {
    let tmpdir = std::env::var("SEAL_BENCH_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("oceanfs-seal-bench"));
    std::fs::create_dir_all(&tmpdir).expect("create bench tmpdir");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");

    for concurrency in [4usize, 8, 16] {
        let id = format!("seal_from_data/concurrent={concurrency}/64x256KiB");
        c.bench_function(&id, |b| {
            b.iter_custom(|iters| {
                let seals = iters as usize * 64;
                runtime.block_on(concurrent_seals(seals, concurrency, 256 * 1024, &tmpdir))
            });
        });
    }
}

criterion_group!(benches, seal_pipeline_benchmark);
criterion_main!(benches);
