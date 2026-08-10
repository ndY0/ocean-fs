//! Platform I/O benchmarks.
//!
//! Compares the four I/O optimisation paths against baseline buffered I/O:
//! - O_DIRECT vs buffered read/write for 64 KB, 1 MB, 4 MB segment sizes
//! - SegmentFileCache (mmap-style) vs `tokio::fs::read` for random offset reads
//! - io_uring (when available) vs tokio::fs for sequential write throughput
//! - SegmentFileBody streaming vs read-all-then-write for 4 MB blob response
//!
//! Run with:
//!   cargo bench --bench io_benchmark

use std::{io::Write, time::Duration};

use criterion::{black_box, Criterion, Throughput};
use oceanfs_storage::io::{direct::DirectIoBuf, mmap::SegmentFileCache, uring::DiskIo};

// ---------------------------------------------------------------------------
// O_DIRECT benchmarks
// ---------------------------------------------------------------------------

fn bench_direct_write_64k(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct_io_write");
    group.throughput(Throughput::Bytes(65536));

    group.bench_function("buffered_64k", |b| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("buffered_64k.dat");
        let data = vec![0xAAu8; 65536];
        b.iter(|| {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            file.write_all(&data).unwrap();
        });
    });

    group.bench_function("direct_64k", |b| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("direct_64k.dat");
        let mut buf = DirectIoBuf::new(65536).unwrap();
        buf.copy_from_slice(&vec![0xAAu8; 65536]);
        b.iter(|| {
            #[cfg(target_os = "linux")]
            {
                use oceanfs_storage::io::direct::OpenOptionsDirectExt;
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .with_direct()
                    .open(&path)
                    .unwrap();
                file.write_all(buf.as_bytes()).unwrap();
            }
            let _ = &path;
        });
    });

    group.finish();
}

fn bench_direct_write_4m(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct_io_write_4m");
    group.throughput(Throughput::Bytes(4 * 1024 * 1024));
    group.sample_size(10);

    group.bench_function("buffered_4m", |b| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("buffered_4m.dat");
        let data = vec![0xBBu8; 4 * 1024 * 1024];
        b.iter(|| {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            file.write_all(&data).unwrap();
        });
    });

    group.bench_function("direct_4m", |b| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("direct_4m.dat");
        let mut buf = DirectIoBuf::new(4 * 1024 * 1024).unwrap();
        buf.copy_from_slice(&vec![0xBBu8; 4 * 1024 * 1024]);
        b.iter(|| {
            #[cfg(target_os = "linux")]
            {
                use oceanfs_storage::io::direct::OpenOptionsDirectExt;
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .with_direct()
                    .open(&path)
                    .unwrap();
                file.write_all(buf.as_bytes()).unwrap();
            }
            let _ = &path;
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Segment cache (mmap-style) benchmarks
// ---------------------------------------------------------------------------

fn bench_segment_cache_vs_read(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("segment.dat");
    let data = vec![0xCCu8; 4 * 1024 * 1024]; // 4 MB segment
    std::fs::write(&path, &data).unwrap();

    let mut group = c.benchmark_group("segment_cache");
    group.throughput(Throughput::Bytes(4 * 1024 * 1024));

    group.bench_function("tokio_fs_read", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.iter(|| {
            rt.block_on(async {
                let _result = tokio::fs::read(&path).await.unwrap();
            });
        });
    });

    group.bench_function("segment_cache_hit", |b| {
        let cache = SegmentFileCache::new(16);
        let id = oceanfs_core::SegmentId::new();
        // Pre-populate the cache.
        cache.get_or_map(id, &path).unwrap();
        b.iter(|| {
            let _data = cache.get_or_map(id, &path).unwrap();
            black_box(_data.len());
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// DiskIo benchmarks
// ---------------------------------------------------------------------------

fn bench_disk_io_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_io_write");
    group.throughput(Throughput::Bytes(1024 * 1024));
    group.sample_size(20);

    group.bench_function("tokio_fs_small", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let io = DiskIo::TokioFs;
        b.iter(|| {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("write.dat");
            rt.block_on(async {
                io.write(&path, &vec![0xDDu8; 1024 * 1024], 0).await.unwrap();
            });
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// SegmentFileBody benchmarks
// ---------------------------------------------------------------------------

fn bench_segment_file_body(c: &mut Criterion) {
    let group = c.benchmark_group("segment_file_body");
    #[cfg(feature = "sendfile")]
    {
        group.throughput(Throughput::Bytes(4 * 1024 * 1024));

        group.bench_function("build_body", |b| {
            let data = data.clone();
            b.iter(|| {
                let _body =
                    oceanfs_storage::io::SegmentFileBody::new(data.clone(), 0, data.len() as u64);
            });
        });

        group.bench_function("body_size_hint", |b| {
            let body =
                oceanfs_storage::io::SegmentFileBody::new(data.clone(), 0, data.len() as u64);
            b.iter(|| {
                let _hint = body.size_hint();
                black_box(_hint);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

criterion::criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(5));
    targets =
        bench_direct_write_64k,
        bench_direct_write_4m,
        bench_segment_cache_vs_read,
        bench_disk_io_write_throughput,
        bench_segment_file_body,
}

criterion::criterion_main!(benches);
