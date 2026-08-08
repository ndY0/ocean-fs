//! WAL sync latency benchmarks.
//!
//! Compares `sync_file_range` + `fdatasync` vs `sync_all` for
//! append-only WAL writes. On NVMe, the combined approach is
//! expected to be 2-3x faster because it saves one disk barrier
//! (inode metadata flush).
//!
//! Run with:
//!   cargo bench --bench wal_sync_benchmark

use std::io::Write;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tempfile::tempdir;

fn write_and_sync_all(file: &mut std::fs::File, data: &[u8]) {
    file.write_all(data).unwrap();
    file.sync_all().unwrap();
}

fn write_and_sync_range_fdatasync(file: &mut std::fs::File, data: &[u8]) {
    use std::os::unix::io::AsRawFd;

    let offset = file.metadata().unwrap().len();
    file.write_all(data).unwrap();
    file.flush().unwrap();

    #[cfg(target_os = "linux")]
    {
        let fd = file.as_raw_fd();
        let len = data.len() as u64;
        unsafe {
            libc::sync_file_range(
                fd,
                offset as libc::off64_t,
                len as libc::off64_t,
                libc::SYNC_FILE_RANGE_WRITE,
            );
        }
    }
    file.sync_data().unwrap();
}

fn bench_wal_sync_small(c: &mut Criterion) {
    let data = vec![0xABu8; 512]; // typical WAL entry size
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal_small.log");

    let mut group = c.benchmark_group("wal_sync/small_512b");
    group.throughput(Throughput::Bytes(512));

    group.bench_function("sync_all", |b| {
        b.iter(|| {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            write_and_sync_all(&mut file, &data);
        });
    });

    group.bench_function("sync_range_fdatasync", |b| {
        b.iter(|| {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            write_and_sync_range_fdatasync(&mut file, &data);
        });
    });

    group.finish();
}

fn bench_wal_sync_batch(c: &mut Criterion) {
    let data = vec![0xCDu8; 4096]; // batched entry
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal_batch.log");

    let mut group = c.benchmark_group("wal_sync/batch_4kb");
    group.throughput(Throughput::Bytes(4096));

    group.bench_function("sync_all", |b| {
        b.iter(|| {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            write_and_sync_all(&mut file, &data);
        });
    });

    group.bench_function("sync_range_fdatasync", |b| {
        b.iter(|| {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            write_and_sync_range_fdatasync(&mut file, &data);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_wal_sync_small, bench_wal_sync_batch);
criterion_main!(benches);
