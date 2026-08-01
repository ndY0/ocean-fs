//! BLAKE3 hashing benchmarks.
//!
//! Measures BLAKE3 throughput at various data sizes to characterize
//! the hashing bottleneck in the write/read path.

use criterion::{black_box, Criterion, Throughput};

/// Benchmark: BLAKE3 hash at 1 KB (small inline blob).
fn bench_blake3_1kb(c: &mut Criterion) {
    let data = vec![0xABu8; 1024];
    let mut group = c.benchmark_group("blake3");
    group.throughput(Throughput::Bytes(1024));
    group.bench_function("1kb", |b| {
        b.iter(|| {
            black_box(blake3::hash(black_box(&data)));
        });
    });
    group.finish();
}

/// Benchmark: BLAKE3 hash at 64 KB (small segment stripe).
fn bench_blake3_64kb(c: &mut Criterion) {
    let data = vec![0xABu8; 64 * 1024];
    let mut group = c.benchmark_group("blake3");
    group.throughput(Throughput::Bytes(64 * 1024));
    group.bench_function("64kb", |b| {
        b.iter(|| {
            black_box(blake3::hash(black_box(&data)));
        });
    });
    group.finish();
}

/// Benchmark: BLAKE3 hash at 1 MB (medium blob).
fn bench_blake3_1mb(c: &mut Criterion) {
    let data = vec![0xABu8; 1024 * 1024];
    let mut group = c.benchmark_group("blake3");
    group.throughput(Throughput::Bytes(1024 * 1024));
    group.bench_function("1mb", |b| {
        b.iter(|| {
            black_box(blake3::hash(black_box(&data)));
        });
    });
    group.finish();
}

/// Benchmark: BLAKE3 hash at 100 MB (large blob / multi-segment).
fn bench_blake3_100mb(c: &mut Criterion) {
    let data = vec![0xABu8; 100 * 1024 * 1024];
    let mut group = c.benchmark_group("blake3");
    group.throughput(Throughput::Bytes(100 * 1024 * 1024));
    group.sample_size(10);
    group.bench_function("100mb", |b| {
        b.iter(|| {
            black_box(blake3::hash(black_box(&data)));
        });
    });
    group.finish();
}

/// Benchmark: BLAKE3 streaming hash at 1 MB (simulates network receive).
fn bench_blake3_streaming_1mb(c: &mut Criterion) {
    // Simulates streaming hash: update in 4 KB chunks
    let chunk = vec![0xABu8; 4096];
    let num_chunks = 256; // 256 * 4 KB = 1 MB

    c.bench_function("blake3_streaming_1mb", |b| {
        b.iter(|| {
            let mut hasher = blake3::Hasher::new();
            for _ in 0..num_chunks {
                hasher.update(black_box(&chunk));
            }
            black_box(hasher.finalize());
        });
    });
}

criterion::criterion_group!(
    benches,
    bench_blake3_1kb,
    bench_blake3_64kb,
    bench_blake3_1mb,
    bench_blake3_100mb,
    bench_blake3_streaming_1mb,
);
criterion::criterion_main!(benches);
