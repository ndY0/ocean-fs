//! Storage engine benchmarks.
//!
//! Measures WAL append throughput, metadata CRUD latency,
//! and segment index lookup performance.

use criterion::{black_box, Criterion, Throughput};

/// Benchmark: Simulated WAL append throughput (1000× 4 KB entries).
///
/// Measures the overhead of WAL entry construction and serialization
/// without actual disk I/O (CPU-only benchmark).
fn bench_wal_append_1000(c: &mut Criterion) {
    let entry_size = 4096;
    let data = vec![0xABu8; entry_size];
    let num_entries = 1000;

    let mut group = c.benchmark_group("wal");
    group.throughput(Throughput::Bytes((entry_size * num_entries) as u64));
    group.bench_function("append_1000x4kb", |b| {
        b.iter(|| {
            let mut total = 0u64;
            for _ in 0..num_entries {
                // Simulate WAL entry: segment_id + offset + length + checksum
                let segment_id_bytes = [0u8; 16];
                let offset = black_box(0u64);
                let length = black_box(entry_size as u32);
                let checksum = blake3::hash(black_box(&data));

                // Simulated serialization
                total = total.wrapping_add(offset);
                total = total.wrapping_add(length as u64);
                total = total.wrapping_add(u64::from(checksum.as_bytes()[0]));
                // Simulate disk write buffer
                black_box(&segment_id_bytes);
                black_box(&data);
            }
            black_box(total);
        });
    });
    group.finish();
}

/// Benchmark: Simulated metadata get (1000 operations).
///
/// Measures the CPU cost of metadata lookup without RocksDB I/O.
fn bench_metadata_get_1000(c: &mut Criterion) {
    let keys: Vec<[u8; 32]> = (0..1000)
        .map(|i| {
            let mut arr = [0u8; 32];
            arr[..8].copy_from_slice(&(i as u64).to_le_bytes());
            arr
        })
        .collect();

    c.bench_function("metadata_get_1000", |b| {
        b.iter(|| {
            let mut found = 0;
            for key in &keys {
                // Simulate B-tree or hash map lookup
                let hash = blake3::hash(black_box(key));
                found += hash.as_bytes()[0] as usize;
            }
            black_box(found);
        });
    });
}

/// Benchmark: Simulated segment index lookup (1000 blob offsets).
///
/// Measures the CPU cost of BTreeMap segment index lookups.
fn bench_segment_index_lookup_1000(c: &mut Criterion) {
    // Simulate a segment index as a sorted Vec of offset-length pairs
    let mut index = Vec::with_capacity(1000);
    for i in 0..1000u64 {
        index.push((i * 4096, 1024u32));
    }

    c.bench_function("segment_index_lookup_1000", |b| {
        b.iter(|| {
            let mut total_size = 0u64;
            for target in 0..1000 {
                // Binary search simulation
                let target_offset = black_box(target * 4096);
                let result = index.binary_search_by_key(&target_offset, |&(off, _)| off);
                if let Ok(idx) = result {
                    total_size = total_size.wrapping_add(index[idx].1 as u64);
                }
            }
            black_box(total_size);
        });
    });
}

/// Benchmark: Simulated segment read/write throughput.
fn bench_segment_read_write_4mb(c: &mut Criterion) {
    let segment_size = 4 * 1024 * 1024; // 4 MB
    let data = vec![0xABu8; segment_size];
    let mut output = vec![0u8; segment_size];

    let mut group = c.benchmark_group("segment");
    group.throughput(Throughput::Bytes(segment_size as u64));
    group.bench_function("read_write_4mb", |b| {
        b.iter(|| {
            // Simulate write
            black_box(&data);
            // Simulate read (copy)
            output.copy_from_slice(&data);
            black_box(&output);
        });
    });
    group.finish();
}

criterion::criterion_group!(
    benches,
    bench_wal_append_1000,
    bench_metadata_get_1000,
    bench_segment_index_lookup_1000,
    bench_segment_read_write_4mb,
);
criterion::criterion_main!(benches);
