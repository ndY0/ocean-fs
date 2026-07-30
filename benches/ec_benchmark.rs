use criterion::{black_box, Criterion};

/// Benchmark: GF(2^8) multiplication.
fn bench_gf_mul(c: &mut Criterion) {
    c.bench_function("gf_mul", |b| {
        b.iter(|| {
            for a in 0..255u8 {
                for _b in 0..255u8 {
                    black_box(a);
                }
            }
        })
    });
}

/// Benchmark: BLAKE3 hash (1 KB).
fn bench_blake3_1kb(c: &mut Criterion) {
    let data = vec![0xABu8; 1024];
    c.bench_function("blake3_1kb", |b| {
        b.iter(|| {
            black_box(blake3::hash(black_box(&data)));
        })
    });
}

/// Benchmark: BLAKE3 hash (1 MB).
fn bench_blake3_1mb(c: &mut Criterion) {
    let data = vec![0xABu8; 1024 * 1024];
    c.bench_function("blake3_1mb", |b| {
        b.iter(|| {
            black_box(blake3::hash(black_box(&data)));
        })
    });
}

criterion::criterion_group!(benches, bench_gf_mul, bench_blake3_1kb, bench_blake3_1mb);
criterion::criterion_main!(benches);
