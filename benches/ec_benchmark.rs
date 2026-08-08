//! EC encode/decode benchmarks.
//!
//! Measures Cauchy Reed-Solomon encode and decode throughput
//! at various k/m parameters and data sizes.

use std::time::Duration;

use criterion::{black_box, Criterion, Throughput};
use oceanfs_core::CodecConfig;
use oceanfs_ec::{
    gf::gf_mul_simd, matrix::get_const_cauchy_matrix, CauchyEncoder, Decoder, Encoder,
};
use rand::Rng;

/// Benchmark: GF(2^8) multiplication (warmup benchmark).
fn bench_gf_mul(c: &mut Criterion) {
    c.bench_function("gf_mul", |b| {
        b.iter(|| {
            for a in 0..255u8 {
                for _b in 0..255u8 {
                    black_box(a);
                }
            }
        });
    });
}

/// Benchmark: EC encode at k=4, m=2, 64 KB stripe (standard OceanFS config).
fn bench_ec_encode_4_2_64k(c: &mut Criterion) {
    let config = CodecConfig {
        data_shards: 4,
        parity_shards: 2,
        strip_size_bytes: 65536,
        ..Default::default()
    };
    let encoder = CauchyEncoder::new(config);

    let data: Vec<Vec<u8>> = (0..4).map(|i| vec![(i as u8).wrapping_mul(64); 65536]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

    let mut group = c.benchmark_group("ec_encode");
    group.throughput(Throughput::Bytes(65536 * 4));
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("k4_m2_64k", |b| {
        b.iter(|| encoder.encode(black_box(&shard_refs), 2).unwrap());
    });
    group.finish();
}

/// Benchmark: EC encode at k=8, m=4, 64 KB stripe.
fn bench_ec_encode_8_4_64k(c: &mut Criterion) {
    let config = CodecConfig {
        data_shards: 8,
        parity_shards: 4,
        strip_size_bytes: 65536,
        ..Default::default()
    };
    let encoder = CauchyEncoder::new(config);

    let data: Vec<Vec<u8>> = (0..8).map(|i| vec![(i as u8).wrapping_mul(64); 65536]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

    let mut group = c.benchmark_group("ec_encode");
    group.throughput(Throughput::Bytes(65536 * 8));
    group.bench_function("k8_m4_64k", |b| {
        b.iter(|| encoder.encode(black_box(&shard_refs), 4).unwrap());
    });
    group.finish();
}

/// Benchmark: EC encode at k=16, m=8, 64 KB stripe.
fn bench_ec_encode_16_8_64k(c: &mut Criterion) {
    let config = CodecConfig {
        data_shards: 16,
        parity_shards: 8,
        strip_size_bytes: 65536,
        ..Default::default()
    };
    let encoder = CauchyEncoder::new(config);

    let data: Vec<Vec<u8>> = (0..16).map(|i| vec![(i as u8).wrapping_mul(64); 65536]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

    let mut group = c.benchmark_group("ec_encode");
    group.throughput(Throughput::Bytes(65536 * 16));
    group.bench_function("k16_m8_64k", |b| {
        b.iter(|| encoder.encode(black_box(&shard_refs), 8).unwrap());
    });
    group.finish();
}

/// Benchmark: EC encode at k=4, m=2, 4 KB stripe (small segment).
fn bench_ec_encode_4_2_4k(c: &mut Criterion) {
    let config = CodecConfig {
        data_shards: 4,
        parity_shards: 2,
        strip_size_bytes: 4096,
        ..Default::default()
    };
    let encoder = CauchyEncoder::new(config);

    let data: Vec<Vec<u8>> = (0..4).map(|i| vec![(i as u8).wrapping_mul(64); 4096]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

    let mut group = c.benchmark_group("ec_encode");
    group.throughput(Throughput::Bytes(4096 * 4));
    group.bench_function("k4_m2_4k", |b| {
        b.iter(|| encoder.encode(black_box(&shard_refs), 2).unwrap());
    });
    group.finish();
}

/// Benchmark: EC decode (recovery of 1 missing data shard) at k=4, m=2, 64 KB.
fn bench_ec_decode_4_2_64k(c: &mut Criterion) {
    let config = CodecConfig {
        data_shards: 4,
        parity_shards: 2,
        strip_size_bytes: 65536,
        ..Default::default()
    };
    let encoder = CauchyEncoder::new(config);

    let data: Vec<Vec<u8>> = (0..4).map(|i| vec![(i as u8).wrapping_mul(64); 65536]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity = encoder.encode(&shard_refs, 2).unwrap();

    // Simulate missing shard 0
    let available: Vec<Option<&[u8]>> = vec![
        None,
        Some(&data[1]),
        Some(&data[2]),
        Some(&data[3]),
        Some(&parity[0]),
        Some(&parity[1]),
    ];

    let mut group = c.benchmark_group("ec_decode");
    group.throughput(Throughput::Bytes(65536 * 4));
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("k4_m2_64k_recover1", |b| {
        b.iter(|| encoder.decode(black_box(&available), 4, 2).unwrap());
    });
    group.finish();
}

/// Benchmark: EC decode at k=8, m=4, 64 KB (recover 2 shards).
fn bench_ec_decode_8_4_64k(c: &mut Criterion) {
    let config = CodecConfig {
        data_shards: 8,
        parity_shards: 4,
        strip_size_bytes: 65536,
        ..Default::default()
    };
    let encoder = CauchyEncoder::new(config);

    let data: Vec<Vec<u8>> = (0..8).map(|i| vec![(i as u8).wrapping_mul(64); 65536]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity = encoder.encode(&shard_refs, 4).unwrap();

    // Simulate missing shards 0 and 2
    let available: Vec<Option<&[u8]>> = vec![
        None,
        Some(&data[1]),
        None,
        Some(&data[3]),
        Some(&data[4]),
        Some(&data[5]),
        Some(&data[6]),
        Some(&data[7]),
        Some(&parity[0]),
        Some(&parity[1]),
        Some(&parity[2]),
        Some(&parity[3]),
    ];

    let mut group = c.benchmark_group("ec_decode");
    group.throughput(Throughput::Bytes(65536 * 8));
    group.bench_function("k8_m4_64k_recover2", |b| {
        b.iter(|| encoder.decode(black_box(&available), 8, 4).unwrap());
    });
    group.finish();
}

/// Benchmark: GF(2^8) batched multiply via `gf_mul_simd` (64 KB).
///
/// Exercises the fastest available SIMD path — GFNI on Ice Lake+ / Zen 4+,
/// AVX-512 / AVX2 / SSE4.1 PSHUFB otherwise. Measures raw GF multiplication
/// throughput independent of EC matrix overhead.
fn bench_gf_mul_simd_64k(c: &mut Criterion) {
    let coeff = 0x7Bu8;
    let len = 65536;
    let data: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(3).wrapping_add(1)).collect();
    let mut dst = vec![0u8; len];

    let mut group = c.benchmark_group("gf_mul_simd");
    group.throughput(Throughput::Bytes(len as u64));
    group.bench_function("64k", |b| {
        b.iter(|| gf_mul_simd(coeff, black_box(&data), black_box(&mut dst)));
    });
    group.finish();
}

/// Benchmark: GF(2^8) batched multiply — 256 KB (one stripe, k=4, strip=64KB).
fn bench_gf_mul_simd_256k(c: &mut Criterion) {
    let coeff = 0x7Bu8;
    let len = 262144;
    let data: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(3).wrapping_add(1)).collect();
    let mut dst = vec![0u8; len];

    let mut group = c.benchmark_group("gf_mul_simd");
    group.throughput(Throughput::Bytes(len as u64));
    group.bench_function("256k", |b| {
        b.iter(|| gf_mul_simd(coeff, black_box(&data), black_box(&mut dst)));
    });
    group.finish();
}

/// Benchmark: detect SIMD level once (cached atomic load after first call).
fn bench_simd_level_detect(c: &mut Criterion) {
    use oceanfs_ec::gf::GfSimdLevel;
    // Force initial detection.
    let _ = GfSimdLevel::detect();

    c.bench_function("simd_level_detect_cached", |b| {
        b.iter(|| {
            let level = GfSimdLevel::detect();
            black_box(level);
        });
    });
}

/// Benchmark: const matrix lookup vs runtime matrix computation.
///
/// Measures the setup overhead saved by precomputed const matrices.
fn bench_matrix_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("cauchy_matrix");

    // Const matrix lookup (zero GF computation).
    group.bench_function("const_lookup_k4_m2", |b| {
        b.iter(|| {
            let m = get_const_cauchy_matrix(4, 2);
            black_box(m);
        });
    });

    // Runtime matrix computation (GF inverses).
    group.bench_function("runtime_compute_k4_m2", |b| {
        b.iter(|| {
            let m = CauchyEncoder::runtime_cauchy_matrix(4, 2);
            black_box(m);
        });
    });

    // Const matrix lookup — k=10, m=6 (largest supported preset).
    group.bench_function("const_lookup_k10_m6", |b| {
        b.iter(|| {
            let m = get_const_cauchy_matrix(10, 6);
            black_box(m);
        });
    });

    group.bench_function("runtime_compute_k10_m6", |b| {
        b.iter(|| {
            let m = CauchyEncoder::runtime_cauchy_matrix(10, 6);
            black_box(m);
        });
    });

    group.finish();
}

/// Benchmark: full EC encode of one stripe (k=4, m=2, strip=64KB) —
/// measuring the total encode cost that streaming encode pays per stripe.
fn bench_encode_single_stripe(c: &mut Criterion) {
    let config = CodecConfig {
        data_shards: 4,
        parity_shards: 2,
        strip_size_bytes: 65536,
        ..Default::default()
    };
    let encoder = CauchyEncoder::new(config);

    let mut rng = rand::thread_rng();
    let data: Vec<Vec<u8>> = (0..4).map(|_| (0..65536).map(|_| rng.gen()).collect()).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

    let mut group = c.benchmark_group("ec_encode_single_stripe");
    group.throughput(Throughput::Bytes(65536 * 4));
    group.bench_function("k4_m2_64k", |b| {
        b.iter(|| encoder.encode(black_box(&shard_refs), 2).unwrap());
    });
    group.finish();
}

criterion::criterion_group!(
    benches,
    bench_gf_mul,
    bench_gf_mul_simd_64k,
    bench_gf_mul_simd_256k,
    bench_simd_level_detect,
    bench_matrix_lookup,
    bench_encode_single_stripe,
    bench_ec_encode_4_2_64k,
    bench_ec_encode_8_4_64k,
    bench_ec_encode_16_8_64k,
    bench_ec_encode_4_2_4k,
    bench_ec_decode_4_2_64k,
    bench_ec_decode_8_4_64k,
);
criterion::criterion_main!(benches);
