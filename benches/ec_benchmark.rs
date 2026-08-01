//! EC encode/decode benchmarks.
//!
//! Measures Cauchy Reed-Solomon encode and decode throughput
//! at various k/m parameters and data sizes.

use std::time::Duration;

use criterion::{black_box, Criterion, Throughput};
use oceanfs_core::CodecConfig;
use oceanfs_ec::{CauchyEncoder, Decoder, Encoder};

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

criterion::criterion_group!(
    benches,
    bench_gf_mul,
    bench_ec_encode_4_2_64k,
    bench_ec_encode_8_4_64k,
    bench_ec_encode_16_8_64k,
    bench_ec_encode_4_2_4k,
    bench_ec_decode_4_2_64k,
    bench_ec_decode_8_4_64k,
);
criterion::criterion_main!(benches);
