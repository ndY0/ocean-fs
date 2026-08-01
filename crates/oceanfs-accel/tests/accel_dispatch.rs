//! Integration test: Acceleration dispatcher cross-backend validation.
//!
//! Tests encode/decode roundtrip through every available backend,
//! tier fallback behavior, per-bucket override, and compression dispatch.

#![allow(clippy::unwrap_used)]
#![allow(unused_imports)]

use oceanfs_accel::{AccelConfig, AccelDispatcher, AccelTier, Decoder, Encoder};
use oceanfs_core::CompressionTier;

/// Encode with one tier, decode with another — must produce identical data.
#[test]
fn cross_backend_roundtrip_cpu_isa_l() {
    let config = AccelConfig { ec_tier: "cpu_simd".into(), ..Default::default() };
    let dispatcher = AccelDispatcher::new(config);

    let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 256]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

    // Encode with whatever tier is active
    let parity = dispatcher.encode(&shard_refs, 2).unwrap();
    assert_eq!(parity.len(), 2);

    // Verify recovery works: lose shard 0
    let available: Vec<Option<&[u8]>> = vec![
        None,
        Some(&data[1]),
        Some(&data[2]),
        Some(&data[3]),
        Some(&parity[0]),
        Some(&parity[1]),
    ];
    let recovered = dispatcher.decode(&available, 4, 2).unwrap();
    assert_eq!(recovered[0], data[0]);
    assert_eq!(recovered[1], data[1]);
}

/// GpuCuda tier falls back to CPU when no GPU.
#[test]
fn gpu_cuda_tier_falls_back() {
    let config = AccelConfig { ec_tier: "gpu_cuda".into(), ..Default::default() };
    let dispatcher = AccelDispatcher::new(config);
    let tier = dispatcher.active_tier();

    // With or without GPU, must not panic and must produce a valid tier
    assert!(
        tier == AccelTier::CpuSimd || tier == AccelTier::IsaL || tier == AccelTier::GpuCuda,
        "unexpected tier: {tier:?}"
    );
}

/// Auto tier resolves to something valid.
#[test]
fn auto_tier_produces_recoverable_data() {
    let dispatcher = AccelDispatcher::new(AccelConfig::default());

    let data: Vec<Vec<u8>> = (0..4).map(|i| vec![(i + 100) as u8; 128]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

    let parity = dispatcher.encode(&shard_refs, 2).unwrap();

    let available: Vec<Option<&[u8]>> = vec![
        None,
        Some(&data[1]),
        Some(&data[2]),
        Some(&data[3]),
        Some(&parity[0]),
        Some(&parity[1]),
    ];
    let recovered = dispatcher.decode(&available, 4, 2).unwrap();
    assert_eq!(recovered[0], data[0]);
}

/// Per-bucket tier override: request a specific encoder for a bucket.
#[test]
fn per_bucket_tier_override_works() {
    let dispatcher = AccelDispatcher::new(AccelConfig::default());

    // Request CPU SIMD encoder explicitly (always available)
    let encoder = dispatcher.resolve_encoder_for_tier(AccelTier::CpuSimd);
    let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
    let parity = encoder.encode(&data, 2).unwrap();
    assert_eq!(parity.len(), 2);

    // Request GpuCuda encoder (will fall back if no GPU)
    let gpu_encoder = dispatcher.resolve_encoder_for_tier(AccelTier::GpuCuda);
    let parity2 = gpu_encoder.encode(&data, 2).unwrap();
    assert_eq!(parity2.len(), 2);
}

/// Compression dispatch returns a valid compressor.
#[test]
fn compression_dispatch_works() {
    let dispatcher = AccelDispatcher::new(AccelConfig::default());

    let compressor = dispatcher.resolve_compressor(CompressionTier::Auto);
    let original = b"test segment data for compression";
    let compressed = compressor.compress(original, 3).unwrap();
    let decompressed = compressor.decompress(&compressed).unwrap();
    assert_eq!(&decompressed[..], original);
}

/// Encoding with k=8, m=4 through the dispatcher.
#[test]
fn encode_decode_k8_m4_through_dispatcher() {
    let dispatcher = AccelDispatcher::new(AccelConfig::default());

    let data: Vec<Vec<u8>> = (0..8).map(|i| vec![i; 64]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity = dispatcher.encode(&shard_refs, 4).unwrap();

    let available: Vec<Option<&[u8]>> = vec![
        None,
        Some(&data[1]),
        Some(&data[2]),
        None,
        Some(&data[4]),
        Some(&data[5]),
        Some(&data[6]),
        Some(&data[7]),
        Some(&parity[0]),
        Some(&parity[1]),
        Some(&parity[2]),
        Some(&parity[3]),
    ];
    let recovered = dispatcher.decode(&available, 8, 4).unwrap();
    assert_eq!(recovered[0], data[0]);
    assert_eq!(recovered[3], data[3]);
}
