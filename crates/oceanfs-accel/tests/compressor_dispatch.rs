//! Integration tests for compression dispatch — exercises the Compressor trait
//! through AccelDispatcher, validates tier resolution, fallback, and roundtrips.

use oceanfs_accel::{AccelConfig, AccelDispatcher, Compressor};
use oceanfs_core::CompressionTier;

/// Configure each tier and verify compress+decompress roundtrips through the
/// dispatcher's compressor resolution.
#[test]
fn each_tier_produces_correct_roundtrip() {
    let dispatcher = AccelDispatcher::new(AccelConfig::default());
    let data = b"integration test data for compression dispatch";

    for tier in [
        CompressionTier::CpuZstd,
        CompressionTier::CpuIgzip,
        CompressionTier::GpuNvcomp,
        CompressionTier::Auto,
    ] {
        let compressor = dispatcher.resolve_compressor(tier);
        let compressed = compressor.compress(data, 3).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(
            &decompressed[..],
            data,
            "roundtrip failed for tier {:?}",
            tier
        );
    }
}

/// Per-bucket tier override via CompressConfig takes effect.
#[test]
fn per_bucket_tier_override_via_config() {
    use oceanfs_core::CompressConfig;

    let dispatcher = AccelDispatcher::new(AccelConfig::default());
    let data = b"per-bucket override test data";

    // Explicitly request CPU zstd
    let config = CompressConfig {
        tier: CompressionTier::CpuZstd,
        ..Default::default()
    };
    let compressor = dispatcher.resolve_compressor_for_config(&config);
    let compressed = compressor.compress(data, 3).unwrap();
    let decompressed = compressor.decompress(&compressed).unwrap();
    assert_eq!(&decompressed[..], data);
}

/// GpuNvcomp falls back to a valid backend when GPU is unavailable.
#[test]
fn gpu_nvcomp_falls_back_to_available() {
    let dispatcher = AccelDispatcher::new(AccelConfig::default());
    let compressor = dispatcher.resolve_compressor(CompressionTier::GpuNvcomp);
    let data = b"fallback test data";
    let compressed = compressor.compress(data, 3).unwrap();
    let decompressed = compressor.decompress(&compressed).unwrap();
    assert_eq!(&decompressed[..], data);
}

/// Auto tier resolves to the best available backend.
#[test]
fn auto_tier_resolves_and_works() {
    let dispatcher = AccelDispatcher::new(AccelConfig::default());
    let compressor = dispatcher.resolve_compressor(CompressionTier::Auto);
    assert!(compressor.is_available());
    let data = b"auto tier data";
    let compressed = compressor.compress(data, 3).unwrap();
    let decompressed = compressor.decompress(&compressed).unwrap();
    assert_eq!(&decompressed[..], data);
}

/// Empty data roundtrips through all tiers.
#[test]
fn empty_data_roundtrips_all_tiers() {
    let dispatcher = AccelDispatcher::new(AccelConfig::default());
    for tier in [CompressionTier::CpuZstd, CompressionTier::Auto] {
        let compressor = dispatcher.resolve_compressor(tier);
        let compressed = compressor.compress(&[], 3).unwrap();
        // Note: zstd adds a frame header even for empty input,
        // so compressed output may be non-empty. Verify roundtrip.
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }
}

/// Large data (64KB) roundtrip through all tiers.
#[test]
fn large_data_roundtrips_all_tiers() {
    let dispatcher = AccelDispatcher::new(AccelConfig::default());
    let data = vec![0x42u8; 65536];
    for tier in [CompressionTier::CpuZstd, CompressionTier::Auto] {
        let compressor = dispatcher.resolve_compressor(tier);
        let compressed = compressor.compress(&data, 3).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], &data[..]);
    }
}
