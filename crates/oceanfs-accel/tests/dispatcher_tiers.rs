//! Integration test for AccelDispatcher tier selection.

#![allow(clippy::unwrap_used)]

use oceanfs_accel::{AccelConfig, AccelDispatcher, AccelTier};

#[test]
fn auto_tier_resolves_to_cpu_simd() {
    let dispatcher = AccelDispatcher::new(AccelConfig::default());
    let tier = dispatcher.active_tier();
    // Without cuda, auto → CpuSimd. With cuda + GPU, auto → GpuCuda.
    assert!(tier == AccelTier::CpuSimd || tier == AccelTier::GpuCuda, "unexpected tier: {tier:?}");
}

#[test]
fn cpu_simd_explicit_selects_simd() {
    let config = AccelConfig { ec_tier: "cpu_simd".into(), ..Default::default() };
    let dispatcher = AccelDispatcher::new(config);
    assert_eq!(dispatcher.active_tier(), AccelTier::CpuSimd);
}

#[test]
fn isal_falls_back_to_cpu_simd_without_feature() {
    let config = AccelConfig { ec_tier: "isa_l".into(), ..Default::default() };
    let dispatcher = AccelDispatcher::new(config);
    assert_eq!(dispatcher.active_tier(), AccelTier::CpuSimd);
}

#[test]
fn gpu_cuda_falls_back_to_cpu_simd_without_feature() {
    let config = AccelConfig { ec_tier: "gpu_cuda".into(), ..Default::default() };
    let dispatcher = AccelDispatcher::new(config);
    // Without cuda feature: falls back to CpuSimd.
    // With cuda feature + GPU: stays at GpuCuda. Both are valid.
    let tier = dispatcher.active_tier();
    assert!(tier == AccelTier::CpuSimd || tier == AccelTier::GpuCuda, "unexpected tier: {tier:?}");
}

#[test]
fn all_tiers_can_be_constructed() {
    let tiers = ["auto", "cpu_simd", "isa_l", "gpu_cuda"];
    for &tier in &tiers {
        let config = AccelConfig { ec_tier: tier.into(), ..Default::default() };
        let dispatcher = AccelDispatcher::new(config);
        let resolved = dispatcher.active_tier();
        // All tiers resolve to something valid
        assert!(
            resolved == AccelTier::CpuSimd
                || resolved == AccelTier::IsaL
                || resolved == AccelTier::GpuCuda
                || resolved == AccelTier::Auto,
            "unexpected tier for config '{tier}': {resolved:?}"
        );
    }
}

#[test]
fn dispatcher_encode_decode_roundtrip() {
    use oceanfs_accel::{Decoder, Encoder};

    let dispatcher = AccelDispatcher::new(AccelConfig::default());
    let data: Vec<Vec<u8>> = (0..4).map(|i| vec![b'0' + i; 128]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity = dispatcher.encode(&shard_refs, 2).unwrap();
    assert_eq!(parity.len(), 2);

    let available: Vec<Option<&[u8]>> = data
        .iter()
        .map(|v| v.as_slice())
        .map(Some)
        .chain(parity.iter().map(|v| v.as_slice()).map(Some))
        .collect();
    let recovered = dispatcher.decode(&available, 4, 2).unwrap();
    assert_eq!(recovered, data);
}

#[test]
fn dispatcher_resolve_encoder_for_tier_fallback() {
    let dispatcher = AccelDispatcher::new(AccelConfig::default());
    let encoder = dispatcher.resolve_encoder_for_tier(AccelTier::GpuCuda);
    // Should fall back to CPU encoder (which works)
    let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
    let _parity = encoder.encode(&data, 2).unwrap();
}
