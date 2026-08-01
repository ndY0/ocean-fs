//! Integration test: GPU-accelerated EC roundtrip.
//!
//! Tests GPU encode + CPU decode roundtrip, small segment CPU fallback,
//! and GPU cooldown/recovery behavior.
//!
//! All tests gracefully skip when no GPU is available.

#![cfg(feature = "cuda")]
#![allow(clippy::unwrap_used)]

use oceanfs_accel::CudaBackend;
use oceanfs_core::GpuConfig;
use oceanfs_ec::{Decoder, Encoder};

/// GPU encode → CPU Cauchy decode: must produce identical data.
/// This is the core cross-backend compatibility test.
#[test]
fn gpu_encode_cpu_decode_roundtrip() {
    let config = GpuConfig {
        min_segment_size: 0, // Allow small segments for testing
        ..Default::default()
    };
    let backend = match CudaBackend::new(config) {
        Some(b) => b,
        None => {
            eprintln!("SKIP: no CUDA device available");
            return;
        }
    };

    assert!(backend.is_available());

    // 4 data shards, each 256 bytes
    let data: Vec<Vec<u8>> = (0..4).map(|i| vec![(i * 16) as u8; 256]).collect();
    let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

    // GPU encode
    let parity = backend.encode(&shard_refs, 2).unwrap();
    assert_eq!(parity.len(), 2);
    assert_eq!(parity[0].len(), 256);
    assert_eq!(parity[1].len(), 256);

    // CPU decode: lose shard 0
    let available: Vec<Option<&[u8]>> = vec![
        None,
        Some(&data[1]),
        Some(&data[2]),
        Some(&data[3]),
        Some(&parity[0]),
        Some(&parity[1]),
    ];
    let recovered = backend.decode(&available, 4, 2).unwrap();
    assert_eq!(recovered.len(), 4);
    assert_eq!(recovered[0], data[0]);
    assert_eq!(recovered[1], data[1]);
    assert_eq!(recovered[2], data[2]);
    assert_eq!(recovered[3], data[3]);
}

/// GPU encode with varying shard sizes.
#[test]
fn gpu_encode_various_sizes() {
    let config = GpuConfig {
        min_segment_size: 0,
        ..Default::default()
    };
    let backend = match CudaBackend::new(config) {
        Some(b) => b,
        None => {
            eprintln!("SKIP: no CUDA device");
            return;
        }
    };

    for &size in &[16usize, 64, 128, 1024] {
        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; size]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = backend.encode(&shard_refs, 2).unwrap();
        assert_eq!(parity[0].len(), size);

        let available: Vec<Option<&[u8]>> = vec![
            Some(&data[0]),
            None,
            Some(&data[2]),
            Some(&data[3]),
            Some(&parity[0]),
            Some(&parity[1]),
        ];
        let recovered = backend.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered[1], data[1]);
    }
}

/// GPU cooldown: mark unavailable, then verify encode returns error.
#[test]
fn gpu_cooldown_prevents_encode() {
    let config = GpuConfig {
        min_segment_size: 0,
        ..Default::default()
    };
    let backend = match CudaBackend::new(config) {
        Some(b) => b,
        None => {
            eprintln!("SKIP: no CUDA device");
            return;
        }
    };

    assert!(backend.is_available());
    backend.mark_unavailable();
    assert!(!backend.is_available());

    // Encode should fail when GPU is in cooldown
    let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
    let result = backend.encode(&data, 2);
    assert!(result.is_err());
}

/// should_use_gpu respects min_segment_size threshold.
#[test]
fn should_use_gpu_threshold() {
    let config = GpuConfig {
        min_segment_size: 1024,
        ..Default::default()
    };
    let backend = match CudaBackend::new(config) {
        Some(b) => b,
        None => {
            eprintln!("SKIP: no CUDA device");
            return;
        }
    };

    assert!(!backend.should_use_gpu(512));
    assert!(backend.should_use_gpu(2048));
}
