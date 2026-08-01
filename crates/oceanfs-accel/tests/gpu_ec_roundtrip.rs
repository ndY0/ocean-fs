//! Integration test: GPU-accelerated EC roundtrip.
//!
//! Tests GPU encode + CPU decode roundtrip, small segment CPU fallback,
//! and GPU cooldown/recovery behavior.
//!
//! All tests gracefully skip when no GPU is available.

#![allow(clippy::unwrap_used)]

#[cfg(all(feature = "cuda", not(no_cuda_toolkit)))]
mod gpu_tests {
    use oceanfs_accel::CudaBackend;
    use oceanfs_core::GpuConfig;
    use oceanfs_ec::{Decoder, Encoder};

    /// GPU encode → CPU Cauchy decode: must produce identical data.
    #[test]
    fn gpu_encode_cpu_decode_roundtrip() {
        let config = GpuConfig { min_segment_size: 0, ..Default::default() };
        let backend = match CudaBackend::new(config) {
            Some(b) => b,
            None => { eprintln!("SKIP: no GPU"); return; }
        };

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 256]).collect();
        let refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = backend.encode(&refs, 2).unwrap();

        let available: Vec<Option<&[u8]>> = vec![
            None, Some(&data[1]), Some(&data[2]), Some(&data[3]),
            Some(&parity[0]), Some(&parity[1]),
        ];
        let recovered = backend.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered[0], data[0]);
    }

    /// GPU should be skipped for segments below min_segment_size.
    #[test]
    fn gpu_skipped_for_small_segments() {
        let config = GpuConfig { min_segment_size: 1_000_000, ..Default::default() };
        let backend = match CudaBackend::new(config) {
            Some(b) => b,
            None => { eprintln!("SKIP: no GPU"); return; }
        };
        assert!(!backend.should_use_gpu(4096));
        assert!(backend.should_use_gpu(2_000_000));
    }

    /// Marking GPU unavailable prevents further use.
    #[test]
    fn gpu_cooldown_marks_unavailable() {
        let config = GpuConfig::default();
        let backend = match CudaBackend::new(config) {
            Some(b) => b,
            None => { eprintln!("SKIP: no GPU"); return; }
        };
        assert!(backend.is_available());
        backend.mark_unavailable();
        assert!(!backend.is_available());
    }

    /// GPU cooldown: encode fails after unavailable.
    #[test]
    fn gpu_encode_fails_after_unavailable() {
        let config = GpuConfig { min_segment_size: 0, ..Default::default() };
        let backend = match CudaBackend::new(config) {
            Some(b) => b,
            None => { eprintln!("SKIP: no GPU"); return; }
        };
        backend.mark_unavailable();
        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 64]).collect();
        let refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        assert!(backend.encode(&refs, 2).is_err());
    }
}

#[cfg(not(all(feature = "cuda", not(no_cuda_toolkit))))]
#[test]
fn gpu_unavailable_on_this_build() {}
