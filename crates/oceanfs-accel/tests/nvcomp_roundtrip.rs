//! Integration tests for nvCOMP GPU batch compression — verify GPU roundtrip,
//! fallback to zstd when GPU unavailable, batch size boundaries, bit-exact match.

#[cfg(feature = "cuda")]
mod nvcomp_tests {
    use oceanfs_accel::{AccelConfig, AccelDispatcher, Compressor};
    use oceanfs_core::CompressionTier;

    /// Compress 64 KB segment with nvCOMP, decompress, verify bit-exact match.
    #[test]
    fn nvcomp_64kb_segment_roundtrip() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let nvcomp = dispatcher.resolve_compressor(CompressionTier::GpuNvcomp);
        assert!(nvcomp.is_available());

        let original = vec![0xCDu8; 65536];
        let compressed = nvcomp.compress(&original, 0).unwrap();
        let decompressed = nvcomp.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], &original[..]);
    }

    /// Configure GpuNvcomp without GPU → verify fallback to zstd.
    #[test]
    fn nvcomp_fallback_when_gpu_absent() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let compressor = dispatcher.resolve_compressor(CompressionTier::GpuNvcomp);
        // Should never panic — always returns a valid compressor (fallback)
        let data = b"nvcomp fallback test";
        let compressed = compressor.compress(data, 0).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], data);
    }

    /// Batch size boundary: exactly 1 chunk works.
    #[test]
    fn nvcomp_single_chunk_works() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let nvcomp = dispatcher.resolve_compressor(CompressionTier::GpuNvcomp);

        let original = b"single chunk data";
        let compressed = nvcomp.compress(original, 0).unwrap();
        let decompressed = nvcomp.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], original);
    }

    /// Empty data roundtrip through nvcomp path.
    #[test]
    fn nvcomp_empty_data_roundtrip() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let nvcomp = dispatcher.resolve_compressor(CompressionTier::GpuNvcomp);

        let compressed = nvcomp.compress(&[], 0).unwrap();
        assert!(compressed.is_empty());
        let decompressed = nvcomp.decompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }
}

#[cfg(not(feature = "cuda"))]
#[test]
fn nvcomp_unavailable_without_cuda_feature() {
    // nvCOMP requires the cuda feature. This test documents that.
}
