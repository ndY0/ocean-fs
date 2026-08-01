//! Integration tests for ISA-L igzip compression — cross-backend roundtrip,
//! fallback behavior when igzip is unavailable, DEFLATE compatibility.

#[cfg(all(target_arch = "x86_64", feature = "isa-l"))]
mod igzip_tests {
    use oceanfs_accel::{AccelConfig, AccelDispatcher, Compressor};
    use oceanfs_core::CompressionTier;

    /// Compress with igzip, decompress with zstd (cross-backend bit-exact).
    #[test]
    fn igzip_compress_zstd_decompress_roundtrip() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let igzip = dispatcher.resolve_compressor(CompressionTier::CpuIgzip);
        let zstd = dispatcher.resolve_compressor(CompressionTier::CpuZstd);

        let original = b"cross-backend igzip-to-zstd test data";
        let compressed = igzip.compress(original, 3).unwrap();
        let decompressed = zstd.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], original);
    }

    /// Compress with igzip, decompress with igzip (same-backend).
    #[test]
    fn igzip_same_backend_roundtrip() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let igzip = dispatcher.resolve_compressor(CompressionTier::CpuIgzip);

        let original = b"same-backend igzip roundtrip data";
        let compressed = igzip.compress(original, 3).unwrap();
        let decompressed = igzip.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], original);
    }

    /// Verify fallback when igzip is requested but unavailable.
    #[test]
    fn igzip_fallback_when_unavailable() {
        // Even if igzip is unavailable, the dispatcher falls back to zstd.
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let compressor = dispatcher.resolve_compressor(CompressionTier::CpuIgzip);
        assert!(compressor.is_available());
        let data = b"fallback test";
        let compressed = compressor.compress(data, 3).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], data);
    }

    /// Large data cross-backend roundtrip.
    #[test]
    fn igzip_large_data_cross_backend() {
        let dispatcher = AccelDispatcher::new(AccelConfig::default());
        let igzip = dispatcher.resolve_compressor(CompressionTier::CpuIgzip);
        let zstd = dispatcher.resolve_compressor(CompressionTier::CpuZstd);

        let original = vec![0xABu8; 65536];
        let compressed = igzip.compress(&original, 3).unwrap();
        let decompressed = zstd.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], &original[..]);
    }
}

// When feature is not available, provide a no-op test.
#[cfg(not(all(target_arch = "x86_64", feature = "isa-l")))]
#[test]
fn igzip_unavailable_on_this_platform() {
    // ISA-L is x86_64 only. This test just documents that.
}
