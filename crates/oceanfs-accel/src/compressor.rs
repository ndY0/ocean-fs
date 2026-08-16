//! Compression trait and backends for the acceleration subsystem.
//!
//! The [`Compressor`] trait is modeled on the `Encoder` trait from `oceanfs-ec`.
//! It abstracts compression and decompression of segment data, enabling
//! pluggable backends: Tier 0 (zstd crate, always available), Tier 1 (ISA-L
//! igzip, feature-gated behind `isa-l`), and Tier 2 (nvCOMP GPU batch,
//! feature-gated behind `cuda`). Per-bucket tier selection is driven by the
//! `compress_tier` field in `BucketPolicy`.
//!
//! ## Fallback Chain
//!
//! ```text
//! GpuNvcomp → CpuIgzip → CpuZstd   (always terminates at Zstd)
//! ```

use bytes::Bytes;
use oceanfs_core::CompressionTier;

use crate::{error::AccelError, Result};

/// A compression backend for segment data.
///
/// Implementations range from CPU zstd (always available) to GPU-accelerated
/// nvCOMP (requires CUDA feature + nvCOMP library). The trait is `Send + Sync`
/// so backends can be called from Rayon parallel iterators during segment
/// sealing.
///
/// # Examples
///
/// ```
/// use oceanfs_accel::{Compressor, ZstdCompressor};
/// use oceanfs_core::CompressionTier;
///
/// let compressor = ZstdCompressor::new(3);
/// assert!(compressor.is_available());
/// assert_eq!(compressor.compression_tier(), CompressionTier::CpuZstd);
///
/// let data = b"segment data to compress";
/// let compressed = compressor.compress(data, 3).unwrap();
/// let decompressed = compressor.decompress(&compressed).unwrap();
/// assert_eq!(&decompressed[..], data);
/// ```
pub trait Compressor: Send + Sync {
    /// Compresses data with the given compression level.
    ///
    /// Higher levels produce smaller output at the cost of more CPU/GPU time.
    /// Level interpretation varies by backend:
    /// - zstd: 0-22 (3 is default, balanced)
    /// - igzip: 0-3 (3 is maximum DEFLATE compression)
    /// - nvCOMP: codec-dependent
    ///
    /// # Errors
    ///
    /// Returns [`AccelError::CompressionError`] if compression fails
    /// (e.g., corrupt internal state, memory allocation failure, GPU OOM).
    fn compress(&self, data: &[u8], level: u32) -> Result<Bytes>;

    /// Decompresses previously compressed data.
    ///
    /// The caller must ensure `data` was produced by a compatible compressor.
    /// Cross-backend decompression (e.g., igzip-compressed data decompressed
    /// by zstd) is supported for DEFLATE-compatible codecs but not guaranteed
    /// for all backend pairs.
    ///
    /// # Errors
    ///
    /// Returns [`AccelError::CompressionError`] if decompression fails
    /// (e.g., corrupt data, unsupported format).
    fn decompress(&self, data: &[u8]) -> Result<Bytes>;

    /// Decompresses data whose uncompressed size is known in advance.
    ///
    /// Backends that support exact-size decompression allocate exactly
    /// `expected_len` bytes — a single allocation, no reallocations on
    /// the read path. The default implementation falls back to
    /// [`Self::decompress`].
    ///
    /// # Errors
    ///
    /// Returns [`AccelError::CompressionError`] if decompression fails.
    fn decompress_exact(&self, data: &[u8], _expected_len: usize) -> Result<Bytes> {
        self.decompress(data)
    }

    /// Compresses data into a caller-provided output buffer.
    ///
    /// `out` must be large enough for the compressed output; backends
    /// that expose worst-case bounds (zstd, DEFLATE) write directly and
    /// return the number of bytes written — enabling buffer reuse across
    /// chunks (the write path compresses whole objects chunk by chunk
    /// into a single per-put scratch buffer). The default implementation
    /// falls back to [`Self::compress`] and copies into `out` when it
    /// fits.
    ///
    /// # Errors
    ///
    /// Returns [`AccelError::CompressionError`] if compression fails or
    /// `out` is too small.
    fn compress_into(&self, data: &[u8], level: u32, out: &mut [u8]) -> Result<usize> {
        let compressed = self.compress(data, level)?;
        if compressed.len() > out.len() {
            return Err(AccelError::CompressionError {
                reason: "output buffer too small for compressed data".into(),
            });
        }
        out[..compressed.len()].copy_from_slice(&compressed);
        Ok(compressed.len())
    }

    /// Worst-case compressed output size for `input_len` input bytes.
    ///
    /// Used to size reusable scratch buffers (the write path allocates
    /// one per PUT and reuses it across chunks). The default is a
    /// conservative over-estimate; zstd overrides with its exact bound.
    fn worst_case_bound(&self, input_len: usize) -> usize {
        input_len + input_len / 16 + 64
    }

    /// Returns the compression tier this backend implements.
    fn compression_tier(&self) -> CompressionTier;

    /// Returns `true` if this backend is currently available on this hardware.
    ///
    /// Tier 0 (CpuZstd) is always available. Tier 1 (CpuIgzip) requires
    /// AVX-512. Tier 2 (GpuNvcomp) requires CUDA + nvCOMP library.
    fn is_available(&self) -> bool {
        true
    }
}

/// A zstd-based compression backend (Tier 0, always available).
///
/// Uses the `zstd` crate for DEFLATE-compatible compression. This is the
/// terminal fallback in the compression chain — if this backend fails,
/// compression is not possible.
///
/// # Examples
///
/// ```
/// use oceanfs_accel::{Compressor, ZstdCompressor};
///
/// let compressor = ZstdCompressor::new(3);
/// let original = b"test data for zstd compression";
/// let compressed = compressor.compress(original, 3).unwrap();
/// // Zstd typically achieves good compression ratios
/// assert!(compressed.len() <= original.len() + 20); // small overhead
/// let decompressed = compressor.decompress(&compressed).unwrap();
/// assert_eq!(&decompressed[..], original);
/// ```
#[derive(Debug, Clone)]
pub struct ZstdCompressor {
    /// Default compression level (0-22).
    level: u32,
}

impl ZstdCompressor {
    /// Creates a new zstd compressor with the given default compression level.
    ///
    /// The level is a hint — callers may override it per call via the `level`
    /// parameter in [`Compressor::compress`].
    ///
    /// Level range: 0 (fastest, least compression) to 22 (slowest, most
    /// compression). Level 3 is the `zstd` crate default.
    pub fn new(level: u32) -> Self {
        Self { level }
    }

    /// Returns the default compression level this instance was created with.
    pub fn default_level(&self) -> u32 {
        self.level
    }
}

impl Default for ZstdCompressor {
    fn default() -> Self {
        Self::new(3)
    }
}

impl Compressor for ZstdCompressor {
    fn compress(&self, data: &[u8], level: u32) -> Result<Bytes> {
        let effective_level = if level == 0 { self.level } else { level };
        // Precompute zstd's worst-case bound and compress into a single
        // allocation (encode_all would grow the buffer dynamically).
        let bound = zstd::zstd_safe::compress_bound(data.len());
        let mut buf = vec![0u8; bound];
        let written = zstd::bulk::compress_to_buffer(data, &mut buf, effective_level as i32)
            .map_err(|e| AccelError::CompressionError {
                reason: format!("zstd compress failed: {e}"),
            })?;
        let mut out = Bytes::from(buf);
        out.truncate(written);
        Ok(out)
    }

    fn compress_into(&self, data: &[u8], level: u32, out: &mut [u8]) -> Result<usize> {
        let effective_level = if level == 0 { self.level } else { level };
        zstd::bulk::compress_to_buffer(data, out, effective_level as i32).map_err(|e| {
            AccelError::CompressionError { reason: format!("zstd compress failed: {e}") }
        })
    }

    fn worst_case_bound(&self, input_len: usize) -> usize {
        zstd::zstd_safe::compress_bound(input_len)
    }

    fn decompress(&self, data: &[u8]) -> Result<Bytes> {
        zstd::decode_all(data).map(Bytes::from).map_err(|e| AccelError::CompressionError {
            reason: format!("zstd decompress failed: {e}"),
        })
    }

    fn decompress_exact(&self, data: &[u8], expected_len: usize) -> Result<Bytes> {
        // Exact-size destination: one allocation, no reallocations.
        let mut buf = vec![0u8; expected_len];
        let written = zstd::bulk::decompress_to_buffer(data, &mut buf).map_err(|e| {
            AccelError::CompressionError { reason: format!("zstd decompress failed: {e}") }
        })?;
        debug_assert!(written == expected_len, "zstd output size mismatch");
        let mut out = Bytes::from(buf);
        out.truncate(written);
        Ok(out)
    }

    fn compression_tier(&self) -> CompressionTier {
        CompressionTier::CpuZstd
    }

    fn is_available(&self) -> bool {
        true // Always available on all platforms
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // -- ZstdCompressor --

    #[test]
    fn zstd_compress_decompress_roundtrip() {
        let compressor = ZstdCompressor::new(3);
        let original = b"hello world, this is a test of zstd compression round-trip";
        let compressed = compressor.compress(original, 3).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], original);
    }

    #[test]
    fn zstd_compress_empty_data() {
        let compressor = ZstdCompressor::default();
        let original: &[u8] = &[];
        let compressed = compressor.compress(original, 3).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn zstd_compress_large_data() {
        let compressor = ZstdCompressor::default();
        let original = vec![0xABu8; 65536]; // 64 KB
        let compressed = compressor.compress(&original, 3).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], &original[..]);
    }

    #[test]
    fn zstd_compression_tier_is_cpu_zstd() {
        let compressor = ZstdCompressor::default();
        assert_eq!(compressor.compression_tier(), CompressionTier::CpuZstd);
    }

    #[test]
    fn zstd_is_always_available() {
        let compressor = ZstdCompressor::default();
        assert!(compressor.is_available());
    }

    #[test]
    fn zstd_default_level_is_3() {
        let compressor = ZstdCompressor::default();
        assert_eq!(compressor.default_level(), 3);
    }

    #[test]
    fn zstd_higher_level_produces_smaller_output() {
        let compressor = ZstdCompressor::new(3);
        let data = vec![0u8; 65536]; // Highly compressible

        let compressed_low = compressor.compress(&data, 1).unwrap();
        let compressed_high = compressor.compress(&data, 15).unwrap();

        // Higher level should produce smaller or equal output
        assert!(compressed_high.len() <= compressed_low.len());
    }

    #[test]
    fn zstd_decompress_corrupt_data_returns_error() {
        let compressor = ZstdCompressor::default();
        let corrupt = b"this is not valid zstd data!";
        let result = compressor.decompress(corrupt);
        assert!(result.is_err());
    }
}
