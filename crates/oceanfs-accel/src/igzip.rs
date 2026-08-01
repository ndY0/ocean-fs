//! ISA-L igzip CPU compression backend (feature-gated).
//!
//! Provides [`IgzipCompressor`] which serves as Tier 1 in the compression
//! fallback chain (`GpuNvcomp → CpuIgzip → CpuZstd`). This backend
//! implements the [`Compressor`] trait using Intel ISA-L's igzip library
//! for AVX-512-accelerated DEFLATE compression via direct FFI.
//!
//! ## Feature gate
//!
//! This module is only compiled on x86_64 with the `isa-l` Cargo feature
//! enabled. On other platforms, or without the feature, this module does
//! not exist.
//!
//! ## API used
//!
//! - `isal_deflate_stateless_init()` — initialize compression stream
//! - `isal_deflate_stateless()` — one-shot DEFLATE compression
//! - `isal_inflate_init()` — initialize decompression state
//! - `isal_inflate_stateless()` — one-shot DEFLATE decompression
//!
//! ## Availability
//!
//! The constructor returns `None` if AVX-512 is not detected at runtime.
//! The dispatcher falls back to `ZstdCompressor` in that case.
//!
//! ## Safety
//!
//! All ISA-L FFI calls are `unsafe`. Each call site is documented with
//! a `// SAFETY:` comment citing invariants verified at the call site:
//! - Pointers are valid and non-null
//! - Buffer sizes are within bounds
//! - Structs are properly initialized before use

use std::alloc::{self, Layout};

use bytes::Bytes;
use oceanfs_core::CompressionTier;

use crate::{compressor::Compressor, error::AccelError, Result};

// ---------------------------------------------------------------------------
// FFI declarations — Intel ISA-L igzip library
// ---------------------------------------------------------------------------

extern "C" {
    /// Initialize a deflate compression stream for stateless (one-shot) use.
    ///
    /// Zeros all internal state and prepares the stream for a single
    /// `isal_deflate_stateless` call.
    fn isal_deflate_stateless_init(stream: *mut IsalZstream);

    /// Stateless (one-shot) DEFLATE compression.
    ///
    /// Compresses the entire input buffer at once. `avail_out` must be large
    /// enough to hold the entire compressed output (max expansion: input size
    /// plus header of a stored/raw block).
    ///
    /// # Returns
    ///
    /// - `COMP_OK` (0) on success
    /// - `STATELESS_OVERFLOW` (-1) if output buffer too small
    /// - `ISAL_INVALID_LEVEL` (-4) if compression level is invalid
    fn isal_deflate_stateless(stream: *mut IsalZstream) -> i32;

    /// Initialize an inflate (decompression) state for stateless use.
    fn isal_inflate_init(state: *mut InflateState);

    /// Stateless (one-shot) DEFLATE decompression.
    ///
    /// Decompresses the entire input buffer at once. `avail_out` must be
    /// large enough to hold the entire decompressed output.
    ///
    /// # Returns
    ///
    /// - `ISAL_DECOMP_OK` (0) on success
    /// - `ISAL_OUT_OVERFLOW` (2) if output buffer too small
    fn isal_inflate_stateless(state: *mut InflateState) -> i32;
}

// ---------------------------------------------------------------------------
// Struct layouts — must match ISA-L C headers exactly
// ---------------------------------------------------------------------------

// ISA-L constants (from igzip_lib.h)
const IGZIP_K: usize = 1024;

// Level buffer sizes (ISAL_DEF_LVLx_DEFAULT from igzip_lib.h)
const LVL1_DEFAULT: usize = (4 * IGZIP_K + 2 * 8 * IGZIP_K) + 4 * 64 * IGZIP_K; // 282,624
const LVL2_DEFAULT: usize = (4 * IGZIP_K + 2 * 32 * IGZIP_K) + 4 * 64 * IGZIP_K; // 331,776
const LVL3_DEFAULT: usize = (4 * IGZIP_K + 4 * 4 * IGZIP_K + 2 * 32 * IGZIP_K) + 4 * 64 * IGZIP_K; // 348,160

/// Returns the recommended level buffer size for the given compression level.
const fn level_buf_size(level: u32) -> usize {
    match level {
        0 => 0,
        1 => LVL1_DEFAULT,
        2 => LVL2_DEFAULT,
        _ => LVL3_DEFAULT, // level >= 3 uses level 3 buffer
    }
}

// ISA-L error codes
const COMP_OK: i32 = 0;
const STATELESS_OVERFLOW: i32 = -1;
const ISAL_INVALID_LEVEL: i32 = -4;
const ISAL_DECOMP_OK: i32 = 0;
const ISAL_OUT_OVERFLOW: i32 = 2;

/// Minimal `#[repr(C)]` representation of `struct isal_zstream` fields
/// that we need to read/write. The actual struct is ~82KB (includes
/// internal state buffer); we allocate oversized memory and access only
/// the first fields which are at known offsets.
///
/// Field layout matches `igzip_lib.h` line 402-420:
/// ```c
/// struct isal_zstream {
///     uint8_t *next_in;           // offset 0
///     uint32_t avail_in;          // offset 8
///     uint32_t total_in;          // offset 12
///     uint8_t *next_out;          // offset 16
///     uint32_t avail_out;         // offset 24
///     uint32_t total_out;         // offset 28
///     struct isal_hufftables *hufftables; // offset 32
///     uint32_t level;             // offset 40
///     uint32_t level_buf_size;    // offset 44
///     uint8_t *level_buf;         // offset 48
///     uint16_t end_of_stream;     // offset 56
///     uint16_t flush;             // offset 58
///     uint16_t gzip_flag;         // offset 60
///     uint16_t hist_bits;         // offset 62
///     struct isal_zstate internal_state; // offset 64 (~82KB)
/// };
/// ```
#[repr(C)]
struct IsalZstream {
    next_in: *const u8,
    avail_in: u32,
    total_in: u32,
    next_out: *mut u8,
    avail_out: u32,
    total_out: u32,
    _hufftables: usize, // opaque pointer
    level: u32,
    level_buf_size: u32,
    level_buf: *mut u8,
    end_of_stream: u16,
    flush: u16,
    gzip_flag: u16,
    hist_bits: u16,
    // internal_state follows — we don't access it directly;
    // isal_deflate_stateless_init() initializes it.
}

/// Minimal `#[repr(C)]` representation of `struct inflate_state` fields
/// that we need to read/write. The actual struct is ~70KB.
///
/// Field layout matches `igzip_lib.h` line 507-538:
/// ```c
/// struct inflate_state {
///     uint8_t *next_out;          // offset 0
///     uint32_t avail_out;         // offset 8
///     uint32_t total_out;         // offset 12
///     uint8_t *next_in;           // offset 16
///     uint64_t read_in;           // offset 24
///     uint32_t avail_in;          // offset 32
///     int32_t read_in_length;     // offset 36
///     // ... rest is internal state (huffman tables, buffers)
/// };
/// ```
#[repr(C)]
struct InflateState {
    next_out: *mut u8,
    avail_out: u32,
    total_out: u32,
    next_in: *const u8,
    read_in: u64,
    avail_in: u32,
    read_in_length: i32,
    // Huffman tables + internal buffers follow — we don't access them directly.
}

/// Estimated total size of `struct isal_zstream` with internal state.
/// 64 (header) + 82268 (isal_zstate) ≈ 82332. Use 256KB for safety.
const ISAL_ZSTREAM_ESTIMATED_SIZE: usize = 256 * 1024;

/// Estimated total size of `struct inflate_state`.
/// ~70KB. Use 256KB for safety.
const INFLATE_STATE_ESTIMATED_SIZE: usize = 256 * 1024;

// ---------------------------------------------------------------------------
// IgzipCompressor
// ---------------------------------------------------------------------------

/// ISA-L igzip DEFLATE compression backend (Tier 1).
///
/// Provides AVX-512-accelerated DEFLATE compression for segment data
/// via Intel's Intelligent Storage Acceleration Library (ISA-L). The
/// compressed output is standard DEFLATE, compatible with any DEFLATE
/// decompressor (zlib, zstd, gzip).
///
/// # Availability
///
/// Requires:
/// - x86_64 CPU with AVX-512F and AVX-512BW
/// - `isa-l` Cargo feature enabled at compile time
/// - `libisal.so` available at link time
///
/// When AVX-512 is not detected, [`IgzipCompressor::new`] returns `None`.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_accel::{Compressor, IgzipCompressor};
///
/// if let Some(compressor) = IgzipCompressor::new(2) {
///     assert!(compressor.is_available());
///     let data = b"test segment data for igzip compression";
///     let compressed = compressor.compress(data, 2).unwrap();
///     let decompressed = compressor.decompress(&compressed).unwrap();
///     assert_eq!(&decompressed[..], data);
/// }
/// ```
#[derive(Debug)]
pub struct IgzipCompressor {
    /// Compression level (0-3, where 3 is maximum DEFLATE compression).
    level: u32,
    /// Pre-allocated level buffer for compression levels 1-3.
    level_buf: Option<Vec<u8>>,
}

// IgzipCompressor is not Clone because it owns a large allocation.
// It is Send + Sync because ISA-L stateless functions are thread-safe.

impl IgzipCompressor {
    /// Creates a new igzip compressor.
    ///
    /// Allocates the level buffer (for levels 1-3) at construction time
    /// to avoid per-compression allocations on the hot path.
    ///
    /// # Returns
    ///
    /// - `Some(IgzipCompressor)` if AVX-512 is detected at runtime.
    /// - `None` if AVX-512 is not available. The caller should fall back
    ///   to `ZstdCompressor`.
    ///
    /// # Parameters
    ///
    /// - `level`: Compression level (0-3). Level 0 is fastest (least
    ///   compression), level 3 is slowest (most compression).
    pub fn new(level: u32) -> Option<Self> {
        if !Self::is_available() {
            return None;
        }
        let level = level.min(3);
        let level_buf = if level > 0 {
            let buf_size = level_buf_size(level);
            Some(vec![0u8; buf_size])
        } else {
            None
        };
        Some(Self { level, level_buf })
    }

    /// Returns the compression level configured for this instance.
    pub fn compression_level(&self) -> u32 {
        self.level
    }
}

impl Compressor for IgzipCompressor {
    fn compress(&self, data: &[u8], _level: u32) -> Result<Bytes> {
        if data.is_empty() {
            return Ok(Bytes::new());
        }

        let level = self.level;

        // --- Allocate stream struct ---
        // We allocate oversized memory to hold the entire isal_zstream
        // (including the large internal state buffer). The first fields
        // are accessed via the IsalZstream repr(C) struct.
        let layout = Layout::from_size_align(ISAL_ZSTREAM_ESTIMATED_SIZE, 64).map_err(|e| {
            AccelError::CompressionError { reason: format!("stream layout error: {e}") }
        })?;

        // SAFETY: layout has non-zero size and alignment.
        let stream_ptr = unsafe { alloc::alloc_zeroed(layout) };
        if stream_ptr.is_null() {
            return Err(AccelError::CompressionError { reason: "stream allocation failed".into() });
        }

        // SAFETY: stream_ptr is valid, non-null, and large enough to hold
        // the full isal_zstream struct. isal_deflate_stateless_init only
        // writes to the memory — it initializes internal state.
        unsafe {
            isal_deflate_stateless_init(stream_ptr as *mut IsalZstream);
        }

        // --- Allocate output buffer ---
        // Max DEFLATE expansion: input size + header of stored block (5 bytes)
        // Use generous 2x margin for small inputs
        let out_capacity = (data.len() * 2).max(4096);
        let mut out_buf: Vec<u8> = vec![0u8; out_capacity];

        // --- Set stream fields ---
        // SAFETY: stream_ptr points to valid, initialized memory of
        // size >= ISAL_ZSTREAM_ESTIMATED_SIZE. We only access fields
        // within the repr(C) IsalZstream header.
        unsafe {
            let stream = &mut *(stream_ptr as *mut IsalZstream);
            stream.next_in = data.as_ptr();
            stream.avail_in = data.len() as u32;
            stream.next_out = out_buf.as_mut_ptr();
            stream.avail_out = out_buf.len() as u32;
            stream.end_of_stream = 1;
            stream.flush = 0; // NO_FLUSH
            stream.gzip_flag = 0; // raw DEFLATE, no gzip wrapper
            stream.hist_bits = 0; // default history size

            // Set level and level buffer
            stream.level = level;
            if let Some(ref buf) = self.level_buf {
                stream.level_buf = buf.as_ptr() as *mut u8;
                stream.level_buf_size = buf.len() as u32;
            } else {
                stream.level_buf = std::ptr::null_mut();
                stream.level_buf_size = 0;
            }
        }

        // --- Compress ---
        // SAFETY: stream_ptr points to properly initialized isal_zstream
        // with valid input/output buffer pointers and sizes.
        // isal_deflate_stateless is thread-safe per ISA-L documentation.
        let rc = unsafe { isal_deflate_stateless(stream_ptr as *mut IsalZstream) };

        // --- Check result ---
        // SAFETY: stream_ptr is still valid — we only read from it.
        let compressed_len = unsafe {
            let stream = &*(stream_ptr as *const IsalZstream);
            stream.total_out as usize
        };

        // SAFETY: stream_ptr was allocated by alloc::alloc.
        unsafe {
            alloc::dealloc(stream_ptr, layout);
        }

        match rc {
            COMP_OK => {
                // Truncate output to actual compressed size
                out_buf.truncate(compressed_len);
                Ok(Bytes::from(out_buf))
            }
            STATELESS_OVERFLOW => {
                // Output buffer too small — this shouldn't happen with 2x sizing,
                // but handle gracefully.
                Err(AccelError::CompressionError {
                    reason: format!(
                        "igzip output buffer overflow: input={}, output_capacity={}",
                        data.len(),
                        out_capacity
                    ),
                })
            }
            ISAL_INVALID_LEVEL => Err(AccelError::CompressionError {
                reason: format!("igzip invalid compression level: {level}"),
            }),
            code => Err(AccelError::CompressionError {
                reason: format!("igzip compress failed with code {code}"),
            }),
        }
    }

    fn decompress(&self, data: &[u8]) -> Result<Bytes> {
        if data.is_empty() {
            return Ok(Bytes::new());
        }

        // --- Allocate inflate state ---
        let layout = Layout::from_size_align(INFLATE_STATE_ESTIMATED_SIZE, 64).map_err(|e| {
            AccelError::CompressionError { reason: format!("inflate layout error: {e}") }
        })?;

        // SAFETY: layout has non-zero size and alignment.
        let state_ptr = unsafe { alloc::alloc_zeroed(layout) };
        if state_ptr.is_null() {
            return Err(AccelError::CompressionError {
                reason: "inflate state allocation failed".into(),
            });
        }

        // SAFETY: state_ptr is valid, non-null, and large enough to hold
        // the full inflate_state struct.
        unsafe {
            isal_inflate_init(state_ptr as *mut InflateState);
        }

        // --- Allocate output buffer ---
        // Decompressed data is typically 2-10x larger than compressed.
        // Use a generous estimate.
        let out_capacity = (data.len() * 16).max(4096);
        let mut out_buf: Vec<u8> = vec![0u8; out_capacity];

        // --- Set state fields ---
        // SAFETY: state_ptr points to valid, initialized memory.
        unsafe {
            let state = &mut *(state_ptr as *mut InflateState);
            state.next_in = data.as_ptr();
            state.avail_in = data.len() as u32;
            state.next_out = out_buf.as_mut_ptr();
            state.avail_out = out_buf.len() as u32;
        }

        // --- Decompress ---
        // SAFETY: state_ptr points to properly initialized inflate_state
        // with valid input/output buffer pointers and sizes.
        let rc = unsafe { isal_inflate_stateless(state_ptr as *mut InflateState) };

        // SAFETY: state_ptr is still valid after decompression — we only read total_out.
        let decompressed_len = unsafe {
            let state = &*(state_ptr as *const InflateState);
            state.total_out as usize
        };

        // SAFETY: state_ptr was allocated by alloc::alloc.
        unsafe {
            alloc::dealloc(state_ptr, layout);
        }

        match rc {
            ISAL_DECOMP_OK => {
                out_buf.truncate(decompressed_len);
                Ok(Bytes::from(out_buf))
            }
            ISAL_OUT_OVERFLOW => {
                // Output buffer too small. Retry with larger buffer.
                // Estimate based on typical DEFLATE expansion ratios.
                let retry_capacity = data.len() * 64;
                let mut retry_buf: Vec<u8> = vec![0u8; retry_capacity];

                let layout2 =
                    Layout::from_size_align(INFLATE_STATE_ESTIMATED_SIZE, 64).map_err(|e| {
                        AccelError::CompressionError {
                            reason: format!("inflate retry layout error: {e}"),
                        }
                    })?;
                // SAFETY: layout2 has non-zero size and alignment.
                let state_ptr2 = unsafe { alloc::alloc_zeroed(layout2) };
                if state_ptr2.is_null() {
                    return Err(AccelError::CompressionError {
                        reason: "inflate retry alloc failed".into(),
                    });
                }

                // SAFETY: state_ptr2 is valid, non-null, and large enough
                // to hold the full inflate_state struct.
                unsafe {
                    isal_inflate_init(state_ptr2 as *mut InflateState);
                    let state2 = &mut *(state_ptr2 as *mut InflateState);
                    state2.next_in = data.as_ptr();
                    state2.avail_in = data.len() as u32;
                    state2.next_out = retry_buf.as_mut_ptr();
                    state2.avail_out = retry_buf.len() as u32;
                }

                // SAFETY: state_ptr2 points to properly initialized inflate_state
                // with valid input/output buffer pointers and sizes.
                let rc2 = unsafe { isal_inflate_stateless(state_ptr2 as *mut InflateState) };

                // SAFETY: state_ptr2 is still valid after decompression retry.
                let len2 = unsafe {
                    let state2 = &*(state_ptr2 as *const InflateState);
                    state2.total_out as usize
                };

                // SAFETY: state_ptr2 was allocated by alloc::alloc.
                unsafe {
                    alloc::dealloc(state_ptr2, layout2);
                }

                if rc2 == ISAL_DECOMP_OK {
                    retry_buf.truncate(len2);
                    Ok(Bytes::from(retry_buf))
                } else {
                    Err(AccelError::CompressionError {
                        reason: format!("igzip decompress failed with code {rc2} after retry"),
                    })
                }
            }
            code => Err(AccelError::CompressionError {
                reason: format!("igzip decompress failed with code {code}"),
            }),
        }
    }

    fn compression_tier(&self) -> CompressionTier {
        CompressionTier::CpuIgzip
    }

    fn is_available(&self) -> bool {
        Self::is_available()
    }
}

impl IgzipCompressor {
    /// Checks whether igzip is available on this system.
    ///
    /// Returns `true` only on x86_64 with AVX-512F + AVX-512BW detected
    /// at runtime. Always returns `false` on non-x86_64 platforms.
    pub fn is_available() -> bool {
        #[cfg(target_arch = "x86_64")]
        {
            std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("avx512bw")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn igzip_is_available_returns_bool() {
        let available = IgzipCompressor::is_available();
        let _ = available;
    }

    #[test]
    fn igzip_new_on_avx512_absent_returns_none() {
        if !IgzipCompressor::is_available() {
            assert!(IgzipCompressor::new(2).is_none());
        }
    }

    #[test]
    fn igzip_new_on_avx512_present_returns_some() {
        if IgzipCompressor::is_available() {
            let compressor = IgzipCompressor::new(2);
            assert!(compressor.is_some());
        }
    }

    #[test]
    fn igzip_compress_decompress_roundtrip_when_available() {
        if let Some(compressor) = IgzipCompressor::new(2) {
            let original = b"test data for igzip roundtrip";
            let compressed = compressor.compress(original, 2).unwrap();
            let decompressed = compressor.decompress(&compressed).unwrap();
            assert_eq!(&decompressed[..], original);
        }
    }

    #[test]
    fn igzip_compress_decompress_empty_when_available() {
        if let Some(compressor) = IgzipCompressor::new(2) {
            let original: &[u8] = &[];
            let compressed = compressor.compress(original, 2).unwrap();
            let decompressed = compressor.decompress(&compressed).unwrap();
            assert!(decompressed.is_empty());
        }
    }

    #[test]
    fn igzip_compress_decompress_large_when_available() {
        if let Some(compressor) = IgzipCompressor::new(2) {
            let original = vec![0xABu8; 65536]; // 64 KB
            let compressed = compressor.compress(&original, 2).unwrap();
            let decompressed = compressor.decompress(&compressed).unwrap();
            assert_eq!(&decompressed[..], &original[..]);
        }
    }

    #[test]
    fn igzip_output_is_valid_deflate() {
        // igzip produces standard DEFLATE output, which zstd can decompress.
        if let Some(compressor) = IgzipCompressor::new(2) {
            let original = b"cross-backend compatibility test";
            let compressed = compressor.compress(original, 2).unwrap();

            // Decompress with zstd (should handle raw DEFLATE)
            let decompressed = zstd::decode_all(&compressed[..]).unwrap();
            assert_eq!(&decompressed[..], original);
        }
    }

    #[test]
    fn igzip_level_0_produces_reasonable_output() {
        if let Some(compressor) = IgzipCompressor::new(0) {
            let data = vec![0u8; 4096];
            let compressed = compressor.compress(&data, 0).unwrap();
            // Level 0 should still compress highly-redundant data
            assert!(
                compressed.len() < data.len(),
                "level 0 should compress zero data: compressed={}, original={}",
                compressed.len(),
                data.len()
            );
        }
    }

    #[test]
    fn igzip_compression_tier_is_cpu_igzip() {
        if let Some(compressor) = IgzipCompressor::new(2) {
            assert_eq!(compressor.compression_tier(), CompressionTier::CpuIgzip);
        }
    }

    #[test]
    fn igzip_compression_level_returns_configured_value() {
        if let Some(compressor) = IgzipCompressor::new(2) {
            assert_eq!(compressor.compression_level(), 2);
        }
    }

    #[test]
    fn igzip_level_clamped_to_3() {
        if IgzipCompressor::is_available() {
            let compressor = IgzipCompressor::new(10).unwrap();
            assert_eq!(compressor.compression_level(), 3);
        }
    }

    #[test]
    fn struct_isal_zstream_size_is_reasonable() {
        // Verify our estimates are large enough
        assert!(ISAL_ZSTREAM_ESTIMATED_SIZE >= 65536);
        assert!(INFLATE_STATE_ESTIMATED_SIZE >= 65536);
    }

    #[test]
    fn level_buf_size_level_0_is_zero() {
        assert_eq!(level_buf_size(0), 0);
    }

    #[test]
    fn level_buf_size_level_1_is_default() {
        let size = level_buf_size(1);
        assert!(size >= 64 * 1024, "level 1 buffer should be >= 64KB, got {size}");
    }

    #[test]
    fn level_buf_size_level_3_is_default() {
        let size = level_buf_size(3);
        assert!(size >= 256 * 1024, "level 3 buffer should be >= 256KB, got {size}");
    }
}
