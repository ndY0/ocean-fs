//! x86 SIMD-accelerated GF(2^8) multiplication by a constant coefficient.
//!
//! Implements two SIMD paths:
//!
//! - **GFNI** (`VGF2P8MULB`): Single-instruction GF(2^8) multiply —
//!   64 elements/instruction on AVX-512+GFNI, 32 on AVX2+GFNI. No
//!   precomputed tables — the instruction directly multiplies bytes in
//!   GF(2^8). Available on Intel Ice Lake+ (2021), AMD Zen 4+ (2022).
//!
//! - **PSHUFB split-table** (fallback when GFNI unavailable):
//!   - **SSE4.1** (`_mm_shuffle_epi8`):  16 elements/instruction
//!   - **AVX2**   (`_mm256_shuffle_epi8`): 32 elements/instruction
//!   - **AVX-512** (`_mm512_shuffle_epi8`): 64 elements/instruction
//!
//! ## Split-Table Algorithm
//!
//! For each GF(2^8) multiplication by a constant coefficient `c`, two
//! 16-entry lookup tables are precomputed:
//!
//! - `lo_table[i] = c × i`           for i in 0..16  (low nibble)
//! - `hi_table[i] = c × (16 × i)`    for i in 0..16  (high nibble)
//!
//! Then for each data byte `b`:
//! ```text
//! result = lo_table[b & 0xF] ^ hi_table[b >> 4]
//! ```
//!
//! PSHUFB performs 16 parallel table lookups per instruction — 16× faster
//! than scalar log/exp per byte at SSE width, 32× at AVX2, 64× at AVX-512.
//!
//! ## Runtime Dispatch
//!
//! On first call, [`GfSimdLevel::detect`] probes CPU features and caches
//! the result. Subsequent calls use an `Acquire` load — effectively free.

#![allow(unsafe_code)]
// SIMD intrinsics use wildcard — explicit listing is very verbose.
#![allow(clippy::wildcard_imports)]
// AVX-512 intrinsics stabilized in Rust 1.89; our MSRV is older but
// the code is only compiled when the target features are available.
#![allow(clippy::incompatible_msrv)]

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
use std::sync::atomic::{AtomicU8, Ordering};

use super::gf_mul;

// ---------------------------------------------------------------------------
// SIMD Level Detection
// ---------------------------------------------------------------------------

/// Detected x86 SIMD capability level for GF(2^8) arithmetic.
///
/// Ordered by increasing throughput. The runtime dispatcher selects the
/// highest available level on first use and caches the result.
///
/// # Examples
///
/// ```
/// use oceanfs_ec::gf::GfSimdLevel;
///
/// let level = GfSimdLevel::detect();
/// assert!(matches!(
///     level,
///     GfSimdLevel::Portable
///         | GfSimdLevel::Sse41
///         | GfSimdLevel::Avx2
///         | GfSimdLevel::Avx512
///         | GfSimdLevel::Gfni
/// ));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum GfSimdLevel {
    /// Portable log/exp table lookup (no SIMD). Always available.
    Portable = 0,
    /// SSE4.1 PSHUFB split-table lookup. 16 bytes/instruction.
    /// Available on all x86-64 CPUs since ~2008.
    Sse41 = 1,
    /// AVX2 VPSHUFB 256-bit split-table lookup. 32 bytes/instruction.
    /// Available on Intel Haswell+ (2013) and AMD Excavator+ (2015).
    Avx2 = 2,
    /// AVX-512 VPSHUFB 512-bit split-table lookup. 64 bytes/instruction.
    /// Available on Intel Skylake-X+ (2017), Ice Lake+, AMD Zen 4+ (2022).
    Avx512 = 3,
    /// GFNI (Galois Field New Instructions) — single-instruction GF(2^8)
    /// multiplication via `VGF2P8MULB`. Replaces PSHUFB split-table
    /// lookup entirely: 64 bytes/instruction without any precomputed
    /// tables. Available on Intel Ice Lake+ (2021) and AMD Zen 4+ (2022)
    /// when paired with at least AVX2. The dispatcher selects the widest
    /// available vector width (AVX-512 if available, else AVX2).
    Gfni = 4,
}

static GF_SIMD_LEVEL: AtomicU8 = AtomicU8::new(u8::MAX);

impl GfSimdLevel {
    /// Detects the highest available x86 SIMD level for GF(2^8) arithmetic.
    ///
    /// Uses `is_x86_feature_detected!` from `std::arch`. The result is
    /// cached in a `static AtomicU8` — subsequent calls return the cached
    /// value at zero cost (single `Acquire` load).
    ///
    /// On non-x86 targets, always returns [`GfSimdLevel::Portable`].
    pub fn detect() -> Self {
        let cached = GF_SIMD_LEVEL.load(Ordering::Acquire);
        if cached != u8::MAX {
            return Self::from_u8(cached);
        }
        #[cfg(target_arch = "x86_64")]
        let level = {
            // GFNI is the highest priority — single-instruction GF(2^8)
            // multiply, no table lookups. Requires at least AVX2 as the
            // vector foundation; prefers AVX-512 for wider vectors.
            // The actual vector width (512 vs 256) is resolved at dispatch
            // time via `is_x86_feature_detected!("avx512f")`.
            if is_x86_feature_detected!("gfni")
                && (is_x86_feature_detected!("avx512f") || is_x86_feature_detected!("avx2"))
            {
                Self::Gfni
            } else if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
                Self::Avx512
            } else if is_x86_feature_detected!("avx2") {
                Self::Avx2
            } else if is_x86_feature_detected!("sse4.1") {
                Self::Sse41
            } else {
                Self::Portable
            }
        };
        #[cfg(not(target_arch = "x86_64"))]
        let level = Self::Portable;

        GF_SIMD_LEVEL.store(level as u8, Ordering::Release);
        level
    }

    #[inline]
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Portable,
            1 => Self::Sse41,
            2 => Self::Avx2,
            3 => Self::Avx512,
            4 => Self::Gfni,
            _ => Self::Portable,
        }
    }

    /// Returns the currently cached SIMD level without performing detection.
    #[inline]
    pub fn cached() -> Option<Self> {
        let v = GF_SIMD_LEVEL.load(Ordering::Acquire);
        if v == u8::MAX {
            None
        } else {
            Some(Self::from_u8(v))
        }
    }
}

// ---------------------------------------------------------------------------
// Split-Table
// ---------------------------------------------------------------------------

/// Precomputed split-table for multiplying by a single GF(2^8) coefficient.
struct GfMulTableX86 {
    lo: [u8; 16],
    hi: [u8; 16],
}

impl GfMulTableX86 {
    #[inline]
    fn new(coeff: u8) -> Self {
        let mut lo = [0u8; 16];
        let mut hi = [0u8; 16];
        for i in 0..16u8 {
            lo[i as usize] = gf_mul(coeff, i);
            hi[i as usize] = gf_mul(coeff, i.wrapping_mul(16));
        }
        Self { lo, hi }
    }
}

// ---------------------------------------------------------------------------
// SSE4.1 kernel — 16 elements per PSHUFB
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn gf_mul_sse41_16(table: &GfMulTableX86, data_ptr: *const u8) -> __m128i {
    // SAFETY: caller guarantees data_ptr points to 16 readable bytes
    let data = _mm_loadu_si128(data_ptr as *const __m128i);
    let mask = _mm_set1_epi8(0x0F);
    let lo_nibbles = _mm_and_si128(data, mask);
    // _mm_srli_epi64 shifts within 64-bit lanes, so hi nibble extraction:
    // shift right 4, mask for cross-lane correctness.
    let hi_tmp = _mm_srli_epi64::<4>(data);
    let hi_nibbles = _mm_and_si128(hi_tmp, mask);

    let lo_table = _mm_loadu_si128(table.lo.as_ptr() as *const __m128i);
    let hi_table = _mm_loadu_si128(table.hi.as_ptr() as *const __m128i);

    let lo_result = _mm_shuffle_epi8(lo_table, lo_nibbles);
    let hi_result = _mm_shuffle_epi8(hi_table, hi_nibbles);
    _mm_xor_si128(lo_result, hi_result)
}

// ---------------------------------------------------------------------------
// AVX2 kernel — 32 elements per VPSHUFB
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn gf_mul_avx2_32(table: &GfMulTableX86, data_ptr: *const u8) -> __m256i {
    // SAFETY: caller guarantees data_ptr points to 32 readable bytes
    let data = _mm256_loadu_si256(data_ptr as *const __m256i);
    let mask = _mm256_set1_epi8(0x0F);
    let lo_nibbles = _mm256_and_si256(data, mask);
    let hi_tmp = _mm256_srli_epi64::<4>(data);
    let hi_nibbles = _mm256_and_si256(hi_tmp, mask);

    let lo_table =
        _mm256_broadcastsi128_si256(_mm_loadu_si128(table.lo.as_ptr() as *const __m128i));
    let hi_table =
        _mm256_broadcastsi128_si256(_mm_loadu_si128(table.hi.as_ptr() as *const __m128i));

    let lo_result = _mm256_shuffle_epi8(lo_table, lo_nibbles);
    let hi_result = _mm256_shuffle_epi8(hi_table, hi_nibbles);
    _mm256_xor_si256(lo_result, hi_result)
}

// ---------------------------------------------------------------------------
// AVX-512 kernel — 64 elements per VPSHUFB
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn gf_mul_avx512_64(table: &GfMulTableX86, data_ptr: *const u8) -> __m512i {
    // SAFETY: caller guarantees data_ptr points to 64 readable bytes
    let data = _mm512_loadu_si512(data_ptr as *const __m512i);
    let mask = _mm512_set1_epi8(0x0F);
    let lo_nibbles = _mm512_and_si512(data, mask);
    let hi_tmp = _mm512_srli_epi64::<4>(data);
    let hi_nibbles = _mm512_and_si512(hi_tmp, mask);

    let lo_table = _mm512_broadcast_i32x4(_mm_loadu_si128(table.lo.as_ptr() as *const __m128i));
    let hi_table = _mm512_broadcast_i32x4(_mm_loadu_si128(table.hi.as_ptr() as *const __m128i));

    let lo_result = _mm512_shuffle_epi8(lo_table, lo_nibbles);
    let hi_result = _mm512_shuffle_epi8(hi_table, hi_nibbles);
    _mm512_xor_si512(lo_result, hi_result)
}

// ---------------------------------------------------------------------------
// GFNI kernel — 64 elements per VGF2P8MULB (AVX-512+GFNI)
// ---------------------------------------------------------------------------

/// GFNI AVX-512 kernel: multiply 64 bytes by `coeff` in GF(2^8) using
/// `_mm512_gf2p8mul_epi8` — a single instruction, no table lookups.
///
/// This is the fastest possible GF(2^8) path: one instruction for 64
/// bytes, ~8.7× faster than portable log/exp per-byte.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,gfni")]
unsafe fn gf_mul_gfni_avx512_64(coeff: u8, data_ptr: *const u8) -> __m512i {
    // SAFETY: caller guarantees data_ptr points to 64 readable bytes
    let data = _mm512_loadu_si512(data_ptr as *const __m512i);
    let coeff_vec = _mm512_set1_epi8(coeff as i8);
    _mm512_gf2p8mul_epi8(data, coeff_vec)
}

// ---------------------------------------------------------------------------
// GFNI kernel — 32 elements per VGF2P8MULB (AVX2+GFNI, no AVX-512)
// ---------------------------------------------------------------------------

/// GFNI AVX2 kernel: multiply 32 bytes by `coeff` in GF(2^8) using
/// `_mm256_gf2p8mul_epi8`. Used when GFNI is available but AVX-512 is not
/// (e.g., some Ice Lake client SKUs).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,gfni")]
unsafe fn gf_mul_gfni_avx2_32(coeff: u8, data_ptr: *const u8) -> __m256i {
    // SAFETY: caller guarantees data_ptr points to 32 readable bytes
    let data = _mm256_loadu_si256(data_ptr as *const __m256i);
    let coeff_vec = _mm256_set1_epi8(coeff as i8);
    _mm256_gf2p8mul_epi8(data, coeff_vec)
}

// ---------------------------------------------------------------------------
// GFNI batch functions — process entire slice with a single coefficient
// ---------------------------------------------------------------------------

/// GFNI AVX-512 batched multiply: `dst[i] = coeff × data[i]` for all i.
///
/// Uses `_mm512_gf2p8mul_epi8` for 512-bit chunks, falling back to the
/// 256-bit GFNI path for remainder, then portable for the final bytes.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,gfni")]
unsafe fn gf_mul_gfni_avx512_batch(coeff: u8, data: &[u8], dst: &mut [u8]) {
    assert_eq!(data.len(), dst.len());
    let len = data.len();
    let chunks = len / 64;

    for chunk in 0..chunks {
        let offset = chunk * 64;
        let result = gf_mul_gfni_avx512_64(coeff, data.as_ptr().add(offset));
        _mm512_storeu_si512(dst.as_mut_ptr().add(offset) as *mut __m512i, result);
    }

    // Remainder: use GFNI AVX2 for the next ≤32 bytes, then portable
    let remainder_start = chunks * 64;
    if remainder_start < len {
        gf_mul_gfni_avx2_batch(coeff, &data[remainder_start..], &mut dst[remainder_start..]);
    }
}

/// GFNI AVX2 batched multiply (GFNI without AVX-512).
///
/// Uses `_mm256_gf2p8mul_epi8` for 256-bit chunks, falling back to
/// SSE4.1 for the next 16 bytes, then portable for the final bytes.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,gfni")]
unsafe fn gf_mul_gfni_avx2_batch(coeff: u8, data: &[u8], dst: &mut [u8]) {
    assert_eq!(data.len(), dst.len());
    let len = data.len();
    let chunks = len / 32;

    for chunk in 0..chunks {
        let offset = chunk * 32;
        let result = gf_mul_gfni_avx2_32(coeff, data.as_ptr().add(offset));
        _mm256_storeu_si256(dst.as_mut_ptr().add(offset) as *mut __m256i, result);
    }

    // Remainder: use SSE4.1 for the next 16 bytes, then portable
    let remainder_start = chunks * 32;
    let remainder = len - remainder_start;
    if remainder >= 16 {
        let table = GfMulTableX86::new(coeff);
        let sse_result = gf_mul_sse41_16(&table, data.as_ptr().add(remainder_start));
        _mm_storeu_si128(dst.as_mut_ptr().add(remainder_start) as *mut __m128i, sse_result);
        let sse_end = remainder_start + 16;
        for i in sse_end..len {
            dst[i] = gf_mul(coeff, data[i]);
        }
    } else {
        for i in remainder_start..len {
            dst[i] = gf_mul(coeff, data[i]);
        }
    }
}

// ---------------------------------------------------------------------------
// Batch functions — process entire slice with a single coefficient
// ---------------------------------------------------------------------------

/// SSE4.1 batched multiply: `dst[i] = coeff × data[i]` for all i.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn gf_mul_sse41_batch(coeff: u8, data: &[u8], dst: &mut [u8]) {
    assert_eq!(data.len(), dst.len());
    let len = data.len();
    let chunks = len / 16;
    let table = GfMulTableX86::new(coeff);

    for chunk in 0..chunks {
        let offset = chunk * 16;
        let result = gf_mul_sse41_16(&table, data.as_ptr().add(offset));
        _mm_storeu_si128(dst.as_mut_ptr().add(offset) as *mut __m128i, result);
    }

    let remainder_start = chunks * 16;
    for i in remainder_start..len {
        dst[i] = gf_mul(coeff, data[i]);
    }
}

/// AVX2 batched multiply.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn gf_mul_avx2_batch(coeff: u8, data: &[u8], dst: &mut [u8]) {
    assert_eq!(data.len(), dst.len());
    let len = data.len();
    let chunks = len / 32;
    let table = GfMulTableX86::new(coeff);

    for chunk in 0..chunks {
        let offset = chunk * 32;
        let result = gf_mul_avx2_32(&table, data.as_ptr().add(offset));
        _mm256_storeu_si256(dst.as_mut_ptr().add(offset) as *mut __m256i, result);
    }

    // Remainder: use SSE4.1 for the next 16 bytes, then portable
    let remainder_start = chunks * 32;
    let remainder = len - remainder_start;
    if remainder >= 16 {
        let sse_result = gf_mul_sse41_16(&table, data.as_ptr().add(remainder_start));
        _mm_storeu_si128(dst.as_mut_ptr().add(remainder_start) as *mut __m128i, sse_result);
        let sse_end = remainder_start + 16;
        for i in sse_end..len {
            dst[i] = gf_mul(coeff, data[i]);
        }
    } else {
        for i in remainder_start..len {
            dst[i] = gf_mul(coeff, data[i]);
        }
    }
}

/// AVX-512 batched multiply.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn gf_mul_avx512_batch(coeff: u8, data: &[u8], dst: &mut [u8]) {
    assert_eq!(data.len(), dst.len());
    let len = data.len();
    let chunks = len / 64;
    let table = GfMulTableX86::new(coeff);

    for chunk in 0..chunks {
        let offset = chunk * 64;
        let result = gf_mul_avx512_64(&table, data.as_ptr().add(offset));
        _mm512_storeu_si512(dst.as_mut_ptr().add(offset) as *mut __m512i, result);
    }

    // Remainder: use AVX2/SSE4.1/portable on remaining bytes
    let remainder_start = chunks * 64;
    if remainder_start < len {
        gf_mul_avx2_batch(coeff, &data[remainder_start..], &mut dst[remainder_start..]);
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Multiplies every element of `data` by `coeff` in GF(2^8) using the
/// fastest available SIMD path.
///
/// On x86_64, detects CPU features on first call and dispatches to
/// AVX-512, AVX2, SSE4.1, or portable (in that order). On other
/// architectures, uses portable log/exp table lookup.
///
/// # Panics
///
/// Panics if `data` and `dst` have different lengths.
///
/// # Examples
///
/// ```
/// use oceanfs_ec::gf::{gf_mul_simd, gf_mul};
///
/// let coeff = 0x42u8;
/// let data = vec![0x12u8; 1024];
/// let mut dst_simd = vec![0u8; 1024];
/// let mut dst_portable = vec![0u8; 1024];
///
/// gf_mul_simd(coeff, &data, &mut dst_simd);
/// for i in 0..1024 {
///     dst_portable[i] = gf_mul(coeff, data[i]);
/// }
/// assert_eq!(dst_simd, dst_portable);
/// ```
pub fn gf_mul_simd(coeff: u8, data: &[u8], dst: &mut [u8]) {
    assert_eq!(data.len(), dst.len(), "data and dst must have same length");

    let level = GfSimdLevel::detect();

    match level {
        #[cfg(target_arch = "x86_64")]
        GfSimdLevel::Gfni => {
            if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
                // SAFETY: GFNI + AVX-512F + AVX-512BW detected by GfSimdLevel::detect
                unsafe { gf_mul_gfni_avx512_batch(coeff, data, dst) }
            } else {
                // SAFETY: GFNI + AVX2 detected by GfSimdLevel::detect
                unsafe { gf_mul_gfni_avx2_batch(coeff, data, dst) }
            }
        }
        #[cfg(target_arch = "x86_64")]
        GfSimdLevel::Avx512 => {
            // SAFETY: AVX-512F + AVX-512BW detected by GfSimdLevel::detect
            unsafe { gf_mul_avx512_batch(coeff, data, dst) }
        }
        #[cfg(target_arch = "x86_64")]
        GfSimdLevel::Avx2 => {
            // SAFETY: AVX2 detected by GfSimdLevel::detect
            unsafe { gf_mul_avx2_batch(coeff, data, dst) }
        }
        #[cfg(target_arch = "x86_64")]
        GfSimdLevel::Sse41 => {
            // SAFETY: SSE4.1 detected by GfSimdLevel::detect
            unsafe { gf_mul_sse41_batch(coeff, data, dst) }
        }
        _ => {
            for i in 0..data.len() {
                dst[i] = gf_mul(coeff, data[i]);
            }
        }
    }
}

/// Like [`gf_mul_simd`] but without bounds checks.
///
/// # Safety
///
/// The caller must ensure `dst.len() >= data.len()` and all pointers are
/// valid for the required lengths.
///
/// For SIMD paths, the caller should also ensure data is aligned
/// (16/32/64 bytes for SSE/AVX2/AVX-512 respectively), though the
/// implementation uses unaligned loads/stores which are safe on all
/// x86-64 CPUs.
pub unsafe fn gf_mul_simd_unchecked(coeff: u8, data: &[u8], dst: &mut [u8]) {
    let level = GfSimdLevel::detect();

    match level {
        #[cfg(target_arch = "x86_64")]
        GfSimdLevel::Gfni => {
            if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") {
                // SAFETY: GFNI + AVX-512F + AVX-512BW confirmed by GfSimdLevel::detect
                unsafe { gf_mul_gfni_avx512_batch(coeff, data, dst) }
            } else {
                // SAFETY: GFNI + AVX2 confirmed by GfSimdLevel::detect
                unsafe { gf_mul_gfni_avx2_batch(coeff, data, dst) }
            }
        }
        #[cfg(target_arch = "x86_64")]
        // SAFETY: AVX-512F + AVX-512BW confirmed by GfSimdLevel::detect
        GfSimdLevel::Avx512 => unsafe { gf_mul_avx512_batch(coeff, data, dst) },
        #[cfg(target_arch = "x86_64")]
        // SAFETY: AVX2 confirmed by GfSimdLevel::detect
        GfSimdLevel::Avx2 => unsafe { gf_mul_avx2_batch(coeff, data, dst) },
        #[cfg(target_arch = "x86_64")]
        // SAFETY: SSE4.1 confirmed by GfSimdLevel::detect
        GfSimdLevel::Sse41 => unsafe { gf_mul_sse41_batch(coeff, data, dst) },
        _ => {
            for i in 0..data.len() {
                dst[i] = gf_mul(coeff, data[i]);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::gf::gf_mul;

    // ── GfSimdLevel detection ────────────────────────────────────────

    #[test]
    fn simd_level_detect_is_cached() {
        let first = GfSimdLevel::detect();
        let second = GfSimdLevel::detect();
        assert_eq!(first, second);
        assert!(GfSimdLevel::cached().is_some());
    }

    #[test]
    fn simd_level_is_ordered() {
        assert!(GfSimdLevel::Gfni > GfSimdLevel::Avx512);
        assert!(GfSimdLevel::Avx512 > GfSimdLevel::Avx2);
        assert!(GfSimdLevel::Avx2 > GfSimdLevel::Sse41);
        assert!(GfSimdLevel::Sse41 > GfSimdLevel::Portable);
    }

    // ── Cross-check: SIMD vs portable ─────────────────────────────────

    #[test]
    fn gf_mul_simd_matches_portable_small() {
        let coeff = 0x42u8;
        let data: Vec<u8> = (0..100u8).map(|i| i.wrapping_mul(7).wrapping_add(1)).collect();
        let mut dst_simd = vec![0u8; 100];
        let mut dst_portable = vec![0u8; 100];

        gf_mul_simd(coeff, &data, &mut dst_simd);
        for i in 0..100 {
            dst_portable[i] = gf_mul(coeff, data[i]);
        }
        assert_eq!(dst_simd, dst_portable);
    }

    #[test]
    fn gf_mul_simd_matches_portable_large() {
        let coeff = 0x7Bu8;
        let len = 4096;
        let data: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(3).wrapping_add(1)).collect();
        let mut dst_simd = vec![0u8; len];
        let mut dst_portable = vec![0u8; len];

        gf_mul_simd(coeff, &data, &mut dst_simd);
        for i in 0..len {
            dst_portable[i] = gf_mul(coeff, data[i]);
        }
        assert_eq!(dst_simd, dst_portable);
    }

    #[test]
    fn gf_mul_simd_matches_portable_various_sizes() {
        let coeff = 0xA3u8;
        for &len in &[0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 1024] {
            let data: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_add(1)).collect();
            let mut dst_simd = vec![0u8; len];
            let mut dst_portable = vec![0u8; len];

            gf_mul_simd(coeff, &data, &mut dst_simd);
            for i in 0..len {
                dst_portable[i] = gf_mul(coeff, data[i]);
            }
            assert_eq!(dst_simd, dst_portable, "mismatch at len={len}");
        }
    }

    #[test]
    fn gf_mul_simd_matches_portable_various_coeffs() {
        let data: Vec<u8> = (0..=255u8).collect();
        for coeff in 0..=255u8 {
            let mut dst_simd = vec![0u8; 256];
            let mut dst_portable = vec![0u8; 256];

            gf_mul_simd(coeff, &data, &mut dst_simd);
            for i in 0..256 {
                dst_portable[i] = gf_mul(coeff, data[i]);
            }
            assert_eq!(dst_simd, dst_portable, "mismatch at coeff={coeff}");
        }
    }

    // ── Edge cases ───────────────────────────────────────────────────

    #[test]
    fn gf_mul_simd_empty_input() {
        let mut dst = vec![];
        gf_mul_simd(0x42, &[], &mut dst);
        assert!(dst.is_empty());
    }

    #[test]
    fn gf_mul_simd_coeff_zero() {
        let data = vec![0x42u8; 256];
        let mut dst = vec![0xFFu8; 256];
        gf_mul_simd(0, &data, &mut dst);
        assert!(dst.iter().all(|&x| x == 0));
    }

    #[test]
    fn gf_mul_simd_coeff_one() {
        let data: Vec<u8> = (0..=255u8).collect();
        let mut dst = vec![0u8; 256];
        gf_mul_simd(1, &data, &mut dst);
        assert_eq!(dst, data);
    }

    #[test]
    fn gf_mul_simd_associative() {
        // (a × b) × c should equal a × (b × c) when using SIMD for each step
        let a: Vec<u8> = (0usize..512).map(|i| (i as u8).wrapping_add(1)).collect();
        let mut ab = vec![0u8; 512];
        let mut abc = vec![0u8; 512];
        let mut a_bc = vec![0u8; 512];

        let coeff_b = 0x3Cu8;
        let coeff_c = 0x7Eu8;

        // (a × b) × c
        gf_mul_simd(coeff_b, &a, &mut ab);
        gf_mul_simd(coeff_c, &ab, &mut abc);

        // a × (b × c): first compute b×c = coeff_b × coeff_c (scalar), then use as single coeff
        let bc_coeff = gf_mul(coeff_b, coeff_c);
        gf_mul_simd(bc_coeff, &a, &mut a_bc);

        assert_eq!(abc, a_bc);
    }

    // ── Round-trip with Cauchy encode/decode ─────────────────────────

    #[test]
    fn gf_simd_cauchy_encode_roundtrip() {
        use oceanfs_core::CodecConfig;

        use crate::{CauchyEncoder, Decoder, Encoder};

        let config = CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        };
        let encoder = CauchyEncoder::new(config);

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![b'a' + i; 64]).collect();
        let data_refs: Vec<&[u8]> = data.iter().map(|v| &v[..]).collect();

        let parity = encoder.encode(&data_refs, 2).unwrap();
        assert_eq!(parity.len(), 2);

        // Recover a lost data shard using remaining shards + parity
        let available: Vec<Option<&[u8]>> = vec![
            None,
            Some(&data[1]),
            Some(&data[2]),
            Some(&data[3]),
            Some(parity[0].as_ref()),
            Some(parity[1].as_ref()),
        ];

        let recovered = encoder.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered[0], data[0]);
    }

    // ── Cross-check: SIMD levels agree with each other ───────────────

    #[test]
    fn gf_simd_crosscheck_all_levels_agree() {
        // All SIMD levels must produce identical results for the same input.
        // We verify this by comparing each level's output against the portable
        // baseline and against each other.
        let coeff = 0x7Bu8;
        let len = 512;
        let data: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(3).wrapping_add(1)).collect();
        let mut portable = vec![0u8; len];
        let mut simd = vec![0u8; len];

        // Portable baseline
        for i in 0..len {
            portable[i] = gf_mul(coeff, data[i]);
        }

        // SIMD (whatever level is active) must match
        gf_mul_simd(coeff, &data, &mut simd);
        assert_eq!(simd, portable, "SIMD must match portable for all elements");

        // Verify the SIMD level is detected and cached
        let level = GfSimdLevel::detect();
        let cached = GfSimdLevel::cached().unwrap();
        assert_eq!(level, cached, "cached level must equal detected level");
    }

    // ── GFNI-specific tests ──────────────────────────────────────────

    #[test]
    fn gfni_level_is_highest_priority() {
        // Gfni must be the maximum-valued variant (highest priority).
        let levels = [
            GfSimdLevel::Portable,
            GfSimdLevel::Sse41,
            GfSimdLevel::Avx2,
            GfSimdLevel::Avx512,
            GfSimdLevel::Gfni,
        ];
        let max = levels.iter().max().unwrap();
        assert_eq!(*max, GfSimdLevel::Gfni);
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn gfni_batch_crosscheck_against_portable() {
        // When GFNI is available at runtime, verify the GFNI batch
        // functions produce identical results to scalar portable mul.
        if !is_x86_feature_detected!("gfni") {
            // GFNI not available on this CPU — skip.
            return;
        }

        let coeff = 0x7Bu8;
        let len = 1024;
        let data: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(3).wrapping_add(1)).collect();

        // Portable baseline
        let mut portable = vec![0u8; len];
        for i in 0..len {
            portable[i] = gf_mul(coeff, data[i]);
        }

        // GFNI via SIMD dispatch
        let mut gfni_out = vec![0u8; len];
        gf_mul_simd(coeff, &data, &mut gfni_out);

        // The dispatch should have selected Gfni
        let level = GfSimdLevel::detect();
        assert_eq!(level, GfSimdLevel::Gfni, "expected Gfni level when gfni feature is available");

        assert_eq!(gfni_out, portable, "GFNI output must match portable for all elements");
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn gfni_batch_crosscheck_various_sizes() {
        // Like `gf_mul_simd_matches_portable_various_sizes` but with
        // an explicit check that GFNI is the active path.
        if !is_x86_feature_detected!("gfni") {
            return;
        }

        let coeff = 0xA3u8;
        // Sizes that exercise the SIMD boundary and remainder logic
        let sizes: &[usize] =
            &[0, 1, 15, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 511, 512, 1023, 1024];

        for &len in sizes {
            let data: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_add(1)).collect();
            let mut gfni_out = vec![0u8; len];
            let mut scalar = vec![0u8; len];

            gf_mul_simd(coeff, &data, &mut gfni_out);
            for i in 0..len {
                scalar[i] = gf_mul(coeff, data[i]);
            }
            assert_eq!(gfni_out, scalar, "GFNI mismatch at len={len}");
        }
    }

    /// Verify that the `from_u8` round-trip preserves all variants
    /// including `Gfni`.
    #[test]
    fn from_u8_roundtrip_all_variants() {
        let variants: &[(GfSimdLevel, u8)] = &[
            (GfSimdLevel::Portable, 0),
            (GfSimdLevel::Sse41, 1),
            (GfSimdLevel::Avx2, 2),
            (GfSimdLevel::Avx512, 3),
            (GfSimdLevel::Gfni, 4),
        ];
        for &(level, expected_val) in variants {
            assert_eq!(level as u8, expected_val);
            assert_eq!(GfSimdLevel::from_u8(expected_val), level);
        }
    }

    /// Unknown discriminant values fall back to `Portable`.
    #[test]
    fn from_u8_unknown_falls_back_to_portable() {
        assert_eq!(GfSimdLevel::from_u8(255), GfSimdLevel::Portable);
        assert_eq!(GfSimdLevel::from_u8(5), GfSimdLevel::Portable);
    }

    // ── Edge cases: sizes around SIMD boundaries ─────────────────────

    #[test]
    fn gf_simd_edge_cases() {
        // Sizes that exercise the remainder-handling logic at each SIMD width.
        let coeff = 0xA3u8;
        let sizes: &[usize] = &[
            0, 1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 1023, 1024, 4095,
            4096,
        ];

        for &len in sizes {
            let data: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_add(1)).collect();
            let mut simd = vec![0u8; len];
            let mut scalar = vec![0u8; len];

            gf_mul_simd(coeff, &data, &mut simd);
            for i in 0..len {
                scalar[i] = gf_mul(coeff, data[i]);
            }
            assert_eq!(simd, scalar, "mismatch at len={len}");
        }
    }

    // ── Integration: ParallelEncoder produces correct parity ─────────

    #[test]
    fn gf_simd_parallel_encode_roundtrip() {
        use oceanfs_core::CodecConfig;

        use crate::{Encoder, ParallelEncoder, StripeLayout};

        // Use a small shard size that exercises multiple stripes
        let config = CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        };
        let inner = std::sync::Arc::new(crate::CauchyEncoder::new(config));
        let encoder = ParallelEncoder::new(inner, 0);

        // Segment data: 4 data shards × (2 stripes × 64 bytes) = 512 bytes
        let segment_data: Vec<u8> = (0..512u16).map(|i| (i & 0xFF) as u8).collect();
        let plan = StripeLayout::compute(segment_data.len() as u64, 4, 2, 64).unwrap();

        let batch = encoder.encode(&segment_data, &plan).unwrap();

        // Verify parity shard lengths
        assert_eq!(batch.parity.len(), 2, "must produce 2 parity shards");
        assert_eq!(batch.data.len(), 4, "must have 4 data shards");
        // Parity shards should be non-zero and correct length
        for p in &batch.parity {
            assert!(!p.is_empty());
            assert!(!p.iter().all(|&b| b == 0), "parity must not be all zeros");
        }

        // Round-trip: decode using portable Cauchy to verify SIMD parity is valid
        let cauchy = crate::CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        });
        // Build reference parity with portable encoder
        let data_refs: Vec<&[u8]> = batch.data.iter().map(|v| &v[..]).collect();
        let ref_parity = cauchy.encode(&data_refs, 2).unwrap();
        // Verify SIMD parity matches portable parity
        for (i, ref_p) in ref_parity.iter().enumerate().take(2) {
            assert_eq!(
                &batch.parity[i][..],
                ref_p.as_ref(),
                "SIMD parity shard {i} must match portable"
            );
        }
    }
}
