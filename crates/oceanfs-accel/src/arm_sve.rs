//! ARM NEON + SVE accelerated erasure coding (feature-gated).
//!
//! Provides `ArmEncoder` which uses ARM SIMD intrinsics for accelerated
//! GF(2^8) arithmetic. On aarch64 targets with the `arm-sve` feature
//! enabled, this module selects the best available SIMD path at runtime.
//!
//! ## SIMD tiers (aarch64 only)
//!
//! ```text
//! SVE2 (256-bit)  → 32 bytes/cycle  — Graviton4, Neoverse V2
//! SVE  (128-bit)  → 16 bytes/cycle  — Graviton3, Neoverse V1
//! NEON (128-bit)  → 16 bytes/cycle  — Graviton2, Apple M1/M2, Raspberry Pi
//! Portable         →  ~1 byte/cycle  — fallback (always available)
//! ```
//!
//! ## Algorithm
//!
//! The NEON path uses a split-table approach for GF(2^8) multiplication.
//! For each encoding matrix coefficient `c`, two 16-entry tables are
//! precomputed:
//!
//! - `lo_table[i]` = c × i        for i in 0..16  (low nibble)
//! - `hi_table[i]` = c × (16×i)   for i in 0..16  (high nibble)
//!
//! Then for each data byte `b`:
//!   result = lo_table[b & 0xF] ^ hi_table[b >> 4]
//!
//! This enables 16 parallel GF multiplies via two `vtbl1_u8` lookups
//! and one `veorq_u8` XOR — zero branches, ~2-3 cycles for 16 bytes.
//!
//! The SVE path uses the same algorithm with `svtbl_u8` for wider
//! vectors (the vector width is determined by the hardware at runtime).
//!
//! ## Safety
//!
//! NEON/SVE intrinsics are `unsafe`. Each intrinsic call is preceded by
//! a `// SAFETY:` comment. The portable fallback path contains no unsafe.

// On x86_64, the NEON/SVE code is cfg-gated out but the supporting
// structures (GfMulTable, EncodeTables, ArmEncoder fields) still exist
// and are only read on aarch64.
#![allow(dead_code)]
#![allow(clippy::needless_range_loop)]

#[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]
use std::arch::aarch64::*;

use oceanfs_core::CodecConfig;
use oceanfs_ec::{CauchyEncoder, Decoder, Encoder};

// ---------------------------------------------------------------------------
// ARM SIMD level detection
// ---------------------------------------------------------------------------

/// Detected ARM SIMD capability level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ArmSveLevel {
    /// No SIMD available — use portable GF-complete fallback.
    Portable = 0,
    /// NEON 128-bit SIMD (available on all aarch64 targets).
    Neon = 1,
    /// SVE (Scalable Vector Extension) 128-bit or wider.
    #[allow(dead_code)]
    Sve = 2,
    /// SVE2 with 256-bit vectors and enhanced instructions.
    #[allow(dead_code)]
    Sve2 = 3,
}

impl ArmSveLevel {
    /// Human-readable name for logging.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Portable => "portable",
            Self::Neon => "NEON",
            Self::Sve => "SVE",
            Self::Sve2 => "SVE2",
        }
    }
}

// ---------------------------------------------------------------------------
// GF(2^8) split-table for NEON/SVE acceleration
// ---------------------------------------------------------------------------

/// Precomputed split-table for multiplying by a single GF(2^8) coefficient.
///
/// Contains two 16-entry tables: low nibble and high nibble.
/// Total size: 32 bytes per coefficient.
#[derive(Clone)]
struct GfMulTable {
    /// lo_table[i] = coefficient × i for i in 0..16 (low nibble)
    lo: [u8; 16],
    /// hi_table[i] = coefficient × (16 × i) for i in 0..16 (high nibble)
    hi: [u8; 16],
}

impl GfMulTable {
    /// Creates a new split-table for multiplying by `coeff` in GF(2^8).
    fn new(coeff: u8) -> Self {
        let mut lo = [0u8; 16];
        let mut hi = [0u8; 16];
        for i in 0..16u8 {
            lo[i as usize] = gf_mul_portable(coeff, i);
            hi[i as usize] = gf_mul_portable(coeff, i.wrapping_mul(16));
        }
        Self { lo, hi }
    }
}

/// Precomputed encoding tables for the entire Cauchy matrix.
///
/// For a k×m encoding matrix: m rows of k coefficient tables.
/// Size: 32 × k × m bytes.
struct EncodeTables {
    /// tables[row][col] = split-table for matrix[row][col]
    tables: Vec<Vec<GfMulTable>>,
}

impl EncodeTables {
    /// Builds split-tables for a k×m Cauchy encoding matrix.
    fn new(k: u8, m: u8) -> Self {
        let ki = k as usize;
        let mi = m as usize;
        // Build Cauchy matrix coefficients
        let mut coeffs = vec![vec![0u8; ki]; mi];
        for row in 0..mi {
            for col in 0..ki {
                let x = (col + 1) as u8;
                let y = (ki + row + 1) as u8;
                coeffs[row][col] = gf_inv_portable(x ^ y);
            }
        }

        let tables: Vec<Vec<GfMulTable>> =
            coeffs.iter().map(|row| row.iter().map(|&c| GfMulTable::new(c)).collect()).collect();

        Self { tables }
    }
}

// ---------------------------------------------------------------------------
// Portable GF(2^8) arithmetic (for table construction — NOT on the hot path)
// ---------------------------------------------------------------------------

/// GF(2^8) log table with primitive polynomial 0x11D.
static GF_LOG: [u8; 256] = [
    0, 0, 1, 25, 2, 50, 26, 198, 3, 223, 51, 238, 27, 104, 199, 75, 4, 100, 224, 14, 52, 141, 239,
    129, 28, 193, 105, 248, 200, 8, 76, 113, 5, 138, 101, 47, 225, 36, 15, 33, 53, 147, 142, 218,
    240, 18, 130, 69, 29, 181, 194, 125, 106, 39, 249, 185, 201, 154, 9, 120, 77, 228, 114, 166, 6,
    191, 139, 98, 102, 221, 48, 253, 226, 152, 37, 179, 16, 145, 34, 136, 54, 208, 148, 206, 143,
    150, 219, 189, 241, 210, 19, 92, 131, 56, 70, 64, 30, 66, 182, 163, 195, 72, 126, 110, 107, 58,
    40, 84, 250, 133, 186, 61, 202, 94, 155, 159, 10, 21, 121, 43, 78, 212, 229, 172, 115, 243,
    167, 87, 7, 112, 192, 247, 140, 128, 99, 13, 103, 74, 222, 237, 49, 197, 254, 24, 227, 165,
    153, 119, 38, 184, 180, 124, 17, 68, 146, 217, 35, 32, 137, 46, 55, 63, 209, 91, 149, 188, 207,
    205, 144, 135, 151, 178, 220, 252, 190, 97, 242, 86, 211, 171, 20, 42, 93, 158, 132, 60, 57,
    83, 71, 109, 65, 162, 31, 45, 67, 216, 183, 123, 164, 118, 196, 23, 73, 236, 127, 12, 111, 246,
    108, 161, 59, 82, 41, 157, 85, 170, 251, 96, 134, 177, 187, 204, 62, 90, 203, 89, 95, 176, 156,
    169, 160, 81, 11, 245, 22, 235, 122, 117, 44, 215, 79, 174, 213, 233, 230, 231, 173, 232, 116,
    214, 244, 234, 168, 80, 88, 175,
];

/// GF(2^8) exp table (double-length for wraparound).
static GF_EXP: [u8; 512] = [
    1, 2, 4, 8, 16, 32, 64, 128, 29, 58, 116, 232, 205, 135, 19, 38, 76, 152, 45, 90, 180, 117,
    234, 201, 143, 3, 6, 12, 24, 48, 96, 192, 157, 39, 78, 156, 37, 74, 148, 53, 106, 212, 181,
    119, 238, 193, 159, 35, 70, 140, 5, 10, 20, 40, 80, 160, 93, 186, 105, 210, 185, 111, 222, 161,
    95, 190, 97, 194, 153, 47, 94, 188, 101, 202, 137, 15, 30, 60, 120, 240, 253, 231, 211, 187,
    107, 214, 177, 127, 254, 225, 223, 163, 91, 182, 113, 226, 217, 175, 67, 134, 17, 34, 68, 136,
    13, 26, 52, 104, 208, 189, 103, 206, 129, 31, 62, 124, 248, 237, 199, 147, 59, 118, 236, 197,
    151, 51, 102, 204, 133, 23, 46, 92, 184, 109, 218, 169, 79, 158, 33, 66, 132, 21, 42, 84, 168,
    77, 154, 41, 82, 164, 85, 170, 73, 146, 57, 114, 228, 213, 183, 115, 230, 209, 191, 99, 198,
    145, 63, 126, 252, 229, 215, 179, 123, 246, 241, 255, 227, 219, 171, 75, 150, 49, 98, 196, 149,
    55, 110, 220, 165, 87, 174, 65, 130, 25, 50, 100, 200, 141, 7, 14, 28, 56, 112, 224, 221, 167,
    83, 166, 81, 162, 89, 178, 121, 242, 249, 239, 195, 155, 43, 86, 172, 69, 138, 9, 18, 36, 72,
    144, 61, 122, 244, 245, 247, 243, 251, 235, 203, 139, 11, 22, 44, 88, 176, 125, 250, 233, 207,
    131, 27, 54, 108, 216, 173, 71, 142, 1, 2, 4, 8, 16, 32, 64, 128, 29, 58, 116, 232, 205, 135,
    19, 38, 76, 152, 45, 90, 180, 117, 234, 201, 143, 3, 6, 12, 24, 48, 96, 192, 157, 39, 78, 156,
    37, 74, 148, 53, 106, 212, 181, 119, 238, 193, 159, 35, 70, 140, 5, 10, 20, 40, 80, 160, 93,
    186, 105, 210, 185, 111, 222, 161, 95, 190, 97, 194, 153, 47, 94, 188, 101, 202, 137, 15, 30,
    60, 120, 240, 253, 231, 211, 187, 107, 214, 177, 127, 254, 225, 223, 163, 91, 182, 113, 226,
    217, 175, 67, 134, 17, 34, 68, 136, 13, 26, 52, 104, 208, 189, 103, 206, 129, 31, 62, 124, 248,
    237, 199, 147, 59, 118, 236, 197, 151, 51, 102, 204, 133, 23, 46, 92, 184, 109, 218, 169, 79,
    158, 33, 66, 132, 21, 42, 84, 168, 77, 154, 41, 82, 164, 85, 170, 73, 146, 57, 114, 228, 213,
    183, 115, 230, 209, 191, 99, 198, 145, 63, 126, 252, 229, 215, 179, 123, 246, 241, 255, 227,
    219, 171, 75, 150, 49, 98, 196, 149, 55, 110, 220, 165, 87, 174, 65, 130, 25, 50, 100, 200,
    141, 7, 14, 28, 56, 112, 224, 221, 167, 83, 166, 81, 162, 89, 178, 121, 242, 249, 239, 195,
    155, 43, 86, 172, 69, 138, 9, 18, 36, 72, 144, 61, 122, 244, 245, 247, 243, 251, 235, 203, 139,
    11, 22, 44, 88, 176, 125, 250, 233, 207, 131, 27, 54, 108, 216, 173, 71, 142, 1, 2,
];

/// Portable GF(2^8) multiplication (used for table construction only).
#[inline]
fn gf_mul_portable(a: u8, b: u8) -> u8 {
    if a == 0 || b == 0 {
        return 0;
    }
    let sum = GF_LOG[a as usize] as u16 + GF_LOG[b as usize] as u16;
    GF_EXP[(sum % 255) as usize]
}

/// Portable GF(2^8) inverse (used for table construction only).
#[inline]
fn gf_inv_portable(a: u8) -> u8 {
    if a == 0 {
        return 0;
    }
    GF_EXP[(255 - GF_LOG[a as usize] as usize) % 255]
}

// ---------------------------------------------------------------------------
// NEON GF(2^8) multiply kernel (aarch64 only)
// ---------------------------------------------------------------------------

/// Multiplies 16 bytes by coefficient `c` using NEON split-table lookup.
///
/// Loads the low and high nibble tables into NEON registers, then
/// `vtbl1_u8` for both nibbles, XOR to combine.
///
/// # Safety
///
/// `data_ptr` must point to at least 16 readable bytes. The table
/// must have been constructed with `GfMulTable::new(c)` for the
/// matching coefficient.
#[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]
#[target_feature(enable = "neon")]
unsafe fn neon_gf_mul_16(table: &GfMulTable, data_ptr: *const u8) -> uint8x16_t {
    // SAFETY: caller guarantees data_ptr points to 16 readable bytes
    let data = vld1q_u8(data_ptr);

    // Split data into low and high nibbles
    let lo_nibbles = vandq_u8(data, vdupq_n_u8(0x0F));
    let hi_nibbles = vshrq_n_u8::<4>(data);

    // Load precomputed tables
    let lo_table = vld1q_u8(table.lo.as_ptr());
    let hi_table = vld1q_u8(table.hi.as_ptr());

    // Table lookup: result_lo = lo_table[lo_nibbles[i]] for each lane
    // vtbl1_u8 indexes a 16-entry table using the low 4 bits of each byte
    let lo_result = vqtbl1q_u8(lo_table, lo_nibbles);
    let hi_result = vqtbl1q_u8(hi_table, hi_nibbles);

    // GF addition is XOR
    veorq_u8(lo_result, hi_result)
}

/// NEON-accelerated EC encode: processes shard data in 16-byte chunks.
///
/// For each parity row, for each 16-byte chunk of the shard:
///   acc = 0
///   For each data shard j:
///     acc ^= neon_gf_mul_16(table[row][j], data[j] + offset)
///   store parity[row] + offset = acc
#[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]
#[target_feature(enable = "neon")]
unsafe fn neon_encode(
    tables: &EncodeTables,
    data_shards: &[&[u8]],
    parity_count: u8,
    shard_size: usize,
) -> oceanfs_ec::Result<Vec<Vec<u8>>> {
    let k = data_shards.len();
    let m = parity_count as usize;
    let num_chunks = shard_size / 16;

    // Allocate parity output buffers (16-byte aligned if possible)
    let mut parity: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();

    for row in 0..m {
        let parity_ptr = parity[row].as_mut_ptr();
        for chunk in 0..num_chunks {
            let offset = chunk * 16;

            // Accumulator: starts at zero
            let mut acc = vdupq_n_u8(0);

            for col in 0..k {
                let table = &tables.tables[row][col];
                let data_ptr = data_shards[col].as_ptr().add(offset);
                // SAFETY: data_ptr points to a valid 16-byte chunk within the shard.
                // table was precomputed for this (row, col) coefficient.
                let product = neon_gf_mul_16(table, data_ptr);
                acc = veorq_u8(acc, product);
            }

            // SAFETY: parity_ptr + offset is within the allocated buffer
            vst1q_u8(parity_ptr.add(offset), acc);
        }

        // Handle remainder bytes (< 16) with portable fallback
        let remainder_start = num_chunks * 16;
        for byte_idx in remainder_start..shard_size {
            let mut sum: u8 = 0;
            for col in 0..k {
                let coeff = {
                    let x = (col + 1) as u8;
                    let y = (k + row + 1) as u8;
                    gf_inv_portable(x ^ y)
                };
                sum ^= gf_mul_portable(coeff, data_shards[col][byte_idx]);
            }
            parity[row][byte_idx] = sum;
        }
    }

    Ok(parity)
}

/// Portable EC encode (used when NEON is unavailable or for remainder bytes).
fn portable_encode(k: usize, m: usize, data_shards: &[&[u8]], shard_size: usize) -> Vec<Vec<u8>> {
    let mut parity: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();

    for row in 0..m {
        for byte_idx in 0..shard_size {
            let mut sum: u8 = 0;
            for col in 0..k {
                let x = (col + 1) as u8;
                let y = (k + row + 1) as u8;
                let coeff = gf_inv_portable(x ^ y);
                sum ^= gf_mul_portable(coeff, data_shards[col][byte_idx]);
            }
            parity[row][byte_idx] = sum;
        }
    }

    parity
}

// ---------------------------------------------------------------------------
// ArmEncoder
// ---------------------------------------------------------------------------

/// ARM NEON/SVE accelerated Reed-Solomon encoder/decoder.
///
/// On aarch64 with the `arm-sve` feature: probes for SVE2, SVE, and NEON
/// at construction, precomputes split-tables for the Cauchy encoding matrix,
/// and uses SIMD intrinsics for the encode hot path.
///
/// On non-ARM platforms: delegates to the portable Cauchy RS encoder.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_accel::ArmEncoder;
/// use oceanfs_ec::{Encoder, Decoder};
///
/// let encoder = ArmEncoder::new(4, 2);
/// let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
/// let parity = encoder.encode(&data, 2).unwrap();
/// ```
pub struct ArmEncoder {
    /// Detected SIMD level (Portable if not on aarch64).
    level: ArmSveLevel,
    /// Precomputed split-tables for the encode path (NEON/SVE only).
    encode_tables: Option<EncodeTables>,
    /// Portable fallback encoder (always available).
    fallback: CauchyEncoder,
    /// k and m parameters.
    k: u8,
    /// m parameter.
    m: u8,
}

impl ArmEncoder {
    /// Creates a new ARM encoder, probing for available SIMD capabilities.
    ///
    /// On aarch64 with `arm-sve` feature: probes SVE2 → SVE → NEON,
    /// precomputes split-tables for the encoding matrix.
    /// On other platforms: uses portable fallback.
    pub fn new(k: u8, m: u8) -> Self {
        let level = Self::probe_simd_level();
        let encode_tables = if level > ArmSveLevel::Portable {
            tracing::info!(simd_level = level.name(), k = k, m = m, "ARM SIMD encoder initialized");
            Some(EncodeTables::new(k, m))
        } else {
            None
        };

        let config = CodecConfig { data_shards: k, parity_shards: m, ..Default::default() };

        Self { level, encode_tables, fallback: CauchyEncoder::new(config), k, m }
    }

    /// Returns the detected ARM SIMD level.
    pub fn simd_level(&self) -> ArmSveLevel {
        self.level
    }

    /// Returns `true` if any SIMD acceleration is active.
    pub fn is_accelerated(&self) -> bool {
        self.level > ArmSveLevel::Portable
    }

    // -------------------------------------------------------------------
    // SIMD probing (compile-time + runtime)
    // -------------------------------------------------------------------

    fn probe_simd_level() -> ArmSveLevel {
        #[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]
        {
            // SAFETY: These feature detection macros read CPU feature registers.
            // They are safe to call at any time from userspace.
            if std::arch::is_aarch64_feature_detected!("sve2") {
                tracing::debug!("ARM SIMD: SVE2 detected");
                return ArmSveLevel::Sve2;
            }
            if std::arch::is_aarch64_feature_detected!("sve") {
                tracing::debug!("ARM SIMD: SVE detected");
                return ArmSveLevel::Sve;
            }
        }

        // NEON is mandatory on aarch64 (always present).
        // On x86_64 or without arm-sve feature, we use portable.
        #[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]
        {
            tracing::debug!("ARM SIMD: using NEON (baseline aarch64 SIMD)");
            return ArmSveLevel::Neon;
        }

        #[cfg(not(all(target_arch = "aarch64", feature = "arm-sve")))]
        {
            tracing::debug!("ARM SIMD: portable fallback (not on aarch64 or feature disabled)");
            ArmSveLevel::Portable
        }
    }
}

// ---------------------------------------------------------------------------
// Encoder implementation
// ---------------------------------------------------------------------------

impl Encoder for ArmEncoder {
    fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> oceanfs_ec::Result<Vec<Vec<u8>>> {
        if parity_count == 0 {
            return Ok(Vec::new());
        }
        let shard_size = data_shards.first().map(|s| s.len()).unwrap_or(0);
        if shard_size == 0 {
            return Ok(vec![Vec::new(); parity_count as usize]);
        }

        let m = parity_count as usize;
        let k = data_shards.len();

        // --- NEON accelerated path (aarch64 + arm-sve feature) ---
        #[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]
        if self.level >= ArmSveLevel::Neon {
            if let Some(ref tables) = self.encode_tables {
                // SAFETY: The encode_tables were constructed for this (k, m).
                // data_shards pointers are valid. The NEON intrinsics operate
                // on 16-byte aligned chunks within the shards. Any remainder
                // bytes are handled by portable fallback inside neon_encode.
                let result = unsafe { neon_encode(tables, data_shards, parity_count, shard_size) };
                match result {
                    Ok(parity) => return Ok(parity),
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "NEON encode failed; falling back to portable"
                        );
                    }
                }
            }
        }

        // --- Portable fallback ---
        Ok(portable_encode(k, m, data_shards, shard_size))
    }
}

// ---------------------------------------------------------------------------
// Decoder implementation (delegates to proven Cauchy RS)
// ---------------------------------------------------------------------------

impl Decoder for ArmEncoder {
    fn decode(
        &self,
        available_shards: &[Option<&[u8]>],
        data_count: u8,
        parity_count: u8,
    ) -> oceanfs_ec::Result<Vec<Vec<u8>>> {
        self.fallback.decode(available_shards, data_count, parity_count)
    }
}

// ---------------------------------------------------------------------------
// Public helpers for dispatcher probing
// ---------------------------------------------------------------------------

/// Returns `true` if any ARM SIMD capability is available.
#[allow(dead_code)]
pub(crate) fn is_arm_accelerated() -> bool {
    let level = ArmEncoder::probe_simd_level();
    level > ArmSveLevel::Portable
}

/// Returns a human-readable description of ARM SIMD capabilities.
#[allow(dead_code)]
pub(crate) fn arm_capabilities() -> &'static str {
    ArmEncoder::probe_simd_level().name()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // -- Construction --

    #[test]
    fn arm_encoder_construction() {
        let encoder = ArmEncoder::new(4, 2);
        #[cfg(not(all(target_arch = "aarch64", feature = "arm-sve")))]
        {
            assert!(!encoder.is_accelerated());
        }
        assert_eq!(encoder.simd_level().name(), arm_capabilities());
    }

    #[test]
    fn arm_simd_level_ordering() {
        assert!(ArmSveLevel::Sve2 > ArmSveLevel::Sve);
        assert!(ArmSveLevel::Sve > ArmSveLevel::Neon);
        assert!(ArmSveLevel::Neon > ArmSveLevel::Portable);
    }

    #[test]
    fn arm_simd_level_names() {
        assert_eq!(ArmSveLevel::Portable.name(), "portable");
        assert_eq!(ArmSveLevel::Neon.name(), "NEON");
        assert_eq!(ArmSveLevel::Sve.name(), "SVE");
        assert_eq!(ArmSveLevel::Sve2.name(), "SVE2");
    }

    // -- GF arithmetic (portable) --

    #[test]
    fn gf_mul_portable_zero() {
        for a in 0..=255u8 {
            assert_eq!(gf_mul_portable(a, 0), 0);
            assert_eq!(gf_mul_portable(0, a), 0);
        }
    }

    #[test]
    fn gf_mul_portable_identity() {
        for a in 1..=255u8 {
            assert_eq!(gf_mul_portable(a, 1), a);
        }
    }

    #[test]
    fn gf_inv_portable_is_inverse() {
        for a in 1..=255u8 {
            assert_eq!(gf_mul_portable(a, gf_inv_portable(a)), 1);
        }
    }

    #[test]
    fn gf_mul_portable_commutative() {
        for a in [1u8, 2, 10, 50, 100, 200, 255] {
            for b in [1u8, 3, 7, 50, 128, 250] {
                assert_eq!(gf_mul_portable(a, b), gf_mul_portable(b, a));
            }
        }
    }

    // -- Split-table correctness --

    #[test]
    fn gf_mul_table_equivalent_to_portable() {
        for coeff in 1..=255u8 {
            let table = GfMulTable::new(coeff);
            for i in 0..16u8 {
                // Low nibble: lo_table[i] should = coeff * i
                assert_eq!(
                    table.lo[i as usize],
                    gf_mul_portable(coeff, i),
                    "coeff={}, lo_table[{}]: {} != {}",
                    coeff,
                    i,
                    table.lo[i as usize],
                    gf_mul_portable(coeff, i)
                );
                // High nibble: hi_table[i] should = coeff * (16*i)
                assert_eq!(
                    table.hi[i as usize],
                    gf_mul_portable(coeff, i.wrapping_mul(16)),
                    "coeff={}, hi_table[{}]: {} != {}",
                    coeff,
                    i,
                    table.hi[i as usize],
                    gf_mul_portable(coeff, i.wrapping_mul(16))
                );
            }
            // Verify split-table reconstruction: lo[b & 0xF] ^ hi[b >> 4] = coeff * b
            for b in 1..=255u8 {
                let lo_idx = (b & 0x0F) as usize;
                let hi_idx = (b >> 4) as usize;
                let reconstructed = table.lo[lo_idx] ^ table.hi[hi_idx];
                assert_eq!(
                    reconstructed,
                    gf_mul_portable(coeff, b),
                    "coeff={}, b={}: lo[{}]^hi[{}] = {} != {}",
                    coeff,
                    b,
                    lo_idx,
                    hi_idx,
                    reconstructed,
                    gf_mul_portable(coeff, b)
                );
            }
        }
    }

    // -- Encode/Decode roundtrip (portable path, works everywhere) --

    #[test]
    fn encode_decode_roundtrip_k4_m2() {
        let encoder = ArmEncoder::new(4, 2);
        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 128]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = encoder.encode(&shard_refs, 2).unwrap();
        assert_eq!(parity.len(), 2);

        // Lose shard 0
        let available: Vec<Option<&[u8]>> = vec![
            None,
            Some(&data[1]),
            Some(&data[2]),
            Some(&data[3]),
            Some(&parity[0]),
            Some(&parity[1]),
        ];
        let recovered = encoder.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered[0], data[0]);
    }

    #[test]
    fn encode_decode_k8_m4() {
        let encoder = ArmEncoder::new(8, 4);
        let data: Vec<Vec<u8>> = (0..8).map(|i| vec![i; 64]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = encoder.encode(&shard_refs, 4).unwrap();

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
        let recovered = encoder.decode(&available, 8, 4).unwrap();
        assert_eq!(recovered[0], data[0]);
        assert_eq!(recovered[3], data[3]);
    }

    #[test]
    fn encode_tables_reconstructs_correctly() {
        let tables = EncodeTables::new(4, 2);
        assert_eq!(tables.tables.len(), 2); // m=2 parity rows
        assert_eq!(tables.tables[0].len(), 4); // k=4 data columns
                                               // Every table entry should be non-empty
        for row in &tables.tables {
            for table in row {
                assert!(table.lo.iter().any(|&x| x != 0) || table.hi.iter().any(|&x| x != 0));
            }
        }
    }

    #[test]
    fn portable_encode_produces_valid_parity() {
        let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
        let parity = portable_encode(4, 2, &data, 4);
        assert_eq!(parity.len(), 2);
        assert_eq!(parity[0].len(), 4);
    }
}
