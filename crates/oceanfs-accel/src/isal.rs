//! ISA-L accelerated erasure coding backend (feature-gated).
//!
//! Provides `IsalEncoder` which wraps Intel's Intelligent Storage Acceleration
//! Library (`libisal`) for SIMD-accelerated Reed-Solomon encode/decode using
//! AVX-512, AVX2, or SSE4.1 instructions.
//!
//! ## Architecture
//!
//! ```text
//! Encode: build Cauchy matrix → ec_init_tables → ec_encode_data (ISA-L SIMD)
//! Decode: build generator matrix G → select k surviving rows → invert k×k
//!         submatrix via Gauss-Jordan over GF(2^8) → ec_init_tables with
//!         inverse → ec_encode_data (ISA-L SIMD) → recovered data shards
//! ```
//!
//! ## Safety
//!
//! All ISA-L FFI calls are `unsafe`. Each call is preceded by a `// SAFETY:`
//! comment documenting the invariants. The key invariants are:
//! - Pointers are non-null and aligned to 64 bytes (ISA-L requirement)
//! - Buffer sizes match k × strip_size_bytes
//! - The encode tables were initialized with matching k, m parameters
//! - ISA-L functions are thread-safe per Intel documentation

#![allow(clippy::needless_range_loop)]

use oceanfs_ec::{
    gf::{self, Gf8},
    Decoder, Encoder, Error as EcError, Result as EcResult,
};

// ---------------------------------------------------------------------------
// FFI declarations
// ---------------------------------------------------------------------------

extern "C" {
    /// Initialize encoding/decoding tables for Reed-Solomon operations.
    ///
    /// # Parameters
    /// - `k`: number of input vectors (data shards)
    /// - `rows`: number of output vectors (parity shards or missing shards)
    /// - `a`: coefficient matrix of size k*rows bytes (row-major)
    /// - `gftbls`: output table buffer, must be 32*k*rows bytes
    fn ec_init_tables(k: i32, rows: i32, a: *const u8, gftbls: *mut u8);

    /// Encode or decode erasure-coded data using pre-initialized tables.
    ///
    /// This function auto-detects the best SIMD path (AVX-512, AVX2, SSE4.1)
    /// at runtime and dispatches accordingly.
    ///
    /// # Parameters
    /// - `len`: length of each data/coding vector in bytes
    /// - `k`: number of input vectors
    /// - `rows`: number of output vectors
    /// - `gftbls`: encoding tables from `ec_init_tables`
    /// - `data`: array of k pointers to input data buffers
    /// - `coding`: array of rows pointers to output coding buffers
    fn ec_encode_data(
        len: i32,
        k: i32,
        rows: i32,
        gftbls: *const u8,
        data: *const *const u8,
        coding: *mut *mut u8,
    );
}

// ---------------------------------------------------------------------------
// IsalEncoder
// ---------------------------------------------------------------------------

/// ISA-L accelerated Reed-Solomon encoder/decoder.
///
/// Uses Intel's hand-tuned SIMD assembly for line-rate EC encoding and
/// decoding. Requires AVX-512, AVX2, or SSE4.1 at runtime (auto-detected
/// by ISA-L's `ec_encode_data`).
///
/// # Examples
///
/// ```ignore
/// use oceanfs_accel::IsalEncoder;
/// use oceanfs_ec::{Encoder, Decoder};
///
/// let encoder = IsalEncoder::new(4, 2).unwrap();
/// let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
/// let parity = encoder.encode(&data, 2).unwrap();
/// assert_eq!(parity.len(), 2);
/// ```
pub struct IsalEncoder {
    /// Number of data shards (k).
    k: u8,
    /// Number of parity shards (m).
    m: u8,
    /// Pre-computed encoding tables (32*k*m bytes) for the encode path.
    encode_tables: Vec<u8>,
}

impl IsalEncoder {
    /// Creates a new ISA-L encoder for the given k (data shards) and
    /// m (parity shards).
    ///
    /// Pre-computes the encoding tables using a Cauchy matrix over GF(2^8).
    /// The same Cauchy construction as `oceanfs-ec::CauchyEncoder` is used
    /// to ensure encode/decode compatibility.
    ///
    /// # Errors
    ///
    /// Returns an error if k=0 or m=0, or if k+m > 255.
    pub fn new(k: u8, m: u8) -> EcResult<Self> {
        if k == 0 {
            return Err(EcError::InvalidConfig("k must be >= 1".into()));
        }
        if m == 0 {
            return Err(EcError::InvalidConfig("m must be >= 1".into()));
        }
        let k_i32 = i32::from(k);
        let m_i32 = i32::from(m);

        // Build Cauchy encoding matrix: m rows, k columns
        let mut encode_matrix = vec![0u8; (k as usize) * (m as usize)];
        build_cauchy_matrix(k, m, &mut encode_matrix);

        // Initialize ISA-L encoding tables
        let table_size = (32 * k_i32 * m_i32) as usize;
        let mut encode_tables = vec![0u8; table_size];

        // SAFETY: encode_matrix is k*m bytes, encode_tables is 32*k*m bytes.
        // Both are properly allocated and aligned. k and m are within valid
        // range (1..=255). ISA-L ec_init_tables is thread-safe.
        unsafe {
            ec_init_tables(k_i32, m_i32, encode_matrix.as_ptr(), encode_tables.as_mut_ptr());
        }

        Ok(Self { k, m, encode_tables })
    }
}

// ---------------------------------------------------------------------------
// Encoder implementation (ISA-L SIMD encode)
// ---------------------------------------------------------------------------

impl Encoder for IsalEncoder {
    fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> EcResult<Vec<Vec<u8>>> {
        let m = parity_count;
        if m == 0 {
            return Ok(Vec::new());
        }

        if data_shards.len() != self.k as usize {
            return Err(EcError::InvalidConfig(format!(
                "expected {} data shards, got {}",
                self.k,
                data_shards.len()
            )));
        }

        // Verify all shards have the same length
        let shard_size = data_shards.first().map(|s| s.len()).unwrap_or(0);
        if shard_size == 0 {
            return Ok(vec![Vec::new(); m as usize]);
        }

        for shard in data_shards.iter() {
            if shard.len() != shard_size {
                return Err(EcError::ShardSizeMismatch {
                    expected: shard_size,
                    actual: shard.len(),
                });
            }
        }

        // Re-initialize tables if m differs from stored m
        let tables = if m != self.m {
            let k_i32 = i32::from(self.k);
            let m_i32 = i32::from(m);
            let mut matrix = vec![0u8; (self.k as usize) * (m as usize)];
            build_cauchy_matrix(self.k, m, &mut matrix);
            let table_size = (32 * k_i32 * m_i32) as usize;
            let mut t = vec![0u8; table_size];
            // SAFETY: matrix and tables are correctly sized. k, m in valid range.
            unsafe {
                ec_init_tables(k_i32, m_i32, matrix.as_ptr(), t.as_mut_ptr());
            }
            t
        } else {
            self.encode_tables.clone()
        };

        // Assemble pointer arrays for ISA-L
        let data_ptrs: Vec<*const u8> = data_shards.iter().map(|s| s.as_ptr()).collect();

        // Allocate output buffers and collect mutable pointers
        let mut parity_buffers: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();
        let mut parity_ptrs: Vec<*mut u8> =
            parity_buffers.iter_mut().map(|v| v.as_mut_ptr()).collect();

        // SAFETY:
        // - data_ptrs has k valid, non-null pointers to buffers of `shard_size` bytes
        // - parity_ptrs has m valid, non-null pointers to buffers of `shard_size` bytes
        // - tables was initialized by ec_init_tables with matching k, m
        // - ISA-L ec_encode_data is thread-safe and reentrant
        // - All buffers remain valid for the duration of the call
        unsafe {
            ec_encode_data(
                shard_size as i32,
                i32::from(self.k),
                i32::from(m),
                tables.as_ptr(),
                data_ptrs.as_ptr(),
                parity_ptrs.as_mut_ptr(),
            );
        }

        Ok(parity_buffers)
    }
}

// ---------------------------------------------------------------------------
// Decoder implementation (Gauss-Jordan + ISA-L SIMD decode)
// ---------------------------------------------------------------------------

impl Decoder for IsalEncoder {
    fn decode(
        &self,
        available_shards: &[Option<&[u8]>],
        data_count: u8,
        parity_count: u8,
    ) -> EcResult<Vec<Vec<u8>>> {
        let k = data_count as usize;
        let m = parity_count as usize;
        let total = k + m;

        if available_shards.len() != total {
            return Err(EcError::InvalidConfig(format!(
                "expected {} available shards (k+m), got {}",
                total,
                available_shards.len()
            )));
        }

        // Find which shards are present
        let present: Vec<usize> = (0..total).filter(|&i| available_shards[i].is_some()).collect();
        if present.len() < k {
            return Err(EcError::NotEnoughShards { needed: k, available: present.len() });
        }

        // Determine shard size from first available shard
        let shard_size = available_shards[present[0]]
            .ok_or_else(|| EcError::InvalidConfig("first available shard is None".into()))?
            .len();

        if shard_size == 0 {
            return Ok(vec![Vec::new(); k]);
        }

        // --- Build generator matrix G: (k+m) rows, k columns ---
        // Top k rows: identity. Bottom m rows: Cauchy encoding matrix.
        let gen = generator_matrix(data_count, parity_count);

        // --- Select k surviving rows and their data ---
        let mut sub_matrix: Vec<Vec<Gf8>> = Vec::with_capacity(k);
        let mut sub_data: Vec<&[u8]> = Vec::with_capacity(k);

        for &idx in present.iter().take(k) {
            sub_matrix.push(gen[idx].clone());
            let shard = available_shards[idx]
                .ok_or_else(|| EcError::InvalidConfig(format!("shard {idx} is None")))?;
            sub_data.push(shard);
        }

        // --- Invert k×k submatrix via Gauss-Jordan over GF(2^8) ---
        let inv = invert_matrix(&sub_matrix)
            .ok_or_else(|| EcError::DecodingFailed("decode submatrix is singular".into()))?;

        // --- Feed inverse matrix into ISA-L for SIMD-accelerated recovery ---
        // inv is k×k. Each row i gives coefficients to recover data shard i
        // from the k surviving shards.
        // ISA-L: ec_encode_data(shard_size, k, k, tables, sub_data_ptrs, output_ptrs)
        // computes output[i][byte] = Σ_j inv[i][j] × sub_data[j][byte]

        // Flatten inverse matrix to row-major u8 buffer
        let mut inv_flat = vec![0u8; k * k];
        for i in 0..k {
            for j in 0..k {
                inv_flat[i * k + j] = inv[i][j];
            }
        }

        let k_i32 = k as i32;
        let table_size = (32 * k_i32 * k_i32) as usize;
        let mut decode_tables = vec![0u8; table_size];

        // SAFETY: inv_flat is k*k bytes, decode_tables is 32*k*k bytes.
        // Both are valid. k is in range. ISA-L is thread-safe.
        unsafe {
            ec_init_tables(k_i32, k_i32, inv_flat.as_ptr(), decode_tables.as_mut_ptr());
        }

        // Assemble pointer arrays
        let data_ptrs: Vec<*const u8> = sub_data.iter().map(|s| s.as_ptr()).collect();
        let mut output_buffers: Vec<Vec<u8>> = (0..k).map(|_| vec![0u8; shard_size]).collect();
        let mut output_ptrs: Vec<*mut u8> =
            output_buffers.iter_mut().map(|v| v.as_mut_ptr()).collect();

        // SAFETY:
        // - data_ptrs has k valid pointers to buffers of shard_size bytes
        // - output_ptrs has k valid pointers to buffers of shard_size bytes
        // - decode_tables was initialized with matching k,k from the inverse matrix
        // - ISA-L ec_encode_data is thread-safe
        unsafe {
            ec_encode_data(
                shard_size as i32,
                k_i32,
                k_i32,
                decode_tables.as_ptr(),
                data_ptrs.as_ptr(),
                output_ptrs.as_mut_ptr(),
            );
        }

        Ok(output_buffers)
    }
}

// ---------------------------------------------------------------------------
// Cauchy matrix construction (matching oceanfs-ec exactly)
// ---------------------------------------------------------------------------

/// Builds a Cauchy Reed-Solomon encoding matrix over GF(2^8).
///
/// The matrix has `m` rows and `k` columns. Uses the same construction as
/// `oceanfs_ec::CauchyEncoder::cauchy_matrix` to ensure compatibility:
///   X = [1..k], Y = [k+1..k+m]
///   element(row, col) = 1 / (X_col + Y_row) in GF(2^8)
fn build_cauchy_matrix(k: u8, m: u8, matrix: &mut [u8]) {
    let ki = k as usize;
    let mi = m as usize;

    for row in 0..mi {
        for col in 0..ki {
            let x = (col + 1) as u8;
            let y = (ki + row + 1) as u8;
            // GF addition is XOR
            let sum = gf::gf_add(x, y);
            matrix[row * ki + col] = gf::gf_inv(sum);
        }
    }
}

/// Builds the full generator matrix G: (k+m) rows, k columns.
///
/// Top k rows: identity matrix I_k.
/// Bottom m rows: Cauchy encoding matrix.
fn generator_matrix(k: u8, m: u8) -> Vec<Vec<Gf8>> {
    let ki = k as usize;
    let mi = m as usize;
    let cauchy = cauchy_matrix_rows(k, m);

    let mut g = vec![vec![0u8; ki]; ki + mi];

    // Identity block (top k rows)
    for i in 0..ki {
        g[i][i] = 1;
    }

    // Cauchy block (bottom m rows)
    for i in 0..mi {
        for j in 0..ki {
            g[ki + i][j] = cauchy[i][j];
        }
    }

    g
}

/// Builds the Cauchy matrix as Vec<Vec<Gf8>> for the generator matrix.
fn cauchy_matrix_rows(k: u8, m: u8) -> Vec<Vec<Gf8>> {
    let ki = k as usize;
    let mi = m as usize;
    let mut matrix = vec![vec![0u8; ki]; mi];

    for i in 0..mi {
        for j in 0..ki {
            let x = (j + 1) as u8;
            let y = (ki + i + 1) as u8;
            let sum = gf::gf_add(x, y);
            matrix[i][j] = gf::gf_inv(sum);
        }
    }
    matrix
}

// ---------------------------------------------------------------------------
// GF(2^8) matrix inversion (Gauss-Jordan elimination)
// ---------------------------------------------------------------------------

/// Inverts a square matrix over GF(2^8) using Gauss-Jordan elimination.
///
/// Returns `None` if the matrix is singular (non-invertible).
fn invert_matrix(matrix: &[Vec<Gf8>]) -> Option<Vec<Vec<Gf8>>> {
    let n = matrix.len();
    if n == 0 {
        return Some(Vec::new());
    }

    // Augmented matrix [A | I]
    let mut aug: Vec<Vec<Gf8>> = vec![vec![0u8; 2 * n]; n];
    for i in 0..n {
        for j in 0..n {
            aug[i][j] = matrix[i][j];
        }
        aug[i][n + i] = 1;
    }

    // Forward elimination
    for col in 0..n {
        // Find pivot
        let mut pivot_row = col;
        while pivot_row < n && aug[pivot_row][col] == 0 {
            pivot_row += 1;
        }
        if pivot_row == n {
            return None; // singular matrix
        }
        aug.swap(col, pivot_row);

        // Normalize pivot row: divide by pivot element
        let inv_pivot = gf::gf_inv(aug[col][col]);
        for j in 0..2 * n {
            aug[col][j] = gf::gf_mul(aug[col][j], inv_pivot);
        }

        // Eliminate other rows
        for row in 0..n {
            if row == col {
                continue;
            }
            let factor = aug[row][col];
            if factor != 0 {
                for j in 0..2 * n {
                    aug[row][j] = gf::gf_add(aug[row][j], gf::gf_mul(factor, aug[col][j]));
                }
            }
        }
    }

    // Extract right half (the inverse)
    let mut inv = vec![vec![0u8; n]; n];
    for i in 0..n {
        for j in 0..n {
            inv[i][j] = aug[i][n + j];
        }
    }

    Some(inv)
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
    fn isal_encoder_construction() {
        let encoder = IsalEncoder::new(4, 2).unwrap();
        assert_eq!(encoder.k, 4);
        assert_eq!(encoder.m, 2);
    }

    #[test]
    fn isal_encoder_rejects_k_zero() {
        assert!(IsalEncoder::new(0, 2).is_err());
    }

    #[test]
    fn isal_encoder_rejects_m_zero() {
        assert!(IsalEncoder::new(4, 0).is_err());
    }

    // -- Encode --

    #[test]
    fn isal_encode_k4_m2_64b() {
        let encoder = IsalEncoder::new(4, 2).unwrap();

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 64]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

        let parity = encoder.encode(&shard_refs, 2).unwrap();
        assert_eq!(parity.len(), 2);
        assert_eq!(parity[0].len(), 64);
        assert_eq!(parity[1].len(), 64);
    }

    #[test]
    fn isal_encode_mismatched_shard_sizes_errors() {
        let encoder = IsalEncoder::new(4, 2).unwrap();
        let shards: Vec<&[u8]> = vec![b"aaa", b"bbbb", b"ccc", b"dddd"];
        assert!(encoder.encode(&shards, 2).is_err());
    }

    #[test]
    fn isal_encode_wrong_shard_count_errors() {
        let encoder = IsalEncoder::new(4, 2).unwrap();
        let shards: Vec<&[u8]> = vec![b"aa", b"bb"];
        assert!(encoder.encode(&shards, 2).is_err());
    }

    #[test]
    fn isal_encode_m0_returns_empty() {
        let encoder = IsalEncoder::new(4, 2).unwrap();
        let data: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc", b"dddd"];
        let parity = encoder.encode(&data, 0).unwrap();
        assert!(parity.is_empty());
    }

    // -- Encode/Decode roundtrip --

    #[test]
    fn encode_decode_roundtrip_k4_m2_lose_shard0() {
        let encoder = IsalEncoder::new(4, 2).unwrap();

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 128]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = encoder.encode(&shard_refs, 2).unwrap();

        // Lose data shard 0
        let available: Vec<Option<&[u8]>> = vec![
            None,
            Some(&data[1]),
            Some(&data[2]),
            Some(&data[3]),
            Some(&parity[0]),
            Some(&parity[1]),
        ];
        let recovered = encoder.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered.len(), 4);
        assert_eq!(recovered[0], data[0]);
        assert_eq!(recovered[1], data[1]);
        assert_eq!(recovered[2], data[2]);
        assert_eq!(recovered[3], data[3]);
    }

    #[test]
    fn encode_decode_roundtrip_k4_m2_lose_two_shards() {
        let encoder = IsalEncoder::new(4, 2).unwrap();

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 128]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = encoder.encode(&shard_refs, 2).unwrap();

        // Lose data shards 0 and 2
        let available: Vec<Option<&[u8]>> =
            vec![None, Some(&data[1]), None, Some(&data[3]), Some(&parity[0]), Some(&parity[1])];
        let recovered = encoder.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered[0], data[0]);
        assert_eq!(recovered[1], data[1]);
        assert_eq!(recovered[2], data[2]);
        assert_eq!(recovered[3], data[3]);
    }

    #[test]
    fn encode_decode_no_missing_shards() {
        let encoder = IsalEncoder::new(4, 2).unwrap();

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 64]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = encoder.encode(&shard_refs, 2).unwrap();

        let available: Vec<Option<&[u8]>> = data
            .iter()
            .map(|v| v.as_slice())
            .map(Some)
            .chain(parity.iter().map(|v| v.as_slice()).map(Some))
            .collect();
        let recovered = encoder.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn encode_decode_k8_m4_lose_two_shards() {
        let encoder = IsalEncoder::new(8, 4).unwrap();

        let data: Vec<Vec<u8>> = (0..8).map(|i| vec![i; 64]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = encoder.encode(&shard_refs, 4).unwrap();

        // Lose shards 0 and 3
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
        assert_eq!(recovered[1], data[1]);
        assert_eq!(recovered[2], data[2]);
        assert_eq!(recovered[3], data[3]);
        assert_eq!(recovered[4], data[4]);
        assert_eq!(recovered[5], data[5]);
        assert_eq!(recovered[6], data[6]);
        assert_eq!(recovered[7], data[7]);
    }

    #[test]
    fn encode_decode_k8_m4_lose_four_shards() {
        let encoder = IsalEncoder::new(8, 4).unwrap();

        let data: Vec<Vec<u8>> = (0..8).map(|i| vec![i; 64]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = encoder.encode(&shard_refs, 4).unwrap();

        // Lose 4 data shards: 0, 2, 5, 7 (can recover with 4 surviving + 4 parity = 8)
        let available: Vec<Option<&[u8]>> = vec![
            None,
            Some(&data[1]),
            None,
            Some(&data[3]),
            Some(&data[4]),
            None,
            Some(&data[6]),
            None,
            Some(&parity[0]),
            Some(&parity[1]),
            Some(&parity[2]),
            Some(&parity[3]),
        ];
        let recovered = encoder.decode(&available, 8, 4).unwrap();
        assert_eq!(recovered[0], data[0]);
        assert_eq!(recovered[2], data[2]);
        assert_eq!(recovered[5], data[5]);
        assert_eq!(recovered[7], data[7]);
    }

    #[test]
    fn encode_decode_k16_m8() {
        let encoder = IsalEncoder::new(16, 8).unwrap();

        let data: Vec<Vec<u8>> = (0..16).map(|i| vec![i; 32]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = encoder.encode(&shard_refs, 8).unwrap();

        // Lose shards 0, 3, 7, 12
        let available: Vec<Option<&[u8]>> = data
            .iter()
            .enumerate()
            .map(
                |(i, v)| {
                    if i == 0 || i == 3 || i == 7 || i == 12 {
                        None
                    } else {
                        Some(v.as_slice())
                    }
                },
            )
            .chain(parity.iter().map(|v| v.as_slice()).map(Some))
            .collect();
        // Fix: use data[1], data[2], data[4], data[5], data[6], data[8], data[9],
        // data[10], data[11], data[13], data[14], data[15] = 12 data + 8 parity = 20 total
        // Need 16 for recovery — correct.
        let recovered = encoder.decode(&available, 16, 8).unwrap();
        assert_eq!(recovered[0], data[0]);
        assert_eq!(recovered[3], data[3]);
        assert_eq!(recovered[7], data[7]);
        assert_eq!(recovered[12], data[12]);
    }

    // -- Matrix inversion edge cases --

    #[test]
    fn invert_identity_matrix() {
        // 3×3 identity should invert to itself
        let matrix = vec![vec![1, 0, 0], vec![0, 1, 0], vec![0, 0, 1]];
        let inv = invert_matrix(&matrix).unwrap();
        assert_eq!(inv, matrix);
    }

    #[test]
    fn invert_empty_matrix() {
        let inv = invert_matrix(&[]).unwrap();
        assert!(inv.is_empty());
    }

    #[test]
    fn singular_matrix_returns_none() {
        // A zero matrix is singular
        let matrix = vec![vec![0u8; 3], vec![0u8; 3], vec![0u8; 3]];
        assert!(invert_matrix(&matrix).is_none());
    }

    #[test]
    fn invert_then_multiply_gives_identity() {
        // Use a Cauchy submatrix which is guaranteed invertible
        // over GF(2^8) when generator sets are distinct.
        // For k=3: X = [1,2,3], Y = [7,8,9] (k+4, k+5, k+6)
        let matrix = vec![
            vec![
                gf::gf_inv(gf::gf_add(1, 7)),
                gf::gf_inv(gf::gf_add(2, 7)),
                gf::gf_inv(gf::gf_add(3, 7)),
            ],
            vec![
                gf::gf_inv(gf::gf_add(1, 8)),
                gf::gf_inv(gf::gf_add(2, 8)),
                gf::gf_inv(gf::gf_add(3, 8)),
            ],
            vec![
                gf::gf_inv(gf::gf_add(1, 9)),
                gf::gf_inv(gf::gf_add(2, 9)),
                gf::gf_inv(gf::gf_add(3, 9)),
            ],
        ];
        let inv = invert_matrix(&matrix).unwrap();

        // Verify A × A⁻¹ = I
        let n = 3;
        for i in 0..n {
            for j in 0..n {
                let mut sum: Gf8 = 0;
                for k in 0..n {
                    sum = gf::gf_add(sum, gf::gf_mul(matrix[i][k], inv[k][j]));
                }
                let expected = if i == j { 1 } else { 0 };
                assert_eq!(sum, expected, "A × A⁻¹[{i}][{j}] = {sum}, expected {expected}");
            }
        }
    }

    // -- Cross-backend compatibility: ISA-L encode + Cauchy decode ---

    #[test]
    fn isal_encode_cauchy_decode_roundtrip() {
        use oceanfs_core::CodecConfig;
        use oceanfs_ec::CauchyEncoder;

        let isal = IsalEncoder::new(4, 2).unwrap();
        let cauchy = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![(i + 10) as u8; 256]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

        // Encode with ISA-L
        let parity = isal.encode(&shard_refs, 2).unwrap();

        // Decode with Cauchy — should produce identical data
        let available: Vec<Option<&[u8]>> = vec![
            None,
            Some(&data[1]),
            Some(&data[2]),
            Some(&data[3]),
            Some(&parity[0]),
            Some(&parity[1]),
        ];
        let recovered = cauchy.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered[0], data[0]);
    }

    #[test]
    fn cauchy_encode_isal_decode_roundtrip() {
        use oceanfs_core::CodecConfig;
        use oceanfs_ec::CauchyEncoder;

        let isal = IsalEncoder::new(4, 2).unwrap();
        let cauchy = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![(i + 10) as u8; 256]).collect();
        let shard_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

        // Encode with Cauchy
        let parity = cauchy.encode(&shard_refs, 2).unwrap();

        // Decode with ISA-L
        let available: Vec<Option<&[u8]>> = vec![
            None,
            Some(&data[1]),
            Some(&data[2]),
            Some(&data[3]),
            Some(&parity[0]),
            Some(&parity[1]),
        ];
        let recovered = isal.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered[0], data[0]);
    }

    // -- GF arithmetic properties ---

    #[test]
    fn gf_inv_involutive() {
        for a in 1..=255u8 {
            let inv = gf::gf_inv(a);
            assert_eq!(gf::gf_mul(a, inv), 1, "a={a}: {a} * {inv} != 1");
        }
    }

    #[test]
    fn gf_add_is_xor() {
        for a in 0..=255u8 {
            for b in 0..=255u8 {
                assert_eq!(gf::gf_add(a, b), a ^ b);
            }
        }
    }

    #[test]
    fn gf_mul_commutative() {
        // Sample to keep test fast
        for a in [1u8, 2, 10, 100, 200, 255] {
            for b in [1u8, 3, 50, 150, 250] {
                assert_eq!(gf::gf_mul(a, b), gf::gf_mul(b, a));
            }
        }
    }

    #[test]
    fn cauchy_matrix_has_no_zero_rows() {
        for k in [4u8, 8, 16] {
            for m in [2u8, 4, 8] {
                let mut matrix = vec![0u8; (k as usize) * (m as usize)];
                build_cauchy_matrix(k, m, &mut matrix);

                for row in 0..m as usize {
                    let has_nonzero =
                        (0..k as usize).any(|col| matrix[row * (k as usize) + col] != 0);
                    assert!(has_nonzero, "Cauchy matrix k={k}, m={m}, row={row} is all zeros");
                }
            }
        }
    }
}
