//! Compile-time precomputed Cauchy encode matrices.
//!
//! For common (k,m) pairs, the Cauchy RS encode matrix is stored as a `const`
//! array — eliminating the ~30-100µs of runtime GF(2⁸) computation per segment
//! encode (computing the Cauchy matrix, inverses of (X⊕Y) for each element).
//!
//! For uncommon (k,m) pairs, the caller falls back to runtime computation.
//!
//! ## Supported Pairs
//!
//! | (k, m) | Matrix Size | Use Case |
//! |---|---|---|
//! | (4, 2) | 2 × 4 = 8 bytes | Small segments, low overhead |
//! | (6, 3) | 3 × 6 = 18 bytes | Balanced reliability |
//! | (8, 4) | 4 × 8 = 32 bytes | High durability |
//! | (10, 6) | 6 × 10 = 60 bytes | Maximum redundancy |

// ---------------------------------------------------------------------------
// Precomputed matrices — generated from `gf_inv(gf_add(x, y))` with
// X = [1..k], Y = [k+1..k+m], primitive polynomial 0x11D.
// ---------------------------------------------------------------------------

/// Precomputed Cauchy encode matrix for (k=4, m=2).
///
/// Dimension: 2×4 (parity rows × data columns).
pub(crate) const CAUCHY_MATRIX_4_2: [[u8; 4]; 2] =
    [[0x47, 0xba, 0x7a, 0x01], [0xba, 0x47, 0xa7, 0x8e]];

/// Precomputed Cauchy encode matrix for (k=6, m=3).
///
/// Dimension: 3×6 (parity rows × data columns).
pub(crate) const CAUCHY_MATRIX_6_3: [[u8; 6]; 3] = [
    [0x7a, 0xa7, 0x47, 0xf4, 0x8e, 0x01],
    [0x9d, 0xdd, 0x98, 0x3d, 0xaa, 0x5d],
    [0xad, 0x98, 0xdd, 0xaa, 0x3d, 0x96],
];

/// Precomputed Cauchy encode matrix for (k=8, m=4).
///
/// Dimension: 4×8 (parity rows × data columns).
pub(crate) const CAUCHY_MATRIX_8_4: [[u8; 8]; 4] = [
    [0xad, 0x98, 0xdd, 0xaa, 0x3d, 0x96, 0x5d, 0x01],
    [0x98, 0xad, 0x9d, 0x5d, 0x96, 0x3d, 0xaa, 0x8e],
    [0xdd, 0x9d, 0xad, 0x96, 0x5d, 0xaa, 0x3d, 0xf4],
    [0xaa, 0x5d, 0x96, 0xad, 0x9d, 0xdd, 0x98, 0x47],
];

/// Precomputed Cauchy encode matrix for (k=10, m=6).
///
/// Dimension: 6×10 (parity rows × data columns).
pub(crate) const CAUCHY_MATRIX_10_6: [[u8; 10]; 6] = [
    [0xdd, 0x9d, 0xad, 0x96, 0x5d, 0xaa, 0x3d, 0xf4, 0x8e, 0x01],
    [0xaa, 0x5d, 0x96, 0xad, 0x9d, 0xdd, 0x98, 0x47, 0xa7, 0x7a],
    [0x3d, 0x96, 0x5d, 0x9d, 0xad, 0x98, 0xdd, 0xa7, 0x47, 0xba],
    [0x96, 0x3d, 0xaa, 0xdd, 0x98, 0xad, 0x9d, 0x7a, 0xba, 0x47],
    [0x5d, 0xaa, 0x3d, 0x98, 0xdd, 0x9d, 0xad, 0xba, 0x7a, 0xa7],
    [0x72, 0xc0, 0x58, 0xe0, 0x3e, 0x4c, 0x66, 0x90, 0xde, 0x55],
];

// ---------------------------------------------------------------------------
// Accessor — returns a flat `&[u8]` view of the precomputed matrix, or
// `None` if (k,m) is not one of the supported pairs.
// ---------------------------------------------------------------------------

/// Returns a reference to a precomputed Cauchy encode matrix for the given
/// (k, m) pair, or `None` if the pair is not among the supported presets.
///
/// The returned slice is a flat row-major view: `matrix[i * k + j]` accesses
/// the coefficient at parity row `i`, data column `j`.
///
/// # Examples
///
/// ```
/// use oceanfs_ec::matrix::get_const_cauchy_matrix;
///
/// // (4, 2) is supported — returns the const matrix.
/// let cm = get_const_cauchy_matrix(4, 2).expect("(4,2) should be precomputed");
/// // cm[0*4 + 3] = element at parity row 0, data column 3
/// assert_eq!(cm.len(), 8); // 2 rows × 4 columns
///
/// // (5, 3) is not supported — returns None.
/// assert!(get_const_cauchy_matrix(5, 3).is_none());
/// ```
pub fn get_const_cauchy_matrix(k: u8, m: u8) -> Option<&'static [u8]> {
    match (k, m) {
        (4, 2) => Some(flatten::<4, 2>(&CAUCHY_MATRIX_4_2)),
        (6, 3) => Some(flatten::<6, 3>(&CAUCHY_MATRIX_6_3)),
        (8, 4) => Some(flatten::<8, 4>(&CAUCHY_MATRIX_8_4)),
        (10, 6) => Some(flatten::<10, 6>(&CAUCHY_MATRIX_10_6)),
        _ => None,
    }
}

/// Returns the list of (k, m) pairs that have precomputed const matrices.
///
/// Used in tests to iterate over all supported configurations.
#[cfg_attr(not(test), allow(dead_code))]
fn supported_pairs() -> &'static [(u8, u8)] {
    &[(4, 2), (6, 3), (8, 4), (10, 6)]
}

// ---------------------------------------------------------------------------
// Helper — reinterpret a `[[u8; K]; M]` as a flat `&[u8]` of length `K*M`.
// ---------------------------------------------------------------------------

/// Returns a flat row-major slice view of an `M × K` matrix.
///
/// `[[u8; K]; M]` has a guaranteed contiguous layout in Rust (elements are
/// laid out in row-major order without padding between rows). This function
/// reinterprets the 2D `const` array as a 1D slice.
#[inline]
fn flatten<const K: usize, const M: usize>(matrix: &[[u8; K]; M]) -> &[u8] {
    let ptr = matrix.as_ptr() as *const u8;
    // SAFETY: Rust guarantees [[u8; K]; M] is laid out as [u8; K*M]
    // contiguously in memory, with no padding between elements.
    // The pointer is valid for reads of K * M bytes, and the lifetime
    // is tied to the input reference.
    unsafe { std::slice::from_raw_parts(ptr, K * M) }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Every supported pair must have a non-empty const matrix.
    #[test]
    fn all_supported_pairs_return_some() {
        for &(k, m) in supported_pairs() {
            let cm = get_const_cauchy_matrix(k, m);
            assert!(cm.is_some(), "({k}, {m}) should be supported");
            let cm = cm.unwrap();
            assert_eq!(cm.len(), (k as usize) * (m as usize));
        }
    }

    /// Unsupported pairs return `None`.
    #[test]
    fn unsupported_pairs_return_none() {
        assert!(get_const_cauchy_matrix(1, 1).is_none());
        assert!(get_const_cauchy_matrix(3, 2).is_none());
        assert!(get_const_cauchy_matrix(5, 3).is_none());
        assert!(get_const_cauchy_matrix(7, 4).is_none());
        assert!(get_const_cauchy_matrix(12, 8).is_none());
    }

    /// The const matrix values must match the runtime-computed Cauchy matrix
    /// for every supported (k, m) pair.
    #[test]
    fn const_matrix_matches_runtime() {
        // Import the runtime computation (private, allowed in test).
        use crate::cauchy::CauchyEncoder;

        for &(k, m) in supported_pairs() {
            let const_cm = get_const_cauchy_matrix(k, m).unwrap();
            let runtime_cm = CauchyEncoder::runtime_cauchy_matrix(k, m);

            let ki = k as usize;
            let mi = m as usize;

            for i in 0..mi {
                for j in 0..ki {
                    let const_val = const_cm[i * ki + j];
                    let runtime_val = runtime_cm[i][j];
                    assert_eq!(
                        const_val, runtime_val,
                        "mismatch at (k={k}, m={m}, row={i}, col={j}): \
                         const=0x{const_val:02x}, runtime=0x{runtime_val:02x}"
                    );
                }
            }
        }
    }

    /// The flatten helper must produce correct row-major layout.
    #[test]
    fn flatten_is_row_major() {
        let m: [[u8; 3]; 2] = [[1, 2, 3], [4, 5, 6]];
        let flat = flatten(&m);
        assert_eq!(flat, &[1, 2, 3, 4, 5, 6]);
    }

    /// Each element of every const matrix must be non-zero (Cauchy matrices
    /// have no zero entries by construction).
    #[test]
    fn const_matrices_have_no_zero_entries() {
        for &(k, m) in supported_pairs() {
            let cm = get_const_cauchy_matrix(k, m).unwrap();
            for (idx, &val) in cm.iter().enumerate() {
                let row = idx / k as usize;
                let col = idx % k as usize;
                assert_ne!(val, 0, "zero entry at (k={k}, m={m}, row={row}, col={col})");
            }
        }
    }
}
