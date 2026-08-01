//! Cauchy Reed-Solomon erasure coding over GF(2^8).
//!
//! Uses Cauchy matrices for efficient encoding and decoding. The
//! generator matrix is [I_k | C^T] where C is the Cauchy matrix.

#![allow(clippy::needless_range_loop)]

use oceanfs_core::CodecConfig;

use crate::{
    error::{Error, Result},
    gf,
    traits::{Decoder, Encoder},
};

/// A Cauchy Reed-Solomon encoder/decoder.
///
/// # Examples
///
/// ```
/// use oceanfs_core::CodecConfig;
/// use oceanfs_ec::CauchyEncoder;
/// use oceanfs_ec::{Encoder, Decoder};
///
/// let config = CodecConfig { data_shards: 4, parity_shards: 2, ..Default::default() };
/// let codec = CauchyEncoder::new(config);
///
/// let data: &[&[u8]] = &[b"aaaa", b"bbbb", b"cccc", b"dddd"];
/// let parity = codec.encode(data, 2).unwrap();
/// assert_eq!(parity.len(), 2);
/// ```
pub struct CauchyEncoder {
    k: u8,
    m: u8,
}

impl CauchyEncoder {
    /// Creates a new Cauchy encoder with the given configuration.
    pub fn new(config: CodecConfig) -> Self {
        Self { k: config.data_shards, m: config.parity_shards }
    }

    /// Returns the number of data shards (k).
    pub fn data_shards(&self) -> u8 {
        self.k
    }

    /// Returns the number of parity shards (m).
    pub fn parity_shards(&self) -> u8 {
        self.m
    }

    /// Generates the Cauchy matrix for (k, m).
    fn cauchy_matrix(k: u8, m: u8) -> Vec<Vec<gf::Gf8>> {
        let ki = k as usize;
        let mi = m as usize;

        // Distinct elements for Cauchy construction: X = [1..k], Y = [k+1..k+m]
        let mut matrix = vec![vec![0u8; ki]; mi];
        for i in 0..mi {
            for j in 0..ki {
                let x = (j + 1) as u8;
                let y = (ki + i + 1) as u8;
                matrix[i][j] = gf::gf_inv(gf::gf_add(x, y));
            }
        }
        matrix
    }

    /// Builds the generator matrix G (k+m)×k.
    /// Top k rows: identity. Bottom m rows: Cauchy matrix.
    fn generator_matrix(k: u8, m: u8) -> Vec<Vec<gf::Gf8>> {
        let ki = k as usize;
        let mi = m as usize;
        let cauchy = Self::cauchy_matrix(k, m);

        let mut g = vec![vec![0u8; ki]; ki + mi];

        // Identity block (top k rows).
        for i in 0..ki {
            g[i][i] = 1;
        }

        // Cauchy block (bottom m rows).
        for i in 0..mi {
            for j in 0..ki {
                g[ki + i][j] = cauchy[i][j];
            }
        }

        g
    }

    /// Encodes data shards into parity shards using the Cauchy matrix.
    fn encode_cauchy(k: u8, m: u8, data_shards: &[&[u8]]) -> Result<Vec<Vec<u8>>> {
        let ki = k as usize;
        let mi = m as usize;
        let cm = Self::cauchy_matrix(k, m);
        let shard_size = data_shards[0].len();

        let mut parity: Vec<Vec<u8>> = (0..mi).map(|_| vec![0u8; shard_size]).collect();

        for i in 0..mi {
            for byte_idx in 0..shard_size {
                let mut sum: gf::Gf8 = 0;
                for j in 0..ki {
                    sum = gf::gf_add(sum, gf::gf_mul(cm[i][j], data_shards[j][byte_idx]));
                }
                parity[i][byte_idx] = sum;
            }
        }

        Ok(parity)
    }

    /// Inverts a square matrix over GF(2^8) using Gauss-Jordan elimination.
    fn invert_matrix(matrix: &[Vec<gf::Gf8>]) -> Option<Vec<Vec<gf::Gf8>>> {
        let n = matrix.len();
        // Augmented matrix [A | I].
        let mut aug: Vec<Vec<gf::Gf8>> = vec![vec![0u8; 2 * n]; n];
        for i in 0..n {
            for j in 0..n {
                aug[i][j] = matrix[i][j];
            }
            aug[i][n + i] = 1;
        }

        // Forward elimination.
        for col in 0..n {
            // Find pivot.
            let mut pivot_row = col;
            while pivot_row < n && aug[pivot_row][col] == 0 {
                pivot_row += 1;
            }
            if pivot_row == n {
                return None; // singular
            }
            aug.swap(col, pivot_row);

            // Normalize pivot row.
            let inv = gf::gf_inv(aug[col][col]);
            for j in 0..2 * n {
                aug[col][j] = gf::gf_mul(aug[col][j], inv);
            }

            // Eliminate other rows.
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

        // Extract right half.
        let mut inv = vec![vec![0u8; n]; n];
        for i in 0..n {
            for j in 0..n {
                inv[i][j] = aug[i][n + j];
            }
        }
        Some(inv)
    }
}

impl Encoder for CauchyEncoder {
    fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> Result<Vec<Vec<u8>>> {
        if data_shards.len() != self.k as usize {
            return Err(Error::InvalidConfig(format!(
                "expected {} data shards, got {}",
                self.k,
                data_shards.len()
            )));
        }
        if data_shards.is_empty() {
            return Ok(Vec::new());
        }

        let shard_size = data_shards[0].len();
        for shard in data_shards.iter() {
            if shard.len() != shard_size {
                return Err(Error::ShardSizeMismatch { expected: shard_size, actual: shard.len() });
            }
        }

        Self::encode_cauchy(self.k, parity_count, data_shards)
    }
}

impl Decoder for CauchyEncoder {
    fn decode(
        &self,
        available_shards: &[Option<&[u8]>],
        data_count: u8,
        parity_count: u8,
    ) -> Result<Vec<Vec<u8>>> {
        let k = data_count as usize;
        let m = parity_count as usize;
        let total = k + m;

        if available_shards.len() != total {
            return Err(Error::InvalidConfig(format!(
                "expected {} available shards, got {}",
                total,
                available_shards.len()
            )));
        }

        // Count available shards and identify which ones.
        let present: Vec<usize> = (0..total).filter(|&i| available_shards[i].is_some()).collect();
        if present.len() < k {
            return Err(Error::NotEnoughShards { needed: k, available: present.len() });
        }

        // Determine shard size from first available shard.
        let first_shard = available_shards[present[0]]
            .ok_or_else(|| Error::InvalidConfig("first shard is None".into()))?;
        let shard_size = first_shard.len();

        // Build generator matrix G.
        let gen = Self::generator_matrix(data_count, parity_count);

        // Extract surviving rows of G corresponding to available shards.
        let mut sub_matrix: Vec<Vec<gf::Gf8>> = Vec::with_capacity(k);
        let mut sub_data: Vec<&[u8]> = Vec::with_capacity(k);
        let mut selected_indices = Vec::with_capacity(k);

        for &idx in present.iter().take(k) {
            sub_matrix.push(gen[idx].clone());
            let shard = available_shards[idx]
                .ok_or_else(|| Error::InvalidConfig(format!("shard {idx} is None")))?;
            sub_data.push(shard);
            selected_indices.push(idx);
        }

        // Invert sub-matrix.
        let inv = Self::invert_matrix(&sub_matrix)
            .ok_or_else(|| Error::DecodingFailed("matrix is singular".into()))?;

        // Reconstruct data shards: data[i] = sum_j inv[i][j] * sub_data[j].
        let mut recovered: Vec<Vec<u8>> = (0..k).map(|_| vec![0u8; shard_size]).collect();

        for i in 0..k {
            for byte_idx in 0..shard_size {
                let mut sum: gf::Gf8 = 0;
                for j in 0..k {
                    sum = gf::gf_add(sum, gf::gf_mul(inv[i][j], sub_data[j][byte_idx]));
                }
                recovered[i][byte_idx] = sum;
            }
        }

        // Recovered data is the original k data shards in order.
        Ok(recovered)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip_k4_m2() {
        let codec = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i as u8 + 1; 1024]).collect();
        let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = codec.encode(&data_refs, 2).unwrap();

        // Build available shards: lose shard 0 and shard 2.
        let available: Vec<Option<&[u8]>> =
            vec![None, Some(&data[1]), None, Some(&data[3]), Some(&parity[0]), Some(&parity[1])];

        let recovered = codec.decode(&available, 4, 2).unwrap();
        assert_eq!(recovered[0], data[0]);
        assert_eq!(recovered[1], data[1]);
        assert_eq!(recovered[2], data[2]);
        assert_eq!(recovered[3], data[3]);
    }

    #[test]
    fn encode_decode_k1_m0() {
        let codec = CauchyEncoder::new(CodecConfig {
            data_shards: 1,
            parity_shards: 0,
            ..Default::default()
        });
        let data = [&b"hello"[..]];
        let parity = codec.encode(&data, 0).unwrap();
        assert!(parity.is_empty());
    }

    #[test]
    fn decode_needs_at_least_k_shards() {
        let codec = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });
        let available: Vec<Option<&[u8]>> = vec![Some(b"a"), None, None, Some(b"d"), None, None];
        let result = codec.decode(&available, 4, 2);
        assert!(result.is_err());
    }

    #[test]
    fn encode_with_mismatched_sizes_errors() {
        let codec = CauchyEncoder::new(CodecConfig {
            data_shards: 2,
            parity_shards: 1,
            ..Default::default()
        });
        let data = [&b"short"[..], &b"longer"[..]];
        let result = codec.encode(&data, 1);
        assert!(result.is_err());
    }

    #[test]
    fn data_shards_getter_returns_k() {
        let codec = CauchyEncoder::new(CodecConfig {
            data_shards: 7,
            parity_shards: 3,
            ..Default::default()
        });
        assert_eq!(codec.data_shards(), 7);
        assert_eq!(codec.parity_shards(), 3);
    }

    #[test]
    fn encode_with_wrong_shard_count_errors() {
        let codec = CauchyEncoder::new(CodecConfig {
            data_shards: 3,
            parity_shards: 1,
            ..Default::default()
        });
        // Provide 2 data shards instead of 3.
        let data = [&b"a"[..], &b"b"[..]];
        let result = codec.encode(&data, 1);
        assert!(result.is_err());
    }

    #[test]
    fn encode_with_empty_data_shards_returns_empty() {
        let codec = CauchyEncoder::new(CodecConfig {
            data_shards: 0, // k=0 allows empty shards
            parity_shards: 2,
            ..Default::default()
        });
        let empty: &[&[u8]] = &[];
        let parity = codec.encode(empty, 2).unwrap();
        assert!(parity.is_empty());
    }

    #[test]
    fn decode_with_wrong_available_count_errors() {
        let codec = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });
        // Provide 5 available shards instead of 6 (k + m).
        let available: Vec<Option<&[u8]>> =
            vec![Some(b"a"), Some(b"b"), Some(b"c"), Some(b"d"), Some(b"e")];
        let result = codec.decode(&available, 4, 2);
        assert!(result.is_err());
    }

    // ------------------------------------------------------------------
    // Property-based tests (proptest)
    // ------------------------------------------------------------------
    mod proptests {
        use proptest::prelude::*;

        use super::*;

        proptest! {
            /// Round-trip: encode then decode should recover original data.
            ///
            /// Tests random data sizes from 1 to 8192 bytes, k in [1,8],
            /// and m in [1,4]. Up to m shards may be missing.
            #[test]
            fn roundtrip_encode_decode_recover_original(
                size in 1usize..4096,
                k in 1u8..8,
                m in 1u8..4,
                seed in any::<u64>(),
                missing_count in 0usize..3,
            ) {
                // Generate deterministic pseudo-random data from seed.
                let shard_size = size;
                let data: Vec<Vec<u8>> = (0..k as usize)
                    .map(|shard_idx| {
                        (0..shard_size)
                            .map(|i| {
                                ((seed.wrapping_mul(17 + shard_idx as u64)
                                    .wrapping_add(i as u64 * 13)) % 251) as u8
                            })
                            .collect()
                    })
                    .collect();

                let codec = CauchyEncoder::new(CodecConfig {
                    data_shards: k,
                    parity_shards: m,
                    strip_size_bytes: shard_size,
                    ..Default::default()
                });

                let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
                let parity = codec.encode(&data_refs, m).unwrap();
                assert_eq!(parity.len(), m as usize);

                // Build available shards: drop up to m data shards.
                let total = (k + m) as usize;
                let actual_missing = missing_count.min(m as usize).min(k as usize);

                let available: Vec<Option<&[u8]>> = (0..total)
                    .map(|i| {
                        if i < k as usize {
                            if i < actual_missing {
                                None // missing data shard
                            } else {
                                Some(data_refs[i])
                            }
                        } else {
                            Some(parity[i - k as usize].as_slice())
                        }
                    })
                    .collect();

                let recovered = codec.decode(&available, k, m).unwrap();

                // Verify recovered data matches original.
                for i in 0..k as usize {
                    assert_eq!(recovered[i], data[i],
                        "data shard {i} mismatch (k={k}, m={m}, size={shard_size})");
                }
            }

            /// Cauchy matrix should be non-zero for any (k, m).
            #[test]
            fn cauchy_matrix_is_invertible(k in 1u8..16, m in 1u8..8) {
                let matrix = CauchyEncoder::cauchy_matrix(k, m);
                let ki = k as usize;
                let mi = m as usize;

                // Check that each row has at least one non-zero entry.
                for i in 0..mi {
                    let has_nonzero = (0..ki).any(|j| matrix[i][j] != 0);
                    assert!(has_nonzero, "Cauchy matrix row {i} is all zeros (k={k}, m={m})");
                }
            }

            /// Encode with random data should produce parity shards of correct size.
            #[test]
            fn encode_produces_correct_parity_count(
                size in 1usize..4096,
                k in 1u8..8,
                m in 0u8..6,
            ) {
                let codec = CauchyEncoder::new(CodecConfig {
                    data_shards: k,
                    parity_shards: m,
                    strip_size_bytes: size,
                    ..Default::default()
                });

                let data: Vec<Vec<u8>> = (0..k as usize)
                    .map(|_| vec![0xAAu8; size])
                    .collect();
                let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

                let parity = codec.encode(&data_refs, m).unwrap();
                assert_eq!(parity.len(), m as usize);

                for p in &parity {
                    assert_eq!(p.len(), size);
                }
            }

            /// Encode with m=0 should return empty parity vector.
            #[test]
            fn encode_m0_returns_empty(k in 1u8..16, size in 1usize..4096) {
                let codec = CauchyEncoder::new(CodecConfig {
                    data_shards: k,
                    parity_shards: 0,
                    strip_size_bytes: size,
                    ..Default::default()
                });

                let data: Vec<Vec<u8>> = (0..k as usize)
                    .map(|_| vec![0x55u8; size])
                    .collect();
                let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

                let parity = codec.encode(&data_refs, 0).unwrap();
                assert!(parity.is_empty());
            }
        }
    }
}
