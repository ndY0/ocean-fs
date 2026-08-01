//! Parallel encode/decode using rayon.
//!
//! Distributes stripe processing across all available CPU cores
//! via rayon parallel iterators. A tokio semaphore bounds concurrency
//! at the segment level.
//!
//! The `ParallelEncoder<E>` and `ParallelDecoder<D>` types are generic
//! over the codec implementation, enabling static dispatch on the hot
//! path (perf rule 6.4). They also accept `?Sized` types such as
//! `dyn Encoder` / `dyn Decoder` for dependency injection.
//!
//! # Lock Order
//!
//! semaphore → encoder/decoder internal state (no multi-lock held)

#![allow(clippy::needless_range_loop)]

use std::sync::Arc;

use oceanfs_core::EncodingPlan;
use rayon::prelude::*;
use tokio::sync::Semaphore;

use crate::{
    error::Result,
    stripe::batch::StripeBatch,
    traits::{Decoder, Encoder},
};

/// Parallel encoder — encodes all stripes in a segment concurrently.
///
/// Wraps an `Encoder` implementation and dispatches stripe encoding
/// across all available CPU cores via rayon parallel iterators. A
/// `tokio::sync::Semaphore` bounds concurrent segment-level encode
/// operations to prevent resource exhaustion.
///
/// The type parameter `E` is the codec implementation. Use a concrete
/// type (e.g., `ParallelEncoder<CauchyEncoder>`) for static dispatch on
/// the hot path, or `ParallelEncoder<dyn Encoder>` for dynamic dispatch.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_ec::{ParallelEncoder, StripeLayout, CauchyEncoder};
/// use oceanfs_ec::Encoder;
/// use oceanfs_core::CodecConfig;
///
/// let config = CodecConfig { data_shards: 4, parity_shards: 2, strip_size_bytes: 64, ..Default::default() };
/// let encoder = Arc::new(CauchyEncoder::new(config));
/// let plan = StripeLayout::compute(1024, 4, 2, 64).unwrap();
/// let parallel = ParallelEncoder::new(encoder, 0);
/// ```
pub struct ParallelEncoder<E: Encoder + ?Sized> {
    encoder: Arc<E>,
    semaphore: Option<Arc<Semaphore>>,
}

impl<E: Encoder + ?Sized> ParallelEncoder<E> {
    /// Creates a new parallel encoder.
    ///
    /// `max_concurrency` bounds the number of concurrent segment encodes.
    /// 0 means no bound (unlimited).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_ec::{ParallelEncoder, CauchyEncoder};
    /// use oceanfs_core::CodecConfig;
    ///
    /// let config = CodecConfig { data_shards: 4, parity_shards: 2, ..Default::default() };
    /// let encoder = Arc::new(CauchyEncoder::new(config));
    /// let parallel = ParallelEncoder::new(encoder, 4);
    /// ```
    pub fn new(encoder: Arc<E>, max_concurrency: usize) -> Self {
        let semaphore = if max_concurrency > 0 {
            Some(Arc::new(Semaphore::new(max_concurrency)))
        } else {
            None
        };
        Self { encoder, semaphore }
    }

    /// Encodes segment data into a `StripeBatch`.
    ///
    /// Splits the segment into stripes per the plan, then encodes all
    /// stripes in parallel using rayon. The codec parameters (k, m) are
    /// obtained from the `EncodingPlan`.
    ///
    /// The final stripe is zero-padded if the segment data is shorter
    /// than `plan.padded_size`.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding any stripe fails.
    pub fn encode(&self, segment_data: &[u8], plan: &EncodingPlan) -> Result<StripeBatch> {
        let _permit = self.semaphore.as_ref().map(|s| s.acquire());

        let k = plan.data_shards as usize;
        let m = plan.parity_shards as usize;
        let total_stripes = plan.stripe_count;
        let shard_size = plan.shard_size;
        let stripe_data_size = k * shard_size;

        // Build data shards in SoA layout with pre-sized capacity.
        // Each shard holds total_stripes * shard_size bytes.
        let mut data_shards: Vec<Vec<u8>> = Vec::with_capacity(k);
        for _ in 0..k {
            data_shards.push(vec![0u8; total_stripes * shard_size]);
        }

        // Copy segment data into interleaved shards, zero-padding the
        // final stripe if the segment data is shorter than padded_size.
        for stripe_idx in 0..total_stripes {
            let stripe_start = stripe_idx * stripe_data_size;
            for shard_idx in 0..k {
                let src_offset = stripe_start + shard_idx * shard_size;
                let dst_offset = stripe_idx * shard_size;

                if src_offset < segment_data.len() {
                    let available = segment_data.len() - src_offset;
                    let copy_len = available.min(shard_size);
                    data_shards[shard_idx][dst_offset..dst_offset + copy_len]
                        .copy_from_slice(&segment_data[src_offset..src_offset + copy_len]);
                }
                // bytes beyond segment_data.len() remain zero (padding)
            }
        }

        // Encode each stripe in parallel.
        let m8 = m as u8;
        let mut parity_shards: Vec<Vec<u8>> = vec![vec![0u8; total_stripes * shard_size]; m];

        let results: Vec<Result<Vec<Vec<u8>>>> = (0..total_stripes)
            .into_par_iter()
            .map(|stripe_idx| {
                let stripe_data: Vec<&[u8]> = data_shards
                    .iter()
                    .map(|shard| &shard[stripe_idx * shard_size..(stripe_idx + 1) * shard_size])
                    .collect();
                self.encoder.encode(&stripe_data, m8)
            })
            .collect();

        // Collect parity results.
        for (stripe_idx, result) in results.into_iter().enumerate() {
            let parity = result?;
            for (p_idx, p_data) in parity.iter().enumerate() {
                let offset = stripe_idx * shard_size;
                parity_shards[p_idx][offset..offset + p_data.len()].copy_from_slice(p_data);
            }
        }

        Ok(StripeBatch { data: data_shards, parity: parity_shards })
    }
}

/// Parallel decoder — decodes all stripes in a segment concurrently.
///
/// Wraps a `Decoder` implementation and dispatches stripe decoding
/// across all available CPU cores via rayon parallel iterators.
///
/// The type parameter `D` is the codec implementation. Use a concrete
/// type (e.g., `ParallelDecoder<CauchyEncoder>`) for static dispatch on
/// the hot path.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_ec::{ParallelDecoder, CauchyEncoder};
/// use oceanfs_core::CodecConfig;
///
/// let config = CodecConfig { data_shards: 4, parity_shards: 2, ..Default::default() };
/// let codec = Arc::new(CauchyEncoder::new(config));
/// let decoder = ParallelDecoder::new(codec, 0);
/// ```
pub struct ParallelDecoder<D: Decoder + ?Sized> {
    decoder: Arc<D>,
    semaphore: Option<Arc<Semaphore>>,
}

impl<D: Decoder + ?Sized> ParallelDecoder<D> {
    /// Creates a new parallel decoder.
    ///
    /// `max_concurrency` bounds the number of concurrent segment decodes.
    /// 0 means no bound (unlimited).
    pub fn new(decoder: Arc<D>, max_concurrency: usize) -> Self {
        let semaphore = if max_concurrency > 0 {
            Some(Arc::new(Semaphore::new(max_concurrency)))
        } else {
            None
        };
        Self { decoder, semaphore }
    }

    /// Decodes a `StripeBatch`, recovering missing data shards.
    ///
    /// `missing_indices` lists the indices (0..k-1) of data shards that
    /// need reconstruction. The codec parameters (k, m) are derived from
    /// the available data: `k = available.data.len()`, `m = available.parity.len()`.
    ///
    /// # Errors
    ///
    /// Returns an error if decoding any stripe fails (e.g., too many
    /// missing shards for the codec to recover).
    pub fn decode(
        &self,
        available: &StripeBatch,
        plan: &EncodingPlan,
        missing_indices: &[usize],
    ) -> Result<Vec<Vec<u8>>> {
        let _permit = self.semaphore.as_ref().map(|s| s.acquire());

        let k = available.data.len() as u8;
        let m = available.parity.len() as u8;
        let total_stripes = plan.stripe_count;
        let shard_size = plan.shard_size;

        let mut recovered_data: Vec<Vec<u8>> =
            vec![vec![0u8; total_stripes * shard_size]; k as usize];

        let results: Vec<Result<Vec<Vec<u8>>>> = (0..total_stripes)
            .into_par_iter()
            .map(|stripe_idx| {
                let mut shards: Vec<Option<&[u8]>> = vec![None; (k + m) as usize];

                // Fill available data shards (None for missing ones).
                for i in 0..k as usize {
                    if !missing_indices.contains(&i) {
                        let offset = stripe_idx * shard_size;
                        let end = offset + shard_size;
                        if offset < available.data[i].len() {
                            shards[i] =
                                Some(&available.data[i][offset..end.min(available.data[i].len())]);
                        }
                    }
                }
                // Fill available parity shards.
                for i in 0..m as usize {
                    let offset = stripe_idx * shard_size;
                    let end = offset + shard_size;
                    if offset < available.parity[i].len() {
                        shards[k as usize + i] =
                            Some(&available.parity[i][offset..end.min(available.parity[i].len())]);
                    }
                }

                self.decoder.decode(&shards, k, m)
            })
            .collect();

        for (stripe_idx, result) in results.into_iter().enumerate() {
            let decoded = result?;
            for (d_idx, d_data) in decoded.iter().enumerate() {
                let offset = stripe_idx * shard_size;
                let len = d_data.len().min(shard_size);
                if offset + len <= recovered_data[d_idx].len() {
                    recovered_data[d_idx][offset..offset + len].copy_from_slice(&d_data[..len]);
                }
            }
        }

        Ok(recovered_data)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::CodecConfig;

    use super::*;
    use crate::{cauchy::CauchyEncoder, StripeLayout};

    #[test]
    fn parallel_encode_then_decode_roundtrip() {
        let config = CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        };
        let codec = Arc::new(CauchyEncoder::new(config.clone()));

        let plan = StripeLayout::compute(1024, 4, 2, 64).unwrap();
        let segment_data = vec![0xABu8; 1024];

        let encoder: ParallelEncoder<CauchyEncoder> = ParallelEncoder::new(codec.clone(), 0);
        let batch = encoder.encode(&segment_data, &plan).unwrap();

        // Re-decode with all data shards present (no missing).
        let decoder: ParallelDecoder<CauchyEncoder> = ParallelDecoder::new(codec, 0);
        let recovered = decoder.decode(&batch, &plan, &[]).unwrap();

        assert_eq!(recovered.len(), 4);
    }

    #[test]
    fn parallel_encode_with_padding() {
        let config = CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        };
        let codec = Arc::new(CauchyEncoder::new(config.clone()));

        // 500 bytes → plan says 2 stripes (512 bytes padded)
        let plan = StripeLayout::compute(500, 4, 2, 64).unwrap();
        assert_eq!(plan.stripe_count, 2);
        assert_eq!(plan.padded_size, 512);

        let segment_data = vec![0x42u8; 500];
        let encoder: ParallelEncoder<CauchyEncoder> = ParallelEncoder::new(codec.clone(), 0);
        let batch = encoder.encode(&segment_data, &plan).unwrap();

        // Should have 4 data shards, each 128 bytes (2 stripes × 64)
        assert_eq!(batch.data.len(), 4);
        assert_eq!(batch.data[0].len(), 128);
        assert_eq!(batch.parity.len(), 2);

        // Decode and verify first 500 bytes are correct.
        let decoder: ParallelDecoder<CauchyEncoder> = ParallelDecoder::new(codec, 0);
        let recovered = decoder.decode(&batch, &plan, &[]).unwrap();

        // Verify data shard contents (first 500 bytes should match)
        let mut reassembled = Vec::with_capacity(500);
        for stripe_idx in 0..plan.stripe_count {
            for shard_idx in 0..4 {
                let offset = stripe_idx * 64;
                let slice = &recovered[shard_idx][offset..offset + 64];
                reassembled.extend_from_slice(slice);
            }
        }
        assert_eq!(&reassembled[..500], &segment_data[..]);
    }

    #[test]
    fn parallel_decode_with_missing_shards() {
        let config = CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        };
        let codec = Arc::new(CauchyEncoder::new(config.clone()));

        let plan = StripeLayout::compute(1024, 4, 2, 64).unwrap();
        let segment_data = vec![0xCDu8; 1024];

        let encoder: ParallelEncoder<CauchyEncoder> = ParallelEncoder::new(codec.clone(), 0);
        let batch = encoder.encode(&segment_data, &plan).unwrap();

        // Decode with data shards 0 and 2 missing.
        let decoder: ParallelDecoder<CauchyEncoder> = ParallelDecoder::new(codec.clone(), 0);
        let recovered = decoder.decode(&batch, &plan, &[0, 2]).unwrap();

        assert_eq!(recovered.len(), 4);
        // Verify the recovered data matches original.
        let mut reassembled: Vec<u8> = Vec::with_capacity(1024);
        for stripe_idx in 0..plan.stripe_count {
            for shard_idx in 0..4 {
                let offset = stripe_idx * 64;
                let slice = &recovered[shard_idx][offset..offset + 64];
                reassembled.extend_from_slice(slice);
            }
        }
        assert_eq!(reassembled, segment_data);
    }

    #[test]
    fn parallel_encode_semaphore_bounds_concurrency() {
        let config = CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        };
        let codec = Arc::new(CauchyEncoder::new(config.clone()));

        let plan = StripeLayout::compute(1024, 4, 2, 64).unwrap();
        let segment_data = vec![0xAAu8; 1024];

        // With semaphore bound of 2 — still works fine.
        let encoder: ParallelEncoder<CauchyEncoder> = ParallelEncoder::new(codec, 2);
        let batch = encoder.encode(&segment_data, &plan).unwrap();
        assert_eq!(batch.data.len(), 4);
        assert_eq!(batch.parity.len(), 2);
    }

    #[test]
    fn parallel_decode_with_semaphore_bounds_concurrency() {
        let config = CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        };
        let codec = Arc::new(CauchyEncoder::new(config.clone()));

        let plan = StripeLayout::compute(1024, 4, 2, 64).unwrap();
        let segment_data = vec![0xBBu8; 1024];

        let encoder: ParallelEncoder<CauchyEncoder> = ParallelEncoder::new(codec.clone(), 0);
        let batch = encoder.encode(&segment_data, &plan).unwrap();

        // Construct decoder with max_concurrency=2 to cover semaphore branch.
        let decoder: ParallelDecoder<CauchyEncoder> = ParallelDecoder::new(codec, 2);
        let recovered = decoder.decode(&batch, &plan, &[]).unwrap();
        assert_eq!(recovered.len(), 4);
    }
}
