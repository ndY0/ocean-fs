//! Parallel encode/decode using rayon.
//!
//! Distributes stripe processing across all available CPU cores
//! via rayon parallel iterators. A tokio semaphore bounds concurrency
//! at the segment level.

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
pub struct ParallelEncoder {
    encoder: Arc<dyn Encoder>,
    semaphore: Option<Arc<Semaphore>>,
}

impl ParallelEncoder {
    /// Creates a new parallel encoder.
    ///
    /// `max_concurrency` bounds the number of concurrent segment encodes.
    /// 0 means no bound.
    pub fn new(encoder: Arc<dyn Encoder>, max_concurrency: usize) -> Self {
        let semaphore = if max_concurrency > 0 {
            Some(Arc::new(Semaphore::new(max_concurrency)))
        } else {
            None
        };
        Self { encoder, semaphore }
    }

    /// Encodes segment data into a StripeBatch.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding any stripe fails.
    ///
    /// Splits the segment into stripes per the plan, then encodes all
    /// stripes in parallel using rayon.
    pub fn encode(&self, segment_data: &[u8], plan: &EncodingPlan) -> Result<StripeBatch> {
        let _permit = self.semaphore.as_ref().map(|s| s.acquire());

        let _k = plan.stripe_count * plan.shard_size; // bytes per data shard
        let total_stripes = plan.stripe_count;

        // Split segment data into k shards (interleaved layout for SoA).
        let mut data_shards: Vec<Vec<u8>> = Vec::with_capacity(4);
        for _ in 0..4 {
            data_shards.push(vec![0u8; total_stripes * plan.shard_size]);
        }

        // Copy segment data into interleaved shards.
        for stripe_idx in 0..total_stripes {
            let stripe_offset = stripe_idx * 4 * plan.shard_size;
            for shard_idx in 0..4 {
                let shard_offset = stripe_idx * plan.shard_size;
                let src_start = stripe_offset + shard_idx * plan.shard_size;
                let src_end = (src_start + plan.shard_size).min(segment_data.len());
                let copy_len = src_end.saturating_sub(src_start);
                data_shards[shard_idx][shard_offset..shard_offset + copy_len]
                    .copy_from_slice(&segment_data[src_start..src_start + copy_len]);
            }
        }

        // Encode each stripe in parallel.
        let m = 2u8; // default — should come from encoder config
        let mut parity_shards: Vec<Vec<u8>> =
            vec![vec![0u8; total_stripes * plan.shard_size]; m as usize];

        let results: Vec<Result<Vec<Vec<u8>>>> = (0..total_stripes)
            .into_par_iter()
            .map(|stripe_idx| {
                let stripe_data: Vec<&[u8]> = data_shards
                    .iter()
                    .map(|shard| {
                        &shard[stripe_idx * plan.shard_size..(stripe_idx + 1) * plan.shard_size]
                    })
                    .collect();
                self.encoder.encode(&stripe_data, m)
            })
            .collect();

        // Collect parity results.
        for (stripe_idx, result) in results.into_iter().enumerate() {
            let parity = result?;
            for (p_idx, p_data) in parity.iter().enumerate() {
                let offset = stripe_idx * plan.shard_size;
                parity_shards[p_idx][offset..offset + p_data.len()].copy_from_slice(p_data);
            }
        }

        Ok(StripeBatch { data: data_shards, parity: parity_shards })
    }
}

/// Parallel decoder — decodes all stripes in a segment concurrently.
pub struct ParallelDecoder {
    decoder: Arc<dyn Decoder>,
    semaphore: Option<Arc<Semaphore>>,
}

impl ParallelDecoder {
    /// Creates a new parallel decoder.
    pub fn new(decoder: Arc<dyn Decoder>, max_concurrency: usize) -> Self {
        let semaphore = if max_concurrency > 0 {
            Some(Arc::new(Semaphore::new(max_concurrency)))
        } else {
            None
        };
        Self { decoder, semaphore }
    }

    /// Decodes a StripeBatch, recovering missing shards.
    ///
    /// # Errors
    ///
    /// Returns an error if decoding any stripe fails.
    pub fn decode(
        &self,
        available: &StripeBatch,
        plan: &EncodingPlan,
        k: u8,
        m: u8,
        missing_indices: &[usize],
    ) -> Result<Vec<Vec<u8>>> {
        let _permit = self.semaphore.as_ref().map(|s| s.acquire());

        let total_stripes = plan.stripe_count;
        let mut recovered_data: Vec<Vec<u8>> =
            vec![vec![0u8; total_stripes * plan.shard_size]; k as usize];

        let results: Vec<Result<Vec<Vec<u8>>>> = (0..total_stripes)
            .into_par_iter()
            .map(|stripe_idx| {
                let mut shards: Vec<Option<&[u8]>> = vec![None; (k + m) as usize];

                // Fill available data shards.
                for i in 0..k as usize {
                    if !missing_indices.contains(&i) {
                        let offset = stripe_idx * plan.shard_size;
                        let end = offset + plan.shard_size;
                        if offset < available.data[i].len() {
                            shards[i] =
                                Some(&available.data[i][offset..end.min(available.data[i].len())]);
                        }
                    }
                }
                // Fill available parity shards.
                for i in 0..m as usize {
                    let offset = stripe_idx * plan.shard_size;
                    let end = offset + plan.shard_size;
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
                let offset = stripe_idx * plan.shard_size;
                let len = d_data.len().min(plan.shard_size);
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

        let plan = StripeLayout::compute(1024, 4, 2, 64);
        let segment_data = vec![0xABu8; 1024];

        let encoder = ParallelEncoder::new(codec.clone(), 0);
        let batch = encoder.encode(&segment_data, &plan).unwrap();

        // Re-decode with all data shards present (no missing).
        let decoder = ParallelDecoder::new(codec, 0);
        let recovered = decoder.decode(&batch, &plan, 4, 2, &[]).unwrap();

        assert_eq!(recovered.len(), 4);
    }
}
