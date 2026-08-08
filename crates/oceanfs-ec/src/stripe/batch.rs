//! Stripe batch — Struct of Arrays (SoA) layout for EC data.

use bytes::BytesMut;

/// A batch of stripes in SoA layout.
///
/// `data[i]` is the i-th data shard for all stripes concatenated.
/// `parity[j]` is the j-th parity shard for all stripes concatenated.
///
/// Uses `BytesMut` for zero-copy extraction: individual shards can be
/// `.freeze()`d into `Bytes` without copying.
///
/// This layout ensures sequential memory access during GF(2^8) matrix
/// operations, per performance guideline §6.2.
#[derive(Debug)]
pub struct StripeBatch {
    /// Data shards (k mutable buffers, each `stripe_count × shard_size` bytes).
    pub data: Vec<BytesMut>,
    /// Parity shards (m mutable buffers, same size as data shards).
    pub parity: Vec<BytesMut>,
}

impl StripeBatch {
    /// Creates an empty stripe batch with pre-allocated shard vectors.
    pub fn new(data_count: usize, parity_count: usize) -> Self {
        Self {
            data: vec![BytesMut::new(); data_count],
            parity: vec![BytesMut::new(); parity_count],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_empty_batch() {
        let batch = StripeBatch::new(4, 2);
        assert_eq!(batch.data.len(), 4);
        assert_eq!(batch.parity.len(), 2);
        assert!(batch.data.iter().all(|v| v.is_empty()));
        assert!(batch.parity.iter().all(|v| v.is_empty()));
    }

    #[test]
    fn new_zero_shards() {
        let batch = StripeBatch::new(0, 0);
        assert!(batch.data.is_empty());
        assert!(batch.parity.is_empty());
    }
}
