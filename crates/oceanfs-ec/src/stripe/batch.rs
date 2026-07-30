//! Stripe batch — Struct of Arrays (SoA) layout for EC data.

/// A batch of stripes in SoA layout.
///
/// `data[i]` is the i-th data shard for all stripes concatenated.
/// `parity[j]` is the j-th parity shard for all stripes concatenated.
///
/// This layout ensures sequential memory access during GF(2^8) matrix
/// operations, per performance guideline §6.2.
#[derive(Debug, Clone)]
pub struct StripeBatch {
    /// Data shards (k vectors, each `stripe_count × shard_size` bytes).
    pub data: Vec<Vec<u8>>,
    /// Parity shards (m vectors, same size as data shards).
    pub parity: Vec<Vec<u8>>,
}

impl StripeBatch {
    /// Creates an empty stripe batch.
    pub fn new(data_count: usize, parity_count: usize) -> Self {
        Self { data: vec![Vec::new(); data_count], parity: vec![Vec::new(); parity_count] }
    }
}
