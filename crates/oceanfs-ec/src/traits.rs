//! Encoder and Decoder traits for erasure coding.

use crate::error::Result;

/// Encodes k data shards into m parity shards.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_ec::{Encoder, CauchyEncoder};
/// use oceanfs_core::CodecConfig;
///
/// let config = CodecConfig { data_shards: 4, parity_shards: 2, ..Default::default() };
/// let encoder = CauchyEncoder::new(config);
/// let data = [b"hello", b"world", b"foo__", b"bar__"];
/// let parity = encoder.encode(&data, 2).unwrap();
/// assert_eq!(parity.len(), 2);
/// ```
pub trait Encoder: Send + Sync {
    /// Encodes k data shards into m parity shards.
    ///
    /// All data shards must have the same length.
    ///
    /// # Errors
    ///
    /// Returns an error if shard sizes differ or k/m are invalid.
    fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> Result<Vec<Vec<u8>>>;
}

/// Decodes available shards to recover missing data shards.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_ec::{Decoder, CauchyEncoder};
/// use oceanfs_core::CodecConfig;
///
/// let config = CodecConfig { data_shards: 4, parity_shards: 2, ..Default::default() };
/// let encoder = CauchyEncoder::new(config);
/// // Recover data shards 0 and 2 using other available shards.
/// let available: Vec<Option<&[u8]>> = vec![None, Some(b"B"), Some(b"C"), Some(b"D"), Some(b"P0"), Some(b"P1")];
/// let recovered = encoder.decode(&available, 4, 2).unwrap();
/// ```
pub trait Decoder: Send + Sync {
    /// Decodes available shards to recover the original k data shards.
    ///
    /// `available` has length k+m. `None` entries indicate missing shards.
    /// At least k entries must be `Some`.
    ///
    /// # Errors
    ///
    /// Returns an error if fewer than k shards are available.
    fn decode(
        &self,
        available_shards: &[Option<&[u8]>],
        data_count: u8,
        parity_count: u8,
    ) -> Result<Vec<Vec<u8>>>;
}
