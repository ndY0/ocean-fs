//! Erasure coding codec types.
//!
//! Contains `CodecType` (the codec enum), `CodecConfig` (codec parameters),
//! and `EncodingPlan` (pre-computed segment encoding plan).

// ---------------------------------------------------------------------------
// CodecType
// ---------------------------------------------------------------------------

/// Supported erasure coding codecs.
///
/// # Examples
///
/// ```
/// use oceanfs_core::CodecType;
///
/// let codec = CodecType::CauchyRs;
/// assert!(matches!(codec, CodecType::CauchyRs));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum CodecType {
    /// Cauchy Reed-Solomon over GF(2^8).
    CauchyRs,
    /// Standard Reed-Solomon (reserved for future use).
    StandardRs,
    /// Locally Recoverable Codes (reserved for future use).
    Lrc,
    /// Clay codes (reserved for future use).
    Clay,
}

// ---------------------------------------------------------------------------
// CodecConfig
// ---------------------------------------------------------------------------

/// Configuration for an erasure coding codec.
#[derive(Debug, Clone)]
pub struct CodecConfig {
    /// The codec to use.
    pub codec_type: CodecType,
    /// Number of data shards (k).
    pub data_shards: u8,
    /// Number of parity shards (m).
    pub parity_shards: u8,
    /// Size of each shard in bytes.
    pub strip_size_bytes: usize,
}

impl Default for CodecConfig {
    fn default() -> Self {
        Self {
            codec_type: CodecType::CauchyRs,
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 65536,
        }
    }
}

// ---------------------------------------------------------------------------
// EncodingPlan
// ---------------------------------------------------------------------------

/// A pre-computed plan for encoding a segment.
///
/// Contains the stripe count, padding, shard size, and codec parameters
/// (k = data shards, m = parity shards) needed for parallel encode/decode.
///
/// # Examples
///
/// ```
/// use oceanfs_core::EncodingPlan;
///
/// let plan = EncodingPlan {
///     stripe_count: 16,
///     padded_size: 4_194_304,
///     shard_size: 65536,
///     data_shards: 4,
///     parity_shards: 2,
/// };
/// assert_eq!(plan.total_shards(), 6);
/// ```
#[derive(Debug, Clone)]
pub struct EncodingPlan {
    /// Number of stripes in the segment.
    pub stripe_count: usize,
    /// Total size of the segment data after zero-padding.
    pub padded_size: u64,
    /// Size of each individual shard in bytes.
    pub shard_size: usize,
    /// Number of data shards (k).
    pub data_shards: u8,
    /// Number of parity shards (m).
    pub parity_shards: u8,
}

impl EncodingPlan {
    /// Returns the total number of shards (k + m).
    pub fn total_shards(&self) -> u8 {
        self.data_shards + self.parity_shards
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // -- CodecConfig --

    #[test]
    fn codec_config_default_values() {
        let cfg = CodecConfig::default();
        assert!(matches!(cfg.codec_type, CodecType::CauchyRs));
        assert_eq!(cfg.data_shards, 4);
        assert_eq!(cfg.parity_shards, 2);
        assert_eq!(cfg.strip_size_bytes, 65536);
    }

    // -- EncodingPlan --

    #[test]
    fn encoding_plan_total_shards() {
        let plan = EncodingPlan {
            stripe_count: 8,
            padded_size: 4096,
            shard_size: 128,
            data_shards: 4,
            parity_shards: 2,
        };
        assert_eq!(plan.total_shards(), 6);
    }

    #[test]
    fn encoding_plan_total_shards_only_data() {
        let plan = EncodingPlan {
            stripe_count: 1,
            padded_size: 256,
            shard_size: 64,
            data_shards: 3,
            parity_shards: 0,
        };
        assert_eq!(plan.total_shards(), 3);
    }
}
