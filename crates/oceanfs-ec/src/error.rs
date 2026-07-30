//! EC error types.

/// Erasure coding errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Not enough shards available for reconstruction.
    #[error("need at least {needed} shards, got {available}")]
    NotEnoughShards { needed: usize, available: usize },

    /// Invalid codec configuration.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Shard size mismatch.
    #[error("shard size mismatch: expected {expected}, got {actual}")]
    ShardSizeMismatch { expected: usize, actual: usize },

    /// Internal computation error.
    #[error("decode error: {0}")]
    DecodingFailed(String),
}

/// Convenience result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod assertions {
    use static_assertions::assert_impl_all;

    use super::Error;
    assert_impl_all!(Error: std::error::Error, Send, Sync);
}
