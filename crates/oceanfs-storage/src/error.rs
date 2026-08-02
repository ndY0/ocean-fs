//! OceanFS storage error types.
//!
//! Variants are grouped by cause, not by API method. This crate's error
//! type wraps I/O errors from the underlying storage and validates
//! input at the segment buffer boundary.

/// Storage engine errors.
///
/// # Examples
///
/// ```ignore
/// // This example requires oceanfs-core to be in scope,
/// // which is always the case within the crate.
/// use oceanfs_storage::Error;
///
/// let err = Error::SegmentFull {
///     segment_id: oceanfs_core::SegmentId::new(),
///     current_size: 4194304,
/// };
/// assert!(err.to_string().contains("segment is full"));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The active segment has reached its target size and cannot accept
    /// more appends.
    #[error("segment {segment_id} is full ({current_size} bytes)")]
    SegmentFull {
        /// The segment that is full.
        segment_id: oceanfs_core::SegmentId,
        /// Current size of the segment in bytes.
        current_size: u64,
    },

    /// The requested segment was not found in the active pool.
    #[error("segment {0} not found")]
    SegmentNotFound(oceanfs_core::SegmentId),

    /// A blob size exceeds the maximum allowed for a single segment.
    #[error("blob size {size} exceeds max segment size {max}")]
    BlobTooLarge {
        /// The blob size that was rejected.
        size: u64,
        /// The maximum allowed size for this tier.
        max: u64,
    },

    /// The buffer pool is exhausted and cannot allocate a new buffer.
    #[error("buffer pool exhausted")]
    BufferPoolExhausted,

    /// An internal I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid configuration provided to the storage engine.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// A Merkle tree hash mismatch was detected.
    #[error("merkle hash mismatch at leaf index {leaf_index}")]
    MerkleMismatch {
        /// The index of the leaf that diverged.
        leaf_index: u64,
        /// The expected hash.
        expected: oceanfs_core::HashOutput,
        /// The actual hash computed.
        actual: oceanfs_core::HashOutput,
    },

    /// An error occurred during anti-entropy exchange.
    #[error("anti-entropy error: {0}")]
    AntiEntropy(String),

    /// An error occurred during garbage collection.
    #[error("GC error: {0}")]
    Gc(String),

    /// An error occurred during distributed scrubbing.
    #[error("scrub error: {0}")]
    Scrub(String),

    /// An error occurred during orphan reaping.
    #[error("orphan reaper error: {0}")]
    OrphanReaper(String),

    /// The heal queue is full and cannot accept new requests.
    #[error("heal queue is full")]
    HealQueueFull,

    /// A heal operation failed after exhausting all retry attempts.
    #[error("heal failed for segment {segment_id} after {retries} retries: {reason}")]
    HealFailed {
        /// The segment that could not be healed.
        segment_id: oceanfs_core::SegmentId,
        /// Number of retry attempts made.
        retries: u32,
        /// Reason for the failure.
        reason: String,
    },

    /// An error occurred during EC healing.
    #[error("heal error: {0}")]
    Heal(String),
}

/// Convenience alias for `std::result::Result<T, Error>`.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod assertions {
    use static_assertions::assert_impl_all;

    use super::Error;

    assert_impl_all!(Error: std::error::Error, Send, Sync);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn segment_full_error_includes_segment_id() {
        let id = oceanfs_core::SegmentId::new();
        let err = Error::SegmentFull { segment_id: id, current_size: 1000 };
        let msg = err.to_string();
        assert!(msg.contains(&id.to_string()));
        assert!(msg.contains("1000"));
    }

    #[test]
    fn io_error_conversion_works() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn blob_too_large_reports_both_sizes() {
        let err = Error::BlobTooLarge { size: 5000000, max: 4194304 };
        let msg = err.to_string();
        assert!(msg.contains("5000000"));
        assert!(msg.contains("4194304"));
    }
}
