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

    /// The segment failed its integrity check and could not be repaired
    /// from the stored EC parity (too many corrupt shards, or the
    /// corruption is in the un-encoded tail).
    #[error("segment {0} corrupt and not repairable from parity")]
    SegmentCorrupt(oceanfs_core::SegmentId),

    /// A blob size exceeds the maximum allowed for a single segment.
    #[error("blob size {size} exceeds max segment size {max}")]
    BlobTooLarge {
        /// The blob size that was rejected.
        size: u64,
        /// The maximum allowed size for this tier.
        max: u64,
    },

    /// The buffer pool is exhausted and cannot allocate a new buffer.
    /// (No longer raised — `BufferPool::acquire` allocates on demand.)
    #[allow(dead_code)]
    #[error("buffer pool exhausted")]
    BufferPoolExhausted,

    /// An internal I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid configuration provided to the storage engine.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// The async append path waited for a slot re-activation past its
    /// deadline (bounded backpressure). The caller propagates this as a
    /// retryable overload response (`503 SlowDown`) — the write was not
    /// recorded anywhere, so the client may safely retry.
    #[error("write backpressure timeout: no segment slot re-activated within the deadline")]
    WriteBackpressureTimeout,

    /// An unknown or unsupported storage tier was encountered.
    #[error("invalid storage tier: {0}")]
    InvalidTier(String),

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

    /// A torn (partial, checksum-failed, or corrupt) record at the tail of
    /// the segment event WAL.
    ///
    /// [`crate::segment::event_wal::EventWalReader`] stops at the first
    /// such record (stop-at-first-bad-tail semantics — the recovery fold
    /// consumes the error and ignores everything after it). Records are
    /// never silently skipped or truncated mid-file.
    #[error("torn event WAL record at {pos:?}: {detail}")]
    TornEventRecord {
        /// The position where the bad record starts.
        pos: crate::segment::event_wal::EventWalPos,
        /// Why the record failed framing or checksum.
        detail: &'static str,
    },

    /// A corrupt record **in the middle** of the segment event log —
    /// valid bytes follow the bad record, so this is disk corruption, not
    /// a crash window.
    ///
    /// The recovery fold aborts startup with the record position (a torn
    /// tail is a clean stop; a mid-log corruption is not). The event log
    /// is never silently truncated past a mid-log corruption.
    #[error("corrupt event WAL record at {pos:?}: {detail} (valid data follows)")]
    CorruptEventLog {
        /// The position where the corrupt record starts.
        pos: crate::segment::event_wal::EventWalPos,
        /// Why the record failed framing or checksum.
        detail: &'static str,
    },

    /// A lifecycle transition rejected during the event fold — the event
    /// sequence is out of order, which the typed transition API cannot
    /// express (corruption). Startup aborts with the record position;
    /// the fold is never silently partial.
    #[error("event fold rejected at {pos:?}: {detail}")]
    EventFoldError {
        /// The position of the event that failed to fold.
        pos: crate::segment::event_wal::EventWalPos,
        /// The transition rejection reason.
        detail: String,
    },

    /// The node booted onto a directory written by a **pre-pool** node
    /// (ADR-0031 D3): a `pool_id`-less Seal record in the event WAL or a
    /// v2 checkpoint. Pre-pool data directories are refused — there is
    /// no migration and no continued decode; the operator must start on
    /// a fresh pools-enabled layout.
    #[error("unsupported pre-pool data directory: {detail}")]
    UnsupportedPrePoolDataDir {
        /// Why the directory was classified as pre-pool.
        detail: String,
    },
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
