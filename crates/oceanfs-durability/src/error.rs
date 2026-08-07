//! Durability error types.
//!
//! Errors for anti-entropy, garbage collection, heal, scrub, and gRPC services.

/// Durability errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A storage error occurred.
    #[error("storage error: {0}")]
    Storage(String),

    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),

    /// Failed to forward a request to the target node.
    #[error("forward failed to {target}: {reason}")]
    ForwardFailed {
        /// The target node the request was forwarded to.
        target: String,
        /// Reason for the failure.
        reason: String,
    },

    /// An operation timed out.
    #[error("operation timed out after {elapsed_ms}ms")]
    Timeout {
        /// Elapsed time in milliseconds before timeout.
        elapsed_ms: u64,
    },

    /// A segment was not found.
    #[error("segment {0} not found")]
    SegmentNotFound(oceanfs_core::SegmentId),

    /// Invalid configuration provided.
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

impl From<oceanfs_storage::Error> for Error {
    fn from(e: oceanfs_storage::Error) -> Self {
        Error::Storage(e.to_string())
    }
}

/// Convenience alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
