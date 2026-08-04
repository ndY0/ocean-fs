//! Storage API error types.
//!
//! A minimal error type covering the failure modes common to all storage
//! backend implementations.

use oceanfs_core::SegmentId;

/// Errors returned by storage API operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The requested resource was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// The requested segment was not found.
    #[error("segment {0} not found")]
    SegmentNotFound(SegmentId),

    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An invalid argument was provided.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Convenience alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
