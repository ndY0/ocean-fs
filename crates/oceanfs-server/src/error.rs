//! Server error types.
//!
//! These errors span the entire server crate: routing, coordination,
//! HTTP handling, and metadata operations. Each variant maps to an
//! S3-compatible HTTP status code via [`Error::s3_code`] and
//! [`Error::s3_status`].

use axum::http::StatusCode;

/// Server errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No reachable node for the given key.
    #[error("no reachable node for key")]
    NoReachableNode,

    /// A routing error occurred.
    #[error("routing error: {0}")]
    Routing(String),

    /// Failed to forward a request to the target node.
    #[error("forward failed to {target}: {reason}")]
    ForwardFailed {
        /// The target node the request was forwarded to.
        target: String,
        /// Reason for the failure.
        reason: String,
    },

    /// All forwarding attempts failed.
    #[error("all forwarding attempts failed after {attempts} attempts")]
    AllForwardingFailed {
        /// Number of forwarding attempts made.
        attempts: usize,
    },

    /// Write quorum was not met.
    #[error("write quorum not met: required {required} but only {received} acks received")]
    QuorumNotMet {
        /// Required number of acknowledgments.
        required: u8,
        /// Number of acknowledgments received.
        received: usize,
    },

    /// A write operation failed.
    #[error("write failed: {0}")]
    WriteFailed(String),

    /// The write backpressure queue is saturated: the request waited for
    /// a write permit (or for a segment slot re-activation) past
    /// `operation_timeouts.write_queue_ms`. Retryable — nothing was
    /// recorded for this request.
    #[error("write queue overloaded; retry later")]
    WriteOverloaded,

    /// An operation timed out.
    #[error("operation timed out after {elapsed_ms}ms")]
    Timeout {
        /// Elapsed time in milliseconds before timeout.
        elapsed_ms: u64,
    },

    /// A storage error occurred.
    #[error("storage error: {0}")]
    Storage(String),

    /// Object not found.
    #[error("object not found: {0}")]
    NotFound(String),

    /// Hash verification failed.
    #[error("hash verification failed: expected {expected}, got {actual}")]
    HashMismatch {
        /// Expected hash value.
        expected: String,
        /// Actual hash value.
        actual: String,
    },

    /// Invalid bucket name.
    #[error("invalid bucket name: {0}")]
    InvalidBucketName(String),

    /// Invalid object key.
    #[error("invalid object key: {0}")]
    InvalidKey(String),

    /// Bucket is not empty and cannot be deleted.
    #[error("bucket not empty: {0}")]
    BucketNotEmpty(String),

    /// Bucket already exists.
    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),

    /// Authentication failed.
    #[error("access denied: {0}")]
    AccessDenied(String),

    /// An internal server error.
    #[error("internal server error: {0}")]
    Internal(String),

    /// Invalid request (malformed XML, bad parameters).
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// An error from the metadata operations layer.
    #[error("metadata error: {0}")]
    Metadata(#[from] crate::metadata_ops::MetadataError),
}

impl Error {
    /// Returns the S3 error code string for this error.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_server::Error;
    /// let err = Error::NotFound("obj".into());
    /// assert_eq!(err.s3_code(), "NoSuchKey");
    /// ```
    pub fn s3_code(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "NoSuchKey",
            Self::AccessDenied(_) => "AccessDenied",
            Self::BucketNotEmpty(_) => "BucketNotEmpty",
            Self::BucketAlreadyExists(_) => "BucketAlreadyExists",
            Self::InvalidBucketName(_) => "InvalidBucketName",
            Self::InvalidKey(_) => "InvalidArgument",
            Self::InvalidRequest(_) => "InvalidRequest",
            Self::Timeout { .. } => "RequestTimeout",
            Self::HashMismatch { .. } => "BadDigest",
            Self::QuorumNotMet { .. } | Self::WriteFailed(_) => "InternalError",
            Self::WriteOverloaded => "SlowDown",
            Self::NoReachableNode
            | Self::Routing(_)
            | Self::ForwardFailed { .. }
            | Self::AllForwardingFailed { .. } => "ServiceUnavailable",
            Self::Storage(_) | Self::Internal(_) => "InternalError",
            Self::Metadata(ref e) => match e {
                crate::metadata_ops::MetadataError::NotFound(_) => "NoSuchKey",
                _ => "InternalError",
            },
        }
    }

    /// Returns the HTTP status code for this error.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_server::Error;
    /// use axum::http::StatusCode;
    /// assert_eq!(Error::NotFound("x".into()).s3_status(), StatusCode::NOT_FOUND);
    /// ```
    pub fn s3_status(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::AccessDenied(_) => StatusCode::FORBIDDEN,
            Self::BucketNotEmpty(_) => StatusCode::CONFLICT,
            Self::BucketAlreadyExists(_) => StatusCode::CONFLICT,
            Self::InvalidBucketName(_) => StatusCode::BAD_REQUEST,
            Self::InvalidKey(_) | Self::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            Self::Timeout { .. } => StatusCode::REQUEST_TIMEOUT,
            Self::HashMismatch { .. } | Self::WriteFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::WriteOverloaded => StatusCode::SERVICE_UNAVAILABLE,
            Self::QuorumNotMet { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::NoReachableNode
            | Self::Routing(_)
            | Self::ForwardFailed { .. }
            | Self::AllForwardingFailed { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::Storage(_) | Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Metadata(ref e) => match e {
                crate::metadata_ops::MetadataError::NotFound(_) => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }

    /// Returns the error message suitable for the S3 XML response body.
    pub fn s3_message(&self) -> String {
        self.to_string()
    }
}

/// Convenience result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_maps_to_nosuchkey() {
        let err = Error::NotFound("my-key".into());
        assert_eq!(err.s3_code(), "NoSuchKey");
        assert_eq!(err.s3_status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn access_denied_maps_to_403() {
        let err = Error::AccessDenied("no".into());
        assert_eq!(err.s3_code(), "AccessDenied");
        assert_eq!(err.s3_status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn bucket_errors_map_correctly() {
        assert_eq!(Error::InvalidBucketName("x".into()).s3_status(), StatusCode::BAD_REQUEST);
        assert_eq!(Error::BucketNotEmpty("x".into()).s3_status(), StatusCode::CONFLICT);
        assert_eq!(Error::BucketAlreadyExists("x".into()).s3_status(), StatusCode::CONFLICT);
    }

    #[test]
    fn internal_errors_map_to_500() {
        assert_eq!(Error::Internal("oops".into()).s3_status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(Error::Storage("disk".into()).s3_status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            Error::HashMismatch { expected: "a".into(), actual: "b".into() }.s3_status(),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }
}
