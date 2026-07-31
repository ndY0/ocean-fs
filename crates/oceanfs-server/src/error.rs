//! Server error types.

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
}

/// Convenience result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;
