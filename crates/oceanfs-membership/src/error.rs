//! Membership error types.

/// Membership errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The requested node was not found in the membership.
    #[error("node not found: {0}")]
    NodeNotFound(String),

    /// The membership system has shut down.
    #[error("membership shut down")]
    Shutdown,

    /// Background tasks have already been started.
    #[error("membership already started")]
    AlreadyStarted,

    /// Background tasks have not been started.
    #[error("membership not started")]
    NotStarted,

    /// Failed to join the cluster.
    #[error("join failed: {0}")]
    JoinFailed(String),

    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Graceful leave handoff failed.
    #[error("graceful leave failed: {0}")]
    Leave(String),
}

/// Convenience result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod assertions {
    use static_assertions::assert_impl_all;

    use super::Error;
    assert_impl_all!(Error: std::error::Error, Send, Sync);
}
