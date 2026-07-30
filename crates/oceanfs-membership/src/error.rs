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

    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod assertions {
    use static_assertions::assert_impl_all;

    use super::Error;
    assert_impl_all!(Error: std::error::Error, Send, Sync);
}
