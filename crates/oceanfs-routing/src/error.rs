//! OceanFS routing error types.

/// Routing errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The ring is empty (no nodes).
    #[error("ring is empty")]
    RingEmpty,

    /// A node was not found in the ring.
    #[error("node not found in ring: {0}")]
    NodeNotFound(String),

    /// Invalid ring configuration.
    #[error("invalid ring config: {0}")]
    InvalidConfig(String),
}

/// Convenience result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod assertions {
    use static_assertions::assert_impl_all;

    use super::Error;
    assert_impl_all!(Error: std::error::Error, Send, Sync);
}
