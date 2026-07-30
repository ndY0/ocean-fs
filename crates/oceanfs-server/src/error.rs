/// Server error types.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum Error {
    /// No reachable node for the given key.
    #[error("no reachable node for key")]
    NoReachableNode,

    /// A routing error occurred.
    #[error("routing error: {0}")]
    Routing(String),
}

/// Convenience result alias.
#[allow(dead_code)]
pub type Result<T, E = Error> = std::result::Result<T, E>;
