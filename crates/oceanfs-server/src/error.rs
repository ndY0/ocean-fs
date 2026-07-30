/// Server error types.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum Error {
    #[error("no reachable node for key")]
    NoReachableNode,

    #[error("routing error: {0}")]
    Routing(String),
}

/// Convenience result alias.
#[allow(dead_code)]
pub type Result<T, E = Error> = std::result::Result<T, E>;
