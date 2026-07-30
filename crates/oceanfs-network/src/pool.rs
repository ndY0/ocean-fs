//! Persistent gRPC connection pool per peer node.

/// Error type for connection pool operations.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum Error {
    #[error("no available connections for peer {0}")]
    NoConnection(String),

    #[error("connection failed: {0}")]
    ConnectionFailed(String),
}

/// A pool of gRPC channels per peer node.
pub struct ConnectionPool {
    _pool_size: usize,
}

impl ConnectionPool {
    /// Creates a new connection pool with the given maximum size per peer.
    pub fn new(pool_size: usize) -> Self {
        Self { _pool_size: pool_size }
    }

    /// Returns the maximum number of channels per peer.
    pub fn pool_size(&self) -> usize {
        self._pool_size
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn pool_creation() {
        let pool = ConnectionPool::new(4);
        assert_eq!(pool.pool_size(), 4);
    }
}
