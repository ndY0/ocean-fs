//! OceanFS cache error types.
//!
//! The caching layer is best-effort — most operations are infallible.
//! Errors primarily arise from cache rebuild operations that interact
//! with the underlying metadata store.

/// The error type for `oceanfs-cache`.
///
/// # Examples
///
/// ```
/// use oceanfs_cache::Error;
///
/// let err = Error::InvalidConfig("max_size_bytes must be > 0".into());
/// assert!(err.is_invalid_config());
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Configuration is invalid or incomplete.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// An I/O error occurred during a cache rebuild.
    #[error("cache rebuild I/O error: {0}")]
    RebuildIo(#[from] std::io::Error),
}

impl Error {
    /// Returns `true` if this is an `InvalidConfig` error.
    pub fn is_invalid_config(&self) -> bool {
        matches!(self, Self::InvalidConfig(_))
    }
}

/// Convenience alias for `std::result::Result<T, Error>`.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod assertions {
    use static_assertions::assert_impl_all;

    use super::Error;

    assert_impl_all!(Error: std::error::Error, Send, Sync);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_config_to_string_includes_message() {
        let err = Error::InvalidConfig("bad setting".into());
        assert!(err.to_string().contains("bad setting"));
    }

    #[test]
    fn is_invalid_config_returns_true() {
        assert!(Error::InvalidConfig("x".into()).is_invalid_config());
    }

    #[test]
    fn io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "nope");
        let err: Error = io_err.into();
        assert!(err.to_string().contains("nope"));
    }
}
