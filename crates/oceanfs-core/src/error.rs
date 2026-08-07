//! OceanFS error types.
//!
//! Every OceanFS crate defines its own `Error` enum. This is the root
//! error type for `oceanfs-core`. All other crates wrap or map into their
//! own error types at crate boundaries.

/// The root error type for `oceanfs-core`.
///
/// Variants are grouped by cause, not by API method:
/// - Input validation: `InvalidConfig`
/// - Not found: `BucketNotFound`
/// - Internal: `Internal`
///
/// # Examples
///
/// ```
/// use oceanfs_core::Error;
///
/// let err = Error::InvalidConfig("missing data_dir".into());
/// assert_eq!(err.to_string(), "invalid config: missing data_dir");
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Configuration is invalid or incomplete.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// The requested bucket does not exist.
    #[error("bucket not found: {0}")]
    BucketNotFound(String),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),

    /// An error during graceful cluster leave.
    #[error("graceful leave failed: {0}")]
    Leave(String),
}

impl Error {
    /// Returns `true` if this is an `InvalidConfig` error.
    pub fn is_invalid_config(&self) -> bool {
        matches!(self, Self::InvalidConfig(_))
    }

    /// Returns `true` if this is a `BucketNotFound` error.
    pub fn is_bucket_not_found(&self) -> bool {
        matches!(self, Self::BucketNotFound(_))
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
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn invalid_config_to_string_includes_message() {
        let err = Error::InvalidConfig("port must be > 0".into());
        assert!(err.to_string().contains("port must be > 0"));
    }

    #[test]
    fn is_invalid_config_returns_true_for_invalid_config() {
        assert!(Error::InvalidConfig("bad".into()).is_invalid_config());
    }

    #[test]
    fn is_invalid_config_returns_false_for_bucket_not_found() {
        assert!(!Error::BucketNotFound("b".into()).is_invalid_config());
    }

    #[test]
    fn is_bucket_not_found_returns_true_for_bucket_not_found() {
        assert!(Error::BucketNotFound("missing".into()).is_bucket_not_found());
    }

    #[test]
    fn is_bucket_not_found_returns_false_for_other_variants() {
        assert!(!Error::InvalidConfig("x".into()).is_bucket_not_found());
        assert!(!Error::Internal("x".into()).is_bucket_not_found());
    }

    #[test]
    fn result_type_alias_works() {
        // Verify the Result type alias compiles and can be used.
        fn returns_result() -> Result<i32> {
            Ok::<i32, Error>(42)
        }
        assert_eq!(returns_result().unwrap(), 42);

        fn returns_error() -> Result<i32> {
            Err(Error::Internal("fail".into()))
        }
        assert!(returns_error().is_err());
    }

    #[test]
    fn display_output_contains_message() {
        let err = Error::Internal("test".into());
        assert!(format!("{err}").contains("test"));
    }
}
