//! Authentication — AWS SigV4 verification and tower middleware.
//!
//! Provides pluggable authentication for the S3 HTTP API. When
//! enabled, the [`AuthMiddleware`] layer intercepts requests and
//! verifies AWS Signature V4 credentials before allowing the
//! request to reach the S3 handler.
//!
//! ## Components
//!
//! - [`SigV4Verifier`]: verifies AWS Signature V4 signatures
//! - [`KeyStore`]: file-based access key → secret key mapping
//! - [`AuthMiddleware`]: tower `Layer` that enforces authentication
//!
//! ## Configuration
//!
//! Authentication is controlled by `AuthConfig` in `oceanfs-core`.
//! When `s3_auth_enabled` is `false`, all requests pass through
//! unauthenticated (development mode).

pub mod key_store;
pub mod middleware;
pub mod sigv4;

pub use key_store::{Credentials, KeyStore};
pub use middleware::AuthMiddleware;
pub use sigv4::SigV4Verifier;
