//! TLS configuration for node-to-node communication.
//!
//! Provides client-side TLS configuration for mTLS-protected gRPC
//! connections. Currently a placeholder for future mTLS implementation
//! (Phase 5 — Authentication & mTLS).

use std::path::Path;

/// Error type for TLS configuration operations.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub(crate) enum TlsError {
    /// Failed to read TLS certificate.
    #[error("failed to read TLS certificate from {path}: {source}")]
    CertificateReadError {
        /// Path to the certificate file.
        path: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Invalid TLS certificate format.
    #[error("invalid TLS certificate: {0}")]
    InvalidCertificate(String),
}

/// Builds a client TLS configuration from a certificate path.
///
/// Currently a placeholder — returns `true` if a cert path is provided
/// (indicating TLS should be configured). The actual implementation will
/// be added in Phase 5.
///
/// Returns `true` if TLS should be enabled for the given configuration.
pub(crate) fn tls_enabled(_cert_path: Option<&Path>) -> bool {
    // Placeholder: TLS will be implemented in Phase 5.
    false
}
