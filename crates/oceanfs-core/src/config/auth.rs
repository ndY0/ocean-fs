//! Authentication and mTLS configuration.
//!
//! Controls S3 SigV4 authentication, mTLS certificate paths,
//! and access key management.

/// Configuration for authentication and mTLS.
///
/// Controls S3 SigV4 authentication enable/disable, TLS certificate
/// paths, and mTLS settings for internal gRPC.
///
/// # Examples
///
/// ```
/// use oceanfs_core::AuthConfig;
///
/// let config = AuthConfig::default();
/// assert!(!config.s3_auth_enabled);
/// ```
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Whether S3 Signature V4 authentication is enforced.
    pub s3_auth_enabled: bool,
    /// Whether mutual TLS is enabled for gRPC.
    pub mtls_enabled: bool,
    /// Path to the TLS server certificate (PEM).
    pub tls_cert_path: Option<std::path::PathBuf>,
    /// Path to the TLS server private key (PEM).
    pub tls_key_path: Option<std::path::PathBuf>,
    /// Path to the client CA certificate for mTLS verification.
    pub client_ca_path: Option<std::path::PathBuf>,
    /// Path to the access keys file (TOML format).
    pub access_keys_path: Option<std::path::PathBuf>,
}

impl AuthConfig {
    /// Returns `true` if any auth feature is enabled.
    pub fn auth_enabled(&self) -> bool {
        self.s3_auth_enabled || self.mtls_enabled
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn auth_config_default_is_disabled() {
        let cfg = AuthConfig::default();
        assert!(!cfg.s3_auth_enabled);
        assert!(!cfg.mtls_enabled);
    }

    #[test]
    fn auth_config_auth_enabled_when_any_flag_is_set() {
        let mut cfg = AuthConfig::default();
        assert!(!cfg.auth_enabled());
        cfg.s3_auth_enabled = true;
        assert!(cfg.auth_enabled());
    }
}
