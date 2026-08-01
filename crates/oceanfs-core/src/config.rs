//! OceanFS node configuration.
//!
//! Configuration is loaded from `oceanfs.toml` at startup. This module
//! defines the root config struct and its sub-components. Per-bucket
//! policy overrides are defined in `oceanfs-server` (Phase 5).

use std::path::PathBuf;

use crate::types::GpuConfig;

/// Root configuration for an OceanFS node.
///
/// Loaded from `oceanfs.toml` on startup. All fields have sensible
/// defaults so that a minimal config file is sufficient for development.
///
/// # Examples
///
/// ```
/// use oceanfs_core::NodeConfig;
///
/// let config = NodeConfig::default();
/// assert_eq!(config.data_dir.to_str().unwrap(), "/var/lib/oceanfs");
/// ```
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Unique identifier for this node.
    pub node_id: String,
    /// Directory for all persistent data (RocksDB, WAL, segments).
    pub data_dir: PathBuf,
    /// Address the S3 HTTP API listens on.
    pub listen_addr: String,
    /// Address for internal gRPC node-to-node communication.
    pub grpc_listen_addr: String,
    /// Bootstrap nodes for cluster discovery.
    pub seed_nodes: Vec<String>,
    /// Log level: trace, debug, info, warn, error.
    pub log_level: String,
    /// Whether to enable the Prometheus metrics endpoint.
    pub metrics_enabled: bool,
    /// Address for the Prometheus metrics HTTP endpoint.
    pub metrics_listen_addr: String,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: "node-1".into(),
            data_dir: PathBuf::from("/var/lib/oceanfs"),
            listen_addr: "0.0.0.0:9000".into(),
            grpc_listen_addr: "0.0.0.0:9001".into(),
            seed_nodes: vec![],
            log_level: "info".into(),
            metrics_enabled: true,
            metrics_listen_addr: "0.0.0.0:9090".into(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_expected_listen_addr() {
        let config = NodeConfig::default();
        assert_eq!(config.listen_addr, "0.0.0.0:9000");
    }

    #[test]
    fn default_config_seed_nodes_is_empty() {
        let config = NodeConfig::default();
        assert!(config.seed_nodes.is_empty());
    }

    #[test]
    fn wal_config_defaults_are_sensible() {
        let config = WalConfig::default();
        assert_eq!(config.max_file_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.fsync_batch_timeout_ms, 5);
    }

    #[test]
    fn ring_config_default_values() {
        let config = RingConfig::default();
        assert_eq!(config.vnodes_per_node, 256);
        assert_eq!(config.replication_factor, 3);
    }

    #[test]
    fn metadata_config_default_values() {
        let config = MetadataConfig::default();
        assert_eq!(config.block_cache_size, 128 * 1024 * 1024);
        assert_eq!(config.memtable_size, 64 * 1024 * 1024);
        assert_eq!(
            config.data_dir,
            std::path::PathBuf::from("/var/lib/oceanfs/metadata")
        );
    }
}

// ---------------------------------------------------------------------------
// WalConfig
// ---------------------------------------------------------------------------

/// Configuration for the Write-Ahead Log.
///
/// Controls WAL directory, file rotation, and fsync batching.
///
/// # Examples
///
/// ```
/// use oceanfs_core::WalConfig;
///
/// let config = WalConfig::default();
/// assert_eq!(config.max_file_size_bytes, 64 * 1024 * 1024);
/// ```
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory where WAL files are stored.
    pub data_dir: PathBuf,
    /// Maximum size of a single WAL file before rotation (default 64 MB).
    pub max_file_size_bytes: u64,
    /// Maximum time to wait before fsyncing a batch of WAL entries (default 5 ms).
    ///
    /// Shorter values reduce latency at the cost of more frequent fsyncs.
    pub fsync_batch_timeout_ms: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/oceanfs/wal"),
            max_file_size_bytes: 64 * 1024 * 1024,
            fsync_batch_timeout_ms: 5,
        }
    }
}

// ---------------------------------------------------------------------------
// RingConfig
// ---------------------------------------------------------------------------

/// Configuration for the consistent hashing ring.
///
/// # Examples
///
/// ```
/// use oceanfs_core::RingConfig;
///
/// let config = RingConfig::default();
/// assert_eq!(config.vnodes_per_node, 256);
/// assert_eq!(config.replication_factor, 3);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RingConfig {
    /// Number of virtual nodes per physical node (default 256).
    pub vnodes_per_node: u32,
    /// Number of successors for each key (default 3).
    pub replication_factor: u8,
}

impl Default for RingConfig {
    fn default() -> Self {
        Self { vnodes_per_node: 256, replication_factor: 3 }
    }
}

/// Configuration for the RocksDB metadata store.
///
/// # Examples
///
/// ```
/// use oceanfs_core::MetadataConfig;
///
/// let config = MetadataConfig::default();
/// assert_eq!(config.block_cache_size, 128 * 1024 * 1024);
/// ```
#[derive(Debug, Clone)]
pub struct MetadataConfig {
    /// Directory for RocksDB data files.
    pub data_dir: std::path::PathBuf,
    /// Size of the RocksDB block cache in bytes (default 128 MB).
    pub block_cache_size: usize,
    /// Size of the RocksDB memtable in bytes (default 64 MB).
    pub memtable_size: usize,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            data_dir: std::path::PathBuf::from("/var/lib/oceanfs/metadata"),
            block_cache_size: 128 * 1024 * 1024,
            memtable_size: 64 * 1024 * 1024,
        }
    }
}

// ---------------------------------------------------------------------------
// AuthConfig
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// AccelConfig
// ---------------------------------------------------------------------------

/// Configuration for the acceleration subsystem.
///
/// Controls EC encoding tier, hash tier, and GPU-specific options.
/// Loaded from the `[acceleration]` section of `oceanfs.toml`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{AccelConfig, GpuConfig};
///
/// let config = AccelConfig::default();
/// assert!(config.ec_tier_is_auto());
/// ```
#[derive(Debug, Clone)]
pub struct AccelConfig {
    /// EC acceleration tier: "auto", "cpu_simd", "isa_l", or "gpu_cuda".
    pub ec_tier: String,
    /// Hash acceleration tier: "auto" or "avx512" (delegates to blake3 crate).
    pub hash_tier: String,
    /// GPU-specific configuration (None if no GPU config is provided).
    pub gpu: Option<GpuConfig>,
    /// Prefer AVX-512 code path in ISA-L if available (default true).
    pub isal_prefer_avx512: bool,
}

impl Default for AccelConfig {
    fn default() -> Self {
        Self {
            ec_tier: "auto".into(),
            hash_tier: "auto".into(),
            gpu: None,
            isal_prefer_avx512: true,
        }
    }
}

impl AccelConfig {
    /// Returns `true` if the EC tier is set to `"auto"`.
    pub fn ec_tier_is_auto(&self) -> bool {
        self.ec_tier == "auto"
    }

    /// Returns `true` if GPU configuration is provided.
    pub fn has_gpu_config(&self) -> bool {
        self.gpu.is_some()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod auth_tests {
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod accel_config_tests {
    use super::*;

    #[test]
    fn accel_config_default_is_auto() {
        let cfg = AccelConfig::default();
        assert!(cfg.ec_tier_is_auto());
        assert_eq!(cfg.hash_tier, "auto");
        assert!(cfg.gpu.is_none());
        assert!(cfg.isal_prefer_avx512);
    }

    #[test]
    fn accel_config_has_gpu_config() {
        let cfg = AccelConfig {
            ec_tier: "gpu_cuda".into(),
            hash_tier: "auto".into(),
            gpu: Some(GpuConfig::default()),
            isal_prefer_avx512: true,
        };
        assert!(cfg.has_gpu_config());
    }

    #[test]
    fn accel_config_not_auto() {
        let cfg = AccelConfig {
            ec_tier: "cpu_simd".into(),
            hash_tier: "auto".into(),
            gpu: None,
            isal_prefer_avx512: false,
        };
        assert!(!cfg.ec_tier_is_auto());
        assert!(!cfg.has_gpu_config());
    }
}
