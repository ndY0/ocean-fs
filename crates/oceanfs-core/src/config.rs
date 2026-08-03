//! OceanFS node configuration.
//!
//! Configuration is loaded from `oceanfs.toml` at startup. This module
//! defines the root config struct and its sub-components. Per-bucket
//! policy overrides are defined in `oceanfs-server` (Phase 5).

use std::path::PathBuf;

use crate::types::{CompressionTier, GpuConfig};

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
#[derive(Debug, Clone, serde::Deserialize)]
pub struct NodeConfig {
    /// Unique identifier for this node.
    #[serde(default = "default_node_id")]
    pub node_id: String,
    /// Directory for all persistent data (RocksDB, WAL, segments).
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    /// Address the S3 HTTP API listens on.
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    /// Address for internal gRPC node-to-node communication.
    #[serde(default = "default_grpc_listen_addr")]
    pub grpc_listen_addr: String,
    /// Bootstrap nodes for cluster discovery.
    #[serde(default)]
    pub seed_nodes: Vec<String>,
    /// Log level: trace, debug, info, warn, error.
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// Whether to enable the Prometheus metrics endpoint.
    #[serde(default = "default_metrics_enabled")]
    pub metrics_enabled: bool,
    /// Address for the Prometheus metrics HTTP endpoint.
    #[serde(default = "default_metrics_listen_addr")]
    pub metrics_listen_addr: String,
    /// Whether S3 Signature V4 authentication is enforced.
    ///
    /// When `true`, all S3 object and bucket operations require valid
    /// AWS SigV4 credentials. When `false` (default), requests pass
    /// through unauthenticated (development mode).
    #[serde(default)]
    pub s3_auth_enabled: bool,
    /// Whether the prefetch engine warms caches after LIST/GET.
    ///
    /// Enables anticipatory cache population for improved read
    /// latency. Prefetch runs as a background task and does not
    /// block request handling.
    #[serde(default)]
    pub prefetch_enabled: bool,
    /// Maximum HTTP body size in bytes (default 2 MB = 2097152).
    #[serde(default = "default_max_body_size")]
    pub max_body_size: usize,
    /// Garbage collection interval in seconds (default 3600).
    #[serde(default = "default_gc_interval")]
    pub gc_interval_sec: u64,
    /// Tombstone TTL in seconds before deleted objects are
    /// permanently reclaimed (default 259200 = 3 days).
    #[serde(default = "default_tombstone_ttl")]
    pub tombstone_ttl_sec: u64,
    /// Anti-entropy Merkle verification interval in seconds
    /// (default 300).
    #[serde(default = "default_ae_interval")]
    pub ae_interval_sec: u64,
    /// Scrub cycle interval in seconds (default 604800 = 7 days).
    #[serde(default = "default_scrub_interval")]
    pub scrub_interval_sec: u64,
    /// Orphan reaper interval in seconds (default 3600).
    #[serde(default = "default_orphan_interval")]
    pub orphan_reaper_interval_sec: u64,
}

fn default_node_id() -> String {
    "node-1".into()
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/oceanfs")
}
fn default_listen_addr() -> String {
    "0.0.0.0:9000".into()
}
fn default_grpc_listen_addr() -> String {
    "0.0.0.0:9001".into()
}
fn default_log_level() -> String {
    "info".into()
}
fn default_metrics_enabled() -> bool {
    true
}
fn default_metrics_listen_addr() -> String {
    "0.0.0.0:9090".into()
}
fn default_max_body_size() -> usize {
    2 * 1024 * 1024 // 2 MB
}
fn default_gc_interval() -> u64 {
    3600
}
fn default_tombstone_ttl() -> u64 {
    259200 // 3 days
}
fn default_ae_interval() -> u64 {
    300
}
fn default_scrub_interval() -> u64 {
    604800 // 7 days
}
fn default_orphan_interval() -> u64 {
    3600
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
            s3_auth_enabled: false,
            prefetch_enabled: false,
            max_body_size: 2 * 1024 * 1024,
            gc_interval_sec: 3600,
            tombstone_ttl_sec: 259200,
            ae_interval_sec: 300,
            scrub_interval_sec: 604800,
            orphan_reaper_interval_sec: 3600,
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
        assert_eq!(config.data_dir, std::path::PathBuf::from("/var/lib/oceanfs/metadata"));
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
// CompressionConfig
// ---------------------------------------------------------------------------

/// Node-level compression governance configuration.
///
/// Per ADR-0007, the node operator controls what compression backends
/// are available. The node-level `tier` sets the **ceiling** — the
/// maximum tier any bucket may use. Per-bucket `compress_tier` can only
/// select from or downgrade from the node ceiling; it cannot upgrade.
///
/// Loaded from the `[compression]` section of `oceanfs.toml`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{CompressionConfig, CompressionTier};
///
/// let config = CompressionConfig::default();
/// assert!(config.enabled);
/// assert_eq!(config.tier, CompressionTier::Auto);
/// ```
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Whether segment compression is enabled at all.
    /// When `false`, no compression is applied regardless of bucket settings.
    pub enabled: bool,
    /// Compression acceleration tier ceiling for this node.
    /// Buckets may only select this tier or lower.
    pub tier: CompressionTier,
    /// Minimum batch bytes for GPU offload (only relevant when tier ≥ GpuNvcomp).
    pub gpu_min_batch_bytes: u64,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self { enabled: true, tier: CompressionTier::Auto, gpu_min_batch_bytes: 1_048_576 }
    }
}

// ---------------------------------------------------------------------------
// AccelConfig
// ---------------------------------------------------------------------------

/// Configuration for the acceleration subsystem.
///
/// Controls EC encoding tier, hash tier, GPU-specific options,
/// and node-level compression governance. Loaded from the
/// `[acceleration]` and `[compression]` sections of `oceanfs.toml`.
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
    /// Node-level compression governance (per ADR-0007).
    /// Controls the compression ceiling — buckets may only select
    /// this tier or lower. Default: enabled, tier=auto.
    pub compression: CompressionConfig,
}

impl Default for AccelConfig {
    fn default() -> Self {
        Self {
            ec_tier: "auto".into(),
            hash_tier: "auto".into(),
            gpu: None,
            isal_prefer_avx512: true,
            compression: CompressionConfig::default(),
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
            compression: CompressionConfig::default(),
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
            compression: CompressionConfig::default(),
        };
        assert!(!cfg.ec_tier_is_auto());
        assert!(!cfg.has_gpu_config());
    }
}
