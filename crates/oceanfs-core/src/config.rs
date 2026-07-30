//! OceanFS node configuration.
//!
//! Configuration is loaded from `oceanfs.toml` at startup. This module
//! defines the root config struct and its sub-components. Per-bucket
//! policy overrides are defined in `oceanfs-server` (Phase 5).

use std::path::PathBuf;

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
#[derive(Debug, Clone)]
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
