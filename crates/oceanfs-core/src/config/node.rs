//! Root node configuration.
//!
//! The [`NodeConfig`] struct is the top-level configuration for an OceanFS
//! node, loaded from `oceanfs.toml`. It contains all operational settings
//! including networking, storage paths, and maintenance intervals.

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
    /// Gossip membership protocol configuration.
    ///
    /// Controls SWIM gossip interval, suspicion/failure timeouts,
    /// indirect ping count, and bootstrap seed nodes.
    #[serde(default)]
    pub gossip: crate::GossipConfig,
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
    /// Number of virtual nodes per physical node in the DHT ring (default 256).
    #[serde(default = "default_vnodes_per_node")]
    pub vnodes_per_node: u32,
    /// Number of replicas for each data item (default 3).
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    /// Number of gRPC channels per peer (default 4).
    #[serde(default = "default_pool_size_per_peer")]
    pub pool_size_per_peer: usize,
    /// Keepalive interval in seconds for idle channels (default 30).
    #[serde(default = "default_keepalive_sec")]
    pub keepalive_sec: u64,
    /// Connection establishment timeout in milliseconds (default 5000).
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Default per-request timeout in milliseconds (default 30000).
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Whether to cache segment reads via mmap.
    ///
    /// When `true` (read-optimized profile), frequently-accessed segment
    /// shard files are memory-mapped for zero-copy reads from the kernel
    /// page cache. When `false` (write-optimized profile), segment data
    /// files are opened with `O_DIRECT` to bypass the page cache.
    #[serde(default = "default_read_cache_segments")]
    pub read_cache_segments: bool,
    /// Whether to use io_uring for disk I/O (Linux only).
    ///
    /// When `true` and running on Linux 5.1+, `tokio-uring` provides
    /// true async disk I/O. Falls back to `tokio::fs` on non-Linux
    /// platforms or when the feature is disabled.
    #[serde(default = "default_io_uring_enabled")]
    pub io_uring_enabled: bool,
    /// Maximum number of segment files to keep memory-mapped in the
    /// `SegmentFileCache`. Only meaningful when `read_cache_segments = true`.
    /// Default: 64.
    #[serde(default = "default_segment_cache_max_entries")]
    pub segment_cache_max_entries: usize,
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
fn default_vnodes_per_node() -> u32 {
    256
}
fn default_replication_factor() -> u32 {
    3
}
fn default_pool_size_per_peer() -> usize {
    4
}
fn default_keepalive_sec() -> u64 {
    30
}
fn default_connect_timeout_ms() -> u64 {
    5000
}
fn default_request_timeout_ms() -> u64 {
    30000
}
fn default_read_cache_segments() -> bool {
    false
}
fn default_io_uring_enabled() -> bool {
    cfg!(target_os = "linux")
}
fn default_segment_cache_max_entries() -> usize {
    64
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: "node-1".into(),
            data_dir: PathBuf::from("/var/lib/oceanfs"),
            listen_addr: "0.0.0.0:9000".into(),
            grpc_listen_addr: "0.0.0.0:9001".into(),
            gossip: crate::GossipConfig::default(),
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
            vnodes_per_node: 256,
            replication_factor: 3,
            pool_size_per_peer: 4,
            keepalive_sec: 30,
            connect_timeout_ms: 5000,
            request_timeout_ms: 30000,
            read_cache_segments: false,
            io_uring_enabled: cfg!(target_os = "linux"),
            segment_cache_max_entries: 64,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::{MetadataConfig, RingConfig, WalConfig};

    #[test]
    fn default_config_has_expected_listen_addr() {
        let config = NodeConfig::default();
        assert_eq!(config.listen_addr, "0.0.0.0:9000");
    }

    #[test]
    fn default_config_seed_nodes_is_empty() {
        let config = NodeConfig::default();
        assert!(config.gossip.seed_nodes.is_empty());
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

    #[test]
    fn default_read_cache_segments_is_false() {
        let config = NodeConfig::default();
        assert!(!config.read_cache_segments);
    }

    #[test]
    fn default_io_uring_enabled_matches_platform() {
        let config = NodeConfig::default();
        assert_eq!(config.io_uring_enabled, cfg!(target_os = "linux"));
    }

    #[test]
    fn default_segment_cache_max_entries_is_64() {
        let config = NodeConfig::default();
        assert_eq!(config.segment_cache_max_entries, 64);
    }
}
