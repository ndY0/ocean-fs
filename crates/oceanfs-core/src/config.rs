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
}
