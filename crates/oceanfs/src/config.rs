//! CLI argument parsing and configuration loading.
//!
//! Parses command-line arguments, environment variables, and TOML
//! config files. Merges them in priority order: CLI > env > TOML > defaults.

use std::path::PathBuf;

use oceanfs_core::NodeConfig;

/// Parsed command-line arguments for the OceanFS binary.
#[derive(Debug, Clone)]
pub struct CliArgs {
    /// Path to the TOML configuration file.
    pub config: PathBuf,
    /// Override the data directory.
    pub data_dir: Option<PathBuf>,
    /// Override the HTTP listen address.
    pub listen_addr: Option<String>,
    /// Override the gRPC listen address.
    pub grpc_listen_addr: Option<String>,
    /// Comma-separated list of seed nodes.
    pub seed_nodes: Vec<String>,
    /// Log output format: "human" or "json".
    pub log_format: String,
    /// Log level filter.
    pub log_level: Option<String>,
}

impl Default for CliArgs {
    fn default() -> Self {
        Self {
            config: PathBuf::from("oceanfs.toml"),
            data_dir: None,
            listen_addr: None,
            grpc_listen_addr: None,
            seed_nodes: Vec::new(),
            log_format: "human".to_string(),
            log_level: None,
        }
    }
}

/// Loads configuration with priority: CLI > env > TOML > defaults.
///
/// # Errors
///
/// Returns an error if the TOML file cannot be parsed or required
/// configuration is invalid.
pub fn load_config(args: &CliArgs) -> Result<NodeConfig, Box<dyn std::error::Error>> {
    // Start with defaults.
    let mut config = NodeConfig::default();

    // Layer 1: TOML config file.
    if args.config.exists() {
        let toml_str = std::fs::read_to_string(&args.config)
            .map_err(|e| format!("cannot read config: {e}"))?;
        let file_config: NodeConfig =
            toml::from_str(&toml_str).map_err(|e| format!("invalid config TOML: {e}"))?;
        merge_config(&mut config, &file_config);
    }

    // Layer 2: Environment variables.
    if let Ok(val) = std::env::var("OCEANFS_LISTEN_ADDR") {
        config.listen_addr = val;
    }
    if let Ok(val) = std::env::var("OCEANFS_GRPC_LISTEN_ADDR") {
        config.grpc_listen_addr = val;
    }
    if let Ok(val) = std::env::var("OCEANFS_DATA_DIR") {
        config.data_dir = PathBuf::from(val);
    }
    if let Ok(val) = std::env::var("OCEANFS_SEED_NODES") {
        config.seed_nodes = val.split(',').map(|s| s.trim().to_string()).collect();
    }
    if let Ok(val) = std::env::var("OCEANFS_LOG_LEVEL") {
        config.log_level = val;
    }

    // Layer 3: CLI overrides (highest priority).
    if let Some(ref addr) = args.listen_addr {
        config.listen_addr = addr.clone();
    }
    if let Some(ref addr) = args.grpc_listen_addr {
        config.grpc_listen_addr = addr.clone();
    }
    if let Some(ref dir) = args.data_dir {
        config.data_dir = dir.clone();
    }
    if !args.seed_nodes.is_empty() {
        config.seed_nodes = args.seed_nodes.clone();
    }
    if let Some(ref level) = args.log_level {
        config.log_level = level.clone();
    }

    Ok(config)
}

/// Merges config fields: non-default values from `source` override `target`.
fn merge_config(target: &mut NodeConfig, source: &NodeConfig) {
    if !source.node_id.is_empty() && source.node_id != "node-1" {
        target.node_id = source.node_id.clone();
    }
    if source.data_dir.as_os_str() != "/var/lib/oceanfs" {
        target.data_dir = source.data_dir.clone();
    }
    if source.listen_addr != "0.0.0.0:9000" {
        target.listen_addr = source.listen_addr.clone();
    }
    if source.grpc_listen_addr != "0.0.0.0:9001" {
        target.grpc_listen_addr = source.grpc_listen_addr.clone();
    }
    if !source.seed_nodes.is_empty() {
        target.seed_nodes = source.seed_nodes.clone();
    }
    if source.log_level != "info" {
        target.log_level = source.log_level.clone();
    }
}
