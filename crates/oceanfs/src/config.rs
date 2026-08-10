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
        merge_config(&mut config, &file_config, args)?;
    } else {
        // No config file: still apply env vars and CLI on top of defaults.
        apply_env_overrides(&mut config);
        apply_cli_overrides(&mut config, args);
    }

    Ok(config)
}

/// Merges all fields from `source` (TOML) into `target`, then applies
/// environment variable overrides and CLI overrides on top.
///
/// Priority order: CLI > env > TOML > defaults.
///
/// # Errors
///
/// Returns an error if environment variable values cannot be parsed.
pub fn merge_config(
    target: &mut NodeConfig,
    source: &NodeConfig,
    cli_overrides: &CliArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Clone all TOML fields into target (no sentinel checks).
    *target = source.clone();

    // 2. Apply env var overrides on top.
    apply_env_overrides(target);

    // 3. Apply CLI overrides on top (last-wins).
    apply_cli_overrides(target, cli_overrides);

    Ok(())
}

/// Applies environment variable overrides to the given config.
///
/// Supported env vars:
/// - `OCEANFS_LISTEN_ADDR`, `OCEANFS_GRPC_LISTEN_ADDR`, `OCEANFS_DATA_DIR`,
///   `OCEANFS_SEED_NODES`, `OCEANFS_LOG_LEVEL`
/// - `OCEANFS_GC_INTERVAL`, `OCEANFS_AE_INTERVAL`, `OCEANFS_GOSSIP_INTERVAL_MS`,
///   `OCEANFS_SUSPICION_TIMEOUT_MS`, `OCEANFS_FAILURE_TIMEOUT_MS`,
///   `OCEANFS_MAX_BODY_SIZE`, `OCEANFS_SCRUB_INTERVAL`,
///   `OCEANFS_ORPHAN_REAPER_INTERVAL`, `OCEANFS_METRICS_ENABLED`,
///   `OCEANFS_PREFETCH_ENABLED`, `OCEANFS_S3_AUTH_ENABLED`
pub fn apply_env_overrides(config: &mut NodeConfig) {
    // Networking and basic.
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
        config.gossip.seed_nodes = val.split(',').map(|s| s.trim().to_string()).collect();
    }
    if let Ok(val) = std::env::var("OCEANFS_LOG_LEVEL") {
        config.log_level = val;
    }

    // Maintenance intervals.
    if let Ok(val) = std::env::var("OCEANFS_GC_INTERVAL") {
        if let Ok(parsed) = val.parse::<u64>() {
            config.gc_interval_sec = parsed;
        }
    }
    if let Ok(val) = std::env::var("OCEANFS_AE_INTERVAL") {
        if let Ok(parsed) = val.parse::<u64>() {
            config.ae_interval_sec = parsed;
        }
    }
    if let Ok(val) = std::env::var("OCEANFS_SCRUB_INTERVAL") {
        if let Ok(parsed) = val.parse::<u64>() {
            config.scrub_interval_sec = parsed;
        }
    }
    if let Ok(val) = std::env::var("OCEANFS_ORPHAN_REAPER_INTERVAL") {
        if let Ok(parsed) = val.parse::<u64>() {
            config.orphan_reaper_interval_sec = parsed;
        }
    }

    // Gossip tuning.
    if let Ok(val) = std::env::var("OCEANFS_GOSSIP_INTERVAL_MS") {
        if let Ok(parsed) = val.parse::<u64>() {
            config.gossip.interval_ms = parsed;
        }
    }
    if let Ok(val) = std::env::var("OCEANFS_SUSPICION_TIMEOUT_MS") {
        if let Ok(parsed) = val.parse::<u64>() {
            config.gossip.suspicion_timeout_ms = parsed;
        }
    }
    if let Ok(val) = std::env::var("OCEANFS_FAILURE_TIMEOUT_MS") {
        if let Ok(parsed) = val.parse::<u64>() {
            config.gossip.failure_timeout_ms = parsed;
        }
    }

    // Limits.
    if let Ok(val) = std::env::var("OCEANFS_MAX_BODY_SIZE") {
        if let Ok(parsed) = val.parse::<usize>() {
            config.max_body_size = parsed;
        }
    }

    // Feature toggles (boolean env vars: "1", "true", "yes" → true).
    if let Ok(val) = std::env::var("OCEANFS_METRICS_ENABLED") {
        config.metrics_enabled = parse_bool_env(&val);
    }
    if let Ok(val) = std::env::var("OCEANFS_PREFETCH_ENABLED") {
        config.prefetch_enabled = parse_bool_env(&val);
    }
    if let Ok(val) = std::env::var("OCEANFS_S3_AUTH_ENABLED") {
        config.s3_auth_enabled = parse_bool_env(&val);
    }
}

/// Applies CLI argument overrides to the given config (highest priority).
pub fn apply_cli_overrides(config: &mut NodeConfig, args: &CliArgs) {
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
        config.gossip.seed_nodes = args.seed_nodes.clone();
    }
    if let Some(ref level) = args.log_level {
        config.log_level = level.clone();
    }
}

/// Parses a boolean environment variable value.
///
/// Recognizes `"1"`, `"true"`, `"yes"` (case-insensitive) as `true`.
/// All other values are `false`.
fn parse_bool_env(val: &str) -> bool {
    matches!(val.to_lowercase().as_str(), "1" | "true" | "yes")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::io::Write;

    use super::*;
    use oceanfs_core::GossipConfig;

    /// Writes a TOML snippet to a temp directory and returns the path.
    fn write_temp_toml(dir: &std::path::Path, content: &str) -> PathBuf {
        let path = dir.join("oceanfs.toml");
        let mut f = std::fs::File::create(&path).expect("create config file");
        f.write_all(content.as_bytes()).expect("write config");
        path
    }

    // -- merge_config tests --

    #[test]
    fn merge_config_applies_gc_interval_from_toml() {
        let mut target = NodeConfig::default();
        let source = NodeConfig { gc_interval_sec: 10, ..NodeConfig::default() };
        let cli = CliArgs::default();
        merge_config(&mut target, &source, &cli).expect("merge_config");
        assert_eq!(target.gc_interval_sec, 10, "gc_interval_sec should carry through merge");
    }

    #[test]
    fn merge_config_applies_ae_interval_from_toml() {
        let mut target = NodeConfig::default();
        let source = NodeConfig { ae_interval_sec: 600, ..NodeConfig::default() };
        let cli = CliArgs::default();
        merge_config(&mut target, &source, &cli).expect("merge_config");
        assert_eq!(target.ae_interval_sec, 600);
    }

    #[test]
    fn merge_config_applies_orphan_reaper_interval_from_toml() {
        let mut target = NodeConfig::default();
        let source = NodeConfig { orphan_reaper_interval_sec: 7200, ..NodeConfig::default() };
        let cli = CliArgs::default();
        merge_config(&mut target, &source, &cli).expect("merge_config");
        assert_eq!(target.orphan_reaper_interval_sec, 7200);
    }

    #[test]
    fn merge_config_applies_max_body_size_from_toml() {
        let mut target = NodeConfig::default();
        let source = NodeConfig { max_body_size: 10 * 1024 * 1024, ..NodeConfig::default() };
        let cli = CliArgs::default();
        merge_config(&mut target, &source, &cli).expect("merge_config");
        assert_eq!(target.max_body_size, 10 * 1024 * 1024);
    }

    #[test]
    fn merge_config_applies_all_fields() {
        let mut target = NodeConfig::default();
        let source = NodeConfig {
            node_id: "custom-node".into(),
            listen_addr: "127.0.0.1:8080".into(),
            grpc_listen_addr: "127.0.0.1:8081".into(),
            gc_interval_sec: 60,
            tombstone_ttl_sec: 7200,
            ae_interval_sec: 100,
            scrub_interval_sec: 7200,
            orphan_reaper_interval_sec: 1800,
            max_body_size: 5 * 1024 * 1024,
            metrics_enabled: false,
            prefetch_enabled: true,
            s3_auth_enabled: true,
            vnodes_per_node: 512,
            replication_factor: 5,
            pool_size_per_peer: 8,
            keepalive_sec: 60,
            connect_timeout_ms: 10000,
            request_timeout_ms: 60000,
            gossip: GossipConfig {
                interval_ms: 500,
                suspicion_timeout_ms: 10000,
                failure_timeout_ms: 30000,
                ..Default::default()
            },
            ..NodeConfig::default()
        };
        let cli = CliArgs::default();
        merge_config(&mut target, &source, &cli).expect("merge_config");
        assert_eq!(target.node_id, "custom-node");
        assert_eq!(target.gc_interval_sec, 60);
        assert_eq!(target.ae_interval_sec, 100);
        assert_eq!(target.max_body_size, 5 * 1024 * 1024);
        assert!(!target.metrics_enabled);
        assert!(target.s3_auth_enabled);
        assert_eq!(target.vnodes_per_node, 512);
        assert_eq!(target.replication_factor, 5);
        assert_eq!(target.pool_size_per_peer, 8);
        assert_eq!(target.gossip.interval_ms, 500);
    }

    // -- env var tests -- (moved to tests/config_env.rs because set_var requires unsafe)

    // -- CLI override tests --

    #[test]
    fn cli_override_listen_addr_wins_over_default() {
        let mut config = NodeConfig::default();
        let args = CliArgs { listen_addr: Some("10.0.0.1:9999".into()), ..CliArgs::default() };
        apply_cli_overrides(&mut config, &args);
        assert_eq!(config.listen_addr, "10.0.0.1:9999");
    }

    #[test]
    fn cli_override_seed_nodes_wins() {
        let mut config = NodeConfig::default();
        let args = CliArgs {
            seed_nodes: vec!["node-a:9001".into(), "node-b:9001".into()],
            ..CliArgs::default()
        };
        apply_cli_overrides(&mut config, &args);
        assert_eq!(config.gossip.seed_nodes, vec!["node-a:9001", "node-b:9001"]);
    }

    // -- load_config tests (with temp TOML files) --

    #[test]
    fn load_config_applies_toml_values() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let toml_content = r#"
            node_id = "loaded-node"
            gc_interval_sec = 42
            ae_interval_sec = 77
            max_body_size = 3145728
        "#;
        let config_path = write_temp_toml(tmp.path(), toml_content);
        let args = CliArgs { config: config_path, ..CliArgs::default() };
        let config = load_config(&args).expect("load_config");
        assert_eq!(config.node_id, "loaded-node");
        assert_eq!(config.gc_interval_sec, 42);
        assert_eq!(config.ae_interval_sec, 77);
        assert_eq!(config.max_body_size, 3_145_728);
    }

    #[test]
    fn load_config_cli_overrides_toml() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let toml_content = r#"listen_addr = "127.0.0.1:3000""#;
        let config_path = write_temp_toml(tmp.path(), toml_content);
        let args = CliArgs {
            config: config_path,
            listen_addr: Some("10.0.0.1:8000".into()),
            ..CliArgs::default()
        };
        let config = load_config(&args).expect("load_config");
        assert_eq!(config.listen_addr, "10.0.0.1:8000");
    }

    // -- TOML deserialization tests (NodeConfig) --

    #[test]
    fn toml_deserializes_all_node_config_fields() {
        let toml_content = r#"
            node_id = "toml-node"
            listen_addr = "0.0.0.0:7000"
            grpc_listen_addr = "0.0.0.0:7001"
            log_level = "debug"
            metrics_enabled = true
            s3_auth_enabled = true
            prefetch_enabled = true
            max_body_size = 4194304
            gc_interval_sec = 1800
            tombstone_ttl_sec = 86400
            ae_interval_sec = 150
            scrub_interval_sec = 3600
            orphan_reaper_interval_sec = 900
            vnodes_per_node = 512
            replication_factor = 5
            pool_size_per_peer = 8
            keepalive_sec = 60
            connect_timeout_ms = 10000
            request_timeout_ms = 60000

            [gossip]
            interval_ms = 500
            suspicion_timeout_ms = 10000
            failure_timeout_ms = 30000
            indirect_ping_count = 5
            seed_nodes = ["peer1:9001", "peer2:9001"]
        "#;
        let config: NodeConfig = toml::from_str(toml_content).expect("deserialize");
        assert_eq!(config.node_id, "toml-node");
        assert_eq!(config.listen_addr, "0.0.0.0:7000");
        assert_eq!(config.gc_interval_sec, 1800);
        assert_eq!(config.max_body_size, 4_194_304);
        assert!(config.prefetch_enabled);
        assert!(config.s3_auth_enabled);
        assert_eq!(config.vnodes_per_node, 512);
        assert_eq!(config.replication_factor, 5);
        assert_eq!(config.pool_size_per_peer, 8);
        assert_eq!(config.gossip.interval_ms, 500);
        assert_eq!(config.gossip.suspicion_timeout_ms, 10000);
        assert_eq!(config.gossip.failure_timeout_ms, 30000);
        assert_eq!(config.gossip.indirect_ping_count, 5);
        assert_eq!(config.gossip.seed_nodes, vec!["peer1:9001", "peer2:9001"]);
    }

    #[test]
    fn toml_deserializes_segment_config() {
        use oceanfs_core::SegmentSizeConfig;
        let toml_content = r#"
            inline_threshold_bytes = 8192
            small_threshold_bytes = 524288
            small_target_size = 131072
            default_target_size = 8388608
        "#;
        let config: SegmentSizeConfig = toml::from_str(toml_content).expect("deserialize");
        assert_eq!(config.inline_threshold_bytes, 8192);
        assert_eq!(config.small_threshold_bytes, 524_288);
        assert_eq!(config.small_target_size, 131_072);
        assert_eq!(config.default_target_size, 8_388_608);
    }
}
