//! OceanFS — distributed, orchestrator-free blob storage.
//!
//! Binary entrypoint. Parses CLI arguments, loads configuration,
//! initializes tracing, and starts the node. Handles graceful
//! shutdown on SIGTERM and SIGINT.

#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    missing_docs
)]

mod config;

use std::process;

use oceanfs_node::Node;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

/// OceanFS entrypoint.
///
/// 1. Parses CLI arguments.
/// 2. Loads and merges configuration (CLI > env > TOML > defaults).
/// 3. Initializes the tracing subscriber.
/// 4. Starts the OceanFS node.
/// 5. Waits for shutdown signal (SIGTERM or SIGINT).
/// 6. Gracefully shuts down the node.
#[tokio::main]
async fn main() {
    // Parse CLI arguments.
    let args = parse_args();

    // Initialize tracing early for diagnostics during startup.
    init_tracing(&args);

    info!(
        version = env!("CARGO_PKG_VERSION"),
        config = %args.config.display(),
        "OceanFS starting"
    );

    // Load configuration.
    let node_config = match config::load_config(&args) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {e}");
            process::exit(1);
        }
    };

    // Start the node.
    let node = match Node::start(node_config).await {
        Ok(n) => n,
        Err(e) => {
            error!("Failed to start OceanFS node: {e}");
            process::exit(1);
        }
    };

    info!(
        http_addr = %node.server_addr(),
        grpc_addr = %node.grpc_addr(),
        "OceanFS node is ready"
    );

    // Wait for shutdown signal.
    wait_for_shutdown().await;

    // Graceful shutdown.
    if let Err(e) = node.shutdown().await {
        error!("Error during shutdown: {e}");
        process::exit(1);
    }

    info!("OceanFS exited");
}

// ---------------------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------------------

/// Parses command-line arguments into a `CliArgs` struct.
///
/// Supports:
/// - `--config <path>`: Path to oceanfs.toml (default: `oceanfs.toml`)
/// - `--data-dir <path>`: Override data directory
/// - `--listen-addr <addr>`: Override HTTP listen address
/// - `--grpc-listen-addr <addr>`: Override gRPC listen address
/// - `--seed-nodes <n1,n2>`: Comma-separated seed node addresses
/// - `--log-format <fmt>`: "human" or "json"
/// - `--log-level <level>`: Log level (trace, debug, info, warn, error)
fn parse_args() -> config::CliArgs {
    let mut args = config::CliArgs::default();
    let raw: Vec<String> = std::env::args().collect();
    let mut i = 1;

    while i < raw.len() {
        match raw[i].as_str() {
            "--config" => {
                i += 1;
                if i < raw.len() {
                    args.config = std::path::PathBuf::from(&raw[i]);
                }
            }
            "--data-dir" => {
                i += 1;
                if i < raw.len() {
                    args.data_dir = Some(std::path::PathBuf::from(&raw[i]));
                }
            }
            "--listen-addr" => {
                i += 1;
                if i < raw.len() {
                    args.listen_addr = Some(raw[i].clone());
                }
            }
            "--grpc-listen-addr" => {
                i += 1;
                if i < raw.len() {
                    args.grpc_listen_addr = Some(raw[i].clone());
                }
            }
            "--seed-nodes" => {
                i += 1;
                if i < raw.len() {
                    args.seed_nodes = raw[i].split(',').map(|s| s.trim().to_string()).collect();
                }
            }
            "--log-format" => {
                i += 1;
                if i < raw.len() {
                    args.log_format = raw[i].clone();
                }
            }
            "--log-level" => {
                i += 1;
                if i < raw.len() {
                    args.log_level = Some(raw[i].clone());
                }
            }
            _ => {
                // Unknown flag; skip.
            }
        }
        i += 1;
    }

    args
}

// ---------------------------------------------------------------------------
// Tracing initialization
// ---------------------------------------------------------------------------

/// Initializes the tracing subscriber.
///
/// Supports human-readable and JSON output formats. The log level
/// can be overridden via the `--log-level` flag or `OCEANFS_LOG`
/// environment variable.
fn init_tracing(args: &config::CliArgs) {
    let default_filter = match &args.log_level {
        Some(level) => format!("oceanfs={level}"),
        None => "oceanfs=info".to_string(),
    };

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&default_filter));

    match args.log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt().json().with_env_filter(filter).with_target(false).init();
        }
        _ => {
            tracing_subscriber::fmt().with_env_filter(filter).with_target(false).init();
        }
    }
}

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

/// Waits for SIGTERM or SIGINT, then returns.
///
/// On Unix, registers signal handlers. On non-Unix platforms,
/// this is a no-op that returns immediately.
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal;

        let sigterm = async {
            if let Ok(mut s) = signal::unix::signal(signal::unix::SignalKind::terminate()) {
                s.recv().await;
            }
        };

        let sigint = async {
            if let Ok(mut s) = signal::unix::signal(signal::unix::SignalKind::interrupt()) {
                s.recv().await;
            }
        };

        tokio::select! {
            _ = sigterm => {
                info!("Received SIGTERM, initiating graceful shutdown");
            }
            _ = sigint => {
                info!("Received SIGINT, initiating graceful shutdown");
            }
        }
    }

    #[cfg(not(unix))]
    {
        // On non-Unix, wait for Ctrl+C.
        tokio::signal::ctrl_c().await.expect("failed to listen for Ctrl+C");
        info!("Received shutdown signal, initiating graceful shutdown");
    }
}
