//! OceanFS — distributed, orchestrator-free blob storage.
//!
//! Binary entrypoint. Parses CLI arguments, loads configuration,
//! initializes tracing, and starts the node.

#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    missing_docs
)]

/// OceanFS entrypoint.
///
/// 1. Initializes tracing subscriber.
/// 2. Loads `oceanfs.toml` configuration.
/// 3. Starts the OceanFS node.
fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "oceanfs=info".into()),
        )
        .init();

    tracing::info!("OceanFS v{} starting", env!("CARGO_PKG_VERSION"));
    tracing::info!("Phase 0 scaffold — node startup not yet implemented");
}
