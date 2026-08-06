//! OceanFS binary crate — library portion for integration test access.
//!
//! Exposes configuration loading utilities for integration testing.

#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    missing_docs
)]

/// Configuration loading and merging: CLI args, TOML, env vars.
pub mod config;
