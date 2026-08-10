//! Integration tests for environment variable overrides.
//!
//! Separated from the inline unit tests because `set_var` requires
//! `unsafe` (Rust 2024 edition), which is forbidden by the binary
//! crate but allowed in integration test crates.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use oceanfs::config::apply_env_overrides;
use oceanfs_core::NodeConfig;

#[test]
fn env_var_gc_interval_overrides_default() {
    // SAFETY: test is single-threaded, no other test manipulates this env var.
    unsafe {
        std::env::set_var("OCEANFS_GC_INTERVAL", "30");
    }
    let mut config = NodeConfig::default();
    apply_env_overrides(&mut config);
    assert_eq!(config.gc_interval_sec, 30);
    std::env::remove_var("OCEANFS_GC_INTERVAL");
}

#[test]
fn env_var_ae_interval_overrides_default() {
    // SAFETY: test is single-threaded, no other test manipulates this env var.
    unsafe {
        std::env::set_var("OCEANFS_AE_INTERVAL", "120");
    }
    let mut config = NodeConfig::default();
    apply_env_overrides(&mut config);
    assert_eq!(config.ae_interval_sec, 120);
    std::env::remove_var("OCEANFS_AE_INTERVAL");
}

#[test]
fn env_var_metrics_enabled_toggle() {
    // SAFETY: test is single-threaded, no other test manipulates this env var.
    unsafe {
        std::env::set_var("OCEANFS_METRICS_ENABLED", "false");
    }
    let mut config = NodeConfig::default();
    apply_env_overrides(&mut config);
    assert!(!config.metrics_enabled);
    std::env::remove_var("OCEANFS_METRICS_ENABLED");
}

#[test]
fn env_var_s3_auth_enabled_toggle() {
    // SAFETY: test is single-threaded, no other test manipulates this env var.
    unsafe {
        std::env::set_var("OCEANFS_S3_AUTH_ENABLED", "1");
    }
    let mut config = NodeConfig::default();
    apply_env_overrides(&mut config);
    assert!(config.s3_auth_enabled);
    std::env::remove_var("OCEANFS_S3_AUTH_ENABLED");
}

#[test]
fn env_var_max_body_size_overrides() {
    // SAFETY: test is single-threaded, no other test manipulates this env var.
    unsafe {
        std::env::set_var("OCEANFS_MAX_BODY_SIZE", "10485760");
    }
    let mut config = NodeConfig::default();
    apply_env_overrides(&mut config);
    assert_eq!(config.max_body_size, 10 * 1024 * 1024);
    std::env::remove_var("OCEANFS_MAX_BODY_SIZE");
}

#[test]
fn env_var_gossip_interval_ms_overrides() {
    // SAFETY: test is single-threaded, no other test manipulates this env var.
    unsafe {
        std::env::set_var("OCEANFS_GOSSIP_INTERVAL_MS", "200");
    }
    let mut config = NodeConfig::default();
    apply_env_overrides(&mut config);
    assert_eq!(config.gossip.interval_ms, 200);
    std::env::remove_var("OCEANFS_GOSSIP_INTERVAL_MS");
}

#[test]
fn env_var_suspicion_timeout_ms_overrides() {
    // SAFETY: test is single-threaded, no other test manipulates this env var.
    unsafe {
        std::env::set_var("OCEANFS_SUSPICION_TIMEOUT_MS", "7500");
    }
    let mut config = NodeConfig::default();
    apply_env_overrides(&mut config);
    assert_eq!(config.gossip.suspicion_timeout_ms, 7500);
    std::env::remove_var("OCEANFS_SUSPICION_TIMEOUT_MS");
}

#[test]
fn env_var_failure_timeout_ms_overrides() {
    // SAFETY: test is single-threaded, no other test manipulates this env var.
    unsafe {
        std::env::set_var("OCEANFS_FAILURE_TIMEOUT_MS", "20000");
    }
    let mut config = NodeConfig::default();
    apply_env_overrides(&mut config);
    assert_eq!(config.gossip.failure_timeout_ms, 20000);
    std::env::remove_var("OCEANFS_FAILURE_TIMEOUT_MS");
}

#[test]
fn env_var_scrub_interval_overrides() {
    // SAFETY: test is single-threaded, no other test manipulates this env var.
    unsafe {
        std::env::set_var("OCEANFS_SCRUB_INTERVAL", "43200");
    }
    let mut config = NodeConfig::default();
    apply_env_overrides(&mut config);
    assert_eq!(config.scrub_interval_sec, 43_200);
    std::env::remove_var("OCEANFS_SCRUB_INTERVAL");
}

#[test]
fn env_var_orphan_reaper_interval_overrides() {
    // SAFETY: test is single-threaded, no other test manipulates this env var.
    unsafe {
        std::env::set_var("OCEANFS_ORPHAN_REAPER_INTERVAL", "600");
    }
    let mut config = NodeConfig::default();
    apply_env_overrides(&mut config);
    assert_eq!(config.orphan_reaper_interval_sec, 600);
    std::env::remove_var("OCEANFS_ORPHAN_REAPER_INTERVAL");
}

#[test]
fn env_var_prefetch_enabled_true() {
    // SAFETY: test is single-threaded, no other test manipulates this env var.
    unsafe {
        std::env::set_var("OCEANFS_PREFETCH_ENABLED", "yes");
    }
    let mut config = NodeConfig::default();
    apply_env_overrides(&mut config);
    assert!(config.prefetch_enabled);
    std::env::remove_var("OCEANFS_PREFETCH_ENABLED");
}
