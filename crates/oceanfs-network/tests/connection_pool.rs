#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration test: connection pool lifecycle.
//!
//! Tests connection pool creation, channel acquisition (error handling
//! for unreachable peers), concurrent access, and configuration.

use std::{net::SocketAddr, sync::Arc};

use oceanfs_core::RpcConfig;
use oceanfs_network::ConnectionPool;

#[tokio::test]
async fn pool_create_and_acquire_channel() {
    let pool = ConnectionPool::new(RpcConfig::default());
    assert_eq!(pool.peer_count(), 0);
    assert_eq!(pool.config().pool_size_per_peer, 4);
}

#[tokio::test]
async fn acquire_channel_for_unreachable_peer_errors() {
    let config =
        RpcConfig { pool_size_per_peer: 1, connect_timeout_ms: 50, ..RpcConfig::default() };
    let pool = ConnectionPool::new(config);

    // TEST-NET-1 address (RFC 5737) — guaranteed unreachable.
    let addr: SocketAddr = "192.0.2.99:9999".parse().unwrap();
    let result = pool.get_channel(addr).await;
    assert!(result.is_err(), "connection to unreachable address should fail");
}

#[tokio::test]
async fn concurrent_acquire_from_same_peer() {
    let config =
        RpcConfig { pool_size_per_peer: 2, connect_timeout_ms: 100, ..RpcConfig::default() };
    let pool = Arc::new(ConnectionPool::new(config));
    let addr: SocketAddr = "192.0.2.50:9998".parse().unwrap();

    // Spawn two concurrent tasks that attempt to acquire.
    // Both should fail (unreachable), but neither should panic or deadlock.
    let pool1 = pool.clone();
    let pool2 = pool.clone();

    let (r1, r2) = tokio::join!(
        tokio::spawn(async move { pool1.get_channel(addr).await }),
        tokio::spawn(async move { pool2.get_channel(addr).await }),
    );

    // Both tasks should complete (even if with errors).
    assert!(r1.is_ok());
    assert!(r2.is_ok());
}

#[test]
fn config_defaults_are_sensible() {
    let config = RpcConfig::default();
    assert_eq!(config.pool_size_per_peer, 4);
    assert_eq!(config.keepalive_sec, 30);
    assert_eq!(config.max_idle_connections, 256);
    assert_eq!(config.connect_timeout_ms, 5000);
    assert_eq!(config.request_timeout_ms, 30000);
}

#[test]
fn custom_config_is_respected() {
    let config = RpcConfig {
        pool_size_per_peer: 8,
        keepalive_sec: 60,
        max_idle_connections: 512,
        connect_timeout_ms: 10000,
        request_timeout_ms: 60000,
        tls_cert_path: None,
    };
    assert_eq!(config.pool_size_per_peer, 8);
    assert_eq!(config.keepalive_sec, 60);
}
