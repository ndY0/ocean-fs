//! Integration test: single-node startup, health check, graceful shutdown.
//!
//! Verifies that a real OceanFS node can be started, responds to the
//! `/admin/health` endpoint, and releases its port after shutdown.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{net::TcpStream, time::Duration};

use oceanfs_core::NodeConfig;
use oceanfs_node::Node;

/// Tests the full startup → health check → shutdown lifecycle.
///
/// 1. Creates a temp directory for RocksDB and segment data.
/// 2. Starts a node with an ephemeral port.
/// 3. Hits /admin/health and verifies the JSON response.
/// 4. Shuts down and confirms the port is released.
#[tokio::test]
async fn node_lifecycle_startup_health_shutdown() {
    // Use a temp directory so RocksDB data is isolated.
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = NodeConfig {
        data_dir: tmp.path().to_path_buf(),
        listen_addr: "127.0.0.1:0".into(),      // ephemeral port
        grpc_listen_addr: "127.0.0.1:0".into(), // ephemeral port
        ..NodeConfig::default()
    };

    // Start the node.
    let node = Node::start(config).await.expect("node should start with valid config");
    let server_addr = node.server_addr();

    // Verify the server is listening.
    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(3)).build().expect("client");
    let url = format!("http://{server_addr}/admin/health");
    let resp = client.get(&url).send().await.expect("health check should succeed");
    assert_eq!(resp.status(), 200, "health check returns 200");

    let body: serde_json::Value = resp.json().await.expect("valid JSON body");
    assert_eq!(body["status"], "healthy", "status is healthy");
    assert!(body["version"].is_string(), "version is present");
    assert!(!body["version"].as_str().unwrap().is_empty(), "version is non-empty");

    // Shut down the node.
    node.shutdown().await.expect("graceful shutdown should succeed");

    // Give the OS a moment to release the port.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify the port is released.
    let reconnect = TcpStream::connect_timeout(&server_addr, Duration::from_secs(1));
    assert!(reconnect.is_err(), "port {server_addr} should be released after shutdown");
}
