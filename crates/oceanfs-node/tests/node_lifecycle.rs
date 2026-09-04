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
    /// ADR-0031 (f1): mandatory role-complete pool topology for tests — one
    /// data (id 0), one wal, one metadata, one hints pool on sibling roots.
    fn storage_pools(tmp: &tempfile::TempDir) -> oceanfs_core::StorageConfig {
        fn pool(
            name: &str,
            role: oceanfs_core::PoolRole,
            root: std::path::PathBuf,
        ) -> oceanfs_core::StoragePoolConfig {
            oceanfs_core::StoragePoolConfig {
                name: name.into(),
                role,
                root,
                weight: None,
                tech: Default::default(),
                health: Default::default(),
            }
        }
        oceanfs_core::StorageConfig {
            pools: vec![
                pool("data-0", oceanfs_core::PoolRole::Data, tmp.path().join("pool-data")),
                pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.path().join("pool-wal")),
                pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.path().join("pool-meta")),
                pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.path().join("pool-hints")),
            ],
            missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
        }
    }

    let config = NodeConfig {
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),      // ephemeral port
        grpc_listen_addr: "127.0.0.1:0".into(), // ephemeral port
        storage: storage_pools(&tmp),
        event_wal: oceanfs_core::EventWalConfig {
            event_wal_dir: tmp.path().join("event-wal"),
            ..Default::default()
        },
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
