//! Remote-target mode integration test.
//!
//! Validates the ADR-0019 remote-target path (`RemoteCluster`) against a
//! locally spawned node: connecting by host:port, scraping metrics,
//! running the load generator, and verifying the manifest — everything
//! the Phase 2 cloud full mode does over the network, minus the network.
//! In the two-VM topology the same code path targets `TARGET_HOST`.
//!
//! ```bash
//! cargo test -p e2e --test remote_target_mode -- --test-threads=1
//! ```

use std::{sync::Arc, time::Duration};

use e2e::{
    harness::{config_sustained, Cluster, NodeOptions},
    load::{
        BlobSizeDist, KeySpace, LoadScenario, Manifest, MetricsSnapshot, OpWeight, Operation,
        Orchestrator,
    },
    remote::RemoteCluster,
};

#[tokio::test(flavor = "multi_thread")]
async fn remote_target_connects_and_runs_load() {
    // Spawn a local node and connect to it as a REMOTE target — the
    // remote code path must not know or care that the process is local.
    let cluster = Cluster::spawn_with_options(1, &config_sustained(), &NodeOptions::default())
        .await
        .expect("spawn local node");
    let addr = cluster.node_http_addr(0);

    let remote = RemoteCluster::connect(&addr.to_string()).expect("connect remote target");
    remote.wait_for_health(Duration::from_secs(10)).await.expect("remote health");
    assert_eq!(remote.len(), 1);

    // Metrics scraping through the remote path.
    let snap = MetricsSnapshot::scrape(&remote, 0).await.expect("remote metrics scrape");
    assert!(
        snap.gauge("process_resident_memory_bytes").is_some(),
        "remote scrape must include process gauges"
    );

    // A short load run through the remote path (PUTs + GETs).
    let manifest = Arc::new(Manifest::new());
    let scenario = LoadScenario {
        concurrency: 4,
        duration: Duration::from_secs(10),
        operations: vec![
            OpWeight { op: Operation::Put, weight: 0.5 },
            OpWeight { op: Operation::Get, weight: 0.5 },
        ],
        blob_sizes: BlobSizeDist::Fixed(64 * 1024),
        key_space: KeySpace::Sequential { prefix: "remote".to_string(), start: 0, count: 50 },
        seed: 42,
    };
    let stats = Orchestrator::run(scenario, Arc::new(remote), Arc::clone(&manifest)).await;
    assert!(stats.ops_total > 0, "remote load must complete operations");

    // Manifest verification through the remote path (the Arc was moved
    // into the orchestrator; connect a fresh handle to the same target).
    let remote2 = RemoteCluster::connect(&addr.to_string()).expect("reconnect remote target");
    let summary = manifest.verify_summary(&remote2).await;
    assert_eq!(
        summary.mismatches, 0,
        "all objects written via the remote path must verify: {}",
        summary.mismatches
    );

    let _ = cluster.shutdown().await;
}
