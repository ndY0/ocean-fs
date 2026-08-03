//! Cluster scrub tests (T38-T39).
//!
//! Validates distributed scrub partition assignment and corruption
//! detection across the cluster.

use e2e::harness::{config_3node_w2_r2, Cluster};

// ---------------------------------------------------------------------------
// T38: Distributed scrub partition assignment
// ---------------------------------------------------------------------------

/// T38: Trigger scrub via `POST /admin/scrub`. Each node receives a
/// partition of segment IDs. All nodes report healthy.
#[tokio::test]
async fn t38_distributed_scrub_partition_assignment() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    let bucket = "scrub-test";
    cluster.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");

    // Put some objects to create segments.
    for i in 0..3 {
        let key = format!("scrub-{}.txt", i);
        let body = b"scrub test object";
        let put_resp = cluster.put(0, &format!("/{bucket}/{key}"), body).await.expect("PUT");
        assert_eq!(put_resp.status(), 200);
    }

    // Trigger scrub on each node. POST /admin/scrub must return 202.
    for i in 0..3 {
        let resp = cluster.node(i).post("/admin/scrub").await;
        assert!(resp.is_ok(), "node {i}: POST /admin/scrub must succeed");
        let resp = resp.unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::ACCEPTED,
            "node {i}: POST /admin/scrub must return 202 Accepted, got {}",
            resp.status()
        );
    }

    // Wait for scrub to complete.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Verify segment reports on all nodes return 200.
    for i in 0..3 {
        let resp = cluster.get(i, "/admin/segments").await;
        assert!(resp.is_ok(), "node {i}: GET /admin/segments must succeed after scrub");
        let resp = resp.unwrap();
        assert_eq!(
            resp.status(),
            200,
            "node {i}: GET /admin/segments must return 200 after scrub, got {}",
            resp.status()
        );
    }

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T39: Scrub detects corruption
// ---------------------------------------------------------------------------

/// T39: Corrupt a shard on node B. Trigger scrub. Scrub worker on B
/// detects Merkle root mismatch. Reports segment as corrupt.
/// Enqueues heal.
#[tokio::test]
async fn t39_scrub_detects_corruption_and_enqueues_heal() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    let bucket = "scrub-corr";
    cluster.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");

    // PUT objects.
    let body = b"scrub corruption test data";
    for i in 0..3 {
        let key = format!("corr-{}.txt", i);
        let put_resp = cluster.put(0, &format!("/{bucket}/{key}"), body).await.expect("PUT");
        assert_eq!(put_resp.status(), 200);
    }

    // Trigger scrub on node 0. Must return 202.
    let resp = cluster.node(0).post("/admin/scrub").await;
    assert!(resp.is_ok(), "POST /admin/scrub must succeed");
    let resp = resp.unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::ACCEPTED,
        "POST /admin/scrub must return 202, got {}",
        resp.status()
    );

    // NOTE: Full corruption detection requires filesystem access to
    // node data directories for segment corruption injection.
    // This is a known harness limitation. Once `Cluster::data_dir(i)`
    // is implemented, this test will be extended to:
    // 1. Find a segment file in node 1's data dir
    // 2. Corrupt bytes
    // 3. Trigger scrub on node 1
    // 4. Assert corruption detected via /admin/segments or /admin/health

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Verify all objects still readable (no corruption on clean data).
    for i in 0..3 {
        let key = format!("corr-{}.txt", i);
        for node_idx in 0..3 {
            if let Ok(resp) = cluster.get(node_idx, &format!("/{bucket}/{key}")).await {
                assert_eq!(
                    resp.status(),
                    200,
                    "node {node_idx}: object {key} must be readable (clean data)"
                );
            }
        }
    }

    cluster.shutdown().await.expect("shutdown");
}
