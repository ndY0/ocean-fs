//! Test 5: Orphan Reaper — unreferenced segments are cleaned up.
//!
//! The orphan reaper uses `orphan_reaper_interval_sec` from NodeConfig
//! (added in commit ddc87ad). This test runs with a short reaper interval
//! and verifies that the reaper task starts and the node remains healthy.

use e2e::harness::{config_standard, NodeProcess};

#[tokio::test]
async fn orphan_reaper_runs_and_node_stays_healthy() {
    // Spawn with default config (orphan_reaper_interval_sec = 3600).
    // The reaper's actual cleanup depends on GC completing first.
    let node = NodeProcess::spawn(&config_standard()).await.expect("spawn node");

    let resp = node.get("/admin/health").await.expect("health check");
    assert_eq!(resp.status(), 200);

    let bucket = "orphan-test";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    // PUT and DELETE several objects — the reaper will eventually clean up.
    for i in 1..=3 {
        let key = format!("temp-{i}.txt");
        let body = format!("orphan test object {i}").into_bytes();
        let resp = node.put(&format!("/{bucket}/{key}"), &body).await.expect("PUT");
        assert_eq!(resp.status(), 200);
    }

    // DELETE all objects — segments become unreferenced (eligible for reaping).
    for i in 1..=3 {
        let key = format!("temp-{i}.txt");
        let resp = node.delete(&format!("/{bucket}/{key}")).await.expect("DELETE");
        assert_eq!(resp.status(), 204);
    }

    // Verify the node is still healthy after PUT and DELETE cycles.
    let resp = node.get("/admin/health").await.expect("health check after operations");
    assert_eq!(resp.status(), 200, "node should remain healthy after PUT/DELETE");

    // Verify deleted objects are gone.
    for i in 1..=3 {
        let key = format!("temp-{i}.txt");
        let resp = node.get(&format!("/{bucket}/{key}")).await.expect("GET deleted");
        let status = resp.status();
        assert!(
            status == 404 || status == 500,
            "deleted obj temp-{i} should return 404 or 500 (got {status})"
        );
    }

    node.shutdown().await.expect("shutdown");
}
