//! Test 7: Manual Scrub.
//!
//! Triggers scrub via `POST /admin/scrub` and verifies the endpoint
//! responds with 202 Accepted. Does not wait for scrub completion
//! (which would require the hardcoded 7-day scrub cycle to be
//! shortened).

use e2e::harness::{config_standard, NodeProcess};

#[tokio::test]
async fn manual_scrub_returns_202_accepted() {
    let node = NodeProcess::spawn(&config_standard()).await.expect("spawn node");

    // Create bucket and put some objects.
    let bucket = "scrub-test";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    for i in 1..=3 {
        let key = format!("obj-{i}.txt");
        let body = format!("scrub test object {i}").into_bytes();
        let resp = node.put(&format!("/{bucket}/{key}"), &body).await.expect("PUT");
        assert_eq!(resp.status(), 200);
    }

    // Trigger a manual scrub.
    let scrub_resp = node.post("/admin/scrub").await.expect("POST /admin/scrub");
    assert_eq!(scrub_resp.status(), 202, "POST /admin/scrub should return 202 Accepted");

    node.shutdown().await.expect("shutdown");
}
