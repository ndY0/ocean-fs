//! Test 8: Heal Pipeline.
//!
//! **DEFERRED per DK-003**: The heal pipeline smoke test is conditional.
//! If `heal::enqueue_heal()` is exposed via the admin API, we test it
//! directly. Otherwise, heal is tested indirectly via scrub and
//! anti-entropy in cluster mode.
//!
//! The current admin API does not expose a direct heal endpoint.
//! Introducing data corruption for a smoke test is risky and complex.
//! Full end-to-end heal testing is deferred to cluster mode tests
//! where node failure naturally creates heal scenarios.

use e2e::harness::{config_standard, NodeProcess};

#[tokio::test]
async fn heal_pipeline_deferred() {
    // Verify the node starts. Heal worker runs as a background task
    // and will be exercised indirectly by other durability tests.
    let node = NodeProcess::spawn(&config_standard()).await.expect("spawn node");

    let resp = node.get("/admin/health").await.expect("health check");
    assert_eq!(resp.status(), 200);

    node.shutdown().await.expect("shutdown");
}
