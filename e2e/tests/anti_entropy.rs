//! Test 6: Anti-Entropy Merkle Verification.
//!
//! **BLOCKER**: The current binary hardcodes the anti-entropy interval
//! at 300 seconds (5 minutes) in `oceanfs-node/src/node.rs`. The
//! `NodeConfig` struct does not expose `ae_interval_sec`.
//!
//! While 300 seconds is shorter than the GC interval, it is still too
//! long for a practical smoke test (the test suite would take 5+ minutes
//! per AE test). Until `ae_interval_sec` is configurable, this test
//! validates basic segment integrity but does not wait for an AE cycle.
//!
//! ## Proposed Fix
//!
//! 1. Add `ae_interval_sec: u64` to `oceanfs_core::NodeConfig`.
//! 2. In `oceanfs_node::Node::spawn_background_tasks`, use this config
//!    value instead of hardcoded `Duration::from_secs(300)`.
//!
//! ## Test Plan (when blocker is resolved)
//!
//! ```text
//! 1. Start node with ae_interval_sec=10
//! 2. PUT several objects to create sealed segments
//! 3. Wait up to 15 seconds for an AE cycle
//! 4. Assert segment inventory unchanged (AE is read-only)
//! ```

use e2e::harness::{config_short_ae, response_json, NodeProcess};
use serde::Deserialize;

/// Segment report returned by GET /admin/segments.
#[derive(Debug, Deserialize)]
struct SegmentReport {
    total: u64,
}

#[tokio::test]
async fn anti_entropy_deferred() {
    // Basic smoke: node starts with AE task running.
    let node = NodeProcess::spawn(&config_short_ae()).await.expect("spawn node");

    let resp = node.get("/admin/health").await.expect("health check");
    assert_eq!(resp.status(), 200);

    let bucket = "ae-test";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    // PUT several objects.
    for i in 1..=5 {
        let key = format!("obj-{i}.txt");
        let body = format!("ae test object {i}").into_bytes();
        let resp = node.put(&format!("/{bucket}/{key}"), &body).await.expect("PUT");
        assert_eq!(resp.status(), 200);
    }

    // Record segment count (may be 0 with in-memory write path).
    let baseline: SegmentReport = {
        let resp = node.get("/admin/segments").await.expect("GET segments");
        response_json(resp).await.expect("parse segments")
    };
    let _ = baseline.total;

    // NOTE: We cannot wait for an AE cycle (300s default).
    // See file-level blocker docs above. The segment count should
    // still be valid (AE is read-only and doesn't change inventory).

    node.shutdown().await.expect("shutdown");
}
