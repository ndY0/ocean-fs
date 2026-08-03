//! Test 4: Garbage Collection.
//!
//! **BLOCKER**: The current binary hardcodes the GC interval at 3600 seconds
//! and tombstone TTL at 259200 seconds (in `oceanfs-node/src/node.rs`).
//! The `NodeConfig` struct does not expose `gc_interval_sec` or
//! `tombstone_ttl_sec` fields.
//!
//! Until these configuration options are added to `NodeConfig` and wired
//! through the composition root, this test cannot validate GC within a
//! reasonable test timeout.
//!
//! ## Proposed Fix
//!
//! 1. Add `gc_interval_sec: u64` and `tombstone_ttl_sec: u64` to
//!    `oceanfs_core::NodeConfig`.
//! 2. In `oceanfs_node::Node::spawn_background_tasks`, use these config
//!    values instead of hardcoded `Duration::from_secs(3600)`.
//! 3. Update `GcConfig` in `oceanfs_storage` to use the config values.
//!
//! ## Test Plan (when blocker is resolved)
//!
//! ```text
//! 1. Start node with gc_interval_sec=10, tombstone_ttl_sec=5
//! 2. PUT several objects
//! 3. Record baseline segment count
//! 4. DELETE some objects
//! 5. Poll /admin/segments until segment count decreases
//! 6. Assert live objects still readable, deleted return 404
//! ```

use e2e::harness::{config_short_gc, response_json, NodeProcess};
use serde::Deserialize;

/// Segment report returned by GET /admin/segments.
#[derive(Debug, Deserialize)]
struct SegmentReport {
    total: u64,
}

#[tokio::test]
async fn garbage_collection_deferred() {
    // This test documents the GC blocker and performs a basic
    // smoke check that the node starts with GC enabled (even
    // though we can't test the short cycle).
    let node = NodeProcess::spawn(&config_short_gc()).await.expect("spawn node");

    // Basic health check.
    let resp = node.get("/admin/health").await.expect("health check");
    assert_eq!(resp.status(), 200);

    // Create a bucket.
    let bucket = "gc-test";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    // PUT an object.
    let resp = node.put(&format!("/{bucket}/keep.txt"), b"important data").await.expect("PUT");
    assert_eq!(resp.status(), 200);

    // DELETE it.
    let resp = node.delete(&format!("/{bucket}/keep.txt")).await.expect("DELETE");
    assert_eq!(resp.status(), 204);

    // Check segment report (basic sanity).
    let report: SegmentReport = {
        let resp = node.get("/admin/segments").await.expect("GET segments");
        response_json(resp).await.expect("parse segments")
    };
    // report.total is a u64, always >= 0. Just verify it parsed.
    let _ = report.total;

    // NOTE: We cannot test segment count decrease because GC runs
    // every 3600s by default. See the file-level blocker docs above.

    node.shutdown().await.expect("shutdown");
}
