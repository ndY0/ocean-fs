//! Test 5: Orphan Reaper.
//!
//! **BLOCKER**: Depends on the garbage collection test (Test 4), which
//! is blocked because GC interval is hardcoded at 3600s. Additionally,
//! the orphan reaper interval is hardcoded at 3600s in the composition
//! root (`oceanfs-node/src/node.rs`).
//!
//! Until configurable intervals are exposed, this test cannot validate
//! orphan reaping within a reasonable test timeout.
//!
//! ## Proposed Fix
//!
//! Same as Test 4 — add configurable intervals to `NodeConfig`.
//!
//! ## Test Plan (when blocker is resolved)
//!
//! ```text
//! 1. After GC test runs (segments decreased), verify orphan reaper
//!    cleaned up fully-dead segments
//! 2. Assert /admin/segments shows no segments with zero live objects
//! ```

use e2e::harness::{config_standard, NodeProcess};

#[tokio::test]
async fn orphan_reaper_deferred() {
    // This test documents the orphan reaper blocker. The orphan reaper
    // depends on GC completing, which needs configurable intervals.
    // We perform a basic smoke check that the node starts cleanly.
    let node = NodeProcess::spawn(&config_standard()).await.expect("spawn node");

    let resp = node.get("/admin/health").await.expect("health check");
    assert_eq!(resp.status(), 200);

    let bucket = "orphan-test";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    // PUT and DELETE — the reaper will (eventually) clean up.
    let resp = node.put(&format!("/{bucket}/temp.txt"), b"temporary").await.expect("PUT");
    assert_eq!(resp.status(), 200);

    let resp = node.delete(&format!("/{bucket}/temp.txt")).await.expect("DELETE");
    assert_eq!(resp.status(), 204);

    // NOTE: Cannot assert orphan cleanup within test timeout.
    // See file-level blocker docs above.

    node.shutdown().await.expect("shutdown");
}
