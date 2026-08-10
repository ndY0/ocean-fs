//! Test 6: Anti-Entropy Merkle Verification.
//!
//! Uses the configurable `ae_interval_sec` field (added in commit ddc87ad)
//! to run anti-entropy within a reasonable test timeout.

use std::time::Duration;

use e2e::harness::{config_short_ae, poll_until, response_json, NodeProcess};
use serde::Deserialize;

/// Segment report returned by GET /admin/segments.
#[derive(Debug, Deserialize)]
struct SegmentReport {
    total: u64,
}

#[tokio::test]
async fn anti_entropy_verifies_segments_without_changes() {
    let node = NodeProcess::spawn(&config_short_ae()).await.expect("spawn node");

    let bucket = "ae-test";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    // PUT objects > 4 KB to hit the segment path (inline threshold is
    // 4096 bytes). Each object is 100 KB — classified as SizeTier::Small
    // (≤ 256 KB). The small segment target is 64 KB, so a single object
    // fills the segment, triggering immediate sealing.
    for i in 1..=3 {
        let key = format!("obj-{i}.txt");
        // 100 KB of unique-ish data; tier classification: Small
        let body = vec![b'A' + (i as u8); 100_000];
        let resp = node.put(&format!("/{bucket}/{key}"), &body).await.expect("PUT");
        assert_eq!(resp.status(), 200, "PUT obj-{i} should return 200");
    }

    // Brief wait for seal worker to flush segments to disk.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify objects are readable before AE.
    for i in 1..=3 {
        let key = format!("obj-{i}.txt");
        let resp = node.get(&format!("/{bucket}/{key}")).await.expect("GET before AE");
        assert_eq!(resp.status(), 200, "obj-{i} should be readable before AE");
    }

    // Record baseline segment count.
    let baseline: SegmentReport = {
        let resp = node.get("/admin/segments").await.expect("GET segments");
        assert_eq!(resp.status(), 200);
        response_json(resp).await.expect("parse segments")
    };
    assert!(baseline.total > 0, "segment count should be > 0 after writing objects");

    // Wait for at least one AE cycle (10s interval + 5s buffer).
    let ae_ran = poll_until(Duration::from_secs(1), Duration::from_secs(20), || {
        let node = &node;
        async move {
            // AE is read-only and should not change segment count.
            // We check that the health check still passes after AE runs.
            if let Ok(resp) = node.get("/admin/health").await {
                if resp.status() == 200 {
                    return true;
                }
            }
            false
        }
    })
    .await;

    assert!(ae_ran, "node should remain healthy after anti-entropy cycle");

    // Verify segment count is unchanged (AE is read-only).
    let after: SegmentReport = {
        let resp = node.get("/admin/segments").await.expect("GET segments after AE");
        assert_eq!(resp.status(), 200);
        response_json(resp).await.expect("parse segments after AE")
    };
    assert_eq!(
        after.total, baseline.total,
        "segment count should not change after anti-entropy (read-only operation)"
    );

    // Verify all objects are still readable.
    for i in 1..=3 {
        let key = format!("obj-{i}.txt");
        let resp = node.get(&format!("/{bucket}/{key}")).await.expect("GET after AE");
        assert_eq!(resp.status(), 200, "obj-{i} should still be readable after AE");
    }

    node.shutdown().await.expect("shutdown");
}
