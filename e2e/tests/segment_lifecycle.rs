//! Test 3: Segment lifecycle — size tiers produce the correct segments.
//!
//! Verifies that objects of different sizes (inline, small, standard, multi)
//! are stored correctly and the segment report reflects them.
//!
//! ## Inline-tier design (ADR-0001)
//!
//! Objects ≤ 4 KB (`inline` tier, see `oceanfs-core` `classify`) are stored
//! directly in object metadata with empty chunk lists — the write path
//! intentionally registers **no segment** for them. Therefore
//! `/admin/segments` (built from `list_segments()`) never reports an
//! `inline` tier entry: this test asserts exactly that, instead of the old
//! "one segment per PUT" assumption. The 1.5 MB blob lands in the
//! `standard` tier (`multi` starts above 4 MB), so no `multi` entry is
//! assumed either.

use e2e::harness::{config_standard, random_bytes, response_json, NodeProcess};
use serde::Deserialize;

/// Segment report returned by GET /admin/segments.
#[derive(Debug, Deserialize)]
struct SegmentReport {
    total: u64,
    #[allow(dead_code)]
    sealed: u64,
    #[allow(dead_code)]
    unsealed: u64,
    #[allow(dead_code)]
    encoding: u64,
    #[serde(default)]
    #[allow(dead_code)]
    by_tier: std::collections::HashMap<String, u64>,
}

#[tokio::test]
async fn segment_lifecycle_all_four_tiers() {
    let node = NodeProcess::spawn(&config_standard()).await.expect("spawn node");

    let bucket = "segment-life";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    // PUT objects of four different sizes:
    // 1. Inline: ~15 bytes
    // 2. Small: ~100 KB
    // 3. Standard: ~1 MB
    // 4. Large (multi-segment): ~1.5 MB (below the default 2MB body limit)
    let small_100k = random_bytes(100 * 1024);
    let std_1m = random_bytes(1024 * 1024);
    let big_1_5m = random_bytes(1536 * 1024); // 1.5 MB

    // Inline (tiny)
    let resp =
        node.put(&format!("/{bucket}/inline.txt"), b"Hello inline!").await.expect("PUT inline");
    assert_eq!(resp.status(), 200);

    // Small (100 KB)
    let resp = node.put(&format!("/{bucket}/small.bin"), &small_100k).await.expect("PUT small");
    assert_eq!(resp.status(), 200);

    // Standard (1 MB)
    let resp = node.put(&format!("/{bucket}/std.bin"), &std_1m).await.expect("PUT standard");
    assert_eq!(resp.status(), 200);

    // Large (1.5 MB)
    let resp = node.put(&format!("/{bucket}/big.bin"), &big_1_5m).await.expect("PUT large");
    assert_eq!(resp.status(), 200);

    // Check segment report: assert that segments were created for
    // the blobs we just PUT (segment metadata is persisted via the S3 handler).
    let report: SegmentReport = {
        let resp = node.get("/admin/segments").await.expect("GET segments");
        assert_eq!(resp.status(), 200, "GET /admin/segments should return 200");
        response_json(resp).await.expect("parse segments")
    };
    assert!(
        report.total > 0,
        "segment total should be > 0 after writing blobs (got {})",
        report.total
    );
    // Verify per-tier breakdown includes entries.
    eprintln!("segment_report: total={}, by_tier={:?}", report.total, report.by_tier);
    // The inline blob (≤ 4 KB) is stored in metadata with no segment —
    // assert the DESIGN, not a phantom segment entry (ADR-0001).
    assert_eq!(
        report.by_tier.get("inline").copied().unwrap_or(0),
        0,
        "inline-tier blobs create no segment records"
    );
    assert!(report.by_tier.contains_key("small"), "by_tier should include small tier");
    assert!(
        report.by_tier.get("standard").copied().unwrap_or(0) > 0,
        "by_tier should include standard tier with count > 0"
    );

    // Read back all objects and verify sizes.
    for (key, expected_size) in &[
        ("inline.txt", 13usize),
        ("small.bin", 100 * 1024),
        ("std.bin", 1024 * 1024),
        ("big.bin", 1536 * 1024),
    ] {
        let resp = node.get(&format!("/{bucket}/{key}")).await.expect("GET");
        assert_eq!(resp.status(), 200, "GET {key} should return 200");

        let content_length: usize = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        assert_eq!(content_length, *expected_size, "content-length mismatch for {key}");

        // Also verify with HEAD.
        let head_resp = node.head(&format!("/{bucket}/{key}")).await.expect("HEAD");
        assert_eq!(head_resp.status(), 200, "HEAD {key} should return 200");

        let head_len: usize = head_resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        assert_eq!(head_len, *expected_size, "HEAD content-length mismatch for {key}");
    }

    node.shutdown().await.expect("shutdown");
}
