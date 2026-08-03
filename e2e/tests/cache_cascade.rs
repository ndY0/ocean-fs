//! Test 1: Cache cascade — L1 object cache hit/miss behavior.
//!
//! Verifies that consecutive GET requests to the same object
//! result in L1 cache hits.

use e2e::harness::{config_standard, response_json, NodeProcess};
use serde::Deserialize;

/// Per-tier cache stats returned by GET /admin/caches.
#[derive(Debug, Deserialize)]
struct CacheStats {
    tier: String,
    hits: u64,
    #[allow(dead_code)]
    misses: u64,
}

#[tokio::test]
async fn cache_cascade_l1_hits_increase_on_repeated_gets() {
    let node = NodeProcess::spawn(&config_standard()).await.expect("spawn node");

    // Create a test bucket.
    let bucket = "cache-test";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    // PUT an object.
    let key = "hello.txt";
    let body = b"Hello, OceanFS!";
    let put_resp = node.put(&format!("/{bucket}/{key}"), body).await.expect("PUT");
    assert_eq!(put_resp.status(), 200, "PUT should return 200");

    // Record baseline cache stats.
    let baseline: Vec<CacheStats> = {
        let resp = node.get("/admin/caches").await.expect("GET caches");
        response_json(resp).await.expect("parse caches")
    };

    // GET the object twice to warm the L1 cache.
    for _ in 0..2 {
        let resp = node.get(&format!("/{bucket}/{key}")).await.expect("GET object");
        assert_eq!(resp.status(), 200, "GET should return 200");
    }

    // Record post-GET cache stats.
    let after: Vec<CacheStats> = {
        let resp = node.get("/admin/caches").await.expect("GET caches");
        response_json(resp).await.expect("parse caches")
    };

    // Find L1 stats and verify hits increased.
    let default_hit = CacheStats { tier: "l1".into(), hits: 0, misses: 0 };
    let baseline_l1 = baseline.iter().find(|s| s.tier == "l1").unwrap_or(&default_hit);
    let after_l1 =
        after.iter().find(|s| s.tier == "l1").expect("L1 cache tier must be present after GETs");
    assert!(
        after_l1.hits > baseline_l1.hits,
        "L1 hits should increase: baseline={}, after={}",
        baseline_l1.hits,
        after_l1.hits
    );

    // Verify all three cache tiers are reported.
    let tiers: Vec<&str> = after.iter().map(|s| s.tier.as_str()).collect();
    assert!(tiers.contains(&"l1"), "L1 cache should be reported");
    assert!(tiers.contains(&"l2"), "L2 cache should be reported");
    assert!(tiers.contains(&"l3"), "L3 cache should be reported");

    node.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cache_cascade_multiple_objects() {
    let node = NodeProcess::spawn(&config_standard()).await.expect("spawn node");

    let bucket = "multi-cache";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    // PUT two objects.
    for (key, data) in &[("a.txt", b"data-aaa" as &[u8]), ("b.txt", b"data-bbb" as &[u8])] {
        let resp = node.put(&format!("/{bucket}/{key}"), data).await.expect("PUT");
        assert_eq!(resp.status(), 200);
    }

    // Record baseline.
    let baseline: Vec<CacheStats> = {
        let resp = node.get("/admin/caches").await.expect("GET caches");
        response_json(resp).await.expect("parse caches")
    };

    // Access each object twice.
    for key in &["a.txt", "b.txt"] {
        for _ in 0..2 {
            let resp = node.get(&format!("/{bucket}/{key}")).await.expect("GET");
            assert_eq!(resp.status(), 200);
        }
    }

    let after: Vec<CacheStats> = {
        let resp = node.get("/admin/caches").await.expect("GET caches");
        response_json(resp).await.expect("parse caches")
    };

    let baseline_l1 = baseline.iter().find(|s| s.tier == "l1");
    let after_l1 = after.iter().find(|s| s.tier == "l1");
    if let (Some(bl1), Some(al1)) = (baseline_l1, after_l1) {
        assert!(al1.hits > bl1.hits, "L1 hits increased after repeated GETs");
    }

    node.shutdown().await.expect("shutdown");
}
