//! Test 2: Negative cache — DELETE inserts key into L3 Bloom filter.
//!
//! Verifies that after deleting an object, subsequent GETs return 404
//! and the L3 negative cache records hits.

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
async fn negative_cache_delete_then_get_returns_404() {
    let node = NodeProcess::spawn(&config_standard()).await.expect("spawn node");

    let bucket = "neg-cache";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    // PUT an ephemeral object.
    let key = "ephemeral.txt";
    let body = b"will be deleted";
    let put_resp = node.put(&format!("/{bucket}/{key}"), body).await.expect("PUT");
    assert_eq!(put_resp.status(), 200, "PUT should succeed");

    // Verify it exists.
    let get_resp = node.get(&format!("/{bucket}/{key}")).await.expect("GET");
    assert_eq!(get_resp.status(), 200, "GET before delete should return 200");

    // DELETE the object.
    let del_resp = node.delete(&format!("/{bucket}/{key}")).await.expect("DELETE");
    assert_eq!(del_resp.status(), 204, "DELETE should return 204");

    // GET should now return 404.
    let get_after_del = node.get(&format!("/{bucket}/{key}")).await.expect("GET after delete");
    assert_eq!(get_after_del.status(), 404, "GET after delete should return 404");

    // Second GET should also return 404 (negative cache hit).
    let second_get = node.get(&format!("/{bucket}/{key}")).await.expect("second GET");
    assert_eq!(second_get.status(), 404, "second GET after delete should return 404");

    // Check that L3 has been populated.
    let stats: Vec<CacheStats> = {
        let resp = node.get("/admin/caches").await.expect("GET caches");
        response_json(resp).await.expect("parse caches")
    };

    let l3 = stats
        .iter()
        .find(|s| s.tier == "l3")
        .expect("L3 cache tier should be reported in /admin/caches");
    assert!(
        l3.hits >= 1,
        "L3 negative cache should have at least 1 hit after DELETE + GET (got {})",
        l3.hits
    );

    node.shutdown().await.expect("shutdown");
}
