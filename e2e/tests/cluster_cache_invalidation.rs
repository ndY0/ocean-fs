//! Cluster cache invalidation tests (T36-T37).
//!
//! Validates remote cache invalidation on write and delete operations
//! across the cluster.

use e2e::harness::{config_3node_w2_r2, response_bytes, Cluster};

// ---------------------------------------------------------------------------
// T36: Remote cache invalidation on write
// ---------------------------------------------------------------------------

/// T36: Node A has object in L1 cache. Node B PUTs a new version.
/// Node A's L1 cache is invalidated via gRPC `CacheInvalidate`.
/// Node A's next GET returns the new version, not the stale cache.
#[tokio::test]
async fn t36_remote_cache_invalidation_on_write() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    let bucket = "cache-inv";
    cluster.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");

    let key = "versioned.txt";
    let v1_body = b"Version 1 - initial write";

    // Write v1 via node 0.
    let put_resp = cluster.put(0, &format!("/{bucket}/{key}"), v1_body).await.expect("PUT");
    assert_eq!(put_resp.status(), 200);

    // Read v1 from node 0 to populate its L1 cache.
    let get_resp = cluster.get(0, &format!("/{bucket}/{key}")).await.expect("GET");
    assert_eq!(get_resp.status(), 200);
    let read_body = response_bytes(get_resp).await;
    assert_eq!(read_body, v1_body, "first read should return v1");

    // Write v2 via node 1 (different node).
    let v2_body = b"Version 2 - updated write";
    let put_resp = cluster.put(1, &format!("/{bucket}/{key}"), v2_body).await.expect("PUT v2");
    assert_eq!(put_resp.status(), 200, "PUT v2 should succeed");

    // Wait for cache invalidation to propagate.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Read from node 0 again. Cache must have been invalidated.
    // Must return v2, NOT stale v1 from L1 cache.
    let get_resp = cluster.get(0, &format!("/{bucket}/{key}")).await.expect("GET after v2");
    assert_eq!(get_resp.status(), 200, "GET after remote write must return 200");
    let read_body = response_bytes(get_resp).await;
    assert_eq!(
        read_body,
        v2_body,
        "cache must be invalidated after remote write: node 0 must return v2, not v1. \
         Got: {:?} (expected: {:?})",
        String::from_utf8_lossy(&read_body),
        String::from_utf8_lossy(v2_body)
    );

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T37: Remote cache invalidation on delete
// ---------------------------------------------------------------------------

/// T37: Node A has object in L1 cache. Node B DELETEs it.
/// Node A's cache invalidated. Node A's next GET returns 404.
#[tokio::test]
async fn t37_remote_cache_invalidation_on_delete() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    let bucket = "cache-del";
    cluster.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");

    let key = "todelete.txt";
    let body = b"Will be deleted - cache test";

    // Write via node 0.
    let put_resp = cluster.put(0, &format!("/{bucket}/{key}"), body).await.expect("PUT");
    assert_eq!(put_resp.status(), 200);

    // Read from node 0 to populate cache.
    let get_resp = cluster.get(0, &format!("/{bucket}/{key}")).await.expect("GET");
    assert_eq!(get_resp.status(), 200);

    // Delete via node 1.
    let del_resp = cluster.delete(1, &format!("/{bucket}/{key}")).await.expect("DELETE");
    let del_status = del_resp.status().as_u16();
    assert!(
        del_status == 200 || del_status == 204,
        "DELETE must return 200 or 204, got {del_status}"
    );

    // Wait for cache invalidation and tombstone propagation.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Read from node 0. Cache must be invalidated; must return 404.
    let get_resp = cluster.get(0, &format!("/{bucket}/{key}")).await.expect("GET after delete");
    assert_eq!(
        get_resp.status().as_u16(),
        404,
        "cache must be invalidated after remote delete: node 0 must return 404, got {}",
        get_resp.status()
    );

    cluster.shutdown().await.expect("shutdown");
}
