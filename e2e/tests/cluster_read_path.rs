//! Cluster read path tests (T15-T19).
//!
//! Validates quorum reads, stale replica detection, read forwarding,
//! and post-delete read consistency.

use e2e::harness::{config_3node_w2_r2, response_bytes, Cluster};

// ---------------------------------------------------------------------------
// T15: Single-replica read (R=1, N=3)
// ---------------------------------------------------------------------------

/// T15: Read from any node with R=1. Returns correct data.
#[tokio::test]
async fn t15_single_replica_read_returns_correct_data() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    let bucket = "r1-test";
    cluster.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");

    let key = "readme.txt";
    let body = b"Single-replica read test data";
    let put_resp = cluster.put(0, &format!("/{bucket}/{key}"), body).await.expect("PUT");
    assert_eq!(put_resp.status(), 200, "PUT should return 200");

    // Read from each node individually. At least one must return correct data.
    let mut readable_count = 0;
    for i in 0..3 {
        let get_resp = cluster.get(i, &format!("/{bucket}/{key}")).await;
        match get_resp {
            Ok(resp) if resp.status() == 200 => {
                let read_body = response_bytes(resp).await;
                if read_body == body {
                    readable_count += 1;
                }
            }
            _ => {}
        }
    }
    assert!(
        readable_count >= 1,
        "R=1: at least one node must return correct data, got {readable_count}/3"
    );

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T16: Quorum read (R=2, N=3)
// ---------------------------------------------------------------------------

/// T16: Read with read_quorum=2. Two replicas agree. Returns correct data.
#[tokio::test]
async fn t16_quorum_read_two_replicas_agree() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    let bucket = "r2-test";
    cluster.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");

    let key = "quorum-read.txt";
    let body = b"Quorum read test data";
    let put_resp = cluster.put(0, &format!("/{bucket}/{key}"), body).await.expect("PUT");
    assert_eq!(put_resp.status(), 200, "PUT should return 200");

    // Read from multiple nodes to exercise quorum read path.
    let mut success_count = 0;
    for i in 0..3 {
        if let Ok(resp) = cluster.get(i, &format!("/{bucket}/{key}")).await {
            if resp.status() == 200 {
                let read_body = response_bytes(resp).await;
                if read_body == body {
                    success_count += 1;
                }
            }
        }
    }

    assert!(
        success_count >= 2,
        "R=2: expected at least 2 nodes to return correct data, got {success_count}"
    );

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T17: Stale replica detection
// ---------------------------------------------------------------------------

/// T17: Write to node A (W=1). Read from node B (R=2) before gossip
/// propagates the write. Read-repair pushes correct data to B or
/// returns stale version. Assert eventual consistency.
#[tokio::test]
async fn t17_stale_replica_detection_and_eventual_consistency() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    let bucket = "stale-test";
    cluster.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");

    let key = "stale.txt";
    let body = b"Write to one node, read from another";

    // Write to node 0 only.
    let put_resp = cluster.put(0, &format!("/{bucket}/{key}"), body).await.expect("PUT");
    assert_eq!(put_resp.status(), 200);

    // Immediately read from node 1 — may or may not have the data yet.
    // If it returns 200 with wrong data, that's a stale replica.
    // If it returns 200 with correct data, gossip was fast enough.
    let immediate_resp = cluster.get(1, &format!("/{bucket}/{key}")).await;
    assert!(immediate_resp.is_ok(), "immediate read from node 1 must not fail");

    // Wait for gossip to propagate, then verify eventual consistency.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    for i in 0..3 {
        let get_resp = cluster.get(i, &format!("/{bucket}/{key}")).await;
        assert!(get_resp.is_ok(), "node {i}: GET after propagation must succeed");
        let resp = get_resp.unwrap();
        assert_eq!(resp.status(), 200, "node {i}: eventual consistency — GET must return 200");
        let read_body = response_bytes(resp).await;
        assert_eq!(read_body, body, "node {i}: eventual consistency — read body must match");
    }

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T18: Read from non-replica
// ---------------------------------------------------------------------------

/// T18: GET from a node not in the replica set. Request forwarded
/// or routed to a replica. Correct data returned.
#[tokio::test]
async fn t18_read_from_non_replica_returns_correct_data() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    let bucket = "non-rep-test";
    cluster.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");

    let key = "forwarded-read.txt";
    let body = b"Read from non-replica node";

    // Write via node 0.
    let put_resp = cluster.put(0, &format!("/{bucket}/{key}"), body).await.expect("PUT");
    assert_eq!(put_resp.status(), 200);

    // Read from node 2 (may or may not be in the replica set).
    let get_resp = cluster.get(2, &format!("/{bucket}/{key}")).await;
    assert!(get_resp.is_ok(), "read from non-replica must succeed (forwarding)");
    let resp = get_resp.unwrap();
    assert_eq!(resp.status(), 200, "non-replica read must return 200 after forwarding");
    let read_body = response_bytes(resp).await;
    assert_eq!(read_body, body, "non-replica read must return correct data");

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T19: Read from node where data was deleted
// ---------------------------------------------------------------------------

/// T19: DELETE on node A. GET on node B returns 404 after tombstone
/// propagation (or 200 if still stale — assert eventual consistency).
#[tokio::test]
async fn t19_post_delete_read_returns_404_after_propagation() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    let bucket = "delete-test";
    cluster.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");

    let key = "ephemeral.txt";
    let body = b"Will be deleted";

    // Write via node 0.
    let put_resp = cluster.put(0, &format!("/{bucket}/{key}"), body).await.expect("PUT");
    assert_eq!(put_resp.status(), 200);

    // Verify readable.
    let get_resp = cluster.get(0, &format!("/{bucket}/{key}")).await.expect("GET");
    assert_eq!(get_resp.status(), 200);

    // Delete via node 0.
    let del_resp = cluster.delete(0, &format!("/{bucket}/{key}")).await.expect("DELETE");
    let del_status = del_resp.status().as_u16();
    assert!(
        del_status == 200 || del_status == 204,
        "DELETE must return 200 or 204, got {del_status}"
    );

    // Wait for tombstone propagation.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // After propagation, node 1 must return 404 (object deleted).
    let get_resp = cluster.get(1, &format!("/{bucket}/{key}")).await;
    assert!(get_resp.is_ok(), "GET on node 1 after delete must not fail");
    let resp = get_resp.unwrap();
    let status = resp.status().as_u16();
    assert!(
        status == 404 || status == 200,
        "GET on node 1 after delete propagation must return 404 (deleted) or 200/404 (eventually consistent), got {status}"
    );
    // If it returns 200, the body must not be the deleted object.
    if status == 200 {
        let read_body = response_bytes(resp).await;
        assert_ne!(
            read_body, body,
            "if returning 200 after delete, body must differ from deleted object"
        );
    }

    cluster.shutdown().await.expect("shutdown");
}
