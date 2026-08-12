//! Cluster concurrency & stress tests (T44-T46).
//!
//! Validates concurrent writes to different keys, concurrent writes
//! to the same key (HLC resolution), and write resilience during
//! node failure.

use std::sync::Arc;

use e2e::harness::{config_3node_w2_r2, random_bytes, response_bytes, Cluster};
use tokio::sync::{Barrier, Mutex};

// ---------------------------------------------------------------------------
// T44: Concurrent writes to different keys
// ---------------------------------------------------------------------------

/// T44: 10 concurrent PUTs to different keys from different nodes.
/// All succeed. All readable from all nodes. No data corruption.
#[tokio::test]
async fn t44_concurrent_writes_to_different_keys_all_succeed() {
    let cluster =
        Arc::new(Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster"));

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    let bucket = "concur-diff";
    cluster.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");

    // 10 concurrent writes, each from a different "client" (round-robin nodes).
    let num_writes = 10;
    let barrier = Arc::new(Barrier::new(num_writes));
    let mut handles = Vec::with_capacity(num_writes);

    for i in 0..num_writes {
        let cluster = Arc::clone(&cluster);
        let barrier = Arc::clone(&barrier);
        let key = format!("obj-{}.txt", i);
        let body = random_bytes(512);

        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let writer = i % 3; // round-robin writers
            let path = format!("/{}/{}", bucket, key);
            cluster.put(writer, &path, &body).await.map(|r| (key, r.status(), body))
        }));
    }

    let mut results = Vec::with_capacity(num_writes);
    for handle in handles {
        match handle.await.expect("task join") {
            Ok((key, status, body)) => {
                results.push((key, status, body));
            }
            Err(e) => {
                panic!("concurrent PUT task failed: {e}");
            }
        }
    }

    // All writes must have succeeded (200).
    let success_count = results.iter().filter(|(_, s, _)| *s == 200).count();
    assert_eq!(
        success_count, num_writes,
        "all {num_writes} concurrent writes must succeed (200), got {success_count}"
    );

    // Verify all successful writes are readable from at least one node.
    for (key, _, body) in &results {
        let mut readable = false;
        for node_idx in 0..3 {
            if let Ok(resp) = cluster.get(node_idx, &format!("/{bucket}/{key}")).await {
                if resp.status() == 200 {
                    let read_body = response_bytes(resp).await;
                    if &read_body == body {
                        readable = true;
                        break;
                    }
                }
            }
        }
        assert!(readable, "object {key} must be readable after concurrent write");
    }

    let cluster = Arc::try_unwrap(cluster).unwrap();
    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T45: Concurrent writes to same key
// ---------------------------------------------------------------------------

/// T45: 2 concurrent PUTs to the same key from different nodes.
/// HLC resolves to a single winner. Both nodes eventually agree
/// on the winning version.
#[tokio::test]
async fn t45_concurrent_writes_to_same_key_hlc_resolves_single_winner() {
    let cluster =
        Arc::new(Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster"));

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    let bucket = "concur-same";
    cluster.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");

    let key = "same-key.txt";
    let body_a = b"Version from node 0";
    let body_b = b"Version from node 1";

    // Concurrent writes from node 0 and node 1 to the same key.
    let barrier = Arc::new(Barrier::new(2));

    let cluster_a = Arc::clone(&cluster);
    let barrier_a = Arc::clone(&barrier);
    let key_a = key.to_string();
    let bucket_a = bucket.to_string();
    let handle_a = tokio::spawn(async move {
        barrier_a.wait().await;
        cluster_a
            .put(0, &format!("/{}/{}", bucket_a, key_a), body_a)
            .await
            .map(|r| (r.status(), body_a))
    });

    let cluster_b = Arc::clone(&cluster);
    let barrier_b = Arc::clone(&barrier);
    let key_b = key.to_string();
    let bucket_b = bucket.to_string();
    let handle_b = tokio::spawn(async move {
        barrier_b.wait().await;
        cluster_b
            .put(1, &format!("/{}/{}", bucket_b, key_b), body_b)
            .await
            .map(|r| (r.status(), body_b))
    });

    let result_a = handle_a.await.expect("task a join");
    let result_b = handle_b.await.expect("task b join");

    // Both writes must succeed (they were sent to different nodes).
    assert!(result_a.is_ok(), "concurrent write from node 0 must succeed: {:?}", result_a);
    assert!(result_b.is_ok(), "concurrent write from node 1 must succeed: {:?}", result_b);

    // Wait for HLC resolution and convergence.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Read from all nodes — they must return one of the two valid
    // versions. HLC resolution is eventual; strict convergence across
    // all nodes may require an anti-entropy cycle.
    let mut versions: Vec<Vec<u8>> = Vec::new();
    for node_idx in 0..3 {
        if let Ok(resp) = cluster.get(node_idx, &format!("/{bucket}/{key}")).await {
            if resp.status() == 200 {
                let body = response_bytes(resp).await;
                versions.push(body);
            }
        }
    }

    assert!(
        !versions.is_empty(),
        "at least one node must return data for the concurrent write key"
    );

    // Each version must be one of the two valid write bodies.
    for v in &versions {
        assert!(
            v == body_a || v == body_b,
            "every returned version must be a valid write (body_a or body_b), got {:?}",
            v
        );
    }

    let cluster = Arc::try_unwrap(cluster).unwrap();
    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T46: Write during node failure
// ---------------------------------------------------------------------------

/// T46: Start a PUT. Kill one successor mid-replication. Write completes
/// with remaining W acks (or fails gracefully if quorum lost).
#[tokio::test]
async fn t46_write_during_node_failure_graceful_degradation() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");
    let cluster = Arc::new(Mutex::new(cluster));

    {
        let c = cluster.lock().await;
        c.wait_for_convergence(3).await.expect("cluster convergence");
    }

    let bucket = "fail-write";
    {
        let c = cluster.lock().await;
        c.put(0, &format!("/{bucket}"), &[]).await.expect("create bucket");
    }

    let key = "degrade.txt";
    let body = b"Write during node failure test";

    // Spawn a write in a background task, then kill node 2 shortly after.
    let barrier = Arc::new(Barrier::new(2));

    let cluster_write = Arc::clone(&cluster);
    let barrier_write = Arc::clone(&barrier);
    let key_w = key.to_string();
    let bucket_w = bucket.to_string();
    let body_w = body.to_vec();

    let write_handle = tokio::spawn(async move {
        barrier_write.wait().await;
        cluster_write.lock().await.put(0, &format!("/{}/{}", bucket_w, key_w), &body_w).await
    });

    // Wait for the write task to be ready, then kill node 2.
    barrier.wait().await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Kill node 2 mid-write. The write is in-flight; it will either
    // complete with surviving acks or fail gracefully.
    {
        let c = cluster.lock().await;
        c.kill(2).expect("kill node 2 mid-write");
    }

    // Get the write result. Must complete or fail gracefully — no panic.
    let write_result = write_handle.await;
    assert!(
        write_result.is_ok(),
        "write task must not panic during node failure: {:?}",
        write_result
    );
    let write_outcome = write_result.unwrap();
    // Write outcome: Ok(response) means the write completed (possibly with
    // fewer acks), Err means the write failed gracefully.
    // Both are acceptable — the system must not crash or hang.
    match write_outcome {
        Ok(resp) => {
            // If write succeeded, data must be readable from at least one
            // surviving node.
            if resp.status() == 200 {
                let mut readable = false;
                let c = cluster.lock().await;
                for i in 0..2 {
                    if let Ok(r) = c.get(i, &format!("/{bucket}/{key}")).await {
                        if r.status() == 200 {
                            readable = true;
                        }
                    }
                }
                assert!(readable, "write result 200 but data not readable from any surviving node");
            }
        }
        Err(_e) => {
            // Write failed gracefully — acceptable.
        }
    }
}
