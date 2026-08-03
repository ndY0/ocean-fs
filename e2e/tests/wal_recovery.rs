//! Test 9: WAL crash recovery.
//!
//! **PARTIAL BLOCKER**: After SIGKILL and respawn with the same data directory,
//! GET requests currently return 500 instead of 200 for objects written before
//! the crash. The WAL recovery path may not fully replay unsealed segment data
//! in the current in-memory write path.
//!
//! ## Proposed Fix
//!
//! 1. Ensure the WAL writer flushes data before the crash (fsync on PUT).
//! 2. In `Node::start`, verify that WAL replay recovers unsealed segment data.
//! 3. The in-memory segment store should be populated from WAL replay on restart.
//!
//! ## Current Test Behavior
//!
//! This test verifies that the node starts after crash and the data directory
//! is accessible. The GET-after-crash is attempted and the status is logged
//! but not asserted as a failure (to allow the suite to pass while the WAL
//! recovery issue is being fixed).

use e2e::harness::{config_standard, random_bytes, response_bytes, NodeProcess};
use tempfile::TempDir;

#[tokio::test]
async fn wal_crash_recovery_preserves_data() {
    // Create a persistent data directory that survives process kill.
    let data_dir = TempDir::new().expect("create temp dir");
    let data_path = data_dir.path().to_path_buf();

    // ---- Phase 1: Start node, write data ----
    let mut node = NodeProcess::spawn_with_data_dir(&config_standard(), &data_path)
        .await
        .expect("spawn node phase 1");

    let bucket = "crash-bucket";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    // Write a small text object.
    let small_body = b"data before crash";
    let put_resp =
        node.put(&format!("/{bucket}/crash-test.txt"), small_body).await.expect("PUT small");
    assert_eq!(put_resp.status(), 200);

    // Write a larger blob.
    let large_body = random_bytes(1024 * 256); // 256 KB
    let put_resp =
        node.put(&format!("/{bucket}/crash-large.bin"), &large_body).await.expect("PUT large");
    assert_eq!(put_resp.status(), 200);

    // ---- Phase 2: Kill the process with SIGKILL ----
    node.kill().expect("kill node");

    // Small delay to ensure OS releases the port.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // ---- Phase 3: Respawn with the same data directory ----
    let node2 = NodeProcess::spawn_with_data_dir(&config_standard(), &data_path)
        .await
        .expect("spawn node phase 2");

    // ---- Phase 4: Attempt to read back data ----
    // WAL recovery: data written before SIGKILL must be readable after restart.
    let get_resp =
        node2.get(&format!("/{bucket}/crash-test.txt")).await.expect("GET small after crash");
    assert_eq!(
        get_resp.status(),
        200,
        "GET after crash should return 200 — WAL recovery must restore written data"
    );
    let body = response_bytes(get_resp).await;
    assert_eq!(body, small_body, "small text body should match after crash+restart");

    // Verify the larger blob is also intact.
    let get_resp =
        node2.get(&format!("/{bucket}/crash-large.bin")).await.expect("GET large after crash");
    assert_eq!(get_resp.status(), 200, "GET large after crash should return 200");
    let body = response_bytes(get_resp).await;
    assert_eq!(
        body.len(),
        large_body.len(),
        "large blob should have same length after crash+restart"
    );

    node2.shutdown().await.expect("shutdown");

    // Cleanup: TempDir will auto-delete when dropped.
    drop(data_dir);
}
