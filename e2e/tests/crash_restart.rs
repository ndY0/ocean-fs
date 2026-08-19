//! Test: read-after-restart — every acknowledged object survives a
//! SIGKILL at an arbitrary window (startup-rebuild-from-machine DoD).
//!
//! Writes a tier-mixed set of objects (inline / small / standard /
//! multi — every segment shape, with the last writes landing in the
//! mid-seal window), SIGKILLs the node without a settle, restarts it
//! from the same data dir, and verifies every acknowledged object
//! reads back intact with a matching digest. Also asserts the startup
//! rebuild metric is reported.

use std::time::Duration;

use e2e::{
    harness::{config_standard, Cluster, NodeOptions},
    load::MetricsSnapshot,
};

fn make_body(size: usize, seed: u8) -> Vec<u8> {
    (0..size).map(|i| (i as u8).wrapping_mul(29).wrapping_add(seed)).collect()
}

#[tokio::test]
async fn acknowledged_objects_survive_sigkill_and_restart() {
    let cluster = Cluster::spawn_with_options(1, &config_standard(), &NodeOptions::default())
        .await
        .expect("spawn node");

    // Tier-mixed objects: inline (100 B), small (8 KiB), standard
    // (512 KiB), multi (4 MiB → split chunks). The final writes are
    // NOT settled — their segments are mid-seal at the kill.
    let mut manifest: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..16u32 {
        let size = match i % 4 {
            0 => 100,
            1 => 8 * 1024,
            2 => 512 * 1024,
            _ => 4 * 1024 * 1024,
        };
        let body = make_body(size, i as u8);
        let key = format!("obj-{i:02}");
        let resp = cluster.put(0, &format!("/crash/{key}"), &body).await.expect("PUT");
        assert!(resp.status().is_success(), "PUT {key} must succeed: {}", resp.status());
        manifest.push((key, body));
    }

    // SIGKILL immediately — no settle, the last segments are mid-seal.
    cluster.kill(0).expect("kill node 0");
    cluster.restart(0).await.expect("restart with same data dir");
    // Let startup churn settle (reaper/AE bursts can drop connections
    // in the first seconds — a verify-time transport flake is not a
    // data-loss signal).
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Every acknowledged object reads back intact, digest-verified.
    for (key, expected) in &manifest {
        let resp = cluster.get(0, &format!("/crash/{key}")).await.expect("GET");
        assert_eq!(resp.status(), 200, "{key} must be readable after restart");
        let body = resp.bytes().await.expect("body");
        assert_eq!(&body[..], expected.as_slice(), "{key} must read back exact bytes");
    }

    // The startup rebuild metric is reported.
    let snap = MetricsSnapshot::scrape(&cluster, 0).await.expect("metrics scrape");
    let rebuild_ms = snap.metrics.get("oceanfs_startup_rebuild_ms").copied().unwrap_or(0.0);
    assert!(rebuild_ms > 0.0, "oceanfs_startup_rebuild_ms must be reported after the restart");

    cluster.shutdown().await.expect("shutdown");
}
