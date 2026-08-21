//! Local experiment: single node, heavy rewrite of ONE key (4 MiB each),
//! measure the segment dir growth + orphan-reaper/GC activity.
use std::time::{Duration, Instant};

use e2e::harness::{Cluster, NodeOptions};

#[tokio::test(flavor = "multi_thread")]
async fn probe() {
    let cfg = e2e::harness::config_cluster_churn()
        .replace("gc_interval_sec = 10", "gc_interval_sec = 10\norphan_reaper_interval_sec = 10");
    let cluster =
        Cluster::spawn_with_options(1, &cfg, &NodeOptions::default()).await.expect("spawn");
    let data_dir = cluster.node(0).data_dir().to_path_buf();
    let seg_dir = data_dir.join("segments");

    let _ = cluster.put(0, "/load-test", &[]).await;
    let body = vec![0x5Au8; 1024 * 1024]; // 1 MiB incompressible (local profile caps at 2 MiB)
    println!("segments BEFORE: {} files", dir_count(&seg_dir));

    let start = Instant::now();
    for i in 0..30 {
        let resp = cluster.put(0, "/load-test/leak-key", &body).await.unwrap();
        let st = resp.status().as_u16();
        assert_eq!(st, 200, "put {i} failed with {st}");
    }
    println!("30 rewrites in {:?}", start.elapsed());
    println!(
        "segments AFTER rewrites: {} files, {} bytes",
        dir_count(&seg_dir),
        dir_bytes(&seg_dir)
    );

    // Wait for the orphan reaper (interval 10s, TTL 5s) + GC cycles.
    tokio::time::sleep(Duration::from_secs(30)).await;
    println!(
        "segments AFTER 30s settle: {} files, {} bytes",
        dir_count(&seg_dir),
        dir_bytes(&seg_dir)
    );

    // Delete the key, wait again — the tombstone should drive the GC now.
    let _ = cluster.delete(0, "/load-test/leak-key").await.unwrap();
    tokio::time::sleep(Duration::from_secs(25)).await;
    println!(
        "segments AFTER delete + 25s: {} files, {} bytes",
        dir_count(&seg_dir),
        dir_bytes(&seg_dir)
    );

    cluster.shutdown().await.unwrap();
}

fn dir_count(p: &std::path::Path) -> usize {
    std::fs::read_dir(p).map(|d| d.count()).unwrap_or(0)
}
fn dir_bytes(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p)
        .map(|d| d.flatten().map(|e| e.metadata().map(|m| m.len()).unwrap_or(0)).sum())
        .unwrap_or(0)
}
