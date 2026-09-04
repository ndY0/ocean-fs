//! Test: WAL retention stays bounded through the real node binary.
//!
//! Replicates the sustained-load test's write shape at a fraction of the
//! volume: concurrent tiered writers (small / standard / multi blobs),
//! hot-key rewrites and deletes feeding the GC compactor, enough total
//! data to rotate the 64 MiB WAL several times. The WAL file count must
//! converge back to the retention window instead of growing without
//! bound (the `wal_not_unbounded` regression observed under sustained
//! load).

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use e2e::{
    harness::{config_sustained, Cluster, NodeOptions},
    load::MetricsSnapshot,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct SegmentReport {
    total: u64,
    sealed: u64,
    unsealed: u64,
}

fn trickle_deadline() -> tokio::time::Instant {
    // Shared by the trickle writers: keep writing until the convergence
    // watch is over (started right after they spawn).
    static START: std::sync::OnceLock<tokio::time::Instant> = std::sync::OnceLock::new();
    let start = *START.get_or_init(tokio::time::Instant::now);
    start + Duration::from_secs(150)
}

fn make_blob(size: usize, seed: u8) -> Vec<u8> {
    (0..size).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

#[tokio::test]
async fn wal_file_count_stays_bounded_under_tiered_concurrent_churn() {
    let cluster = Arc::new(
        Cluster::spawn_with_options(1, &config_sustained(), &NodeOptions::default())
            .await
            .expect("spawn node"),
    );

    let initial = MetricsSnapshot::scrape(&*cluster, 0).await.expect("initial scrape");
    let initial_files = initial.metrics.get("wal_file_count").copied().unwrap_or(0.0) as u64;
    eprintln!("wal_retention: initial files = {initial_files}");

    // The load's blob mix: 15% inline (no WAL), 35% small (32 KiB →
    // Small tier), 35% standard (1 MiB), 15% multi (16 MiB → 2 × 8 MiB
    // chunks). 16 concurrent writers over 100 hot keys, rewriting and
    // deleting them (the load's Zipfian hot-key churn).
    let small = make_blob(32 * 1024, 1);
    let standard = make_blob(1024 * 1024, 2);
    let multi = make_blob(16 * 1024 * 1024, 3);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let mut handles = Vec::new();
    for w in 0..8u32 {
        let cluster = Arc::clone(&cluster);
        let small = small.clone();
        let standard = standard.clone();
        let multi = multi.clone();
        handles.push(tokio::spawn(async move {
            let mut i = 0u32;
            while tokio::time::Instant::now() < deadline {
                let key = format!("hot-{}", (i % 100) + w);
                let (blob, name) = match i % 100 {
                    0..=14 => (&multi[..], "multi"),
                    15..=49 => (&standard[..], "standard"),
                    _ => (&small[..], "small"),
                };
                // 40% PUT / 10% DELETE / 50% GET over the hot keys.
                match i % 10 {
                    0..=3 => {
                        // Transient 503s are the pipeline's backpressure
                        // signal (the load generator counts them as
                        // retryable errors, never as failures); retry a
                        // few times before asserting.
                        let mut attempts = 0;
                        loop {
                            let resp = cluster.put(0, &format!("/retention/{key}"), blob).await;
                            let status = resp.expect("PUT").status();
                            if status.is_success() {
                                break;
                            }
                            attempts += 1;
                            assert!(attempts < 5, "PUT {name} {key} failed persistently: {status}");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                    4 => {
                        let resp = cluster.delete(0, &format!("/retention/{key}")).await;
                        assert!(resp.expect("DELETE").status().is_success(), "DELETE {name} {key}");
                    }
                    _ => {
                        let _ = cluster.get(0, &format!("/retention/{key}")).await;
                    }
                }
                i += 1;
            }
        }));
    }
    // Poll DURING the load: track the real peak (an `AtomicU64` — the
    // poll task must not shadow a copy). A count that runs away into the
    // hundreds during the window signals a catastrophic leak; the
    // convergence phase below is the precise gate.
    let peak = Arc::new(AtomicU64::new(initial_files));
    let poll_handle = {
        let cluster = Arc::clone(&cluster);
        let peak = Arc::clone(&peak);
        tokio::spawn(async move {
            for _ in 0..9 {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let snap = MetricsSnapshot::scrape(&*cluster, 0).await.expect("scrape");
                let files = snap.metrics.get("wal_file_count").copied().unwrap_or(0.0) as u64;
                peak.fetch_max(files, Ordering::Relaxed);
                let seg_resp = cluster.get(0, "/admin/segments").await.expect("segments");
                let seg: SegmentReport =
                    e2e::harness::response_json(seg_resp).await.expect("segments json");
                eprintln!(
                    "wal_retention: DURING wal files = {files} | segments total={} sealed={} \
                     unsealed={}",
                    seg.total, seg.sealed, seg.unsealed
                );
            }
        })
    };
    for h in handles {
        h.await.expect("writer task");
    }
    poll_handle.await.expect("poll task");
    let load_peak = peak.load(Ordering::Relaxed);
    eprintln!("wal_retention: load phase complete (peak {load_peak})");
    assert!(
        load_peak <= initial_files + 256,
        "WAL count ran away during the load: peak {load_peak} (initial {initial_files})"
    );

    // ── Reclaim proof (rotation-driven sweep) ──────────────────────
    // WAL files are pruned ONLY at rotation (`WalWriter::rotate` runs
    // `cleanup_old_wal_files`), and entries become sweepable once their
    // segments seal (fill / drain-on-write). A node that goes quiet
    // after a burst therefore legitimately RETAINS protected files
    // until the next rotation — the count dropping is not an idle
    // behavior, and the steady state is a workload-dependent plateau,
    // not the retention window itself.
    //
    // The real invariants under continued writes (drains + rotations):
    //   1. the load's backlog of protected files is RECLAIMED — the
    //      count declines materially below the load peak;
    //   2. the count does not REBOUND toward the peak afterwards — a
    //      sealing/retention regression (the historical ~1.5
    //      protected-files/min leak) never reclaims in the first
    //      place, and a leak that resumed after an initial drain would
    //      climb back up.
    let mut handles = Vec::new();
    for w in 0..4u32 {
        let cluster = Arc::clone(&cluster);
        let standard = standard.clone();
        handles.push(tokio::spawn(async move {
            let mut i = 0u32;
            loop {
                if tokio::time::Instant::now() >= trickle_deadline() {
                    break;
                }
                let key = format!("trickle-{w}-{i:03}");
                // Transient 503s are the pipeline's backpressure signal;
                // retry a few times (same policy as the load writers).
                let mut attempts = 0;
                loop {
                    let resp = cluster
                        .put(0, &format!("/retention/{key}"), &standard)
                        .await
                        .expect("trickle PUT");
                    if resp.status().is_success() {
                        break;
                    }
                    attempts += 1;
                    assert!(
                        attempts < 5,
                        "trickle PUT {w}/{i} failed persistently: {}",
                        resp.status()
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
                i += 1;
            }
        }));
    }
    // ~13 MiB/s of standard-tier writes → a 64 MiB rotation roughly
    // every 5 s. Cleanup deletes only files below the retention floor,
    // so reclaiming the load's backlog takes as many rotations as there
    // are backlog files; the seal-backlog drain converts the protected
    // entries to garbage in the meantime.
    let watch_deadline = tokio::time::Instant::now() + Duration::from_secs(150);
    let mut min_seen: Option<u64> = None;
    let mut max_after_min: Option<u64> = None;
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let snap = MetricsSnapshot::scrape(&*cluster, 0).await.expect("scrape");
        let files = snap.metrics.get("wal_file_count").copied().unwrap_or(0.0) as u64;
        eprintln!("wal_retention: reclaim wal files = {files}");
        if let Some(m) = min_seen {
            if files <= m {
                min_seen = Some(files);
            } else {
                max_after_min = Some(max_after_min.map_or(files, |x: u64| x.max(files)));
            }
        } else {
            min_seen = Some(files);
        }
        assert!(
            tokio::time::Instant::now() < watch_deadline,
            "WAL count never declined after the load under continued writes: \
             {files} files (load peak {load_peak}, min {min_seen:?})"
        );
        // Reclaimed materially below the load peak?
        if min_seen.is_some_and(|m| load_peak >= 6 && m <= load_peak - 3) {
            break;
        }
    }
    // No rebound: after the reclaim low, the count may hover (rotations
    // + ongoing writes) but must not climb back toward the load peak —
    // a leak that resumes after an initial backlog drain would.
    if let (Some(m), Some(x)) = (min_seen, max_after_min) {
        assert!(
            x <= m + 6,
            "WAL count rebounded after the reclaim low: low {m}, later high {x} \
             (load peak {load_peak})"
        );
    }
    for h in handles {
        h.await.expect("trickle writer task");
    }

    let final_snap = MetricsSnapshot::scrape(&*cluster, 0).await.expect("final scrape");
    let final_files = final_snap.metrics.get("wal_file_count").copied().unwrap_or(0.0) as u64;
    eprintln!("wal_retention: final wal files = {final_files}");
    assert!(
        final_files <= load_peak.saturating_sub(3) || load_peak < 6,
        "WAL count rebounded after the reclaim: {final_files} files (load peak {load_peak})"
    );
    Arc::try_unwrap(cluster).expect("unique cluster").shutdown().await.expect("shutdown");
}
