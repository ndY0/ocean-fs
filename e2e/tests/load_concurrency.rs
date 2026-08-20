//! Phase 1 — Single-Node Concurrency Correctness Test.
//!
//! Validates single-node concurrency correctness. Spawns one NodeProcess,
//! launches `N = CPU count × 4` concurrent workers performing PUT/GET/DELETE/HEAD
//! with randomized blob sizes across all 4 segment tiers, including concurrent
//! writes to the same key (testing HLC conflict resolution). Runs for 60 seconds
//! (configurable via `LOAD_TEST_DURATION_SECS`). Asserts manifest integrity,
//! zero 4xx PUT rejections, zero transport errors, all workers active,
//! minimum write volume, all four tiers successfully exercised,
//! `/admin/health` healthy, `accel_ec_fallback_total == 0`, and `logs_clean`
//! — the captured node logs (default `e2e/target/e2e-logs`, debug level for
//! this test) contain none of the three gap-closure failure signatures
//! (`no appending segment available in pool`, `BadDigest`,
//! `cannot fetch chunk`).
//!
//! This is the cheapest test that catches the most dangerous bugs:
//! data races, deadlocks, and data corruption under concurrent access.
//!
//! ## Usage
//!
//! ```bash
//! LOAD_TEST_SEED=42 cargo test -p e2e -- load_concurrency
//! ```
//!
//! With TSAN (requires nightly Rust):
//! ```bash
//! RUSTFLAGS="-Z sanitizer=thread" cargo +nightly test -p e2e -- load_concurrency
//! ```
//!
//! ## Environment Variables
//!
//! | Variable | Default | Description |
//! |---|---|---|
//! | `LOAD_TEST_SEED` | random | Deterministic seed for reproducible runs. Logged at start. |
//! | `LOAD_TEST_DURATION_SECS` | 60 | Override the test duration (e.g., `10` for CI smoke). |
//! | `LOAD_TEST_DEBUG` | unset | When `1` (or `true`), prints a per-op debug trace `[worker-N] PUT … gen_ms=… total_ms=… status=…` to stderr, breaking down client-side blob generation vs HTTP round-trip per operation. |
//!
//! The node log level is forced to `debug` via `NodeOptions` (not the
//! harness default `info`) so the `logs_clean` assertion can see the
//! debug-level seal-worker signatures.

use std::{path::Path, sync::Arc, time::Duration};

use e2e::{
    harness::{config_standard, Cluster, NodeOptions},
    load::{
        assert_that, parse_prometheus_text, BlobSizeDist, KeySpace, LoadReport, LoadScenario,
        Manifest, OpWeight, Operation, Orchestrator, ReportResult,
    },
};

// ── Test ──────────────────────────────────────────────────────────────────────

/// Single-node concurrency correctness test.
///
/// Spawns one `NodeProcess`, launches N = CPU count × 4 concurrent workers
/// performing PUT/GET/DELETE/HEAD with tiered blob sizes and same-key
/// concurrency. Runs for 60 seconds (configurable). Asserts manifest
/// integrity, health, `accel_fallback_total == 0`, and non-zero worker stats.
///
/// Uses the multi-threaded tokio runtime: the default `current_thread`
/// flavor would serialize all workers and their blob generation on a
/// single OS thread, making the harness measure itself instead of the
/// server.
#[tokio::test(flavor = "multi_thread")]
async fn load_concurrency() {
    // ── Parse environment variables ────────────────────────────
    let seed: u64 =
        std::env::var("LOAD_TEST_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or_else(|| {
            let s: u64 = rand::random();
            eprintln!("LOAD_TEST_SEED not set, using random seed: {s}");
            s
        });
    eprintln!("load_concurrency: seed={seed}");

    let duration_secs: u64 =
        std::env::var("LOAD_TEST_DURATION_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(60);
    eprintln!("load_concurrency: duration={duration_secs}s");

    // ── Compute concurrency ────────────────────────────────────
    let concurrency = (num_cpus::get() * 4).clamp(8, 64);
    eprintln!("load_concurrency: concurrency={concurrency}");

    // ── Spawn single-node cluster ──────────────────────────────
    // Debug node logs: the three `logs_clean` signatures are debug-level
    // or error-level, and the gap-closure forensics relied on debug
    // traces; capture is on by default into `e2e/target/e2e-logs`.
    let cluster = Cluster::spawn_with_options(
        1,
        &config_standard(),
        &NodeOptions::default().with_log_level("debug"),
    )
    .await
    .expect("cluster spawn");
    let cluster = Arc::new(cluster);

    // ── Build load scenario ────────────────────────────────────
    let scenario = LoadScenario {
        concurrency,
        duration: Duration::from_secs(duration_secs),
        operations: vec![
            OpWeight { op: Operation::Put, weight: 0.50 },
            OpWeight { op: Operation::Get, weight: 0.40 },
            OpWeight { op: Operation::Delete, weight: 0.05 },
            OpWeight { op: Operation::Head, weight: 0.05 },
        ],
        blob_sizes: BlobSizeDist::Tiered {
            inline_pct: 10.0,
            small_pct: 30.0,
            standard_pct: 40.0,
            multi_pct: 20.0,
        },
        key_space: KeySpace::RandomUuidWithSharedPool { shared_pool_size: 100, shared_ratio: 0.2 },
        seed,
    };

    let manifest = Arc::new(Manifest::new());

    // ── Run load test ──────────────────────────────────────────
    let stats = Orchestrator::run(scenario, Arc::clone(&cluster), Arc::clone(&manifest)).await;

    // ── Scrape metrics ─────────────────────────────────────────
    let accel_fallback = scrape_accel_fallback(&cluster).await;

    // ── Verify manifest ────────────────────────────────────────
    let manifest_summary = manifest.verify_summary(&*cluster).await;

    // ── Health check ───────────────────────────────────────────
    let health_ok =
        cluster.get(0, "/admin/health").await.map(|r| r.status().is_success()).unwrap_or(false);

    // ── Build report ───────────────────────────────────────────
    let mut report = LoadReport::new(1, "load_concurrency", seed);
    report.duration_secs = duration_secs as f64;

    // Record assertions. Data is captured before moving into the report
    // so that assertion messages can still reference the values.
    let manifest_objects_written = manifest_summary.objects_written;
    let manifest_objects_verified = manifest_summary.objects_verified;
    let manifest_mismatches = manifest_summary.mismatches;

    report.assert(assert_that(
        "manifest_integrity",
        manifest_mismatches == 0,
        "0 hash mismatches — all written keys readable with correct content",
        format!(
            "{} objects written, {} verified, {} mismatches",
            manifest_objects_written, manifest_objects_verified, manifest_mismatches,
        ),
    ));

    report.assert(assert_that(
        "health",
        health_ok,
        "GET /admin/health returns 200",
        format!("health {}", if health_ok { "OK" } else { "FAIL" }),
    ));

    report.assert(assert_that(
        "accel_fallback_zero",
        accel_fallback.is_some_and(|v| v == 0.0),
        "accel_ec_fallback_total == 0 (no acceleration fallbacks)",
        format!(
            "accel_ec_fallback_total = {}",
            accel_fallback.map_or("N/A (metric absent — defect)".to_string(), |v| v.to_string()),
        ),
    ));

    let ops_total = stats.ops_total;
    let puts_total = stats.puts_total;
    let puts_4xx = stats.puts_4xx;
    let gets_total = stats.gets_total;
    let deletes_total = stats.deletes_total;
    let heads_total = stats.heads_total;
    let errors_total = stats.errors_total;
    let active_workers = stats.active_workers;
    let puts_inline = stats.puts_inline;
    let puts_small = stats.puts_small;
    let puts_standard = stats.puts_standard;
    let puts_multi = stats.puts_multi;
    let min_writes: u64 = std::cmp::max(5, duration_secs / 5);

    report.assert(assert_that(
        "zero_4xx_puts",
        puts_4xx == 0,
        "zero PUTs rejected with an HTTP 4xx status (body-limit 413s were silently invisible)",
        format!("{} PUTs rejected with 4xx (of {} total)", puts_4xx, puts_total),
    ));

    report.assert(assert_that(
        "zero_transport_errors",
        errors_total == 0,
        "zero transport errors (pooled-connection teardown, mid-upload resets)",
        format!("{} transport errors ({} total ops)", errors_total, ops_total),
    ));

    report.assert(assert_that(
        "all_workers_active",
        active_workers == concurrency as u64,
        format!("all {concurrency} workers completed at least one operation"),
        format!("{active_workers} active workers"),
    ));

    report.assert(assert_that(
        "minimum_write_volume",
        manifest_objects_written as u64 >= min_writes,
        format!("at least {min_writes} objects written (the manifest must prove something)"),
        format!(
            "{} objects written ({} PUTs, {} GETs, {} DELETEs, {} HEADs)",
            manifest_objects_written, puts_total, gets_total, deletes_total, heads_total,
        ),
    ));

    report.assert(assert_that(
        "all_four_tiers_exercised",
        puts_inline > 0 && puts_small > 0 && puts_standard > 0 && puts_multi > 0,
        "all 4 blob size tiers successfully PUT (inline, small, standard, multi)",
        format!(
            "tiers: inline={}, small={}, standard={}, multi={}",
            puts_inline, puts_small, puts_standard, puts_multi,
        ),
    ));

    // ── Log cleanliness ────────────────────────────────────────
    // The three failure signatures from the gap-closure post-review:
    // pool exhaustion, hash mismatch (BadDigest), and chunk-fetch
    // failure. Any occurrence in the captured node logs is a real
    // failure signature that HTTP-level assertions can miss (e.g. a
    // retried 500 that ends in success).
    const LOG_CLEAN_SIGNATURES: [&str; 3] =
        ["no appending segment available in pool", "BadDigest", "cannot fetch chunk"];

    let log_hits: Vec<(String, Vec<String>)> = LOG_CLEAN_SIGNATURES
        .iter()
        .filter(|sig| cluster.any_node_logs_contain(sig))
        .map(|sig| {
            // Collect up to two example lines from node 0 for the
            // assertion message (single-node cluster).
            let examples =
                cluster.node(0).grep_logs(sig).unwrap_or_default().into_iter().take(2).collect();
            ((*sig).to_string(), examples)
        })
        .collect();
    let logs_clean = log_hits.is_empty();

    let log_clean_detail = if logs_clean {
        "no failure signatures found in captured node logs".to_string()
    } else {
        log_hits
            .iter()
            .map(|(sig, examples)| format!("{sig}: {examples:?}"))
            .collect::<Vec<_>>()
            .join("; ")
    };

    report.assert(assert_that(
        "logs_clean",
        logs_clean,
        "captured node logs contain none of the three failure signatures \
         (pool exhaustion, hash mismatch, chunk fetch failure)",
        &log_clean_detail,
    ));

    // Populate report with collected data after assertions are recorded.
    report.worker_stats = Some(stats);
    report.manifest = Some(manifest_summary);

    report.finalize();

    // ── Write report ───────────────────────────────────────────
    let output_dir = Path::new("target/load-reports");
    if let Err(e) = report.write_json_atomic(output_dir) {
        eprintln!("load_concurrency: failed to write JSON report: {e}");
    }
    if let Err(e) = report.write_textfile_atomic(output_dir) {
        eprintln!("load_concurrency: failed to write textfile: {e}");
    }

    // ── Shutdown cluster ───────────────────────────────────────
    let cluster = Arc::try_unwrap(cluster).expect("cluster Arc should be uniquely owned");
    let _ = cluster.shutdown().await;

    // ── Final assertion ────────────────────────────────────────
    let fail_msg = format!(
        "load concurrency test FAILED:\n\
         manifest_integrity: {} mismatches ({} written, {} verified)\n\
         health: {}\n\
         accel_fallback: {:?}\n\
         puts_4xx: {}\n\
         errors_total: {}\n\
         active_workers: {} / {}\n\
         ops_total: {}\n\
         tiers: inline={}, small={}, standard={}, multi={}\n\
         logs_clean: {}",
        manifest_mismatches,
        manifest_objects_written,
        manifest_objects_verified,
        if health_ok { "OK" } else { "FAIL" },
        accel_fallback,
        puts_4xx,
        errors_total,
        active_workers,
        concurrency,
        ops_total,
        puts_inline,
        puts_small,
        puts_standard,
        puts_multi,
        log_clean_detail,
    );
    assert_eq!(report.result, ReportResult::Pass, "{}", fail_msg);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Scrapes `/admin/metrics` from the cluster and returns the value of
/// `accel_ec_fallback_total`, or `None` if the endpoint is unavailable
/// or the metric is absent (which the assertion treats as a defect —
/// the counter is registered at node startup).
async fn scrape_accel_fallback(cluster: &Cluster) -> Option<f64> {
    match cluster.get(0, "/admin/metrics").await {
        Ok(resp) if resp.status().is_success() => {
            let text = resp.text().await.unwrap_or_default();
            if text.trim().is_empty() {
                return None;
            }
            let metrics = parse_prometheus_text(&text);
            metrics.get("accel_ec_fallback_total").copied()
        }
        _ => None,
    }
}
