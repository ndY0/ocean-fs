//! Fleet injector validation — all five families over SSH (fleet-degradation f2).
//!
//! Drives the f2 injectors against the f1 **volume-backed** fleet and writes
//! a [`LoadReport`] carrying every [`FailureInjectionRecord`]:
//!
//! 1. disk fill + remove on the `spare` volume (a real volume, not a pool),
//! 2. sysfs yank → `device_gone` → SCSI replug → device present,
//! 3. segment discovery on the `data` mount,
//! 4. corruption → `POST /admin/scrub` → read-back heal (correctness loop),
//! 5. `netem` latency add/remove on the internal interface.
//!
//! ## Scope rules
//!
//! - **Correctness only.** No throughput or latency threshold is asserted
//!   (volume-backed runs are not comparable to local-disk runs).
//! - **Fleet-only.** `TARGET_HOSTS` unset ⇒ the test prints a skip notice
//!   and passes, so a local `cargo test -p e2e --test fleet_injectors` is
//!   safe. The real run happens on the Harness VM; this is a short
//!   functional suite, not a load suite (PIPELINE §6).
//!
//! ## Environment
//!
//! | Variable | Purpose |
//! |---|---|
//! | `TARGET_HOSTS` | Comma-separated `host:9000` fleet endpoints (required for a real run). |
//! | `TARGET_HOST_SSH` | Comma-separated per-node SSH targets (required; also accepted from the record's `internal_ip`). |
//! | `LOAD_TEST_RECORD_FILE` | f1 provisioning record copied to the harness (or `LOAD_TEST_VOLUMES_JSON` inline). |
//! | `LOAD_TEST_REPORT_DIR` | Report output dir (default `/tmp/oceanfs-reports`). |
//! | `FLEET_INJECT_NODE` | Node index to inject into (default `1`, clamped to the fleet). |
//! | `LOAD_TEST_SEED` | Report seed (default `42`). |
//!
//! ```bash
//! # On the Harness VM (fleet provisioned with --volume-pools):
//! TARGET_HOSTS=10.0.0.2:9000,10.0.0.3:9000,10.0.0.4:9000 \
//! TARGET_HOST_SSH=root@10.0.0.2,root@10.0.0.3,root@10.0.0.4 \
//! LOAD_TEST_RECORD_FILE=/root/oceanfs-fleet-record.json \
//! cargo test -p e2e --release --test fleet_injectors -- --test-threads=1 --nocapture
//! ```

use std::{path::Path, time::Duration};

use e2e::{
    harness::{random_bytes, LoadTarget},
    load::{assert_that, FleetInjector, LoadReport, ReportResult},
    remote::RemoteCluster,
};

/// The seven records the full sequence must produce (one per attempt).
const EXPECTED_TYPES: [&str; 7] = [
    "disk_fill",
    "disk_fill_remove",
    "device_yank",
    "device_replug",
    "segment_corrupt",
    "latency",
    "latency_remove",
];

#[tokio::test(flavor = "multi_thread")]
async fn fleet_injectors_drive_all_families() {
    let hosts = match std::env::var("TARGET_HOSTS") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            eprintln!(
                "fleet_injectors: TARGET_HOSTS unset — skipping fleet-only validation \
                 (provision a --volume-pools fleet and run on the Harness VM)"
            );
            return;
        }
    };
    let report_dir = std::env::var("LOAD_TEST_REPORT_DIR")
        .unwrap_or_else(|_| "/tmp/oceanfs-reports".to_string());
    let seed: u64 = std::env::var("LOAD_TEST_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(42);

    let remote = RemoteCluster::connect(&hosts).expect("valid TARGET_HOSTS");
    remote.wait_for_health(Duration::from_secs(30)).await.expect("fleet must be healthy");
    assert!(
        remote.len() >= 2,
        "fleet injector validation needs >= 2 nodes (replication + latency peers); got {}",
        remote.len()
    );
    let node: usize = std::env::var("FLEET_INJECT_NODE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .min(remote.len() - 1);
    eprintln!("fleet_injectors: {} nodes, injecting into node {node}", remote.len());

    let mut injector = FleetInjector::from_env(&remote).expect("provisioning record");
    assert_eq!(injector.targets().len(), remote.len(), "one target per node");

    // Fail fast on missing topology: every required volume + SSH target.
    {
        let target = &injector.targets()[node];
        assert!(
            target.ssh().is_some(),
            "node {node} has no SSH target (set TARGET_HOST_SSH or record internal_ip)"
        );
        for role in ["spare", "data"] {
            assert!(
                target.volume(role).is_some(),
                "node {node} has no '{role}' volume — was the fleet provisioned with \
                 --volume-pools and is LOAD_TEST_RECORD_FILE current?"
            );
        }
    }

    let mut report = LoadReport::new(4, "fleet_injectors", seed);
    let start = std::time::Instant::now();
    let mut checks: Vec<(&'static str, bool, String)> = Vec::new();

    // ── 1. Disk fill on the spare (never a pool mount), then remove. ────
    // The spare must be usable first: a previous hard yank leaves its fs
    // stale by design, and remount/format is the scenario's job (f4 P1b).
    let spare_mount = injector.targets()[node].volume("spare").expect("spare volume").mount.clone();
    let spare_usable = injector.mount_usable(node, &spare_mount).await.expect("spare mount probe");
    assert!(
        spare_usable,
        "{spare_mount} on node {node} is not a usable mountpoint — restore it before re-running \
         (a prior hard yank leaves the fs stale; f4 remounts/formats as part of the scenario)"
    );
    injector.fill_volume(node, "spare", 20).await.expect("fill spare volume");
    injector.remove_fill(node, "spare").await.expect("remove spare fill");

    // ── 2. Hard yank → gone → replug → back with the same serial. ───────
    injector.yank_volume(node, "spare").await.expect("yank spare volume");
    let gone = injector.device_gone(node, "spare").await.expect("device_gone after yank");
    checks.push(("device_gone_after_yank", gone, format!("device_gone={gone}")));
    injector.replug_volume(node, "spare").await.expect("replug spare volume");
    let back = injector.device_gone(node, "spare").await.expect("device_gone after replug");
    checks.push(("device_present_after_replug", !back, format!("device_gone={back}")));

    // ── 3. Segment discovery on the data pool. ──────────────────────────
    remote.put(0, "/fleet-injectors", &[]).await.expect("create bucket");
    let blob = random_bytes(1024 * 1024);
    remote
        .put(0, "/fleet-injectors/segment-seed", &blob)
        .await
        .expect("seed blob so data segments exist");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let segment_ids = injector.list_segment_ids(node, "data").await.expect("discover segments");
    checks.push((
        "data_segments_discovered",
        !segment_ids.is_empty(),
        format!("{} segment(s) on node {node}", segment_ids.len()),
    ));
    let segment_id = injector
        .newest_segment_id(node, "data")
        .await
        .expect("discover newest segment")
        .expect("at least one data segment");

    // ── 4. Corruption + heal (generous timeout; no perf bound). ─────────
    injector
        .corrupt_and_verify_heal_remote(node, "data", &segment_id, 64, Duration::from_secs(180))
        .await
        .expect("corrupt one segment and verify the node heals/keeps serving the blob");

    // ── 5. Latency on the internal interface, observed on the wire. ─────
    let iface = injector.latency_iface(node).await.expect("resolve internal interface");
    checks.push(("latency_iface_not_lo", iface != "lo", iface.clone()));
    let peer = if node == 0 { 1 } else { 0 };
    let rtt_before = injector.measure_rtt_ms(node, peer).await.expect("baseline RTT");
    injector.inject_latency(node, &iface, 100).await.expect("inject latency");
    let rtt_during = injector.measure_rtt_ms(node, peer).await.expect("delayed RTT");
    checks.push((
        "latency_observable_on_wire",
        rtt_during >= rtt_before + 50.0,
        format!("before={rtt_before:.2}ms during={rtt_during:.2}ms (injected +100ms)"),
    ));
    injector.remove_latency(node, &iface).await.expect("remove latency");
    let rtt_after = injector.measure_rtt_ms(node, peer).await.expect("restored RTT");
    checks.push((
        "latency_removal_restores_rtt",
        rtt_after < rtt_during - 50.0,
        format!("during={rtt_during:.2}ms after={rtt_after:.2}ms"),
    ));

    // ── Report wiring: every attempt recorded, attributed, successful. ──
    let records = injector.records().to_vec();
    report.record_injections(records.clone());

    // Gate exercise in the same fleet run: an injector built without a
    // provisioning record must refuse to claim success and record the skip.
    let mut skip_injector =
        FleetInjector::from_provisioning_record(&remote, "{}").expect("empty record");
    let skip_error =
        skip_injector.yank_volume(0, "data").await.expect_err("an unconfigured injector must skip");
    assert!(skip_error.to_string().contains("skipped"), "got: {skip_error}");
    let skip_records = skip_injector.records().to_vec();
    report.record_injections(skip_records.clone());

    for injection_type in EXPECTED_TYPES {
        report.assert(assert_that(
            format!("injection_type_{injection_type}_recorded"),
            records.iter().any(|r| r.injection_type == injection_type),
            format!(">= 1 '{injection_type}' record"),
            format!(
                "types: {:?}",
                records.iter().map(|r| r.injection_type.as_str()).collect::<Vec<_>>()
            ),
        ));
    }
    report.assert(assert_that(
        "exactly_one_record_per_attempt",
        records.len() == EXPECTED_TYPES.len(),
        format!("{} records", EXPECTED_TYPES.len()),
        format!("{} records", records.len()),
    ));
    report.assert(assert_that(
        "all_injections_succeeded",
        records.iter().all(|r| r.success),
        "every attempt success=true",
        format!(
            "{} failure(s): {:?}",
            records.iter().filter(|r| !r.success).count(),
            records.iter().filter(|r| !r.success).map(|r| r.detail.as_str()).collect::<Vec<_>>()
        ),
    ));
    report.assert(assert_that(
        "records_attributed_to_injection_node",
        records.iter().all(|r| r.node_index == node),
        format!("all node_index={node}"),
        format!("nodes: {:?}", records.iter().map(|r| r.node_index).collect::<Vec<_>>()),
    ));
    // Roles ride the detail prefix (`spare:` / `data:` / `<iface>:`).
    let role_attribution_ok = records.iter().all(|r| {
        if r.injection_type.starts_with("device_") || r.injection_type.starts_with("disk_fill") {
            r.detail.starts_with("spare:")
        } else if r.injection_type == "segment_corrupt" {
            r.detail.starts_with("data:")
        } else {
            r.detail.starts_with(&format!("{iface}:"))
        }
    });
    report.assert(assert_that(
        "records_role_attribution",
        role_attribution_ok,
        "spare:/data:/<iface>: detail prefixes",
        format!("details: {:?}", records.iter().map(|r| r.detail.as_str()).collect::<Vec<_>>()),
    ));
    report.assert(assert_that(
        "skip_path_records_failure",
        skip_records.len() == 1
            && !skip_records[0].success
            && skip_records[0].detail.contains("skipped"),
        "1 skipped record with success=false",
        format!("{skip_records:?}"),
    ));
    for (name, condition, detail) in &checks {
        report.assert(assert_that(*name, *condition, "expected true", detail.clone()));
    }
    report.duration_secs = start.elapsed().as_secs_f64();
    report.finalize();

    let path = report.write_json_atomic(Path::new(&report_dir)).expect("write report");
    eprintln!("fleet_injectors: report at {}", path.display());

    // The written artifact must carry the records verbatim.
    let written = std::fs::read_to_string(&path).expect("read back report");
    let parsed: serde_json::Value = serde_json::from_str(&written).expect("report is valid JSON");
    assert_eq!(
        parsed["injection_records"].as_array().map(Vec::len),
        Some(records.len() + skip_records.len()),
        "report must serialize every injection record (successes and skips)"
    );

    assert!(
        report.result == ReportResult::Pass,
        "fleet_injectors failed; report at {}:\n{written}",
        path.display()
    );
}
