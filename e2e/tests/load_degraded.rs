//! Phase 4 — Fleet-Ready Degraded Mode Under Load (fleet-degradation f3).
//!
//! Runs the four adapted degraded-mode scenarios against the **ADR-0026
//! fleet** (N dedicated node VMs + CX43 harness, real per-role volumes) or a
//! local 3-node spawn for dev/CI smoke:
//!
//! 1. **Mid-write kill (VM-level).** `SIGKILL` one dedicated node's
//!    `oceanfs` unit over SSH while a known 1 MiB blob is being written
//!    through a survivor; the write must still ack (quorum satisfied by the
//!    remaining nodes), the victim must rejoin, hints must drain, and the
//!    blob must be readable from the restarted node with correct bytes.
//! 2. **Slow node.** `tc netem` +500ms on the victim's **internal**
//!    interface (never `lo`), a bounded concurrent read/write window, and
//!    then removal: the cluster must keep serving correct bytes throughout,
//!    and membership must recover after removal. The observed membership
//!    timeline is recorded as evidence; **no latency bound is asserted** —
//!    if SWIM legitimately suspects the delayed node, that is recorded, and
//!    the assertion is the no-data-loss/no-cascade recovery property (the
//!    Degraded-vs-Dead classifier is a follow-up feature).
//! 3. **Disk-full (data volume).** Fill the victim's `/mnt/oceanfs-data`
//!    mount to 95% (f2 injector), run a bounded write window against that
//!    node: every attempt must produce an HTTP status (no panic/reset), the
//!    process must stay up (`/admin/health` answers), baseline reads must
//!    serve correct bytes from the other replicas, and after `remove_fill`
//!    the node must accept writes again. RSS and GC counters are recorded as
//!    evidence — never as threshold assertions.
//! 4. **Corruption + heal (remote segment).** Write a known blob with
//!    RF ≥ 2, corrupt 64 bytes in the newest live segment on the victim's
//!    data volume, trigger `POST /admin/scrub`, and poll until the victim
//!    serves the original bytes again. A second scrub pass must still serve
//!    correct bytes. Detection is evidenced by the scrub/AE/heal counters
//!    (summed across the fleet); precise local-copy repair assertions are
//!    f4's role scenarios (the product exposes no key→segment map).
//!
//! ## Failure-semantics classification (epic contract)
//!
//! | Scenario | Path | Why |
//! |---|---|---|
//! | S1 mid-write kill | **Hard crash** (`SIGKILL`, systemd `Restart=no`) | Process crash with WAL replay on rejoin — the intended recovery path |
//! | S2 slow node | **Network degradation** (internal-interface `netem`) | Latency-only; data must survive and membership must recover |
//! | S3 disk-full | **Fill on the dedicated data volume** | ENOSPC pressure on the pool root, never the VM root or harness path |
//! | S4 corruption + heal | **In-place remote corruption** (no yank) | Exercises detection/repair; the device stays present |
//!
//! No scenario hard-yanks a volume here — the pool-role hard-failure matrix
//! (yank/replug, WAL boot variant, metadata rebuild) is **f4**.
//!
//! ## Recovery and convergence
//!
//! ENOSPC drives the data pool through confirmed-loss (Dead), and pool-health
//! recovery is window-based (300s default). After S3 removes the fill the
//! test waits (bounded) for no Dead pool and for a successful recovery
//! write; before the final manifest verification it triggers a fleet-wide
//! scrub and settles until both quorum levels are clean on two consecutive
//! checks (bounded at 360s). Durability/repair counters (hints dropped,
//! repair enqueued, scrub corruption) are recorded as evidence — never as
//! threshold assertions.
//!
//! ## Correctness only — no performance assertions
//!
//! Volume-backed runs cross network block storage; their latency and
//! throughput are **not comparable** to local-disk runs and no scenario
//! asserts a latency or throughput bound. RTT/RSS/GC/counter observations are
//! recorded as data. The report carries an explicit `perf_assertions_none`
//! assertion (`perf: []` in the feature frontmatter).
//!
//! ## Topology modes
//!
//! - **Fleet (primary, Phase 4):** `TARGET_HOSTS` set to the comma-separated
//!   node endpoints as seen from the harness; all injections go through the
//!   f2 SSH black-box injectors. Fleet mode **fails fast** when SSH targets
//!   or the provisioning record's volumes are missing — an impossible
//!   scenario is a failed assertion, never a silent skip.
//! - **Local spawn (dev/CI smoke):** no `TARGET_HOSTS` — a local 3-node
//!   [`Cluster`] runs on the current host. Disk-fill and corruption run on
//!   the local role roots; the network injector cannot target one node
//!   (loopback contaminates every node), so it is **skipped with a recorded
//!   warning** (`success=false`, `skipped:` detail) — no silent success.
//! - **Control (`LOAD_TEST_NO_INJECTIONS=1`):** background load + manifest
//!   verification only, zero scenario injections (the runner's
//!   `--no-injections` smoke run; confirms the load path itself is clean).
//!
//! ## Environment
//!
//! | Variable | Default | Purpose |
//! |---|---|---|
//! | `LOAD_TEST_SEED` | random | Deterministic seed. |
//! | `LOAD_TEST_DURATION_SECS` | 300 | Background load duration (quick mode: 120). |
//! | `TARGET_HOSTS` | unset | Comma-separated `host:9000` fleet endpoints — enables fleet mode. |
//! | `TARGET_HOST_SSH` | unset | Comma-separated per-node SSH targets (else the record's `internal_ip`). |
//! | `TARGET_SERVICE` | `oceanfs` | systemd unit name on every fleet node. |
//! | `LOAD_TEST_RECORD_FILE` / `LOAD_TEST_VOLUMES_JSON` | unset | f1 provisioning record (copied to the harness) or inline JSON. |
//! | `LOAD_TEST_NO_INJECTIONS` | unset | `1` = control run (no injections). |
//! | `LOAD_TEST_CONCURRENCY` | derived | Background-load worker count. |
//! | `LOAD_TEST_LATENCY_IFACE` | derived | Interface override for the latency injector. |
//! | `LOAD_TEST_REPORT_DIR` | `/tmp/oceanfs-reports` | Report output dir (tmpfs per ADR-0019). |
//!
//! ```bash
//! # On the Harness VM (fleet provisioned with --volume-pools):
//! TARGET_HOSTS=10.0.0.2:9000,10.0.0.3:9000,10.0.0.4:9000 \
//! TARGET_HOST_SSH=root@10.0.0.2,root@10.0.0.3,root@10.0.0.4 \
//! LOAD_TEST_RECORD_FILE=/root/oceanfs-phase4-record.json \
//! LOAD_TEST_DURATION_SECS=300 \
//! cargo test -p e2e --release --test load_degraded -- --test-threads=1
//! ```
//!
//! Never run this suite on the development machine (PIPELINE §6) — it is a
//! load suite.

use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use e2e::{
    harness::{config_cluster_churn, random_bytes, Cluster, LoadTarget, NodeOptions},
    load::{
        assert_that, AssertionResult, BlobSizeDist, ClusterViewSnapshot, FailureInjectionRecord,
        FleetInjector, KeySpace, LoadReport, LoadScenario, Manifest, MetricsSnapshot, OpWeight,
        Operation, Orchestrator, ReportResult,
    },
    remote::RemoteCluster,
};

// ── Constants ───────────────────────────────────────────────────────────────

/// Fleet size (ADR-0026 default; quorum semantics need ≥ 3).
const NODE_COUNT: usize = 3;
/// Read quorum for the manifest check.
const READ_QUORUM: usize = 2;
/// Keys sampled for manifest verification (per-key × per-node GETs are slow).
const MANIFEST_SAMPLE: usize = 150;
/// Membership convergence timeout after a kill/restart or latency removal.
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(90);
/// Injection target (never node 0 — the bootstrap must stay up).
const VICTIM: usize = 1;
/// `netem` delay applied to the victim's internal interface.
const S2_DELAY_MS: u64 = 500;
/// Blob written while the victim is killed (multi-chunk, exercises quorum).
const S1_BLOB_BYTES: usize = 1024 * 1024;
/// Corruption width per S4 segment.
const S4_CORRUPT_BYTES: usize = 64;
/// The injection record types a full (non-control) run must produce.
const EXPECTED_INJECTION_TYPES: [&str; 6] =
    ["vm_kill", "latency", "latency_remove", "disk_fill", "disk_fill_remove", "segment_corrupt"];

// ── Topology abstraction ────────────────────────────────────────────────────

/// A unified target handle: either a locally spawned [`Cluster`] or a
/// remote [`RemoteCluster`]; the orchestrator and manifest verifier are
/// generic over [`LoadTarget`].
enum Target {
    /// Locally spawned cluster (dev/CI smoke).
    Local(Arc<Cluster>),
    /// Remote fleet (cloud mode, ADR-0026).
    Remote(Arc<RemoteCluster>),
}

impl LoadTarget for Target {
    fn len(&self) -> usize {
        match self {
            Target::Local(cluster) => cluster.len(),
            Target::Remote(remote) => remote.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Target::Local(cluster) => cluster.is_empty(),
            Target::Remote(remote) => remote.is_empty(),
        }
    }

    fn node_addr(&self, i: usize) -> std::net::SocketAddr {
        match self {
            Target::Local(cluster) => cluster.node_addr(i),
            Target::Remote(remote) => remote.node_addr(i),
        }
    }

    fn client(&self) -> &reqwest::Client {
        match self {
            Target::Local(cluster) => cluster.client(),
            Target::Remote(remote) => remote.client(),
        }
    }

    async fn get(&self, i: usize, path: &str) -> Result<reqwest::Response, e2e::harness::Error> {
        match self {
            Target::Local(cluster) => cluster.get(i, path).await,
            Target::Remote(remote) => remote.get(i, path).await,
        }
    }

    async fn put(
        &self,
        i: usize,
        path: &str,
        body: &[u8],
    ) -> Result<reqwest::Response, e2e::harness::Error> {
        match self {
            Target::Local(cluster) => cluster.put(i, path, body).await,
            Target::Remote(remote) => remote.put(i, path, body).await,
        }
    }

    async fn delete(&self, i: usize, path: &str) -> Result<reqwest::Response, e2e::harness::Error> {
        match self {
            Target::Local(cluster) => cluster.delete(i, path).await,
            Target::Remote(remote) => remote.delete(i, path).await,
        }
    }

    async fn head(&self, i: usize, path: &str) -> Result<reqwest::Response, e2e::harness::Error> {
        match self {
            Target::Local(cluster) => cluster.head(i, path).await,
            Target::Remote(remote) => remote.head(i, path).await,
        }
    }

    async fn post(&self, i: usize, path: &str) -> Result<reqwest::Response, e2e::harness::Error> {
        match self {
            Target::Local(cluster) => cluster.post(i, path).await,
            Target::Remote(remote) => remote.post(i, path).await,
        }
    }
}

// ── Scenario log ────────────────────────────────────────────────────────────

/// Accumulator for scenario assertions, injection records, and membership
/// views; merged into the [`LoadReport`] at the end.
#[derive(Default)]
struct ScenarioLog {
    checks: Vec<AssertionResult>,
    records: Vec<FailureInjectionRecord>,
    views: Vec<ClusterViewSnapshot>,
}

impl ScenarioLog {
    fn check(
        &mut self,
        name: impl Into<String>,
        passed: bool,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) {
        self.checks.push(assert_that(name, passed, expected, actual));
    }
}

// ── Cluster view helpers ────────────────────────────────────────────────────

/// Parsed `/admin/cluster` response.
#[derive(Debug, Clone, Default)]
struct ClusterView {
    members: usize,
    alive: usize,
    members_detail: Vec<(String, String, u64)>,
}

/// Parses `/admin/cluster` on node `i`. Returns `None` when the node is
/// unreachable or the endpoint is unavailable (e.g. mid-restart).
async fn fetch_cluster_view(target: &Target, i: usize) -> Option<ClusterView> {
    let resp = target.get(i, "/admin/cluster").await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: serde_json::Value = e2e::harness::response_json(resp).await.ok()?;
    let mut view = ClusterView::default();
    for node in body["nodes"].as_array()? {
        let id = node["id"].as_str().unwrap_or("?").to_string();
        let state = node["state"].as_str().unwrap_or("?").to_string();
        let incarnation = node["incarnation"].as_u64().unwrap_or(0);
        view.members += 1;
        if state == "Alive" {
            view.alive += 1;
        }
        view.members_detail.push((id, state, incarnation));
    }
    Some(view)
}

/// Waits until every reachable node reports `expected_alive` alive members
/// (or the timeout elapses). Returns `true` on convergence.
async fn wait_for_convergence(target: &Target, expected_alive: usize, timeout: Duration) -> bool {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return false;
        }
        let mut converged = true;
        for i in 0..target.len() {
            match fetch_cluster_view(target, i).await {
                Some(view) if view.alive >= expected_alive => {}
                Some(view) => {
                    eprintln!(
                        "  load_degraded: node {i} reports {}/{} alive (expected {expected_alive})",
                        view.alive, view.members
                    );
                    converged = false;
                }
                None => converged = false,
            }
        }
        if converged {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Snapshots every node's membership view into the scenario log; returns how
/// many node views were captured (0 = nothing reachable).
async fn record_views(target: &Target, start: Instant, log: &mut ScenarioLog) -> usize {
    let mut captured = 0;
    for i in 0..target.len() {
        if let Some(view) = fetch_cluster_view(target, i).await {
            log.views.push(ClusterViewSnapshot {
                t_secs: start.elapsed().as_secs_f64(),
                node_index: i,
                members: view.members,
                alive: view.alive,
                members_detail: view.members_detail,
                // Ring probes are not part of the Phase 4 contract; Phase 3
                // (load_cluster_churn) owns ring-consistency observations.
                probe_successors: Vec::new(),
            });
            captured += 1;
        }
    }
    captured
}

// ── Small helpers ───────────────────────────────────────────────────────────

/// PUTs `body` to node `i`; returns the HTTP status or a transport error.
async fn put_blob(target: &Target, i: usize, path: &str, body: &[u8]) -> Result<u16, String> {
    match target.put(i, path, body).await {
        Ok(resp) => Ok(resp.status().as_u16()),
        Err(e) => Err(e.to_string()),
    }
}

/// PUTs through the first node that accepts the request (2xx), skipping
/// `exclude`. A volume-backed node can be temporarily write-degraded by the
/// pool-health classifier; a client may route to any node, so scenario
/// writes must not pin one endpoint.
async fn put_via_any(
    target: &Target,
    path: &str,
    body: &[u8],
    exclude: &[usize],
) -> Result<usize, String> {
    let mut last = "no node attempted".to_string();
    for i in 0..target.len() {
        if exclude.contains(&i) {
            continue;
        }
        match put_blob(target, i, path, body).await {
            Ok(code) if (200..300).contains(&code) => return Ok(i),
            Ok(code) => last = format!("node {i}: HTTP {code}"),
            Err(e) => last = format!("node {i}: {e}"),
        }
    }
    Err(last)
}

/// GETs `path` from node `i`; `None` unless 2xx (body read included).
async fn read_body(target: &Target, i: usize, path: &str) -> Option<Vec<u8>> {
    match target.get(i, path).await {
        Ok(resp) if resp.status().is_success() => resp.bytes().await.ok().map(|b| b.to_vec()),
        _ => None,
    }
}

/// Polls a GET until the body matches `expected` or `timeout` elapses.
async fn poll_read(
    target: &Target,
    i: usize,
    path: &str,
    expected: &[u8],
    timeout: Duration,
) -> (bool, String) {
    let start = Instant::now();
    loop {
        let last = match target.get(i, path).await {
            Ok(resp) if resp.status().is_success() => match resp.bytes().await {
                Ok(bytes) if bytes.as_ref() == expected => {
                    return (
                        true,
                        format!("correct bytes after {:.1}s", start.elapsed().as_secs_f64()),
                    );
                }
                Ok(bytes) => {
                    format!("HTTP 200 but {} bytes (expected {})", bytes.len(), expected.len())
                }
                Err(e) => format!("body read failed: {e}"),
            },
            Ok(resp) => format!("HTTP {}", resp.status()),
            Err(e) => format!("transport error: {e}"),
        };
        if start.elapsed() > timeout {
            return (false, format!("timed out after {timeout:?}: {last}"));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Scrapes every node's `/admin/metrics` (default snapshot on failure).
async fn scrape_all(target: &Target) -> Vec<MetricsSnapshot> {
    let mut snaps = Vec::with_capacity(target.len());
    for i in 0..target.len() {
        snaps.push(MetricsSnapshot::scrape(target, i).await.unwrap_or_default());
    }
    snaps
}

/// Monotonic per-node counter delta (restart-reset safe: `.max(0)`).
fn counter_delta(after: &MetricsSnapshot, before: &MetricsSnapshot, name: &str) -> f64 {
    match (after.counter(name), before.counter(name)) {
        (Some(a), Some(b)) => (a - b).max(0.0),
        (Some(a), None) => a,
        _ => 0.0,
    }
}

/// Sums a counter delta across every node (the scrub partition owner and the
/// repair target may both differ from the corrupted node).
fn total_counter_delta(before: &[MetricsSnapshot], after: &[MetricsSnapshot], name: &str) -> f64 {
    before.iter().zip(after).map(|(b, a)| counter_delta(a, b, name)).sum()
}

/// Waits until all hint debt has drained (3 consecutive quiet checks across
/// reachable nodes) or the timeout elapses.
async fn settle_hints(target: &Target, timeout: Duration) -> (bool, String) {
    let start = Instant::now();
    let mut quiet_checks = 0u32;
    let mut last = "no samples".to_string();
    while start.elapsed() < timeout {
        let mut pending = 0.0;
        let mut unreachable = Vec::new();
        for i in 0..target.len() {
            match MetricsSnapshot::scrape(target, i).await {
                Ok(snap) => {
                    pending += snap.counter("hinted_handoff_hints_stored_total").unwrap_or(0.0)
                        - snap.counter("hinted_handoff_hints_delivered_total").unwrap_or(0.0)
                        - snap.counter("hinted_handoff_hints_expired_total").unwrap_or(0.0)
                        - snap.counter("hinted_handoff_hints_dropped_total").unwrap_or(0.0);
                }
                Err(_) => unreachable.push(i),
            }
        }
        last = format!("pending={pending:.0} unreachable={unreachable:?}");
        if unreachable.is_empty() && pending <= 0.0 {
            quiet_checks += 1;
            if quiet_checks >= 3 {
                return (true, format!("{last} (3 quiet checks)"));
            }
        } else {
            quiet_checks = 0;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    (false, format!("still draining after {timeout:?}: {last}"))
}

/// Waits until every node answers `/admin/health` (or the timeout elapses).
async fn wait_all_healthy(target: &Target, timeout: Duration) -> bool {
    let start = Instant::now();
    loop {
        let mut all = true;
        for i in 0..target.len() {
            let ok = target
                .get(i, "/admin/health")
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false);
            if !ok {
                all = false;
            }
        }
        if all {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Parses `/admin/pools` on node `i` into `(name, status)` pairs.
async fn fetch_pool_statuses(target: &Target, i: usize) -> Option<Vec<(String, String)>> {
    let resp = target.get(i, "/admin/pools").await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: serde_json::Value = e2e::harness::response_json(resp).await.ok()?;
    Some(
        body["pools"]
            .as_array()?
            .iter()
            .map(|pool| {
                (
                    pool["name"].as_str().unwrap_or("?").to_string(),
                    pool["status"].as_str().unwrap_or("?").to_string(),
                )
            })
            .collect(),
    )
}

/// Returns every pool currently reporting `dead` as `node i:name`.
async fn dead_pools(target: &Target) -> Vec<String> {
    let mut dead = Vec::new();
    for i in 0..target.len() {
        if let Some(pools) = fetch_pool_statuses(target, i).await {
            for (name, status) in pools {
                if status == "dead" {
                    dead.push(format!("node {i}:{name}"));
                }
            }
        }
    }
    dead
}

/// Waits until no pool reports `dead` (or the timeout elapses).
async fn wait_no_dead_pools(target: &Target, timeout: Duration) -> (bool, String) {
    let start = Instant::now();
    loop {
        let dead = dead_pools(target).await;
        if dead.is_empty() {
            return (true, format!("no dead pools after {:.0}s", start.elapsed().as_secs_f64()));
        }
        if start.elapsed() > timeout {
            return (false, format!("dead pools after {timeout:?}: {dead:?}"));
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Returns `true` when `service` is safe to interpolate into a journalctl
/// command (systemd unit name character set).
fn service_name_is_safe(service: &str) -> bool {
    !service.is_empty()
        && service.len() <= 64
        && service.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '@'))
}

/// Counts `panicked` lines in every fleet node's `oceanfs` journal.
async fn fleet_panic_lines(remote: &RemoteCluster, service: &str) -> Result<u64, String> {
    let mut total = 0u64;
    for i in 0..remote.len() {
        let Some(ssh) = remote.ssh_target_for(i) else {
            return Err(format!("node {i}: no SSH target configured"));
        };
        let command = format!(
            "journalctl -u {service} --since '-2 hours' --no-pager 2>/dev/null | grep -c panicked || true"
        );
        let output =
            remote.ssh_exec_on(ssh, &command).await.map_err(|e| format!("node {i}: {e}"))?;
        total += output.stdout.trim().parse::<u64>().unwrap_or(0);
    }
    Ok(total)
}

/// The newest `*.dat` stem under a local pool root (local corrupt target).
fn newest_local_segment(root: &Path) -> Option<String> {
    let mut newest: Option<(SystemTime, String)> = None;
    for entry in fs::read_dir(root).ok()?.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("dat") {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else { continue };
        let modified =
            entry.metadata().and_then(|m| m.modified()).unwrap_or(SystemTime::UNIX_EPOCH);
        let stem = name.trim_end_matches(".dat").to_string();
        if newest.as_ref().map(|(t, _)| modified > *t).unwrap_or(true) {
            newest = Some((modified, stem));
        }
    }
    newest.map(|(_, stem)| stem)
}

// ── Scenario 1: mid-write kill ──────────────────────────────────────────────

/// Fleet S1 — SIGKILL the victim's unit over SSH while a survivor is writing.
async fn scenario1_fleet(
    remote: &Arc<RemoteCluster>,
    ssh: &str,
    service: &str,
    target: &Target,
    start: Instant,
    log: &mut ScenarioLog,
) {
    let victim = VICTIM.min(target.len().saturating_sub(1));
    let path = "/load-degraded/s1-midwrite-blob";
    let blob = random_bytes(S1_BLOB_BYTES);
    let _ = put_via_any(target, "/load-degraded", &[], &[]).await;

    record_views(target, start, log).await;

    let kill = tokio::spawn({
        let remote = Arc::clone(remote);
        let ssh = ssh.to_string();
        let service = service.to_string();
        async move { remote.kill_and_restart_node_via_ssh(victim, &ssh, &service).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let put = put_via_any(target, path, &blob, &[victim]).await;
    let kill_result = kill.await.expect("kill task panicked");

    log.records.push(FailureInjectionRecord::new(
        "vm_kill",
        victim,
        kill_result.is_ok(),
        match &kill_result {
            Ok(()) => format!("node {victim}: SIGKILL + restart via ssh ({service})"),
            Err(e) => format!("node {victim}: kill/restart failed: {e}"),
        },
    ));
    log.check(
        "s1_kill_succeeded",
        kill_result.is_ok(),
        format!("node {victim} SIGKILLed and restarted"),
        format!("{kill_result:?}"),
    );
    log.check(
        "s1_write_completed_with_node_down",
        put.is_ok(),
        "PUT acked 2xx while the victim is down (quorum from survivors)",
        format!("{put:?}"),
    );

    let converged = wait_for_convergence(target, target.len(), CONVERGENCE_TIMEOUT).await;
    log.check(
        "s1_membership_converged_after_restart",
        converged,
        format!("all {} nodes Alive after the victim rejoins", target.len()),
        format!("converged={converged}"),
    );
    record_views(target, start, log).await;

    let (read_ok, detail) = poll_read(target, victim, path, &blob, Duration::from_secs(90)).await;
    log.check(
        "s1_object_readable_from_restarted_node",
        read_ok,
        "the blob is served with correct bytes from the restarted node",
        detail,
    );
    let survivor = (victim + 1) % target.len();
    let (read_ok, detail) = poll_read(target, survivor, path, &blob, Duration::from_secs(30)).await;
    log.check(
        "s1_object_readable_from_survivor",
        read_ok,
        "the blob is served with correct bytes from a survivor",
        detail,
    );

    let (settled, detail) = settle_hints(target, Duration::from_secs(180)).await;
    log.check(
        "s1_hints_reconverged",
        settled,
        "hint debt is fully delivered after the victim rejoins",
        detail,
    );
    record_views(target, start, log).await;
}

/// Local S1 — SIGKILL the victim child process while a survivor is writing.
async fn scenario1_local(
    cluster: &Arc<Cluster>,
    target: &Arc<Target>,
    start: Instant,
    log: &mut ScenarioLog,
) {
    let victim = VICTIM.min(target.len().saturating_sub(1));
    let path = "/load-degraded/s1-midwrite-blob";
    let blob = random_bytes(S1_BLOB_BYTES);
    let _ = put_via_any(target, "/load-degraded", &[], &[]).await;
    record_views(target, start, log).await;

    let put = tokio::spawn({
        let target = Arc::clone(target);
        let path = path.to_string();
        let blob = blob.clone();
        async move { put_via_any(&target, &path, &blob, &[victim]).await }
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let kill_result = cluster.kill(victim);
    let put = put.await.expect("put task panicked");

    log.records.push(FailureInjectionRecord::new(
        "vm_kill",
        victim,
        kill_result.is_ok(),
        match &kill_result {
            Ok(()) => format!("node {victim}: SIGKILL (local child process)"),
            Err(e) => format!("node {victim}: local kill failed: {e}"),
        },
    ));
    log.check(
        "s1_kill_succeeded",
        kill_result.is_ok(),
        format!("node {victim} SIGKILLed"),
        format!("{kill_result:?}"),
    );
    log.check(
        "s1_write_completed_with_node_down",
        put.is_ok(),
        "PUT acked 2xx while the victim is down (quorum from survivors)",
        format!("{put:?}"),
    );

    let restart = cluster.restart(victim).await;
    log.check(
        "s1_restart_succeeded",
        restart.is_ok(),
        "the victim restarts with its preserved data directory",
        format!("{restart:?}"),
    );
    let converged = wait_for_convergence(target, target.len(), CONVERGENCE_TIMEOUT).await;
    log.check(
        "s1_membership_converged_after_restart",
        converged,
        format!("all {} nodes Alive after the victim rejoins", target.len()),
        format!("converged={converged}"),
    );
    record_views(target, start, log).await;

    let (read_ok, detail) = poll_read(target, victim, path, &blob, Duration::from_secs(90)).await;
    log.check(
        "s1_object_readable_from_restarted_node",
        read_ok,
        "the blob is served with correct bytes from the restarted node",
        detail,
    );
    let survivor = (victim + 1) % target.len();
    let (read_ok, detail) = poll_read(target, survivor, path, &blob, Duration::from_secs(30)).await;
    log.check(
        "s1_object_readable_from_survivor",
        read_ok,
        "the blob is served with correct bytes from a survivor",
        detail,
    );

    let (settled, detail) = settle_hints(target, Duration::from_secs(180)).await;
    log.check(
        "s1_hints_reconverged",
        settled,
        "hint debt is fully delivered after the victim rejoins",
        detail,
    );
    record_views(target, start, log).await;
}

// ── Scenario 2: slow node ───────────────────────────────────────────────────

/// Fleet S2 — internal-interface `netem`, bounded window, removal, recovery.
async fn scenario2_fleet(
    injector: &mut FleetInjector<'_>,
    target: &Target,
    start: Instant,
    window: Duration,
    log: &mut ScenarioLog,
) {
    let node = VICTIM.min(target.len().saturating_sub(1));
    let peer = if node == 0 { 1 } else { 0 };
    let iface = match injector.latency_iface(node).await {
        Ok(iface) => iface,
        Err(e) => {
            log.check(
                "s2_latency_injected",
                false,
                format!("resolve the internal interface on node {node}"),
                format!("{e} — set LOAD_TEST_LATENCY_IFACE to override"),
            );
            return;
        }
    };
    let rtt_before = injector.measure_rtt_ms(node, peer).await.ok();
    let injected = injector.inject_latency(node, &iface, S2_DELAY_MS).await.is_ok();
    let rtt_during = injector.measure_rtt_ms(node, peer).await.ok();
    let observable = matches!((rtt_before, rtt_during), (Some(b), Some(d)) if d > b);
    log.check(
        "s2_latency_injected",
        injected,
        format!("+{S2_DELAY_MS}ms netem on {iface} (node {node})"),
        format!("injected={injected} rtt_before={rtt_before:?}ms rtt_during={rtt_during:?}ms"),
    );
    log.check(
        "s2_injection_observable_on_wire",
        observable,
        "RTT increases while netem is applied (functional injection check, not a latency bound)",
        format!("before={rtt_before:?}ms during={rtt_during:?}ms"),
    );

    // Bounded read/write window with membership polling; the cluster must
    // serve correct bytes through the degradation. Reads are taken from the
    // **writer** node (its own copy is immediate); non-writer replicas are
    // checked afterwards on the first key once replication has settled.
    let deadline = Instant::now() + window;
    let mut seq = 0u64;
    let mut views_captured = 0usize;
    let mut writes_ok = 0usize;
    let mut ok_reads = 0usize;
    let mut read_misses = 0usize;
    let mut wrong_bytes = 0usize;
    let mut per_node_ok = vec![0usize; target.len()];
    let mut last_written: Option<(String, Vec<u8>)> = None;
    let mut first_written: Option<(String, Vec<u8>)> = None;
    while Instant::now() < deadline {
        let body = random_bytes(8 * 1024);
        let path = format!("/load-degraded/s2-{seq}");
        // Route through any accepting node (a write-degraded node must not
        // make the scenario itself fail); read back from the writer.
        if let Ok(writer) = put_via_any(target, &path, &body, &[]).await {
            if first_written.is_none() {
                first_written = Some((path.clone(), body.clone()));
            }
            writes_ok += 1;
            last_written = Some((path.clone(), body.clone()));
            match read_body(target, writer, &path).await {
                Some(got) if got == body => ok_reads += 1,
                Some(_) => wrong_bytes += 1,
                None => read_misses += 1,
            }
        }
        seq += 1;
        views_captured += record_views(target, start, log).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // Replication settles asynchronously: the first key written (before the
    // latency window elapsed) must now be served from every node.
    if let Some((path, body)) = &first_written {
        for (i, served) in per_node_ok.iter_mut().enumerate() {
            let (ok, _) = poll_read(target, i, path, body, Duration::from_secs(20)).await;
            if ok {
                *served = 1;
            }
        }
    }
    let all_nodes_served = per_node_ok.iter().all(|count| *count > 0);
    log.check(
        "s2_cluster_served_correct_bytes_during_latency",
        writes_ok > 0 && ok_reads > 0 && wrong_bytes == 0 && all_nodes_served,
        "every write is readable from its writer with correct bytes; the first key replicates to every node",
        format!(
            "writes_ok={writes_ok} ok_reads={ok_reads} read_misses={read_misses} \
             wrong_bytes={wrong_bytes} per_node={per_node_ok:?} \
             writes={seq} membership_views={views_captured}"
        ),
    );
    log.check(
        "s2_membership_timeline_recorded",
        views_captured > 0,
        "the membership timeline is recorded as evidence (suspicion is not a failure)",
        format!("{views_captured} per-node views recorded during the injection"),
    );

    let removed = injector.remove_latency(node, &iface).await.is_ok();
    let rtt_after = injector.measure_rtt_ms(node, peer).await.ok();
    log.check(
        "s2_latency_removed",
        removed,
        "the netem qdisc is removed",
        format!("removed={removed} rtt_after={rtt_after:?}ms"),
    );
    let converged = wait_for_convergence(target, target.len(), CONVERGENCE_TIMEOUT).await;
    log.check(
        "s2_membership_recovers_after_removal",
        converged,
        format!("all {} nodes Alive after latency removal", target.len()),
        format!("converged={converged}"),
    );
    record_views(target, start, log).await;

    if let Some((path, body)) = &last_written {
        let (read_ok, detail) = poll_read(target, node, path, body, Duration::from_secs(20)).await;
        log.check(
            "s2_no_data_loss_after_removal",
            read_ok,
            "a key written under latency is served correctly after removal",
            detail,
        );
    }
}

/// Local S2 — the network injector cannot target one node; record the skip.
fn scenario2_local(log: &mut ScenarioLog) {
    let node = VICTIM.min(NODE_COUNT - 1);
    for injection_type in ["latency", "latency_remove"] {
        log.records.push(FailureInjectionRecord::new(
            injection_type,
            node,
            false,
            "skipped: local spawns share loopback — netem on `lo` would delay every node; \
             fleet scenarios use the internal-interface injector (f2). Scenarios impossible \
             in local mode: S2 slow-node. (Raw loopback injection requires \
             E2E_ALLOW_LOOPBACK_LATENCY=1.)"
                .to_string(),
        ));
    }
    let recorded = log
        .records
        .iter()
        .filter(|r| {
            !r.success
                && r.detail.starts_with("skipped:")
                && r.injection_type.starts_with("latency")
        })
        .count()
        == 2;
    log.check(
        "s2_platform_skipped_local",
        recorded,
        "the network injector is skipped in local mode with a recorded reason (no silent success)",
        format!("{recorded} skipped latency records"),
    );
}

// ── Scenario 3: disk-full ───────────────────────────────────────────────────

/// Fleet S3 — fill the victim's data volume mount, write bounded, recover.
async fn scenario3_fleet(
    injector: &mut FleetInjector<'_>,
    target: &Target,
    window: Duration,
    log: &mut ScenarioLog,
) {
    let victim = VICTIM.min(target.len().saturating_sub(1));
    let mount = injector.targets()[victim]
        .volume("data")
        .map(|volume| volume.mount.clone())
        .unwrap_or_default();
    let usable = !mount.is_empty() && injector.mount_usable(victim, &mount).await.unwrap_or(false);
    log.check(
        "s3_data_mount_usable",
        usable,
        format!("{mount} is a usable mountpoint on node {victim}"),
        format!("usable={usable}"),
    );
    if !usable {
        // A stale mount means a prior scenario did not recover it; fail the
        // fill attempt loudly instead of writing into the root filesystem.
        log.check(
            "s3_data_volume_filled",
            false,
            "fill the dedicated data volume",
            format!("{mount} unusable — refusing to fill"),
        );
        return;
    }

    let _ = put_via_any(target, "/load-degraded", &[], &[]).await;
    let mut baseline_keys: Vec<(String, Vec<u8>)> = Vec::new();
    let mut baseline_errors: Vec<String> = Vec::new();
    for k in 0..5 {
        let path = format!("/load-degraded/s3-baseline-{k}");
        let body = random_bytes(64 * 1024);
        match put_via_any(target, &path, &body, &[]).await {
            Ok(_) => baseline_keys.push((path, body)),
            Err(e) => baseline_errors.push(format!("key {k}: {e}")),
        }
    }
    // Replication is asynchronous: wait until every baseline key is served
    // from every node before measuring reads under the fill.
    let mut replicated = 0usize;
    for (path, body) in &baseline_keys {
        for i in 0..target.len() {
            let (ok, _) = poll_read(target, i, path, body, Duration::from_secs(30)).await;
            if ok {
                replicated += 1;
            }
        }
    }
    let baseline_total = baseline_keys.len() * target.len();
    log.check(
        "s3_baseline_replicated",
        baseline_total > 0 && replicated == baseline_total,
        "every baseline key is served from every node before the fill",
        format!(
            "{replicated}/{baseline_total} replica reads correct; write errors: {baseline_errors:?}"
        ),
    );

    let before = MetricsSnapshot::scrape(target, victim).await.unwrap_or_default();
    let fill_ok = injector.fill_volume(victim, "data", 95).await.is_ok();
    log.check(
        "s3_data_volume_filled",
        fill_ok,
        format!("{mount} filled to 95% on node {victim}"),
        format!("fill_ok={fill_ok}"),
    );

    let deadline = Instant::now() + window;
    let mut statuses: Vec<u16> = Vec::new();
    let mut transport_errors: Vec<String> = Vec::new();
    let mut seq = 0u64;
    while Instant::now() < deadline {
        let body = random_bytes(64 * 1024);
        let path = format!("/load-degraded/s3-{seq}");
        match put_blob(target, victim, &path, &body).await {
            Ok(code) => statuses.push(code),
            Err(e) => transport_errors.push(e),
        }
        seq += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let after = MetricsSnapshot::scrape(target, victim).await.unwrap_or_default();
    let health_ok =
        target.get(victim, "/admin/health").await.map(|r| r.status().is_success()).unwrap_or(false);
    log.check(
        "s3_node_survived_fill",
        health_ok,
        "/admin/health keeps answering during the fill",
        format!(
            "health_ok={health_ok} statuses={statuses:?} transport_errors={transport_errors:?}"
        ),
    );
    log.check(
        "s3_writes_graceful",
        transport_errors.is_empty(),
        "every write to the full node produced an HTTP status (no panic/reset)",
        format!("transport_errors={transport_errors:?} statuses={statuses:?}"),
    );

    // Reads of data that predates the fill must serve from other replicas.
    let mut reads_ok = 0usize;
    let mut reads_total = 0usize;
    for (path, body) in &baseline_keys {
        for i in 0..target.len() {
            reads_total += 1;
            if let Some(got) = read_body(target, i, path).await {
                if got == *body {
                    reads_ok += 1;
                }
            }
        }
    }
    log.check(
        "s3_reads_served_correct_bytes_during_fill",
        reads_total > 0 && reads_ok == reads_total,
        "every baseline read is correct on every node during the fill",
        format!("{reads_ok}/{reads_total} correct"),
    );

    // Evidence only: RSS and GC counters (never threshold assertions).
    let rss_before = before.gauge("process_resident_memory_bytes").unwrap_or(0.0);
    let rss_after = after.gauge("process_resident_memory_bytes").unwrap_or(0.0);
    let gc_dead = counter_delta(&after, &before, "gc_dead_bytes_total");
    let gc_compacted = counter_delta(&after, &before, "gc_segments_compacted_total");
    log.check(
        "s3_rss_and_gc_observed_evidence",
        true,
        "RSS/GC recorded as data — no absolute threshold is asserted",
        format!(
            "rss_before={rss_before:.0} rss_after={rss_after:.0} \
             gc_dead_delta={gc_dead:.0} gc_compacted_delta={gc_compacted:.0}"
        ),
    );

    let removed = injector.remove_fill(victim, "data").await.is_ok();
    log.check(
        "s3_fill_removed",
        removed,
        "the fill file is removed and synced",
        format!("removed={removed}"),
    );

    // ENOSPC drives the data pool through confirmed-loss (Dead); recovery is
    // producer-window based, so give it a bounded window before the node is
    // expected to accept writes again.
    let (pools_alive, pool_detail) = wait_no_dead_pools(target, Duration::from_secs(300)).await;
    log.check(
        "s3_pool_recovered_after_fill_removal",
        pools_alive,
        "no pool remains Dead after cleanup",
        pool_detail,
    );
    let recovery_start = Instant::now();
    let mut recovery =
        put_via_any(target, "/load-degraded/s3-recovery", &random_bytes(64 * 1024), &[]).await;
    while recovery.is_err() && recovery_start.elapsed() < Duration::from_secs(300) {
        tokio::time::sleep(Duration::from_secs(5)).await;
        recovery =
            put_via_any(target, "/load-degraded/s3-recovery", &random_bytes(64 * 1024), &[]).await;
    }
    log.check(
        "s3_recovery_write_after_fill_removal",
        recovery.is_ok(),
        "the node accepts writes again after cleanup",
        format!("{recovery:?} after {:.0}s", recovery_start.elapsed().as_secs_f64()),
    );
}

/// Local S3 — fill the local `pool-data` role root, write bounded, recover.
async fn scenario3_local(
    cluster: &Cluster,
    target: &Target,
    window: Duration,
    log: &mut ScenarioLog,
) {
    let victim = VICTIM.min(target.len().saturating_sub(1));
    let root = match cluster.local_role_root(victim, "data") {
        Ok(root) => root,
        Err(e) => {
            log.check(
                "s3_data_mount_usable",
                false,
                format!("local pool-data root exists for node {victim}"),
                format!("{e}"),
            );
            return;
        }
    };
    // Stale-fill hygiene: a killed prior run can leave fill.bin behind and
    // the local root shares the host filesystem.
    let stale = root.join("fill.bin");
    if stale.exists() {
        let _ = fs::remove_file(&stale);
    }
    let fill = match cluster.fill_role_root(victim, "data", 95).await {
        Ok(path) => path,
        Err(e) => {
            log.records.push(FailureInjectionRecord::new(
                "disk_fill",
                victim,
                false,
                format!("data: local fill failed: {e}"),
            ));
            log.check(
                "s3_data_volume_filled",
                false,
                "fill the local pool-data root",
                format!("{e}"),
            );
            return;
        }
    };
    log.records.push(FailureInjectionRecord::new(
        "disk_fill",
        victim,
        true,
        format!("data: local pool root {} filled to 95%", fill.display()),
    ));
    log.check(
        "s3_data_volume_filled",
        true,
        format!("local pool-data root {} filled to 95%", fill.display()),
        format!("{}", fill.display()),
    );

    let _ = put_via_any(target, "/load-degraded", &[], &[]).await;
    let mut baseline_keys: Vec<(String, Vec<u8>)> = Vec::new();
    let mut baseline_errors: Vec<String> = Vec::new();
    for k in 0..5 {
        let path = format!("/load-degraded/s3-baseline-{k}");
        let body = random_bytes(64 * 1024);
        match put_via_any(target, &path, &body, &[]).await {
            Ok(_) => baseline_keys.push((path, body)),
            Err(e) => baseline_errors.push(format!("key {k}: {e}")),
        }
    }
    // Replication is asynchronous: wait until every baseline key is served
    // from every node before measuring reads under the fill.
    let mut replicated = 0usize;
    for (path, body) in &baseline_keys {
        for i in 0..target.len() {
            let (ok, _) = poll_read(target, i, path, body, Duration::from_secs(30)).await;
            if ok {
                replicated += 1;
            }
        }
    }
    let baseline_total = baseline_keys.len() * target.len();
    log.check(
        "s3_baseline_replicated",
        baseline_total > 0 && replicated == baseline_total,
        "every baseline key is served from every node before the fill",
        format!(
            "{replicated}/{baseline_total} replica reads correct; write errors: {baseline_errors:?}"
        ),
    );

    let before = MetricsSnapshot::scrape(target, victim).await.unwrap_or_default();
    let deadline = Instant::now() + window;
    let mut statuses: Vec<u16> = Vec::new();
    let mut transport_errors: Vec<String> = Vec::new();
    let mut seq = 0u64;
    while Instant::now() < deadline {
        let body = random_bytes(64 * 1024);
        let path = format!("/load-degraded/s3-local-{seq}");
        match put_blob(target, victim, &path, &body).await {
            Ok(code) => statuses.push(code),
            Err(e) => transport_errors.push(e),
        }
        seq += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let after = MetricsSnapshot::scrape(target, victim).await.unwrap_or_default();
    let health_ok =
        target.get(victim, "/admin/health").await.map(|r| r.status().is_success()).unwrap_or(false);
    log.check(
        "s3_node_survived_fill",
        health_ok,
        "/admin/health keeps answering during the fill",
        format!(
            "health_ok={health_ok} statuses={statuses:?} transport_errors={transport_errors:?}"
        ),
    );
    log.check(
        "s3_writes_graceful",
        transport_errors.is_empty(),
        "every write to the full node produced an HTTP status (no panic/reset)",
        format!("transport_errors={transport_errors:?} statuses={statuses:?}"),
    );

    let mut reads_ok = 0usize;
    let mut reads_total = 0usize;
    for (path, body) in &baseline_keys {
        for i in 0..target.len() {
            reads_total += 1;
            if let Some(got) = read_body(target, i, path).await {
                if got == *body {
                    reads_ok += 1;
                }
            }
        }
    }
    log.check(
        "s3_reads_served_correct_bytes_during_fill",
        reads_total > 0 && reads_ok == reads_total,
        "every baseline read is correct on every node during the fill",
        format!("{reads_ok}/{reads_total} correct"),
    );

    let rss_before = before.gauge("process_resident_memory_bytes").unwrap_or(0.0);
    let rss_after = after.gauge("process_resident_memory_bytes").unwrap_or(0.0);
    let gc_dead = counter_delta(&after, &before, "gc_dead_bytes_total");
    let gc_compacted = counter_delta(&after, &before, "gc_segments_compacted_total");
    log.check(
        "s3_rss_and_gc_observed_evidence",
        true,
        "RSS/GC recorded as data — no absolute threshold is asserted",
        format!(
            "rss_before={rss_before:.0} rss_after={rss_after:.0} \
             gc_dead_delta={gc_dead:.0} gc_compacted_delta={gc_compacted:.0}"
        ),
    );

    let removed = fs::remove_file(&fill);
    log.records.push(FailureInjectionRecord::new(
        "disk_fill_remove",
        victim,
        removed.is_ok(),
        match &removed {
            Ok(()) => format!("removed {}", fill.display()),
            Err(e) => format!("remove {} failed: {e}", fill.display()),
        },
    ));
    log.check(
        "s3_fill_removed",
        removed.is_ok(),
        "the fill file is removed and synced",
        format!("{removed:?}"),
    );
    let (pools_alive, pool_detail) = wait_no_dead_pools(target, Duration::from_secs(300)).await;
    log.check(
        "s3_pool_recovered_after_fill_removal",
        pools_alive,
        "no pool remains Dead after cleanup",
        pool_detail,
    );
    let recovery_start = Instant::now();
    let mut recovery =
        put_via_any(target, "/load-degraded/s3-recovery", &random_bytes(64 * 1024), &[]).await;
    while recovery.is_err() && recovery_start.elapsed() < Duration::from_secs(300) {
        tokio::time::sleep(Duration::from_secs(5)).await;
        recovery =
            put_via_any(target, "/load-degraded/s3-recovery", &random_bytes(64 * 1024), &[]).await;
    }
    log.check(
        "s3_recovery_write_after_fill_removal",
        recovery.is_ok(),
        "the node accepts writes again after cleanup",
        format!("{recovery:?} after {:.0}s", recovery_start.elapsed().as_secs_f64()),
    );
}

// ── Scenario 4: corruption + heal ───────────────────────────────────────────

/// Fleet S4 — corrupt a live segment, scrub, poll read-back, second pass.
async fn scenario4_fleet(
    injector: &mut FleetInjector<'_>,
    target: &Target,
    timeout: Duration,
    log: &mut ScenarioLog,
) {
    let victim = VICTIM.min(target.len().saturating_sub(1));
    let path = "/load-degraded/s4-corrupt-blob";
    let body = random_bytes(256 * 1024);
    let _ = put_via_any(target, "/load-degraded", &[], &[]).await;
    // The cluster must be writable again by now (S3 gates recovery); retry
    // briefly to absorb the tail of the pool re-activation.
    let mut written = false;
    let mut write_detail = String::new();
    for attempt in 1..=24 {
        match put_via_any(target, path, &body, &[]).await {
            Ok(node) => {
                written = true;
                write_detail = format!("HTTP 2xx via node {node} on attempt {attempt}");
                break;
            }
            Err(e) => write_detail = e,
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    log.check("s4_blob_written", written, "a known blob is written with RF >= 2", write_detail);
    let (replicated, detail) =
        poll_read(target, victim, path, &body, Duration::from_secs(60)).await;
    log.check(
        "s4_replicated_to_target",
        replicated,
        "the victim holds a replica before corruption",
        detail,
    );
    if !replicated {
        return;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let segment = injector.newest_segment_id(victim, "data").await.ok().flatten();
    log.check(
        "s4_segment_discovered",
        segment.is_some(),
        "a live segment id is discovered on the victim's data volume",
        format!("{segment:?}"),
    );
    let Some(segment) = segment else { return };

    let before = scrape_all(target).await;
    let corrupted =
        injector.corrupt_segment_bytes(victim, "data", &segment, S4_CORRUPT_BYTES).await.is_ok();
    log.check(
        "s4_corruption_applied",
        corrupted,
        format!("{S4_CORRUPT_BYTES} bytes overwritten in a live segment"),
        format!("segment={segment} corrupted={corrupted}"),
    );
    let scrub = target.post(victim, "/admin/scrub").await.map(|r| r.status().as_u16()).unwrap_or(0);
    log.check(
        "s4_scrub_triggered",
        scrub == 202,
        "POST /admin/scrub returns 202 Accepted",
        format!("status={scrub}"),
    );

    let (healed, detail) = poll_read(target, victim, path, &body, timeout).await;
    log.check(
        "s4_readback_after_scrub",
        healed,
        "the victim serves the original bytes again within the timeout",
        detail,
    );

    // `POST /admin/scrub` is asynchronous (202): poll until a detection
    // counter moves (scrub cycle execution + any heal dispatch), bounded
    // by a generous window. Counters may move on any node — the scrub
    // partition owner and the repair target can differ from the victim.
    let evidence_start = Instant::now();
    let (d_scrub, d_ae, d_req, d_done, d_failed) = loop {
        let after = scrape_all(target).await;
        let d_scrub = total_counter_delta(&before, &after, "scrub_segments_corrupt_total");
        let d_ae = total_counter_delta(&before, &after, "ae_mismatches_found_total");
        let d_req = total_counter_delta(&before, &after, "heal_requests_total");
        let d_done = total_counter_delta(&before, &after, "heal_completed_total");
        let d_failed = total_counter_delta(&before, &after, "heal_failed_total");
        if d_scrub > 0.0 || d_ae > 0.0 || d_req > 0.0 || d_done > 0.0 {
            break (d_scrub, d_ae, d_req, d_done, d_failed);
        }
        if evidence_start.elapsed() > Duration::from_secs(240) {
            break (d_scrub, d_ae, d_req, d_done, d_failed);
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    };
    log.check(
        "s4_detection_observed",
        d_scrub > 0.0 || d_ae > 0.0 || d_req > 0.0 || d_done > 0.0,
        "the corruption is detected (scrub/AE/heal counters move fleet-wide)",
        format!(
            "scrub_segments_corrupt={d_scrub:.0} ae_mismatches_found={d_ae:.0} \
             heal_requests={d_req:.0} heal_completed={d_done:.0} heal_failed={d_failed:.0} \
             waited={:.0}s",
            evidence_start.elapsed().as_secs_f64()
        ),
    );
    // Heal counters conflate successful repair with benign stale-segment
    // races: a segment compacted/replaced between detection and the heal
    // worker's fetch is logged as a permanent failure (observed on the
    // f5 acceptance run — `EC decode failed: need at least 4 shards,
    // got 0` and `segment not found`). f3's S4 contract asserts the
    // READ-BACK path (correct bytes + clean second pass), so the count is
    // recorded as evidence; precise repair assertions are f4's.
    log.check(
        "s4_heal_failures_observed_evidence",
        true,
        "heal-failure count recorded as evidence (precise repair assertions are f4's)",
        format!("heal_requests={d_req:.0} heal_completed={d_done:.0} heal_failed={d_failed:.0}"),
    );

    // Second verification pass: trigger scrub again and confirm the blob is
    // still served correctly (the read-back is the black-box proof; the
    // counters are recorded in the detail).
    let scrub2 =
        target.post(victim, "/admin/scrub").await.map(|r| r.status().as_u16()).unwrap_or(0);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (ok2, detail2) = poll_read(target, victim, path, &body, Duration::from_secs(60)).await;
    log.check(
        "s4_second_pass_no_further_mismatch",
        scrub2 == 202 && ok2,
        "a second scrub pass leaves the blob readable with correct bytes",
        format!("scrub2={scrub2} {detail2}"),
    );
}

/// Local S4 — corrupt the newest local `pool-data` segment, scrub, read back.
async fn scenario4_local(
    cluster: &Cluster,
    target: &Target,
    timeout: Duration,
    log: &mut ScenarioLog,
) {
    let victim = VICTIM.min(target.len().saturating_sub(1));
    let path = "/load-degraded/s4-corrupt-blob";
    let body = random_bytes(256 * 1024);
    let _ = put_via_any(target, "/load-degraded", &[], &[]).await;
    // The cluster must be writable again by now (S3 gates recovery); retry
    // briefly to absorb the tail of the pool re-activation.
    let mut written = false;
    let mut write_detail = String::new();
    for attempt in 1..=24 {
        match put_via_any(target, path, &body, &[]).await {
            Ok(node) => {
                written = true;
                write_detail = format!("HTTP 2xx via node {node} on attempt {attempt}");
                break;
            }
            Err(e) => write_detail = e,
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    log.check("s4_blob_written", written, "a known blob is written with RF >= 2", write_detail);
    let (replicated, detail) =
        poll_read(target, victim, path, &body, Duration::from_secs(60)).await;
    log.check(
        "s4_replicated_to_target",
        replicated,
        "the victim holds a replica before corruption",
        detail,
    );
    if !replicated {
        return;
    }
    let root = match cluster.local_role_root(victim, "data") {
        Ok(root) => root,
        Err(e) => {
            log.records.push(FailureInjectionRecord::new(
                "segment_corrupt",
                victim,
                false,
                format!("data: local pool root missing: {e}"),
            ));
            log.check(
                "s4_segment_discovered",
                false,
                "local pool-data root exists",
                format!("{e}"),
            );
            return;
        }
    };
    tokio::time::sleep(Duration::from_secs(3)).await;
    let Some(segment) = newest_local_segment(&root) else {
        log.records.push(FailureInjectionRecord::new(
            "segment_corrupt",
            victim,
            false,
            "data: no .dat segment found on the local pool root".to_string(),
        ));
        log.check(
            "s4_segment_discovered",
            false,
            "a live segment id is discovered on the local data pool root",
            format!("no .dat under {}", root.display()),
        );
        return;
    };
    log.check(
        "s4_segment_discovered",
        true,
        "a live segment id is discovered on the local data pool root",
        format!("segment={segment}"),
    );

    let before = scrape_all(target).await;
    let corrupted = cluster.corrupt_shard(victim, &segment).await;
    log.records.push(FailureInjectionRecord::new(
        "segment_corrupt",
        victim,
        corrupted.is_ok(),
        match &corrupted {
            Ok(()) => format!("data: 64 bytes overwritten in segment {segment}"),
            Err(e) => format!("data: corrupt segment {segment} failed: {e}"),
        },
    ));
    log.check(
        "s4_corruption_applied",
        corrupted.is_ok(),
        format!("{S4_CORRUPT_BYTES} bytes overwritten in a live segment"),
        format!("segment={segment} result={corrupted:?}"),
    );

    let scrub = target.post(victim, "/admin/scrub").await.map(|r| r.status().as_u16()).unwrap_or(0);
    log.check(
        "s4_scrub_triggered",
        scrub == 202,
        "POST /admin/scrub returns 202 Accepted",
        format!("status={scrub}"),
    );

    let (healed, detail) = poll_read(target, victim, path, &body, timeout).await;
    log.check(
        "s4_readback_after_scrub",
        healed,
        "the victim serves the original bytes again within the timeout",
        detail,
    );

    // `POST /admin/scrub` is asynchronous (202): poll until a detection
    // counter moves (scrub cycle execution + any heal dispatch), bounded
    // by a generous window. Counters may move on any node — the scrub
    // partition owner and the repair target can differ from the victim.
    let evidence_start = Instant::now();
    let (d_scrub, d_ae, d_req, d_done, d_failed) = loop {
        let after = scrape_all(target).await;
        let d_scrub = total_counter_delta(&before, &after, "scrub_segments_corrupt_total");
        let d_ae = total_counter_delta(&before, &after, "ae_mismatches_found_total");
        let d_req = total_counter_delta(&before, &after, "heal_requests_total");
        let d_done = total_counter_delta(&before, &after, "heal_completed_total");
        let d_failed = total_counter_delta(&before, &after, "heal_failed_total");
        if d_scrub > 0.0 || d_ae > 0.0 || d_req > 0.0 || d_done > 0.0 {
            break (d_scrub, d_ae, d_req, d_done, d_failed);
        }
        if evidence_start.elapsed() > Duration::from_secs(240) {
            break (d_scrub, d_ae, d_req, d_done, d_failed);
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    };
    log.check(
        "s4_detection_observed",
        d_scrub > 0.0 || d_ae > 0.0 || d_req > 0.0 || d_done > 0.0,
        "the corruption is detected (scrub/AE/heal counters move fleet-wide)",
        format!(
            "scrub_segments_corrupt={d_scrub:.0} ae_mismatches_found={d_ae:.0} \
             heal_requests={d_req:.0} heal_completed={d_done:.0} heal_failed={d_failed:.0} \
             waited={:.0}s",
            evidence_start.elapsed().as_secs_f64()
        ),
    );
    // Heal counters conflate successful repair with benign stale-segment
    // races: a segment compacted/replaced between detection and the heal
    // worker's fetch is logged as a permanent failure (observed on the
    // f5 acceptance run — `EC decode failed: need at least 4 shards,
    // got 0` and `segment not found`). f3's S4 contract asserts the
    // READ-BACK path (correct bytes + clean second pass), so the count is
    // recorded as evidence; precise repair assertions are f4's.
    log.check(
        "s4_heal_failures_observed_evidence",
        true,
        "heal-failure count recorded as evidence (precise repair assertions are f4's)",
        format!("heal_requests={d_req:.0} heal_completed={d_done:.0} heal_failed={d_failed:.0}"),
    );

    let scrub2 =
        target.post(victim, "/admin/scrub").await.map(|r| r.status().as_u16()).unwrap_or(0);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (ok2, detail2) = poll_read(target, victim, path, &body, Duration::from_secs(60)).await;
    log.check(
        "s4_second_pass_no_further_mismatch",
        scrub2 == 202 && ok2,
        "a second scrub pass leaves the blob readable with correct bytes",
        format!("scrub2={scrub2} {detail2}"),
    );
}

// ── Main test ───────────────────────────────────────────────────────────────

/// Phase 4 — degraded mode under load. See the module docs for the contract.
#[tokio::test(flavor = "multi_thread")]
async fn load_degraded() {
    // ── Environment ────────────────────────────────────────────
    let seed: u64 =
        std::env::var("LOAD_TEST_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or_else(|| {
            let s: u64 = rand::random();
            eprintln!("LOAD_TEST_SEED not set, using random seed: {s}");
            s
        });
    eprintln!("load_degraded: seed={seed}");
    let duration_secs: u64 =
        std::env::var("LOAD_TEST_DURATION_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(300);
    let no_injections = matches!(
        std::env::var("LOAD_TEST_NO_INJECTIONS").ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    );
    let report_dir = std::env::var("LOAD_TEST_REPORT_DIR")
        .unwrap_or_else(|_| "/tmp/oceanfs-reports".to_string());
    let service = std::env::var("TARGET_SERVICE").unwrap_or_else(|_| "oceanfs".to_string());
    let target_hosts = std::env::var("TARGET_HOSTS").ok().filter(|s| !s.is_empty());
    let run_start = Instant::now();
    eprintln!(
        "load_degraded: duration={duration_secs}s control={no_injections} fleet={}",
        target_hosts.is_some()
    );

    // ── Topology ───────────────────────────────────────────────
    let mut local_cluster: Option<Arc<Cluster>> = None;
    let mut remote_cluster: Option<Arc<RemoteCluster>> = None;
    let target: Arc<Target> = match &target_hosts {
        Some(hosts) => {
            let remote = Arc::new(RemoteCluster::connect(hosts).expect("invalid TARGET_HOSTS"));
            remote.wait_for_health(Duration::from_secs(30)).await.expect("fleet health");
            assert!(
                remote.len() >= NODE_COUNT,
                "fleet mode needs >= {NODE_COUNT} nodes (quorum semantics); got {}",
                remote.len()
            );
            eprintln!("load_degraded: remote fleet at {hosts}");
            remote_cluster = Some(Arc::clone(&remote));
            Arc::new(Target::Remote(remote))
        }
        None => {
            let cluster = Arc::new(
                Cluster::spawn_with_options(
                    NODE_COUNT,
                    &config_cluster_churn(),
                    &NodeOptions::default(),
                )
                .await
                .expect("local cluster spawn"),
            );
            cluster.wait_for_convergence(NODE_COUNT).await.expect("local convergence");
            eprintln!(
                "load_degraded: local {NODE_COUNT}-node spawn (data_dir={})",
                cluster.node(0).data_dir().display()
            );
            local_cluster = Some(Arc::clone(&cluster));
            Arc::new(Target::Local(cluster))
        }
    };
    let is_fleet = remote_cluster.is_some();
    let victim = VICTIM.min(target.len().saturating_sub(1));

    let mut log = ScenarioLog::default();
    let initial_snaps = scrape_all(&target).await;

    // ── Injectors / control ────────────────────────────────────
    let mut fleet_injector: Option<FleetInjector<'_>> = None;
    let mut prereq_ok = true;
    if no_injections {
        eprintln!("load_degraded: control mode — no injections");
    } else if let Some(remote) = remote_cluster.as_ref() {
        let injector = FleetInjector::from_env(remote).expect("parse provisioning record");
        let mut missing: Vec<String> = Vec::new();
        for i in 0..target.len() {
            if injector.targets().get(i).and_then(|t| t.ssh()).is_none() {
                missing.push(format!(
                    "node {i}: no SSH target (set TARGET_HOST_SSH or a record internal_ip)"
                ));
            }
        }
        if injector.targets().get(victim).and_then(|t| t.volume("data")).is_none() {
            missing.push(format!(
                "node {victim}: no 'data' volume (provision with --volume-pools and pass the record)"
            ));
        }
        prereq_ok = missing.is_empty();
        log.check(
            "fleet_prerequisites",
            prereq_ok,
            "SSH for every node + a data volume for the injection target",
            format!("{missing:?}"),
        );
        fleet_injector = Some(injector);
    }
    let injections_ran = !no_injections && (!is_fleet || prereq_ok);

    // ── Background load (runs through the scenarios) ───────────
    let concurrency = std::env::var("LOAD_TEST_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| (num_cpus::get() * 2).clamp(4, 16));
    eprintln!("load_degraded: concurrency={concurrency}");
    let scenario = LoadScenario {
        concurrency,
        duration: Duration::from_secs(duration_secs),
        operations: vec![
            OpWeight { op: Operation::Put, weight: 0.40 },
            OpWeight { op: Operation::Get, weight: 0.50 },
            OpWeight { op: Operation::Delete, weight: 0.10 },
        ],
        blob_sizes: BlobSizeDist::Tiered {
            inline_pct: 15.0,
            small_pct: 35.0,
            standard_pct: 35.0,
            multi_pct: 15.0,
        },
        key_space: KeySpace::Zipfian { hot_keys: 100, cold_keys: 9900, skew: 1.0 },
        seed,
    };
    let manifest = Arc::new(Manifest::new());
    let load_handle = tokio::spawn({
        let target = Arc::clone(&target);
        let manifest = Arc::clone(&manifest);
        async move { Orchestrator::run(scenario, target, manifest).await }
    });

    // ── Scenarios ──────────────────────────────────────────────
    let s2_window = Duration::from_secs((duration_secs / 6).clamp(10, 30));
    let s3_window = Duration::from_secs((duration_secs / 12).clamp(8, 15));
    let heal_timeout = Duration::from_secs(180);

    if injections_ran {
        if let Some(injector) = fleet_injector.as_mut() {
            let remote = remote_cluster.as_ref().expect("fleet mode");
            let ssh = remote.ssh_target_for(victim).unwrap_or_default().to_string();
            scenario1_fleet(remote, &ssh, &service, &target, run_start, &mut log).await;
            scenario2_fleet(injector, &target, run_start, s2_window, &mut log).await;
            scenario3_fleet(injector, &target, s3_window, &mut log).await;
            scenario4_fleet(injector, &target, heal_timeout, &mut log).await;
        } else if let Some(cluster) = local_cluster.as_ref() {
            scenario1_local(cluster, &target, run_start, &mut log).await;
            scenario2_local(&mut log);
            scenario3_local(cluster, &target, s3_window, &mut log).await;
            scenario4_local(cluster, &target, heal_timeout, &mut log).await;
        }
    }

    // ── Load completes ─────────────────────────────────────────
    let stats = load_handle.await.expect("load task panicked");
    log.check(
        "background_load_served",
        stats.ops_total > 0,
        "the background workload served at least one operation",
        format!("ops_total={} errors_total={}", stats.ops_total, stats.errors_total),
    );

    if injections_ran {
        let (settled, detail) = settle_hints(&target, Duration::from_secs(60)).await;
        log.check(
            "hints_settled_at_end",
            settled,
            "no hint debt remains at the end of the run",
            detail,
        );
    }

    // ── Cross-cutting: health, panics, manifest ────────────────
    let healthy = wait_all_healthy(&target, Duration::from_secs(60)).await;
    log.check(
        "cluster_healthy_at_end",
        healthy,
        "every node answers /admin/health at the end of the run",
        format!("healthy={healthy}"),
    );

    if is_fleet {
        let remote = remote_cluster.as_ref().expect("fleet mode");
        if remote.ssh_target_for(0).is_none() {
            // Control runs do not require TARGET_HOST_SSH; a full run's
            // prerequisites already fail when it is missing.
            log.check(
                "no_node_panics",
                no_injections,
                "control run without SSH: journald scan not applicable",
                "TARGET_HOST_SSH unset",
            );
        } else if service_name_is_safe(&service) {
            match fleet_panic_lines(remote, &service).await {
                Ok(0) => {
                    log.check("no_node_panics", true, "zero 'panicked' lines in node journald", "0")
                }
                Ok(n) => log.check(
                    "no_node_panics",
                    false,
                    "zero 'panicked' lines in node journald",
                    format!("{n} panicked lines"),
                ),
                Err(e) => log.check(
                    "no_node_panics",
                    false,
                    "scan node journald for panics",
                    format!("scan failed: {e}"),
                ),
            }
        } else {
            log.check(
                "no_node_panics",
                false,
                "valid TARGET_SERVICE (journald unit name)",
                format!("unsafe service {service:?}"),
            );
        }
    } else if let Some(cluster) = local_cluster.as_ref() {
        let dirty = cluster.any_node_logs_contain("panicked");
        log.check(
            "no_node_panics",
            !dirty,
            "zero 'panicked' lines in local node logs",
            format!("panicked_found={dirty}"),
        );
    }

    let mut alive_indices = Vec::new();
    for i in 0..target.len() {
        if target.get(i, "/admin/health").await.map(|r| r.status().is_success()).unwrap_or(false) {
            alive_indices.push(i);
        }
    }

    // ── Pre-verify settle ──────────────────────────────────────
    // A dead data pool can leave ranges with fewer live replicas than RF
    // until recovery + repair converge. Trigger a fleet-wide scrub, then
    // wait (bounded by the 300s health recovery window plus repair time)
    // for no Dead pool and for both sampled quorum levels to be clean on
    // two consecutive checks.
    for i in 0..target.len() {
        let _ = target.post(i, "/admin/scrub").await;
    }
    let settle_start = Instant::now();
    let settle_limit = Duration::from_secs(360);
    let mut settle_clean = 0u32;
    let mut missing_keys =
        manifest.verify_read_quorum(&*target, &alive_indices, 1, Some(MANIFEST_SAMPLE)).await;
    let mut quorum_failed = manifest
        .verify_read_quorum(&*target, &alive_indices, READ_QUORUM, Some(MANIFEST_SAMPLE))
        .await;
    while settle_start.elapsed() < settle_limit
        && !(settle_clean >= 2 && missing_keys.is_empty() && quorum_failed.is_empty())
    {
        let dead = dead_pools(&target).await;
        if dead.is_empty() && missing_keys.is_empty() && quorum_failed.is_empty() {
            settle_clean += 1;
        } else {
            settle_clean = 0;
            if !missing_keys.is_empty() {
                missing_keys = manifest
                    .verify_read_quorum_keys(&missing_keys, &*target, &alive_indices, 1)
                    .await;
            }
            if !quorum_failed.is_empty() {
                quorum_failed = manifest
                    .verify_read_quorum_keys(&quorum_failed, &*target, &alive_indices, READ_QUORUM)
                    .await;
            }
            eprintln!(
                "load_degraded: settling ({:.0}s) dead_pools={dead:?} absent={} below_quorum={}",
                settle_start.elapsed().as_secs_f64(),
                missing_keys.len(),
                quorum_failed.len(),
            );
            // Re-trigger repair while settling (every ~60s).
            if settle_start.elapsed().as_secs() % 60 < 15 {
                for i in 0..target.len() {
                    let _ = target.post(i, "/admin/scrub").await;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(15)).await;
    }
    // Authoritative full passes after the settle.
    missing_keys =
        manifest.verify_read_quorum(&*target, &alive_indices, 1, Some(MANIFEST_SAMPLE)).await;
    quorum_failed = manifest
        .verify_read_quorum(&*target, &alive_indices, READ_QUORUM, Some(MANIFEST_SAMPLE))
        .await;
    let final_snaps = scrape_all(&target).await;
    let dropped_hints =
        total_counter_delta(&initial_snaps, &final_snaps, "hinted_handoff_hints_dropped_total");
    let repair_enqueued =
        total_counter_delta(&initial_snaps, &final_snaps, "oceanfs_repair_enqueued_total");
    let scrub_corrupt =
        total_counter_delta(&initial_snaps, &final_snaps, "scrub_segments_corrupt_total");
    log.check(
        "manifest_integrity",
        missing_keys.is_empty(),
        "every written key is readable with a recorded version from >= 1 node",
        format!(
            "{} of {} keys absent from every node after {:.0}s settle",
            missing_keys.len(),
            manifest.len(),
            settle_start.elapsed().as_secs_f64()
        ),
    );
    log.check(
        "manifest_read_quorum",
        quorum_failed.is_empty(),
        format!("every sampled key is served from >= {READ_QUORUM} nodes"),
        format!(
            "{} below quorum={READ_QUORUM} after {:.0}s settle (dead pools: {:?})",
            quorum_failed.len(),
            settle_start.elapsed().as_secs_f64(),
            dead_pools(&target).await
        ),
    );
    log.check(
        "durability_counters_observed_evidence",
        true,
        "durability/repair counters recorded as evidence — never threshold assertions",
        format!(
            "hints_dropped={dropped_hints:.0} repair_enqueued={repair_enqueued:.0} \
             scrub_segments_corrupt={scrub_corrupt:.0} final_pools={:?}",
            dead_pools(&target).await
        ),
    );

    // ── Injection record assertions ────────────────────────────
    let mut records: Vec<FailureInjectionRecord> = log.records.clone();
    if let Some(injector) = fleet_injector.as_ref() {
        records.extend(injector.records().iter().cloned());
    }
    if no_injections {
        log.check(
            "control_mode_no_injections",
            records.is_empty(),
            "control run records zero injection attempts",
            format!("{} records", records.len()),
        );
    } else if injections_ran {
        for injection_type in EXPECTED_INJECTION_TYPES {
            log.check(
                format!("injection_type_{injection_type}_recorded"),
                records.iter().any(|r| r.injection_type == injection_type),
                format!("at least one '{injection_type}' record"),
                format!(
                    "types: {:?}",
                    records.iter().map(|r| r.injection_type.as_str()).collect::<Vec<_>>()
                ),
            );
        }
        let is_skip = |r: &FailureInjectionRecord| r.detail.starts_with("skipped:");
        let failures: Vec<&FailureInjectionRecord> =
            records.iter().filter(|r| !r.success && !is_skip(r)).collect();
        let skips: Vec<&FailureInjectionRecord> =
            records.iter().filter(|r| !r.success && is_skip(r)).collect();
        log.check(
            "all_injections_succeeded",
            failures.is_empty(),
            "every non-skipped injection attempt reports success",
            format!(
                "{} failure(s): {:?}",
                failures.len(),
                failures.iter().map(|r| r.detail.as_str()).collect::<Vec<_>>()
            ),
        );
        log.check(
            "platform_skips_recorded",
            if is_fleet { skips.is_empty() } else { !skips.is_empty() },
            if is_fleet {
                "fleet mode executes every injector (zero skips)".to_string()
            } else {
                "local mode records the impossible network injector as skipped".to_string()
            },
            format!(
                "{} skip(s): {:?}",
                skips.len(),
                skips.iter().map(|r| r.detail.as_str()).collect::<Vec<_>>()
            ),
        );
        log.check(
            "records_attributed_to_injection_node",
            records.iter().all(|r| r.node_index == victim),
            format!("all records attributed to node {victim}"),
            format!("nodes: {:?}", records.iter().map(|r| r.node_index).collect::<Vec<_>>()),
        );
    }

    log.check(
        "perf_assertions_none",
        true,
        "no throughput/latency threshold is asserted anywhere (correctness only)",
        "perf: [] — RTT/RSS/GC observations are recorded as data; volume-backed numbers are not comparable to local-disk runs",
    );

    // ── Report ─────────────────────────────────────────────────
    let mut report = LoadReport::new(4, "load_degraded", seed);
    report.duration_secs = run_start.elapsed().as_secs_f64();
    report.worker_stats = Some(stats.clone());
    report.manifest = Some(e2e::load::ManifestSummary {
        objects_written: manifest.len(),
        objects_verified: manifest.len().saturating_sub(missing_keys.len()),
        mismatches: missing_keys.len(),
        mismatch_details: missing_keys
            .iter()
            .map(|k| e2e::load::Mismatch {
                key: k.clone(),
                expected_hash: "one of recorded versions".into(),
                actual_hash: "absent from every alive node".into(),
                node: "(all nodes)".into(),
            })
            .collect(),
    });
    report.metric_snapshots = initial_snaps;
    report.cluster_views = log.views.clone();
    report.record_injections(records.clone());
    for check in log.checks {
        report.assert(check);
    }
    report.harness_metrics = Some(e2e::load::HarnessSelfMetrics {
        process_resident_memory_bytes: e2e::harness::read_self_memory_bytes().unwrap_or(0),
        process_open_fds: e2e::harness::read_self_open_fds().unwrap_or(0),
    });
    report.finalize();

    let json_path = report.write_json_atomic(Path::new(&report_dir));
    match &json_path {
        Ok(path) => eprintln!("load_degraded: report written to {}", path.display()),
        Err(e) => eprintln!("load_degraded: FAILED to write JSON report: {e}"),
    }
    if let Err(e) = report.write_textfile_atomic(Path::new(&report_dir)) {
        eprintln!("load_degraded: failed to write textfile: {e}");
    }

    // ── Shutdown local cluster (no-op in remote mode) ──────────
    if !is_fleet {
        drop(local_cluster.take());
        if let Ok(Target::Local(cluster)) = Arc::try_unwrap(target) {
            let cluster = Arc::try_unwrap(cluster).expect("cluster Arc uniquely owned");
            let _ = cluster.shutdown().await;
        }
    }

    // ── Report self-check + final verdict ──────────────────────
    if let Ok(path) = &json_path {
        let written = fs::read_to_string(path).expect("read back report");
        let parsed: serde_json::Value = serde_json::from_str(&written).expect("valid JSON");
        if !records.is_empty() {
            assert_eq!(
                parsed["injection_records"].as_array().map(Vec::len),
                Some(records.len()),
                "report must serialize every injection record"
            );
        }
    }

    let fail_msg = format!(
        "load_degraded FAILED:\n\
         assertions: {} passed / {} total\n\
         injection records: {:?}\n\
         manifest: {} absent, {} quorum failures\n\
         worker ops: {} (errors {})\n\
         control mode: {no_injections}, fleet: {is_fleet}",
        report.assertions.iter().filter(|a| a.passed).count(),
        report.assertions.len(),
        records.iter().map(|r| format!("{}:{}", r.injection_type, r.success)).collect::<Vec<_>>(),
        missing_keys.len(),
        quorum_failed.len(),
        report.worker_stats.as_ref().map(|s| s.ops_total).unwrap_or(0),
        report.worker_stats.as_ref().map(|s| s.errors_total).unwrap_or(0),
    );
    assert_eq!(report.result, ReportResult::Pass, "{fail_msg}");
}
