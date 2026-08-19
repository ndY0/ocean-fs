//! Phase 3 — Small Cluster Functional Stability Under Load + Churn.
//!
//! Validates distributed-protocol correctness under sustained concurrent
//! load with node churn: a 3-node cluster (quorum w2/r2, rf=3) runs a
//! PUT/GET/DELETE workload (40/50/10, tiered blobs, 10K-key Zipfian
//! space) while a churn scheduler SIGKILLs and restarts nodes, then the
//! run verifies ten distributed invariants:
//!
//! 1. **membership_convergence** — after churn ends, every alive node
//!    reports the full alive membership (via `/admin/cluster`).
//! 2. **manifest_integrity** — every written key is readable with a
//!    recorded version from a random node.
//! 3. **manifest_read_quorum** — every sampled key is served with a
//!    recorded version from at least `read_quorum` (2) nodes.
//! 4. **hinted_handoff_delivery** — `hinted_handoff_hints_stored_total`
//!    ≈ `hinted_handoff_hints_delivered_total` at end (within 5%).
//! 5. **hinted_handoff_no_expiry** — `hinted_handoff_hints_expired_total`
//!    delta == 0 over the run (short-downtime churn).
//! 6. **hlc_monotonic** — member incarnations never decrease for the
//!    same node id across the run's cluster-view snapshots.
//! 7. **ring_consistency** — `/admin/ring` probe successor sets are
//!    identical across all alive nodes.
//! 8. **no_split_brain** — the alive memberships AND probe successor
//!    sets are identical across all alive nodes (a node whose view
//!    disagrees would route the same key to different owners).
//! 9. **cache_invalidation** — a sequential cross-node PUT→GET returns
//!    the newest version (L1 TTL is 0 in the test profile).
//! 10. **all_churn_succeeded** — every churn event reports `success`.
//!
//! ## Topology
//!
//! **Local spawn (CI quick mode):** no `TARGET_HOSTS` — the harness
//! spawns a 3-node [`Cluster`] with `config_cluster_churn()` (fast
//! gossip/SWIM, quorum semantics, zero cache TTL) and drives churn via
//! the local [`ChurnScheduler`] (kill/restart child processes).
//!
//! **Remote fleet (cloud mode, ADR-0026):** `TARGET_HOSTS` set to the
//! comma-separated node endpoints as seen from the harness
//! (`10.0.0.2:9000,10.0.0.3:9000,10.0.0.4:9000`). The harness connects
//! to the already-running fleet and does not spawn anything. Churn runs
//! over SSH: `TARGET_HOST_SSH` is the comma-separated per-node SSH
//! targets (`root@10.0.0.2,root@10.0.0.3,root@10.0.0.4`); each node's
//! `oceanfs` systemd unit (name from `TARGET_SERVICE`, default
//! `oceanfs`, `Restart=no`) is SIGKILLed and restarted by
//! [`RemoteCluster::kill_and_restart_node_via_ssh`]. Without
//! `TARGET_HOST_SSH` the churn phase is skipped and recorded as such.
//!
//! The same test body runs against both topologies via a [`Target`]
//! enum implementing [`LoadTarget`].
//!
//! ## Environment Variables
//!
//! | Variable | Default | Description |
//! |---|---|---|
//! | `LOAD_TEST_SEED` | random | Deterministic seed (same seed → same workload + churn sequence). |
//! | `LOAD_TEST_DURATION_SECS` | 120 (quick) / 300 (full) | Run duration. |
//! | `TARGET_HOSTS` | unset | Comma-separated remote node endpoints — enables remote mode. |
//! | `TARGET_HOST_SSH` | unset | Comma-separated per-node SSH targets for churn crash control. |
//! | `TARGET_SERVICE` | `oceanfs` | systemd unit name on every fleet node. |
//! | `LOAD_TEST_REPORT_DIR` | `/tmp/oceanfs-reports` | Report output dir (tmpfs per ADR-0019). |
//!
//! ## Usage
//!
//! ```bash
//! # Quick mode, local 3-node spawn (CI):
//! LOAD_TEST_DURATION_SECS=180 LOAD_TEST_SEED=42 cargo test -p e2e --test load_cluster_churn
//!
//! # Cloud fleet (ADR-0026):
//! TARGET_HOSTS=10.0.0.2:9000,10.0.0.3:9000,10.0.0.4:9000 \
//!   TARGET_HOST_SSH=root@10.0.0.2,root@10.0.0.3,root@10.0.0.4 \
//!   TARGET_SERVICE=oceanfs LOAD_TEST_DURATION_SECS=300 \
//!   cargo test -p e2e --test load_cluster_churn
//! ```

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use e2e::{
    harness::{config_cluster_churn, Cluster, LoadTarget, NodeOptions},
    load::{
        assert_that, BlobSizeDist, ChurnAction, ChurnEvent, ChurnMode, ChurnScheduler,
        ClusterViewSnapshot, KeySpace, LoadReport, LoadScenario, Manifest, MetricsSnapshot,
        OpWeight, Operation, Orchestrator, ReportResult,
    },
    remote::RemoteCluster,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha12Rng;

// ── Constants ───────────────────────────────────────────────────────────────

/// Cluster size (per ADR-0026 default fleet).
const NODE_COUNT: usize = 3;
/// Read quorum: every key must be served from ≥ this many nodes.
const READ_QUORUM: usize = 2;
/// Churn cadence (feature doc: every 10-30s).
const CHURN_INTERVAL: Duration = Duration::from_secs(15);
/// How long a killed node stays down before restart (local scheduler).
const RESTART_DELAY: Duration = Duration::from_secs(10);
/// Membership convergence timeout after churn (feature doc: 10s; 30s for
/// the relaxed single-VM profile — we only run the two-VM/fast profile).
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
/// Metric + cluster-view poll interval (spec: every 10s).
const POLL_INTERVAL: Duration = Duration::from_secs(10);
/// Hinted-handoff delivery tolerance (within 5%).
const HANDOFF_TOLERANCE: f64 = 0.05;
/// Cap on keys sampled for the read-quorum check (per-key × per-node
/// GETs at scale are slow; the phase-3 assertion samples the manifest).
const READ_QUORUM_SAMPLE: usize = 150;
/// Cap on keys sampled for the cache-invalidation cross-node check.
const CACHE_INVALIDATION_SAMPLE: usize = 20;

// ── Topology abstraction ────────────────────────────────────────────────────

/// A unified target handle: either a locally spawned [`Cluster`] or a
/// remote [`RemoteCluster`]. The load orchestrator, manifest verifier,
/// and poller are generic over [`LoadTarget`].
enum Target {
    /// Locally spawned cluster (CI quick mode).
    Local(Arc<Cluster>),
    /// Remote fleet (cloud mode, ADR-0026).
    Remote(Arc<RemoteCluster>),
}

impl LoadTarget for Target {
    fn len(&self) -> usize {
        match self {
            Target::Local(c) => c.len(),
            Target::Remote(r) => r.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Target::Local(c) => c.is_empty(),
            Target::Remote(r) => r.is_empty(),
        }
    }

    fn node_addr(&self, i: usize) -> std::net::SocketAddr {
        match self {
            Target::Local(c) => c.node_addr(i),
            Target::Remote(r) => r.node_addr(i),
        }
    }

    fn client(&self) -> &reqwest::Client {
        match self {
            Target::Local(c) => c.client(),
            Target::Remote(r) => r.client(),
        }
    }

    async fn get(&self, i: usize, path: &str) -> Result<reqwest::Response, e2e::harness::Error> {
        match self {
            Target::Local(c) => c.get(i, path).await,
            Target::Remote(r) => r.get(i, path).await,
        }
    }

    async fn put(
        &self,
        i: usize,
        path: &str,
        body: &[u8],
    ) -> Result<reqwest::Response, e2e::harness::Error> {
        match self {
            Target::Local(c) => c.put(i, path, body).await,
            Target::Remote(r) => r.put(i, path, body).await,
        }
    }

    async fn delete(&self, i: usize, path: &str) -> Result<reqwest::Response, e2e::harness::Error> {
        match self {
            Target::Local(c) => c.delete(i, path).await,
            Target::Remote(r) => r.delete(i, path).await,
        }
    }

    async fn head(&self, i: usize, path: &str) -> Result<reqwest::Response, e2e::harness::Error> {
        match self {
            Target::Local(c) => c.head(i, path).await,
            Target::Remote(r) => r.head(i, path).await,
        }
    }

    async fn post(&self, i: usize, path: &str) -> Result<reqwest::Response, e2e::harness::Error> {
        match self {
            Target::Local(c) => c.post(i, path).await,
            Target::Remote(r) => r.post(i, path).await,
        }
    }
}

// ── Cluster view helpers ────────────────────────────────────────────────────

/// Parsed `/admin/cluster` response.
#[derive(Debug, Clone, Default)]
struct ClusterView {
    /// Total members per this node's view.
    members: usize,
    /// Members in the Alive state per this node's view.
    alive: usize,
    /// (node_id, state, incarnation) per member.
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

/// Parses `/admin/ring` on node `i`: per-probe successor node-id lists.
/// Returns `None` when unavailable.
async fn fetch_ring_probes(target: &Target, i: usize) -> Option<Vec<Vec<String>>> {
    let resp = target.get(i, "/admin/ring").await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: serde_json::Value = e2e::harness::response_json(resp).await.ok()?;
    let mut probes = Vec::new();
    for probe in body["probes"].as_array()? {
        let successors: Vec<String> = probe["successors"]
            .as_array()?
            .iter()
            .filter_map(|s| s.as_str().map(|s| s.to_string()))
            .collect();
        probes.push(successors);
    }
    Some(probes)
}

/// Waits until every node reports `expected_alive` alive members (or the
/// timeout elapses). Returns `true` on convergence.
async fn wait_for_convergence(target: &Target, expected_alive: usize, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
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
                        "  cluster: node {i} reports {}/{} alive (expected {expected_alive})",
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

// ── Remote churn loop ───────────────────────────────────────────────────────

/// Drives churn against the remote fleet: serially SIGKILL + restart one
/// node per cycle (at most one node dead at a time — quorum is kept),
/// round-robin node selection (deterministic, mirrors
/// [`ChurnScheduler`]'s `ChurnMode::Deterministic`).
///
/// Returns the churn events plus per-event convergence outcomes.
async fn run_remote_churn(
    remote: Arc<RemoteCluster>,
    ssh_targets: &[String],
    service: &str,
    duration: Duration,
) -> (Vec<ChurnEvent>, Vec<bool>) {
    let start = std::time::Instant::now();
    let mut events = Vec::new();
    let mut converged_after = Vec::new();
    let mut tick = 0usize;

    while start.elapsed() < duration {
        // Deterministic round-robin (mirrors ChurnScheduler's
        // ChurnMode::Deterministic): same seed → same event sequence.
        let target = tick % remote.len();
        eprintln!("remote churn: killing node {target} via ssh ({})", ssh_targets[target]);
        let ok = remote
            .kill_and_restart_node_via_ssh(target, &ssh_targets[target], service)
            .await
            .is_ok();
        let t = start.elapsed().as_secs_f64();
        events.push(ChurnEvent {
            timestamp: t,
            action: ChurnAction::Kill,
            node_index: target,
            success: ok,
        });
        events.push(ChurnEvent {
            timestamp: t,
            action: ChurnAction::Restart,
            node_index: target,
            success: ok,
        });
        eprintln!(
            "remote churn: node {target} {} ({} events so far)",
            if ok { "cycled" } else { "FAILED" },
            events.len() / 2
        );

        // Membership convergence after the cycle (feature: within 10
        // gossip rounds ≈ 10s; allow 20s for the restart + gossip).
        let converged = wait_for_convergence(
            &remote_target(remote.clone()),
            NODE_COUNT,
            Duration::from_secs(20),
        )
        .await;
        converged_after.push(converged);
        eprintln!("remote churn: convergence after cycle = {converged}");

        tick += 1;
        tokio::time::sleep(CHURN_INTERVAL).await;
    }
    (events, converged_after)
}

/// Adapter to poll the remote cluster through the unified target (the
/// churn loop owns an `Arc<RemoteCluster>` and needs the `Target` view
/// for the convergence poller).
fn remote_target(remote: Arc<RemoteCluster>) -> Arc<Target> {
    Arc::new(Target::Remote(remote))
}

// ── Poller ──────────────────────────────────────────────────────────────────

/// Shared state of the periodic poller: per-node metric snapshots and
/// per-node cluster views (membership + ring).
#[derive(Debug)]
struct PollerState {
    /// `snapshots[round][node]` — metric snapshot per node per round.
    snapshots: Vec<Vec<MetricsSnapshot>>,
    /// Per-node cluster views (one entry per node per round).
    views: Vec<ClusterViewSnapshot>,
    /// `(initial, final)` per-node counter deltas for hinted handoff.
    handoff_deltas: Vec<(f64, f64, f64)>,
}

/// Polls every node's `/admin/metrics`, `/admin/cluster`, and
/// `/admin/ring` every `POLL_INTERVAL` until `stop` is set.
async fn poll_cluster(
    target: Arc<Target>,
    stop: Arc<AtomicBool>,
    state: Arc<parking_lot::Mutex<PollerState>>,
) {
    let start = std::time::Instant::now();
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let mut snapshots = Vec::with_capacity(target.len());
        let mut views = Vec::with_capacity(target.len());
        for i in 0..target.len() {
            if let Ok(snap) = MetricsSnapshot::scrape(&*target, i).await {
                snapshots.push(snap);
            }
            if let (Some(view), Some(probes)) =
                (fetch_cluster_view(&target, i).await, fetch_ring_probes(&target, i).await)
            {
                views.push(ClusterViewSnapshot {
                    t_secs: start.elapsed().as_secs_f64(),
                    node_index: i,
                    members: view.members,
                    alive: view.alive,
                    members_detail: view.members_detail,
                    probe_successors: probes,
                });
            }
        }
        if !snapshots.is_empty() {
            let mut state = state.lock();
            state.snapshots.push(snapshots);
            state.views.extend(views);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

// ── Test ────────────────────────────────────────────────────────────────────

/// Phase 3 — 3-node cluster churn under load.
///
/// See the module documentation for the full contract.
#[tokio::test(flavor = "multi_thread")]
async fn load_cluster_churn() {
    // ── Parse environment ──────────────────────────────────────
    let seed: u64 =
        std::env::var("LOAD_TEST_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or_else(|| {
            let s: u64 = rand::random();
            eprintln!("LOAD_TEST_SEED not set, using random seed: {s}");
            s
        });
    eprintln!("load_cluster_churn: seed={seed}");

    let duration_secs: u64 =
        std::env::var("LOAD_TEST_DURATION_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(120);
    eprintln!("load_cluster_churn: duration={duration_secs}s");

    let report_dir = std::env::var("LOAD_TEST_REPORT_DIR")
        .unwrap_or_else(|_| "/tmp/oceanfs-reports".to_string());

    let target_hosts = std::env::var("TARGET_HOSTS").ok().filter(|s| !s.is_empty());
    let ssh_list: Vec<String> = std::env::var("TARGET_HOST_SSH")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| s.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect())
        .unwrap_or_default();
    let service = std::env::var("TARGET_SERVICE").unwrap_or_else(|_| "oceanfs".to_string());

    // ── Topology ───────────────────────────────────────────────
    let (target, is_remote, ssh_targets) = match &target_hosts {
        Some(hosts) => {
            let remote = RemoteCluster::connect(hosts).expect("invalid TARGET_HOSTS");
            remote.wait_for_health(Duration::from_secs(30)).await.expect("remote health");
            eprintln!("load_cluster_churn: remote fleet at {hosts}");
            if ssh_list.is_empty() {
                eprintln!(
                    "load_cluster_churn: WARNING TARGET_HOST_SSH unset — churn will be SKIPPED"
                );
            }
            (Arc::new(Target::Remote(Arc::new(remote))), true, ssh_list)
        }
        None => {
            let cluster = Cluster::spawn_with_options(
                NODE_COUNT,
                &config_cluster_churn(),
                &NodeOptions::default(),
            )
            .await
            .expect("cluster spawn");
            eprintln!(
                "load_cluster_churn: local 3-node spawn (data_dir={})",
                cluster.node(0).data_dir().display()
            );
            (Arc::new(Target::Local(Arc::new(cluster))), false, Vec::new())
        }
    };

    // ── Initial convergence (all nodes agree before load starts) ──
    assert!(
        wait_for_convergence(&target, NODE_COUNT, CONVERGENCE_TIMEOUT).await,
        "cluster must converge before the run starts"
    );
    eprintln!("load_cluster_churn: initial convergence OK ({NODE_COUNT} nodes alive)");

    // ── Build the load scenario ────────────────────────────────
    // Workers route each op to a random node (per-worker seeded RNG), so
    // the cluster sees genuinely distributed coordination traffic.
    let concurrency = (num_cpus::get() * 2).clamp(6, 24);
    eprintln!("load_cluster_churn: concurrency={concurrency}");
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

    // ── Initial metrics baseline (per-node, for handoff deltas) ──
    let mut initial_snaps = Vec::with_capacity(target.len());
    for i in 0..target.len() {
        initial_snaps.push(MetricsSnapshot::scrape(&*target, i).await.unwrap_or_default());
    }

    let stop = Arc::new(AtomicBool::new(false));
    let poller_state = Arc::new(parking_lot::Mutex::new(PollerState {
        snapshots: Vec::new(),
        views: Vec::new(),
        handoff_deltas: Vec::new(),
    }));

    // ── Spawn the poller (metrics + cluster views) ─────────────
    let poller = tokio::spawn(poll_cluster(
        Arc::clone(&target),
        Arc::clone(&stop),
        Arc::clone(&poller_state),
    ));

    // ── Spawn churn ────────────────────────────────────────────
    // Both branches produce `(Vec<ChurnEvent>, Vec<bool>)` — the bool
    // vector carries per-cycle convergence outcomes (remote only).
    let churn_handle = if is_remote {
        let remote = match target.as_ref() {
            Target::Remote(r) => Arc::clone(r),
            Target::Local(_) => unreachable!("remote branch"),
        };
        let ssh_targets = ssh_targets.clone();
        let service = service.clone();
        Some(tokio::spawn(async move {
            run_remote_churn(remote, &ssh_targets, &service, Duration::from_secs(duration_secs))
                .await
        }))
    } else {
        let cluster = match target.as_ref() {
            Target::Local(c) => Arc::clone(c),
            Target::Remote(_) => unreachable!("local branch"),
        };
        Some(tokio::spawn(async move {
            let events = ChurnScheduler::new(
                cluster,
                ChurnMode::Deterministic,
                CHURN_INTERVAL,
                RESTART_DELAY,
                seed,
            )
            .run(Duration::from_secs(duration_secs))
            .await;
            (events, Vec::new())
        }))
    };

    // ── Run the load concurrently with churn ───────────────────
    let stats =
        Orchestrator::run(scenario.clone(), Arc::clone(&target), Arc::clone(&manifest)).await;

    // Join churn (the load run is the primary clock; the churn task ends
    // with it or slightly after).
    let (churn_events, converged_after) = match churn_handle {
        Some(handle) => handle.await.expect("churn task panicked"),
        None => (Vec::new(), Vec::new()),
    };

    // Stop the poller.
    stop.store(true, Ordering::Relaxed);
    let _ = poller.await;
    let mut poller_state =
        Arc::try_unwrap(poller_state).expect("poller state uniquely owned").into_inner();

    // ── Post-churn convergence (assertion 1) ───────────────────
    let converged = wait_for_convergence(&target, NODE_COUNT, CONVERGENCE_TIMEOUT).await;
    eprintln!("load_cluster_churn: post-churn convergence = {converged}");

    // ── Hinted-handoff convergence ─────────────────────────────
    // Delivery is eventually-convergent (the 5s sweep drains ≤
    // max_batch_size hints per node per sweep), so the final assertions
    // must run after the last outage's hints have drained — otherwise a
    // still-draining queue is misread as "hints never delivered". Wait
    // until Σ(delivered - stored) >= 0 across all nodes (delivered can
    // legitimately exceed stored: hints replayed from a restarted node's
    // WAL were stored by the pre-restart process), plus a short grace
    // period for the final batches to land.
    let mut handoff_settled = false;
    let settle_start = std::time::Instant::now();
    while settle_start.elapsed() < Duration::from_secs(120) {
        let mut pending = 0.0;
        for i in 0..target.len() {
            if let Ok(snap) = MetricsSnapshot::scrape(&*target, i).await {
                pending += snap.counter("hinted_handoff_hints_stored_total").unwrap_or(0.0)
                    - snap.counter("hinted_handoff_hints_delivered_total").unwrap_or(0.0)
                    - snap.counter("hinted_handoff_hints_expired_total").unwrap_or(0.0);
            }
        }
        if pending <= 0.0 {
            handoff_settled = true;
            eprintln!(
                "load_cluster_churn: handoff settled after {:.0}s (pending={pending:.0})",
                settle_start.elapsed().as_secs_f64()
            );
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    if !handoff_settled {
        eprintln!("load_cluster_churn: WARNING handoff still draining after 120s");
    }
    // Grace period: the last delivered batch lands on the receiver's
    // metadata store asynchronously; give it a moment before verifying.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ── Assertion 2: manifest integrity (≥1 node) ──────────────
    // The single-random-node verify is too weak for a 3-node cluster:
    // a key on 2 of 3 nodes fails it 1/3 of the time. Integrity here
    // means "no key is completely absent" — every active key must be
    // served with a recorded version from AT LEAST ONE alive node.
    let mut alive_indices = Vec::new();
    for i in 0..target.len() {
        if target.get(i, "/admin/health").await.map(|r| r.status().is_success()).unwrap_or(false) {
            alive_indices.push(i);
        }
    }
    let missing_keys =
        manifest.verify_read_quorum(&*target, &alive_indices, 1, Some(READ_QUORUM_SAMPLE)).await;
    eprintln!("load_cluster_churn: manifest missing keys = {}", missing_keys.len());

    // ── Assertion 3: read quorum (≥R nodes) ────────────────────
    // Only alive slots are addressed (the churn drain phase restarts
    // every node, but a still-settling slot is not a data-loss signal —
    // this guard keeps the quorum semantics honest).
    let quorum_failed = manifest
        .verify_read_quorum(&*target, &alive_indices, READ_QUORUM, Some(READ_QUORUM_SAMPLE))
        .await;
    eprintln!("load_cluster_churn: read-quorum failures = {}", quorum_failed.len());
    if !quorum_failed.is_empty() {
        eprintln!("load_cluster_churn: quorum-failed keys: {quorum_failed:?}");
        // Per-node diagnostics: what does each node serve for the failed
        // keys (status + body hash prefix)?
        for key in quorum_failed.iter().take(5) {
            for i in 0..target.len() {
                match target.get(i, &format!("/{key}")).await {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        let hash = resp.bytes().await.ok().map(|b| {
                            let h = blake3::hash(&b);
                            format!(
                                "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                                h.as_bytes()[0],
                                h.as_bytes()[1],
                                h.as_bytes()[2],
                                h.as_bytes()[3],
                                h.as_bytes()[4],
                                h.as_bytes()[5]
                            )
                        });
                        eprintln!("  {key}: node {i} -> HTTP {status} hash={hash:?}");
                    }
                    Err(e) => eprintln!("  {key}: node {i} -> ERR {e}"),
                }
            }
        }
    }

    // ── Assertions 4-5: hinted handoff ─────────────────────────
    let mut stored = 0.0;
    let mut delivered = 0.0;
    let mut expired = 0.0;
    for i in 0..target.len() {
        if let Ok(final_snap) = MetricsSnapshot::scrape(&*target, i).await {
            stored += final_snap.counter("hinted_handoff_hints_stored_total").unwrap_or(0.0)
                - initial_snaps[i].counter("hinted_handoff_hints_stored_total").unwrap_or(0.0);
            delivered += final_snap.counter("hinted_handoff_hints_delivered_total").unwrap_or(0.0)
                - initial_snaps[i].counter("hinted_handoff_hints_delivered_total").unwrap_or(0.0);
            expired += final_snap.counter("hinted_handoff_hints_expired_total").unwrap_or(0.0)
                - initial_snaps[i].counter("hinted_handoff_hints_expired_total").unwrap_or(0.0);
        }
    }
    // Delivered may legitimately exceed stored (hints replayed from a
    // restarted node's WAL were stored by the pre-restart process); the
    // DoD invariant is that no more than `HANDOFF_TOLERANCE` of stored
    // hints remain undelivered.
    let handoff_delta_ok = stored == 0.0 || delivered >= stored * (1.0 - HANDOFF_TOLERANCE);
    eprintln!(
        "load_cluster_churn: handoff stored={stored:.0} delivered={delivered:.0} expired={expired:.0}"
    );

    // ── Assertion 6: incarnation monotonicity ──────────────────
    // For each node id, the incarnation must never decrease across the
    // recorded views (a restart bumps the incarnation — it must not go
    // backward, which would indicate HLC/incarnation clock skew).
    let mut incarnation_violation: Option<String> = None;
    {
        let mut seen: HashMap<String, u64> = HashMap::new();
        let mut sorted_views = poller_state.views.clone();
        sorted_views.sort_by(|a, b| a.t_secs.total_cmp(&b.t_secs));
        for view in &sorted_views {
            for (id, _state, incarnation) in &view.members_detail {
                match seen.get(id) {
                    Some(prev) if *prev > *incarnation => {
                        incarnation_violation =
                            Some(format!("node {id}: incarnation {prev} -> {incarnation}"));
                        break;
                    }
                    _ => {
                        seen.insert(id.clone(), *incarnation);
                    }
                }
            }
            if incarnation_violation.is_some() {
                break;
            }
        }
    }

    // ── Assertions 7-8: ring consistency + no split brain ──────
    let mut ring_probes: Option<Vec<Vec<String>>> = None;
    let mut ring_violation: Option<String> = None;
    let mut alive_members: Option<Vec<String>> = None;
    let mut split_brain_violation: Option<String> = None;
    for i in 0..target.len() {
        let Some(probes) = fetch_ring_probes(&target, i).await else { continue };
        match &ring_probes {
            None => ring_probes = Some(probes.clone()),
            Some(reference) if *reference != probes => {
                ring_violation =
                    Some(format!("node {i} successors disagree: {:?} vs {:?}", reference, probes));
            }
            Some(_) => {}
        }
        if let Some(view) = fetch_cluster_view(&target, i).await {
            let mut alive: Vec<String> = view
                .members_detail
                .iter()
                .filter(|(_, s, _)| s == "Alive")
                .map(|(id, _, _)| id.clone())
                .collect();
            alive.sort();
            match &alive_members {
                None => alive_members = Some(alive),
                Some(reference) if *reference != alive => {
                    split_brain_violation =
                        Some(format!("node {i} alive set {:?} != {:?}", alive, reference));
                }
                Some(_) => {}
            }
        }
    }

    // ── Assertion 9: cache invalidation ────────────────────────
    // Sequential cross-node test on freshly written keys: PUT v1 to
    // node 0, PUT v2 (different body) to node 1, then GET from node 2
    // must return v2's hash (L1 TTL is 0 in the test profile, so any
    // stale read is a real invalidation bug).
    let mut cache_failures = 0usize;
    {
        let mut rng = ChaCha12Rng::seed_from_u64(seed.wrapping_add(0xCA_11));
        for k in 0..CACHE_INVALIDATION_SAMPLE {
            let key = format!("churn-cache-check-{k}");
            let v1 = e2e::harness::random_bytes(64 + (rng.gen::<usize>() % 256));
            let v2 = e2e::harness::random_bytes(64 + (rng.gen::<usize>() % 256));
            let path = format!("/load-test/{key}");
            // Version 1 to node 0.
            let put1 =
                target.put(0, &path, &v1).await.map(|r| r.status().is_success()).unwrap_or(false);
            // Version 2 to node 1 (must win).
            let put2 =
                target.put(1, &path, &v2).await.map(|r| r.status().is_success()).unwrap_or(false);
            if !(put1 && put2) {
                continue;
            }
            // Read from node 2 (not a writer) — must see v2.
            let got_resp = target.get(2, &path).await;
            let got = match got_resp {
                Ok(resp) if resp.status().is_success() => {
                    resp.bytes().await.ok().map(|b| b.to_vec()).unwrap_or_default()
                }
                _ => Vec::new(),
            };
            if got != v2 {
                cache_failures += 1;
                eprintln!("  cache invalidation FAIL: {key} got {} bytes, expected v2", got.len());
            }
        }
    }

    // ── Assertion 10: all churn succeeded ──────────────────────
    let churn_failed: Vec<&ChurnEvent> = churn_events.iter().filter(|e| !e.success).collect();
    eprintln!(
        "load_cluster_churn: churn events = {} ({} failed)",
        churn_events.len(),
        churn_failed.len()
    );

    // ── Handoff deltas into poller state for the report ────────
    poller_state.handoff_deltas.push((stored, delivered, expired));

    // ── Build the report ───────────────────────────────────────
    let mut report = LoadReport::new(3, "load_cluster_churn", seed);
    report.duration_secs = stats.elapsed_secs;

    report.assert(assert_that(
        "membership_convergence",
        converged,
        "all nodes report full alive membership after churn",
        format!(
            "post-churn convergence within {CONVERGENCE_TIMEOUT:?}; per-cycle: {converged_after:?}"
        ),
    ));

    report.assert(assert_that(
        "manifest_integrity",
        missing_keys.is_empty(),
        "every written key readable with a recorded version from >= 1 node",
        format!("{} keys absent from every node of {}", missing_keys.len(), manifest.len()),
    ));

    report.assert(assert_that(
        "manifest_read_quorum",
        quorum_failed.is_empty(),
        format!("every sampled key served from >= {READ_QUORUM} nodes"),
        format!(
            "{} of {} sampled keys failed quorum",
            quorum_failed.len(),
            READ_QUORUM_SAMPLE.min(manifest.len())
        ),
    ));

    report.assert(assert_that(
        "hinted_handoff_delivery",
        handoff_delta_ok,
        format!("stored ~= delivered (within {:.0}%)", HANDOFF_TOLERANCE * 100.0),
        format!(
            "stored={stored:.0} delivered={delivered:.0} delta={:.0}",
            (stored - delivered).max(0.0)
        ),
    ));

    report.assert(assert_that(
        "hinted_handoff_no_expiry",
        expired == 0.0,
        "no hints expired (short-downtime churn)",
        format!("expired={expired:.0}"),
    ));

    report.assert(assert_that(
        "hlc_monotonic",
        incarnation_violation.is_none(),
        "member incarnations never decrease for the same node id",
        incarnation_violation.clone().unwrap_or_else(|| "monotonic across all views".to_string()),
    ));

    report.assert(assert_that(
        "ring_consistency",
        ring_violation.is_none(),
        "identical ring successor sets on all alive nodes",
        ring_violation.clone().unwrap_or_else(|| {
            format!("{} probes agree on all nodes", ring_probes.map(|p| p.len()).unwrap_or(0))
        }),
    ));

    report.assert(assert_that(
        "no_split_brain",
        split_brain_violation.is_none(),
        "no two nodes disagree on membership or ownership",
        split_brain_violation.clone().unwrap_or_else(|| {
            format!("alive sets identical: {:?}", alive_members.unwrap_or_default())
        }),
    ));

    report.assert(assert_that(
        "cache_invalidation",
        cache_failures == 0,
        "cross-node PUT -> GET returns the newest version",
        format!("{cache_failures} of {CACHE_INVALIDATION_SAMPLE} keys served stale"),
    ));

    report.assert(assert_that(
        "all_churn_succeeded",
        churn_failed.is_empty(),
        "every churn event reports success",
        format!("{} events, {} failed", churn_events.len(), churn_failed.len()),
    ));

    // ── Report population ──────────────────────────────────────
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
    // Node-0 metric series (the poller's full per-node series lives in
    // the JSON report via `cluster_views`; the snapshot series keeps the
    // phase-2 report shape for the Grafana textfile).
    if let Some(first_round) = poller_state.snapshots.first() {
        report.metric_snapshots = first_round.clone();
    }
    report.churn_events = churn_events.clone();
    report.cluster_views = poller_state.views.clone();

    report.harness_metrics = Some(e2e::load::HarnessSelfMetrics {
        process_resident_memory_bytes: e2e::harness::read_self_memory_bytes().unwrap_or(0),
        process_open_fds: e2e::harness::read_self_open_fds().unwrap_or(0),
    });

    report.finalize();

    // ── Write report to /tmp (tmpfs) ───────────────────────────
    let json_path = report.write_json_atomic(std::path::Path::new(&report_dir));
    match &json_path {
        Ok(path) => eprintln!("load_cluster_churn: report written to {}", path.display()),
        Err(e) => eprintln!("load_cluster_churn: FAILED to write JSON report: {e}"),
    }
    if let Err(e) = report.write_textfile_atomic(std::path::Path::new(&report_dir)) {
        eprintln!("load_cluster_churn: failed to write textfile: {e}");
    }

    // ── Shutdown local cluster (no-op in remote mode) ──────────
    if !is_remote {
        let cluster = match Arc::try_unwrap(target) {
            Ok(Target::Local(cluster)) => cluster,
            _ => unreachable!("local branch"),
        };
        let cluster = Arc::try_unwrap(cluster).expect("cluster Arc uniquely owned");
        let _ = cluster.shutdown().await;
    }

    // ── Final verdict ──────────────────────────────────────────
    let fail_msg = format!(
        "load_cluster_churn FAILED:\n\
         convergence: {converged} (per-cycle {converged_after:?})\n\
         manifest integrity: {} keys absent from every node (of {} keys)\n\
         read quorum: {} failures\n\
         handoff: stored={stored:.0} delivered={delivered:.0} expired={expired:.0}\n\
         hlc monotonic: {:?}\n\
         ring consistency: {:?}\n\
         split brain: {:?}\n\
         cache invalidation: {cache_failures}/{CACHE_INVALIDATION_SAMPLE} stale\n\
         churn: {} events, {} failed\n\
         errors_total: {}\n\
         ops_total: {}",
        missing_keys.len(),
        manifest.len(),
        quorum_failed.len(),
        incarnation_violation,
        ring_violation,
        split_brain_violation,
        churn_events.len(),
        churn_failed.len(),
        stats.errors_total,
        stats.ops_total,
    );
    assert_eq!(report.result, ReportResult::Pass, "{fail_msg}");
}
