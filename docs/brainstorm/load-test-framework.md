# Load Test Framework — Operational Design

**Author:** Brainstorm Agent (Architect)
**Date:** 2026-08-05
**Context:** Design of the operational scaffolding for the phased load test
campaign (see `load-test-campaign.md`). Covers the VM deployment model, the
observability stack, the harness architecture, the skills agents use to drive
tests, and the results format consumed by both humans and agents.
**References:** `load-test-campaign.md`, `ROADMAP.md` item #17.

---

## 1. Operational Model

### 1.1 Topology

```
┌─ Developer Laptop ───────────────────────┐
│                                           │
│  Grafana :3000                            │
│    datasource → localhost:9090 (tunneled) │
│                                           │
│  Agents (Architect, Reviewer,             │
│          Implementer)                     │
│    │  query results via:                  │
│    │    - Grafana HTTP API (:3000)        │
│    │    - Prometheus HTTP API (:9090)     │
│    │    - JSON report files (rsynced)     │
│                                           │
│  ssh -L 9090:localhost:9090 vm            │
│  ssh vm <command>                         │
└───────────────┬───────────────────────────┘
                │ SSH (tunnel + commands)
                ▼
┌─ Cloud VM (e.g., 4 vCPU / 16 GB) ────────┐
│                                           │
│  prometheus :9090                         │
│    scrapes: :9000/admin/metrics           │
│    textfile: /var/lib/prometheus/textfile/│
│                                           │
│  oceanfs :9000                            │
│    SUT — system under test                │
│    /admin/metrics (Prometheus format)     │
│                                           │
│  e2e harness                              │
│    spawns oceanfs, runs load scenarios,   │
│    writes textfile metrics + JSON report  │
│                                           │
│  journald                                 │
│    oceanfs stdout/stderr                  │
│    harness stdout/stderr                  │
└───────────────────────────────────────────┘
```

### 1.2 Why This Model

- **Cheap.** A single 4 vCPU / 16 GB VM on Hetzner (~$25/month) runs Phases 1-4.
  Only Phase 5 requires a larger fleet.
- **Observability is colocated but decoupled.** Prometheus runs on the VM so
  scrape latency is negligible, but it's a separate process so the harness can
  crash/restart without losing metric history.
- **Grafana stays on the laptop.** The VM has no display; the laptop has the
  screen. SSH tunnel gives Grafana access to Prometheus without exposing ports
  to the internet.
- **Agents query the same APIs regardless.** Local agents talk to `localhost:9090`
  or `localhost:3000`. CI agents read the JSON report artifact. Same data, two
  access paths.

### 1.3 VM Sizing

| Phase | oceanfs Nodes | Harness Concurrency | Required RAM | vCPU | Disk |
|---|---|---|---|---|---|
| 1-2 | 1 | 16-32 tokio tasks | 4-6 GB | 4 | 50 GB SSD |
| 3 | 3-5 | 4-8 per node | 8-12 GB | 8 | 100 GB SSD |
| 4 | 3 | 8 per node | 8-12 GB | 8 | 100 GB SSD |
| 5 | 20-50 | TBD | Per-node, not single VM | Per-node | Per-node |

Phase 5 shifts to a per-node deployment model (each oceanfs node on its own
VM/container) with a dedicated observability VM. That migration is described in
`load-test-campaign.md` §6.

---

## 2. Harness Design

### 2.1 Crate Layout

The load test harness extends the existing `e2e/` crate. No new crate is needed
for Phases 1-4.

```
e2e/
├── Cargo.toml
├── src/
│   ├── harness.rs        # NodeProcess, Cluster (exists)
│   ├── load/
│   │   ├── mod.rs
│   │   ├── manifest.rs   # Manifest — PUT tracker + post-run verifier
│   │   ├── metrics.rs    # MetricsSnapshot, Prometheus text parser
│   │   ├── generator.rs  # LoadScenario, Worker — concurrent task spawner
│   │   ├── churn.rs      # ChurnScheduler — random node kill/restart
│   │   ├── degrade.rs    # Failure injectors — tc, disk, corruption
│   │   └── report.rs     # LoadReport — JSON output, seed logging
│   └── lib.rs
└── tests/
    ├── load_concurrency.rs    # Phase 1
    ├── load_sustained.rs      # Phase 2
    ├── load_cluster_churn.rs  # Phase 3
    └── load_degraded.rs       # Phase 4
```

### 2.2 Key Types

#### Manifest

```rust
/// Tracks every object written during a load test and verifies them afterward.
pub struct Manifest {
    entries: DashMap<String, [u8; 32]>, // "{bucket}/{key}" → BLAKE3 hash
}

impl Manifest {
    /// Record a successful PUT. Called by worker tasks.
    pub fn record(&self, bucket: &str, key: &str, body: &[u8]);

    /// GET every recorded key from a random reachable node, hash the response,
    /// and compare. Returns list of mismatched (key, expected_hash, actual_hash).
    pub async fn verify(&self, cluster: &Cluster) -> Vec<Mismatch>;

    /// Number of objects tracked.
    pub fn len(&self) -> usize;
}
```

#### LoadScenario

```rust
/// Describes a load test: how many workers, what operations, for how long.
pub struct LoadScenario {
    pub concurrency: usize,         // number of concurrent tokio tasks
    pub duration: Duration,         // how long to run (None = run N ops)
    pub operations: Vec<OpWeight>,  // weighted operation mix
    pub blob_sizes: BlobSizeDist,   // distribution of blob sizes
    pub key_space: KeySpace,        // key generation strategy
    pub seed: u64,
}

pub struct OpWeight {
    pub op: Operation,  // Put, Get, Delete, Head, List
    pub weight: f64,    // probability, e.g., Put=0.5, Get=0.4, Delete=0.05, Head=0.05
}

pub enum Operation { Put, Get, Delete, Head, List }
```

#### Worker

```rust
/// A single concurrent load-generator task.
pub struct Worker {
    id: usize,
    cluster: ClusterHandle,   // shared, Clone + Send
    manifest: Arc<Manifest>,
    scenario: Arc<LoadScenario>,
    rng: ChaCha12Rng,        // seeded, deterministic
    stats: WorkerStats,       // AtomicU64 counters
}

impl Worker {
    /// Run the worker for the scenario duration.
    pub async fn run(mut self) -> WorkerStats;

    /// Perform one operation (weighted random choice)
    async fn tick(&mut self);
}
```

#### MetricsSnapshot

```rust
/// A point-in-time snapshot of Prometheus metrics from /admin/metrics.
pub struct MetricsSnapshot {
    pub timestamp: Instant,
    pub metrics: HashMap<String, f64>,
}

impl MetricsSnapshot {
    /// Scrape /admin/metrics from a node, parse Prometheus text format.
    pub async fn scrape(node: &NodeProcess) -> Result<Self>;

    /// Difference between two snapshots (for counters).
    pub fn delta(&self, prev: &Self) -> HashMap<String, f64>;
}
```

#### LoadReport

```rust
/// Final structured report produced by every load test.
#[derive(Serialize)]
pub struct LoadReport {
    pub phase: u8,
    pub test: String,
    pub seed: u64,
    pub duration_secs: u64,
    pub result: ReportResult,
    pub worker_stats: AggregateStats,
    pub manifest: ManifestSummary,
    pub metric_snapshots: Vec<TimestampedMetrics>,
    pub assertions: Vec<AssertionResult>,
    pub failures: Vec<FailureDetail>,
}

#[derive(Serialize)]
pub enum ReportResult { Pass, Fail, Timeout }

#[derive(Serialize)]
pub struct ManifestSummary {
    pub objects_written: u64,
    pub objects_verified: u64,
    pub mismatches: u64,
    pub mismatch_details: Vec<Mismatch>,
}

#[derive(Serialize)]
pub struct AssertionResult {
    pub name: String,
    pub passed: bool,
    pub expected: String,
    pub actual: String,
}
```

### 2.3 Harness-to-Prometheus Integration

The harness pushes events and metrics to Prometheus via the **textfile collector**:

```rust
/// Atomically write a Prometheus textfile that Prometheus will scrape.
pub fn write_textfile(path: &Path, metrics: &HashMap<String, f64>) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = File::create(&tmp)?;
    for (name, value) in metrics {
        writeln!(f, "{name} {value}")?;
    }
    f.sync_all()?;
    std::fs::rename(&tmp, path)?; // atomic on Unix
    Ok(())
}
```

Prometheus config snippet:

```yaml
scrape_configs:
  - job_name: oceanfs
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:9000']
    metrics_path: /admin/metrics

  - job_name: load_test
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost']
    file_sd_configs:
      - files:
        - /var/lib/prometheus/textfile/*.prom
```

The harness writes these metrics after each assertion check:

```
# /var/lib/prometheus/textfile/load_test.prom
load_test_phase{test="load_sustained"} 2
load_test_objects_written_total 5241
load_test_mismatches_total 0
load_test_result{result="pass"} 1
process_rss_bytes_at_end 87000000
process_open_fds_at_end 38
```

This feeds into Grafana dashboards natively — no custom data source needed.

---

## 3. Observability Stack

### 3.1 Components

| Component | Location | Purpose | Config Complexity |
|---|---|---|---|
| **Prometheus** | VM | Scrapes `/admin/metrics`, stores locally, serves PromQL API on `:9090` | 10-line `prometheus.yml` |
| **Grafana** | Laptop | Dashboards, Explore, alerting, HTTP API on `:3000`. Datasource: `localhost:9090` (tunneled) | 1 datasource + imported dashboards |
| **journald** | VM | oceanfs + harness stdout/stderr. `journalctl -u oceanfs -f` | Zero config (systemd default) |
| **SSH tunnel** | Laptop | `ssh -L 9090:localhost:9090 -N vm` | 1 alias in `~/.ssh/config` |

### 3.2 What's NOT on the VM (and Why)

| Tool | Why Not Included |
|---|---|
| **Mimir** | Prometheus can handle one scrape target for months without issues. Add Mimir at Phase 5 when you need multi-tenant remote-write from 50 nodes. |
| **Loki** | `journald` + `journalctl` is fine for 1-5 nodes. Add Loki at Phase 5 when you need to correlate logs across 50 nodes. |
| **Grafana Agent** | Prometheus scrapes directly. The agent adds a hop with no benefit at this scale. |
| **Pushgateway** | The textfile collector achieves the same (push metrics from batch job) with zero extra processes. |

### 3.3 Upgrade Path to Phase 5

When Phase 5 requires a 50-node cluster:

1. Deploy a separate observability VM (4 vCPU / 16 GB).
2. Replace Prometheus with **Mimir** (monolithic mode). Point all oceanfs nodes
   at it via `remote_write`.
3. Add **Grafana Agent** on each oceanfs VM to collect logs and ship to **Loki**
   running on the observability VM.
4. Point Grafana at Mimir instead of Prometheus. **Zero dashboard changes.**
   **Zero PromQL changes.** Same Grafana HTTP API for agents.

---

## 4. Skills — Agent Operational Interface

Agents (Architect, Reviewer, Implementer) interact with the load test VM through
skills — composable, well-defined operations that hide SSH details.

### 4.1 Skill Catalog

| Skill | Signature | Purpose |
|---|---|---|
| `vm-status` | `→ {running\|stopped, ip, cost_to_date}` | Check if the VM exists and is reachable |
| `vm-up` | `(phase: u8) → {ip, ssh_key_path}` | Provision the VM (if not running) at the size needed for the given phase. Return connection details. |
| `vm-down` | `→ ()` | Tear down the VM to save costs. Preserves Prometheus data directory on persistent volume. |
| `vm-deploy` | `(branch?: str) → {commit, build_duration}` | `rsync` workspace to VM, `cargo build --release -p oceanfs -p e2e`. Returns what was deployed. |
| `vm-test-phase` | `(phase: u8, opts?: TestOpts) → {report_path, grafana_url}` | Run Phase N tests. Returns path to JSON report and a Grafana dashboard URL for live viewing. |
| `vm-results` | `(phase: u8) → LoadReport` | Fetch the latest JSON report for a given phase. Parse and return structured summary. |
| `vm-metrics` | `(query: str) → {result}` | Execute an arbitrary PromQL query against the VM's Prometheus and return results. |
| `vm-logs` | `(phase: u8, since?: str) → [LogLine]` | Fetch journald logs for the most recent Phase N run. |

### 4.2 Skill Implementation Notes

Skills wrap SSH commands. Example pseudocode for `vm-test-phase`:

```bash
# vm-test-phase 2
ssh vm "cd ~/ocean-fs && LOAD_TEST_SEED=$(date +%s) cargo test -p e2e -- load_sustained --nocapture"
# Returns exit code. If 0, rsync target/load-reports/ back to laptop.
```

Skills are defined in `.opencode/skills/` as OpenCode skill files, making them
available to all agents in the pipeline.

### 4.3 Agent Workflow Example

```
User: "Run Phase 2 against the latest main and tell me if we leak memory"

Agent (Architect):
  1. vm-status          → VM is stopped
  2. vm-up phase=2      → VM provisioned, IP: 10.0.0.5
  3. vm-deploy          → built commit abc1234
  4. vm-test-phase 2    → test running, Grafana: http://localhost:3000/d/load-test
  5. [wait for completion]
  6. vm-results 2       → LoadReport { result: Pass, memory_bounded: true, ... }
  7. → "Phase 2 passed. No memory leak detected. RSS stabilized at 87MB after 600s."
```

---

## 5. Results Format

### 5.1 JSON Report Schema

Every load test writes a single JSON file to `target/load-reports/{phase}_{test}_{timestamp}.json`.

Full schema (see `LoadReport` in §2.2):

```json
{
  "phase": 2,
  "test": "load_sustained",
  "seed": 1723456789,
  "duration_secs": 1800,
  "result": "pass",
  "worker_stats": {
    "puts_total": 8234,
    "puts_200": 8234,
    "puts_5xx": 0,
    "gets_total": 4100,
    "gets_200": 4098,
    "gets_404": 2,
    "deletes_total": 512,
    "deletes_204": 512,
    "avg_put_latency_ms": 12.3,
    "p99_put_latency_ms": 45.7,
    "avg_get_latency_ms": 4.1,
    "p99_get_latency_ms": 18.2
  },
  "manifest": {
    "objects_written": 8234,
    "objects_verified": 8234,
    "mismatches": 0,
    "mismatch_details": []
  },
  "metric_snapshots": [
    {
      "elapsed_secs": 0,
      "process_resident_memory_bytes": 45000000,
      "process_open_fds": 34,
      "rocksdb_num_files_at_level_0": 0
    },
    {
      "elapsed_secs": 900,
      "process_resident_memory_bytes": 87000000,
      "process_open_fds": 38,
      "rocksdb_num_files_at_level_0": 2
    },
    {
      "elapsed_secs": 1800,
      "process_resident_memory_bytes": 86000000,
      "process_open_fds": 37,
      "rocksdb_num_files_at_level_0": 1
    }
  ],
  "assertions": [
    {
      "name": "memory_bounded",
      "passed": true,
      "expected": "RSS drift < 5 MB/min over last 10 min",
      "actual": "RSS stabilized at ~85MB, drift -0.1 MB/min"
    },
    {
      "name": "fds_stable",
      "passed": true,
      "expected": "FD count increase < 10 over run",
      "actual": "FD count 34 → 37 (+3)"
    },
    {
      "name": "rocksdb_no_write_stall",
      "passed": true,
      "expected": "rocksdb_num_files_at_level_0 < 20",
      "actual": "max level-0 files: 2"
    }
  ],
  "failures": []
}
```

### 5.2 Agent Consumption

An agent reads the report and makes decisions:

```python
# Pseudocode: agent checking phase 2 results
report = json.load("target/load-reports/load_sustained_2026-08-05T100000.json")

if report["result"] == "fail":
    for f in report["failures"]:
        print(f"FAIL: {f['assertion']} — {f['detail']}")
    sys.exit(1)

# Drill into metrics if borderline
snapshots = report["metric_snapshots"]
initial_rss = snapshots[0]["process_resident_memory_bytes"]
final_rss = snapshots[-1]["process_resident_memory_bytes"]
growth_pct = (final_rss - initial_rss) / initial_rss * 100
if growth_pct > 200:
    print(f"WARNING: RSS grew {growth_pct:.1f}% — investigate even though test passed")
```

### 5.3 Historical Storage

JSON reports are committed to a `load-test-results/` branch or stored in a
dedicated git repository. This gives you:

- **Diffable history:** `git diff` between two runs shows exactly what changed.
- **CI artifact:** GitHub Actions uploads the report as a workflow artifact.
- **Grafana annotation:** The harness can push a Grafana annotation at
  `phase_start` / `phase_end` so dashboards show test boundaries.

---

## 6. Implementation Prerequisites

Before the harness can be built, these items from `load-test-campaign.md` §9 must
be resolved:

| # | Blocker | Impact |
|---|---|---|
| D1 | Write path doesn't create segment metadata entries | `/admin/segments` returns incomplete data; assertions on segment counts fail |
| D2-D4 | GC, anti-entropy, scrub intervals not configurable | Cannot run background processes at test speed; 1-hour GC cycles are untestable |
| D6 | WAL crash recovery not working | Phase 2 post-crash verification fails |
| D8 | 2MB HTTP body size limit | Cannot test multi-segment blobs (>4MB) |

Additionally, these observability prerequisites:

| # | Requirement | Purpose |
|---|---|---|
| M1 | `/admin/metrics` exposes all metrics listed in `load-test-metrics.md` | Without metrics, there's nothing to assert |
| M2 | `oceanfs` logs structured JSON to stdout/stderr | Enables `journalctl` filtering and future Loki ingestion |
| M3 | Prometheus textfile collector directory exists and is writable by the harness | Harness → Prometheus push path |

---

## 7. Open Questions

1. **VM provisioning automation:** Should the `vm-up` skill use Terraform,
   `hcloud` CLI (Hetzner), `aws` CLI, or a simple cloud-init script? The choice
   depends on which cloud provider you use.

2. **Prometheus retention:** For Phases 1-4, 7 days of retention is plenty
   (enough to compare yesterday's run to today's). Config: `--storage.tsdb.retention.time=7d`.

3. **Grafana dashboard provisioning:** Should dashboards be JSON files committed
   to the repo (Grafana provisioned dashboards) or created manually and exported?
   Committed is better — they evolve with the test suite.

4. **Phase 3 churn model vs. harness design:** Does `ChurnScheduler` need to be
   deterministic (fixed sequence of kill/restart events) or random-with-seed?
   Deterministic is better for CI reproducibility; random-with-seed is better for
   finding unexpected interaction bugs. Can we have both modes?

5. **Should the harness be a separate binary?** Currently the harness lives in
   `e2e/` (a `cargo test` target). For Phase 5, it may make sense to extract the
   load generator into a standalone binary (`loadgen/`) that doesn't depend on
   the `cargo test` runner. This can be deferred until Phase 5.
