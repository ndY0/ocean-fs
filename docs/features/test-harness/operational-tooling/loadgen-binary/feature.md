---
feature: "Load Generator Binary — Standalone Loadgen for Phase 5 Remote Clusters"
epic: "operational-tooling"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: test-harness-extensions/load-scenario-orchestrator
    reason: Need LoadScenario, Worker, Manifest types built first
  - epic: test-harness-extensions/load-report
    reason: Need LoadReport for JSON output
adr: []
perf:
  - "1.1 BytesMut for blob data"
created: 2026-08-05
updated: 2026-08-05
---

# Load Generator Binary — Standalone Loadgen for Phase 5 Remote Clusters

## Summary

Create a standalone binary `crates/loadgen/` (or extend `e2e/`) for Phase 5
remote cluster targeting. The `loadgen` binary connects to existing OceanFS
nodes (does not spawn them) via CLI-specified target hosts, runs a
`LoadScenario`, and produces a `LoadReport` JSON. It reuses the `LoadScenario`,
`Worker`, `Manifest`, and related types from the e2e harness. These types must
be extracted to either a shared library (`e2e-core/`) or kept in `e2e/src/`
with `loadgen` depending on `e2e` as a library. The binary is the foundation for
Phase 5 scale-property testing against 20-50 node cloud clusters.

## Scope

### In Scope

- New workspace crate `crates/loadgen/` (or reuse `e2e` as a library crate)
- CLI interface via `clap`:
  - `--target-hosts host1:9000,host2:9000,...` — comma-separated list of OceanFS HTTP addresses
  - `--phase N` — test phase identifier (for report)
  - `--duration-secs N` — how long to run (default 300)
  - `--concurrency N` — number of workers (default 16)
  - `--output report.json` — path to write LoadReport JSON
  - `--seed SEED` — optional deterministic seed
  - `--operation-mix PUT:50,GET:40,DELETE:10` — weighted operation mix
  - `--blob-tier inline:10,small:30,standard:40,multi:20` — blob size distribution
- Extracts shared types from `e2e/src/load/` into the `e2e` library facade so `loadgen` can depend on `e2e`:
  - `Manifest`, `LoadScenario`, `Worker`, `WorkerStats`, `AggregateStats`, `Orchestrator`, `MetricsSnapshot`, `LoadReport`
  - These are already `pub` in the `e2e` crate (or need to be made `pub`)
  - The `Cluster` and `NodeProcess` types remain in `e2e` (local spawning only)
- `loadgen` provides a `RemoteCluster` adapter:
  - `RemoteCluster` wraps a list of `reqwest::Url`s — does not spawn processes
  - Provides the same HTTP interface as `Cluster` but targets remote hosts
  - No `kill()`, `restart()`, or process management methods
  - `random_node()` picks a random URL from the list for load distribution
- `loadgen` main function:
  - Parse CLI args → build `LoadScenario` → create `RemoteCluster` → create `Orchestrator` → run → write `LoadReport`
- Deterministic seeding support via `--seed`
- Progress output: periodic stats to stderr (every 10s: puts_total, gets_total, avg put latency, etc.)

### Out of Scope

- Local node spawning (use `e2e` crate + `cargo test` for that)
- Churn scheduling or failure injection (those are `e2e/Cluster`-only features)
- Prometheus textfile output (the remote cluster has its own Prometheus; loadgen just produces the JSON report)
- Terraform/Ansible cluster provisioning (handled by `vm-provisioning` feature and Phase 5 deployment automation)
- Kubernetes operator or CRD for loadgen

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | Make `load/` module types `pub` (or re-export via `lib.rs`). Convert `e2e` from a test-only crate to a library crate that also has `[[test]]` targets. |
| `crates/loadgen/` | New workspace crate. `Cargo.toml`: depends on `e2e`, `clap`, `reqwest`, `tokio`, `serde_json`. |

## Interface (Public API)

- `pub struct RemoteCluster` — wraps `Vec<reqwest::Url>`, provides `get()`, `put()`, `delete()`, `head()` async methods
- CLI: `loadgen --target-hosts host1:9000,host2:9000 --phase 2 --duration-secs 300 --output report.json`
- Stdout: path to written JSON report
- Stderr: periodic progress stats

## Data Flow

```
$ loadgen --target-hosts 10.0.0.1:9000,10.0.0.2:9000 --phase 5 --duration-secs 3600 --concurrency 32

  Parse args → RemoteCluster { urls: [10.0.0.1:9000, 10.0.0.2:9000] }
  Build LoadScenario { duration: 3600, concurrency: 32, operations: PUT(50%)+GET(40%)+DELETE(10%), blob: Tiered, key: RandomUuid }
  Create Manifest (Arc)
  Create Orchestrator with RemoteCluster handle
  Run workers for 3600s
  Write LoadReport to report.json
  Print "Phase 5 complete. Report: report.json"
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds; `crates/loadgen/` compiles
- [ ] **Code:** `e2e` crate compiles as both a library and test target (no breakage)
- [ ] **Code:** `RemoteCluster` implements the same HTTP interface as `Cluster` (get/put/delete/head)
- [ ] **Tests:** Unit test: `RemoteCluster` round-robins or random-selects URLs for load distribution
- [ ] **Tests:** Integration test: spawn 1-node local cluster, run `loadgen --target-hosts localhost:{port} --duration-secs 10`, verify report.json produced with non-zero stats
- [ ] **Tests:** CLI parsing: all `--help` output is clear and complete
- [ ] **Tests:** Deterministic seed: same `--seed 42` produces identical operation sequence across two runs
- [ ] **Tests:** Error handling: unreachable target host → clear error message, non-zero exit code
- [ ] **Docs:** Every `pub` item in `loadgen/` has doc comments; `#![deny(missing_docs)]` passes
- [ ] **Integration:** Phase 5 scale test can invoke `loadgen` against a 20-node cloud cluster
