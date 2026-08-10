---
feature: "Phase 2 — Single-Node Sustained Load & Resource Stability Test"
epic: "test-phase-implementations"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: gap-closure/config-system-fix
    reason: Need shortened GC/AE/scrub intervals configurable; need max_body_size
  - epic: gap-closure/metrics-infrastructure
    reason: Need process/RocksDB/WAL/segment metrics for all resource stability assertions
  - epic: gap-closure/write-path-unification
    reason: Need segment metadata in segments CF for GC/scrub/segment assertions
  - epic: gap-closure/correctness-gaps
    reason: Need WAL crash recovery for post-crash read verification
  - epic: test-harness-extensions/manifest-tracker
    reason: Need Manifest for data integrity verification
  - epic: test-harness-extensions/load-scenario-orchestrator
    reason: Need Worker framework for sustained load
  - epic: test-harness-extensions/metrics-scraper
    reason: Need MetricsSnapshot for periodic metric polling
  - epic: test-harness-extensions/load-report
    reason: Need LoadReport for structured results output
  - epic: test-harness-extensions/failure-injectors
    reason: Need Cluster::kill() for post-crash recovery test
adr:
  - 0001-segment-packing
  - 0004-tiered-segment-sizing
  - 0019-test-harness-topology-cost-guardrails
perf:
  - "11.1 Atomic counters on hot paths"
  - "11.2 Resource monitoring instrumentation"
created: 2026-08-05
updated: 2026-08-10
---

# Phase 2 — Single-Node Sustained Load & Resource Stability Test

## Summary

Implement `e2e/tests/load_sustained.rs` — a `#[tokio::test]` that validates
single-node resource stability under sustained load. In two-VM topology (Phase 2
cloud runs), the harness runs on the Harness VM and connects to a single
already-running OceanFS process on the SUT VM via `TARGET_HOST=<sut-ip>:9000`
env var (per ADR-0019). In local-spawn mode (Phase 2 quick mode in CI), the
harness spawns one `NodeProcess` directly. The test runs with shortened
background intervals (GC=10s, AE=10s, scrub=60s), a sustained PUT+GET+DELETE
loop with randomized blob sizes for 5 minutes (quick mode, CI) or 60 minutes
(full mode, cloud) controlled by `LOAD_TEST_DURATION_SECS`. Polls
`/admin/metrics` every 10 seconds and asserts resource invariants on each
snapshot. At the end, kills the node (SIGKILL), restarts with the same data
directory, and verifies all pre-crash objects are readable (WAL recovery).
Produces a `LoadReport` with metric time-series data. The report is written to
`/tmp` (tmpfs) on the Harness VM so that disk-fill scenarios in Phase 4 cannot
prevent report output.

## Scope

### In Scope

- `#[tokio::test]` function in `e2e/tests/load_sustained.rs`
- Two topology modes (per ADR-0019):
  - **Remote target** (cloud, two-VM): connects to running OceanFS at `TARGET_HOST=<ip>:9000`; does not spawn processes
  - **Local spawn** (CI quick mode): spawns `NodeProcess` directly via existing `Cluster` API
- Two duration modes: "quick" (5 min, CI) and "full" (60 min, cloud) controlled by `LOAD_TEST_DURATION_SECS` env var
- Spawns/connects to 1 oceanfs node with config: shortened GC (10s cycle, 5s TTL), AE (10s), scrub (60s), gossip disabled (single-node)
- `LoadScenario` configuration:
  - Concurrency: `num_cpus::get() * 2` workers (moderate load to avoid overwhelming single node)
  - Duration: from env var (default 300s quick / 3600s full)
  - Operations: PUT 40%, GET 50%, DELETE 10% (write-delete-rewrite cycles to exercise compaction)
  - Blob sizes: `Tiered` — 15% inline, 35% small, 35% standard, 15% multi
  - Key space: large (10K keys) to exercise compaction on overlapping key ranges
- Periodic metric polling every 10 seconds: `MetricsSnapshot::scrape(&node)` → store in Vec
- Per-snapshot assertions (check on each poll, record earliest violation):
  1. **memory_bounded**: RSS does not grow >2× from initial over full run; sawtooth pattern acceptable
  2. **fds_stable**: `/proc/{pid}/fd` count does not grow >50 from initial
  3. **rocksdb_no_write_stall**: `rocksdb_num_files_at_level_0` stays < 20
  4. **segment_seal_no_errors**: `segment_seal_errors_total` == 0
  5. **accel_fallback_zero**: `accel_fallback_total` == 0
  6. **wal_not_unbounded**: WAL file count does not grow >10 from initial (segments are being sealed)
- Post-run assertions (end of sustained load, before crash):
  7. **cache_reasonable**: cache hit rates > 50% (L1 object cache)
  8. **segment_active_count**: > 0 (segment pipeline is producing segments)
- Post-crash recovery test:
  9. `cluster.kill(0)` (SIGKILL)
  10. Restart node with same `data_dir`: `NodeProcess::spawn_with_data_dir(config, &data_dir)`
  11. WAL recovery verification: `manifest.verify(&restarted_cluster)` → 0 mismatches (all pre-crash data readable)
  12. Assert `/admin/health` returns 200 after restart
- LoadReport populated with all metric snapshots (time-series), all assertions, manifest summary
- **Report path:** Always writes `LoadReport` JSON to `/tmp` (tmpfs) on the machine where the harness runs (Harness VM in two-VM mode, CI runner in local mode), per ADR-0019 Decision 4
- **Single-VM mode:** If `--single-vm` flag is active (harness co-located with SUT), the harness monitors its own `/proc` metrics separately and includes them in the report as metadata. Relaxed gossip parameters are NOT needed for Phase 2 (single-node, gossip disabled).
- **Harness self-monitoring:** In all modes, the harness records its own `process_open_fds` and `process_resident_memory_bytes` from `/proc` and includes them in the `LoadReport` as metadata (not assertions), per ADR-0019 Decision 4

### Out of Scope

- Cluster-level assertions (that's Phase 3)
- Churn injection (that's Phase 3)
- Failure injection beyond SIGKILL (that's Phase 4)
- Prometheus/Grafana integration (the test produces the textfile; the observability stack is configured separately)

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | New test file `tests/load_sustained.rs`. Uses all harness modules from Epic 1. |

## Interface (Public API)

No new `pub` items — this is a `#[tokio::test]` function.

## Data Flow

```
Test: load_sustained

Topology detection:
  → If TARGET_HOST is set (cloud two-VM mode):
      connect to remote OceanFS at $TARGET_HOST (e.g., 10.0.0.5:9000)
      metrics scraping targets remote /admin/metrics
  → Else (CI local spawn mode):
      spawn NodeProcess locally

Test flow (both modes):
  1. Parse LOAD_TEST_SEED, LOAD_TEST_DURATION_SECS, TARGET_HOST
  2. Build LoadScenario (duration from env var)
  3. Build config with shortened intervals: gc_interval=10, tombstone_ttl=5, ae_interval=10, scrub_interval=60
  4. Connect to or spawn NodeProcess; obtain data_dir
  5. Initial metrics: scrape /admin/metrics → initial_snapshot
  6. Spawn metric polling task (tokio::spawn):
        every 10s:
          snapshot = MetricsSnapshot::scrape(&node)
          check per-snapshot assertions, record violations
          store snapshot in Vec
  7. Spawn orchestrator with workers
  8. Wait for duration; join workers and metric poller
  9. Final metrics: scrape one last time
  10. Post-run assertions (cache hit rate, segment count)
  11. Kill node: cluster.kill(0)
  12. Restart: NodeProcess::spawn_with_data_dir(config, data_dir)
  13. Wait for /admin/health
  14. manifest.verify(&restarted_cluster) → 0 mismatches
  15. Build LoadReport; write JSON to /tmp (tmpfs) + textfile
  16. assert!(report.result == Pass)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `e2e` crate
- [ ] **Code:** Test file `e2e/tests/load_sustained.rs` compiles and links
- [ ] **Code:** Test supports both remote-target mode (`TARGET_HOST` env var) and local-spawn mode (CI)
- [ ] **Tests:** Quick mode (5 min): `LOAD_TEST_DURATION_SECS=300 cargo test -p e2e -- load_sustained` passes (CI, local spawn)
- [ ] **Tests:** Full mode (60 min): `TARGET_HOST=10.0.0.5:9000 LOAD_TEST_DURATION_SECS=3600 cargo test -p e2e -- load_sustained` passes on cloud VM (remote target)
- [ ] **Tests:** Memory bounded: RSS growth < 2× over run duration
- [ ] **Tests:** FDs stable: open fd count growth < 50
- [ ] **Tests:** RocksDB level-0 files < 20 throughout run
- [ ] **Tests:** WAL recovery: all pre-crash objects readable after SIGKILL + restart
- [ ] **Tests:** Segment seal errors: 0
- [ ] **Tests:** Cache hit rate: L1 object cache > 50% by end of run
- [ ] **Tests:** Deterministic: same seed produces same assertion outcomes
- [ ] **Tests:** LoadReport JSON written to `/tmp` (tmpfs), not to OceanFS data directory
- [ ] **Tests:** Remote target mode: harness successfully connects to oceanfs at TARGET_HOST address (does not spawn locally)
- [ ] **Docs:** Test doc comment explains quick vs full mode, remote-target vs local-spawn, environment variables, and expected invariants
- [ ] **Perf:** Metric polling does not add >5% latency to worker operations (polling is async and non-blocking)
- [ ] **Integration:** LoadReport JSON contains all metric snapshots (time-series) for offline analysis
