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
  - epic: refactoring/load-test-harness-fidelity
    reason: Corrects measurement fidelity (multi-thread runtime, 16 MiB max_body_size, 4xx tracking, HTTP-only latency) that Phase 2 builds on
adr:
  - 0001-segment-packing
  - 0004-tiered-segment-sizing
  - 0019-test-harness-topology-cost-guardrails
perf:
  - "11.1 Atomic counters on hot paths"
  - "11.2 Resource monitoring instrumentation"
created: 2026-08-05
updated: 2026-08-15
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

<!--
  REVIEW ITERATION 3 (2026-08-15, FINAL): independently verified by reviewer.
  12/17 pass. Both critical fixes verified CORRECT end-to-end:
  (1) inverted seal-aware WAL retention — cleanup builds the SEALED set
  (sealed_at.is_some()) and protects ANY file with entries for segments
  NOT in it; empty sealed set protects everything (conservative). Write
  path order confirmed (pool append -> WAL append coordinator.rs:290-304
  -> put_object handlers.rs:145 -> phantom put_segment sealed_at:None
  handlers.rs:213) — the phantom window is real and now closed.
  crash_recovery = 0/106 mismatches in FOUR consecutive 300s seed-42
  runs (3 implementer + 1 reviewer rerun). Regression test
  cleanup_protects_files_with_unsealed_entries passes and models the
  phantom registration faithfully. (2) per-CF RocksDB gauges —
  property_u64_cf_sum (store.rs:1079) sums num-files-at-level0 /
  live-sst-files-size / estimate-table-readers-mem across objects/
  segments/deletions CFs; reviewer probe-verified that per-CF reads
  return real values (L0=1, SST=53MB after flush_cf) while DB-level
  reads the EMPTY default CF (0 / 2048 constant seen in reports).
  NEW FINDING (iteration 3): fds_stable is INTERMITTENT at 300s —
  reviewer's own 300s run FAILED it (fds 273 > 38+50, single final-poll
  spike; RocksDB max_open_files=-1 opens one fd per SST on a run-end
  compaction burst) while the implementer's 3 runs passed (<=54). The
  single-poll rule has no transient tolerance, so the outcome varies
  run-to-run with the same seed — determinism item therefore FAILS at
  300s. memory_bounded fails 4/4 runs (RSS 2.1-3.25x baseline crest,
  sawtooth bounded, not a leak — spec/product calibration, needs owner
  decision). Remaining: full mode 3600s not verifiable (no SUT VM) and
  operationally blocked (no oceanfs systemd unit in vm-provision.sh).
-->

- [x] **Code:** `cargo build --all-targets` succeeds in `e2e` crate
- [x] **Code:** Test file `e2e/tests/load_sustained.rs` compiles and links
- [x] **Code:** Test supports both remote-target mode (`TARGET_HOST` env var) and local-spawn mode (CI)
- [ ] **Tests:** Quick mode (5 min): `LOAD_TEST_DURATION_SECS=300 cargo test -p e2e -- load_sustained` passes (CI, local spawn)
<!-- REVIEW: still FAILS at 300s/seed 42 — verified independently in 4 runs (3 implementer reports + reviewer's own 300s rerun 20260815T184219.json). 7/9 assertions PASS in every run (level-0, seal_errors, accel_fallback, wal_not_unbounded, cache 79-92%, segment_active, crash_recovery 0/106). memory_bounded fails 4/4; fds_stable failed 1/4 (reviewer run: fds 273 > 38+50 final-poll spike; implementer runs passed <=54). Both failures are product-side (bounded RSS sawtooth; RocksDB max_open_files=-1 compaction-burst fd spike), documented below. Test logic itself verified faithful to spec. -->
- [ ] **Tests:** Full mode (60 min): `TARGET_HOST=10.0.0.5:9000 LOAD_TEST_DURATION_SECS=3600 cargo test -p e2e -- load_sustained` passes on cloud VM (remote target)
<!-- REVIEW: not verifiable in this environment (no SUT VM). Remote path itself verified: e2e/tests/remote_target_mode.rs passes end-to-end against a locally spawned node (16.2s, reviewer rerun), RemoteCluster/load_sustained remote branch reviewed (e2e/src/remote.rs, e2e/tests/load_sustained.rs:561-584). Additionally blocked operationally: scripts/vm-provision.sh has no oceanfs systemd unit/deploy step on the SUT (implementer disclosed), so TARGET_HOST_SSH crash-control has nothing to restart. -->
- [ ] **Tests:** Memory bounded: RSS growth < 2× over run duration
<!-- REVIEW: FAILS at 300s in all 4 verified runs (reviewer rerun 20260815T184219.json: RSS crest 2.0GB = 3.25× baseline on polls 6-11; implementer reports: 1.5-1.8GB = 2.1-2.4×). Verified NOT a leak: RSS sawtooths back down to ~1.0-1.2GB / 0.99× mid-run (report series). Product-side/spec-calibration: the spec's own config (16 MiB blobs × 16-32 workers, 256 MiB L1 + 128 MiB block cache + 64/256/16 MiB write buffers = ~720 MiB caches alone, plus segment/EC buffers) has a steady-state high-water ~2.4-3.3× the post-warmup baseline; the 2× limit is unachievable for this workload. Test logic faithful to spec (3-poll rule, warmup baseline e2e/tests/load_sustained.rs:228-275, 497-515). Requires product/spec calibration decision (owner), not a test bug. -->
- [ ] **Tests:** FDs stable: open fd count growth < 50
<!-- REVIEW: INTERMITTENT FAIL at 300s. Reviewer's independent rerun (20260815T184219.json) failed: fds 273 > 38+50 at the final poll (mid-run polls 38-53); implementer's 3 runs passed (<=54). Product-side: RocksDB default max_open_files=-1 (crates/oceanfs-core/src/config/metadata.rs:96) opens one fd per SST; a run-end compaction burst spikes fds. The assertion is single-poll (e2e/tests/load_sustained.rs:278-285) with no transient tolerance, so the outcome varies run-to-run with the same seed. 60s runs pass. Product fix: bound max_open_files (e.g. 256) or give fds_stable the same consecutive-poll tolerance as memory_bounded. -->
- [x] **Tests:** RocksDB level-0 files < 20 throughout run
<!-- REVIEW: per-CF gauge fix VERIFIED CORRECT (iteration 3). property_u64_cf_sum (crates/oceanfs-storage/src/metadata/store.rs:1079) sums rocksdb.num-files-at-level0 / live-sst-files-size / estimate-table-readers-mem across CF_OBJECTS/CF_SEGMENTS/CF_DELETIONS via property_int_value_cf; reviewer probe confirmed per-CF reads return real values (L0=1, SST=53MB after flush_cf) while DB-level reads the EMPTY default CF (0; memtable 2048 constant). unresolved_rocksdb_properties validates per-CF on the objects CF (store.rs:1106; test passes on a real DB). Caveat: gauges still read 0.0 across all polls at this scale because the 64/256/16 MiB write buffers never fill in 300s (no memtable flush -> genuinely zero L0/SST files) — structurally fixed, but trivially satisfied under the current workload; it becomes a live invariant when write volume fills memtables. rocksdb_estimate_num_keys remains DB-level (pinned at 0; informational only, not asserted). -->
- [x] **Tests:** WAL recovery: all pre-crash objects readable after SIGKILL + restart
<!-- REVIEW: NOW PASSES at 300s — 0/106 mismatches in FOUR consecutive seed-42 runs (implementer reports 20260815T180353/T181044/T182213 + reviewer rerun 20260815T184219.json). The inverted retention rule is verified correct: (a) sealed set = segments with sealed_at.is_some() (durable on disk); (b) ANY file with entries for a segment NOT in it is protected, closing the phantom window (WAL append -> put_object -> phantom put_segment order confirmed: coordinator.rs:290-304, handlers.rs:132-153, 171-218); (c) empty sealed set protects everything (conservative); (d) sealed segments' entries remain sweepable. Regression test cleanup_protects_files_with_unsealed_entries passes (replay.rs:438). NEW LATENT RISK (iteration 3 finding): segments deleted from the CF by GC compaction (segment_compactor.rs:79) or orphan reaper (orphan_reaper.rs:169) leave their WAL entries permanently 'protected' (they are no longer in the sealed set and the cleanup cannot distinguish deleted from never-registered segments) — unbounded WAL-file growth in principle; bounded at 300s (delta <=6) but a hazard for the 3600s full mode. Fix direction: tombstone deleted segment IDs (e.g. CF_DELETIONS marker) and exempt them from protection. -->
- [x] **Tests:** Segment seal errors: 0
- [x] **Tests:** Cache hit rate: L1 object cache > 50% by end of run
- [ ] **Tests:** Deterministic: same seed produces same assertion outcomes
<!-- REVIEW: workload generation deterministic (ChaCha12Rng seed+worker_id; same_seed_produces_identical_sequence passes). Outcome determinism holds at 60s (seeds 42/7/1234 all pass; reviewer rerun seed 42: 10/10). FAILS at 300s: seed 42 produced fds_stable PASS in the implementer's 3 runs but FAIL in the reviewer's rerun (final-poll fd spike) — the crash-recovery outcome IS now deterministic (0/106 x4) but fds_stable varies with compaction-burst timing. Determinism at 300s requires the fds_stable product fix above. -->
- [x] **Tests:** LoadReport JSON written to `/tmp` (tmpfs), not to OceanFS data directory
- [x] **Tests:** Remote target mode: harness successfully connects to oceanfs at TARGET_HOST address (does not spawn locally)
- [x] **Docs:** Test doc comment explains quick vs full mode, remote-target vs local-spawn, environment variables, and expected invariants
- [x] **Perf:** Metric polling does not add >5% latency to worker operations (polling is async and non-blocking)
<!-- REVIEW: polling is a tokio::spawn'd async task with its own interval (e2e/tests/load_sustained.rs:343-380); no blocking ops on the worker path. Note: the PUT hot path gained one RocksDB point read (get_segment) per unique segment via spawn_blocking (metadata_async.rs:184-198) — negligible at observed rates but on the ack path. -->
- [x] **Integration:** LoadReport JSON contains all metric snapshots (time-series) for offline analysis

## Current State vs. Spec (2026-08-15)

> **Status snapshot.** All facts below were verified against the working tree
> on 2026-08-15. Implementation of this feature has NOT begun — the DoD
> checklist above remains unchecked by design. This section records the delta
> so a fresh implementer session can start without archaeology. Build order:
> **1 → 2 → 3 → 4 → 6** (strictly serial through the critical path, gap 3);
> **5** in parallel with gaps 3–4; **7** after 5 and 6.

### What already exists (do not rebuild)

- **Epic 1 `test-harness-extensions`: all 6 features `done`** (manifest-tracker,
  load-scenario-orchestrator, metrics-scraper, load-report, churn-scheduler,
  failure-injectors). Implemented in `e2e/src/load/`:
  - `generator.rs` — `LoadScenario`, `Worker`, `Orchestrator`, `WorkerStats`
    with `put`/`get`/`delete`/`head` `LatencyHistogram` p50/p99,
    `BlobSizeDist::{Fixed, Range, Tiered}`, `KeySpace::{RandomUuid, Sequential, Zipfian}`
  - `manifest.rs` — `Manifest`, `ManifestSummary`, `verify_summary`, `record_delete`
  - `metrics.rs` — `MetricsSnapshot::scrape`, `gauge`/`counter`/`delta`,
    `parse_prometheus_text`
  - `report.rs` — `LoadReport`, `assert_that`, `write_json_atomic`,
    `write_textfile_atomic`
  - `churn.rs`, `degrade.rs`
- **`phase1-concurrency-test`: `done`** — `e2e/tests/load_concurrency.rs` passing
  (seeds 42/7/1234/987654, 30s + seed-42 120s — all PASS on 2026-08-15). Uses
  50/40/5/5 PUT/GET/DELETE/HEAD, `BlobSizeDist::Tiered` 10/30/40/20, and writes
  reports to `target/load-reports` (**NOT** `/tmp` — a Phase 2 deviation from
  the spec, see gap 4).
- **`operational-tooling`:** `vm-provisioning` `done`
  (`scripts/vm-provision.sh`, Hetzner hcloud, CX22/CX32 sizing, TTL guardrails
  per ADR-0019); `prometheus-grafana-setup` `done`
  (`scripts/setup-observability.sh`); `loadgen-binary` `proposed` (Phase 5
  only, not needed for Phase 2).
- **Harness crash primitives:** `Cluster::kill` (SIGKILL),
  `NodeProcess::restart`, `spawn_with_data_dir` all exist in `e2e/src/harness.rs`.
- **Config helpers:** `config_standard()` (16 MiB `max_body_size`),
  `config_short_gc()` (gc 10s / ttl 5s), `config_short_ae()` (ae 10s) exist in
  `e2e/src/harness.rs`.
- **RocksDB metrics:** `RocksDbMetrics` in
  `crates/oceanfs-storage/src/metadata/store.rs` exposes `block_cache_hit/miss`,
  `memtable_size`, `running_compactions/flushes`, `estimate_num_keys` — polled
  via `metadata_store.start_metrics_task()`; `node.rs` registers
  `process_resident_memory_bytes` + `process_open_fds` gauges (15s poller) and
  the `/admin/metrics` Prometheus endpoint exists.

### Implementation gaps (in build order)

1. **`config_short_scrub` helper missing** in `e2e/src/harness.rs`
   (`scrub_interval_sec=60` alongside `config_short_gc`/`config_short_ae`).
   ~15 min.
2. **RocksDB level-0/SST gauges missing:** `rocksdb_num_files_at_level_0`,
   `live-sst-files-size`, `estimate-table-readers-mem` NOT in
   `RocksDbMetrics`/store poller. Needed for the spec's headline invariant
   (level-0 < 20) AND serves ADR-0023 Phase 0 (metadata memory attribution;
   ADR file: `docs/adr/0023-metadata-store-native-replacement-path.md`).
   ~30 min.
3. **Remote-target mode (`TARGET_HOST`) does NOT exist anywhere in the e2e
   crate** — the critical missing piece. No `RemoteNode`/remote variant of
   `NodeProcess`/`Cluster`; `MetricsSnapshot::scrape` and `Manifest::verify`
   take spawned-process handles only. Requires: remote HTTP client for S3 +
   `/admin/metrics` + `/admin/health`, `scrape_remote` + `verify_remote`
   variants, `LoadReport` self-monitoring (harness's own `/proc` fds/RSS per
   spec items 93–94). **Blocks Steps 4, 6, 7.** 2–3 h — the design-heavy piece.
4. **`e2e/tests/load_sustained.rs` does not exist.** The Phase 2 test itself:
   topology detection (`TARGET_HOST` vs local spawn), short-interval config,
   10s metric polling with the 6 per-snapshot invariants (RSS <2×, fds <+50,
   level-0 <20, `seal_errors=0`, `accel_fallback=0`, WAL files <+10),
   40/50/10 PUT/GET/DELETE on Zipfian 10K keys (delete-rewrite exercises
   compaction), SIGKILL → `spawn_with_data_dir` restart → `manifest.verify` →
   `LoadReport` to `/tmp` (spec requires `/tmp` per ADR-0019 Decision 4;
   current `load_concurrency` writes `target/load-reports` — must differ).
   3–4 h.

### Agent skills (Epic 4) — all proposed, nothing under `.opencode/skills/` yet

- **`vm-skills`** (`vm-status`/`vm-up`/`vm-down`/`vm-deploy`): depend only on
  `vm-provision.sh` (done) + ADR-0019 — can proceed in parallel with gaps 3–4.
  NOTE: Hetzner-specific (hcloud CLI, `~/.ssh/config` aliases
  `oceanfs-sut`/`oceanfs-harness`); if a non-Hetzner VM is used, skills need
  adaptation.
- **`test-execution-skills`** (`vm-test-phase`/`vm-results`/`vm-metrics`/`vm-logs`):
  `vm-test-phase` hard-depends on `load_sustained` (gap 4); the other three
  depend on done items (load-report, prometheus setup).
- **`agent-integration-test`** (`scripts/test-agent-workflow.sh`): depends on
  both skill groups + phase1 (done); its workflow is Phase-1-based so it can
  be validated before gap 4 lands.

### Build order

1. `config_short_scrub` helper (gap 1)
2. RocksDB level-0/SST gauges (gap 2)
3. Remote-target mode — `RemoteNode`, `scrape_remote`, `verify_remote` (gap 3;
   **critical path** — everything else blocks on this)
4. `load_sustained.rs` test itself (gap 4)
5. `vm-skills` (parallel with gaps 3–4)
6. `test-execution-skills` + `agent-integration-test` (after 5)
7. Full Phase 2 campaign on a dedicated VM (intent: sustained load on a
   dedicated VM; a fresh implementer session picks up at Step 1)

### Dependencies status

All feature-level dependencies are satisfied: `config-system-fix` done,
`metrics-infrastructure` done, `write-path-unification` done,
`correctness-gaps` done, all 6 Epic-1 features done, `load-test-harness-fidelity`
done. The only outstanding dependencies are internal to this feature's gaps
above.
