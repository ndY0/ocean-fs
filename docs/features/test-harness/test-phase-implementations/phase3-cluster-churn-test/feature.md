---
feature: "Phase 3 — 3-5 Node Cluster Churn Under Load Test"
epic: "test-phase-implementations"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: gap-closure/config-system-fix
    reason: Need configurable gossip/SWIM intervals, vnodes_per_node
  - epic: gap-closure/metrics-infrastructure
    reason: Need gossip/heal/hinted-handoff/ring metrics for cluster assertions
  - epic: gap-closure/write-path-unification
    reason: Need segment metadata (segments CF populated) for segment count assertions
  - epic: gap-closure/correctness-gaps
    reason: Need hinted handoff delivery, read repair, multi-replica HLC, port preservation
  - epic: test-harness-extensions/manifest-tracker
    reason: Need Manifest for cross-node data integrity verification
  - epic: test-harness-extensions/load-scenario-orchestrator
    reason: Need Worker framework for multi-node load generation
  - epic: test-harness-extensions/metrics-scraper
    reason: Need MetricsSnapshot for gossip/heal metric assertions
  - epic: test-harness-extensions/load-report
    reason: Need LoadReport for structured results output
  - epic: test-harness-extensions/churn-scheduler
    reason: Need ChurnScheduler for periodic node kill/restart
  - epic: refactoring/load-test-harness-fidelity
    reason: Harness fidelity fixes (multi-thread runtime, 4xx tracking) inherited from Phase 1
  - epic: gap-closure/hlc-causality-closure
    reason: HLC wall clock + receive-merge + cross-node propagation required for "timestamps never move backward" and single-winner assertions
adr:
  - 0001-segment-packing
  - 0019-test-harness-topology-cost-guardrails
perf:
  - "11.1 Atomic counters on hot paths"
created: 2026-08-05
updated: 2026-08-10
---

# Phase 3 — 3-5 Node Cluster Churn Under Load Test

## Summary

Implement `e2e/tests/load_cluster_churn.rs` — a `#[tokio::test]` that validates
distributed protocol correctness under sustained load with node churn. In the
two-VM topology (per ADR-0019), the harness runs on the dedicated Harness VM
(CX22) and connects to 3-5 already-running OceanFS processes on the SUT VM
(CX32) via `TARGET_HOSTS=<sut-ip>:9000,<sut-ip>:9001,...`. This eliminates CPU
contention between the harness and OceanFS processes — SWIM gossip, failure
detection, and churn convergence are not affected by the harness's 16-32 Workers.
All 3-5 OceanFS processes run on the single SUT VM (multi-process, not
multi-VM). The test uses shortened gossip/SWIM/anti-entropy intervals (1s gossip,
3s suspicion, 8s failure, 10s anti-entropy) and runs concurrent PUT/GET/DELETE
while a `ChurnScheduler` kills and restarts random nodes every 10-30 seconds.

For single-VM mode (`--single-vm` flag, NOT recommended per ADR-0019 Decision 4):
the harness and OceanFS processes share one VM. CPU contention can cause false
gossip timeouts. The test configures **relaxed gossip parameters** to compensate:
`gossip_interval_ms=3000`, `suspicion_timeout_ms=10000`, `failure_timeout_ms=30000`.
A WARNING banner is printed before the test runs.

## Scope

### In Scope

- `#[tokio::test]` function in `e2e/tests/load_cluster_churn.rs`
- Two topology modes (per ADR-0019):
  - **Two-VM (default, recommended):** Harness on Harness VM connects via `TARGET_HOSTS=<sut-ip>:9000,<sut-ip>:9001,...` to 3-5 OceanFS processes on SUT VM. No CPU contention — gossip timing is reliable.
  - **Single-VM (opt-in via `--single-vm`):** Harness and all OceanFS processes on same VM. Sets relaxed gossip params. Prints WARNING banner.
- Duration: 2-5 minutes (`LOAD_TEST_DURATION_SECS` env var, default 120s quick / 300s full)
- Spawns or connects to 3-5 oceanfs processes with config: `config_fast_gossip()` + shortened AE (10s) + shortened SWIM (suspicion 3s, failure 8s) in two-VM mode
- For single-VM mode, gossip params relaxed to: `gossip_interval=3s, suspicion_timeout=10s, failure_timeout=30s` (per ADR-0019 Decision 4)
- Workers route randomly to any node (not just one coordinator) — exercises per-node routing
- `ChurnScheduler` configuration: churn interval 10-30s random, restart delay 15s, deterministic mode for reproducibility
- Churn mode: Poisson-distributed or fixed-interval with seed; at most 1 node dead at a time (keep quorum)
- Concurrent load scenario (same as Phase 2 simplified):
  - PUT 40%, GET 50%, DELETE 10%
  - Blob sizes: Tiered across all 4 tiers
  - Key space: RandomUuid (large, 10K keys)
- Assertions (checked at end of run, plus some checked per-churn-event):
  1. **membership_convergence**: After each churn event, `cluster.wait_for_convergence(alive_count)` within 10s (30s for single-VM relaxed mode)
  2. **manifest_integrity**: `manifest.verify(&cluster)` → 0 mismatches (any alive node can serve the data)
  3. **manifest_read_quorum**: For each key in manifest, read from R nodes; at least R nodes return correct data
  4. **hinted_handoff_delivery**: `hinted_handoff_hints_stored` ≈ `hinted_handoff_hints_delivered` at end (within 5% tolerance)
  5. **hinted_handoff_no_expiry**: `hinted_handoff_hints_expired_total` == 0 (for short-downtime churn)
  6. **hlc_monotonic**: HLC incarnation numbers never decrease; timestamps never move backward for same key (verified via per-key version tracking in Manifest)
  7. **ring_consistency**: `ring.lookup(hash)` returns identical successor set on all alive nodes for a fixed set of test hashes
  8. **no_split_brain**: No two nodes simultaneously believe they are coordinator for same key range (poll `/admin/cluster` on all nodes; compare ring ownership)
  9. **cache_invalidation**: After node B PUTs new version of key K, node A's subsequent GET returns new version within cache TTL (15s test: wait, verify)
  10. **all_churn_succeeded**: All churn events have `success=true`
- LoadReport includes churn events list and per-event convergence timing
- **Report path:** Always writes `LoadReport` JSON to `/tmp` (tmpfs) on the Harness VM, per ADR-0019 Decision 4

### Out of Scope

- Adversarial churn patterns (kill coordinator of most-written key) — deferred
- Correlated failures (kill 2 nodes simultaneously) — deferred to Phase 4
- Graceful leave testing (SIGTERM) — that's Phase 4/correctness-gaps
- Cluster rebalance migration measurement (key movement volume) — deferred to Phase 5

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | New test file `tests/load_cluster_churn.rs`. |

## Interface (Public API)

No new `pub` items — this is a `#[tokio::test]` function.

## Data Flow

```
Test: load_cluster_churn

Topology detection:
  → If TARGET_HOSTS is set (cloud two-VM mode):
      parse comma-separated host:port pairs
      connect to remote OceanFS processes (no spawning)
      config: fast_gossip (gossip=1s, suspicion=3s, failure=8s)
  → Else if --single-vm flag or no TARGET_HOSTS:
      spawn 3-5 NodeProcess locally
      if --single-vm: config relaxed gossip (gossip=3s, suspicion=10s, failure=30s)
      else: config fast_gossip

Test flow (both modes):
  1. Parse LOAD_TEST_SEED, LOAD_TEST_DURATION_SECS, TARGET_HOSTS
  2. Build config: fast_gossip + fast_swim + fast_ae (or relaxed if single-VM)
  3. Connect to or spawn 3-5 node Cluster
  4. Wait for initial convergence (cluster.wait_for_convergence(alive_count))
  5. Create Manifest, Orchestrator with per-node worker distribution
  6. Create ChurnScheduler (deterministic, 10-30s interval, 15s restart delay)
  7. Spawn metric scraper: poll all nodes every 10s → MetricsSnapshot per node
  8. Spawn churn task: tokio::spawn(churn_scheduler.run(duration))
  9. Spawn load workers: orchestrator.run(scenario, cluster, manifest)
  10. Wait for duration
  11. Join workers, churn, metric scraper
  12. Post-churn convergence: wait until all alive nodes agree on membership
  13. Manifest verification: verify from random alive node; also verify read-quorum (at least R nodes have data)
  14. Per-node metric assertions: hinted handoff, gossip, ring, heal
  15. Cache invalidation test: sequential PUT/GET across nodes, verify propagation
  16. Ring consistency: compare ring views across all alive nodes
  17. Build LoadReport; write JSON to /tmp (tmpfs) + textfile
  18. assert!(report.result == Pass)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `e2e` crate
<!-- REVIEW: verified 2026-08-20 — cargo build --all-targets -p e2e passes (13.96s). -->
- [x] **Code:** Test file `e2e/tests/load_cluster_churn.rs` compiles and links
<!-- REVIEW: verified 2026-08-20 — test binary compiles and runs; 1008-line file with all 10 assertions. -->
- [x] **Code:** Test supports remote-target mode (`TARGET_HOSTS` env var) and local-spawn mode
<!-- REVIEW: verified 2026-08-20 — Target enum (Local/Remote) at load_cluster_churn.rs:124-199; TARGET_HOSTS branch at :442-453; RemoteCluster::connect + TARGET_HOST_SSH churn. -->
- [ ] **Tests:** `cargo test -p e2e -- load_cluster_churn` passes in 2-5 minutes (local spawn, CI quick mode)
<!-- REVIEW: FAILS — test panics at load_cluster_churn.rs:1007 on manifest_read_quorum (1 of 102 keys, hot-75, 404/404/200) and total runtime is 588s (~9.8 min) for a 120s load, far over the 2-5 min window. The verification phase (150-key × 3-node sampling with body hashing) dominates runtime. Even the acknowledged 1-key residual fails the DoD "100% readable from R nodes" assertion. -->
- [ ] **Tests:** Remote target: `TARGET_HOSTS=10.0.0.5:9000,10.0.0.5:9001,10.0.0.5:9002 cargo test -p e2e -- load_cluster_churn` passes (cloud two-VM)
<!-- REVIEW: NOT VERIFIABLE locally (no cloud VMs). Code path exists (remote_target_mode.rs passes), but churn is SKIPPED unless TARGET_HOST_SSH is set, and no evidence of a passing cloud run is recorded in the repo. Unverified, not failed. -->
- [ ] **Tests:** Membership convergence: all churn events converge within 10 gossip rounds (30s for single-VM relaxed mode)
<!-- REVIEW: PARTIAL — post-churn convergence=true in verification run, but per-cycle convergence is only asserted in REMOTE mode (converged_after is Vec::new() for local spawn, load_cluster_churn.rs:551). Local mode asserts only post-churn convergence, not per-churn-event convergence within 10 gossip rounds. -->
- [ ] **Tests:** Manifest integrity: 100% of written keys readable from at least R nodes
<!-- REVIEW: FAILS — 1 of 102 keys (load-test/hot-75) served from only 1 of 3 nodes (404/404/200); assertion at load_cluster_churn.rs:862-871 + :1007. This is the documented residual class (ADR-0027 Decision 5 backstop target) but the DoD assertion still fails. -->
- [x] **Tests:** Hinted handoff: stored ≈ delivered (within 5%)
<!-- REVIEW: verified 2026-08-20 — stored=1875, delivered=1254, obsolete=754, expired=0; delivered+obsolete >= stored*0.95 (assertion at load_cluster_churn.rs:725-726). Note: the assertion counts obsolete-dropped hints as resolved — a documented semantic relaxation of the literal "stored ≈ delivered" wording. -->
- [x] **Tests:** Ring consistency: `ring.lookup(h)` returns identical successors on all alive nodes
<!-- REVIEW: verified 2026-08-20 — 8 probes agree on all nodes (assertion at load_cluster_churn.rs:897-904). -->
- [x] **Tests:** Cache invalidation: cross-node PUT → GET sequence returns newest version
<!-- REVIEW: verified 2026-08-20 — 0 of 20 keys served stale (assertion at load_cluster_churn.rs:915-920). -->
- [x] **Tests:** All churn events report `success=true`
<!-- REVIEW: verified 2026-08-20 — 16 events, 0 failed (assertion at load_cluster_churn.rs:922-927). -->
- [x] **Tests:** Deterministic: same seed produces same churn event sequence
<!-- REVIEW: verified 2026-08-20 — ChurnMode::Deterministic + ChaCha12Rng seed (e2e/src/load/churn.rs:95,127); remote churn round-robin is deterministic. -->
- [ ] **Tests:** Single-VM mode: WARNING printed, relaxed gossip params applied, convergence timeout extended to 30s
<!-- REVIEW: NOT IMPLEMENTED — no --single-vm flag, no WARNING banner, no relaxed-gossip profile anywhere in the test; module doc explicitly says "we only run the two-VM/fast profile" (load_cluster_churn.rs:107). ADR-0026 superseded the two-VM/single-VM decision for Phase 3+, but the feature doc was never updated. -->
- [x] **Tests:** LoadReport JSON written to `/tmp` (tmpfs) on Harness VM
<!-- REVIEW: verified 2026-08-20 — report written to /tmp/oceanfs-reports/3_load_cluster_churn_*.json (load_cluster_churn.rs:962; LOAD_TEST_REPORT_DIR default /tmp/oceanfs-reports). -->
- [x] **Docs:** Test doc comment explains cluster topology, two-VM vs single-VM modes, churn model, and each assertion
<!-- REVIEW: verified 2026-08-20 — module doc (lines 1-73) covers topology, remote/local modes, churn model, env vars, and all 10 assertions. -->
- [x] **Integration:** LoadReport includes churn event timeline and per-node metric snapshots
<!-- REVIEW: verified 2026-08-20 — report.churn_events (line 951), report.cluster_views (line 952), report.metric_snapshots (line 949), harness self-metrics (line 954). -->
