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
adr:
  - 0001-segment-packing
perf:
  - "11.1 Atomic counters on hot paths"
created: 2026-08-05
updated: 2026-08-05
---

# Phase 3 — 3-5 Node Cluster Churn Under Load Test

## Summary

Implement `e2e/tests/load_cluster_churn.rs` — a `#[tokio::test]` that validates
distributed protocol correctness under sustained load with node churn. Spawns
a 3-5 node `Cluster` with shortened gossip/SWIM/anti-entropy intervals (1s gossip,
3s suspicion, 8s failure, 10s anti-entropy). Runs concurrent PUT/GET/DELETE from
all nodes simultaneously for 2-5 minutes while a `ChurnScheduler` kills and
restarts random nodes every 10-30 seconds. Asserts membership convergence after
every churn event, manifest integrity (all writes readable from at least R nodes),
hinted handoff delivery completeness, cache invalidation propagation, HLC
monotonicity, and ring consistency. Produces a `LoadReport` with churn events
and distributed protocol metrics.

## Scope

### In Scope

- `#[tokio::test]` function in `e2e/tests/load_cluster_churn.rs`
- Duration: 2-5 minutes (`LOAD_TEST_DURATION_SECS` env var, default 120s quick / 300s full)
- Spawns 3-5 node `Cluster` with config: `config_fast_gossip()` + shortened AE (10s) + shortened SWIM (suspicion 3s, failure 8s)
- Workers route randomly to any node (not just one coordinator) — exercises per-node routing
- `ChurnScheduler` configuration: churn interval 10-30s random, restart delay 15s, deterministic mode for reproducibility
- Churn mode: Poisson-distributed or fixed-interval with seed; at most 1 node dead at a time (keep quorum)
- Concurrent load scenario (same as Phase 2 simplified):
  - PUT 40%, GET 50%, DELETE 10%
  - Blob sizes: Tiered across all 4 tiers
  - Key space: RandomUuid (large, 10K keys)
- Assertions (checked at end of run, plus some checked per-churn-event):
  1. **membership_convergence**: After each churn event, `cluster.wait_for_convergence(alive_count)` within 10s
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
  1. Parse LOAD_TEST_SEED, LOAD_TEST_DURATION_SECS
  2. Build config: fast_gossip + fast_swim + fast_ae
  3. Spawn 3-node Cluster
  4. Wait for initial convergence (cluster.wait_for_convergence(3))
  5. Create Manifest, Orchestrator with per-node worker distribution
  6. Create ChurnScheduler (deterministic, 10-30s interval, 15s restart delay)
  7. Spawn metric scraper: poll all 3 nodes every 10s → MetricsSnapshot per node
  8. Spawn churn task: tokio::spawn(churn_scheduler.run(duration))
  9. Spawn load workers: orchestrator.run(scenario, cluster, manifest)
  10. Wait for duration
  11. Join workers, churn, metric scraper
  12. Post-churn convergence: wait until all alive nodes agree on membership
  13. Manifest verification: verify from random alive node; also verify read-quorum (at least R nodes have data)
  14. Per-node metric assertions: hinted handoff, gossip, ring, heal
  15. Cache invalidation test: sequential PUT/GET across nodes, verify propagation
  16. Ring consistency: compare ring views across all alive nodes
  17. Build LoadReport; write JSON + textfile
  18. assert!(report.result == Pass)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `e2e` crate
- [ ] **Code:** Test file `e2e/tests/load_cluster_churn.rs` compiles and links
- [ ] **Tests:** `cargo test -p e2e -- load_cluster_churn` passes in 2-5 minutes
- [ ] **Tests:** Membership convergence: all churn events converge within 10 gossip rounds
- [ ] **Tests:** Manifest integrity: 100% of written keys readable from at least R nodes
- [ ] **Tests:** Hinted handoff: stored ≈ delivered at end (within 5%)
- [ ] **Tests:** Ring consistency: `ring.lookup(h)` returns identical successors on all alive nodes
- [ ] **Tests:** Cache invalidation: cross-node PUT → GET sequence returns newest version
- [ ] **Tests:** All churn events report `success=true`
- [ ] **Tests:** Deterministic: same seed produces same churn event sequence
- [ ] **Docs:** Test doc comment explains cluster topology, churn model, and each assertion
- [ ] **Integration:** LoadReport includes churn event timeline and per-node metric snapshots
