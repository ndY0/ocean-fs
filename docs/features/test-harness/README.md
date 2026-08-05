# Test Harness — Master Index

**Date:** 2026-08-05
**Context:** Implementation plan for the OceanFS load test harness, test phases,
and operational tooling. Derived from three brainstorm design documents and
cross-referenced with the gap-closure plan.

**Source Documents:**

| Document | Purpose |
|---|---|
| [`docs/brainstorm/load-test-campaign.md`](../../brainstorm/load-test-campaign.md) | Phased load test roadmap (Phase 0–6), bug classes, assertions, CI strategy |
| [`docs/brainstorm/load-test-framework.md`](../../brainstorm/load-test-framework.md) | Harness architecture, VM topology, key types, skills catalog, results format |
| [`docs/brainstorm/load-test-metrics.md`](../../brainstorm/load-test-metrics.md) | 200+ metrics required per phase, remediation plan (Phases A–E) |
| [`docs/brainstorm/implementation-gap-plan.md`](../../brainstorm/implementation-gap-plan.md) | Dependency chain: config fix → metrics → write-path → correctness → background tasks |

---

## Epic Summary

| # | Epic | Priority | Features | Blocks | Blocked By |
|---|---|---|---|---|---|
| 1 | [test-harness-extensions](#epic-1-test-harness-extensions) | **critical** | 6 | Epics 2, 3, 4 | gap-closure (config, metrics) |
| 2 | [test-phase-implementations](#epic-2-test-phase-implementations) | **critical** | 4 | Epic 4 | Epic 1 + gap-closure (write-path, correctness) |
| 3 | [operational-tooling](#epic-3-operational-tooling) | **high** | 3 | Epic 4 | Epic 1 (shared types) |
| 4 | [agent-skills](#epic-4-agent-skills) | **high** | 3 | — | Epics 1, 2, 3 |

---

## Dependency Graph

```
gap-closure/config-system-fix ────────────────────┐
gap-closure/metrics-infrastructure ───────────────┤
                                                   ├────► Epic 1: test-harness-extensions
                                                   │         │
gap-closure/write-path-unification ────────────────┤         │
gap-closure/correctness-gaps ──────────────────────┤         ├───► Epic 3: operational-tooling
                                                   │         │         │
                                                   │         │         │
                                                   ▼         │         │
                                              Epic 2: test-phase-implementations
                                                   │         │         │
                                                   │         ▼         │
                                                   └───► Epic 4: agent-skills ◄──┘
```

**Execution order:**
1. **Sprint A:** gap-closure (config + metrics) — launched before this epic
2. **Sprint B:** Epic 1 (harness extensions) + Epic 3 (tooling) — parallel
3. **Sprint C:** Epic 2 (test implementations) — depends on Sprint A + B
4. **Sprint D:** Epic 4 (agent skills) — depends on all above

---

## Gap-Closure Dependency Resolution

Every test harness feature that depends on gap-closure work explicitly lists
which epics must be completed first:

| Gap-Closure Epic | Resolves | Needed By |
|---|---|---|
| **config-system-fix** | Configurable GC/AE/scrub intervals, max_body_size, env var overrides | Phase 2-4 sustained/churn/degraded tests |
| **metrics-infrastructure** | Gauge type, labels, AtomicU64 histograms, 25+ wired metrics | All load test assertions (Phase 1-4) |
| **write-path-unification** | Segment metadata in segments CF, real GC/scrub/AE data | Phase 3 segment assertions, Phase 2 leak detection |
| **correctness-gaps** | WAL recovery, read repair, EC decode, hinted handoff delivery, graceful leave | Phase 2 post-crash verification, Phase 3-4 churn/failure recovery |

---

## Phase-to-Epic Mapping

| Load Test Phase | Implementation Location | Gated By |
|---|---|---|
| Phase 0 (micro-benchmarks) | `benches/` — CI job (out of scope for this epic) | CI workflow only |
| Phase 1 (concurrency + TSAN) | Epic 2: `phase1-concurrency-test` | Config fix + metrics wiring |
| Phase 2 (sustained single-node) | Epic 2: `phase2-sustained-load-test` | Write path + correctness + metrics |
| Phase 3 (cluster churn) | Epic 2: `phase3-cluster-churn-test` | Phase 1-2 passing + all gap-closure |
| Phase 4 (degraded mode) | Epic 2: `phase4-degraded-mode-test` | Phase 3 passing + all gap-closure |
| Phase 5 (scale properties) | Epic 3: `loadgen-binary` | Phase 4 passing |
| Phase 6 (simulation 1000+ nodes) | NOT in this plan (separate `oceanfs-sim` crate, tracked in campaign doc §7) | Phase 3-4 passing |

---

## CI Integration Strategy

```
Every PR:
  ├── Phase 0 (micro-benchmarks, <2 min)
  └── Phase 1 (concurrency + TSAN, <2 min)   ← Epic 2, Feature 1

Merge to main:
  ├── Phase 0 + Phase 1
  └── Phase 2 "quick mode" (5 minutes)       ← Epic 2, Feature 2

Nightly:
  ├── Phase 0-2
  └── Phase 3 (3-node cluster + churn, <15 min)  ← Epic 2, Feature 3

Pre-release / weekly:
  ├── Phase 0-4 (including failure injection)     ← Epic 2, Feature 4
```

---

## Epic 1: test-harness-extensions

Build the load test harness types in the `e2e/` crate. These are pure
infrastructure — no test scenarios yet.

| # | Feature | Summary |
|---|---|---|
| 1.1 | [manifest-tracker](test-harness-extensions/manifest-tracker/feature.md) | `Manifest` type — DashMap-based PUT tracker + BLAKE3 verifier |
| 1.2 | [load-scenario-orchestrator](test-harness-extensions/load-scenario-orchestrator/feature.md) | `LoadScenario`, `Worker`, `OpWeight`, `BlobSizeDist`, `KeySpace`, stats types |
| 1.3 | [metrics-scraper](test-harness-extensions/metrics-scraper/feature.md) | `MetricsSnapshot` — Prometheus text format parser + delta computation |
| 1.4 | [load-report](test-harness-extensions/load-report/feature.md) | `LoadReport` — JSON output, Prometheus textfile, assertion tracking |
| 1.5 | [churn-scheduler](test-harness-extensions/churn-scheduler/feature.md) | `ChurnScheduler` — periodic node kill/restart for Phase 3 |
| 1.6 | [failure-injectors](test-harness-extensions/failure-injectors/feature.md) | `Cluster` extensions — latency, disk fill, segment corruption, heal verification |

---

## Epic 2: test-phase-implementations

Implement the actual load test functions. Each is a `#[tokio::test]` in `e2e/tests/`.

| # | Feature | Summary |
|---|---|---|
| 2.1 | [phase1-concurrency-test](test-phase-implementations/phase1-concurrency-test/feature.md) | Single-node, N concurrent workers, TSAN, 60s, manifest integrity |
| 2.2 | [phase2-sustained-load-test](test-phase-implementations/phase2-sustained-load-test/feature.md) | Single-node, 30-60min, resource stability, post-crash WAL recovery |
| 2.3 | [phase3-cluster-churn-test](test-phase-implementations/phase3-cluster-churn-test/feature.md) | 3-5 node, churn, gossip convergence, hinted handoff, ring consistency |
| 2.4 | [phase4-degraded-mode-test](test-phase-implementations/phase4-degraded-mode-test/feature.md) | 3-node, failure injections, mid-write kill, slow-node, disk-full, corruption+heal |

---

## Epic 3: operational-tooling

Build the tools that make the test VM usable by both humans and agents.

| # | Feature | Summary |
|---|---|---|
| 3.1 | [prometheus-grafana-setup](operational-tooling/prometheus-grafana-setup/feature.md) | Prometheus config, systemd unit, Grafana dashboard JSON, SSH tunnel |
| 3.2 | [loadgen-binary](operational-tooling/loadgen-binary/feature.md) | Standalone `loadgen` binary for Phase 5 remote cluster targeting |
| 3.3 | [vm-provisioning](operational-tooling/vm-provisioning/feature.md) | `scripts/vm-provision.sh` — cloud VM provisioning per phase |

---

## Epic 4: agent-skills

Create OpenCode skills that agents use to drive the test VM and consume results.

| # | Feature | Summary |
|---|---|---|
| 4.1 | [vm-skills](agent-skills/vm-skills/feature.md) | `vm-status`, `vm-up`, `vm-down`, `vm-deploy` skill files |
| 4.2 | [test-execution-skills](agent-skills/test-execution-skills/feature.md) | `vm-test-phase`, `vm-results`, `vm-metrics`, `vm-logs` skill files |
| 4.3 | [agent-integration-test](agent-skills/agent-integration-test/feature.md) | End-to-end agent workflow test script |

---

## Cross-Cutting Requirements

1. **Manifest integrity** — Every load test tracks written keys + BLAKE3 hashes
   and verifies them at end of run. Deleted keys (by DELETE workers during
   concurrent load) are skipped during verify, not reported as mismatches.
2. **Deterministic seeding** — All tests accept `LOAD_TEST_SEED` env var. If
   not set, generate random seed and log it.
3. **Metrics-based assertions** — Where possible, tests assert on Prometheus
   metrics rather than log output.
4. **Environment-variable gating** — `LOAD_TEST_DURATION_SECS` for Phase 2
   (5 min quick, 60 min full). `LOAD_TEST_SEED` for reproducibility.
5. **Platform awareness** — Linux-specific injectors (`tc netem`) skip with
   warning on macOS. TSAN requires nightly Rust.
