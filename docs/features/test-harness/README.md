# Test Harness — Master Index

**Date:** 2026-08-16
**Context:** Implementation plan for the OceanFS load test harness, test phases,
and operational tooling. Derived from three brainstorm design documents and
cross-referenced with the gap-closure plan. Updated per
[ADR-0019](../../adr/0019-test-harness-topology-cost-guardrails.md) to use a
**two-VM topology** for cloud-based phases (Phase 2–4): a dedicated **SUT VM**
(running OceanFS + Prometheus) and a dedicated **Harness VM** (running the e2e
harness + Rust toolchain) communicating over Hetzner's internal network.

> **Corrigendum (2026-08-16):** Hetzner retired the cx22/cx32 VM line. The
> provisioning scripts use **cx23** (2 vCPU / 4 GB) and **cx33** (4 vCPU / 8 GB)
> — see `scripts/vm-provision.sh` for the authoritative size mapping. The
> tables below keep the ADR's original names; treat the script as the source
> of truth.

**Source Documents:**

| Document | Purpose |
|---|---|
| [`docs/brainstorm/load-test-campaign.md`](../../brainstorm/load-test-campaign.md) | Phased load test roadmap (Phase 0–6), bug classes, assertions, CI strategy |
| [`docs/brainstorm/load-test-framework.md`](../../brainstorm/load-test-framework.md) | Harness architecture, VM topology, key types, skills catalog, results format |
| [`docs/brainstorm/load-test-metrics.md`](../../brainstorm/load-test-metrics.md) | 200+ metrics required per phase, remediation plan (Phases A–E) |
| [`docs/brainstorm/implementation-gap-plan.md`](../../brainstorm/implementation-gap-plan.md) | Dependency chain: config fix → metrics → write-path → correctness → background tasks |
| [`docs/adr/0019-test-harness-topology-cost-guardrails.md`](../../adr/0019-test-harness-topology-cost-guardrails.md) | Two-VM topology, cost guardrails, network bandwidth analysis |

---

## Topology Overview (per ADR-0019)

Phase 1 runs entirely in CI with local process spawning. Phases 2–4 use a
two-VM cloud topology:

```
┌─ Developer Laptop ────────────────────────────────────────────┐
│  Grafana :3000  (datasource → laptop Prometheus :9091)         │
│  laptop Prometheus :9091  (federates tunneled :9090, 365d)     │
│  ssh -L tunnel 9090 → SUT Prometheus (observe.sh)              │
│  ssh oceanfs-sut     (SUT VM)                                  │
│  ssh oceanfs-harness (Harness VM, optionally)                  │
│  (Zero load generation. SSH + browser + 2 small containers.)   │
└──────────┬──────────────────────────┬──────────────────────────┘
           │ SSH                      │ SSH
           ▼                          ▼
┌─ SUT VM ──────────────────┐  ┌─ Harness VM (CX23) ─────┐
│  oceanfs (1-5 processes)   │  │  e2e harness             │
│  prometheus :9090          │  │  Rust toolchain          │
│  No harness.               │  │  Targets SUT via         │
│  No compile toolchain.     │  │  internal 10.x.x.x:9000  │
│  Internal net: 10.0.0.x    │  │  Internal net: 10.0.0.x  │
└────────────────────────────┘  └──────────────────────────┘
         ▲                               │
         └─── Hetzner internal network ──┘
              (free, uncapped, <0.5ms RTT)
```

| Phase | SUT VM | Harness VM | Mode |
|---|---|---|---|
| Phase 1 | None (CI runner) | None (CI runner) | Local spawn in CI |
| Phase 2 | CX33 (4 vCPU, 8 GB) — per ADR-0019 Corrigendum 2 | CX23 (2 vCPU, 4 GB) | Remote target (`TARGET_HOST`) |
| Phase 3-4 | CX33 (4 vCPU, 8 GB) | CX23 (2 vCPU, 4 GB) | Remote target (`TARGET_HOSTS`) |

---

## Epic Summary

| # | Epic | Priority | Features | Blocks | Blocked By |
|---|---|---|---|---|---|---|
| 1 | [test-harness-extensions](#epic-1-test-harness-extensions) | **critical** | 6 | Epics 2, 3, 4 | gap-closure (config, metrics) |
| 2 | [test-phase-implementations](#epic-2-test-phase-implementations) | **critical** | 4 | Epic 4 | Epic 1 + gap-closure (write-path, correctness) |
| 3 | [operational-tooling](#epic-3-operational-tooling) | **high** | 3 | Epic 4 | Epic 1 (shared types), ADR-0019 (guardrails design) |
| 4 | [agent-skills](#epic-4-agent-skills) | **high** | 3 | — | Epics 1, 2, 3, ADR-0019 (two-VM topology design) |

---

## Script Inventory (current state, 2026-08-16)

The agent skills and the workflow script drive these `scripts/` files.
They are the source of truth for the operational interfaces:

| Script | Purpose | Consumed by |
|---|---|---|
| `lib/env-hetzner.sh` | Shared bootstrap sourced by every laptop-side script: loads `.hetzner/.env` (HCLOUD_TOKEN), ensures ssh-agent + adds `.hetzner/.ssh/hetzner-ssh`, exports `HETZNER_SSH_PUBLIC_KEY` (default provisioning key). No-op without `.hetzner/` (Harness VM) | vm-provision, observe, setup-harness, sut-deploy, run-phase2, test-agent-workflow |
| `vm-provision.sh` | Two-VM provisioning (cx23/cx33), firewalls, TTL timer, observability default, provisioning record `.hetzner/provision-*.json`, `--status`/`--destroy` | vm-up, vm-down, vm-status |
| `setup-harness.sh` | Full deploy pipeline: seed harness→SUT SSH key, repo sync + release build on the Harness, `sut-deploy.sh` to the SUT, observability ensure, health verify | vm-deploy |
| `sut-deploy.sh` | SUT install: binary, `/etc/oceanfs/oceanfs.toml`, systemd unit `oceanfs` (`Restart=no` for crash control) | setup-harness.sh |
| `setup-observability.sh` | SUT-side Prometheus :9090 + Node Exporter textfile collector (systemd) | vm-provision.sh (default), setup-harness.sh |
| `observe.sh` | Idempotent SSH tunnel `localhost:9090 → SUT:9090` (feeds the laptop Prometheus federation) | prometheus (federation), vm-metrics, vm-status |
| `backup-observability.sh` | Backs up the persistent laptop stack: Prometheus TSDB snapshot (admin API) + Grafana state, with rotation (default keep 7). Auto-invoked (best-effort) by `run-phase2.sh` after every remote run; run manually any time. **Use before any `docker compose ... down --volumes`** | run-phase2.sh (auto), agents/humans |
| `run-phase2.sh` | Phase 2 runner: `--harness` mode (payload on the Harness VM) + local mode; env wiring, textfile push, report fetch | vm-test-phase |
| `test-agent-workflow.sh` | End-to-end pipeline validation (provision → deploy → run → assert → teardown) | agent-integration-test |
| `dashboards/load-test.json` | Grafana dashboard (mounted into the laptop Grafana service) | Grafana (`mcps/docker-compose.yml`) |

Grafana itself runs on the **laptop** via `mcps/docker-compose.yml`:

- `grafana` service — UI at `http://localhost:3000`, dashboard auto-provisioned
  from `scripts/dashboards/load-test.json`.
- `prometheus` service — **persistent laptop Prometheus** (host port
  `localhost:9091`, 365-day retention in the `prometheus-storage` volume).
  It federates the SUT's Prometheus through the observe.sh tunnel
  (`/federate`, 15s), so every run's metrics survive VM teardown. Grafana
  reads from it (both use `network_mode: host`; datasource is the host
  loopback `127.0.0.1:9091`), and agents query it via the
  `vm-metrics` skill. `run-phase2.sh` ensures the tunnel automatically
  before each remote run, so archiving is the default.

The SUT-side Prometheus (`setup-observability.sh`) keeps a 7-day local
buffer; the laptop store is the durable copy. `vm-down --preserve-data`
additionally snapshots the SUT TSDB for full-fidelity archives.
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
                                                   │         │    (ADR-0019 guardrails)
ADR-0019 (topology + cost guardrails) ─────────────┘         │         │
                                                   │         │         │
                                                   ▼         │         │
                                              Epic 2: test-phase-implementations
                                                   │         │         │
                                                   │         ▼         │
                                                   └───► Epic 4: agent-skills ◄──┘
```

**Execution order:**
1. **Sprint A:** gap-closure (config + metrics) — launched before this epic
2. **Sprint B:** Epic 1 (harness extensions) + Epic 3 (tooling, incorporating ADR-0019 guardrails) — parallel
3. **Sprint C:** Epic 2 (test implementations) — depends on Sprint A + B; test harness must support `TARGET_HOST`/`TARGET_HOSTS` remote target mode for Phase 2+
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

| Load Test Phase | Implementation Location | Topology | Gated By |
|---|---|---|---|
| Phase 0 (micro-benchmarks) | `benches/` — CI job (out of scope for this epic) | CI runner | CI workflow only |
| Phase 1 (concurrency + TSAN) | Epic 2: `phase1-concurrency-test` | CI runner (single process, local spawn) | Config fix + metrics wiring |
| Phase 2 (sustained single-node) | Epic 2: `phase2-sustained-load-test` | Two-VM (SUT=CX22 + Harness=CX22) | Write path + correctness + metrics + `TARGET_HOST` remote target mode |
| Phase 3 (cluster churn) | Epic 2: `phase3-cluster-churn-test` | Two-VM (SUT=CX32 + Harness=CX22) | Phase 1-2 passing + all gap-closure |
| Phase 4 (degraded mode) | Epic 2: `phase4-degraded-mode-test` | Two-VM (SUT=CX32 + Harness=CX22) | Phase 3 passing + all gap-closure |
| Phase 5 (scale properties) | Epic 3: `loadgen-binary` | Dedicated loadgen binary targeting remote cluster | Phase 4 passing |
| Phase 6 (simulation 1000+ nodes) | NOT in this plan (separate `oceanfs-sim` crate, tracked in campaign doc §7) | Simulation | Phase 3-4 passing |

---

## CI Integration Strategy

```
Every PR:
  ├── Phase 0 (micro-benchmarks, <2 min)
  └── Phase 1 (concurrency + TSAN, <2 min)   ← Epic 2, Feature 1 (runs in CI, local spawn)

Merge to main:
  ├── Phase 0 + Phase 1
  └── Phase 2 "quick mode" (5 min, local spawn in CI)       ← Epic 2, Feature 2

Nightly / Agent-driven (cloud VMs, two-VM topology):
  ├── Phase 2 "full mode" (30-60 min)       ← SUT=CX22 + Harness=CX22, remote target
  └── Phase 3 (3-node cluster + churn, <15 min)  ← SUT=CX32 + Harness=CX22, remote target

Pre-release / Agent-driven (cloud VMs):
  └── Phase 4 (failure injection)           ← SUT=CX32 + Harness=CX22, remote target
```

**Note on CI vs Cloud:** Phase 1 always runs in CI with local process spawning
(`NodeProcess::spawn`). Phase 2 "quick mode" (5 min) also runs in CI with local
spawning. Phase 2 "full mode" and all Phase 3-4 runs use the two-VM cloud
topology with the harness in remote-target mode (`TARGET_HOST`/`TARGET_HOSTS` env
vars). This keeps CI fast and cheap while enabling full-length, contention-free
runs on demand via agent skills.

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

Build the tools that make the test VMs usable by both humans and agents.
Per ADR-0019, this provisions **two VMs** (SUT + Harness) for Phase 2–4
with cost guardrails (size cap, confirmation gate, auto-shutdown TTL).

| # | Feature | Summary |
|---|---|---|
| 3.1 | [prometheus-grafana-setup](operational-tooling/prometheus-grafana-setup/feature.md) | Prometheus config, systemd unit, Grafana dashboard JSON, SSH tunnel (runs on SUT VM) |
| 3.2 | [loadgen-binary](operational-tooling/loadgen-binary/feature.md) | Standalone `loadgen` binary for Phase 5 remote cluster targeting (also serves as remote-target mode foundation for Phase 2+ harness) |
| 3.3 | [vm-provisioning](operational-tooling/vm-provisioning/feature.md) | `scripts/vm-provision.sh` — two-VM provisioning per phase with cost guardrails |

---

## Epic 4: agent-skills

Create OpenCode skills that agents use to drive the two-VM test topology and consume results.
Per ADR-0019, skills manage two VMs: the SUT VM (OceanFS + Prometheus) and the
Harness VM (e2e harness + Rust toolchain), connected over Hetzner internal network.

| # | Feature | Summary |
|---|---|---|
| 4.1 | [vm-skills](agent-skills/vm-skills/feature.md) | `vm-status`, `vm-up`, `vm-down`, `vm-deploy` — two-VM lifecycle management (`.opencode/skills/`, **done 2026-08-16**) |
| 4.2 | [test-execution-skills](agent-skills/test-execution-skills/feature.md) | `vm-test-phase`, `vm-results`, `vm-metrics`, `vm-logs` — remote-target test execution (`.opencode/skills/`, **done 2026-08-16**) |
| 4.3 | [agent-integration-test](agent-skills/agent-integration-test/feature.md) | `scripts/test-agent-workflow.sh` — end-to-end workflow validation (two-VM topology, **done 2026-08-16**) |

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
6. **Remote target mode** — For Phase 2–4 cloud runs, the harness connects to
   already-running OceanFS processes via `TARGET_HOST` (single-node) or
   `TARGET_HOSTS` (multi-node) env vars. Local `NodeProcess::spawn` is
   preserved for Phase 1 CI runs. The `NodeProcess`/`Cluster` abstractions
   grow a `Remote` variant for connecting to running processes (ADR-0019).
7. **Harness report path** — Harness always writes `LoadReport` JSON to
   `/tmp` (tmpfs) regardless of topology, so disk-fill tests cannot prevent
   report output (ADR-0019 Decision 4).
