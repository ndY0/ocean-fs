# ADR-0026: Phase 3+ Cluster Topology — Dedicated Node VMs (supersedes ADR-0019 Decisions 1 & 4)

**Status:** Accepted
**Date:** 2026-08-19
**Deciders:** User (budget owner) + Implementer

**Supersedes:** [ADR-0019 Decision 1](./0019-test-harness-topology-cost-guardrails.md)
(two-VM topology for Phase 3-4) and [ADR-0019 Decision 4](./0019-test-harness-topology-cost-guardrails.md)
(co-location mitigations for Phase 3-4 single-VM), **for Phases 3+ only**.
Phase 1 (CI) and Phase 2 (single-node) topologies are unchanged.

---

## Context

ADR-0019 chose to run Phase 3-4 clusters as **3-5 co-located `oceanfs`
processes on a single SUT VM** (CX33, 4 vCPU / 8 GB), driven by a separate
Harness VM (CX23, 2 vCPU / 4 GB). That decision was a **cost compromise**:
multi-VM fleets were assumed to be too expensive for routine testing.

Two observations from the Phase 2 validation campaign (2026-08-16..19)
invalidate the premise:

1. **Actual cost is negligible.** The user monitored the Hetzner bill across
   multiple 5-minute runs, a 60-minute sustained run, and idle time. The
   two-VM fleet (CX33 + CX23) cost fractions of a Euro for the entire
   campaign. A 3-node cluster + upgraded harness totals roughly
   **€0.075-0.09/hour** — a 5-minute smoke run costs about one cent, a
   full-hour run about ten cents, a forgotten 24-hour fleet about €2.

2. **The Harness VM is the bottleneck.** The CX23 (2 vCPU) was observed
   maxed out during Phase 2 runs: ~130-155 ops/s of BLAKE3 + HTTP + JSON
   generation, plus multi-minute `cargo build --release` bursts on every
   deploy. Phase 3 needs ~3× the aggregate throughput (3 nodes at
   comparable per-node rates), per-node metric polling (3×), churn
   orchestration, and convergence checks — a CX23 cannot sustain that.

3. **Co-location undermines the Phase 3 goal.** ADR-0019's own analysis
   flags CPU starvation as the **High-severity** concern for Phase 3: SWIM
   gossip, failure detection, and churn convergence are sensitive to
   scheduling delays. Co-located processes on one VM still share CPU among
   themselves — one node's compaction/EC spike delays another node's SWIM
   pings, producing false gossip timeouts even without the harness on the
   box. The shared 8 GB RAM is also genuinely tight for 3-5 nodes (Phase 2
   single-node RSS reached ~1.8 GB).

4. **Phase 4 readiness.** Dedicated node VMs give Phase 4 real failure-
   injection semantics: killing a node = killing a VM; network partitions
   become possible; `tc netem` on one VM cannot affect others.

## Decision

### Decision 1: Phase 3+ topology — one VM per cluster node

```
┌─ Developer Laptop ─────────────────────────────────────────────┐
│  Grafana :3000  → laptop Prometheus :9091 (federation sink)     │
│  ssh tunnels: observe.sh → node-0 Prometheus :9090              │
└───────┬────────────────────────────────┬────────────────────────┘
        │ SSH                            │ SSH
        ▼                                ▼
┌─ SUT node 0 (cx33, 10.0.0.2) ─┐  ┌─ Harness VM (cx43) ─────────┐
│  oceanfs :9000/:9001 (bootstrap│  │  e2e harness (TARGET_HOSTS) │
│  Prometheus :9090 (scrapes ALL │  │  Rust toolchain / cargo     │
│  nodes: 10.0.0.2, .3, .4:9000) │  │  Targets 10.0.0.2/.3/.4:9000│
└───────────────┬────────────────┘  └─────────────────────────────┘
        │ SUT node 1 (cx33, 10.0.0.3) — oceanfs :9000/:9001
        │ SUT node 2 (cx33, 10.0.0.4) — oceanfs :9000/:9001
        └──── gossip/replication over Hetzner internal net ───────┘
```

- **Node count:** default **3** (majority quorum semantics, matches the
  Phase 3 DoD). Parametric: `LOAD_TEST_CLUSTER_NODES` (or `--nodes N`)
  at provisioning time, so the fleet can grow to 4-5 without tooling
  changes — provisioning, deploy, runner, and dashboards all derive N from
  the provision record.
- **Per-node sizing:** CX33 (4 vCPU / 8 GB) + the existing 2 GiB swapfile
  setup. Rationale: every node stores/codes a replica of every write, so
  per-node work does not drop by 1/N; CX33 matches the calibrated Phase 2
  SUT profile.
- **Ports:** every node listens on `:9000` (HTTP/S3/admin) and `:9001`
  (gRPC — gossip + replication). No port juggling; nodes differ by IP.
- **Cluster formation (mirrors the local `Cluster` harness):** node 0 is
  the bootstrap (no `seed_nodes`); nodes 1..N-1 get
  `seed_nodes = ["10.0.0.2:9001"]` under `[gossip]`. Fallback seeds
  (`membership_state.rs`) cover node-0 restarts.
- **Observability:** Prometheus + node-exporter textfile collector live on
  **node 0**, scraping all N node endpoints (`localhost` + peers). The
  laptop tunnel/federation pipeline is unchanged (`observe.sh` → node-0
  `:9090`). The `instance` label distinguishes nodes in Grafana.
- **Firewall:** unchanged rules (SSH + `:9000`/`:9001` from
  `10.0.0.0/24` + ICMP) applied to every node VM — the existing
  `sut_rules_json()` already covers inter-node traffic.
- **systemd:** one `oceanfs` unit per VM (`Restart=no` — crash control
  unchanged), one config per node (node_id + seed list differ).

### Decision 2: Harness VM upgrade to CX43 (8 vCPU / 16 GB)

The harness must never be the test bottleneck. Phase 2 maxed a 2-vCPU
CX23 at ~150 ops/s; Phase 3 needs ~3× throughput, 3-node metric polling,
churn orchestration, and faster deploy builds. CX43 provides ~4× the
compute for ~2× the cost (~€0.06/h). The ADR-0019 Layer 1 hard cap
(`MAX_AGENT_VM_TYPE="cx33"`) is **raised to `cx43`** for the Harness role;
SUT nodes stay capped at cx33.

### Decision 3: Phase 2 unchanged

Phase 2 stays on the two-VM topology (CX33 SUT + CX23 Harness). The
upgrade path for the shared Harness VM is noted in `vm-provision.sh` but
Phase 2 runs are unchanged and remain the cheap smoke layer.

### Decision 4: Cost guardrails retained, cost model updated

The ADR-0019 guardrails (TTL timer, hard size cap, internal-network-only
traffic) are retained as-is. The cost model in the guardrails docs is
updated for the Phase 3 fleet:

| Fleet | €/hour (approx) | 24h | Notes |
|---|---|---|---|
| Phase 2 (CX33 + CX23) | ~0.045 | ~€1.1 | unchanged |
| Phase 3 (3×CX33 + CX43) | ~0.09 | ~€2.2 | 3-node cluster |
| Phase 3 (5×CX33 + CX43) | ~0.15 | ~€3.6 | max fleet size |

Intermittent use (the actual model — TTL powers VMs down after 4h):
a full test day of several runs ≈ **€0.30-0.60**.

---

## Consequences

### Positive

1. **Gossip/SWIM timing is trustworthy.** Per-node CPU isolation removes
   the artificial scheduling delays that co-location introduced; Phase 3
   keeps the fast gossip params (1s/3s/8s) without a relaxed fallback.
2. **No shared-RAM squeeze.** Each node has the full calibrated CX33
   memory profile; no 3-5-node budget over one 8 GB box.
3. **Simpler deploy.** One systemd unit, one config per VM; only
   node_id + seed list vary. No multi-unit/port-juggling hacks.
4. **Phase 4 semantics unlocked.** Kill a node = kill a VM; real network
   partitions; failure injection isolated from the harness.
5. **Parametric fleet.** 3 → 4 → 5 nodes is a provisioning flag, not a
   tooling change.
6. **Cost is trivially affordable** (Decision 4 table) — the user's
   observed bills confirm it.

### Negative

1. **More VMs to manage** — 4 instead of 2. The provisioning script
   handles the fleet; the skills (`vm-status`, `vm-test-phase`) must
   report/act on N nodes instead of one SUT.
2. **Provision record schema change** — `sut` (single) becomes
   `sut_nodes` (array) for Phases 3+; phase 2 keeps the legacy single
   `sut` field. Consumers (`vm-up`/`vm-status`/`vm-test-phase` skills,
   `run-phase3.sh`) must read the array.
3. **Node-0 restarts re-bootstrap the cluster** — fallback seeds cover
   this, but a crashed node 0 takes longer to rejoin than a seeded node
   would; acceptable, mirrors the local harness behavior.
4. **Deploy fan-out** — `sut-deploy.sh` must deploy to N VMs; a cluster
   deploy mode (single invocation, per-node seed wiring) is required.

### Neutral

1. **Prometheus placement on node 0** couples observability to the
   bootstrap node; a crashed node 0 loses the scrape during its downtime
   (acceptable — the laptop federation keeps historical data).
2. **The `--single-vm` fallback** remains for Phase 2 only; Phase 3-4
   single-VM mode is removed (it was "NOT recommended" in ADR-0019 and is
   now superseded entirely).

---

## Impact on Existing Documents & Code

| Item | Change |
|---|---|
| `docs/adr/0019-...md` | Add supersession note pointing to this ADR for Decisions 1 & 4 (Phase 3+), and update the VM table cost model. |
| `scripts/vm-provision.sh` | Phase 3/4: provision N SUT VMs (`--nodes N`, default 3) + CX43 harness; `MAX_AGENT_VM_TYPE="cx43"`; provision record gains `sut_nodes[]`; cost estimate computes N×SUT. |
| `scripts/sut-deploy.sh` | New cluster mode: deploy to N VMs, per-node node_id + seed wiring, Prometheus scrape list on node 0. |
| `scripts/run-phase3.sh` | NEW: `TARGET_HOSTS=<ip0..ipN-1>:9000`, per-VM SSH crash control (`TARGET_HOST_SSH` comma-separated), convergence-aware report fetch. |
| `e2e/src/remote.rs` | `TARGET_HOST_SSH` becomes a comma-separated per-host list; `RemoteCluster` maps node index → SSH target for churn kill/restart. |
| `.opencode/skills/vm-*.md` | vm-up/vm-status/vm-deploy/vm-test-phase: N-node awareness, provision-record array parsing, per-node status/health. |
| `docs/features/test-harness/...` | test-harness README topology diagram + phase3-cluster-churn-test topology section (two-VM → 3-node fleet). |
| `scripts/dashboards/load-test.json` | Per-instance panels (RSS, ops, gossip, handoff per node) — `instance` label from the node-0 Prometheus scrape. |

---

## References

- [ADR-0019](./0019-test-harness-topology-cost-guardrails.md) — superseded decisions 1 & 4
- [`docs/brainstorm/load-test-campaign.md`](../brainstorm/load-test-campaign.md) §4 — Phase 3 definition
- [`docs/features/test-harness/test-phase-implementations/phase3-cluster-churn-test/feature.md`](../features/test-harness/test-phase-implementations/phase3-cluster-churn-test/feature.md) — Phase 3 DoD
- [`docs/features/test-harness/README.md`](../features/test-harness/README.md) — master index, topology section
