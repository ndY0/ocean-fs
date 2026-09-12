---
epic: "pool-runtime-lifecycle"
status: proposed
priority: critical
created: 2026-09-12
updated: 2026-09-12
---

# Pool Runtime Lifecycle (pre-f4 Product Fixes) — Epic Plan

Epic: `pool-runtime-lifecycle`
ADRs: [ADR-0029](../../adr/0029-storage-pools-disk-resilience.md) (pools;
§D2/D5 capacity, §D3 data-pool return, §D8 runtime attach),
[ADR-0017](../../adr/0017-durability-task-abstraction.md) (scheduler /
two-tier budget), [ADR-0030](../../adr/0030-re-replication-target-pull.md)
(target-pull repair), [ADR-0033](../../adr/0033-manifest-aware-peer-selection.md)
(manifest-aware selection),
[ADR-0035](../../adr/0035-replicated-segment-lifecycle-state.md)
(replicated lifecycle state / `storage_locations`),
[ADR-0036](../../adr/0036-phase-c-scale-ops-drain.md) (drain/detach),
[ADR-0019](../../adr/0019-test-harness-topology-cost-guardrails.md) and
[ADR-0026](../../adr/0026-phase3-dedicated-node-vms.md) (fleet cost/topology
guardrails — retained at epic level).
Related: the **paused** [fleet-degradation epic](../fleet-degradation/epic.md)
and its [f4 pool-degradation-under-load](../fleet-degradation/f4-pool-degradation-under-load.md),
[f5 degraded-pool-semantics](../fleet-degradation/f5-degraded-pool-semantics.md)
(the accounting substrate pr2's residue sweep feeds).

## User directive (2026-09-12)

> *"I am kinda sick of having to postpone all the time... Kill the cloud test
> suite now, pause the test epic, create all necessary scaffolding for fixing
> both issues in priority. It is pointless to test a faulty behaviour."*

Actions recorded from the directive: the `fleet-degradation` epic is paused
(its pause section and f4's banner already point at this epic), the cloud
fleet is destroyed (**0 servers, 0 volumes**), and this epic is created to fix
the two pool-runtime defects **before** any further fleet testing. The fixes
are product code + config only — no test-only hooks.

## Goal

Fix the two product defects in the pool runtime lifecycle that the f3/f5 fleet
runs and the f4 grounding exposed, so the paused `fleet-degradation` epic can
resume and f4 does not test semantics that are about to change:

1. **pr1 — capacity is stale.** `PoolRegistry::refresh_capacity` has no
   periodic caller: placement decisions, `oceanfs_pool_bytes_*`, and the
   gossiped `capacity_free_bytes` drift under load.
2. **pr2 — a Dead data pool cannot return at runtime.** `Dead` is absorbing,
   the reset hook only serves WAL/metadata recovery, the runtime paths all
   refuse the same Dead entry, and there is no pool-return residue handling
   for the lost local copies.

Both are product-side fixes (a background task and an operator-triggered
admin route), exercised by product tests. They are the only product work
blocking the paused fleet-degradation epic.

## Dependency graph

```
pr1-capacity-refresh  (priority 1 of 2) — done 2026-09-12 (review iteration 2 PASS)
        │
        ▼
pr2-dead-pool-recovery  (priority 2 of 2) — next
        │
        ▼
resume fleet-degradation f4   (paused; resumes after both land with review PASS)
```

`pr1 → pr2` reflects the approved order: pr1 landed 2026-09-12 (review
iteration 2 PASS) and keeps capacity fresh on the periodic tick; pr2 is next
and its reset path re-probes + refreshes capacity on top of that. f4 then
re-provisions the fleet and keeps its original scope; its P1b/P2/P3 recovery
assertions use the new runtime path where it replaces a restart.

## Grounded defects (verified 2026-09-12 at HEAD b3eba7f)

### D1 — Pool capacity is stale

- `PoolRegistry::refresh_capacity` (`crates/oceanfs-storage/src/pool/mod.rs:1300-1310`)
  has **no periodic caller**. The only callers are boot/attach
  (`pool/mod.rs:1551-1583`; capacity probed at `:1565-1566`) and the
  intra-node drain mover after a relocate
  (`crates/oceanfs-storage/src/drain/intra_node.rs:241`).
- Consumers read the stale snapshot: placement filters/scores on `free_bytes`
  (`crates/oceanfs-storage/src/pool/placement.rs:99-103`, `:179-192`), the
  `oceanfs_pool_bytes_free{pool_id}` / `oceanfs_pool_bytes_total{pool_id}`
  gauges (`pool/mod.rs:743-746`, `:789-798`), and the gossiped manifest's
  `capacity_free_bytes` (`crates/oceanfs-node/src/pool_manifest.rs:89-96`).
- The module docs already assume the missing tick: `pool/mod.rs:34-35`
  ("the maintenance-tick capacity refresh") and `pool/mod.rs:1455-1462`
  ("The node's maintenance task normally drives capacity via
  `refresh_capacity`") — the periodic caller was intended but never landed.
- Full grounding, decision, and rejection of the hot-path alternative:
  [pr1-capacity-refresh](pr1-capacity-refresh.md).

### D2 — A Dead data pool cannot return at runtime

- `Dead` is absorbing: `decide_transition` only leaves `Healthy→Degraded` and
  `Degraded→Dead` on `ConfirmedLoss`; there is no `Dead→*` transition
  (`crates/oceanfs-storage/src/pool/health.rs:901-955`, esp. `:922-925`,
  `:940-942`).
- `HealthMonitor::reset_pool` exists (`health.rs:750-757`) but is wired only
  to WAL and metadata recovery (`crates/oceanfs-node/src/modules/wal_recovery.rs:1127`,
  `crates/oceanfs-node/src/modules/metadata_recovery.rs:278-281`); nothing
  for data pools.
- Boot is the only re-probe (`crates/oceanfs-storage/src/pool/mod.rs:1078-1097`);
  with the data device still yanked, `missing_root_policy="fatal"` refuses to
  start the node. `POST /admin/pools` attach accepts only a **new** root
  (duplicate name/root → 409; `crates/oceanfs-server/src/admin.rs:1251-1301`,
  `pool/mod.rs:1551-1583`), and detach refuses a Dead pool (409
  not-Detachable; `admin.rs:1324-1364`). The same Dead entry cannot be revived
  at runtime.
- No pool-return residue handling: the only data-residue sweep is the
  once-per-boot `.dat`-vs-registry classification
  (`crates/oceanfs-storage/src/segment/relocate.rs:242-274`, invoked from
  `crates/oceanfs-node/src/modules/storage.rs:815-884`); the orphan reaper is
  dead-byte accounting only (`crates/oceanfs-durability/src/gc/orphan_reaper.rs:152-199`).
  After a fresh-format recovery the registry still believes the pool holds
  segments; f5 D2's live-copy accounting reads manifest health, not disk
  truth, so a Healthy-but-empty returned pool can mask under-replication
  until repair/scrub notices.
- Full grounding, proposed mechanism, and ADR hand-off:
  [pr2-dead-pool-recovery](pr2-dead-pool-recovery.md).

## Features

| # | Feature | Status | Priority | Depends on | Deliverable in one line |
|---|---|---|---|---|---|
| pr1 | [capacity-refresh](pr1-capacity-refresh.md) | done (2026-09-12, review iteration 2 PASS) | critical | — | Periodic background task calling `refresh_capacity` for all registered pools + `[durability] capacity_refresh_interval_sec` (default 10, `0` disables) + validation; refreshed gauges/manifest reach placement; f4's C2a/C2b dataset consumes the metrics instead of SSH `df` |
| pr2 | [dead-pool-recovery](pr2-dead-pool-recovery.md) | proposed | critical | pr1; f5 (done) | Operator-triggered, probe-gated runtime return of a Dead data pool (`POST /admin/pools/{id}/reset`, name OQ) + return-residue sweep correcting `storage_locations` + reconciliation observes the corrected state; restart stays supported |

## Acceptance bar (epic DoD)

- [ ] **pr1:** lands with review PASS — a periodic background refresh exists
      (no hot-path syscall), the new config knob is parsed/validated and
      documented, the refresh-effect and disabled-interval tests pass, and the
      existing suites show no regression.
- [ ] **pr2:** lands with review PASS — a Dead data pool returns at runtime
      operator-triggered and probe-gated (probe failure keeps it Dead, never a
      silent Healthy), the return-residue sweep corrects `storage_locations`
      (stale entries removed, present files kept), reconciliation observes the
      corrected state, and node-integration coverage passes.
- [ ] **Both:** no test-only hooks; ADR-0029 §D3's data-pool return semantics
      are satisfied (pr2 records the amendment/new-ADR hand-off); no
      performance assertion is added anywhere.
- [ ] **Resume:** `fleet-degradation` f4 re-provisions the fleet only after
      both features are `done` with review PASS, and keeps its original scope
      (P1b/P2/P3 use the new recovery path where it replaces a restart). The
      paused epic's [pause section](../fleet-degradation/epic.md#paused-product-gaps-before-further-testing-2026-09-12-user-decision)
      and [f4's banner](../fleet-degradation/f4-pool-degradation-under-load.md)
      are the resume anchors.

## Cost & process guardrails (while paused)

- **No cloud provisioning while paused.** The fleet is at **0 resources**
  (0 servers, 0 volumes; destroyed 2026-09-12). Nothing may be provisioned
  "to check" either fix — both are verified locally.
- **Observed fleet cost (user, 2026-09-12): the 3-node + volumes fleet lands
  close to €1/h** — roughly 10× the `vm-provision.sh` estimator table, which
  is a stale lower bound. Servers bill while they exist (powered on or off);
  every created resource bills a minimum 1-hour frame; volumes bill while
  they exist, detached included. Teardown is destroy-only. See PIPELINE §7.
- **No load/perf suite on the dev machine** (PIPELINE §6). This epic's
  verification is unit + node integration on temp-backed pools; fleet runs
  belong to the cloud harness and happen only after the epic is unpaused.
- **No destructive/provisioning script checks**; `--dry-run` only for script
  work.

## Non-goals (explicit, recorded)

- **No soft-degradation work.** `dm-flakey` error injection and `dm-delay`
  latency remain a follow-up outside this epic (fleet-degradation epic
  non-goal, unchanged).
- **No rework of f4's scope.** f4 keeps its original scenario matrix and
  assertions; the only delta is that its P1b/P2/P3 recovery uses the new
  runtime path where that path replaces a restart.
- **No test-only hooks.** Both fixes are operator-facing product behavior
  (a background task and an admin route) validated by product tests and, at
  f4 resume time, by the black-box fleet harness.
- **No C2a/C2b implementation.** pr1 supplies the fresh capacity data; the
  ring-weighting decision and any rebalance stay in `disk-resilience-capacity`.
- **No fleet provisioning or load testing while paused.**

## Cross-links

| Consumer | What this epic provides |
|---|---|
| [fleet-degradation f4](../fleet-degradation/f4-pool-degradation-under-load.md) | The pause removes two faulty substrates: f4's capacity dataset is no longer stale (pr1) and its P1b/P2/P3 recovery path no longer requires a restart (pr2). f4 resumes with its original scope. |
| [fleet-degradation f5](../fleet-degradation/f5-degraded-pool-semantics.md) | pr2's residue sweep feeds the corrected `storage_locations`/manifest state into f5's reconciliation accounting (`ReconciliationLoop`, live-copy/under-replication) so a returned-but-empty pool stops counting as a copy. |
| ADR-0029 §D3 | pr2 implements the "data pool returns → healthy; old segments GC'd; placement resumes" row at runtime; the amendment/new-ADR hand-off is recorded in [pr2](pr2-dead-pool-recovery.md). |
| ADR-0017 / ADR-0036 | pr1's ticker is scheduled product work (scheduler/budget seam is an implementer OQ); pr2's reset is operator-triggered and does not fight drain semantics. |
| `disk-resilience-capacity` | pr1's refreshed `oceanfs_pool_bytes_*` + manifest capacity are the input f4's C2a/C2b dataset consumes (instead of an SSH `df` probe). |

## Open Questions (epic-level)

| # | Question | Owner | Blocks |
|---|---|---|---|
| a | **pr2's ADR disposition**: ADR-0029 §D3 already names the data-pool return semantics, but the operator-confirmation requirement, probe gate, and residue/`storage_locations` correction before placement resumes are new normative detail. Decide at review whether this is an amendment to ADR-0029 §D3 (recommended, smallest surface) or a new short ADR, and record what it must state. Hand-off recorded in [pr2](pr2-dead-pool-recovery.md#adr-hand-off-recorded-not-written-here). | user + implementer (review) | pr2 close-out |
| b | **Resume mechanics**: f4's re-provisioning and the C2a/C2b dataset's exact consumption of the refreshed metrics (cross-link) are finalized when f4 un-pauses; no change to f4's scope. | implementer | f4 implementation start |

## Deviations (accepted)

_None yet — filled at feature close-out; per-feature deviations stay in the
feature docs._
