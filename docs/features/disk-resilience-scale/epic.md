---
epic: "disk-resilience-scale"
status: in_progress
priority: high
created: 2026-09-07
updated: 2026-09-09
---

# Disk Resilience — Phase C: Cluster Capacity Operations — Epic Plan

Epic: `disk-resilience-scale`
ADR: [ADR-0036](../adr/0036-phase-c-scale-ops-drain.md)
Brainstorm: [disk-resilience-pools](../../brainstorm/disk-resilience-pools.md) (§7 rev. 3 addendum)
Depends on: Phase A ([disk-resilience](../disk-resilience/epic.md) — done), Phase B
([disk-resilience-healing](../disk-resilience-healing/epic.md) — code-complete),
and the 2026-09 review refactor substrate (ADR-0031…0035, store unification,
composition-root modules, durability scheduler).

## Goal

Make the **cluster's aggregate capacity mutable**: an operator can grow a node
(attach — Phase A f8), shrink it (drain a pool to siblings or off-node),
remove a pool (detach), and retire a whole node — all as paced, observable,
background operations on live nodes, with no data loss and no shutdown-path
terabyte streaming. This is the scale-ops half of the pool model that Phase A
(foundation) and Phase B (failure semantics & healing) deliberately left for
last.

The epic is deliberately bounded: **drain (intra-node C1a + cluster C1b),
detach, drain-state plumbing** — plus an optional late C2a. Proactive
rebalance (C2b), segment self-description (C3), and graceful-leave redesign
are excluded with recorded rationale in ADR-0036.

## Code-grounding facts (verified at HEAD `ca8e1c3`)

1. **No `detach` exists** — only `PoolRegistry::attach` (f8,
   `oceanfs-storage/src/pool/mod.rs:1297`); hot-swap currently requires a
   fresh root/name because the old entry cannot be removed
   (`runtime-attach.md` deviation).
2. **`pool_id` is immutable per segment.** Stamped at seal, folded into the
   event-WAL/checkpoint, cached per segment in the reader
   (`DiskSegmentReader::pool_id_cache`, f5). `MetadataRefreshEvent`
   (`oceanfs-storage/src/segment/lifecycle.rs`) currently carries merkle_root
   + `storage_locations` only — **no pool_id**. The unified store resolves a
   segment's pool from the lifecycle registry (`data_store.rs::resolve_pool`,
   registry-only).
3. **Graceful leave is `leave(None)` + peer healing** (`membership/manager.rs`,
   `Node::shutdown`); the leave-handler streaming was deleted in the refactor.
   This is intended; node retirement is served by a paced drain *before*
   leave, not by changing leave.
4. **The C1b mover already exists**: ADR-0030 target-pull —
   `RequestReReplication` RPC, `ReRepWorker` (target fetches + writes +
   registers + stamps), `ManifestRepairTargetSelector` (capacity-aware, node
   `repair.rs`). `storage_locations` payload is a **set replacement**, so
   source-release (self-removal) is expressible via refresh.
5. **Reconciliation counts holders from `storage_locations`** (g4
   `reconcile.rs`, HolderIndex), so a source that releases itself stops being
   counted immediately after the durable refresh.
6. **Background loops are scheduled** (ADR-0017): `DurabilityTask` +
   two-tier `DurabilityBudget`; Tier-0 = repair/heal/re-rep/hint-apply, Tier-1
   = housekeeping. Drain is Tier-1.
7. **Bounded-metadata discipline** (ADR-0034): drain enumerates the lifecycle
   registry (per-segment), never a disk/object scan.
8. **Composition root is modular**: new wiring lands in
   `oceanfs-node/src/modules/*` (storage/durability/background builders), not
   `node.rs`.

## Feature DAG

```
d0 regression-gate
 └── d1 pool-drain-state
      ├── d2 segment-relocation
      │    ├── d3 intra-node-drain       (C1a — sibling-pool mover)
      │    └── d4 cluster-drain          (C1b — off-node mover + source-release)
      └── d5 detach-and-drop             (inverse of f8)
 d4 ──→ (optional, late) d6 capacity-weighted-ownership (C2a)
```

Implementation order: **d0 → d1 → d2 → d3 → d4 → d5**, then evaluate d6.
d6's evaluation (its only deliverable in this epic) completed 2026-09-09 →
**deferred**; see the [Status & disposition](#status--disposition-2026-09-09)
note below.

| # | Feature | Status | Doc | Touches | Depends on | Notes |
|---|---|---|---|---|---|---|
| d0 | `regression-gate` | done | [d0-regression-gate.md](d0-regression-gate.md) | workspace | — | **Pre-epic baseline fix + gate**: fix the seven documented pre-existing failures to zero (decision 2026-09-07), then full green gate (crates lib + node integration + quick e2e allowlist, `--test-threads=1`, PIPELINE §4.6/§6); clippy/fmt/rustdoc clean; green baseline recorded |
| d1 | `pool-drain-state` | done | [d1-pool-drain-state.md](d1-pool-drain-state.md) | storage, core | d0 | Registry `Draining` state; placement exclusion; read-while-draining; health monitor ignores Draining; admin status + `oceanfs_pool_drain_blocked_reason` |
| d2 | `segment-relocation` | done | [d2-segment-relocation.md](d2-segment-relocation.md) | storage | d1 | Durable `pool_id` mutation (MetadataRefreshEvent optional section, ADR-0030-style decode); copy→commit→unlink under per-segment write lock; reader-cache purge; GC/reaper/crash-window interplay |
| d3 | `intra-node-drain` | done | [d3-intra-node-drain.md](d3-intra-node-drain.md) | storage, node | d2 | C1a worker (DurabilityTask Tier-1): registry-enumerated relocation to placement-chosen sibling pools; configurable `max_bytes_per_tick`; blocked-state logic |
| d4 | `cluster-drain` | done | [d4-cluster-drain.md](d4-cluster-drain.md) | node, durability, storage | d2, d3 (mover reuse) | C1b controller: drain pool-only and node-level; target via existing selector/RPC; **source-release** (holder-set refresh minus self + local unlink); admin drain API; paced/pausable/terminal; reconciliation interaction |
| d5 | `detach-and-drop` | done | [d5-detach-and-drop.md](d5-detach-and-drop.md) | storage, node | d3 (empty precondition) | `PoolRegistry::detach` on empty pool; config/topology drop; manifest rebuild + re-gossip; no restart |
| d6 | `capacity-weighted-ownership` (C2a) | deferred | [d6-capacity-weighted-ownership.md](d6-capacity-weighted-ownership.md) | membership, routing, node | d4 (measure difficulty) | **Optional/late — DEFERRED 2026-09-09 (not done)**: difficulty measurement complete; C2a judged same redistribution as C2b at another granularity → backlog `disk-resilience-capacity`; C2b-only decision gated on fleet/load-test data |

Feature-spec documents exist for every DAG row (d0–d6) under this directory;
each carries its own Definition of Done. The **epic DoD below is the
acceptance bar** for the whole phase — an individual feature closing green
does not close the epic until the epic DoD holds.

### Explicit non-goals (recorded, not code)

- **C2b proactive rebalance** → backlog (`disk-resilience-capacity`); now
  joined by deferred C2a (d6, 2026-09-09) — C2a and C2b are the same
  redistribution functionality at two granularities, so only one of the two
  will ever be built; decision gated on fleet/load-test data (see [Status &
  disposition](#status--disposition-2026-09-09)).
- **C3 segment self-description** → dropped (logically wrong after g8; ADR-0036 §D1).
- **Graceful-leave redesign / shutdown streaming** → dropped (leave stays `leave(None)`).
- **Migration-plane isolation** (ADR-0030 D4) → remains a recorded future consequence; not in this epic.
- **Fleet/load-test phase-4 degraded-mode scenarios** → harness epic, not this epic.

## Admin surface (sketch)

- `POST /admin/pools/{id}/drain` — begin intra-node (C1a) or cluster (C1b) drain
  (target mode parameter; pool-only retirement allowed even when the node has one
  data pool — the mover is off-node).
- `POST /admin/nodes/{node}/drain` (or `POST /admin/nodes/{node}/retire`) — C1b
  node-level drain-to-empty, run before `leave`; node keeps serving until empty.
- `POST /admin/pools/{id}/drain/pause` · `resume` — pacing control.
- `POST /admin/pools/{id}/detach` — accepted only on an empty (`Detachable`) pool.
- Status: pool `Draining`/`Detachable`/`Draining(blocked: reason)`; per-pool progress.

Exact verbs/routes are for the spec-writer to finalize in d4/d5.

## Status & disposition (2026-09-09)

**The DAG is code-complete — d0–d5 done, d6 deferred-with-findings.** The
epic frontmatter `status` is therefore `in_progress` (code-complete with
recorded residuals), **not `done`**: the remaining recorded residuals below
are genuinely open or carried, so the epic-level DoD does not yet hold.

### What closed

- d0–d5 are `done` (regression gate green; pool drain state; segment
  relocation; intra-node drain; cluster drain; detach-and-drop). Each feature
  doc records its review PASS + accepted deviations.

### Recorded residuals (carried / recorded, not silently dropped)

- **d6 (C2a) — deferred.** The difficulty measurement completed and the
  stakeholder deferred the feature to the backlog epic
  `disk-resilience-capacity`. Grounded finding: making ring share track
  capacity is ownership redistribution (C2b-class machinery — metadata row
  migration/chase) or a data-plane redesign, precisely the ADR-0028
  disruption ADR-0036 D1's deferral clause anticipates; the ring is the only
  metadata-discovery index and ring membership is quasi-static by design. The
  C2a-vs-C2b decision (only one of the two will ever be built) is gated on
  the pending fleet/load-test data; the deferred feature doc retains the full
  record for if/when the C2b attempt is made. **Carried** — the feature doc's
  `status` is `deferred`, its DoD items remain the record for the backlog
  attempt, not closed.
- **d4 `oceanfs_drain_*` throughput counters — recorded, not closed.** d4
  Deviation (e) accepted that `oceanfs_drain_dispatched_total` /
  `oceanfs_drain_released_total` / `oceanfs_drain_remaining{source}` were not
  registered (`DrainCycleStats` is the observable); its wording — "close or
  document before the epic DoD" — means the counter registration remains a
  recorded, unclosed residual carried to the backlog, not silently dropped.
- **Epic-DoD wording residuals.** Two epic-DoD acceptance items are not fully
  exercised as worded and are recorded, not dropped: (1) the "no-op
  `leave(None)`" ending of the cluster-drain DoD bullet was not exercised by
  the integration tests (node shutdown used instead — recorded in d4's
  Integration REVIEW, non-blocking); (2) any DoD language that still reads as
  if d6/C2a would land in this epic is superseded by the deferral record in
  the d6 feature doc and this disposition note.

### Backlog ownership

The C2b/C2a decision and the deferred d6 attempt belong to the
`disk-resilience-capacity` backlog epic, together with the d4 counter-close
residual. This epic is **not** marked `done`; it is left as code-complete
with recorded residuals so the carry-over is explicit.

## Acceptance bar (epic DoD)

- [ ] ADR-0036 D1–D7 implemented: drain-state plumbing; durable `pool_id`
      relocation (event-WAL only writer); intra-node drain to siblings; cluster
      drain (pool-only + node-level) via ADR-0030 target-pull with source-release;
      detach on empty; no-destructive-failure blocked semantics; per-task byte
      budgets configurable.
- [ ] A 2-data-pool node drains pool 0 to pool 1 under live load: GETs keep
      serving, `.dat` all moved, manifest 2→1 pools after detach, no restart.
- [ ] A node with a single data pool (or a targeted pool) cluster-drains to other
      nodes: copies land on capacity-aware targets, source removes itself from
      `storage_locations`, reconciliation stops counting it, pool/node detaches;
      zero data loss; `leave(None)` afterwards is a no-op drain.
- [ ] Drain-blocked behavior: no eligible target anywhere → pool stays Draining,
      blocked reason surfaced, nothing deleted.
- [ ] Crash-window tests: copy→commit→unlink interruptions leave either source or
      target authoritative; no data loss, no orphaned-then-lost copy.
- [ ] All Phase A/B suites stay green (regression) — d0's pre-epic fix list
      closed to zero and the green baseline recorded; clippy/fmt/rustdoc
      clean across affected crates.

## Crate/DAG notes

- New `pool_id` payload lives in `oceanfs-storage::segment::lifecycle`
  (event-WAL family); node orchestrates. No new proto for the intra-node half
  (`pool_id` is node-local; `storage_locations` never references pools).
- C1b reuses the durability crate's repair RPC/worker surface and the node
  repair dispatcher/selector seams; the new "source-release" primitive is
  specified in d4 with its event-WAL fold, reconciliation, GC and residue
  implications.
- DAG constraint check: no cycle introduced — storage exposes the relocate
  primitive and drain state; node/durability consume them through existing
  trait seams (ADR-0005 pattern).
