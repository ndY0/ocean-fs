---
feature: "Intra-Node Drain (C1a — Sibling-Pool Mover)"
epic: "disk-resilience-scale"
status: proposed
priority: high
owner: ""
dependencies: ["d2-segment-relocation"]
adr: [0036, 0017, 0032, 0034]
perf: []
created: 2026-09-07
updated: 2026-09-07
---

# Intra-Node Drain (C1a — Sibling-Pool Mover)

## Summary

ADR-0036 C1a: a paced **Tier-1 `DurabilityTask`** that empties a `Draining`
data pool by relocating its sealed segments to **sibling data pools on the
same node** (no cluster traffic, `storage_locations` untouched). Given a
`Draining` pool on a node with ≥ 2 eligible data pools, the worker walks
the node's lifecycle registry for entries whose `pool_id` is the source
(**never a disk scan** — ADR-0034), relocates each via d2's
`SegmentRelocator` to a placement-chosen sibling (reusing
`PlacementPolicy` scoring with a Draining-exclusion + headroom filter), is
paced by a configurable `max_bytes_per_tick` (ADR-0036 D5 — its own knob,
not a shared budget framework), parks with a **blocked reason** when no
sibling fits (nothing deleted, pool stays `Draining` — ADR-0036 D6), and on
empty transitions the pool to `Detachable` (d5's precondition). The
workflow this feature documents end-to-end: **attach a sibling (f8) →
drain → detach**. The worker object and its scheduler adaptor follow the
ADR-0017 pattern (GC/orphan/scrub/AE adaptors in
`oceanfs-durability/src/scheduler/adaptors.rs`); node wiring + the
start/pause/resume admin surface land in the composition root.

## Scope

### In Scope

- **Drain worker object** (storage-side, pure orchestration; e.g.
  `oceanfs-storage/src/drain/intra_node.rs` or a node-side worker — the
  split follows the epic crate note: *storage exposes the relocate
  primitive and drain state; node/durability consume them through existing
  trait seams*, ADR-0005 pattern):
  - enumerates the **lifecycle registry** for sealed entries with
    `pool_id == source` (`SegmentLifecycleRegistry::for_each`,
    `crates/oceanfs-storage/src/segment/lifecycle.rs:793`); no disk scan
    (ADR-0034);
  - per tick, relocates entries via `SegmentRelocator::relocate`
    (d2) to a sibling chosen by placement;
  - tracks bytes moved against `max_bytes_per_tick` and stops the cycle at
    the budget;
  - reports blocked when no eligible sibling exists (see the placement
    filter below): `PoolRegistry::set_drain_blocked(pool_id, reason)`
    (d1), nothing deleted, pool stays `Draining`;
  - on empty (registry holds no sealed/unsealed entry with the source
    `pool_id`), calls `PoolRegistry::set_pool_empty(pool_id)` → the pool
    becomes `Detachable`.
- **Sibling selection: reuse `PlacementPolicy`, don't invent a selector.**
  The sketch's key decision is settled: reuse the policy with a filter.
  - The worker lists the node's data pools, excludes the source (and any
    other `Draining` pool), and scores the rest with the *same*
    `max free/weight` rule as `PlacementPolicy::select_from_pools`
    (`crates/oceanfs-storage/src/pool/placement.rs:168-196`). Add one
    headroom-aware helper so the scoring lives in one place:
    `PlacementPolicy::select_data_pool_with_headroom(&registry,
    exclude: &[u32], required_free: u64) -> Option<Arc<StoragePool>>` —
    candidate must have `free_bytes >= required_free +
    MIN_FREE_HEADROOM_BYTES` (segment `total_bytes` comes from the
    registry entry, `SegmentMetadata.total_bytes`). No eligible candidate
    with enough headroom ⇒ **blocked** (reason: no sibling capacity).
  - `<!-- TODO(spec): verify anchor -->` confirm whether the sealer's
    pool snapshot needs the same Draining filter or whether per-seal
    registry reads already exclude (d1's open question; if a snapshot is
    used, the drain candidate set and the write path must agree).
- **`DurabilityTask` integration (ADR-0017).**
  - The drain runs as a Tier-1 (housekeeping) task under the durability
    scheduler: it implements/adapts `DurabilityTask`
    (`crates/oceanfs-durability/src/scheduler/task.rs:50`), each
    `run_cycle(KeyspaceWindow::Full)` processes up to
    `max_bytes_per_tick` of relocation and returns the number of segments
    moved (the scheduler acquires a Tier-1 `DurabilityBudget` permit per
    cycle — `scheduler/budget.rs:72`; Tier-0 repair/heal is never blocked
    by a drain, ADR-0036 D4/D5). A task whose source pool is not draining,
    or whose drain is paused, is a no-op cycle.
  - Adaptor/wiring: the four existing Tier-1 adaptors live in
    `oceanfs-durability/src/scheduler/adaptors.rs` wrapping durability
    crate workers; a drain adaptor either joins them (worker object
    exposed through the durability crate facade, like `GarbageCollector`)
    or node implements `DurabilityTask` directly (node already depends on
    durability for `RepairTargetSelector`). **Implementer decision** on
    the seam — record in Deviations. Registered in the durability module
    builder (`crates/oceanfs-node/src/modules/durability.rs`) alongside
    the other tasks.
- **Config (ADR-0036 D5 — per-task knob, no shared byte-budget
  abstraction).** New config fields in the scheduler/drain config family:
  - `drain_max_bytes_per_tick` (default e.g. 256 MiB — a bounded tick so a
    large pool drains over minutes/hours without starving the node's I/O);
  - drain interval/cadence rides the scheduler interval pattern (the task
    `interval()` may be a coarse e.g. 1 s; the byte budget is the real
    pace-setter);
  - per-feature own knob, **not** a shared framework — if a second
    consumer appears the shared helper is factored then (ADR-0036 D5).
  - Start/pause/resume control: the worker checks a pause flag per cycle;
    pausing stops new relocations between ticks without aborting a
    mid-`relocate` (which holds the per-segment lock until commit+unlink —
    d2's invariant).
- **Blocked-state logic (ADR-0036 D6).** No eligible sibling with headroom
  anywhere on the node ⇒ `set_drain_blocked(reason)`; the pool stays
  `Draining`; a later capacity change (attach, GC freeing space, another
  drain finishing) clears the blocked reason and the next cycle retries.
  Nothing is deleted and nothing is half-removed.
- **Admin start surface.** `POST /admin/pools/{id}/drain` with target mode
  `intra-node` begins the drain (marks `Draining`, d1, and enables the
  task's source set); `pause`/`resume` set the pause flag. Exact verbs are
  finalized across d3/d4 (the epic leaves route wording to d4/d5 — d3 adds
  the intra-node mode and the pause/resume controls).
- **Workflow the feature documents end-to-end:** attach a sibling via f8
  (`POST /admin/pools`, `runtime-attach.md`) → `POST /admin/pools/{id}/drain`
  (mode intra-node) → observe progress → pool `Detachable` → d5 detach.
- Tests:
  - unit: registry enumeration returns exactly the sealed entries with the
    source `pool_id` (and excludes entries on other pools);
  - unit: sibling selection excludes the source + other draining pools,
    respects the required-headroom rule, and falls back through the same
    tie-break as `select_data_pool`;
  - unit: a tick stops at `max_bytes_per_tick` (bytes moved ≤ budget) and
    resumes at the next cycle;
  - unit: blocked behavior — single-node, one draining data pool with an
    only-sibling too full ⇒ worker parks, `blocked_reason` set, pool stays
    `Draining`, **no `.dat` deleted** (no-destructive-failure scenario);
  - unit: on empty, `set_pool_empty` transitions the pool to `Detachable`;
  - unit: pause flag makes a cycle a no-op; resume continues;
  - integration (local node, 2 data pools): write objects so segments
    spread on both pools → drain pool 0 under **live read load** → GETs of
    pool-0 objects keep serving throughout (read-while-draining via d1) →
    all pool-0 `.dat` move to pool 1 (`.dat` set empty on root 0) →
    `storage_locations` untouched (single-node, no cluster traffic) →
    pool becomes `Detachable` → manifest shows the state → no restart.

### Out of Scope (for this feature)

- Cluster drain (d4/C1b) — off-node mover, source-release, node-level
  drain, the controller + drain RPC/reason questions; d4 reuses this
  feature's mover pattern for the copy half but is its own controller.
- Detach (d5) — this feature *produces* the `Detachable` precondition.
- Relocation primitive (d2) and drain-state plumbing (d1) — consumed.
- Ring re-weighting (d6/C2a), proactive rebalance (C2b), segment
  self-description (C3), graceful-leave redesign — epic non-goals.
- Migration-plane isolation (ADR-0030 D4) — recorded future consequence,
  not implemented.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | Drain worker object (registry enumeration + tick budget + blocked/empty logic); `PlacementPolicy::select_data_pool_with_headroom` (or equivalent) |
| `oceanfs-durability` | Drain Tier-1 adaptor implementing `DurabilityTask` (if the adaptor seam is chosen — otherwise node implements the trait) |
| `oceanfs-node` | Scheduler registration + worker wiring (`modules/durability.rs`); admin start/pause/resume routes (intra-node mode); config knobs (`drain_max_bytes_per_tick`) |
| `oceanfs-server` | Admin HTTP surface additions for the intra-node drain verbs (with the node's composition root) |

## Interface (Public API)

- `PlacementPolicy::select_data_pool_with_headroom(&self, registry:
  &PoolRegistry, exclude: &[u32], required_free: u64) ->
  Option<Arc<StoragePool>>` — sibling target pick for relocation.
- `pub struct IntraNodeDrainConfig { pub max_bytes_per_tick: u64, pub
  interval: Duration, … }` — per-task knob (ADR-0036 D5).
- Drain worker: `run_cycle(registry…) -> DrainCycleStats { segments_moved,
  bytes_moved, blocked_reason: Option<String>, remaining: usize,
  pool_empty: bool }` (the storage-side object the Tier-1 adaptor wraps).
- `DurabilityTask` impl (name `"drain_intra"`): `run_cycle` delegates to
  the worker and returns `segments_moved`.
- `PoolRegistry::begin_drain` / `set_drain_blocked` / `set_pool_empty` /
  `drain_state` — consumed from d1 (no new registry surface unless the
  pause flag lives on the registry rather than the worker).
- Admin: `POST /admin/pools/{id}/drain` (mode `intra-node`),
  `POST /admin/pools/{id}/drain/pause`, `POST /admin/pools/{id}/drain/resume`.
  `<!-- TODO(spec): verify anchor -->` exact route verbs are for d4/d5 to
  finalize against the existing admin router
  (`crates/oceanfs-server/src/admin.rs`, attach at `:899`); d3 implements
  the intra-node mode + pause/resume semantics.

## Data Flow

```
operator ──▶ POST /admin/pools/{id}/drain (mode intra-node)
   └─ PoolRegistry::begin_drain(pool_id) → status Draining (d1) → manifest "draining"
   └─ scheduler runs the Tier-1 drain task each interval (budget permit)
        └─ run_cycle:
             enumerate lifecycle registry (for_each) → sealed entries with pool_id == source
             loop while bytes_moved < max_bytes_per_tick:
                 target = PlacementPolicy::select_data_pool_with_headroom(exclude=[source,…])
                 ├─ None → set_drain_blocked("no sibling with headroom") → park; nothing deleted
                 └─ Some(pool) → SegmentRelocator::relocate(id, pool)   (d2: copy→commit→unlink)
             pool empty? → set_pool_empty(pool_id) → Detachable → d5 may detach
progress: bytes/segments remaining per pool; blocked reason surfaced; reads served throughout
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage`,
      `oceanfs-node`, `oceanfs-durability` (+ `oceanfs-server` for the
      admin routes).
- [ ] **Tests:** `cargo test -p oceanfs-storage -p oceanfs-node -p
      oceanfs-durability --lib -- --test-threads=1` passes; the Scope
      scenario list is green — including the **no-destructive-failure**
      blocked test (no eligible sibling ⇒ pool stays `Draining`, reason
      surfaced, zero `.dat` deleted), the byte-budget tick stop/resume,
      pause/resume no-op cycles, Detachable transition on empty, and the
      integration scenario (live read load through a pool-0 drain; all
      `.dat` moved; `storage_locations` untouched; no restart).
- [ ] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes.
- [ ] **ADR:** ADR-0036 C1a (intra-node drain to siblings), D4 (Tier-1
      task; Tier-0 never blocked by drain), D5 (configurable per-task
      `max_bytes_per_tick`, no shared budget framework), D6 (blocked ⇒
      pool stays Draining + reason surfaced; empty ⇒ Detachable) satisfied;
      ADR-0017 (scheduled background task), ADR-0032 (relocation via the
      unified store lock — through d2), ADR-0034 (registry enumeration,
      never a disk scan) satisfied.
- [ ] **Perf:** frontmatter `perf: []`; prose constraints: enumeration is
      a registry `for_each` (in-memory, no disk scan); the byte budget
      bounds per-tick I/O; relocation batches share the per-segment lock
      discipline (d2); no per-segment allocation churn beyond the d2 copy
      path (pre-sized candidate vecs — perf rule 1.3).
- [ ] **Integration:** integration test at the node boundary exercises the
      complete attach → drain → `Detachable` workflow under live reads
      with no restart, and asserts no `.dat` is lost or left behind on the
      drained root. **No load suite is run locally** (PIPELINE §6).

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

- **Drain order.** The sketch asks: largest-segments-first vs.
  oldest-first for bounded-tick usefulness. Largest-first makes progress
  visible fastest and unblocks headroom sooner on the source; oldest-first
  is fairer to long-lived segments. Recommend largest-first per tick
  (fewer ticks, faster empty), but measure against the byte budget — 
  record the choice.
- **In-flight active segments on the newly-Draining pool.** d1 guarantees
  new reservations never target a Draining pool; a segment that was
  already appending when the pool flipped cannot be re-homed mid-life.
  Recommended (from d1): let it finish and seal; the sealed entry then has
  `pool_id == source` and the worker relocates it. Confirm and record.
- **Task-adaptor seam.** Durability's Tier-1 adaptors wrap
  durability-crate workers; the drain worker needs placement + relocator
  (storage) and the registry. Decide whether the drain worker object is
  exposed through the durability facade (adaptor joins
  `scheduler/adaptors.rs`) or node implements `DurabilityTask` directly
  (node depends on durability already). Record in Deviations.
- **Pause-flag ownership.** Registry-level (visible to admin status, d1)
  vs. worker-level (simplest). If admin status must reflect pause state,
  the flag belongs where `drain_state` lives.
- **Concurrent drains on the same node.** Two data pools draining at once
  (pool-only retirement of two disks) — the shared Tier-1 budget + per-pool
  byte knobs must not double-oversubscribe the node's I/O. Default:
  serialize intra-node drains on one worker task (a task per source pool
  would multiply; the epic does not require parallel pool drains).

## Deviations (accepted)

None yet — this document is proposed. Expected-deviation candidates: the
worker/adaptor seam, the drain-order rule, and the in-flight-segment rule
(see Open Questions). Record each with its resolution at implementation.
