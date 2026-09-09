---
feature: "Intra-Node Drain (C1a — Sibling-Pool Mover)"
epic: "disk-resilience-scale"
status: done
priority: high
owner: ""
dependencies: ["d2-segment-relocation"]
adr: [0036, 0017, 0032, 0034]
perf: []
created: 2026-09-07
updated: 2026-09-09
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
  - on empty (no live **Reserved-or-Sealed** entry carries the source
    `pool_id` — re-enumerated immediately before the transition, closing
    the reserve→seal race; see Resolved Decisions 7), calls
    `PoolRegistry::set_pool_empty(pool_id)` → the pool becomes
    `Detachable`.
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
  - Sealer agreement: **resolved by d1's Deviation 3** — the f5 sealer
    refreshes its data-pool snapshot from the live registry per seal, so a
    pool that turns `Draining` never receives a new segment while a healthy
    sibling exists; the drain candidate set and the write path agree with
    no d3 change.
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
  - Adaptor/wiring: the Tier-1 adaptors live in
    `oceanfs-durability/src/scheduler/adaptors.rs` wrapping durability
    crate workers; the drain worker is storage-side and the node's
    composition root builds it (see Deviation b). Registered in the
    durability module builder (`crates/oceanfs-node/src/modules/durability.rs`)
    alongside the other tasks.
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
  task's source set); `pause`/`resume` set the pause flag. The route verbs
  shipped in d3 are `POST /admin/pools/{id}/drain[/pause|/resume]` (see
  the Interface section and Deviation e); d4 later adds the `cluster`
  mode.
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
| `oceanfs-storage` | Drain worker object `drain/intra_node.rs` (`IntraNodeDrain`, `IntraNodeDrainConfig`, `DrainCycleStats`, re-exported from the facade); `PlacementPolicy::select_data_pool_with_headroom` (`pool/placement.rs:242`); per-relocate `refresh_capacity()` in the worker; d1 `pool/drain.rs` gains `paused: bool` + `PoolRegistry::set_drain_paused`/`DrainState::is_paused` |
| `oceanfs-durability` | Drain Tier-1 adaptor `DrainIntraTask` in `scheduler/adaptors.rs` (name `"drain_intra"`, `keyspace_fraction() == 1.0`), exported through the scheduler facade; registered alongside the GC/orphan/scrub/AE tasks |
| `oceanfs-node` | Scheduler registration + worker wiring in `modules/durability.rs` (one `IntraNodeDrain` + one `DrainIntraTask`); `StorageModule` retains `Arc<oceanfs_storage::SegmentRelocator>` (built pre-trait-erasure at `modules/storage.rs:494`); `Node::drain_worker()` accessor (`node.rs:680`); admin begin-hook wiring `with_pool_drain_begin` (`modules/server.rs`) |
| `oceanfs-server` | Admin HTTP verbs `POST /admin/pools/{id}/drain[/pause|/resume]` (`admin.rs:777-779`, `#[cfg(feature="storage")]`) + `drain_paused` in `GET /admin/pools`; `AdminHandler::with_pool_drain_begin` builder |
| `oceanfs-core` | `[durability]` config fields `drain_max_bytes_per_tick` (256 MiB) + `drain_interval_sec` (1) in `config/durability.rs` |
| tests | New `crates/oceanfs-node/tests/intra_node_drain.rs` (3 scenarios); d1's `crates/oceanfs-node/tests/pool_drain_state.rs` boots with `drain_interval_sec = 3600` so the live d3 worker cannot race the d1 state assertions |

## Interface (Public API)

- `PlacementPolicy::select_data_pool_with_headroom(&self, registry:
  &PoolRegistry, exclude: &[u32], required_free: u64) ->
  Option<Arc<StoragePool>>` — sibling target pick for relocation
  (`oceanfs-storage/src/pool/placement.rs:242`): excludes the source +
  other `Draining` pools, requires `free_bytes >= required_free +
  MIN_FREE_HEADROOM_BYTES`, scores with the same weighted-least-free rule
  as `select_from_pools`.
- `pub struct IntraNodeDrainConfig { pub max_bytes_per_tick: u64 }`
  (`oceanfs-storage`, `Default = 256 MiB`) — the per-tick byte knob
  (ADR-0036 D5). The cadence knob (`drain_interval_sec`) is a separate
  `[durability]` config field passed to the adaptor, not a field on the
  worker config struct.
- `pub struct IntraNodeDrain` (`oceanfs-storage`, re-exported):
  `new(config, registry, lifecycle_registry, relocator)` and async
  `run_cycle(&self) -> DrainCycleStats`. Storage-side pure orchestration:
  registry enumeration + placement pick + d2 relocation. A no-draining /
  all-paused cycle is a no-op.
- `pub struct DrainCycleStats { pub segments_moved: u64, pub
  bytes_moved: u64, pub emptied: Vec<u32>, pub blocked: Vec<(u32,
  String)> }` — one cycle's outcome **aggregated over every draining
  pool** (see Deviation f; supersedes the single-pool sketch shape).
- `DrainIntraTask` (`oceanfs-durability`, exported via the scheduler
  facade): `DurabilityTask` impl with `name() == "drain_intra"`,
  `keyspace_fraction() == 1.0`; `new(Arc<IntraNodeDrain>, interval)`.
- `PoolRegistry::begin_drain` / `set_drain_blocked` / `set_pool_empty` /
  `drain_state` — consumed from d1 (no new registry surface unless the
  pause flag lives on the registry rather than the worker). **d3 pause
  addition (registry-level, on d1's record):** `paused: bool` on
  `DrainState::Draining`, `PoolRegistry::set_drain_paused(pool_id,
  paused) -> Result<(), DrainStateError>`, `DrainState::is_paused()`.
- `Node::begin_pool_drain` — d1 seam reused verbatim by the admin
  begin-hook (Deviation e); `Node::drain_worker() ->
  Arc<oceanfs_storage::IntraNodeDrain>` accessor (`node.rs:680`) for
  node-level tests and future admin progress surfaces.
- Config: `[durability] drain_max_bytes_per_tick` (default 256 MiB) +
  `drain_interval_sec` (default 1) in `oceanfs-core`
  (`config/durability.rs`); the byte budget lands in the storage worker's
  `IntraNodeDrainConfig`, the interval on the adaptor.
- Admin (`#[cfg(feature = "storage")]`):
  - `POST /admin/pools/{id}/drain` — begin intra-node drain; body `{}`
    or `{"mode": "intra-node"}` (empty body defaults intra-node);
    `"cluster"` mode → `501` (d4). Wired via
    `AdminHandler::with_pool_drain_begin`; the injected closure does
    registry `begin_drain` + manifest rebuild + `set_self_manifest`
    (mirrors `Node::begin_pool_drain`), so errors stringify textually.
  - `POST /admin/pools/{id}/drain/pause` ·
    `POST /admin/pools/{id}/drain/resume` — set/clear the registry pause
    flag; responses carry `{"pool_id", "drain_paused": bool}`.
  - `GET /admin/pools` per-pool entry gains `drain_paused` alongside
    `drain_state`, `drain_blocked`, `blocked_reason` (`admin.rs:1074`).

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

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage`,
      `oceanfs-node`, `oceanfs-durability` (+ `oceanfs-server` for the
      admin routes).
<!-- REVIEW: verified 2026-09-09 — `cargo build --all-targets` green for core/storage/durability/node/server AND the whole workspace (`cargo build --workspace --all-targets`); the relocator is built at the single DiskSegmentStore construction site (crates/oceanfs-node/src/modules/storage.rs:480-497, before trait erasure); DurabilityModule builds one IntraNodeDrain + one DrainIntraTask (modules/durability.rs:523-531); `Node::drain_worker()` accessor present (node.rs:618+). The d3 admin verbs are `#[cfg(feature="storage")]`; server builds with no-default-features fail at 3b01447 too (53-54 pre-existing errors, not a d3 regression). -->
- [x] **Tests:** `cargo test -p oceanfs-storage -p oceanfs-node -p
      oceanfs-durability --lib -- --test-threads=1` passes; the Scope
      scenario list is green — including the **no-destructive-failure**
      blocked test (no eligible sibling ⇒ pool stays `Draining`, reason
      surfaced, zero `.dat` deleted), the byte-budget tick stop/resume,
      pause/resume no-op cycles, Detachable transition on empty, and the
      integration scenario (live read load through a pool-0 drain; all
      `.dat` moved; `storage_locations` untouched; no restart).
<!-- REVIEW: verified 2026-09-09 — lib suites green single-threaded: storage 514, durability 280, node 100, server 245, core 232; storage doctests 111; node doctests 46; durability 30; server 14; core 65. Scope scenario list pinned by crates/oceanfs-storage/src/drain/intra_node.rs tests (8: sibling-drain+empty, byte-budget tick stop/resume, no-headroom park+no-delete, pause no-op+resume, two-draining-pools serialize under one global budget, Reserved-keeps-non-empty, ghost-sealed parks with reason, dead-source skipped) + placement.rs headroom tests (5) + drain.rs pause tests (4). Integration: crates/oceanfs-node/tests/intra_node_drain.rs 3/3 (scheduler-driven drain to Detachable under live reads; admin pause/resume; f8-attach→drain) and pool_drain_state.rs 3/3 (incl. blocked-drain no-delete + last-data-pool). No load/e2e suite run locally (PIPELINE §6). Residual LOW (non-blocking): d1's pool_drain_state.rs scenarios 1+2 now run with the live d3 worker ticking at 1s — there is a small latent window between `begin_pool_drain` and the sibling-drain/assertions where the worker could relocate pool-0 segments (observed stable 12/12 consecutive runs; recommend pausing the drain in those d1 scenarios if flakiness ever appears). -->
- [x] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes.
<!-- REVIEW: verified 2026-09-09 — `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` clean on core/storage/durability/node/server; `#![deny(missing_docs)]` present in oceanfs-storage lib.rs:17-25. New pub items carry examples/doctests (IntraNodeDrainConfig, DrainCycleStats, PlacementPolicy::select_data_pool_with_headroom, PoolRegistry::set_drain_paused, DrainState::is_paused); the IntraNodeDrain::new/run_cycle examples are `ignore`-tagged (non-gating per the Lint note). Doc-staleness LOW (non-blocking): task.rs:3 / modules/durability.rs:46,89,488 / node.rs:42 still say "four Tier-1" housekeeping tasks (now five with drain_intra). -->
- [x] **ADR:** ADR-0036 C1a (intra-node drain to siblings), D4 (Tier-1
      task; Tier-0 never blocked by drain), D5 (configurable per-task
      `max_bytes_per_tick`, no shared budget framework), D6 (blocked ⇒
      pool stays Draining + reason surfaced; empty ⇒ Detachable) satisfied;
      ADR-0017 (scheduled background task), ADR-0032 (relocation via the
      unified store lock — through d2), ADR-0034 (registry enumeration,
      never a disk scan) satisfied.
<!-- REVIEW: verified 2026-09-09 — ADR-0036 D4: drain registered as a Tier-1 DurabilityTask; every scheduled cycle acquires a housekeeping (Tier-1) permit in scheduler/engine.rs run_one_cycle:240, so Tier-0 repair is never gated behind the drain. ADR-0036 D5: own knob `durability.drain_max_bytes_per_tick` (default 256 MiB, oceanfs-core config/durability.rs:48) + `drain_interval_sec` (default 1); no shared byte-budget framework. ADR-0036 D6/ADR-0034: enumeration via `SegmentLifecycleRegistry::for_each` only (intra_node.rs:317); blocked ⇒ set_drain_blocked + pool stays Draining + nothing deleted (parks_when_no_sibling_headroom_and_deletes_nothing); empty ⇒ set_pool_empty→Detachable only when NO Reserved-or-Sealed entry carries the source pool_id (collect_live_entries includes Reserved; re-enumeration at intra_node.rs:272 closes the reserve→seal race before the empty transition); a Dead pool is never a source/target (drain.rs begin_drain DeadPool + draining_source_ids status check). ADR-0017 (scheduled task + keyspace_fraction 1.0 + assert_full) and ADR-0032 (relocation under the unified-store per-segment lock via d2 SegmentRelocator) satisfied. One worker instance serializes all draining pools under one global budget (DurabilityModule builds exactly one IntraNodeDrain + one DrainIntraTask; concurrent_cycles defaults false → per-task serial cycles). -->
- [x] **Perf:** frontmatter `perf: []`; prose constraints: enumeration is
      a registry `for_each` (in-memory, no disk scan); the byte budget
      bounds per-tick I/O; relocation batches share the per-segment lock
      discipline (d2); no per-segment allocation churn beyond the d2 copy
      path (pre-sized candidate vecs — perf rule 1.3).
<!-- REVIEW: verified 2026-09-09 — enumeration is registry-only (lifecycle.rs:793 `for_each`, in-memory shard walk); byte budget caps each tick's relocations; relocation I/O is bounded by d2's copy path under the per-segment lock; placement helper pre-sizes its candidate vec (`Vec::with_capacity(pools.len())`, placement.rs:253); the per-cycle candidate Vecs in run_cycle are once-per-cycle allocations, not per-segment churn; `refresh_capacity()` is invoked once per successful relocate (statvfs, cheap relative to the copy). -->
- [x] **Integration:** integration test at the node boundary exercises the
      complete attach → drain → `Detachable` workflow under live reads
      with no restart, and asserts no `.dat` is lost or left behind on the
      drained root. **No load suite is run locally** (PIPELINE §6).
<!-- REVIEW: verified 2026-09-09 — crates/oceanfs-node/tests/intra_node_drain.rs scenarios 1-3 exercise: (1) admin begin over HTTP + real scheduler-driven drain under a live GET reader, pool 0 root emptied, every `.dat` on the sibling, `storage_locations`/topology untouched (manifest still 5 pools), status stays Draining after Detachable, no restart; (2) pause = no-op cycles (manual + scheduler), resume completes to Detachable; (3) boot 1 pool → f8 attach → drain to Detachable. All 3 pass; the storage-side no-destructive-failure blocked test parks with `blocked_reason` surfaced and zero `.dat` deleted (intra_node.rs:530-555). No load suite invoked (PIPELINE §6). -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Resolved Decisions

Recorded 2026-09-09 at final spec close. Every question posed under "Open
Questions for the Implementer" is resolved; where the outcome is also
recorded under [Deviations (accepted)](#deviations-accepted), the
cross-reference is given.

1. **Drain order & blocked semantics.** Largest-first per source pool
   (unblocks source headroom fastest against the byte budget). When the
   largest remaining candidate fits **no** eligible sibling the pool is
   parked blocked — smaller segments are never moved first, because they
   cannot make the largest fit. Deterministic per-segment relocation
   failures (missing file, commit/store error) also park the pool with a
   surfaced reason; the reason clears on the next successful relocate.
   (Deviation c.)
2. **In-flight active segments on a newly-Draining pool.** Resolved by d1's
   Deviation 4 (documented, no code) plus the d3 emptiness rule: a segment
   that was already reserving/appending when the pool flipped finishes and
   seals on the source (its registry entry keeps `pool_id == source`) and
   the worker relocates it like any other sealed segment; a live Reserved
   entry keeps the pool non-empty until it seals and moves (see decision 7).
3. **Task-adaptor seam.** The drain worker object is storage-side
   (`oceanfs-storage/src/drain/intra_node.rs`, `IntraNodeDrain`) and the
   Tier-1 task is a thin `DrainIntraTask` adaptor in
   `oceanfs-durability/src/scheduler/adaptors.rs` (name `"drain_intra"`),
   registered in `modules/durability.rs`. The node retains an
   `Arc<oceanfs_storage::SegmentRelocator>` on `StorageModule`, built at
   the single `DiskSegmentStore` construction site **before** trait
   erasure — no raw concrete-store leak, no `SegmentDataStore` trait
   extension (d2's D3-2 resolution). (Deviation b.)
4. **Pause-flag ownership.** Registry-level, on the d1 drain record: d3
   added `paused: bool` to `DrainState::Draining`, plus
   `PoolRegistry::set_drain_paused` and `DrainState::is_paused()`;
   `GET /admin/pools` exposes `drain_paused`. This is an additive
   evolution of the d1 enum — d1 shipped the variant without the field and
   its doc records the addition. (Deviation a.)
5. **Concurrent drains on the same node.** One serialized worker per node
   drains **all** draining pools in pool-id order under a single global
   `max_bytes_per_tick` (a task per source pool would multiply I/O; the
   scheduler's `concurrent_cycles` default is false → per-task serial
   cycles). `DrainCycleStats` therefore aggregates every pool touched in
   the cycle. (Deviation f.)
6. **Config.** `[durability] drain_max_bytes_per_tick` (default 256 MiB)
   and `drain_interval_sec` (default 1) in `oceanfs-core`
   (`config/durability.rs`). The interval knob lives on the adaptor
   (`DrainIntraTask`), the byte budget in the storage worker's
   `IntraNodeDrainConfig`.
7. **Emptiness / reserve→seal race.** `PoolRegistry::set_pool_empty` fires
   only when **no** live Reserved-or-Sealed entry carries the source
   `pool_id`, with a re-enumeration immediately before the transition —
   closing the reserve→seal race where a just-reserved segment would seal
   onto a pool the worker already declared empty. (Deviation c's
   deterministic-failure surfacing covers the ghost-sealed entry case: it
   parks with a reason rather than silently detaching.)

## Deviations (accepted)

Recorded 2026-09-09 after implementation review (PASS, iteration 1) —
each validated by the stakeholder before/at implementation.

- **a. DrainState pause field (registry-level `paused: bool` on the
  `Draining` variant + `PoolRegistry::set_drain_paused`), extending d1's
  public enum.** Operator-visible pause belongs with the drain record the
  admin status surface already reads, so the worker and the operator see
  one source of truth; the field is an additive evolution of d1's variant
  (d1 doc updated to match).
- **b. Worker home + adaptor seam (storage-side worker; durability-crate
  adaptor; node retains `Arc<SegmentRelocator>` rather than the raw
  concrete store).** The worker stays pure storage orchestration beside the
  registry/placement/relocator primitives it consumes, the durability crate
  gets only a thin ADR-0017 adaptor, and the composition root builds the
  relocator at the single store-construction site before trait erasure — no
  `SegmentDataStore` trait extension and no concrete-store `Arc` through
  the scheduler facade.
- **c. Largest-first park semantics incl. deterministic per-segment
  failure parking.** Moving smaller segments first cannot make the largest
  remaining segment fit, so when the largest candidate fits no eligible
  sibling the pool parks blocked; deterministic per-segment failures
  (missing file / commit-store) surface through the same `blocked_reason`,
  a broader no-destructive-failure surface than "no eligible target", and
  the reason clears on the next successful relocate.
- **d. Per-cycle statvfs `refresh_capacity()` after each successful
  relocate.** Headroom picks need fresh free-space after each move because
  a cycle relocates bytes between pools; statvfs is cheap relative to the
  copy it follows.
- **e. HTTP begin seam mirrors `Node::begin_pool_drain` (registry
  `begin_drain` + manifest rebuild + `set_self_manifest`).** The begin
  route must re-gossip the `"draining"` manifest exactly like the d1 node
  seam; the local `ManifestCache` self-entry update remains deferred from
  d1's LOW note (d5). The begin route maps `DrainStateError` to HTTP
  status **textually** because the injected closure stringifies the error.
- **f. `DrainCycleStats` aggregate multi-pool shape (`emptied: Vec<u32>`,
  `blocked: Vec<(u32, String)>`).** One task serializes every draining pool
  per cycle, so the returned stats must aggregate pools — superseding the
  Interface sketch's single-pool `pool_empty`/`blocked_reason` fields.
- **g. d1 integration test interval override.** d1's
  `pool_drain_state.rs` boots with `drain_interval_sec = 3600` so the now
  live d3 worker cannot race the d1 state assertions between
  `begin_pool_drain` and scenario completion.
