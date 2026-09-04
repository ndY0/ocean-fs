---
feature: "f3: Keyspace Fraction for GC + Orphan Reaper"
epic: "refactoring/durability-scheduler"
status: proposed
priority: medium
owner: ""
dependencies:
  - feature: f1-durability-task-trait
    epic: refactoring/durability-scheduler
    reason: The trait carries keyspace_fraction() and the KeyspaceWindow the scheduler passes per cycle
  - feature: f2-scheduler
    epic: refactoring/durability-scheduler
    reason: The scheduler owns the per-task round-robin cycle_index cursor that would feed Shard windows
  - feature: c2-split-durability-builder
    epic: refactoring/composition-root-decomposition
    reason: Registration values for gc/orphan (fraction) are decided here and wired in f4
  - feature: f2-accounting-liveness
    epic: refactoring/bounded-metadata-scans
    reason: "HARD GATE: naive keyspace sharding over the current whole-CF scans (GC list_objects_all_with_bucket, reaper list_objects_all) would multiply O(objects) passes. ADR-0034 must land first so GC/orphan iterate bounded structures; this feature then ships the fraction mechanism with keyspace_fraction=1.0 until the bounded substrate is proven. Roadmap wave 2 ⑥ before ③-f3."
adr:
  - 0017-durability-task-abstraction
  - 0032-unify-segment-data-access
  - 0025-segment-lifecycle-state-machine
perf:
  - "1.1 avoid O(n) full-store materialization in cycles where a bounded pass suffices"
  - "4.2 bound background scan I/O (no new whole-store passes per cycle)"
created: 2026-09-04
updated: 2026-09-04
---

# f3: Keyspace Fraction for GC + Orphan Reaper

## Summary

ADR-0017 §3 proposes keyspace-fraction round-robin for GC and the orphan
reaper so a "10% per cycle" pass smooths the periodic GC spike (finding #20).
This feature verifies that proposal against today's scan shape, ships the
scheduler-side **mechanism** (from f1/f2: `keyspace_fraction()` +
per-task `cycle_index` rotation feeding `KeyspaceWindow::Shard`), and then
**keeps GC and the orphan reaper at `keyspace_fraction() == 1.0` (full pass)**
because their dominant cost today is a whole-store object/tombstone scan that
cannot be range-limited through the current `MetadataStore` API. Naively
sharding the segment iteration would multiply the O(objects) scans per
unit-time — strictly worse. The O(n) object-list problem flagged in the
review at `gc/orphan_reaper.rs:297` is recorded as a hard constraint: this
feature MUST NOT make it worse.

## Today's scan shape (verified 2026-09-04)

| Task | Cycle cost today | Source |
|---|---|---|
| GC | `process_tombstones`: full `registry.for_each` (register every segment) + `metadata.list_tombstones_all()` + `metadata.list_objects_all_with_bucket()` — all whole-CF, materialized `Vec`s in memory | garbage_collector.rs:453-545 |
| Orphan reaper | `build_referenced_set`: `metadata.list_objects_all()` — **every object row** (`[review][architectural][high]` block at orphan_reaper.rs:297-300) + full `registry.for_each` + `store.list_segment_files()` (on-disk sweep) | orphan_reaper.rs:120-176, 294-313 |

The `MetadataStore` trait in `oceanfs-storage-api` (metadata_store.rs:78)
exposes only whole-CF scans — `list_objects_all`,
`list_objects_all_with_bucket`, `list_tombstones_all` — with **no key-range
or per-segment scan method**. The segment set lives in the ADR-0025
`SegmentLifecycleRegistry` (in-memory, internally sharded 64 ways by hashed
`SegmentId`, `lifecycle.rs:409`), but GC's liveness computation and the
reaper's referenced-set build cross-reference the *entire* objects CF: a
segment's liveness/reference state is determined by object rows anywhere in
the keyspace. There is no segment→objects index.

**Consequence:** ADR-0017's example ("GC `keyspace_fraction = 0.1` → 10×
more frequent, 1/10th the cost per cycle") does not hold on today's shape.
Slicing only the segment iteration to 10% still requires the full
`list_objects_all_with_bucket` scan every cycle to attribute dead/live bytes,
so 10× frequency = ~10× the O(objects) liveness work, plus 10× the
`list_tombstones_all` scans. That is the exact "make it worse" trap this
feature exists to avoid.

## Scope

### In Scope
- Verify and document the scan shape above in the `scheduler` module docs and
  in the `GcTask`/`OrphanTask` adaptor doc comments (f1 types).
- Land the scheduler-side mechanism **ready but inert**:
  - `DurabilityTask::keyspace_fraction()` and
    `DurabilityScheduler`'s round-robin `cycle_index` cursor (f1/f2) remain;
  - `GcTask` and `OrphanTask` return `keyspace_fraction() == 1.0` and their
    `run_cycle` **assert** `KeyspaceWindow::Full`, returning a clear
    `Error::Internal` for any `Shard` window (a wiring bug must fail loudly,
    not silently run an unsharded scan labeled as sharded);
  - a `pub(crate)` guard so the f4 wiring cannot configure a fraction < 1.0
    for GC/orphan through a type-level or construction-time check.
- Regression pins: under the scheduler (Full window), GC and the reaper
  produce byte-identical `segments_scanned`/`orphans_found`/`dead_bytes`
  results to today's direct `run_cycle` calls on the same store.
- Record the follow-up constraint in the scheduler module docs: sharding GC
  or the reaper requires either (a) a segment-scoped/range-scan metadata API
  (`MetadataStore` range methods or a segment→objects index) or (b) a
  segment→tombstone/object index built incrementally — both are future work
  and must be designed as their own features (roadmap wave 5 "adaptive
  full-scan strategies" is the natural home).

### Out of Scope (for this feature)
- Adding range-scan methods to `MetadataStore` or building a
  segment→objects index (follow-up; not this epic).
- Setting `keyspace_fraction < 1.0` for any real task in production config
  (would be a no-op or an error by the guard).
- Sharding scrub or AE (scrub partitions by alive nodes — H5, not keyspace
  fraction; AE's sampling mode is its own ADR-0015 model — see README).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | `GcTask`/`OrphanTask` fraction values + Full-window asserts in `scheduler/adaptors.rs`; scan-shape analysis in module docs; no new public types |

## Interface (Public API)

No new public API in this feature. Behavior contract (public semantics):

- `GcTask::keyspace_fraction() -> f64 == 1.0`
- `OrphanTask::keyspace_fraction() -> f64 == 1.0`
- `GcTask::run_cycle(KeyspaceWindow::Full, …)` == today's
  `GarbageCollector::run_cycle(metadata, &registry)`; `Shard` → `Err`.
- `OrphanTask::run_cycle(KeyspaceWindow::Full, …)` == today's
  `OrphanReaper::run_cycle()`; `Shard` → `Err`.
- The scheduler rotation cursor (f2) remains fully implemented and unit-tested
  via mock tasks so the mechanism is exercised end-to-end before any real
  task opts in.

## Data Flow

```
scheduler per-task loop (f2)
  │  cycle_index = 0,1,2,…
  │  fraction = task.keyspace_fraction()     // 1.0 for GcTask/OrphanTask
  │  window = Full                            // total = 1
  ▼
GcTask::run_cycle(Full)
  → assert Full
  → gc.run_cycle(metadata, &registry)          // unchanged full-space pass
  → GcStats → segments_scanned                 // unchanged counts
```

```
(future, gated — NOT in this epic)
  fraction < 1.0 ⇒ window = Shard{index: cycle_index % total, total}
  ⇒ requires MetadataStore range/index support to bound the object scan
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-durability`.
- [ ] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      (PIPELINE.md §4.6) passes, adding:
      - `GcTask`/`OrphanTask` return 1.0 fraction and reject
        `KeyspaceWindow::Shard` with `Error::Internal`;
      - behavior-preservation pin: running `GcTask::run_cycle(Full)` and
        `OrphanTask::run_cycle(Full)` over a shared test RocksDB store yields
        the same `segments_scanned`/`dead_bytes`/`orphans_found` as calling
        the worker's `run_cycle` directly (same fixture);
      - the f2 mock-task rotation test still passes (mechanism alive).
- [ ] **Docs:** the scheduler module docs contain the scan-shape analysis
      table and the constraint note; every `pub` item keeps `# Examples`;
      `#![deny(missing_docs)]` passes.
- [ ] **ADR:** ADR-0017 §3's sharding intent is preserved as a mechanism;
      its GC/orphan application is explicitly deferred with reasons (this is
      the recorded 2026-09-04 reconciliation). No unaddressed constraint
      from ADR-0032/0025 (registry remains the segment set; store remains the
      single unified data store).
- [ ] **Perf:** no new whole-store scan is added by this feature; no task
      runs more than one full pass per cycle; the `orphan_reaper.rs:297` O(n)
      object-list review block is annotated (not deleted) noting the scheduler
      does not worsen it and that a segment-scoped scan/index is the fix.
- [ ] **Integration:** `cargo test -p oceanfs-node --test orphan_reaper --
      --test-threads=1` and `--test gc_compaction -- --test-threads=1`
      (RocksDB caveat) pass unchanged after the scheduler path exists.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).
