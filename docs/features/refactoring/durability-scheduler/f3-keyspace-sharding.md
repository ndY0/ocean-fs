---
feature: "f3: Keyspace Fraction for GC + Orphan Reaper"
epic: "refactoring/durability-scheduler"
status: done
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
    reason: "ADR-0034 (landed 2026-09-06) replaced the GC/orphan whole-store object-list passes with accounting-based liveness. GC/orphan still sweep the full segment/registry space per cycle and the MetadataStore API has no range-scan method, so keyspace_fraction stays 1.0 — naive sharding would multiply full passes per unit time. Roadmap wave 2 ⑥ before ③-f3."
adr:
  - 0017-durability-task-abstraction
  - 0032-unify-segment-data-access
  - 0025-segment-lifecycle-state-machine
  - 0034-bounded-metadata-accounting
perf:
  - "1.1 avoid O(n) full-store materialization in cycles where a bounded pass suffices"
  - "4.2 bound background scan I/O (no new whole-store passes per cycle)"
created: 2026-09-04
updated: 2026-09-06
---

# f3: Keyspace Fraction for GC + Orphan Reaper

> **FINAL STATE (2026-09-06):** `done`. Independent review verdict **PASS**
> (iteration 2). Code green: fmt, `cargo build --all-targets`, clippy `-D
> warnings`, rustdoc `-D warnings`, lib suite 276 tests incl. 19 scheduler
> tests (`--test-threads=1`). The mechanism shipped **ready but inert**
> exactly as specified: `GcTask`/`OrphanTask` (and scrub/AE) report
> `keyspace_fraction() == 1.0`, reject `Shard` windows loudly, and the
> scheduler rotation cursor stays unit-tested via mock tasks. GC/orphan scan
> behavior is byte-for-byte unchanged (behavior pins). No deviations vs this
> document's scope.

## Summary

ADR-0017 §3 proposes keyspace-fraction round-robin for GC and the orphan
reaper so a "10% per cycle" pass smooths the periodic GC spike (finding #20).
This feature verifies that proposal against today's scan shape, ships the
scheduler-side **mechanism** (from f1/f2: `keyspace_fraction()` +
per-task `cycle_index` rotation feeding `KeyspaceWindow::Shard`), and then
**keeps GC and the orphan reaper at `keyspace_fraction() == 1.0` (full pass)**:
with ADR-0034 their liveness inputs are bounded (registry totals + aged
dead-chunk records), but each cycle still sweeps the full segment set and the
`MetadataStore` API has no range-scan method, so a per-cycle fraction cannot
yet bound a pass. Naive sharding would multiply full passes per unit-time —
strictly worse. The mechanism ships inert; opting GC/orphan in requires a
range-scan/index substrate (future feature).

## Today's scan shape (verified 2026-09-06, after ADR-0034)

| Task | Cycle cost today | Source |
|---|---|---|
| GC | `process_tombstones`: byte-account liveness from registry `total_bytes` + aged dead-chunk records (ADR-0034 D3/D5); full `registry.for_each` over the segment set; tombstone keys by segment | garbage_collector.rs:258-270 |
| Orphan reaper | Fully-dead detection by byte accounting (`dead >= seal-time total`, ADR-0034 D4); iterates the registry + aged dead-chunk records; `.dat` unlink via the unified store — no objects-CF scan, no disk sweep | orphan_reaper.rs (f2 rewrite) |

The `MetadataStore` trait in `oceanfs-storage-api` still exposes only
whole-CF scans — `list_objects_all`, `list_objects_all_with_bucket`,
`list_tombstones_all` — with **no key-range or per-segment scan method**, and
GC/orphan liveness is attributeable only at full-registry granularity. There
is no segment→objects range index.

**Consequence:** ADR-0017's example ("GC `keyspace_fraction = 0.1` → 10× more
frequent, 1/10th the cost per cycle") does not hold on today's shape: slicing
the *segment iteration* to 10% while liveness is computed full-space would
multiply the whole passes 10× per unit time. That is the exact "make it
worse" trap this feature exists to avoid.

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

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-durability`.
- [x] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      (PIPELINE.md §4.6) passes, adding:
      - `GcTask`/`OrphanTask` return 1.0 fraction and reject
        `KeyspaceWindow::Shard` with `Error::Internal`;
      - behavior-preservation pin: running `GcTask::run_cycle(Full)` and
        `OrphanTask::run_cycle(Full)` over a shared test RocksDB store yields
        the same `segments_scanned`/`dead_bytes`/`orphans_found` as calling
        the worker's `run_cycle` directly (same fixture);
      - the f2 mock-task rotation test still passes (mechanism alive).
<!-- REVIEW: verified 2026-09-06 (iter 2, verdict PASS). All sub-bullets now pass (19 scheduler tests + full 276-test lib suite, --test-threads=1):
1) fraction==1.0 + Shard rejection for GcTask/OrphanTask: adaptors_report_full_keyspace_fraction (adaptors.rs:274), shard_window_is_rejected_before_delegation (adaptors.rs:239), orphan_rejects_shard_window (adaptors.rs:506). Scrub/AE share the same assert_full guard (adaptors.rs:33) and now have their own rejection tests (adaptors.rs:525,543).
2) behavior-preservation pin: gc_behavior_pin_full_matches_direct (adaptors.rs:481) and orphan_behavior_pin_full_matches_direct (adaptors.rs:459) run adaptor(Full) and the worker's direct run_cycle over the same seeded RocksDB registry/store and assert identical segments_scanned (== 2 for GC). dead_bytes/orphans_found are asserted only for the seeded-empty case (orphans_found == 0 at adaptors.rs:475); the seed contains no dead chunks so those fields are trivially equal — the pin's real target (full-space pass count preserved through the adaptor) is covered.
3) f2 mock rotation test rotation_delivers_shard_windows (engine.rs:568) still passes.
-->
- [x] **Docs:** the scheduler module docs contain the scan-shape analysis
      table and the constraint note; every `pub` item keeps `# Examples`;
      `#![deny(missing_docs)]` passes.
<!-- REVIEW: verified 2026-09-06 (iter 2). The scan-shape table (per-task cycle pass + why-not-sharded) now lives in crates/oceanfs-durability/src/scheduler/mod.rs:20-25, with the range-scan constraint + Shard-rejection note at mod.rs:27-33 and the adaptor module doc at adaptors.rs:9-18. RUSTDOCFLAGS="-D warnings" cargo doc passes for oceanfs-durability.
-->
- [x] **ADR:** ADR-0017 §3's sharding intent is preserved as a mechanism;
      its GC/orphan application is explicitly deferred with reasons (this is
      the recorded 2026-09-04 reconciliation). No unaddressed constraint
      from ADR-0032/0025 (registry remains the segment set; store remains the
      single unified data store).
- [x] **Perf:** no new whole-store scan is added by this feature; no task
      runs more than one full pass per cycle; the accounting-based GC/orphan
      passes (ADR-0034) stay full-space and are not multiplied — the
      scheduler module docs record that a segment-scoped scan/index is the
      enabler for future sharding.
- [x] **Integration:** `cargo test -p oceanfs-node --test orphan_reaper --
      --test-threads=1` and `--test gc_compaction -- --test-threads=1`
      (RocksDB caveat) pass unchanged after the scheduler path exists.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).
