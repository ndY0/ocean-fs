---
feature: "Split GC Module"
epic: "refactoring/storage-decomposition"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    feature: split-core-types
    reason: Shared types imported by GC code must already be in their split files
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Split GC Module

## Summary

Split `crates/oceanfs-storage/src/gc.rs` (~2,126 lines) into a `gc/` directory
with one file per public type. The current monolithic file contains 7 distinct
public types (`GcConfig`, `GcStats`, `LivenessTracker`, `SegmentCompactor`,
`GarbageCollector`, `OrphanStats`, and `OrphanReaper`), violating the "one
public type per file" rule (architecture guideline §3.3). The pattern to follow
is the existing `oceanfs-storage/src/segment/` directory, which already has
`buffer.rs`, `handle.rs`, `header.rs`, `index.rs`, etc. The existing re-export
of `gc` from `oceanfs-storage/src/lib.rs` must be preserved.

## Scope

### In Scope

- Create `gc/config.rs` containing `GcConfig` struct and its `Default` impl
- Create `gc/stats.rs` containing `GcStats`
- Create `gc/liveness_tracker.rs` containing `LivenessTracker` struct and all
  its `impl` blocks (liveness analysis, reference counting, dead segment
  identification)
- Create `gc/segment_compactor.rs` containing `SegmentCompactor` struct and its
  `impl` blocks (compaction logic, segment merging, live-data extraction)
- Create `gc/garbage_collector.rs` containing `GarbageCollector` struct and its
  `impl` blocks (orchestration, scheduling, coordination with compactor and reaper)
- Create `gc/orphan_reaper.rs` containing `OrphanReaper` struct, `OrphanStats`,
  and their `impl` blocks (orphan detection, segment reclamation)
- Create `gc/mod.rs` as the re-export facade
- Migrate every test function from the `#[cfg(test)] mod tests` block in the
  old `gc.rs` into the file that owns the type under test:
  - `GcConfig` tests → `gc/config.rs`
  - `LivenessTracker` tests → `gc/liveness_tracker.rs`
  - `SegmentCompactor` tests → `gc/segment_compactor.rs`
  - `GarbageCollector` tests → `gc/garbage_collector.rs`
  - `OrphanReaper` tests → `gc/orphan_reaper.rs`
  - etc.
- Delete `src/gc.rs` and replace with the `src/gc/` directory
- Preserve the `pub mod gc;` declaration in `lib.rs` (or update to
  `pub mod gc;` if using 2021 edition directory module convention)

### Out of Scope

- Renaming, refactoring, or changing any type's fields, methods, or visibility
- Changing garbage collection algorithms or behavior
- Splitting any other storage module (e.g., `anti_entropy.rs` — that is feature
  `split-anti-entropy`)
- Moving GC out of `oceanfs-storage` (that is Epic 5, `evaluate-storage-split`)
- Any downstream crate changes

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | Delete `src/gc.rs`; create `src/gc/` directory with 7 files (`config.rs`, `stats.rs`, `liveness_tracker.rs`, `segment_compactor.rs`, `garbage_collector.rs`, `orphan_reaper.rs`, `mod.rs`) |

## Interface (Public API)

No new public items. No removed public items. The facade in `gc/mod.rs`
re-exports every public item that was previously exported from `gc.rs`.
Downstream consumers (`use oceanfs_storage::gc::GarbageCollector`) continue to
work unchanged.

The re-export facade follows the pattern established by
`oceanfs-storage/src/segment/mod.rs`:

```rust
// oceanfs-storage/src/gc/mod.rs
mod config;
mod stats;
mod liveness_tracker;
mod segment_compactor;
mod garbage_collector;
mod orphan_reaper;

pub use config::GcConfig;
pub use stats::GcStats;
pub use liveness_tracker::LivenessTracker;
pub use segment_compactor::SegmentCompactor;
pub use garbage_collector::GarbageCollector;
pub use orphan_reaper::{OrphanReaper, OrphanStats};
```

All types remain in the `oceanfs_storage::gc` namespace.

## Data Flow

This is a pure structural refactor. No runtime data flow changes.

```
Old:  crates use oceanfs_storage::gc::GarbageCollector
            ↓
      oceanfs-storage/src/gc.rs (2,126 lines, monolithic)

New:  crates use oceanfs_storage::gc::GarbageCollector
            ↓
      oceanfs-storage/src/gc/mod.rs (re-exports)
            ↓
      oceanfs-storage/src/gc/garbage_collector.rs (GarbageCollector definition)
```

Inter-module coupling within `gc/` follows the existing conventions: the
`GarbageCollector` (orchestrator) may hold references to `LivenessTracker`,
`SegmentCompactor`, and `OrphanReaper` via `pub(crate)` or `Arc` fields as it
already does today. No structural changes to these relationships are intended.

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds for `oceanfs-storage` and all
  dependent crates; no new warnings
- [ ] **Tests:** `cargo test -p oceanfs-storage` passes; every test from the old
  `gc.rs` continues to pass in its new location
- [ ] **Docs:** Every `pub` item in each new file has a doc comment;
  `cargo doc --no-deps -p oceanfs-storage` produces no `missing_docs` warnings
- [ ] **ADR:** N/A — this implements existing guideline §3.3, no new decision required
- [ ] **Perf:** N/A — no behavioral change
- [ ] **Integration:** Existing integration tests (`oceanfs-storage/tests/` and
  `oceanfs-node/tests/` GC, compaction, and orphan reaper scenarios) pass unchanged
- [ ] **Facade:** `oceanfs-storage/src/gc/mod.rs` re-exports every public item
  from the old `gc.rs` — verified via `cargo doc` showing identical public API
  surface
