---
feature: "Split GC Module"
epic: "refactoring/storage-decomposition"
status: done
priority: high
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    feature: split-core-types
    reason: Shared types imported by GC code must already be in their split files
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-04
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

- [x] **Code:** `cargo build --all-targets -p oceanfs-storage` passes; `cargo build --lib` workspace passes; no new warnings from split
<!-- REVIEW: Verified build passes for oceanfs-storage crate and workspace lib. -->
- [x] **Tests:** `cargo test --all-targets -p oceanfs-storage` passes: 270 unit + all 9 integration binary tests (0 failures)
<!-- REVIEW: Verified 270 unit tests (46 from old gc.rs) + gc_compaction (5), orphan_reaper (7), anti_entropy (14), distributed_scrub (5), metadata_crud (12), pipeline_parallelism (7), segment_roundtrip (14), tiered_routing (20), wal_recovery (6). -->
- [ ] **Tests-Distribution:** Tests were NOT migrated to individual type files per the In-Scope spec. All 46 GC tests remain in `gc/garbage_collector.rs` instead of being distributed to `config.rs`, `liveness_tracker.rs`, `segment_compactor.rs`, `orphan_reaper.rs`.
<!-- REVIEW: In-Scope item 9 requires each type's tests in its own file (e.g., GcConfig tests → gc/config.rs, LivenessTracker tests → gc/liveness_tracker.rs). None of config.rs, stats.rs, liveness_tracker.rs, segment_compactor.rs, or orphan_reaper.rs contain any tests. All 46 tests are consolidated in garbage_collector.rs. -->
- [x] **Docs:** All `pub` items have doc comments; `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p oceanfs-storage` passes (0 warnings)
<!-- REVIEW: Verified module-level `//!` comments in all 6 submodule files and mod.rs. Every pub fn/struct/trait/method has a doc comment. -->
- [x] **ADR:** N/A — no ADR constraints apply
- [x] **Perf:** N/A — no behavioral change
- [x] **Integration:** `gc_compaction.rs` (5 tests) and `orphan_reaper.rs` (7 tests) pass; oceanfs-node `node_start_with_invalid_addr_errors` failure is pre-existing
<!-- REVIEW: Verified both integration test binaries pass with zero failures. -->
- [ ] **Visibility-Preservation:** Five visibility changes detected. `GcConfig` fields (private→pub(crate)), `LivenessTracker` fields (private→pub(crate)), `tier_target_size` (private→pub(crate)), `build_referenced_set` (private→pub(crate)), `is_segment_referenced` (private→pub(crate)).
<!-- REVIEW: These violate Out-of-Scope "no visibility changes." Some are structurally necessary for cross-module access after split, but GcConfig and LivenessTracker field visibility increases are not needed (getters already existed for GcConfig; LivenessTracker tests could use pub(super) or remain with getters). -->
- [x] **Facade:** `gc/mod.rs` re-exports all 7 public types matching old `gc.rs`; `LivenessTracker` and `SegmentCompactor` correctly remain `pub(crate)`
<!-- REVIEW: lib.rs re-exports: GcConfig, GcStats, GarbageCollector, SegmentShardStore, InMemorySegmentShardStore, OrphanReaper, OrphanStats. All verified present via cargo doc. -->

## Reviewer Notes

Review conducted 2026-08-04.

The split is structurally sound: all 7 files created, old `gc.rs` deleted,
build/tests/docs pass, and no downstream breakage. Two issues remain:

1. **Tests not distributed:** The In-Scope spec requires each type's tests in its
   own file. All 46 tests were consolidated in `garbage_collector.rs`. While this
   keeps helper sharing simple, it doesn't follow the specified pattern.

2. **Visibility changes:** Five items had their visibility increased (private→pub(crate)).
   `tier_target_size`, `build_referenced_set`, and `is_segment_referenced` changes
   are structurally necessary post-split. `GcConfig` and `LivenessTracker` field
   visibility changes are unnecessary (getters already exist) and violate
   Out-of-Scope.
