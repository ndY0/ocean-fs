---
feature: "Move Coordinators Into Subdirectories"
epic: "server-cleanup"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: type-system-cleanup
    reason: Coordinators import shared types from oceanfs-core
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Move Coordinators Into Subdirectories

## Summary

Consolidate `read_coordinator.rs` and `write_coordinator.rs` into their
respective `read/` and `write/` subdirectories. Currently the subdirectory
structure (`read/mod.rs`, `read/assembly.rs`, `read/fetch.rs`, `read/repair.rs`
and `write/mod.rs`, `write/replication.rs`) coexists with top-level coordinator
files, creating a split organization. Moving the coordinator files makes each
subdirectory self-contained: the coordinator struct lives alongside its helper
modules.

## Scope

### In Scope

- Move `crates/oceanfs-server/src/read_coordinator.rs` (1,192 lines)
  → `crates/oceanfs-server/src/read/coordinator.rs`
- Move `crates/oceanfs-server/src/write_coordinator.rs` (687 lines)
  → `crates/oceanfs-server/src/write/coordinator.rs`
- Update `read/mod.rs` to declare `pub(crate) mod coordinator;` and
  re-export all public types currently exported from `read_coordinator.rs`
- Update `write/mod.rs` to declare `mod coordinator;` and re-export
  all types currently exported from `write_coordinator.rs`
- Update `oceanfs-server/src/lib.rs` to remove `mod read_coordinator;` and
  `mod write_coordinator;` declarations; update `pub use` re-exports to
  reference the new module paths (`read::coordinator::*` and
  `write::coordinator::*`)
- Update all intra-crate imports (e.g., `s3_handler.rs` imports from
  `read_coordinator` → `read::coordinator`)

### Out of Scope

- Splitting coordinator logic further — this is a pure file move
- Changing any logic or test code
- Altering visibility of any type — everything that was `pub` or
  `pub(crate)` stays the same

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | Delete `src/read_coordinator.rs`, create `src/read/coordinator.rs`. Delete `src/write_coordinator.rs`, create `src/write/coordinator.rs`. Update `src/lib.rs`, `src/read/mod.rs`, `src/write/mod.rs`, and all internal imports. |

## Interface (Public API)

The following re-exports from `lib.rs` must be preserved with their existing
names:

**From `read_coordinator` (moving to `read::coordinator`):**
- `pub use read::coordinator::ReadCoordinator;`
- `pub use read::coordinator::ReadOutcome;`
- `pub use read::coordinator::ReadRequest;`
- `pub use read::coordinator::ReadResult;`
- `pub use read::coordinator::GetResult;`
- `pub use read::coordinator::CacheHitLevel;`
- `pub use read::coordinator::SegmentReader;`
- `pub use read::coordinator::InMemorySegmentReader;`

**From `write_coordinator` (moving to `write::coordinator`):**
- `pub use write::coordinator::WriteCoordinator;`
- `pub use write::coordinator::WriteRequest;`

**No public API change.** Downstream crates (`oceanfs-node`) import from
`oceanfs_server::ReadCoordinator` etc., which remain unchanged.

## Data Flow

Unchanged. The coordinator logic is identical; only the file path changes.

## Implementation Plan

1. Move `read_coordinator.rs` → `read/coordinator.rs`
   - Adjust `use` statements: any `use super::` or `use crate::` that
     referenced the old module path must be updated for the one-level-deeper
     nesting (e.g., `use crate::read_coordinator` → `use super`, or
     intra-module `use crate::read::` → `use super::`)
2. Update `read/mod.rs`:
   - Add `pub(crate) mod coordinator;`
   - Re-export public types via `pub use coordinator::*;` (module-gated:
     only the specific symbols from the original lib.rs re-export list)
   - Keep existing `assembly`, `fetch`, `repair` declarations
3. Move `write_coordinator.rs` → `write/coordinator.rs`
   - Same import adjustment as step 1
4. Update `write/mod.rs`:
   - Add `pub(crate) mod coordinator;`
   - Re-export types: `pub use coordinator::WriteCoordinator;`,
     `pub use coordinator::WriteRequest;`
   - Keep existing `replication` declaration
5. Update `src/lib.rs`:
   - Remove `mod read_coordinator;` and `mod write_coordinator;`
   - Update re-export paths: `read_coordinator::Foo` → `read::coordinator::Foo`
   - The existing `pub use read::assembly::MultiChunkAssembler;` is unchanged
6. Update imports in other server files:
   - `s3_handler.rs`: `use crate::read_coordinator::*` →
     `use crate::read::coordinator::*` (and similarly for write)
   - `admin.rs`, `metadata_ops.rs`, `hinted_handoff.rs`: check for
     any coordinator imports
   - `router.rs`: check for coordinator imports
7. Run `cargo build --all-targets -p oceanfs-server` and fix compile errors
8. Run `cargo test -p oceanfs-server` — all tests must pass

## Definition of Done

- [ ] **Code:** `cargo build --all-targets -p oceanfs-server` succeeds
- [ ] **Tests:** `cargo test -p oceanfs-server` passes; all existing
  coordinator tests pass from the new file location
- [ ] **Docs:** `#![deny(missing_docs)]` passes
- [ ] **ADR:** Not required (pure internal file move, no architectural change)
- [ ] **Perf:** Not applicable
- [ ] **Integration:** `cargo test -p oceanfs-node` (cross-crate integration)
  continues passing; no import breakage in downstream crates
