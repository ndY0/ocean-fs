---
feature: "Split Membership Module"
epic: "refactoring/config-decomposition"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    reason: Membership types reference shared types from oceanfs-core
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Split Membership Module

## Summary

`crates/oceanfs-membership/src/membership.rs` is 822 lines containing both
membership state management (ring membership, node states, cluster view) and
lifecycle logic (join, leave, update, reconciliation). A `state.rs` already
exists in the `membership/` subdirectory but is only 80 lines — expand it to
own the full state representation. Split the remaining lifecycle logic into
`manager.rs`. The sibling file `gossip.rs` is 527 lines and will be monitored
(over 500-line threshold by 27 lines) but not split at this time. Tests move
alongside their types.

## Scope

### In Scope

- Expand `src/membership/state.rs` (currently ~80 lines):
  - Move all state representation types from `membership.rs` into `state.rs`:
    member list, node states, cluster view, epoch tracking, membership version
  - Retain existing `state.rs` content; merge with the extracted types
  - Target: ~250–350 lines after expansion
- Create `src/membership/manager.rs`:
  - Extract lifecycle and mutation logic from `membership.rs`:
    `join`, `leave`, `update_member`, `reconcile`, `handle_gossip_update`
  - Target: ~350–450 lines
- Update `src/membership/mod.rs`:
  - Declare `pub mod state;` and `pub mod manager;`
  - Re-export public types from `state.rs` and `manager.rs`
  - The `Membership` type (if it exists as a coordinator/façade) stays in
    `mod.rs` with thin delegation to the manager
- Migrate tests from the `#[cfg(test)]` block in the old `membership.rs` into
  the file that owns the type/function under test
- Delete the top-level `src/membership.rs` (its content is now in the
  `membership/` directory)
- Monitor `gossip.rs` (527 lines): no split at this time. If it grows beyond
  600 lines, trigger a follow-up split into `gossip/` directory.

### Out of Scope

- Splitting `gossip.rs`. It is over the 500-line guideline by 27 lines but
  the audit (M7) recommends monitoring rather than immediate action.
- Changing membership protocol behavior, state machine, or API. Pure
  structural refactor.
- Moving types between crates. Everything stays in `oceanfs-membership`.
- Adding new tests.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-membership` | Delete `src/membership.rs`; expand `src/membership/state.rs`; create `src/membership/manager.rs`; update `src/membership/mod.rs` |

## Interface (Public API)

No public API additions or removals. The re-export facade in
`src/membership/mod.rs` exports the same types that were previously
exported from `src/membership.rs`. Downstream consumers
(`use oceanfs_membership::Membership`) continue to work.

```rust
// oceanfs-membership/src/membership/mod.rs
pub mod state;
pub mod manager;

// Re-export public types to maintain the existing flat namespace
pub use state::MembershipState;
pub use state::MemberInfo;
pub use manager::MembershipManager;
// ... etc depending on actual type names
```

## Data Flow

Pure structural refactor. No runtime data flow changes.

```
Old:  use oceanfs_membership::membership::Membership
            ↓
      oceanfs-membership/src/membership.rs (822 lines)

New:  use oceanfs_membership::membership::Membership
            ↓
      oceanfs-membership/src/membership/mod.rs (re-exports)
            ↓
      oceanfs-membership/src/membership/state.rs (state types)
      oceanfs-membership/src/membership/manager.rs (lifecycle logic)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds workspace-wide; no new
  warnings
- [ ] **Tests:** `cargo test -p oceanfs-membership` passes; all tests from the
  old `membership.rs` pass in their new file locations
- [ ] **Docs:** Every `pub` item in `state.rs` and `manager.rs` has a doc
  comment; `#![deny(missing_docs)]` passes for `oceanfs-membership`
- [ ] **ADR:** N/A — internal refactor within one crate, no architectural
  decision required
- [ ] **Perf:** N/A — no behavioral change
- [ ] **Integration:** Existing integration tests
  (`oceanfs-membership/tests/` and cross-crate tests) pass unchanged
- [ ] **Line counts:** `state.rs` ~250–350 lines, `manager.rs` ~350–450
  lines, `mod.rs` under 100 lines. No file exceeds 500 lines (excluding
  tests)
- [ ] **Re-exports:** `src/membership/mod.rs` re-exports all previously
  public types; `cargo doc --no-deps -p oceanfs-membership` shows identical
  public API for the `membership` module
