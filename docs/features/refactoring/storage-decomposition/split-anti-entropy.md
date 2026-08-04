---
feature: "Split Anti-Entropy Module"
epic: "refactoring/storage-decomposition"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    feature: split-core-types
    reason: Shared types imported by anti-entropy code must already be in their split files
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-03
---

# Split Anti-Entropy Module

## Summary

Split `crates/oceanfs-storage/src/anti_entropy.rs` (~2,580 lines) into an
`anti_entropy/` directory with one file per public type. The current monolithic
file contains 6+ distinct public types (`MerkleTree`, `MerkleRoot`,
`MerkleProof`, `LeafRange`, `AntiEntropyConfig`, `AntiEntropy`, and
`AntiEntropyStats`) plus test-only types, violating the "one public type per
file" rule (architecture guideline §3.3). The pattern to follow is the existing
`oceanfs-storage/src/segment/` directory, which already has `buffer.rs`,
`handle.rs`, `header.rs`, `index.rs`, etc. The existing re-export of
`anti_entropy` from `oceanfs-storage/src/lib.rs` must be preserved.

## Scope

### In Scope

- Create `anti_entropy/merkle_tree.rs` containing `MerkleTree` struct and all
  its `impl` blocks (`build_from_hashes`, `root`, `hash`, `get_proof`, etc.)
- Create `anti_entropy/merkle_root.rs` containing the `MerkleRoot` type
- Create `anti_entropy/merkle_proof.rs` containing `MerkleProof` and `LeafRange` types
- Create `anti_entropy/config.rs` containing `AntiEntropyConfig`
- Create `anti_entropy/engine.rs` containing the `AntiEntropy` struct,
  `AntiEntropyStats`, and the `run_cycle` and `start_background` methods
- Create `anti_entropy/mod.rs` as the re-export facade
- Migrate every test function from the `#[cfg(test)] mod tests` block in the
  old `anti_entropy.rs` into the file that owns the type under test:
  - `MerkleTree` tests → `anti_entropy/merkle_tree.rs`
  - `MerkleProof` tests → `anti_entropy/merkle_proof.rs`
  - `AntiEntropy` engine tests → `anti_entropy/engine.rs`
  - etc.
- Delete `src/anti_entropy.rs` and replace with the `src/anti_entropy/` directory
- Preserve the `pub mod anti_entropy;` declaration in `lib.rs` (or update to
  `pub mod anti_entropy;` if using 2021 edition directory module convention)

### Out of Scope

- Renaming, refactoring, or changing any type's fields, methods, or visibility
- Changing the anti-entropy protocol or algorithm behavior
- Splitting any other storage module (e.g., `gc.rs` — that is feature `split-gc`)
- Moving anti-entropy out of `oceanfs-storage` (that is Epic 5,
  `evaluate-storage-split`)
- Any downstream crate changes

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | Delete `src/anti_entropy.rs`; create `src/anti_entropy/` directory with 6 files (`merkle_tree.rs`, `merkle_root.rs`, `merkle_proof.rs`, `config.rs`, `engine.rs`, `mod.rs`) |

## Interface (Public API)

No new public items. No removed public items. The facade in
`anti_entropy/mod.rs` re-exports every public item that was previously
exported from `anti_entropy.rs`. Downstream consumers
(`use oceanfs_storage::anti_entropy::MerkleTree`) continue to work unchanged.

The re-export facade follows the pattern established by
`oceanfs-storage/src/segment/mod.rs`:

```rust
// oceanfs-storage/src/anti_entropy/mod.rs
mod merkle_tree;
mod merkle_root;
mod merkle_proof;
mod config;
mod engine;

pub use merkle_tree::MerkleTree;
pub use merkle_root::MerkleRoot;
pub use merkle_proof::{MerkleProof, LeafRange};
pub use config::AntiEntropyConfig;
pub use engine::{AntiEntropy, AntiEntropyStats};
```

All types remain in the `oceanfs_storage::anti_entropy` namespace.

## Data Flow

This is a pure structural refactor. No runtime data flow changes.

```
Old:  crates use oceanfs_storage::anti_entropy::MerkleTree
            ↓
      oceanfs-storage/src/anti_entropy.rs (2,580 lines, monolithic)

New:  crates use oceanfs_storage::anti_entropy::MerkleTree
            ↓
      oceanfs-storage/src/anti_entropy/mod.rs (re-exports)
            ↓
      oceanfs-storage/src/anti_entropy/merkle_tree.rs (MerkleTree definition)
```

The anti-entropy engine (`engine.rs`) has internal `pub(crate)` visibility for
types that sibling modules may need; private helper types stay in
`engine.rs`. This follows the visibility rule from architecture §3.2:
public API → re-exported; internal cross-module use → `pub(crate)`; local use
only → `private`.

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds for `oceanfs-storage` and all
  dependent crates; no new warnings
- [ ] **Tests:** `cargo test -p oceanfs-storage` passes; every test from the old
  `anti_entropy.rs` continues to pass in its new location
- [ ] **Docs:** Every `pub` item in each new file has a doc comment;
  `cargo doc --no-deps -p oceanfs-storage` produces no `missing_docs` warnings
- [ ] **ADR:** N/A — this implements existing guideline §3.3, no new decision required
- [ ] **Perf:** N/A — no behavioral change
- [ ] **Integration:** Existing integration tests (`oceanfs-storage/tests/` and
  `oceanfs-node/tests/` anti-entropy scenarios) pass unchanged
- [ ] **Facade:** `oceanfs-storage/src/anti_entropy/mod.rs` re-exports every
  public item from the old `anti_entropy.rs` — verified via `cargo doc` showing
  identical public API surface
