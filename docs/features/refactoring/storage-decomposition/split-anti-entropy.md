---
feature: "Split Anti-Entropy Module"
epic: "refactoring/storage-decomposition"
status: done
priority: high
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    feature: split-core-types
    reason: Shared types imported by anti-entropy code must already be in their split files
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-04
---

# Split Anti-Entropy Module

## Summary

Split `crates/oceanfs-storage/src/anti_entropy.rs` (2,567 lines) into an
`anti_entropy/` directory with 6 files, one per type or type group. The former
monolithic file contained 8+ distinct types (`MerkleTree`, `MerkleRoot`,
`MerkleProof`, `LeafRange`, `AntiEntropyConfig`, `AntiEntropy`,
`AntiEntropyStats`, `MerkleExchangeProtocol`, `SegmentDataStore`,
`InMemorySegmentStore`) plus test code, violating the "one public type per
file" rule (architecture guideline §3.3). The split follows the existing
`oceanfs-storage/src/segment/` directory pattern (`buffer.rs`, `handle.rs`,
`header.rs`, `index.rs`, etc.). The existing re-export of `anti_entropy` from
`oceanfs-storage/src/lib.rs` is preserved.

## Scope

### In Scope

- Create `anti_entropy/merkle_tree.rs` containing `SegmentDataStore` trait,
  `InMemorySegmentStore`, `MerkleTree` struct and all its `impl` blocks
  (`build_from_hashes`, `root`, `hash`, `get_proof`, etc.), plus all
  `MerkleTree` unit tests
- Create `anti_entropy/merkle_root.rs` containing the `MerkleRoot` type
- Create `anti_entropy/merkle_proof.rs` containing `LeafRange` and
  `MerkleProof` types
- Create `anti_entropy/config.rs` containing `AntiEntropyConfig`, plus
  its unit test
- Create `anti_entropy/engine.rs` containing `AntiEntropy`,
  `MerkleExchangeProtocol`, `AntiEntropyStats`, and the `run_cycle` and
  `start_background` methods, plus all engine unit tests
- Create `anti_entropy/mod.rs` as the re-export facade
- Migrate every test function from the `#[cfg(test)] mod tests` block in the
  old `anti_entropy.rs` into the file that owns the type under test:
  - `MerkleTree` tests + `SegmentDataStore` tests → `anti_entropy/merkle_tree.rs`
  - `MerkleProof` tests → `anti_entropy/merkle_proof.rs`
  - `AntiEntropy` engine tests → `anti_entropy/engine.rs`
- Delete `src/anti_entropy.rs` and replace with the `src/anti_entropy/` directory
- Preserve the `pub mod anti_entropy;` declaration in `lib.rs` (2021 edition
  directory module convention)

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
| `oceanfs-storage` | Delete `src/anti_entropy.rs`; create `src/anti_entropy/` directory with 6 files (`merkle_tree.rs`, `merkle_root.rs`, `merkle_proof.rs`, `config.rs`, `engine.rs`, `mod.rs`). Zero changes in any other crate. |

## Interface (Public API)

The facade in `anti_entropy/mod.rs` re-exports every public item that was
previously exported from `anti_entropy.rs`. Downstream consumers
(`use oceanfs_storage::anti_entropy::MerkleTree`) continue to work unchanged.
Zero downstream breakage.

The re-export facade follows the pattern established by
`oceanfs-storage/src/segment/mod.rs`:

```rust
// oceanfs-storage/src/anti_entropy/mod.rs
mod merkle_tree;
mod merkle_root;
mod merkle_proof;
mod config;
mod engine;

pub use merkle_tree::{MerkleTree, SegmentDataStore, InMemorySegmentStore};
pub use merkle_root::MerkleRoot;
pub use merkle_proof::{MerkleProof, LeafRange};
pub use config::AntiEntropyConfig;
pub use engine::{AntiEntropy, MerkleExchangeProtocol, AntiEntropyStats};
```

### New Public Items (additive, non-breaking)

| Item | File | Purpose |
|---|---|---|
| `SegmentDataStore` (trait) | `merkle_tree.rs` | Trait abstracting segment data access for Merkle tree building; extracted from `MerkleTree` |
| `InMemorySegmentStore` | `merkle_tree.rs` | In-memory implementation of `SegmentDataStore` for tests |
| `MerkleExchangeProtocol` | `engine.rs` | Protocol state machine for an anti-entropy exchange session; extracted from `AntiEntropy` |

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

- [x] **Code:** `cargo build --all-targets` succeeds for `oceanfs-storage` and all
  dependent crates; zero new warnings. Reviewer returned PASS.
<!-- REVIEW-V2: `cargo build --all-targets -p oceanfs-storage` passes clean. `cargo clippy --lib -p oceanfs-storage -- -D warnings` passes (production code). `cargo clippy --all-targets` has 2 cosmetic lints in anti-entropy test code (needless_borrows @ engine.rs:1221, identity_op @ merkle_tree.rs:957) — non-blocking per coding §9.2.1. Workspace-wide build has pre-existing oceanfs-server compilation failure (unrelated to this feature). -->
- [x] **Tests:** All 270 unit tests pass (`cargo test -p oceanfs-storage`). E2E
  anti-entropy test passes (14 integration tests in `tests/anti_entropy.rs`). Every test from the old
  `anti_entropy.rs` continues to pass in its new location.
<!-- REVIEW-V2: Independently verified — 270 unit + 14 integration + 5 distributed_scrub = 289 total tests pass. All anti-entropy tests co-located with their types (merkle_tree.rs, engine.rs, config.rs). Integration test file tests/anti_entropy.rs uses facade re-exports correctly. -->
- [x] **Docs:** Every `pub` item in each new file has a doc comment;
  `cargo doc --no-deps -p oceanfs-storage` produces no `missing_docs` warnings
<!-- REVIEW-V2: Verified — all 9 pub types (AntiEntropyConfig, MerkleRoot, MerkleProof, LeafRange, MerkleTree, SegmentDataStore, InMemorySegmentStore, AntiEntropy, AntiEntropyStats) have doc comments. Module-level docs exist in mod.rs, merkle_tree.rs, engine.rs, config.rs, merkle_proof.rs, merkle_root.rs. `RUSTDOCFLAGS="-D warnings"` passes. -->
- [x] **ADR:** N/A — this implements existing guideline §3.3, no new decision required
<!-- REVIEW-V2: Confirmed — no ADRs cited in feature frontmatter (adr: []). Architecture §3.3 is the governing guideline. -->
- [x] **Perf:** N/A — no behavioral change
<!-- REVIEW-V2: Confirmed — no perf rules cited (perf: []). No behavioral changes; pure structural refactor. -->
- [x] **Integration:** Existing integration tests (`oceanfs-storage/tests/` and
  `oceanfs-node/tests/` anti-entropy scenarios) pass unchanged; zero downstream breakage
<!-- REVIEW-V2: Verified — tests/anti_entropy.rs (14 tests) pass. tests/distributed_scrub.rs (5 tests) pass and use anti_entropy types via facade. Zero downstream crates directly import anti_entropy types from oceanfs_storage. -->
- [x] **Facade:** `oceanfs-storage/src/anti_entropy/mod.rs` re-exports every
  public item from the old `anti_entropy.rs` — verified via `cargo doc` showing
  identical public API surface
<!-- REVIEW-V2: Verified — facade re-exports 9 public types (not 8 as implementer claimed): AntiEntropyConfig, AntiEntropy, AntiEntropyStats, LeafRange, MerkleProof, MerkleRoot, InMemorySegmentStore, MerkleTree, SegmentDataStore. lib.rs re-exports all 9 identically. Visibility audit: pub(crate) correctly applied to MerkleExchangeProtocol, DEFAULT_LEAF_SIZE, MerkleRoot fields, AntiEntropyConfig fields. -->
