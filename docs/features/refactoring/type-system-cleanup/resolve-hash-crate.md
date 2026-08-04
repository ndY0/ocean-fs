---
feature: "Implement Hash Crate"
epic: "refactoring/type-system-cleanup"
status: done
priority: high
owner: ""
dependencies:
  - epic: refactoring/type-system-cleanup
    feature: split-core-types
    reason: Moves HashOutput into types/hash.rs first, making the hash-crate decision cleaner
adr:
  - docs/adr/0008-hash-crate-implementation.md (to be written)
perf: []
created: 2026-08-03
updated: 2026-08-04
---

# Implement Hash Crate

## Summary

**Decision (2026-08-03): Option A — implement `oceanfs-hash`.** The skeleton
crate will be filled with `Blake3Hasher` (streaming), `BatchHasher`
(multi-chunk), and `HashOutput` (moved from `oceanfs-core`). Per spec §9.3.3,
the upstream `blake3` crate handles runtime CPU feature detection.
`oceanfs-hash` provides thin wrapper traits with BLAKE3 as the default
implementation. An ADR is required before implementation to document the
interface design.

## Scope

### In Scope

- **Write ADR** documenting the interface design: `Hasher` trait, `BatchHasher`
  trait, `HashOutput` type, and the `Blake3Hasher` default implementation
- **Implement `oceanfs-hash`:**
  - `src/lib.rs` — facade re-exports
  - `src/hasher.rs` — `pub trait Hasher`, `pub struct Blake3Hasher`
  - `src/batch.rs` — `pub trait BatchHasher`, `pub struct Blake3BatchHasher`
  - `src/hash_output.rs` — `pub struct HashOutput` (moved from `oceanfs-core`)
- **Update `oceanfs-core`:** Remove `HashOutput` from `types/hash.rs`, add
  `oceanfs-hash` as a dependency, re-export `HashOutput` from `oceanfs-core`
  so downstream crates aren't broken
- **Update `oceanfs-storage`:** Add `oceanfs-hash` dependency, use
  `Blake3Hasher` for hashing operations
- **Update `guidelines/architecture.md`** §1.1 and §1.2 to reflect the
  implemented crate (already shows `hash` — just verify consistency)

### Out of Scope

- Option B (delete the crate) — decision is made for Option A
- Changing the hashing algorithm at runtime (spec §9.3.3 mandates BLAKE3)
- Content-defined chunking or higher-level hashing strategies

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-hash` | Filled: add `src/hasher.rs`, `src/batch.rs`, `src/hash_output.rs`, `src/lib.rs` with traits + impls. Add `blake3` dependency. |
| `oceanfs-core` | `types/hash.rs` removes `HashOutput` (moves to `oceanfs-hash`). Add `oceanfs-hash` as dependency. Re-export `HashOutput` from `lib.rs` for backward compatibility. |
| `oceanfs-storage` | Add `oceanfs-hash` dependency. Use `Blake3Hasher` for segment hashing. |
| `guidelines/architecture.md` | Verify §1.1 and §1.2 consistency (already shows `hash` crate). |

## Interface (Public API)

```rust
// oceanfs-hash/src/lib.rs
pub use hasher::Blake3Hasher;
pub use hasher::Hasher;
pub use batch::BatchHasher;
pub use batch::Blake3BatchHasher;
pub use hash_output::HashOutput;

// oceanfs-hash/src/hasher.rs
pub trait Hasher: Send + Sync {
    fn update(&mut self, data: &[u8]);
    fn finalize(&self) -> HashOutput;
}

pub struct Blake3Hasher { inner: blake3::Hasher }

// oceanfs-hash/src/batch.rs
pub trait BatchHasher: Send + Sync {
    fn hash_chunks(&self, chunks: &[&[u8]]) -> Vec<HashOutput>;
}

pub struct Blake3BatchHasher;

// oceanfs-hash/src/hash_output.rs
pub struct HashOutput([u8; 32]);  // moved from oceanfs-core
```

## Data Flow

```
Write path:
  PUT data
    → oceanfs-storage::segment_buffer
      → oceanfs-hash::Blake3Hasher::update(&data)
        → blake3::Hasher (upstream, runtime CPU feature detection)
      → oceanfs-hash::Hasher::finalize() → HashOutput
    → HashOutput stored in SegmentIndexEntry

Anti-entropy / scrub:
  Read chunk
    → oceanfs-hash::Blake3Hasher::update(&chunk)
    → HashOutput compared against stored hash
```

## Definition of Done

- [x] **ADR:** ADR written (`docs/adr/0008-hash-crate-implementation.md`)
  documenting the interface design and tradeoffs
- [x] **Code:** `oceanfs-hash` has `Hasher`, `BatchHasher` traits and
  `Blake3Hasher`, `Blake3BatchHasher` implementations. `HashOutput` moved
  from `oceanfs-core`. `cargo build --workspace --all-targets` succeeds.
- [x] **Tests:** `oceanfs-hash` has unit tests covering `Blake3Hasher` and
  `Blake3BatchHasher` with roundtrip checks. `cargo test --workspace` passes.
- [x] **Docs:** `guidelines/architecture.md` §1.1, §1.2, §1.3 updated and verified consistent
  with the implemented crate
- [x] **Perf:** N/A — BLAKE3 is already fast, upstream handles SIMD
- [x] **Integration:** Cross-crate integration tests pass; existing hash
  behavior verified through integration test suite
