# ADR-0008: Hash Crate Interface Design

**Status:** Accepted
**Date:** 2026-08-04
**Deciders:** Implementer (resolving open question Q1 from structural-roadmap.md)

---

## Context

The `oceanfs-hash` crate exists as a workspace skeleton but is empty — it was part
of the original architecture diagram but never implemented. Hash operations
(BLAKE3 hashing) were performed directly via the `blake3` crate in `oceanfs-storage`,
and the `HashOutput` type lived in `oceanfs-core`.

The structural audit (2026-08-03) identified `oceanfs-hash` as dead code and asked
whether to implement it (Option A) or delete it (Option B). The decision was made
for **Option A** — implement the crate.

This ADR documents the interface design for the implemented crate.

## Decision

### Crate: `oceanfs-hash`

**Dependencies:** `oceanfs-core`, `blake3`, `bytes`

**Interface:**

```rust
// oceanfs-hash/src/lib.rs — facade re-exports
pub use batch::{BatchHasher, Blake3BatchHasher};
pub use hash_output::HashOutput;
pub use hasher::{Blake3Hasher, Hasher};

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
```

### HashOutput: Moved from `oceanfs-core` to `oceanfs-hash`

`HashOutput` is the 256-bit BLAKE3 hash (32 bytes). It was defined in
`oceanfs-core::types::hash`. After this ADR:

- **Definition:** lives in `oceanfs-hash::HashOutput`
- **Re-export:** `oceanfs-core` re-exports it from `oceanfs-hash` so downstream
  crates can continue writing `use oceanfs_core::HashOutput` with zero changes
- **Direct users:** `oceanfs-storage`, `oceanfs-server`, and `oceanfs-node` tests
  can optionally import from `oceanfs_hash::HashOutput`

### stream_hasher / finalize

- `Hasher::update(&mut self, data: &[u8])` — feeds data into the hasher,
  equivalent to `blake3::Hasher::update()`
- `Hasher::finalize(&self) -> HashOutput` — produces the final hash,
  equivalent to `blake3::Hasher::finalize()`. Note: `blake3::Hasher::finalize`
  takes `&self` (non-mutating), so our trait mirrors that

### batch_hasher / hash_chunks

- `BatchHasher::hash_chunks(&self, chunks: &[&[u8]]) -> Vec<HashOutput>` —
  hashes multiple chunks independently and returns one hash per chunk
- `Blake3BatchHasher` hashes each chunk sequentially using a fresh `Blake3Hasher`;
  parallelism can be added later via `rayon` if needed.

### Perf notes

- BLAKE3 upstream handles runtime CPU feature detection (AVX-512, AVX2, SSE4.1,
  NEON) — zero configuration needed
- `Hasher` is a thin wrapper around `blake3::Hasher` with zero overhead
- `BatchHasher` uses a fresh `Blake3Hasher` per chunk (can be parallelized with
  `rayon` if profiling shows need)
- Static dispatch: all usage in `oceanfs-storage` uses `Blake3Hasher` directly
  (not `Box<dyn Hasher>`), per perf rule 6.4

## Consequences

### Positive

- `oceanfs-hash` becomes a real, filled crate — the architecture diagram is
  now accurate
- Hash operations are centralized in one crate with a clean trait interface
- Upstream BLAKE3 handles all SIMD optimization — the crate is thin and
  maintainable
- Downstream crates (`oceanfs-core`, `oceanfs-storage`, `oceanfs-server`)
  continue to work with zero or minimal import changes

### Negative

- `oceanfs-core` gains a dependency on `oceanfs-hash` (previously had zero
  internal deps). This is the first internal dependency for `oceanfs-core`.
  The purity check (`cargo tree -p oceanfs-core | grep oceanfs-`) must be
  updated to allow `oceanfs-hash` as the sole exception.
- `HashOutput` now lives in a different crate from `HashKey` — these were
  co-located in `types/hash.rs`. `HashKey` stays in `oceanfs-core`
  (it's a routing concept, not a hashing concept)

### Neutral

- All existing code paths continue to produce identical hashes (same `blake3`
  upstream library)

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| Option B: Delete `oceanfs-hash` | Less work, no new dependency edges | Architecture diagram wrong, no central hashing abstraction | Chose Option A: implementation adds value (trait abstraction, centralization) |
| Keep `HashOutput` in `oceanfs-core` | No dependency change for core | `Hasher::finalize()` returning `HashOutput` creates circular dep concern | `HashOutput` is the return type of the hashing trait — it belongs with the trait |
| Make `Hasher` generic over output type | Flexible for future hash algorithms | Over-engineering; BLAKE3 is the mandated algorithm per spec §9.3.3 | YAGNI — spec mandates BLAKE3 |

## References

- Spec §9.3.3: BLAKE3 with runtime SIMD detection
- Structural audit (2026-08-03): `oceanfs-hash` identified as dead code
- ADR-0005: Trait-in-consuming-crate pattern (not applied here — `Hasher` trait
  lives in `oceanfs-hash` because it's fundamental to the domain)
- Perf rule 5.1: BLAKE3 with runtime SIMD detection
- Perf rule 5.2: Streaming hash — never buffer the full blob
- Perf rule 6.4: Static dispatch over dynamic dispatch on hot paths
