---
feature: "EC Codec Trait & Cauchy Reed-Solomon"
epic: "phase-3-erasure-coding"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: phase-0-project-scaffold
    reason: Requires oceanfs-core (Error, CodecConfig) and crate layout
  - epic: phase-1-storage-engine
    reason: EC encodes sealed segments from the storage engine
adr: []
perf:
  - "2.1: Rayon parallel iterators for EC stripe encode/decode"
  - "6.2: SoA layout for EC stripe data"
  - "9.4: bytemuck for zero-copy byte-to-struct casting"
  - "4.3: Feature-gated SIMD compilation"
  - "6.4: Static dispatch over dynamic dispatch on hot paths"
created: 2026-07-30
updated: 2026-07-30
---

# EC Codec Trait & Cauchy Reed-Solomon

## Summary

Implement the erasure coding abstraction in `oceanfs-ec`. Define the `Encoder`
and `Decoder` traits, and provide a Cauchy Reed-Solomon implementation over
GF(2^8) as the default codec. The codec layer operates on raw byte slices and is
independent of segment/file semantics. ISA-L acceleration is available via
feature flag on x86. The codec is designed for SoA memory layout and rayon-based
stripe parallelism.

## Scope

### In Scope
- `trait Encoder`: `fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> Result<Vec<Vec<u8>>>`
- `trait Decoder`: `fn decode(&self, available_shards: &[Option<&[u8]>], k: u8, m: u8) -> Result<Vec<Vec<u8>>>`
- `CodecConfig`: `codec_type: CodecType`, `k: u8`, `m: u8`, `strip_size_bytes: usize`
- Cauchy Reed-Solomon implementation using GF(2^8) arithmetic
- Cauchy matrix generation: deterministic matrix from (k, m) parameters
- Galois field operations: add (XOR), multiply (log/exp table), invert
- `#[cfg(feature = "isa-l")]` ISA-L accelerated path for encode/decode on x86
- GF-complete portable fallback for ARM and non-x86 platforms
- `ShardData` type: zero-copy wrapper via `bytemuck` for interpreting `&[u8]` as EC shards
- Property-based tests (proptest): round-trip encode/decode for random data, k, m
- Unit tests for Cauchy matrix properties, GF arithmetic correctness, edge cases (k=1, m=0)

### Out of Scope
- GPU/CUDA codec (Phase 8)
- Clay codes or LRC (future ADR-0003)
- Stripe-level batching and parallel dispatch (separate feature: stripe-layout-parallelism)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `CodecType` enum, `CodecConfig` |
| `oceanfs-ec` | New crate; modules: `traits.rs`, `cauchy.rs`, `gf.rs`, `matrix.rs`, `shard.rs` |
| `oceanfs-ec` | Facade exports: `pub use traits::{Encoder, Decoder}`, `pub use cauchy::CauchyEncoder` |

## Interface (Public API)

- `pub trait Encoder: Send + Sync` — `fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> Result<Vec<Vec<u8>>>`
- `pub trait Decoder: Send + Sync` — `fn decode(&self, available: &[Option<&[u8]>], data_count: u8, parity_count: u8) -> Result<Vec<Vec<u8>>>`
- `pub struct CodecConfig` — `codec_type: CodecType`, `data_shards: u8`, `parity_shards: u8`, `strip_size_bytes: usize`
- `pub enum CodecType` — `CauchyRs`, `StandardRs` (reserved), `Lrc` (reserved), `Clay` (reserved)
- `#[non_exhaustive]` on `CodecType`
- `pub struct CauchyEncoder` — `pub fn new(config: CodecConfig) -> Self`
- `pub struct ShardData<'a>` — zero-copy view over `&'a [u8]` as EC shards

## Data Flow

```
EC Encode (CPU path):
  Segment data: [4 MB buffer]
    → split into stripes: each stripe = k × strip_size_bytes
      → for each stripe row (k data shards):
           CauchyEncoder::encode(&data_shards, m)
             → generate Cauchy matrix G: m × k over GF(2^8)
               → multiply G × [data_shards] → m parity shards
                 → return k data + m parity shards (k+m total)

EC Decode (CPU path):
  Available shards: [Some(d0), None, Some(d2), Some(d3), None, Some(p0)]
    → Identify missing shard indices
      → Build decode matrix: invert surviving rows of generator matrix
        → multiply by available data → recover missing shards
          → return reconstructed k data shards
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-core` and `oceanfs-ec`
- [ ] **Tests:** Proptest: round-trip encode→decode for random data (1B–64KB), all k∈[1,16], m∈[1,8]; Cauchy matrix determinant non-zero; GF multiply commutativity/associativity; decode with exactly k available shards; decode with k+m-1 available; decode fails with < k available
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-ec`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `Encoder`/`Decoder` traits have `# Examples`
- [ ] **ADR:** N/A (ADR-0003 forthcoming; Cauchy RS rationale in spec §6.1)
- [ ] **Perf:** Rule 2.1 (rayon-ready design), 6.2 (SoA via ShardData), 9.4 (bytemuck zero-copy), 4.3 (feature-gated ISA-L), 6.4 (generic over codec, not dyn Trait)
- [ ] **Integration:** `tests/ec_roundtrip.rs`: encode segment-sized data (4 MB), introduce up to m erasures, decode, verify bit-exact original
- [ ] **Manual:** Example in `Encoder` docs compiles and runs
