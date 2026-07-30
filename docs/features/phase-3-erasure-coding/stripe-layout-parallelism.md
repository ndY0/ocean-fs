---
feature: "Stripe Layout & Intra-Segment Parallelism"
epic: "phase-3-erasure-coding"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: ec-codec-trait-cauchy-rs
    reason: Stripe layout feeds data into the codec's encode/decode methods
  - feature: segment-sealing-index
    reason: Stripe layout operates on sealed segment data
adr: []
perf:
  - "2.1: Rayon parallel iterators for EC stripe encode/decode"
  - "6.2: SoA layout for EC stripe data"
  - "9.4: bytemuck for zero-copy byte-to-struct casting"
  - "2.7: Tokio semaphore for concurrency limits"
  - "1.3: Pre-size collections with known capacity"
created: 2026-07-30
updated: 2026-07-30
---

# Stripe Layout & Intra-Segment Parallelism

## Summary

Implement the stripe layout engine and intra-segment parallel encode/decode in
`oceanfs-ec`. A sealed segment is split into independent stripes of size
`k × strip_size_bytes`. Each stripe is encoded/decoded independently using rayon
parallel iteration. The SoA (Struct of Arrays) memory layout ensures sequential
memory access during GF(2^8) matrix operations. A `tokio::sync::Semaphore`
bounds concurrent encode/decode operations to prevent resource exhaustion.

## Scope

### In Scope
- `StripeLayout`: compute stripe dimensions from segment size, k, m, strip_size
- `StripeBatch`: SoA layout holding `[data_shards; k]` and `[parity_shards; m]` as contiguous arrays
- `ParallelEncoder`: wraps an `Encoder`, dispatches all stripes via `rayon::par_iter()`
- `ParallelDecoder`: wraps a `Decoder`, dispatches all stripes via `rayon::par_iter()`
- Padding logic: final stripe padded to full strip_size with zeros (discarded on decode)
- `EncodingPlan`: pre-computed plan for a segment (stripe count, padding, shard sizes)
- Semaphore-bounded parallelism: configurable `ec_parallel_stripes` (0 = auto = num_cpus)
- `bytemuck` casts: interpret raw segment bytes as structured stripe data with zero copy
- Unit tests for layout computation, padding round-trip, parallelism correctness

### Out of Scope
- GPU-accelerated batch encoding (Phase 8)
- Inter-segment parallelism (separate feature: pipeline-parallelism in Phase 4)
- Healing orchestration (Phase 4 read path + Phase 7 scrubbing)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `StripeLayout`, `EncodingPlan` |
| `oceanfs-ec` | New modules: `stripe/layout.rs`, `stripe/batch.rs`, `stripe/parallel.rs` |
| `oceanfs-ec` | New facade export: `pub use stripe::ParallelEncoder`, `pub use stripe::ParallelDecoder` |

## Interface (Public API)

- `pub struct StripeLayout` — `pub fn compute(segment_size: u64, k: u8, m: u8, strip_size: usize) -> EncodingPlan`
- `pub struct EncodingPlan` — `stripe_count: usize`, `padded_size: u64`, `shard_size: usize`
- `pub struct StripeBatch` — SoA: `data: Vec<Vec<u8>>` (k vectors), `parity: Vec<Vec<u8>>` (m vectors)
- `pub struct ParallelEncoder` — `pub fn new(encoder: Arc<dyn Encoder>, max_concurrency: usize) -> Self`, `pub fn encode(&self, segment_data: &[u8], plan: &EncodingPlan) -> Result<StripeBatch>`
- `pub struct ParallelDecoder` — `pub fn new(decoder: Arc<dyn Decoder>, max_concurrency: usize) -> Self`, `pub fn decode(&self, available: &StripeBatch, plan: &EncodingPlan, missing_indices: &[usize]) -> Result<Vec<Vec<u8>>>`

## Data Flow

```
Segment encoding:
  Sealed segment: 4 MB raw data
    → StripeLayout::compute(4MB, k=4, m=2, strip=64KB)
        → EncodingPlan { stripe_count: 16, padded_size: 4194304, shard_size: 65536 }
          → split segment into 16 stripes, each = 4 × 64KB = 256 KB of data
            → ParallelEncoder::encode(segment_data, plan)
              └─ rayon::par_iter over 16 stripes:
                   for each stripe:
                     ├─ extract k data shards (zero-copy via bytemuck)
                     ├─ encoder.encode(&data_shards, m=2) → 2 parity shards
                     └─ collect into StripeBatch
                → Semaphore permit acquired before spawning (bounds concurrency)

Segment decoding (read path):
  Fetch k of k+m shards from nodes
    → assemble StripeBatch from available shards
      → determine missing_indices
        → ParallelDecoder::decode(available, plan, missing_indices)
          └─ rayon::par_iter over stripes:
               for each stripe with missing shards:
                 ├─ decoder.decode(available_row, k=4, m=2) → reconstructed data
                 └─ (stripes with all k data shards: skip decode, copy directly)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in affected crates
- [ ] **Tests:** Unit tests: stripe count for exact division, stripe count with remainder (padding), padding round-trip (encode padded → decode → strip padding), parallel encode vs sequential encode (same output), semaphore bounds concurrency, zero-size segment (error)
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-ec`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `ParallelEncoder` and `ParallelDecoder` have `# Examples`
- [ ] **ADR:** N/A
- [ ] **Perf:** Rule 2.1 (rayon parallel iterators), 6.2 (SoA verified in StripeBatch), 9.4 (bytemuck verified), 2.7 (semaphore bounding), 1.3 (pre-size stripe vectors)
- [ ] **Integration:** `tests/stripe_parallelism.rs`: encode 4 MB segment with k=4,m=2, verify 16 stripes produced, verify encode time scales with core count, decode with 1 missing shard per stripe, verify all data recovered
- [ ] **Manual:** Example in `ParallelEncoder` docs compiles and runs
