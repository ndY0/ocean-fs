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

- [x] **Code:** `cargo build --all-targets` succeeds in affected crates
- [x] **Tests:** Unit tests: stripe count for exact division, stripe count with remainder (padding), padding round-trip (encode padded → decode → strip padding), parallel encode vs sequential encode (same output), semaphore bounds concurrency, zero-size segment (error)
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-ec`
<!-- REVIEW R2: Same workspace-aggregation issue as Feature 10. stripe-specific source coverage: stripe/layout.rs 15/15 (100%), stripe/parallel.rs 74/78 (94.87%), stripe/batch.rs 2/2 (100%). The previously-reported uncovered ParallelDecoder::new semaphore branch is NOW COVERED by the new test `parallel_decode_with_semaphore_bounds_concurrency` (max_concurrency=2). Remaining 4 uncovered lines in parallel.rs (134, 136, 225, 227) are macro-expansion artifacts from `vec![vec![0u8; ...]; m]` — these lines execute but tarpaulin miscounts the generated code. Aggregate fails at 66.41% due to transitive crates. Oceanfs-ec core sources: 241/299 = 80.6%. See Feature 10 coverage comment for details. -->
- [x] **Lint:** `cargo clippy -- -D warnings` passes
- [x] **Docs:** `#![deny(missing_docs)]` passes; `ParallelEncoder` and `ParallelDecoder` have `# Examples`
<!-- REVIEW R2: Verified fixed. All doc examples now use ` ```rust` with proper imports. cargo test --doc -p oceanfs-ec: 10 passed, 0 failed (includes ParallelEncoder at parallel.rs:43, ParallelDecoder at parallel.rs:173, and ParallelEncoder::new at parallel.rs:67). -->
- [x] **ADR:** N/A
- [x] **Perf:** Rule 2.1 (rayon parallel iterators), 6.2 (SoA verified in StripeBatch), 9.4 (bytemuck verified), 2.7 (semaphore bounding), 1.3 (pre-size stripe vectors)
<!-- REVIEW R2: All rules satisfied. Rule 2.1: encode/decode use rayon `into_par_iter()`. Rule 6.2: StripeBatch holds SoA `data: Vec<Vec<u8>>` and `parity: Vec<Vec<u8>>`. Rule 9.4: bytemuck cast_shard_slice/cast_shard_slice_mut in shard.rs. Rule 2.7: Semaphore in ParallelEncoder/ParallelDecoder (0 = unlimited, N > 0 = bounded). Rule 1.3: ParallelEncoder::encode uses `Vec::with_capacity(k)` and pre-allocates `vec![0u8; total_stripes * shard_size]`. Rule 6.4: ParallelEncoder/ParallelDecoder now generic with `?Sized` — static dispatch on hot path (see Feature 10). No `std::sync::Mutex`, no `std::sync::RwLock`, no `Box<dyn>` in oceanfs-ec. -->
- [x] **Integration:** `tests/stripe_parallelism.rs`: encode 4 MB segment with k=4,m=2, verify 16 stripes produced, verify encode time scales with core count, decode with 1 missing shard per stripe, verify all data recovered
- [x] **Manual:** Example in `ParallelEncoder` docs compiles and runs
<!-- REVIEW R2: Verified fixed. cargo test --doc -p oceanfs-ec passes 10/10 doc tests including ParallelEncoder (parallel.rs:43), ParallelDecoder (parallel.rs:173), and ParallelEncoder::new (parallel.rs:67). All use ` ```rust` with proper imports. -->
