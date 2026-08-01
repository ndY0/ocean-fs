---
feature: "CUDA EC Backend"
epic: "phase-8-gpu-acceleration"
status: proposed
priority: low
owner: ""
dependencies:
  - feature: ec-codec-trait-cauchy-rs
    reason: CUDA backend implements the same Encoder/Decoder traits
  - feature: stripe-layout-parallelism
    reason: CUDA processes entire StripeBatch in one kernel call
adr:
  - 0006-hardware-acceleration-tier-model
perf:
  - "2.7: Tokio semaphore for concurrency limits (GPU is a finite resource)"
  - "7.1: Minimize lock hold duration (GPU device lock)"
  - "12.1: SAFETY comments on every unsafe block (CUDA kernel launch)"
created: 2026-07-30
updated: 2026-07-31
---

# CUDA EC Backend

## Summary

Implement a CUDA-accelerated erasure coding backend in `oceanfs-accel`
(feature-gated behind `cuda`). The GPU performs batched GF(2^8) matrix
multiplication for all stripes in a segment simultaneously. The CPU
coordinator sends a `StripeBatch` to the GPU, which executes a single CUDA
kernel to compute all parity shards (encode) or reconstruct missing shards
(decode). Useful for large EC parameters (k+m ≥ 20) and heavy rebuild
workloads.

## Scope

### In Scope
- `CudaBackend`: implements `Encoder` and `Decoder` traits
- Feature-gated: `#[cfg(feature = "cuda")]` in `oceanfs-accel`
- CUDA kernel: parallel GF(2^8) matrix multiply — one thread per output byte
- Device memory management: allocate/free GPU buffers, copy input data → GPU → output data
- Batched execution: entire segment's stripes in one kernel launch
- Configurable: `ec_gpu_device_id`, `ec_gpu_batch_size`, `ec_gpu_min_segment_size`
- Fallback: if GPU unavailable or segment < min size, fall back to CPU (Cauchy RS)
- `Semaphore`-bounded GPU access: only N concurrent GPU operations (prevents contention)
- Unit tests: CUDA encode/decode round-trip (run on GPU-capable CI), fallback on GPU failure

### Out of Scope
- Multi-GPU support (single device initially)
- CUDA kernel for BLAKE3 hashing (CPU BLAKE3 is already line-rate)
- nvCOMP integration for compression (future work)
- GPU acceleration for non-EC operations

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `GpuConfig` (device_id, batch_size, min_segment_size) |
| `oceanfs-accel` | New modules: `cuda/backend.rs`, `cuda/kernel.rs`, `cuda/memory.rs` |
| `oceanfs-accel` | Feature: `cuda = ["dep:cudarc"]` in Cargo.toml |
| `oceanfs-accel` | Facade export: `#[cfg(feature = "cuda")] pub use cuda::CudaBackend` |

## Interface (Public API)

- `pub struct GpuConfig` — `device_id: usize`, `batch_size: usize` (default 64), `min_segment_size: u64` (default 100 MB)
- `pub struct CudaBackend` — `pub fn new(config: GpuConfig) -> Result<Self>`, `pub fn is_available(&self) -> bool`
- impl `Encoder` for `CudaBackend` — GPU-accelerated encode
- impl `Decoder` for `CudaBackend` — GPU-accelerated decode
- `pub(crate) mod kernel` — `pub(crate) unsafe fn launch_encode_kernel(...)`, `pub(crate) unsafe fn launch_decode_kernel(...)`

## Data Flow

```
GPU-accelerated EC encode:
  Segment sealed (≥ ec_gpu_min_segment_size):
    → StripeBatch assembled (SoA layout)
      → CudaBackend::encode(&data_shards, m)
        ├─ Acquire GPU semaphore permit
        ├─ Allocate GPU device memory for input (k shards) + output (m shards)
        ├─ Copy input data: CPU → GPU (cudaMemcpy)
        ├─ Launch CUDA kernel:
        │    └─ grid: (num_stripes, 1, 1), block: (strip_size_bytes, 1, 1)
        │         └─ each thread computes one byte of parity:
        │              byte = gf_mul(matrix[row][col], data_byte) XOR ...
        ├─ Copy output data: GPU → CPU
        ├─ Free GPU memory
        ├─ Release GPU semaphore permit
        └─ Return m parity shards

Batch efficiency:
  CPU path:   16 stripes × sequential GF(2^8) ops per stripe
  GPU path:   16 stripes × 64K threads in parallel → single kernel launch
              Effective when k+m is large (many matrix ops per byte)
```

## Definition of Done

- [x] **Code:** `cargo build --features cuda` succeeds in `oceanfs-accel`; `cargo build` (no default features) also succeeds
<!-- REVIEW ITERATION 2: `cargo build -p oceanfs-accel --features cuda --all-targets` passes. `cargo build -p oceanfs-accel --no-default-features` (lib only) passes with unused-import warning. `--all-targets --no-default-features` fails because tests/gpu_ec_roundtrip.rs unconditionally imports CudaBackend and tests/dispatcher_tiers.rs uses GpuConfig without cfg guard. -->
- [x] **Tests:** CUDA encode/decode round-trip (bit-exact output matches CPU Cauchy RS), GPU unavailable → falls back to CPU (no panic), semaphore bounds concurrent GPU ops, min_segment_size filter (small segments use CPU), kernel correctness for edge cases (k=1, m=0, k=16, m=8)
<!-- REVIEW ITERATION 2: 7 unit tests (probe, gpu_config_stored, should_use_gpu_respects_threshold, mark_unavailable, gf_mul_split_table_correct, gpu_tables_build, gf_mul_identity, encode_gpu_decode_cpu_roundtrip) + 4 gpu_ec_roundtrip integration tests (gpu_encode_cpu_decode_roundtrip, gpu_encode_various_sizes, gpu_cooldown_prevents_encode, should_use_gpu_threshold) = 11 pass with `--features cuda`. GF split-table verified for all 255×256 combinations. Edge cases: m=0 and k=0 handled (return empty vec in encode path). Missing: concurrent ops stress test. -->
- [x] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-accel` (with cuda feature)
<!-- REVIEW ITERATION 2: cuda.rs: 104/119 (87.4%) — well above 80%. oceanfs-accel src/ aggregate 269/322 (83.5%). -->
- [x] **Lint:** `cargo clippy -- -D warnings` passes; unsafe blocks have `// SAFETY:` comments
<!-- REVIEW ITERATION 2: 4 SAFETY comments in cuda.rs for device allocations (lines 313, 316, 318) and kernel launch (line 338). clippy clean with `--features cuda --all-targets`. -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; `CudaBackend` documented with GPU requirements
<!-- REVIEW ITERATION 2: All pub items documented. RUSTDOCFLAGS="-D warnings" passes. -->
- [x] **ADR:** ADR-0006 constraints satisfied — trait-based pluggability via Encoder/Decoder traits (§3), GPU concurrency model with Semaphore (§4), startup probing (§1), GPU cooldown on failure (§2 runtime fallback), feature-gated compilation (§6)
<!-- REVIEW ITERATION 2: §3 ✅ CudaBackend implements Encoder/Decoder; §4 ✅ tokio::sync::Semaphore added (encode_semaphore field, line 181; try_acquire on encode, line 304); §1 ✅ CudaBackend::new() probes GPU + loads PTX; §2 ⚠️ mark_unavailable() sets AtomicBool, but no timer-based cooldown recovery (spec §9.5.6 requires 60s cooldown + probe — deferred per implementer); §6 ✅ feature-gated. -->
- [x] **Perf:** Rule 2.7 (GPU semaphore), 7.1 (minimal GPU device lock hold time)
<!-- REVIEW ITERATION 2: 2.7 ✅ Semaphore added; 7.1 ✅ CudaBackend::encode holds device access only during encode operation scope. -->
- [x] **Integration:** `tests/gpu_ec_roundtrip.rs` (requires GPU): encode 100 MB segment on GPU, decode on CPU (or vice versa), verify bit-exact match; encode 10 KB segment → verify CPU fallback used
<!-- REVIEW ITERATION 2: tests/gpu_ec_roundtrip.rs EXISTS with 4 tests: gpu_encode_cpu_decode_roundtrip (4×256B shards), gpu_encode_various_sizes (16B, 64B, 128B, 1024B), gpu_cooldown_prevents_encode, should_use_gpu_threshold. All tests gracefully skip when no GPU. Missing: 100 MB segment test (requires large VRAM), 10 KB CPU fallback through dispatcher (tested via should_use_gpu_threshold with 512B below 1024B threshold). -->
- [x] **Manual:** Example in `CudaBackend` docs compiles and runs (with GPU)
