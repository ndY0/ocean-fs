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

- [ ] **Code:** `cargo build --features cuda` succeeds in `oceanfs-accel`; `cargo build` (no default features) also succeeds
- [ ] **Tests:** CUDA encode/decode round-trip (bit-exact output matches CPU Cauchy RS), GPU unavailable → falls back to CPU (no panic), semaphore bounds concurrent GPU ops, min_segment_size filter (small segments use CPU), kernel correctness for edge cases (k=1, m=0, k=16, m=8)
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-accel` (with cuda feature)
- [ ] **Lint:** `cargo clippy -- -D warnings` passes; unsafe blocks have `// SAFETY:` comments
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `CudaBackend` documented with GPU requirements
- [ ] **ADR:** ADR-0006 constraints satisfied — trait-based pluggability via Encoder/Decoder traits (§3), GPU concurrency model with Semaphore (§4), startup probing (§1), GPU cooldown on failure (§2 runtime fallback), feature-gated compilation (§6)
- [ ] **Perf:** Rule 2.7 (GPU semaphore), 7.1 (minimal GPU device lock hold time)
- [ ] **Integration:** `tests/gpu_ec_roundtrip.rs` (requires GPU): encode 100 MB segment on GPU, decode on CPU (or vice versa), verify bit-exact match; encode 10 KB segment → verify CPU fallback used
- [ ] **Manual:** Example in `CudaBackend` docs compiles and runs (with GPU)
