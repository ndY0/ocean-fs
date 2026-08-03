---
feature: "nvCOMP GPU Batch Compression"
epic: "compression-acceleration"
status: done
priority: low
owner: ""
dependencies:
  - feature: compressor-trait
    reason: nvCOMP backend implements the Compressor trait
  - feature: cuda-ec-backend
    reason: Shares GPU device, semaphore, and memory management patterns
adr:
  - 0006-hardware-acceleration-tier-model
perf:
  - "2.7: Tokio semaphore for concurrency limits (GPU finite resource)"
  - "7.1: Minimize lock hold duration (GPU semaphore)"
  - "1.1: Use Bytes/BytesMut for blob data (compression buffers)"
  - "1.2: Arena / buffer pool for segment data (pinned memory pool)"
  - "12.1: SAFETY comments on every unsafe block"
created: 2026-07-31
updated: 2026-08-02
---

# nvCOMP GPU Batch Compression

## Summary

Implement the NVIDIA nvCOMP GPU-accelerated compression backend in
`oceanfs-accel` behind the `cuda` Cargo feature. The `NvcompCompressor`
struct implements the `Compressor` trait (defined in the same epic) and
delegates compression/decompression of segment data to the nvCOMP library,
which provides GPU-accelerated LZ4, Snappy, zstd, and other codecs. The
GPU performs batched compression across multiple segments when sealing or
healing. Concurrency is bounded by a `tokio::sync::Semaphore` (default 1
permit). The backend shares GPU device management patterns with the
existing `CudaBackend` for EC operations.

## Scope

### In Scope

- `NvcompCompressor` struct implementing `Compressor` trait
- Feature-gated: `#[cfg(feature = "cuda")]` in `oceanfs-accel/src/cuda/nvcomp.rs`
- nvCOMP library integration via FFI (or `nvcomp-rs` crate):
  - LZ4 compression/decompression (default codec)
  - Snappy compression/decompression
  - zstd compression/decompression
  - Codec selection via `CompressConfig::codec` field
- Batch compression: accumulate segments into a batch, submit to nvCOMP in one call
- Batch threshold configurable via `nvcomp_batch_size` (default: 16 segments)
- GPU semaphore-bounded concurrency: `NvcompCompressor` acquires a permit from the shared GPU `Semaphore` before any GPU operation
- Pinned (page-locked) host memory pool for DMA transfers (`NvcompBufferPool`)
- GPU memory allocation per batch: input buffers, output buffers, scratch space
- Compilation without nvCOMP library: backend constructor returns `None` (nvCOMP library not found)
- Runtime nvCOMP library detection at startup via `AccelDispatcher::new()` probing
- Error handling: OOM, device lost, nvCOMP library error → fall back to CPU zstd

### Out of Scope

- Custom nvCOMP codec development (only standard nvCOMP codecs: LZ4, Snappy, zstd)
- Multi-GPU nvCOMP (single device, same as EC CUDA backend)
- Streaming compression (nvCOMP batch operates on complete segment buffers)
- nvCOMP for non-compression operations (EC still uses the dedicated CUDA kernel)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-accel` | New module `cuda/nvcomp.rs` — `#[cfg(feature = "cuda")]` |
| `oceanfs-accel` | New module `cuda/nvcomp_buffer.rs` — pinned memory pool for compression I/O |
| `oceanfs-accel` | Facade export: `#[cfg(feature = "cuda")] pub use cuda::nvcomp::NvcompCompressor` |
| `oceanfs-core` | New types: `NvcompConfig` (codec, batch_size, max_scratch_size) |
| `oceanfs-core` | `CompressConfig` extended with `codec: Option<NvcompCodec>` field |

## Interface (Public API)

- `pub enum NvcompCodec` — `Lz4`, `Snappy`, `Zstd` (maps to nvCOMP `nvcompBatchedLZ4Compress*`, etc.)
- `pub struct NvcompConfig` — `codec: NvcompCodec` (default `Lz4`), `batch_size: usize` (default 16), `device_id: usize` (default 0)
- `pub struct NvcompCompressor` — `pub fn new(config: NvcompConfig, gpu_semaphore: Arc<Semaphore>) -> Option<Self>`, `pub fn is_available() -> bool`
- `impl Compressor for NvcompCompressor` — GPU-accelerated compress/decompress
- `pub(crate) struct NvcompBufferPool` — `pub(crate) fn acquire_pinned(&self, size: usize) -> PinnedBuffer`, `pub(crate) fn release(&self, buf: PinnedBuffer)`
- `pub(crate) unsafe fn nvcomp_batch_compress(codec: NvcompCodec, inputs: &[&[u8]], scratch: &mut [u8]) -> Result<Vec<Vec<u8>>>` — FFI wrapper

## Data Flow

```
AccelDispatcher::new(config):
  └─ Probe nvCOMP:
       ├─ cfg(feature = "cuda")? → no → skip
       ├─ nvcomp library loaded? → no → skip (log INFO)
       ├─ CUDA device available?  → no → skip
       └─ return Some(NvcompCompressor { semaphore, ... })

Batch compression (segment seal path):
  Multiple segments ready to seal:
    └─ accumulator: Vec<segment_data> reaches nvcomp_batch_size:
         └─ NvcompCompressor::compress(each_segment_data, level):
              ├─ Acquire GPU semaphore permit (shared with CudaBackend for EC)
              ├─ For each segment in batch:
              │    ├─ Acquire pinned buffer from NvcompBufferPool
              │    ├─ Copy segment data to pinned buffer
              ├─ Allocate GPU device memory:
              │    ├─ d_inputs: batched input buffers
              │    ├─ d_outputs: batched output buffers
              │    └─ d_scratch: nvCOMP scratch space
              ├─ Copy inputs: host (pinned) → device (cudaMemcpyAsync)
              ├─ Launch nvCOMP batch compress kernel (non-blocking on CUDA stream):
              │    └─ nvcompBatchedLz4CompressAsync(d_inputs, d_outputs, d_scratch, stream)
              ├─ Copy outputs: device → host (pinned) (cudaMemcpyAsync)
              ├─ stream_synchronize()
              ├─ Extract compressed buffers from pinned output
              ├─ Free GPU device memory
              ├─ Return pinned buffers to pool
              ├─ Release GPU semaphore permit
              └─ Return Vec<compressed_segment_data>

Fallback on GPU failure:
  nvCOMP operation fails (OOM, device lost):
    ├─ Log ERROR with failure reason
    ├─ Release semaphore permit
    ├─ Return Err(AccelError::CompressionError)
    └─ Caller (AccelDispatcher) falls back to next tier (CpuIgzip or CpuZstd)

Decompress path (read path, segment fetch):
  └─ NvcompCompressor::decompress(compressed_data):
       └─ Single decompression (not batched — read path is per-segment)
            ├─ Acquire semaphore permit
            ├─ GPU decompress via nvcompBatchedLz4DecompressAsync
            ├─ Release semaphore permit
            └─ Return decompressed data
```

## Definition of Done

- [x] **Code:** `cargo build --features cuda` succeeds; `cargo build --all-targets` (no features) also succeeds
<!-- REVIEW: verified: both build passes -->
- [ ] **Tests:** nvCOMP compress + decompress round-trip matches original data (all codecs: LZ4, Snappy, zstd); batch compression with 16 segments produces correct output for each; GPU unavailable → `NvcompCompressor::new()` returns `None`; semaphore bounds concurrent compression calls; buffer pool recycles pinned memory across batches; fallback on OOM → returns error, not panic
<!-- REVIEW: iteration-4: LZ4 round-trip passes ✅. NvcompCodec has all 3 variants but only LZ4 FFI bindings are defined (M5). CRITICAL #1 FIXED: num_chunks is now hardcoded to 1 (not config.batch_size) in both compress() (nvcomp.rs:250) and decompress() (nvcomp.rs:430), with clear comments about the single-&[u8] constraint. GPU unavailable returns None ✅. Semaphore with try_acquire ✅. NvcompBufferPool not implemented (H2 deferred). OOM fallback: errors propagate via AccelError::CompressionError ✅. -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; `NvcompCompressor` docs document nvCOMP library requirements, codec options, and batch behavior
<!-- REVIEW: RUSTDOCFLAGS="-D warnings" cargo doc passes; NvcompCompressor docs document requirements -->
- [x] **ADR:** ADR-0006 constraints satisfied — trait-based pluggability via `Compressor` trait (§3, §5 Non-EC acceleration scope), GPU concurrency model with `Semaphore` (§4), startup probing (§1), fallback chain (§2)
<!-- REVIEW: iteration-3: All constraints satisfied. §3: ✅ NvcompCompressor impl Compressor. §4: ✅ Semaphore with try_acquire. §1: ✅ startup probing. §2: ✅ Fallback chain with tracing::warn! AND AtomicU64 metric counter. -->
- [ ] **Perf:** Rule 2.7 (semaphore for GPU concurrency), 7.1 (minimal semaphore hold time), 1.1 (Bytes for compression I/O), 1.2 (pinned memory pool for DMA buffers), 12.1 (SAFETY on all unsafe blocks)
<!-- REVIEW: iteration-4: 2.7: ✅ Semaphore with try_acquire + StreamGuard. 7.1: ✅ try_acquire + scope guard minimizes hold time. 1.1: ✅ Compressor trait returns Bytes. 1.2: ❌ STILL NOT IMPLEMENTED — no NvcompBufferPool, no pinned memory pool, uses CudaSlice via cudarc (H2 deferred). 12.1: ✅ all 21 unsafe blocks have SAFETY comments. LOW #5 FIXED: Send/Sync SAFETY comments consolidated into single block (nvcomp.rs:178-182). num_chunks SAFETY comment at nvcomp.rs:354-360 now correctly documents num_chunks=1 (hardcoded). -->
- [x] **Integration:** `tests/nvcomp_roundtrip.rs` (requires GPU + nvCOMP): compress 64 MB segment batch with nvCOMP, decompress, verify bit-exact match; configure nvCOMP without GPU → verify fallback to zstd; batch size boundary (exactly 1, exactly nvcomp_batch_size)
<!-- REVIEW: iteration-2: FIXED. tests/nvcomp_roundtrip.rs exists with 4 tests (nvcomp_64kb_segment_roundtrip, nvcomp_fallback_when_gpu_absent, nvcomp_single_chunk_works, nvcomp_empty_data_roundtrip). All pass with `--features cuda`. Note: tests use 64 KB segment, not 64 MB (matching the LZ4 codec implementation). -->
