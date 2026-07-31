# OceanFS — Specification §9: Hardware Acceleration (Expanded)

**Status:** Draft for spec-writer incorporation
**Based on:** ADR-0006 (Hardware Acceleration Tier Model)
**Date:** 2026-07-31

---

This document is the proposed replacement for the current §9 in `docs/spec.md`.
It provides the detailed specification for how OceanFS implements hardware
acceleration. The current §9.1–9.3 are brief overview sections; this draft
expands them into implementable detail.

The section numbering continues from the existing spec. Subsections marked
`[NEW]` are entirely new; subsections marked `[EXPANDED]` replace or
substantially extend existing content.

---

## 9. Hardware Acceleration

OceanFS accelerates computationally intensive operations through a three-tier
model that probes available hardware at startup and delegates work to the most
capable backend. The acceleration subsystem lives in `oceanfs-accel` and
implements the `Encoder`/`Decoder` traits from `oceanfs-ec`.

```
Operation              Tier 0 (baseline)        Tier 1 (optimized)         Tier 2 (offload)
------------------------------------------------------------------------------------------
BLAKE3 hashing         CPU (blake3 auto-detect) AVX-512 intrinsics         n/a (line-rate)
EC encode/decode       GF-complete (portable)   ISA-L (Intel, AVX-512)     CUDA kernel
                                               libec (ARM SVE, future)    (batch EC ops)
Compression (zstd)     CPU (zstd crate)         ISA-L igzip                nvCOMP (GPU batch)
Encryption (AES-GCM)   CPU (aes-gcm crate)      AES-NI intrinsics          GPU (future)
```

The tier is selected per bucket via `accel_ec_tier`. The `auto` tier probes:
CUDA → ISA-L → CPU SIMD, selecting the first available.

### 9.1 Acceleration Subsystem Architecture [EXPANDED]

The acceleration subsystem is composed of three layers:

```
oceanfs-ec                          ← trait definitions (Encoder, Decoder)
      ↑
oceanfs-accel                       ← backend implementations + dispatcher
      ↑
oceanfs-storage, oceanfs-server     ← consumers (via AccelDispatcher)
```

#### 9.1.1 Component Diagram

```
+-------------------------------------------------------------+
|                     AccelDispatcher                          |
|                                                              |
|  +------------------+  +------------------+  +------------+  |
|  | Tier 0           |  | Tier 1           |  | Tier 2     |  |
|  | CPU SIMD         |  | ISA-L            |  | GPU/CUDA   |  |
|  |                  |  |                  |  |            |  |
|  | GF-complete RS   |  | Intel ISA-L RS   |  | CUDA       |  |
|  | (portable)       |  | (AVX-512)        |  | kernel     |  |
|  |                  |  |                  |  |            |  |
|  | Runtime SIMD     |  | Feature: isa-l   |  | Feature:   |  |
|  | dispatch         |  |                  |  | cuda       |  |
|  | (SSE4.1/AVX2/    |  | libec (ARM SVE)  |  |            |  |
|  |  AVX-512)        |  | (future)         |  | nvCOMP     |  |
|  +------------------+  +------------------+  +------------+  |
|                                                              |
|  +------------------+  +-----------------------------------+ |
|  | Hash             |  | Compression / Encryption          | |
|  | BLAKE3 (auto)    |  | zstd crate / nvCOMP / AES-GCM    | |
|  +------------------+  +-----------------------------------+ |
+-------------------------------------------------------------+
        |
        | (implements Encoder, Decoder from oceanfs-ec)
        v
+------------------+
| Consumer         |
| ParallelEncoder  |
| WriteCoordinator |
| Heal Scheduler   |
+------------------+
```

#### 9.1.2 Backend Lifecycle

Every backend follows the same lifecycle:

```
Construction → Probe → Initialize → Available
                                    │
                                    ├── encode/decode calls (hot path)
                                    │
                                    └── Drop (release GPU memory, close FFI handles)
```

Backends that fail to probe (e.g., `CudaBackend` when no GPU present, `IsalEncoder`
when AVX-512 absent) are never constructed. The dispatcher skips them during tier
resolution.

#### 9.1.3 Concurrency Model

Each backend declares its own concurrency characteristics:

| Backend | Concurrency | Mechanism |
|---|---|---|
| CPU SIMD | Unlimited (CPU-bound, rayon work-stealing) | None needed |
| ISA-L | Unlimited (CPU-bound, single-threaded per stripe) | None needed |
| CUDA | Semaphore-bounded (default 1) | `tokio::sync::Semaphore` |

The CUDA semaphore is acquired before every GPU operation and released on
completion. This serializes GPU access because GF(2^8) matrix multiplication
saturates GPU compute with a single kernel launch. Multiple concurrent launches
contend for SMs and memory bandwidth, reducing total throughput through context
switching overhead.

```toml
ec_gpu_max_concurrent_ops = 1   # permits for the GPU semaphore
```

### 9.2 Backend Discovery & Selection [NEW]

#### 9.2.1 Startup Probing

When `AccelDispatcher::new(config)` is called at node startup, it performs:

1. **Tier 0 (CPU SIMD):** Always available. Constructs `CauchyEncoder` from
   `oceanfs-ec`. GF arithmetic uses runtime CPU feature detection (SSE4.1, AVX2,
   AVX-512) via `std::is_x86_feature_detected!` or equivalent on ARM (NEON).

2. **Tier 1 (ISA-L):** Available if:
   - The `isa-l` Cargo feature is enabled at compile time.
   - `CPUID` reports AVX-512F + AVX-512BW at runtime.
   - The ISA-L shared library (`libisal.so`) can be loaded (via FFI binding).
   
   If any check fails, ISA-L is marked unavailable with a `DEBUG` log (not a
   warning, since ISA-L absence is expected on most hardware).

3. **Tier 2 (CUDA):** Available if:
   - The `cuda` Cargo feature is enabled at compile time.
   - `cudarc::init()` succeeds.
   - At least one CUDA device is present (`device_count > 0`).
   - The device has sufficient VRAM for the configured `ec_gpu_batch_size`
     (minimum 256 MB).
   
   If any check fails, CUDA is marked unavailable with a `DEBUG` log.

4. **Tier 2 (nvCOMP):** Available if CUDA is available AND the nvCOMP library
   (`libnvcomp.so`) can be loaded. nvCOMP is probed independently from the
   CUDA EC kernel — a system may have CUDA for EC but not nvCOMP for compression.

**Probing latency:** <200ms in the common case. CPUID is a single instruction.
CUDA device enumeration is ~50ms. Library loading is ~10ms.

#### 9.2.2 Tier Resolution

After probing, the dispatcher resolves the effective tier:

```
Requested Tier    Available Backends         Resolved Tier
────────────────  ──────────────────────     ─────────────
Auto              CUDA, ISA-L, CPU           CUDA
Auto              ISA-L, CPU                 ISA-L
Auto              CPU                        CPU SIMD
GpuCuda           CUDA, ISA-L, CPU           CUDA
GpuCuda           ISA-L, CPU                 ISA-L (+ WARN)
GpuCuda           CPU                        CPU SIMD (+ WARN)
IsaL              ISA-L, CPU                 ISA-L
IsaL              CPU                        CPU SIMD (+ WARN)
CpuSimd           CPU                        CPU SIMD
```

When a fallback occurs, the dispatcher:
1. Logs at `WARN` level: `"GPU acceleration requested but CUDA unavailable; falling back to ISA-L"`
2. If ISA-L is also unavailable: `"ISA-L not available; falling back to CPU SIMD"`
3. Increments the `accel_fallback_total` counter (labeled by `from_tier` and `to_tier`)

Falling back from `Auto` (where no explicit tier was requested) produces a
`DEBUG` log, not a `WARN` — because `Auto` means "use whatever is best."

#### 9.2.3 Caching

The resolved backend is cached for the lifetime of the `AccelDispatcher`:

```rust
struct AccelDispatcher {
    encoder: Arc<dyn Encoder>,    // cached, no branches on hot path
    decoder: Arc<dyn Decoder>,
    active_tier: AccelTier,
    // Per-tier caches for per-bucket overrides
    tier_encoders: HashMap<AccelTier, Arc<dyn Encoder>>,
    tier_decoders: HashMap<AccelTier, Arc<dyn Decoder>>,
}
```

There is no re-probing at runtime. Hardware does not change while a process
runs. If the GPU is hot-unplugged (an extremely rare event), the CUDA kernel
launch will fail with an error — the caller receives an `Err` and the healing
or write path retries with CPU SIMD.

#### 9.2.4 Per-Bucket Override

A bucket may specify `accel_ec_tier` in its policy. When `WriteCoordinator`
or `ReadCoordinator` calls the dispatcher for a bucket-scoped operation:

1. If the bucket's tier matches the node's tier: use the cached backend (no
   allocation).
2. If the bucket's tier differs: resolve against available hardware and return
   a temporary `Arc<dyn Encoder>` from the per-tier cache. If the bucket requests
   a tier that is unavailable, the fallback chain applies.

```
WriteCoordinator::put(bucket, key, data):
  encoder = dispatcher.resolve_for_bucket(bucket.accel_ec_tier)
  → if bucket tier == GpuCuda but GPU absent → fallback to ISA-L → WARN
  → encode proceeds with ISA-L
```

### 9.3 Tier 0: CPU SIMD [EXPANDED]

#### 9.3.1 GF-Complete Portable Path

The baseline EC codec is the Cauchy Reed-Solomon implementation in `oceanfs-ec`
(specified in §6.1). It uses GF(2^8) arithmetic with log/exp lookup tables for
multiplication and division. This path requires no SIMD instructions and runs
on any CPU.

#### 9.3.2 Runtime SIMD Dispatch

The GF arithmetic layer in `oceanfs-ec` uses runtime CPU feature detection to
select the fastest available multiplication path:

```
GF(2^8) multiply:
  ├── AVX-512 (VPCLMULQDQ): 512-bit carry-less multiply → ~4× faster than lookup
  ├── AVX2 (PCLMULQDQ):     128-bit carry-less multiply → ~2× faster than lookup
  ├── SSE4.1:               vectorized table lookup        → ~1.5× faster
  └── Portable:             log/exp table lookup            → baseline
```

Detection uses `std::is_x86_feature_detected!` on x86 and
`std::arch::is_aarch64_feature_detected!` on ARM. The selected implementation
is cached in a `static AtomicU8` set once at first GF operation.

#### 9.3.3 BLAKE3 Hashing

BLAKE3 hashing uses the upstream `blake3` crate, which performs its own runtime
CPU feature detection at program initialization. OceanFS does not implement
custom BLAKE3 acceleration. The `accel_hash_tier` configuration is a
pass-through:

- `"auto"`: use the `blake3` crate's default (auto-detect AVX-512, AVX2, SSE4.1, NEON)
- `"avx512"`: force AVX-512 implementation (useful for benchmarking; falls back to portable if unavailable)

No GPU path for BLAKE3 is planned — the crate achieves ~4 GB/s/core on AVX-512,
which is line-rate for any realistic network throughput.

### 9.4 Tier 1: ISA-L / libec [NEW]

#### 9.4.1 Intel ISA-L (x86)

Intel's Intelligent Storage Acceleration Library (ISA-L) provides hand-tuned
AVX-512 assembly for Reed-Solomon encode and decode. It achieves line-rate
encoding for EC parameters up to k=24, m=8 on a single core.

**Integration:**

```rust
// oceanfs-accel/src/isal.rs (feature-gated)
pub struct IsalEncoder {
    // FFI handles to ISA-L encode/decode tables
}

impl Encoder for IsalEncoder {
    fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> Result<Vec<Vec<u8>>> {
        // Calls ISA-L C functions via FFI:
        //   ec_encode_data(strip_size, k, m, encode_table,
        //                  data_ptrs, parity_ptrs)
    }
}
```

**FFI surface:** The ISA-L binding exposes exactly two functions:

| Function | Signature | Purpose |
|---|---|---|
| `ec_init_tables` | `(k, m, &mut [u8; 32*k*m])` | Precompute encoding matrix tables |
| `ec_encode_data` | `(len, k, m, &tables, &[&[u8]; k], &mut [&mut [u8]; m])` | Encode k data shards → m parity shards |

The decode path uses the same functions with a reconstructed matrix.

**Safety:** The ISA-L FFI is `unsafe`. All calls are wrapped in `// SAFETY:`
blocks that verify:
- Input pointers are non-null and aligned to 64 bytes
- Lengths match k × strip_size_bytes
- The encode table was initialized with matching k,m parameters
- The FFI function is guaranteed to be thread-safe by the ISA-L documentation

#### 9.4.2 ARM NEON + SVE / libec

ARM deployments use architecture-specific SIMD paths. The Tier 1 backend on ARM
is a Rust-native implementation using NEON and SVE intrinsics — not an FFI
binding to a C library. This avoids the build-time complexity of cross-compiling
ISA-L (which is Intel x86-only) and keeps the `unsafe` surface auditable in pure
Rust.

**Feature detection (at startup, cached):**

```
Probe ARM capabilities:
  ├── SVE2 available?  → use SVE2 256-bit GF(2^8) multiply  (Graviton4, Neoverse V2)
  ├── SVE available?   → use SVE 128-bit GF(2^8) multiply   (Graviton3, Neoverse V1)
  ├── NEON available?  → use NEON 128-bit GF(2^8) multiply   (Graviton2, Apple M1/M2)
  └── none             → portable GF-complete (log/exp tables)
```

**SVE GF(2^8) multiply kernel (conceptual):**

SVE's key advantage for EC is predicated vector operations — the same kernel
handles any vector width (128–2048 bits) without recompilation:

```rust
// oceanfs-accel/src/arm_sve.rs
#[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]
pub struct ArmEncoder {
    sve_level: ArmSveLevel,  // SVE2, SVE, NEON, or Portable
}

impl Encoder for ArmEncoder {
    fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> Result<Vec<Vec<u8>>> {
        match self.sve_level {
            ArmSveLevel::Sve2  => encode_sve2(data_shards, parity_count),
            ArmSveLevel::Sve   => encode_sve(data_shards, parity_count),
            ArmSveLevel::Neon  => encode_neon(data_shards, parity_count),
            ArmSveLevel::Portable => cauchy_portable_encode(data_shards, parity_count),
        }
    }
}
```

**SVE vs ISA-L performance parity:**

SVE2 on Graviton4 achieves comparable throughput to AVX-512 on x86 for EC
operations because GF(2^8) multiplication is compute-bound, not memory-bound.
Both ISAs perform the same number of XOR + table-lookup operations per byte.

| Architecture | SIMD Width | GF(2^8) Bytes/Cycle | Relative Throughput |
|---|---|---|---|
| x86 AVX-512 (ISA-L) | 512-bit | 64 bytes/cycle | 1.0× (baseline) |
| ARM SVE2 (256-bit) | 256-bit | 32 bytes/cycle | ~0.5× per core |
| ARM NEON | 128-bit | 16 bytes/cycle | ~0.25× per core |
| Portable | — | ~1 byte/cycle | ~0.01× |

ARM servers typically have higher core counts (64–128 cores on Graviton),
so aggregate throughput with SVE across all cores exceeds x86 throughput with
fewer cores.

**Feature gate:** `arm-sve` in `oceanfs-accel/Cargo.toml`:

```toml
[features]
arm-sve = []   # enables SVE/NEON intrinsics on aarch64
```

The `arm-sve` feature compiles only on `aarch64`. On x86, it is a no-op. The
dispatcher probes for the `arm-sve` feature at compile time and for SVE at
runtime, selecting the best available ARM path.

**Epic placement:** ARM SVE / libec is part of the **CPU Acceleration Backends**
epic (separate from Phase 3 EC codec traits and Phase 8 GPU). It is implemented
alongside ISA-L so both CPU-optimized paths ship together.

### 9.5 Tier 2: GPU / CUDA [EXPANDED]

#### 9.5.1 GPU Usage Model

GPUs accelerate **batch EC operations** — not per blob. When a segment is
sealed, or a node is being rebuilt, the CPU coordinator sends a batch of
stripe rows to the GPU:

```
+----------+     batch of stripes        +---------------+
|   CPU    | --------------------------->|  GPU kernel   |
| (coord)  | <---------------------------|  GF(2^8) mat  |
+----------+     parity/decode shards    |  multiply     |
                                         +---------------+
```

#### 9.5.2 CUDA Kernel Design

The GPU kernel performs GF(2^8) matrix multiplication for all stripes in a
segment simultaneously:

```
Kernel: gf256_encode_stripes
  Input:  data_shards[k][strip_size]    (k data shards, 64 KB each)
  Output: parity_shards[m][strip_size]  (m parity shards)
  Matrix: encode_matrix[m][k]           (precomputed on CPU, copied to GPU constant memory)

  Grid:  (num_stripes, 1, 1)            // one block per stripe
  Block: (strip_size, 1, 1)            // one thread per byte

  Each thread (stripe s, byte position b):
    for j in 0..m:
      acc = 0
      for i in 0..k:
        acc ^= gf_mul(encode_matrix[j][i], data_shards[i][s][b])
      parity_shards[j][s][b] = acc
```

**Thread count:** For a 4 MB segment with k=4, m=2, strip_size=64 KB:
- Num stripes = 4 MB / (4 × 64 KB) = 16
- Threads per block = 64 KB = 65,536
- Total threads = 16 × 65,536 = 1,048,576

This saturates a modern GPU (e.g., NVIDIA A100 has 6,912 CUDA cores × 128
threads/SM = ~880K threads in flight).

**GF arithmetic on GPU:** The GF(2^8) multiplication table is stored in GPU
constant memory (64 KB cache, very fast for uniform access). Each thread
performs a single table lookup per multiplication.

#### 9.5.3 Device Memory Management

GPU buffers are allocated per operation and freed immediately after:

```
encode(data_shards, m):
  1. acquire semaphore permit
  2. allocate device memory:
       d_data   = cuda_malloc(k * strip_size * num_stripes)     // input
       d_parity = cuda_malloc(m * strip_size * num_stripes)     // output
       d_matrix = cuda_malloc(m * k)                            // constant
  3. copy data: host → device (cudaMemcpyAsync on stream)
  4. copy matrix: host → device (cudaMemcpyAsync on stream)
  5. launch kernel (non-blocking on stream)
  6. copy output: device → host (cudaMemcpyAsync on stream)
  7. stream_synchronize()
  8. free device memory
  9. release semaphore permit
  10. return parity shards
```

**Pinned memory:** Input data is copied into pinned (page-locked) host memory
before transfer. Pinned memory enables DMA from the GPU without CPU
intervention, doubling PCIe throughput. The pinned buffer is recycled from a
pool (`GpuBufferPool`) to avoid per-operation `cudaMallocHost` overhead.

```
Transfer without pinned memory:  CPU buffer → driver copy → pinned → DMA → GPU  (2 copies)
Transfer with pinned memory:     pinned buffer → DMA → GPU                       (1 copy)
```

#### 9.5.4 CUDA Streams

All GPU operations for a single encode/decode call are submitted to a
dedicated CUDA stream. The stream enables asynchronous overlap of:

- Memory copy H→D (DMA engine)
- Kernel execution (compute)
- Memory copy D→H (DMA engine)

Without streams, each operation blocks until the previous completes. With
streams, the GPU scheduler overlaps DMA and compute automatically.

#### 9.5.5 GPU Batch Threshold

GPU offload has a fixed overhead: device memory allocation (~100 µs), H→D
transfer (~50 µs for 4 MB on PCIe 3.0 x16), kernel launch (~10 µs), D→H
transfer (~50 µs). For small segments, this overhead exceeds the CPU encode
time.

The `ec_gpu_min_segment_size` threshold (default 100 MB) prevents GPU offload
for segments where the CPU is faster. This applies per-segment: a 4 MB
standard segment uses CPU SIMD; a 100 MB multi-segment write uses GPU.

```toml
ec_gpu_min_segment_size = 104857600   # 100 MB — only offload large segments
```

**Break-even analysis (approximate, x86 with AVX-512):**

| Segment Size | CPU (ISA-L) | GPU (RTX 4090) | Winner |
|---|---|---|---|
| 4 MB (1 stripe) | ~50 µs | ~200 µs (overhead dominates) | CPU |
| 64 MB (16 stripes) | ~800 µs | ~300 µs | GPU |
| 256 MB (64 stripes) | ~3.2 ms | ~0.8 ms | GPU (4×) |
| 1 GB (256 stripes) | ~12.8 ms | ~3 ms | GPU (4×) |

#### 9.5.6 GPU Error Handling

GPU operations can fail for reasons outside OceanFS control:

| Failure | Cause | Behavior |
|---|---|---|
| `cudaMalloc` fails | VRAM exhausted | Release semaphore, return `Err(AccelError::GpuOutOfMemory)`, caller falls back to CPU |
| Kernel launch fails | Device lost, driver crash | Release semaphore, log ERROR, return `Err(AccelError::GpuDeviceLost)`, caller falls back to CPU |
| `cudaMemcpy` fails | PCIe error | Release semaphore, log ERROR, return `Err(AccelError::GpuTransferError)` |
| Kernel timeout | Kernel runs >5s (TDR) | Release semaphore, log ERROR, mark GPU unavailable for 60s, fall back to CPU |

After a device-lost error, the `CudaBackend` marks itself as unavailable for a
cooldown period (default 60 seconds). During cooldown, all GPU requests fall
back to ISA-L (or CPU SIMD) without attempting GPU access. After cooldown,
a single probe operation (encode a tiny dummy stripe) tests if the device has
recovered. If successful, GPU operations resume. If not, cooldown restarts.

This prevents the system from hammering a failed GPU with operations that will
all fail.

### 9.6 Non-EC Acceleration [NEW]

#### 9.6.1 BLAKE3 Hashing

BLAKE3 is accelerated via the upstream `blake3` crate, which performs runtime
CPU feature detection at program initialization. The crate benchmarks itself at
initialization and selects:

- AVX-512: ~4 GB/s/core
- AVX2: ~3 GB/s/core
- SSE4.1: ~1.5 GB/s/core
- Portable: ~400 MB/s/core

OceanFS does not implement custom BLAKE3 acceleration. The `accel_hash_tier`
configuration is a pass-through; `"auto"` delegates entirely to the crate.

No GPU path for BLAKE3 is planned — even the portable implementation is faster
than any realistic network throughput, and the overhead of GPU offload (PCIe
transfer + kernel launch) would make it slower than CPU for all practical blob
sizes.

#### 9.6.2 zstd Compression

Segment data may be compressed before EC encoding (future feature, designed
here but implemented as a **separate epic** from GPU EC acceleration). The
compression acceleration model mirrors the EC model:

| Tier | Backend | Availability |
|---|---|---|
| Tier 0 | `zstd` crate (CPU) | Always |
| Tier 1 | ISA-L `igzip` (CPU, AVX-512) | `isa-l` feature + AVX-512 |
| Tier 2 | nvCOMP (GPU batch) | `cuda` feature + nvCOMP library |

Compression tier selection is **per-bucket only** (`compress_tier` in bucket
policy). There is no node-level `compress_tier` default — unlike EC
acceleration, compression is workload-dependent and only meaningful to enable
for specific buckets with compressible data.

**nvCOMP integration:** When the `cuda` feature is enabled and nvCOMP is
available, the dispatcher provides a `Compressor` trait that delegates to
GPU-accelerated compression. The GPU performs batched compression of segment
data using the same semaphore-controlled model as EC encoding.

```rust
pub trait Compressor: Send + Sync {
    fn compress(&self, data: &[u8], level: u32) -> Result<Vec<u8>>;
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;
}
```

**nvCOMP batch behavior:** Compression is batched across multiple segments
when sealing or healing. The CPU accumulates segments into a batch, sends the
batch to nvCOMP, and receives compressed buffers. The batch threshold mirrors
`ec_gpu_batch_size`.

**Epic placement:** The `Compressor` trait + nvCOMP/igzip backends are a
**separate epic** from the GPU EC acceleration epic and the CPU acceleration
backends epic (ISA-L + ARM SVE). This allows compression acceleration to be
prioritized independently and shipped when segment compression is ready.

#### 9.6.3 AES-GCM Encryption

Encryption uses the `aes-gcm` crate, which leverages AES-NI instructions via
the `aes` crate's runtime detection. AES-NI provides hardware-accelerated
AES rounds (~1 CPU cycle per byte on modern x86).

GPU batch encryption is deferred to future work. The current bottleneck for a
blob store is EC encoding, not encryption. A GPU path for AES-GCM would
require a dedicated kernel and adds complexity without a clear throughput
benefit for the target workload (S3-compatible blob storage where TLS
terminates at the load balancer, and most deployments use network-level
encryption, not per-blob encryption).

### 9.7 Fallback & Error Handling [NEW]

#### 9.7.1 Fallback Chain

The fallback chain is fixed and always terminates at CPU SIMD:

```
GpuCuda → IsaL → CpuSimd   (always available)
```

A fallback occurs in two scenarios:

1. **Startup fallback:** The configured tier is unavailable at node startup.
   The dispatcher resolves to the highest available tier and caches it. A
   one-time `WARN` is logged.

2. **Runtime fallback:** The active tier fails during an operation (GPU device
   lost, ISA-L FFI error). The dispatcher:
   - Logs an `ERROR` with the failure reason
   - Marks the failed backend as unavailable
   - Re-resolves to the next available tier
   - Increments `accel_runtime_fallback_total`
   - Retries the operation with the new backend

Runtime fallback is transparent to the caller — the dispatcher handles it
internally. The caller sees only the result of the retried operation (or an
error if all backends fail, which can only happen if CPU SIMD fails — an
extremely unlikely scenario).

#### 9.7.2 GPU Cooldown

When the CUDA backend fails at runtime (device lost, repeated OOM):

1. The backend is marked `Unavailable` with a cooldown timestamp
2. All subsequent GPU requests fall back without attempting GPU access
3. After `ec_gpu_cooldown_sec` (default 60), a probe operation tests recovery
4. If probe succeeds: backend marked `Available`, normal operation resumes
5. If probe fails: cooldown reset, another `ERROR` logged

```toml
ec_gpu_cooldown_sec = 60   # seconds before retrying a failed GPU
```

This prevents thundering-herd GPU failures from flooding the log and ensures
the CPU path is used reliably during GPU outages.

#### 9.7.3 Error Types

```rust
// oceanfs-accel/src/error.rs
pub enum AccelError {
    #[error("GPU out of memory: requested {requested}, available {available}")]
    GpuOutOfMemory { requested: u64, available: u64 },

    #[error("GPU device lost")]
    GpuDeviceLost,

    #[error("GPU data transfer error")]
    GpuTransferError(#[source] std::io::Error),

    #[error("ISA-L FFI error: {0}")]
    IsalFfi(String),

    #[error("Backend temporarily unavailable: {backend}")]
    BackendUnavailable { backend: String },
}
```

### 9.8 Observability [NEW]

#### 9.8.1 Metrics

All metrics are exposed at `/admin/metrics` in Prometheus format.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `accel_tier_active` | Gauge | `tier`, `operation` | Currently active tier (0=CPU, 1=ISA-L, 2=GPU) |
| `accel_encode_duration_seconds` | Histogram | `tier`, `k`, `m` | EC encode latency |
| `accel_decode_duration_seconds` | Histogram | `tier`, `k`, `m` | EC decode latency |
| `accel_bytes_processed_total` | Counter | `tier`, `operation` | Bytes processed by each tier |
| `accel_fallback_total` | Counter | `from_tier`, `to_tier` | Startup fallback events |
| `accel_runtime_fallback_total` | Counter | `from_tier`, `to_tier`, `reason` | Runtime fallback events |
| `accel_gpu_utilization` | Gauge | `device` | GPU utilization (0.0–1.0) |
| `accel_gpu_memory_bytes` | Gauge | `device`, `kind` | GPU memory used/free |
| `accel_gpu_semaphore_wait_seconds` | Histogram | `device` | Time spent waiting for GPU semaphore |
| `accel_compress_duration_seconds` | Histogram | `tier`, `algorithm` | Compression latency |
| `accel_hash_duration_seconds` | Histogram | `tier` | Hash computation latency |

#### 9.8.2 Tracing

The dispatcher emits `tracing` spans at key points:

```
INFO  oceanfs_accel: acceleration subsystem initialized, active_tier=isa_l
DEBUG oceanfs_accel: probing hardware, cuda=unavailable, isa_l=available, cpu=available
WARN  oceanfs_accel: GPU acceleration requested but CUDA unavailable; falling back to ISA-L
ERROR oceanfs_accel: GPU device lost during encode; falling back to ISA-L
DEBUG oceanfs_accel: per-bucket tier override, bucket=my-bucket, requested=gpu_cuda, resolved=isa_l
```

#### 9.8.3 Admin API

The `/admin/acceleration` endpoint returns the current acceleration status:

```json
{
  "active_tier": "isa_l",
  "available_backends": ["cpu_simd", "isa_l"],
  "unavailable_backends": ["gpu_cuda"],
  "gpu_status": {
    "available": false,
    "reason": "no_cuda_device",
    "cooldown_remaining_sec": 0
  },
  "fallback_count": 0,
  "runtime_fallback_count": 0
}
```

### 9.9 Configuration Reference [EXPANDED]

#### 9.9.1 Node Configuration (`oceanfs.toml`)

```toml
[acceleration]
# EC acceleration tier
#   "auto"     — probe: CUDA > ISA-L > CPU SIMD (default)
#   "cpu_simd" — GF-complete portable + runtime SIMD dispatch
#   "isa_l"    — Intel ISA-L (requires AVX-512 + isa-l feature)
#   "gpu_cuda" — NVIDIA CUDA (requires GPU + cuda feature)
ec_tier = "auto"

# Hash acceleration tier
#   "auto"  — BLAKE3 crate auto-detection (AVX-512, AVX2, SSE4.1, NEON)
#   "avx512" — force AVX-512 (falls back to auto if unavailable)
hash_tier = "auto"

# GPU-specific configuration
ec_gpu_device_id          = 0             # CUDA device index
ec_gpu_batch_size         = 64            # stripes per GPU kernel launch
ec_gpu_min_segment_size   = 104857600     # 100 MB — minimum segment size for GPU offload
ec_gpu_max_concurrent_ops = 1             # permits for GPU semaphore (1 = serialize)
ec_gpu_cooldown_sec       = 60            # seconds to wait before retrying after GPU failure

# ISA-L configuration
isal_prefer_avx512        = true          # prefer AVX-512 code path if available
```

#### 9.9.2 Bucket Configuration (per-bucket override)

```toml
[bucket.my-bucket.acceleration]
ec_tier         = "gpu_cuda"   # override node-level ec_tier
hash_tier       = "auto"       # override node-level hash_tier
compress_tier   = "auto"       # per-bucket only — no node-level default
                               #   "auto"  — probe: nvCOMP > ISA-L igzip > CPU
                               #   "cpu"   — zstd crate (CPU)
                               #   "gpu"   — nvCOMP GPU batch (requires cuda feature)
```

Any bucket field left unset inherits the node-level configuration (for `ec_tier`
and `hash_tier`). `compress_tier` has no node-level default — it defaults to
`"cpu"` if unset (no compression acceleration unless explicitly requested per
bucket).

---

## Implementation Notes for the Spec Writer

1. **Section numbering:** This draft assumes §9 remains at its current position.
   If new sections are added before §9 in the spec, adjust accordingly.

2. **Cross-references:** This draft references §6 (Erasure Coding) and §8
   (Throughput Tuning). Verify these references remain valid.

3. **External dependencies:** The `blake3`, `zstd`, `aes-gcm`, `cudarc`,
   `isal-rs` crates are referenced. If the dependency list changes, update
   the relevant sections.

4. **ADR-0006:** The architectural decisions in this spec section are justified
   by ADR-0006. Include a reference to it in §1.2 (Architecture Decision
   Records table).

5. **§9.6.3 (AES-GCM):** The GPU encryption path is explicitly deferred. If
   the project later decides to implement GPU-accelerated encryption, this
   section becomes the placeholder for that design.

6. **§9.5.3 (Pinned memory):** The `GpuBufferPool` for pinned memory is a
   performance optimization that may be deferred to a later iteration if
   `cudaMallocHost` overhead is found to be negligible in benchmarks.

7. **Epic structure:** The acceleration subsystem spans three epics:
   - **CPU Acceleration Backends** (new): ISA-L (x86) + ARM NEON/SVE (aarch64).
     Implements Tier 1 across both architectures. Separate from Phase 3 (which
     delivered the EC codec trait + Cauchy RS + stripe parallelism).
   - **GPU Acceleration** (Phase 8): CUDA EC backend. Already scaffolded as
     `phase-8-gpu-acceleration`.
   - **Compression Acceleration** (new): `Compressor` trait + nvCOMP + igzip
     backends. Separate epic — implemented when segment compression is ready.
   The dispatcher (`AccelDispatcher`) in `oceanfs-accel` is the integration
   point for all three.

8. **`compress_tier` is per-bucket only.** There is no node-level
   `compress_tier` config knob. The bucket policy `compress_tier` defaults to
   `"cpu"` (no acceleration) when unset. This is intentional: compression
   acceleration is workload-dependent and not a universal throughput win.
