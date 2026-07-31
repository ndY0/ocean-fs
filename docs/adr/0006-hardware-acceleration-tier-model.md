# ADR-0006: Hardware Acceleration Tier Model

**Status:** Proposed
**Date:** 2026-07-31
**Deciders:** OceanFS design team

---

## Context

The OceanFS specification §9 defines a three-tier hardware acceleration model
for erasure coding: CPU SIMD (Tier 0), ISA-L/libec (Tier 1), and GPU/CUDA
(Tier 2). The spec also mentions acceleration for BLAKE3 hashing, zstd
compression, and AES-GCM encryption. However, the current spec is a single page
that describes *what* the tiers are without specifying *how* they are
discovered, selected, composed, or made resilient to hardware absence.

The `oceanfs-accel` crate exists as a stub: `AccelTier` is a compile-time
enum, the `AccelDispatcher` performs no runtime hardware probing, there is no
`Encoder`/`Decoder` delegation, the CUDA backend is a placeholder struct, and
ISA-L has zero implementation. Before any acceleration backends can be
implemented, the architectural model must be settled.

Phase 3 (EC codec trait + Cauchy RS + stripe parallelism) is already
implemented. The acceleration backends are new epics, not part of Phase 3:

### Constraints

- The system must operate correctly on hardware without GPU or AVX-512.
  A missing acceleration tier must never cause a crash or incorrect data.
- The `Encoder` and `Decoder` traits already exist in `oceanfs-ec`. All
  acceleration backends must implement these traits, preserving the existing
  `ParallelEncoder`/`ParallelDecoder` orchestration.
- Per-bucket `accel_ec_tier` configuration must be honored (§8.1 of the spec).
- The crate dependency DAG (architecture.md §1.1) places `oceanfs-accel`
  between `oceanfs-ec` and `oceanfs-storage`. This must be preserved.
- `oceanfs-accel` is permitted to use `unsafe` (architecture.md §7.2).
  All other crates are `#![forbid(unsafe_code)]`.
- GPU acceleration (Tier 2) is a Phase 8 deliverable.
- ISA-L (x86) and ARM NEON/SVE (aarch64) are a **new epic: CPU Acceleration
  Backends** — separate from both Phase 3 (already done) and Phase 8.
- nvCOMP compression is a **separate epic** implementing the `Compressor`
  trait — not bundled with GPU EC acceleration.
- `compress_tier` is per-bucket only (no node-level default).
- Compilation without `cuda`, `isa-l`, or `arm-sve` features must produce a
  fully functional system using only Tier 0.

### Prior Art

| System | Approach | Relevant Insight |
|---|---|---|
| Intel ISA-L | C library with hand-tuned AVX-512 assembly for RS encode/decode | De facto standard for line-rate EC on x86; Rust bindings via FFI |
| Ceph | Async op queues with pluggable accelerator backends (ISA-L, QAT) | Backend selection at startup; fallback to software on accelerator failure |
| nvCOMP (NVIDIA) | GPU-accelerated compression library (LZ4, Snappy, zstd) | Batch compression offload with CPU fallback |
| `blake3` crate | Runtime CPU feature detection (AVX-512, AVX2, SSE4.1, NEON) at program init | Single-binary portability; no compile-time feature selection needed |
| Facebook f4 | Dedicated EC encoding cluster nodes with GPU offload | GPU used for batch rebuild (not per-object encode) — same pattern as our heal path |

## Decision

### 1. Startup Hardware Probing, Cached for Lifetime

Hardware capability detection runs once at node startup inside
`AccelDispatcher::new()`. The resolved backends are cached for the lifetime
of the dispatcher. There is no lazy initialization and no re-probing at
runtime.

**Probing sequence:**

```
AccelDispatcher::new(config):
  ├── CPU SIMD: always available (GF-complete portable or runtime SIMD dispatch)
  ├── ISA-L:    available if: cfg(feature = "isa-l") AND CPUID reports AVX-512
  ├── CUDA:     available if: cfg(feature = "cuda") AND cudarc::init() succeeds
  │                              AND device count > 0
  └── nvCOMP:   available if: cfg(feature = "cuda") AND CUDA available
  │                              AND nvcomp library loaded
  │
  ├── Resolve requested tier → best available:
  │     Auto     → CUDA > ISA-L > CPU SIMD (first available)
  │     GpuCuda  → CUDA (if avail) else ISA-L else CPU SIMD
  │     IsaL     → ISA-L (if avail) else CPU SIMD
  │     CpuSimd  → CPU SIMD (always)
  │
  └── Cache: resolved encoder, resolved decoder, active tier
```

**Rationale:** Probing at startup means the hot path (every encode/decode call)
has zero branching for tier selection — it's a straight delegation through a
cached `Arc<dyn Encoder>`. Lazy probing would add a branch and potential
initialization latency to the first EC operation. Since accelerator hardware
does not change while a process is running, there is no benefit to re-probing.

### 2. Fallback Chain with Warnings, Prioritizing Availability

When a configured tier is unavailable, the dispatcher falls back to the next
available tier and emits a `WARN`-level log. The system **never panics or
returns an error** due to missing acceleration hardware.

The fallback order is fixed by capability, not configuration:

```
GpuCuda → IsaL → CpuSimd   (always terminates at CpuSimd)
```

If the user configures `accel_ec_tier = "gpu_cuda"` but no GPU is present:

1. Dispatcher logs: `WARN GPU acceleration requested but no CUDA device found; falling back to ISA-L`
2. If ISA-L is also unavailable: `WARN ISA-L not available; falling back to CPU SIMD`
3. Dispatcher reports `active_tier() = CpuSimd`
4. EC operations proceed correctly (slower, but correct)

**A metric counter `accel_fallback_total` is incremented** for each fallback
event so operators can detect misconfiguration.

If the user configures `accel_ec_tier = "cpu_simd"` (explicitly requesting
the slowest tier): no warning, no fallback — the user knows what they asked
for.

**Rationale:** A storage system must never fail to encode data because a GPU
is missing. Correctness precedes throughput. The warning ensures operators
are aware of the performance degradation without blocking operations.

### 3. Trait-Based Backend Pluggability

All acceleration backends implement the existing `Encoder` and `Decoder`
traits from `oceanfs-ec`. The `AccelDispatcher` holds `Arc<dyn Encoder>` and
`Arc<dyn Decoder>` and delegates all encode/decode calls.

```
oceanfs-ec traits (Encoder, Decoder)
        ↑
        ├── CauchyEncoder  (Tier 0, always available)
        ├── IsalEncoder    (Tier 1, cfg(feature = "isa-l"), x86 only)
        ├── ArmEncoder     (Tier 1, cfg(feature = "arm-sve"), aarch64 only)
        └── CudaBackend    (Tier 2, cfg(feature = "cuda"))

oceanfs-accel
        └── AccelDispatcher
              ├── encoder: Arc<dyn Encoder>
              ├── decoder: Arc<dyn Decoder>
              └── active_tier: AccelTier
```

The dispatcher itself also implements `Encoder` and `Decoder` by delegating
to the cached backend. This means consumers (`ParallelEncoder`,
`WriteCoordinator`) interact only with `AccelDispatcher` and never know which
backend is active.

**Per-bucket override:** When a bucket has `accel_ec_tier` set, the caller
passes the bucket's tier to a `resolve_for_bucket(tier)` method. If the
bucket's tier differs from the node's configured tier, the dispatcher
re-resolves against available hardware and returns a temporary `Arc<dyn
Encoder/Decoder>` for that operation. The node's cached backend is not
replaced.

**Rationale:** The traits already exist and are well-tested (Cauchy RS
implementation in `oceanfs-ec` achieves >80% coverage). Adding new backends
requires only a new struct implementing the traits — no trait changes, no
breaking changes to consumers.

### 4. GPU Concurrency Model

The GPU is a finite, non-parallelizable resource. All GPU operations
(encode, decode, compression) are serialized through a `tokio::sync::Semaphore`
with a configurable permit count (default: 1).

```rust
pub struct CudaBackend {
    device: CudaDevice,
    encode_semaphore: Arc<Semaphore>,   // permits = config.gpu_max_concurrent_ops
    stream: CudaStream,                 // dedicated stream for EC operations
}
```

Each GPU encode/decode call:
1. Acquires a semaphore permit (async, non-blocking to the CPU)
2. Copies input data to GPU (async via CUDA stream)
3. Launches the kernel
4. Copies output data back (async via CUDA stream)
5. Synchronizes the stream
6. Releases the semaphore permit

The default permit count is 1 because GF(2^8) matrix multiplication saturates
GPU compute with a single kernel launch. Concurrent launches contend for the
same SMs and memory bandwidth, reducing total throughput through context
switching overhead.

**Configuration knob:** `ec_gpu_max_concurrent_ops` (default 1) allows
operators with high-end GPUs (e.g., A100 with MIG) to increase concurrency.

**Rationale:** Perf guideline 2.7 mandates semaphore-bounded concurrency for
finite resources. The GPU is the quintessential finite resource — one device,
limited VRAM, one PCIe bus.

### 5. Non-EC Acceleration Scope

The acceleration subsystem covers four operations. Each is independently
feature-gated and probed:

| Operation | Tier 0 | Tier 1 | Tier 2 | Feature Gate |
|---|---|---|---|---|
| EC encode/decode | GF-complete | ISA-L (AVX-512) | CUDA kernel | `isa-l`, `cuda` |
| BLAKE3 hashing | `blake3` crate (auto-detect) | — | — | none (built-in) |
| zstd compression | `zstd` crate | ISA-L igzip (if avail) | nvCOMP | `isa-l`, `cuda` |
| AES-GCM encryption | `aes-gcm` crate | AES-NI intrinsics | GPU batch | `cuda` |

**BLAKE3:** The `blake3` crate already performs runtime CPU feature detection
at program init. The dispatcher's `hash_tier` configuration is a pass-through:
`"auto"` uses the crate's default, `"avx512"` forces the AVX-512
implementation (useful for benchmarking). No custom acceleration code is
written for BLAKE3.

**zstd:** The standard compression path uses the `zstd` crate. When the
`cuda` feature is enabled and nvCOMP is available, `AccelDispatcher` provides
a `Compressor` trait (new, modeled on `Encoder`) that delegates to nvCOMP for
batch compression of segment data. ISA-L's `igzip` is available as a CPU
optimization when the `isa-l` feature is enabled. Compression acceleration
(nvCOMP + igzip + the `Compressor` trait) is a **separate epic** from GPU EC
acceleration — designed here, implemented independently. `compress_tier` is
per-bucket only; there is no node-level compression tier configuration.

**AES-GCM:** The standard encryption path uses the `aes-gcm` crate, which
already leverages AES-NI via the `aes` crate's runtime detection. GPU batch
encryption is deferred to future work (the throughput bottleneck for a blob
store is EC, not encryption).

### 6. Feature-Gated Compilation

All optional acceleration backends are behind Cargo features in
`oceanfs-accel`:

```toml
[features]
default = []
cuda = ["dep:cudarc"]
isa-l = ["dep:isal-rs"]
arm-sve = []                 # enables SVE/NEON intrinsics on aarch64
```

The crate compiles and passes all tests with `--no-default-features` (Tier 0
only). CI runs the full test matrix across feature combinations.

Feature-gated code lives in dedicated modules per architecture.md §2.3:

```
oceanfs-accel/src/
  lib.rs             → facade, re-exports
  dispatcher.rs      → AccelDispatcher, AccelTier (always compiled)
  tier0.rs           → CPU SIMD backend (always compiled; delegates to CauchyEncoder)
  isal.rs            → #[cfg(feature = "isa-l")] IsalEncoder (x86 only)
  arm_sve.rs         → #[cfg(feature = "arm-sve")] ArmEncoder (aarch64 only)
  cuda/
    mod.rs           → #[cfg(feature = "cuda")]
    backend.rs       → CudaBackend (implements Encoder, Decoder)
    kernel.rs        → CUDA kernel source (PTX or inline)
    memory.rs        → device memory management
    nvcomp.rs        → nvCOMP compression integration (separate epic)
```

### 7. Per-Bucket Tier Selection

The `accel_ec_tier` and `accel_hash_tier` fields in `BucketPolicy` (§8.1)
override the node-level configuration. The resolution priority is:

1. Bucket policy `accel_ec_tier` (if set and not `"auto"`)
2. Node config `acceleration.ec_tier`
3. Probe result (for `"auto"`)

A bucket requesting `gpu_cuda` on a node without CUDA follows the fallback
chain (ISA-L → CPU SIMD) with a per-operation warning (throttled to one
warning per minute to avoid log spam).

## Consequences

### Positive
- **Correctness over throughput.** The system always produces correct EC
  output regardless of hardware availability. A missing GPU degrades
  performance but never causes errors or data loss.
- **Single-binary portability.** The same binary runs on x86 with AVX-512,
  ARM with NEON, or a machine with an NVIDIA GPU — it probes and selects
  the best available path at startup.
- **Backend isolation.** Each backend is a self-contained struct implementing
  `Encoder`/`Decoder`. Adding a new backend (e.g., AMD ROCm, Intel QAT)
  requires only a new module in `oceanfs-accel` — no trait changes.
- **Testable without hardware.** The trait-based design allows injecting a
  mock `Encoder`/`Decoder` in place of `CudaBackend` for CI testing.
- **Operator visibility.** Fallbacks are logged at WARN level and counted
  in metrics, so misconfiguration is immediately detectable.

### Negative
- **Startup latency.** Hardware probing adds ~50-200ms to node startup
  (CPUID check, CUDA device enumeration, library loading). Acceptable for
  a long-running storage node; problematic for short-lived CLI tools.
- **`unsafe` surface.** The ISA-L FFI and CUDA kernel code introduce
  `unsafe` blocks that must be audited. Every `unsafe` block requires
  a `// SAFETY:` comment per coding.md §7.2.
- **Build complexity.** The `cuda` feature requires the CUDA toolkit at
  build time. CI must have CUDA-capable runners or skip those tests.
- **Per-bucket dispatch overhead.** When a bucket's tier differs from the
  node tier, a temporary `Arc<dyn Encoder>` is created. This is a minor
  allocation on the encode path. Mitigated by caching per-tier backends
  (at most 4 tiers → at most 4 cached backends).

### Neutral
- **Configuration surface grows.** Operators now have `accel_ec_tier`,
  `accel_hash_tier`, `ec_gpu_*` knobs to understand at the node level, plus
  per-bucket `compress_tier`. Sensible defaults (`auto`) minimize the
  cognitive load.
- **The `AccelTier` enum gains variants.** `Compression`, `Encryption` may
  be added later. The `#[non_exhaustive]` attribute (coding.md §1.5) must
  be applied to allow this without semver breaks.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Lazy probing (probe on first use)** | Faster startup; no probe cost if acceleration never used | Branch on every encode/decode path; first request incurs probe latency; complex error handling (what if probe fails mid-request?) | Rejected: startup latency (~200ms) is negligible for a storage node that runs for weeks. Hot-path branching is a throughput regression. |
| **Compile-time-only tier selection (current code)** | Zero runtime cost; simple | Binary must be compiled for specific hardware; no runtime fallback; no `auto` tier that works across deployments | Rejected: violates "single-binary portability" requirement in spec §1.1 ("every performance property configurable per bucket"). |
| **Separate crate per backend** (`oceanfs-accel-cuda`, `oceanfs-accel-isal`) | Finer-grained compilation; CUDA toolkit not needed for non-GPU builds | Proliferation of crates; `AccelDispatcher` must depend on all of them (or use dynamic loading); features already provide the same isolation with less overhead | Rejected: Cargo features already isolate compilation. A separate crate per backend adds maintenance overhead (N Cargo.toml, N CI configs) for no additional benefit. |
| **GPU tier as primary, CPU as fallback** | GPU gets priority automatically | Most deployments will not have GPUs; `auto` tier on a CPU-only node would log a WARN on every startup | Rejected: `auto` probes CUDA, ISA-L, and falls to CPU SIMD silently. No WARN unless the user explicitly configured GPU and it's absent. |

## References

- [OceanFS Specification §9: Hardware Acceleration](../spec.md#9-hardware-acceleration)
- [OceanFS Specification §6: Erasure Coding](../spec.md#6-erasure-coding)
- [Architecture Guidelines §2.3: Feature Gates for Optional Subsystems](../../guidelines/architecture.md#23-feature-gates-for-optional-subsystems)
- [Architecture Guidelines §1.2: Crate Responsibilities](../../guidelines/architecture.md#12-crate-responsibilities)
- [Performance Guidelines §2.7: Tokio semaphore for concurrency limits](../../guidelines/performance.md#27-tokio-semaphore-for-concurrency-limits)
- [Performance Guidelines §4.3: Feature-gated SIMD compilation](../../guidelines/performance.md#43-feature-gated-simd-compilation)
- [Coding Standards §7: Unsafe Code](../../guidelines/coding.md#7-unsafe-code)
- [Intel ISA-L Documentation](https://github.com/intel/isa-l)
- [ARM SVE / NEON Intrinsics](https://developer.arm.com/architectures/instruction-sets/intrinsics/)
- [NVIDIA nvCOMP Documentation](https://developer.nvidia.com/nvcomp)
- [BLAKE3 crate: runtime CPU detection](https://docs.rs/blake3/latest/blake3/)
- Feature: [Acceleration Dispatcher](../features/phase-8-gpu-acceleration/acceleration-dispatcher.md)
- Feature: [CUDA EC Backend](../features/phase-8-gpu-acceleration/cuda-ec-backend.md)
- Feature: [Hardware Acceleration Spec Draft](../features/phase-8-gpu-acceleration/hardware-acceleration-spec-draft.md)
