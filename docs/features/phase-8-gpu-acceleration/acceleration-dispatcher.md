---
feature: "Acceleration Dispatcher"
epic: "phase-8-gpu-acceleration"
status: proposed
priority: medium
owner: ""
dependencies:
  - feature: ec-codec-trait-cauchy-rs
    reason: Dispatcher selects between CPU codec and CUDA backend
  - feature: cuda-ec-backend
    reason: CUDA is one of the dispatch targets
adr:
  - 0006-hardware-acceleration-tier-model
perf:
  - "2.7: Tokio semaphore for concurrency limits (GPU dispatch)"
  - "4.3: Feature-gated SIMD compilation"
  - "6.4: Static dispatch over dynamic dispatch on hot paths"
  - "12.1: SAFETY comments on every unsafe block (ISA-L FFI path)"
created: 2026-07-30
updated: 2026-07-31
---

# Acceleration Dispatcher

## Summary

Implement the tiered acceleration dispatcher in `oceanfs-accel`. The dispatcher
selects the optimal codec backend at runtime based on configuration and
hardware availability, covering both EC encode/decode and compression:

- **EC backends:** Tier 0 (CPU SIMD / GF-complete), Tier 1a (ISA-L AVX-512, x86),
  Tier 1b (ARM NEON/SVE, aarch64), Tier 2 (GPU/CUDA)
- **Compression backends:** Tier 0 (zstd crate), Tier 1 (ISA-L igzip), Tier 2
  (nvCOMP GPU batch)

EC tier selection is configurable per bucket and per node (`accel_ec_tier`).
Compression tier is per-bucket only (`compress_tier`). The dispatcher provides
unified `Encoder`/`Decoder` and `Compressor` interfaces that route to the most
capable available backend, following the fallback chain defined in ADR-0006.

## Scope

### In Scope
- `AccelDispatcher`: wraps multiple backends, routes encode/decode and compress/decompress to best available
- `AccelTier` enum: `Auto`, `CpuSimd`, `IsaL`, `GpuCuda`
- `CompressionTier` enum: `Auto`, `CpuZstd`, `CpuIgzip`, `GpuNvcomp` (in `oceanfs-core`)
- Tier resolution for EC: `Auto` → probe hardware → pick best available (CUDA > ISA-L > CPU SIMD)
- Tier resolution for compression: `Auto` → probe hardware → pick best available (nvCOMP > igzip > zstd)
- Runtime backend selection: on each encode/decode/compress/decompress call, check backend availability
- Fallback chain (EC): `GpuCuda → IsaL → CpuSimd` (always terminates at CPU SIMD)
- Fallback chain (compression): `GpuNvcomp → CpuIgzip → CpuZstd` (always terminates at zstd)
- CPU EC backends (always available): GF-complete (portable) or ISA-L (x86, feature-gated) or ARM SVE (aarch64, feature-gated)
- GPU EC backend: available only if `cuda` feature enabled + GPU present
- GPU compression backend (nvCOMP): available only if `cuda` feature enabled + nvCOMP library present
- CPU compression backend (igzip): available only if `isa-l` feature enabled + AVX-512 detected
- Per-bucket config override: `accel_ec_tier` and `compress_tier` in bucket policy
- `Compressor` trait dispatch alongside `Encoder`/`Decoder` dispatch
- Hash tier dispatcher: `accel_hash_tier` (auto/cpu/avx512) for BLAKE3 (delegates to `blake3` crate's auto-detection)
- Unit tests for tier resolution, fallback behavior, per-bucket override across all backend combinations

### Out of Scope
- Dynamic tier switching mid-operation (tier is fixed per encode/decode/compress/decompress call)
- GPU batch-size auto-tuning
- Custom hardware backends beyond CUDA/ISA-L/ARM-SVE/CPU
- Node-level `compress_tier` (compression is per-bucket only per ADR-0006)
- AES-GCM encryption acceleration (deferred to future work per spec §9.6.3)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `AccelTier` enum, `AccelConfig`, `CompressionTier` enum |
| `oceanfs-accel` | New modules: `dispatcher.rs`, `tier0.rs`, `isal.rs` (cfg-gated), `arm_sve.rs` (cfg-gated), `cuda/` (cfg-gated), `igzip.rs` (cfg-gated), `compressor.rs` |
| `oceanfs-accel` | Facade export: `pub use dispatcher::AccelDispatcher`; re-exports for all feature-gated backends |

## Interface (Public API)

- `pub enum AccelTier` — `Auto`, `CpuSimd`, `IsaL`, `GpuCuda`
- `pub enum CompressionTier` — `Auto`, `CpuZstd`, `CpuIgzip`, `GpuNvcomp`
- `pub struct AccelConfig` — `ec_tier: AccelTier`, `hash_tier: AccelTier`, `gpu: Option<GpuConfig>`
- `pub struct AccelDispatcher` — `pub fn new(config: AccelConfig) -> Self`, `pub fn resolve_ec_encoder(&self) -> Arc<dyn Encoder>`, `pub fn resolve_ec_decoder(&self) -> Arc<dyn Decoder>`, `pub fn resolve_compressor(&self, tier: CompressionTier) -> Arc<dyn Compressor>`, `pub fn active_tier(&self) -> AccelTier`, `pub fn active_compression_tier(&self) -> CompressionTier`
- impl `Encoder` for `AccelDispatcher` — delegates to resolved backend
- impl `Decoder` for `AccelDispatcher` — delegates to resolved backend

## Data Flow

```
Dispatcher initialization:
  AccelDispatcher::new(config):
    ├─ Determine available EC backends:
    │    ├─ CPU (GF-complete) → always available
    │    ├─ ISA-L → available if: cfg(feature = "isa-l") AND runtime CPU check (x86 with AVX-512)
    │    ├─ ARM SVE → available if: cfg(feature = "arm-sve") AND runtime check (aarch64 with SVE/NEON)
    │    └─ CUDA → available if: cfg(feature = "cuda") AND GPU device present
    ├─ Determine available compression backends:
    │    ├─ zstd (CPU) → always available
    │    ├─ igzip (ISA-L) → available if: cfg(feature = "isa-l") AND AVX-512
    │    └─ nvCOMP → available if: cfg(feature = "cuda") AND nvCOMP library loaded
    ├─ Resolve EC tier:
    │    ├─ Auto → CUDA > ISA-L > CPU SIMD (first available)
    │    ├─ GpuCuda → CUDA (if avail) else ISA-L else CPU
    │    ├─ IsaL → ISA-L (if avail) else CPU
    │    └─ CpuSimd → CPU (always)
    ├─ Resolve compression tier:
    │    ├─ Auto → nvCOMP > igzip > zstd (first available)
    │    ├─ GpuNvcomp → nvCOMP (if avail) else igzip else zstd
    │    ├─ CpuIgzip → igzip (if avail) else zstd
    │    └─ CpuZstd → zstd (always)
    └─ Cache resolved encoder, decoder, compressor backends

Per-operation EC dispatch:
  encode_request comes in:
    ├─ Check bucket policy: bucket.accel_ec_tier overrides node config?
    │    ├─ Yes → re-resolve for this tier
    │    └─ No → use cached backend
    └─ Delegate to resolved Encoder::encode()

Per-operation compression dispatch:
  segment_seal comes in (bucket has compress_tier):
    └─ Resolve compressor for bucket.compress_tier:
         ├─ Tier available? → return Arc<dyn Compressor>
         └─ Fallback chain: GpuNvcomp → CpuIgzip → CpuZstd

Fallback example (EC):
  Config: accel_ec_tier = "gpu_cuda"
  GPU not available → dispatcher logs warning, falls back to ISA-L
  ISA-L also not available → falls back to CPU GF-complete
  → encode succeeds (slower, but correct)

Fallback example (compression):
  Bucket: compress_tier = "gpu_nvcomp"
  nvCOMP not available → dispatcher logs warning, falls back to igzip
  igzip also not available → falls back to zstd
  → compress succeeds (slower, but correct)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` (all feature combos) succeeds
- [ ] **Tests:** Unit tests: Auto tier resolves to best available, GpuCuda falls back when GPU absent, IsaL falls back when ISA-L not compiled, per-bucket override takes effect, active_tier() reports correct tier, dispatch produces identical results across all backends (cross-backend round-trip)
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-accel`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `AccelDispatcher` documented with tier selection logic
- [ ] **ADR:** ADR-0006 constraints satisfied — startup probing cached for lifetime (§1), fallback chain with warnings (§2), trait-based pluggability (§3), GPU concurrency model (§4), Non-EC acceleration scope with Compressor (§5), feature-gated compilation (§6), per-bucket tier selection (§7)
- [ ] **Perf:** Rule 4.3 (feature-gated SIMD), 6.4 (static dispatch via generics in dispatcher internals)
- [ ] **Integration:** `tests/accel_dispatch.rs`: configure each EC tier, encode+decode same data through each backend, verify identical output; configure each compression tier, compress+decompress through each backend, verify identical output; configure GpuCuda without GPU → verify EC fallback and log warning; configure GpuNvcomp without nvCOMP → verify compression fallback and log warning; per-bucket tier override takes effect for both EC and compression
- [ ] **Manual:** Example in `AccelDispatcher` docs compiles and runs
