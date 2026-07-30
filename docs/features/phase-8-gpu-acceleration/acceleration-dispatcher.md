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
adr: []
perf:
  - "4.3: Feature-gated SIMD compilation"
  - "6.4: Static dispatch over dynamic dispatch on hot paths"
created: 2026-07-30
updated: 2026-07-30
---

# Acceleration Dispatcher

## Summary

Implement the tiered acceleration dispatcher in `oceanfs-accel`. The dispatcher
selects the optimal EC codec backend at runtime based on configuration and
hardware availability: Tier 0 (CPU SIMD / portable), Tier 1 (ISA-L optimized),
Tier 2 (GPU/CUDA). The selection is configurable per bucket and per node
(`accel_ec_tier`). The dispatcher provides a single `Encoder`/`Decoder`
interface that routes to the most capable available backend.

## Scope

### In Scope
- `AccelDispatcher`: wraps multiple backends, routes encode/decode to best available
- `AccelTier` enum: `Auto`, `CpuSimd`, `IsaL`, `GpuCuda`
- Tier resolution: `Auto` → probe hardware → pick best available (CUDA > ISA-L > CPU SIMD)
- Runtime backend selection: on each encode/decode call, check backend availability
- Fallback chain: if selected tier unavailable, fall back to next tier (no panic)
- CPU backends (always available): GF-complete (portable) or ISA-L (x86, feature-gated)
- GPU backend: available only if `cuda` feature enabled + GPU present
- Per-bucket config override: `accel_ec_tier` in bucket policy
- Hash tier dispatcher: `accel_hash_tier` (auto/cpu/avx512) for BLAKE3 (delegates to `blake3` crate's auto-detection)
- Unit tests for tier resolution, fallback behavior, per-bucket override

### Out of Scope
- Dynamic tier switching mid-operation (tier is fixed per encode/decode call)
- GPU batch-size auto-tuning
- Custom hardware backends beyond CUDA/ISA-L/CPU

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `AccelTier` enum, `AccelConfig` |
| `oceanfs-accel` | New modules: `dispatcher.rs` |
| `oceanfs-accel` | Facade export: `pub use dispatcher::AccelDispatcher` |

## Interface (Public API)

- `pub enum AccelTier` — `Auto`, `CpuSimd`, `IsaL`, `GpuCuda`
- `pub struct AccelConfig` — `ec_tier: AccelTier`, `hash_tier: AccelTier`, `gpu: Option<GpuConfig>`
- `pub struct AccelDispatcher` — `pub fn new(config: AccelConfig) -> Self`, `pub fn resolve_ec_encoder(&self) -> Arc<dyn Encoder>`, `pub fn resolve_ec_decoder(&self) -> Arc<dyn Decoder>`, `pub fn active_tier(&self) -> AccelTier`
- impl `Encoder` for `AccelDispatcher` — delegates to resolved backend
- impl `Decoder` for `AccelDispatcher` — delegates to resolved backend

## Data Flow

```
Dispatcher initialization:
  AccelDispatcher::new(config):
    ├─ Determine available backends:
    │    ├─ CPU (GF-complete) → always available
    │    ├─ ISA-L → available if: cfg(feature = "isa-l") AND runtime CPU check (x86 with AVX-512)
    │    └─ CUDA → available if: cfg(feature = "cuda") AND GPU device present
    ├─ Resolve tier:
    │    ├─ Auto → ISA-L (if available) else CPU
    │    ├─ GpuCuda → CUDA (if available) else ISA-L else CPU
    │    ├─ IsaL → ISA-L (if available) else CPU
    │    └─ CpuSimd → CPU (always)
    └─ Cache resolved encoder/decoder

Per-operation dispatch:
  encode_request comes in:
    ├─ Check bucket policy: bucket.accel_ec_tier overrides node config?
    │    ├─ Yes → re-resolve for this tier
    │    └─ No → use cached backend
    └─ Delegate to resolved Encoder::encode()

Fallback example:
  Config: accel_ec_tier = "gpu_cuda"
  GPU not available → dispatcher logs warning, falls back to ISA-L
  ISA-L also not available → falls back to CPU GF-complete
  → encode succeeds (slower, but correct)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` (all feature combos) succeeds
- [ ] **Tests:** Unit tests: Auto tier resolves to best available, GpuCuda falls back when GPU absent, IsaL falls back when ISA-L not compiled, per-bucket override takes effect, active_tier() reports correct tier, dispatch produces identical results across all backends (cross-backend round-trip)
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-accel`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `AccelDispatcher` documented with tier selection logic
- [ ] **ADR:** N/A (spec §9.1 covers tiered acceleration model)
- [ ] **Perf:** Rule 4.3 (feature-gated SIMD), 6.4 (static dispatch via generics in dispatcher internals)
- [ ] **Integration:** `tests/accel_dispatch.rs`: configure each tier, encode+decode same data through each backend, verify identical output; configure GpuCuda without GPU → verify fallback and log warning
- [ ] **Manual:** Example in `AccelDispatcher` docs compiles and runs
