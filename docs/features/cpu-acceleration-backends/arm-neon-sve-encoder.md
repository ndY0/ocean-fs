---
feature: "ARM NEON / SVE Encoder"
epic: "cpu-acceleration-backends"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: phase-3-erasure-coding
    reason: Must implement existing Encoder/Decoder traits from oceanfs-ec
  - feature: acceleration-dispatcher
    reason: Dispatcher selects between ARM SVE, ISA-L, CUDA, and CPU SIMD backends
adr:
  - 0006-hardware-acceleration-tier-model
perf:
  - "4.3: Feature-gated SIMD compilation"
  - "10.6: Conditional platform-specific code paths"
  - "6.4: Static dispatch over dynamic dispatch on hot paths"
  - "12.1: SAFETY comments on every unsafe block"
created: 2026-07-31
updated: 2026-07-31
---

# ARM NEON / SVE Encoder

## Summary

Implement a Rust-native ARM NEON/SVE erasure coding backend in
`oceanfs-accel` behind the `arm-sve` Cargo feature. The `ArmEncoder`
and `ArmDecoder` structs implement the existing `Encoder` and `Decoder`
traits from `oceanfs-ec` using architecture-specific SIMD intrinsics
for GF(2^8) matrix multiplication. Unlike the ISA-L backend (which wraps
a C library via FFI), this is a pure-Rust implementation using
`std::arch::aarch64` intrinsics, keeping the `unsafe` surface auditable
within a single crate. Runtime feature detection selects the optimal SIMD
path (SVE2 → SVE → NEON → portable) at startup, cached for the backend's
lifetime.

## Scope

### In Scope

- `ArmEncoder` struct implementing `Encoder` trait from `oceanfs-ec`
- `ArmDecoder` struct implementing `Decoder` trait from `oceanfs-ec`
- Pure-Rust NEON/SVE intrinsics for GF(2^8) vectorized multiply:
  - SVE2 path: 256-bit predicated vector operations (Graviton4, Neoverse V2)
  - SVE path: 128-bit predicated vector operations (Graviton3, Neoverse V1)
  - NEON path: 128-bit SIMD table-lookup (Graviton2, Apple M1/M2)
  - Portable fallback: GF-complete log/exp tables (always available)
- Runtime SIMD feature detection via `std::arch::is_aarch64_feature_detected!`:
  - `"sve2"` → SVE2 kernel
  - `"sve"` → SVE kernel
  - `"neon"` → NEON kernel
  - otherwise → portable GF-complete
- Feature-gated behind `#[cfg(feature = "arm-sve")]` in dedicated module `oceanfs-accel/src/arm_sve.rs`
- Compile-time arch gate: `#[cfg(target_arch = "aarch64")]`
- `ArmSveLevel` enum: `Sve2`, `Sve`, `Neon`, `Portable` — resolved once at construction
- The backend is always constructable (returns `Self`, not `Option`): on aarch64, at minimum the portable path is always available
- Predicated SVE loops: same kernel handles any vector width (128–2048 bits) without recompilation

### Out of Scope

- ARM SVE for non-aarch64 targets (SVE is ARM-specific)
- FFI to external ARM EC libraries (libec); this is a pure-Rust implementation
- ARM SVE for compression (compression acceleration is a separate epic)
- x86 builds of the ARM backend (compile-time excluded via `#[cfg(target_arch = "aarch64")]`)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-accel` | New module `arm_sve.rs` — `#[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]` |
| `oceanfs-accel` | Feature `arm-sve = []` (no external dep — pure Rust intrinsics) in `Cargo.toml` |
| `oceanfs-accel` | Facade export: `#[cfg(feature = "arm-sve")] pub use arm_sve::ArmEncoder`, `ArmDecoder` |
| `oceanfs-core` | New type: `ArmSveLevel` enum (or defined in `oceanfs-accel`) |

## Interface (Public API)

- `pub enum ArmSveLevel` — `Sve2`, `Sve`, `Neon`, `Portable`
- `pub struct ArmEncoder` — `pub fn new() -> Self` (always constructable on aarch64), `pub fn sve_level(&self) -> ArmSveLevel`
- `pub struct ArmDecoder` — `pub fn new() -> Self`, `pub fn sve_level(&self) -> ArmSveLevel`
- `impl Encoder for ArmEncoder` — dispatches to the cached SIMD kernel based on `sve_level`
- `impl Decoder for ArmDecoder` — dispatches to the cached SIMD kernel based on `sve_level`
- `pub(crate) fn encode_sve2(data: &[&[u8]], m: u8) -> Vec<Vec<u8>>` — SVE2 kernel
- `pub(crate) fn encode_sve(data: &[&[u8]], m: u8) -> Vec<Vec<u8>>` — SVE kernel
- `pub(crate) fn encode_neon(data: &[&[u8]], m: u8) -> Vec<Vec<u8>>` — NEON kernel
- `pub(crate) fn encode_portable(data: &[&[u8]], m: u8) -> Vec<Vec<u8>>` — portable GF-complete fallback

## Data Flow

```
AccelDispatcher::resolve(bucket_tier):
  └─ tier == AccelTier::Auto || tier == AccelTier::IsaL:
       └─ cfg(target_arch = "aarch64") && cfg(feature = "arm-sve"):
            └─ ArmEncoder::new():
                 ├─ std::arch::is_aarch64_feature_detected!("sve2")? → level = Sve2
                 ├─ std::arch::is_aarch64_feature_detected!("sve")?  → level = Sve
                 ├─ std::arch::is_aarch64_feature_detected!("neon")? → level = Neon
                 └─ otherwise → level = Portable
            → return ArmEncoder { sve_level: level }

Encode path (per segment):
  Segment sealed → StripeBatch assembled (SoA layout):
    └─ ArmEncoder::encode(&data_shards, m):
         └─ match self.sve_level:
              ├─ Sve2  → encode_sve2(data_shards, m)
              │           ├─ Per stripe: predicated vector GF(2^8) multiply
              │           ├─ // SAFETY: SVE2 intrinsics; vector width queried at
              │           │   //        runtime and loop bounds adjusted accordingly
              │           └─ unsafe { svld1(...), svmla(...), svst1(...) }
              ├─ Sve   → encode_sve(data_shards, m)
              │           └─ Same pattern with SVE predicated ops (128-bit min)
              ├─ Neon  → encode_neon(data_shards, m)
              │           └─ 128-bit NEON intrinsics: vld1q_u8, vmulq_p8, vst1q_u8
              └─ Portable → cauchy_portable_encode(data_shards, m)
                             └─ Delegate to existing GF-complete implementation

Decode path (per segment, reconstruction):
  ArmDecoder::decode(&available_shards, &erased_indices):
    └─ Same SVE/NEON/portable dispatch as encode, with reconstructed decode matrix
```

## Definition of Done

- [ ] **Code:** `cargo build --features arm-sve --target aarch64-unknown-linux-gnu` succeeds; `cargo build --all-targets` (no features) succeeds on all platforms
- [ ] **Tests:** ARM encode round-trip matches Cauchy RS output (bit-exact); SVE2/SVE/NEON/portable kernels all produce identical output; cross-kernel round-trip (encode SVE2, decode NEON) passes; ARM backend not compiled on x86 (verified via CI cross-compilation)
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-accel` with `arm-sve` feature (on aarch64 or via emulation)
- [ ] **Lint:** `cargo clippy -- -D warnings` passes; every `unsafe` block using NEON/SVE intrinsics has `// SAFETY:` comment citing alignment, bounds, and feature-detection invariant
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `ArmEncoder` docs document SVE level resolution and vector width behavior
- [ ] **ADR:** ADR-0006 constraints satisfied — trait-based pluggability (§3), feature-gated compilation (§6), startup probing cached for lifetime (§1)
- [ ] **Perf:** Rule 4.3 (feature-gated SIMD), 10.6 (conditional platform-specific code paths with portable fallback), 6.4 (static dispatch), 12.1 (SAFETY on all `unsafe` blocks)
- [ ] **Integration:** `tests/arm_ec_roundtrip.rs`: encode with `ArmEncoder`, decode with Cauchy RS (portable), verify bit-exact match across all SIMD levels. Run on aarch64 hardware (Graviton) or via QEMU emulation in CI
- [ ] **Manual:** Example in `ArmEncoder` docs compiles and runs on aarch64
