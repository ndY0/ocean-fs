---
feature: "ISA-L x86 AVX-512 Encoder"
epic: "cpu-acceleration-backends"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: phase-3-erasure-coding
    reason: Must implement existing Encoder/Decoder traits from oceanfs-ec
  - feature: acceleration-dispatcher
    reason: Dispatcher selects between ISA-L, ARM SVE, CUDA, and CPU SIMD backends
adr:
  - 0006-hardware-acceleration-tier-model
perf:
  - "4.3: Feature-gated SIMD compilation"
  - "5.3: Feature-gated SIMD compilation"
  - "6.4: Static dispatch over dynamic dispatch on hot paths"
  - "12.1: SAFETY comments on every unsafe block"
created: 2026-07-31
updated: 2026-07-31
---

# ISA-L x86 AVX-512 Encoder

## Summary

Implement the Intel ISA-L (Intelligent Storage Acceleration Library) FFI
integration in `oceanfs-accel` behind the `isa-l` Cargo feature. The
`IsalEncoder` and `IsalDecoder` structs implement the existing `Encoder` and
`Decoder` traits from `oceanfs-ec`, providing hand-tuned AVX-512 assembly for
Reed-Solomon encode and decode. Runtime CPUID detection confirms AVX-512
availability before activation. All FFI calls are wrapped in `unsafe` blocks
with `// SAFETY:` invariants. The backend is selected by the `AccelDispatcher`
as part of Tier 1.

## Scope

### In Scope

- `IsalEncoder` struct implementing `Encoder` trait from `oceanfs-ec`
- `IsalDecoder` struct implementing `Decoder` trait from `oceanfs-ec`
- FFI binding to ISA-L C library (`isal-rs` or raw `libisa-l`):
  - `ec_init_tables(k, m, &mut [u8; 32*k*m])` — precompute encoding matrix tables
  - `ec_encode_data(len, k, m, &tables, &[&[u8]; k], &mut [&mut [u8]; m])` — encode k data shards into m parity shards
- Runtime CPU feature detection via `std::is_x86_feature_detected!("avx512f")`
- Feature-gated behind `#[cfg(feature = "isa-l")]` in dedicated module `oceanfs-accel/src/isal.rs`
- Compile-time arch gate: `#[cfg(target_arch = "x86_64")]` — ISA-L is x86-only
- Encode/decode table caching: precomputed tables reused across stripes in a segment
- Fallback: if AVX-512 is not detected at runtime, backend constructor returns `None` (dispatcher falls back to CPU SIMD)
- `unsafe` FFI wrappers with `// SAFETY:` comments verifying: non-null pointers, 64-byte alignment, matching k/m parameters, thread-safety per ISA-L docs

### Out of Scope

- ISA-L on non-x86 architectures (ISA-L is Intel x86-only)
- ISA-L for compression (that is the `isal-igzip-compression` feature in the `compression-acceleration` epic)
- Multi-threaded stripe encoding within ISA-L (ISA-L EC is single-threaded; parallelism achieved via Rayon at the `ParallelEncoder` level in `oceanfs-ec`)
- Build-time ISA-L library discovery (handled by `isal-rs` crate or `build.rs`)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-accel` | New module `isal.rs` — `#[cfg(feature = "isa-l")]` |
| `oceanfs-accel` | Feature `isa-l = ["dep:isal-rs"]` in `Cargo.toml` |
| `oceanfs-accel` | Facade export: `#[cfg(feature = "isa-l")] pub use isal::IsalEncoder`, `IsalDecoder` |
| `oceanfs-core` | New error variants: `AccelError::IsalInitError`, `AccelError::IsalEncodeError` |

## Interface (Public API)

- `pub struct IsalEncoder` — `pub fn new() -> Option<Self>` (returns `None` if AVX-512 not detected), `pub fn is_available() -> bool`
- `pub struct IsalDecoder` — `pub fn new(tables: &IsalTables) -> Option<Self>`, `pub fn is_available() -> bool`
- `impl Encoder for IsalEncoder` — delegates to ISA-L FFI `ec_encode_data`
- `impl Decoder for IsalDecoder` — delegates to ISA-L FFI (reconstructed matrix)
- `pub(crate) struct IsalTables` — precomputed encoding tables (`[u8; 32*k*m]`), reused across stripes
- `pub(crate) unsafe fn isal_ec_init_tables(k: u8, m: u8, tables: &mut [u8]) -> c_int` — FFI wrapper
- `pub(crate) unsafe fn isal_ec_encode_data(len: usize, k: u8, m: u8, tables: &[u8], data: &[&[u8]], parity: &mut [&mut [u8]]) -> c_int` — FFI wrapper

## Data Flow

```
AccelDispatcher::resolve(bucket_tier):
  └─ tier == AccelTier::IsaL || tier == AccelTier::Auto:
       └─ IsalEncoder::new():
            ├─ cfg(target_arch = "x86_64")? → no → return None
            ├─ cfg(feature = "isa-l")? → no → return None
            ├─ std::is_x86_feature_detected!("avx512f")? → no → return None
            ├─ ec_init_tables(k, m, &mut tables)   // precompute encoding matrix
            └─ return Some(IsalEncoder { tables })

Encode path (per segment):
  Segment sealed → StripeBatch assembled (SoA layout):
    └─ IsalEncoder::encode(&data_shards, m):
         ├─ For each stripe in batch:
         │    ├─ // SAFETY: tables initialized with correct k,m;
         │    │   //         data_shards: k pointers each to strip_size aligned bytes;
         │    │   //         parity_out: m mutable pointers each to strip_size bytes
         │    ├─ unsafe { isal_ec_encode_data(strip_size, k, m, &tables,
         │    │                              data_ptrs, parity_ptrs) }
         │    └─ Check return code → Ok(parity) or Err(AccelError::IsalEncodeError)
         └─ Return m parity shards

Decode path (per segment, reconstruction):
  IsalDecoder::decode(&available_shards, &erased_indices):
    └─ Reconstruct decode matrix from available shard indices
       └─ unsafe { isal_ec_encode_data(...) } with reconstructed mapping
```

## Definition of Done

- [ ] **Code:** `cargo build --features isa-l` succeeds on x86_64; `cargo build --all-targets` (no features) also succeeds
- [ ] **Tests:** ISA-L encode round-trip matches Cauchy RS output (bit-exact comparison); AVX-512 not detected → `IsalEncoder::new()` returns `None`; encode stale tables (mismatched k/m) → returns error; decode with missing shards → reconstructs correctly; table precomputation cached across stripes
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-accel` with `isa-l` feature
- [ ] **Lint:** `cargo clippy -- -D warnings` passes; every `unsafe` block has `// SAFETY:` comment citing the specific invariant
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `IsalEncoder` docs explain AVX-512 requirement, table precomputation, and thread model
- [ ] **ADR:** ADR-0006 constraints satisfied — trait-based pluggability (§3), feature-gated compilation (§6), startup probing cached for lifetime (§1), fallback chain with warnings (§2)
- [ ] **Perf:** Rule 4.3 (feature-gated SIMD), 5.3 (feature-gated SIMD compilation), 6.4 (static dispatch via `Arc<dyn Encoder>` in dispatcher, not hot-path dynamic dispatch), 12.1 (SAFETY comments on every `unsafe` block)
- [ ] **Integration:** `tests/isal_ec_roundtrip.rs`: encode with ISA-L, decode with Cauchy RS (cross-backend round-trip); encode with Cauchy RS, decode with ISA-L; verify bit-exact match. Run with and without AVX-512 hardware
- [ ] **Manual:** Example in `IsalEncoder` docs compiles and runs on x86_64 with AVX-512
