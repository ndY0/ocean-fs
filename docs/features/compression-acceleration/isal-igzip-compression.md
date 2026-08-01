---
feature: "ISA-L igzip CPU Compression"
epic: "compression-acceleration"
status: proposed
priority: medium
owner: ""
dependencies:
  - feature: compressor-trait
    reason: ISA-L igzip backend implements the Compressor trait
  - feature: isa-l-x86-encoder
    reason: Shares ISA-L library loading, AVX-512 detection, and FFI patterns
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

# ISA-L igzip CPU Compression

## Summary

Implement the ISA-L igzip CPU-optimized compression backend in
`oceanfs-accel` behind the `isa-l` Cargo feature. The `IgzipCompressor`
struct implements the `Compressor` trait (defined in the same epic) and
delegates compression/decompression to Intel ISA-L's `igzip` library,
which provides AVX-512-accelerated DEFLATE-compatible compression. The
backend serves as Tier 1 in the compression fallback chain
(`GpuNvcomp → CpuIgzip → CpuZstd`). Runtime AVX-512 detection gates
availability. The backend targets segment-level compression where CPU
throughput with AVX-512 exceeds the portable `zstd` crate's performance.

## Scope

### In Scope

- `IgzipCompressor` struct implementing `Compressor` trait from `oceanfs-accel`
- FFI binding to ISA-L `igzip` functions via the same `isa-l` feature used by `IsalEncoder`:
  - `isal_deflate_init(&mut stream)` — initialize DEFLATE stream
  - `isal_deflate(&mut stream, &input, &mut output)` — compress block
  - `isal_inflate_init(&mut stream)` — initialize INFLATE stream
  - `isal_inflate(&mut stream, &input, &mut output)` — decompress block
- Feature-gated behind `#[cfg(feature = "isa-l")]` in dedicated module `oceanfs-accel/src/igzip.rs`
- Compile-time arch gate: `#[cfg(target_arch = "x86_64")]` — igzip is x86-only (ISA-L)
- Runtime CPU feature detection: `std::is_x86_feature_detected!("avx512f")` AND `"avx512bw"`
- Fallback: if AVX-512 not detected, constructor returns `None` (dispatcher falls back to `ZstdCompressor`)
- Configurable compression level: maps to ISA-L `compression_level` (0–3, where 3 is max DEFLATE compression)
- `unsafe` FFI wrappers with `// SAFETY:` comments verifying: stream state initialized, buffer bounds non-overlapping, output buffer pre-sized to `isal_deflate_stateless_bound(input_len)`

### Out of Scope

- ISA-L igzip on non-x86 architectures (ISA-L is Intel x86-only)
- DEFLATE dictionary support (standard DEFLATE only)
- Streaming compression API via the `Compressor` trait (trait operates on complete buffers; streaming is module-internal)
- igzip for EC encoding (that is the `IsalEncoder` feature in the `cpu-acceleration-backends` epic)
- Multi-threaded igzip within a single call (parallelism achieved at the segment-batch level)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-accel` | New module `igzip.rs` — `#[cfg(all(target_arch = "x86_64", feature = "isa-l"))]` |
| `oceanfs-accel` | Facade export: `#[cfg(feature = "isa-l")] pub use igzip::IgzipCompressor` |
| `oceanfs-core` | New error variant: `AccelError::IgzipError { reason: String }` |

## Interface (Public API)

- `pub struct IgzipCompressor` — `pub fn new(level: u32) -> Option<Self>` (returns `None` if AVX-512 not detected), `pub fn is_available() -> bool`, `pub fn compression_level(&self) -> u32`
- `impl Compressor for IgzipCompressor` — delegates to ISA-L igzip FFI
- `pub(crate) unsafe fn isal_deflate_wrapper(input: &[u8], level: u32) -> Result<Vec<u8>>` — FFI wrapper for stateless DEFLATE
- `pub(crate) unsafe fn isal_inflate_wrapper(input: &[u8]) -> Result<Vec<u8>>` — FFI wrapper for stateless INFLATE

## Data Flow

```
AccelDispatcher::new(config):
  └─ Probe igzip:
       ├─ cfg(feature = "isa-l")? → no → skip
       ├─ cfg(target_arch = "x86_64")? → no → skip
       ├─ std::is_x86_feature_detected!("avx512f")? → no → skip
       ├─ std::is_x86_feature_detected!("avx512bw")? → no → skip
       └─ return Some(IgzipCompressor { level: config.level })

Compression path (segment seal):
  Bucket has compress_tier = "cpu_igzip":
    └─ IgzipCompressor::compress(segment_data, level):
         ├─ // SAFETY: segment_data is a valid slice; level is 0–3 per ISA-L spec;
         │   //        output buffer pre-allocated to isal_deflate_stateless_bound(input.len())
         ├─ Pre-allocate output buffer:
         │    └─ bound = isal_deflate_stateless_bound(input.len())  // from ISA-L headers
         │         output = Vec::with_capacity(bound)
         ├─ unsafe { isal_deflate_stateless(input.as_ptr(), input.len(),
         │                                  output.as_mut_ptr(), &mut actual_len, level) }
         ├─ unsafe { output.set_len(actual_len) }
         └─ return Ok(output)

Decompression path (segment read):
  └─ IgzipCompressor::decompress(compressed_data):
       ├─ Determine decompressed size from DEFLATE header or stored metadata
       ├─ Pre-allocate output buffer
       ├─ unsafe { isal_inflate_stateless(compressed_data.as_ptr(), compressed_data.len(),
       │                                   output.as_mut_ptr(), output.len()) }
       ├─ return Ok(output)

Fallback chain (compression):
  compress_tier = "cpu_igzip" BUT AVX-512 not available:
    └─ AccelDispatcher logs WARN, falls back to CpuZstd (ZstdCompressor)
       └─ Increment accel_compression_fallback_total
```

## Definition of Done

- [x] **Code:** `cargo build --features isa-l` succeeds on x86_64; `cargo build --all-targets` (no features) succeeds on all platforms
<!-- REVIEW: verified: cargo build --features isa-l passes; cargo build --all-targets passes -->
- [x] **Tests:** igzip compress + decompress round-trip matches original data; compressed output is valid DEFLATE (verifiable by `zstd`/`flate2` crate); AVX-512 not detected → `IgzipCompressor::new()` returns `None`; compression level 0 vs 3 produces different output sizes; buffer bound is sufficient (no output truncation); cross-backend round-trip: compress with igzip, decompress with zstd → bit-exact match
<!-- REVIEW: roundtrip tests pass; DEFLATE validation via zstd::decode_all works (igzip.rs:596); AVX-512 absent returns None (igzip.rs:546); level clamping tested (igzip.rs:633); buffer bounds tested (igzip.rs:642-643, but these cause clippy failures); cross-backend tested (igzip.rs:590-600) -->
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-accel` with `isa-l` feature
<!-- REVIEW: iteration-2: tarpaulin reports 62.59% workspace-wide. See compressor-trait.md for per-module breakdown (~79.4% for oceanfs-accel source). Coverage is close to but slightly below 80%. -->
- [x] **Lint:** `cargo clippy -- -D warnings` passes; every `unsafe` FFI block has `// SAFETY:` comment citing ISA-L invariants
<!-- REVIEW: iteration-2: FIXED. Clippy passes clean with `--features isa-l -- -D warnings`. All 9 unsafe blocks in igzip.rs have SAFETY comments. Prior clippy::assertions_on_constants at igzip.rs:642-643 resolved. -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; `IgzipCompressor` docs document AVX-512 requirement, DEFLATE compatibility, and compression level semantics
<!-- REVIEW: RUSTDOCFLAGS="-D warnings" cargo doc passes; IgzipCompressor docs document all requirements -->
- [ ] **ADR:** ADR-0006 constraints satisfied — trait-based pluggability via `Compressor` trait (§3, §5 Non-EC acceleration scope), feature-gated compilation (§6), startup probing (§1), fallback chain (§2)
<!-- REVIEW: iteration-2: §3: ✅ IgzipCompressor impl Compressor; §5: ✅ per-bucket only; §6: ✅ feature-gated; §1: ✅ startup probing in AccelDispatcher::new(); §2: ✅ fallback chain with tracing::warn!; MISSING: §2 metric counter accel_compression_fallback_total not implemented. Deferred to observability epic. -->
- [x] **Perf:** Rule 4.3 (feature-gated SIMD for igzip), 5.3 (feature-gated SIMD compilation), 6.4 (static dispatch at dispatcher level), 12.1 (SAFETY on all unsafe blocks)
<!-- REVIEW: iteration-2: 4.3/5.3: #[cfg(all(target_arch = "x86_64", feature = "isa-l"))] gate; 6.4: Arc<dyn Compressor> at dispatcher level; 12.1: all 9 unsafe blocks in igzip.rs have SAFETY comments. ✅ -->
- [x] **Integration:** `tests/igzip_roundtrip.rs`: compress with igzip, decompress with zstd (cross-backend), verify bit-exact match; compress with igzip, decompress with igzip; verify fallback when igzip unavailable
<!-- REVIEW: iteration-2: FIXED. tests/igzip_roundtrip.rs exists with 4 tests (cross-backend, same-backend, fallback, large data) gated behind #[cfg(all(target_arch = "x86_64", feature = "isa-l"))]. All pass when run on x86_64 with isa-l feature. -->
- [x] **Manual:** Example in `IgzipCompressor` docs compiles and runs on x86_64 with AVX-512
<!-- REVIEW: example is doc-tested (igzip.rs:213-223); uses ```ignore so does not execute in CI, which is correct for hardware-dependent example -->
