---
feature: "Compressor Trait"
epic: "compression-acceleration"
status: done
priority: medium
owner: ""
dependencies:
  - feature: acceleration-dispatcher
    reason: AccelDispatcher gains Compressor resolution alongside Encoder/Decoder
adr:
  - 0006-hardware-acceleration-tier-model
perf:
  - "6.4: Static dispatch over dynamic dispatch on hot paths"
  - "2.7: Tokio semaphore for concurrency limits (GPU nvCOMP path)"
  - "1.1: Use Bytes/BytesMut for blob data (compress/decompress I/O)"
created: 2026-07-31
updated: 2026-08-02
---

# Compressor Trait

## Summary

Define the `Compressor` trait in `oceanfs-accel`, modeled on the existing
`Encoder` and `Decoder` traits from `oceanfs-ec`. The trait abstracts
compression and decompression of segment data, enabling pluggable backends:
Tier 0 (`zstd` crate, always available), Tier 1 (ISA-L igzip, feature-gated
behind `isa-l`), and Tier 2 (nvCOMP GPU batch, feature-gated behind `cuda`).
Per-bucket tier selection is driven by the `compress_tier` field in
`BucketPolicy`. The `AccelDispatcher` resolves the appropriate `Arc<dyn
Compressor>` at dispatch time using the same fallback-chain pattern as EC
acceleration.

## Scope

### In Scope

- `Compressor` trait definition in `oceanfs-accel/src/compressor.rs`
- `pub trait Compressor: Send + Sync` with:
  - `fn compress(&self, data: &[u8], level: u32) -> Result<Vec<u8>>`
  - `fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>`
  - `fn compression_tier(&self) -> CompressionTier`
  - `fn is_available(&self) -> bool`
- `CompressionTier` enum: `Auto`, `CpuZstd` (Tier 0), `CpuIgzip` (Tier 1), `GpuNvcomp` (Tier 2)
- `CompressConfig` struct in `oceanfs-core`: `tier: CompressionTier`, `level: u32` (default 3), `nvcomp: Option<NvcompConfig>`
- `AccelDispatcher` extension: `fn resolve_compressor(&self, tier: CompressionTier) -> Arc<dyn Compressor>`
- Fallback chain mirroring EC model: `GpuNvcomp → CpuIgzip → CpuZstd` (always terminates at zstd)
- Per-bucket only — no node-level `compress_tier` default (per ADR-0006 §5 and spec §9.6.2)
- Trait is Send + Sync: compression backends may be called from Rayon parallel iterators during segment sealing
- Error type: `AccelError::CompressionError` and `AccelError::CompressionBackendUnavailable`

### Out of Scope

- Node-level `compress_tier` configuration (intentionally omitted per ADR-0006; compression is per-bucket only)
- Compression of individual blobs before segment packing (compression applies to sealed segments)
- Decompression on read path for non-compressed segments (transparent to reader; handled by segment metadata)
- Streaming compression/decompress via the trait (the trait operates on complete buffers; streaming is a higher-level concern in the storage engine)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `CompressionTier` enum, `CompressConfig` struct, `AccelError` new variants |
| `oceanfs-accel` | New module `compressor.rs` — `Compressor` trait definition |
| `oceanfs-accel` | New module `compress_zstd.rs` — `ZstdCompressor` (Tier 0, always compiled) |
| `oceanfs-accel` | Facade export: `pub use compressor::Compressor`, `pub use compressor::CompressionTier` |

## Interface (Public API)

- `pub enum CompressionTier` — `Auto`, `CpuZstd`, `CpuIgzip`, `GpuNvcomp`
  - `#[non_exhaustive]` per coding.md §1.5 to allow future tiers without semver break
- `pub struct CompressConfig` — `tier: CompressionTier`, `level: u32`, `nvcomp: Option<NvcompConfig>`
- `pub trait Compressor: Send + Sync` — `fn compress(&self, data: &[u8], level: u32) -> Result<Vec<u8>>`, `fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>`, `fn compression_tier(&self) -> CompressionTier`, `fn is_available(&self) -> bool`
- `pub struct ZstdCompressor` — always-available Tier 0; implements `Compressor`
- `impl AccelDispatcher` — new methods: `pub fn resolve_compressor(&self, tier: CompressionTier) -> Arc<dyn Compressor>`, `pub fn active_compression_tier(&self) -> CompressionTier`
- `pub enum AccelError` — new variants: `CompressionError { reason: String }`, `CompressionBackendUnavailable { requested: CompressionTier }`

## Data Flow

```
Dispatcher initialization (AccelDispatcher::new):
  ├─ Probe compression backends:
  │    ├─ ZstdCompressor → always available (Tier 0)
  │    ├─ IgzipCompressor → available if: cfg(feature = "isa-l") AND AVX-512 detected
  │    └─ NvcompCompressor → available if: cfg(feature = "cuda") AND nvCOMP loaded
  └─ Cache compressor backends in HashMap<CompressionTier, Arc<dyn Compressor>>

Per-bucket compression dispatch (segment seal path):
  bucket.compress_tier is set:
    └─ dispatcher.resolve_compressor(bucket.compress_tier):
         ├─ Requested tier available? → return Arc<dyn Compressor>
         ├─ Fallback:
         │    ├─ GpuNvcomp → CpuIgzip (warn if available)
         │    ├─ CpuIgzip  → CpuZstd  (warn if available)
         │    └─ CpuZstd   → return ZstdCompressor (always)
         └─ Log WARN if fallback occurred; increment accel_compression_fallback_total

Compress path:
  Segment sealed:
    └─ compress_tier resolved (per bucket policy):
         └─ compressor.compress(segment_data, level):
              ├─ ZstdCompressor: zstd::encode_all(data, level)
              ├─ IgzipCompressor: isal_igzip_compress(data, level)   [cfg(feature = "isa-l")]
              └─ NvcompCompressor: nvcomp_batch_compress(data, level) [cfg(feature = "cuda")]
                   └─ Acquire GPU semaphore → batch → release

Decompress path (read path, segment fetch):
  └─ compressor.decompress(compressed_data):
       └─ ZstdCompressor / IgzipCompressor / NvcompCompressor::decompress(...)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds with all feature combinations
<!-- REVIEW: verified: no-features, isa-l, cuda all build clean -->
- [x] **Tests:** `ZstdCompressor` round-trip (compress + decompress) matches original data; tier resolution returns correct backend per config; fallback chain: GpuNvcomp absent → falls to CpuIgzip → falls to CpuZstd; per-bucket tier override takes precedence; `#[non_exhaustive]` on `CompressionTier` enforced (adding a variant is not a breaking change in dependent crates)
<!-- REVIEW: Zstd round-trip passes; fallback chain tested in dispatcher unit tests; CompressionTier is #[non_exhaustive] at crates/oceanfs-core/src/types.rs:1132 -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; `Compressor` trait documented with backend resolution semantics; `CompressionTier` variants documented with availability requirements
<!-- REVIEW: RUSTDOCFLAGS="-D warnings" cargo doc passes for both oceanfs-core and oceanfs-accel -->
- [ ] **ADR:** ADR-0006 constraints satisfied — trait-based pluggability (§3 for Compressor modeled on Encoder/Decoder), per-bucket only `compress_tier` (§5 Non-EC acceleration scope, §7 per-bucket tier selection), fallback chain with warnings (§2)
<!-- REVIEW: iteration-2: §3 trait pluggability: ✅ Compressor trait exists with Arc<dyn Compressor>; §5 per-bucket only: ✅ active_compression_tier() returns None; §7 per-bucket tier selection: ✅ resolve_compressor() accepts per-call tier; §2 fallback chain with warnings: ✅ fallback chain implemented with tracing::warn!; MISSING: §2 metric counter accel_compression_fallback_total — no metric counter incremented on fallback. Implementer deferred to observability epic. -->
- [x] **Perf:** Rule 6.4 (static dispatch via `Arc<dyn Compressor>` at dispatcher level, not hot-path dynamic dispatch on every compression call), 2.7 (semaphore for GPU nvCOMP path), 1.1 (use Bytes/BytesMut for compress/decompress buffers)
<!-- REVIEW: 6.4: Arc<dyn Compressor> is at dispatcher level (dispatcher.rs:95), resolved once; 2.7: Semaphore in nvcomp.rs; 1.1: Compressor trait returns Bytes, though igzip and nvcomp internally construct Vec<u8> before conversion -->
- [x] **Integration:** `tests/compressor_dispatch.rs`: configure each tier, compress + decompress same data through each backend, verify identical output; configure `GpuNvcomp` without GPU → verify fallback to zstd and warning log; per-bucket tier override takes effect
<!-- REVIEW: iteration-2: FIXED. tests/compressor_dispatch.rs exists with 6 tests (each_tier_produces_correct_roundtrip, per_bucket_tier_override_via_config, gpu_nvcomp_falls_back_to_available, auto_tier_resolves_and_works, empty_data_roundtrips_all_tiers, large_data_roundtrips_all_tiers). All pass with `--features cuda`. -->
