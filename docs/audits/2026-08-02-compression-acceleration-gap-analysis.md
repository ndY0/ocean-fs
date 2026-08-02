---
audit_date: 2026-08-02
scope: targeted
target_crates: oceanfs-accel, oceanfs-core, oceanfs-server
severity_counts:
  critical: 3
  high: 3
  medium: 5
  low: 3
adrs_referenced:
  - 0006-hardware-acceleration-tier-model
  - 0007-compression-tier-governance
---

# Audit Report: Compression Acceleration Epic — Gap Analysis (v2)

_Updated 2026-08-02 to incorporate ADR-0007 (Node-Governed Compression Tier)._

## Summary

The compression acceleration epic has substantial implementation progress across
all three tiers (`ZstdCompressor` Tier 0, `IgzipCompressor` Tier 1,
`NvcompCompressor` Tier 2), but has **three critical integration gaps** that
prevent the subsystem from being usable:

1. **ADR-0007 mandates a two-level governance model** (node-level ceiling +
   per-bucket opt-down). Neither the node-level `[compression]` config nor the
   per-bucket `[bucket.my-bucket.compression]` config exist in the codebase.
2. **nvCOMP batch compression is not implemented** (`num_chunks` hardcoded to
   1), contradicting the spec's batched compression model and
   `NvcompConfig::batch_size`.
3. **`CompressionTier` enum has no `None` variant**, but ADR-0007 specifies
   `"none"` as a valid tier for both node-level (`compression.tier = "none"`)
   and per-bucket (`compress_tier = "none"`) to disable compression.

Additionally, ADR-0007's resolution semantics (`effective_tier = min(T_bucket,
T_node)` on the capability ordering) are not implemented in
`resolve_compressor()` — the dispatcher has no node ceiling concept.

The three feature documents (`compressor-trait.md`, `isal-igzip-compression.md`,
`nvcomp-gpu-compression.md`) are marked `in_progress` or `done` with several
unchecked DoD items, including the ADR-mandated
`accel_compression_fallback_total` metric counter.

---

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `oceanfs-core/src/config.rs` (no `CompressionConfig`), `oceanfs-core/src/types.rs` (no `None` variant on `CompressionTier`) | **ADR-0007 node-level `[compression]` config missing.** ADR-0007 Decision mandates a node-level `[compression]` section in `oceanfs.toml` with `enabled`, `tier`, and `compression_gpu_min_batch_bytes` fields. No `CompressionConfig` struct exists in `oceanfs-core`. The `AccelConfig` struct has no `compression` field. `AccelDispatcher::new()` accepts only `AccelConfig` — there is no way to pass a node-level compression ceiling. **Also: `CompressionTier` enum has no `None` variant**, but ADR-0007 specifies `"none"` as a valid tier for disabling compression at both node and bucket levels. | 1) Add `CompressionConfig` struct to `oceanfs-core/src/config.rs` with `enabled: bool`, `tier: CompressionTier`, `gpu_min_batch_bytes: u64`. 2) Add `compression: CompressionConfig` field to `AccelConfig`. 3) Add `None` variant to `CompressionTier` enum in `oceanfs-core/src/types.rs`. 4) Pass `CompressionConfig` to `AccelDispatcher::new()`. |
| C2 | `crates/oceanfs-server/src/bucket_config.rs:36` (`BucketPolicy`) | **ADR-0007 per-bucket `[bucket.my-bucket.compression]` config missing.** ADR-0007 defines a per-bucket compression sub-config with `tier` and `level` fields. The current `BucketPolicy` struct has no compression field whatsoever. `CompressConfig` exists in `oceanfs-core` types but is not wired into `BucketPolicy`. **No bucket can configure compression.** | Add `pub compression: CompressConfig` field to `BucketPolicy`. Wire into server's S3 handler and admin API for per-bucket `compress_tier` + `level` configuration. |
| C3 | `crates/oceanfs-accel/src/cuda/nvcomp.rs:243` (`NvcompCompressor::compress`) | **nvCOMP batch compression not implemented.** `num_chunks` is hardcoded to `1`. Spec §9.6.2 states: "Compression is batched across multiple segments when sealing or healing." `NvcompConfig::batch_size` defaults to 16 but is never used. `NvcompCompressor::new()` does not accept `NvcompConfig` as a parameter (takes only a `Semaphore`). | Accept `NvcompConfig` in `NvcompCompressor::new()`. Implement multi-chunk batched compression using `batch_size`. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `crates/oceanfs-accel/src/dispatcher.rs:314-320` (`resolve_compressor`) | **No node-ceiling resolution in `resolve_compressor()`.** ADR-0007 Decision: `effective_tier = min(T_bucket, T_node)` on the ordering `GpuNvcomp > CpuIgzip > CpuZstd > None`. The current `resolve_compressor()` applies the fallback chain but has no concept of a node-level ceiling — any bucket can request `GpuNvcomp` and it will be used if available, regardless of what the node operator configured. ADR-0007 explicitly rejects this: "A bucket can only select a tier ≤ the node's tier." | Modify `resolve_compressor()` to accept an optional `node_ceiling: CompressionTier`. Cap the effective tier: if `requested > ceiling`, use `ceiling` (with a `DEBUG` log, not `WARN` — the bucket is within its rights to request a higher tier; the node constrains it). |
| H2 | `crates/oceanfs-accel/src/cuda/nvcomp.rs` | **No `NvcompBufferPool` (pinned memory pool).** The feature doc (`nvcomp-gpu-compression.md`) and perf guideline 1.2 require a pinned (page-locked) host memory pool for DMA transfers. Current implementation allocates regular `CudaSlice` objects via `device.alloc()` instead of using pinned host memory + `cudaMemcpyAsync`. Without pinned memory, GPU transfers incur an extra copy through driver bounce buffers, halving PCIe throughput. | Implement `NvcompBufferPool` in `cuda/nvcomp_buffer.rs` with `acquire_pinned()`/`release()`. Use `cudaMallocHost` for pinned allocation. Integrate with `cudaMemcpyAsync` on the CUDA stream. |
| H3 | `crates/oceanfs-accel/src/dispatcher.rs:338-377` (`resolve_compression_tier_with_fallback`), `src/igzip.rs`, `src/cuda/nvcomp.rs` | **Missing `accel_compression_fallback_total` metric counter.** ADR-0006 §2 (as amended) requires: "A metric counter `accel_fallback_total` is incremented for each fallback event." ADR-0007 reinforces this with node-level governance — operators must know when their ceiling is being hit. The fallback chain logs `tracing::warn!` but does not increment any counter. All three feature docs acknowledge this as missing. | Add an `AtomicU64` counter on `AccelDispatcher`. Increment it in `resolve_compression_tier_with_fallback()` when a fallback occurs. Label with `from_tier` and `to_tier`. Wire into the metrics registry. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `docs/features/compression-acceleration/isal-igzip-compression.md:4`, `docs/features/compression-acceleration/nvcomp-gpu-compression.md:4` | **Feature documents still marked `status: in_progress`.** Despite substantial implementation, both feature docs have unchecked DoD items (ADR compliance, NvcompBufferPool, metric counter). | Either complete remaining DoD items or split into separate tracking issues and mark current scope as done. |
| M2 | `crates/oceanfs-accel/src/dispatcher.rs` | **No `parse_compression_tier()` function.** ADR-0007 defines string values for both node-level and bucket-level tiers: `"auto"`, `"cpu_zstd"`, `"cpu_igzip"`, `"gpu_nvcomp"`, `"none"`. The EC tier has `parse_ec_tier()` mapping strings to `AccelTier`. Compression has no equivalent — `CompressionTier` is only constructed as a Rust enum directly in tests. A string parser is needed for TOML config deserialization of both `[compression].tier` and `[bucket.my-bucket.compression].tier`. | Add `parse_compression_tier(&str) -> CompressionTier` to the dispatcher or `CompressionTier` impl. Map all five ADR-0007 string values. Return `Err` for unknown values (unlike `parse_ec_tier()` which silently falls back to `Auto` — per ADR-0007, operators should be told when their config is invalid). |
| M3 | `crates/oceanfs-accel/src/igzip.rs:436-494` (`IgzipCompressor::decompress`) | **Decompress overflow retry has only one retry attempt.** If the initial 16x buffer is insufficient, a 64x retry is attempted. But if that also fails, an error is returned. For extremely compressible data (e.g., 10,000:1 ratio), this could fail. | Replace single retry with a loop that doubles buffer size up to a sane maximum (e.g., 1 GB), or pre-store decompressed size in segment metadata. |
| M4 | `crates/oceanfs-accel/src/cuda/nvcomp.rs` | **`NvcompCompressor::new()` does not use `NvcompConfig`.** The constructor accepts only `Arc<Semaphore>` — ignoring `codec`, `batch_size`, and `device_id`. | Pass `NvcompConfig` into `NvcompCompressor::new()`. Use `config.batch_size` for `num_chunks`. Use `config.codec` for codec dispatch. Use `config.device_id` for `CudaDevice::new(device_id)`. |
| M5 | `crates/oceanfs-accel/src/cuda/nvcomp.rs:40-108 (FFI declarations)` | **Only LZ4 codec implemented.** `NvcompCodec` declares `Lz4`, `Snappy`, `Zstd` but only `Lz4` has FFI bindings. | Add FFI declarations for Snappy and zstd, or remove unimplemented variants from `NvcompCodec` to avoid false expectations. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `crates/oceanfs-accel/src/cuda/nvcomp.rs:176-184` | **Redundant `unsafe impl Send/Sync` comments.** Duplicate/near-duplicate SAFETY blocks for `Send` and `Sync` impls. | Consolidate into a single `// SAFETY:` block per coding.md §7.2. |
| L2 | `docs/spec.md:1263` (Spec §9.6.2) | **Spec trait returns `Vec<u8>`, implementation returns `Bytes`.** The spec shows `fn compress(...) -> Result<Vec<u8>>` but the implementation at `compressor.rs:56` returns `Result<Bytes>`. This is a performance improvement (zero-copy, perf guideline 1.1), but diverges from the spec. | Update spec §9.6.2: use `bytes::Bytes` in the trait signature and document zero-copy semantics. |
| L3 | `crates/oceanfs-storage/src/segment/`, `crates/oceanfs-server/src/` | **No end-to-end compression integration.** Compression is tested at the accel crate level but not wired into the segment seal/read path in `oceanfs-storage` or `oceanfs-server`. Expected for a "separate epic" (spec §9.6.2), but means compression cannot be used in practice even after C1-C3 are addressed. | Wire `resolve_compressor` into `SegmentSealer`. Store `compressed: bool` and `compression_tier` in `SegmentMetadata`. Decompress on read. |

---

## Coupling Hotspots

No compression-specific coupling hotspots detected. The compression subsystem follows the same pattern as EC acceleration: `Compressor` trait in `oceanfs-accel`, resolved by `AccelDispatcher`, consumed by consumers.

| Symbol | Crate | In-Degree | Risk |
|---|---|---|---|
| `AccelDispatcher::new` | oceanfs-accel | 141 | Medium — central dispatcher; ADR-0007 will add a `CompressionConfig` parameter, affecting all call sites |

## Dependency Graph

The DAG constraint from `guidelines/architecture.md` §1.1 is satisfied:

```
oceanfs-core → oceanfs-accel → oceanfs-storage / oceanfs-server
```

`oceanfs-accel` depends on `oceanfs-core` and `oceanfs-ec` (for traits). No circular dependencies. ADR-0007 does not alter this graph.

## Guideline Violations

| Guideline | Location | Violation |
|---|---|---|
| ADR-0007 Decision | `oceanfs-core/src/config.rs` (no `CompressionConfig`) | Node-level `[compression]` section not implemented |
| ADR-0007 Decision | `BucketPolicy` (`bucket_config.rs:36`) | Per-bucket `[bucket.my-bucket.compression]` section not implemented |
| ADR-0007 Decision | `dispatcher.rs:314-320` | `resolve_compressor()` has no node-ceiling capping logic |
| ADR-0007 Decision | `CompressionTier` (`types.rs:1163`) | No `None` variant for disabling compression |
| ADR-0006 §2 (as amended by ADR-0007) | `dispatcher.rs:338-377` | `accel_compression_fallback_total` metric counter not implemented |
| Perf 1.2 | `nvcomp.rs` | No pinned memory pool for DMA buffers |
| Coding §7.2 | `nvcomp.rs:176-184` | Redundant `// SAFETY:` comments |
| Coding §1.5 | `CompressionTier` (`types.rs:1163`) | `#[non_exhaustive]` is present ✅ |
| Coding §5.1 | All public compression types | `#![deny(missing_docs)]` passes ✅ |

## ADR Compliance

### ADR-0006 (Hardware Acceleration Tier Model) — as amended by ADR-0007

| ADR Clause | Status | Notes |
|---|---|---|
| §1: Startup probing | ✅ Compliant | `AccelDispatcher::new()` probes igzip (AVX-512) and nvCOMP (CUDA) at startup |
| §2: Fallback chain with warnings | ⚠️ Partial | Fallback chain works with `tracing::warn!`; metric counter `accel_compression_fallback_total` missing |
| §3: Trait-based pluggability | ✅ Compliant | `Compressor` trait; all three backends implement it |
| §4: GPU concurrency model | ✅ Compliant | `NvcompCompressor` uses `tokio::sync::Semaphore` with `try_acquire()` |
| §5: Per-bucket only compression | ❌ **Amended by ADR-0007** | ADR-0007 introduces node-level governance with per-bucket opt-down. Neither exists in code. |
| §6: Feature-gated compilation | ✅ Compliant | `#[cfg]` gates on `isa-l`, `cuda`, `no_cuda_toolkit`, `no_nvcomp` |
| §7: Per-bucket tier selection | ❌ **Not Implemented** | No `compress_tier` in `BucketPolicy`; ADR-0007 revises this to opt-down model |

### ADR-0007 (Node-Governed Compression Tier)

| ADR Clause | Status | Notes |
|---|---|---|
| Node-level `[compression]` section | ❌ **Not Implemented** | No `CompressionConfig` struct; no `enabled`/`tier`/`gpu_min_batch_bytes` fields |
| Per-bucket `[bucket.my-bucket.compression]` section | ❌ **Not Implemented** | No `compression` field on `BucketPolicy` |
| Ceiling resolution: `min(T_bucket, T_node)` | ❌ **Not Implemented** | `resolve_compressor()` has no node-ceiling parameter |
| Bucket can only downgrade, never upgrade | ❌ **Not Implemented** | Falls out from above — no ceiling means no downgrade enforcement |
| `"none"` tier for disabling compression | ❌ **Not Implemented** | `CompressionTier` enum has no `None` variant |
| Migration path tasks (ADR-0007 §Migration) | ❌ **None started** | Five tasks listed: config struct, `AccelDispatcher` change, `resolve_compressor` change, spec update, ADR-0006 §5 amendment |

## Test Coverage

| Crate / Module | Public Symbols | Tests | Coverage |
|---|---|---|---|
| `oceanfs-accel::compressor` (`ZstdCompressor`) | 1 struct, 1 trait | 6 unit tests | Good |
| `oceanfs-accel::igzip` (`IgzipCompressor`) | 1 struct | 14 unit tests | Good |
| `oceanfs-accel::cuda::nvcomp` (`NvcompCompressor`) | 1 struct | 5 unit tests | Adequate (requires GPU) |
| `oceanfs-accel::dispatcher` (compression dispatch) | `resolve_compressor()` | 2 unit + 6 integration tests | Good |
| `oceanfs-accel/tests/igzip_roundtrip.rs` | — | 4 integration tests | Good (x86_64 + isa-l) |
| `oceanfs-accel/tests/nvcomp_roundtrip.rs` | — | 4 integration tests | Adequate (requires GPU) |
| `oceanfs-core::CompressionTier` | 1 enum | 2 unit tests | Minimal |
| `oceanfs-server::BucketPolicy` | No compression field | 0 compression tests | **None** |
| **Node-level `CompressionConfig`** | **Does not exist** | **None** | **N/A** |

---

## Recommendations

### Immediate (Blocking — Compression Unusable Without These)

1. **Implement ADR-0007 node-level `[compression]` config** (C1):
   - Add `CompressionConfig { enabled, tier, gpu_min_batch_bytes }` to `oceanfs-core/src/config.rs`
   - Add field to `AccelConfig`
   - Add `None` variant to `CompressionTier` enum
   - Pass to `AccelDispatcher::new()`

2. **Add per-bucket `compression` config to `BucketPolicy`** (C2):
   - Add `pub compression: CompressConfig` to `BucketPolicy`
   - Wire through S3 handler and admin API

3. **Implement node-ceiling resolution in `resolve_compressor()`** (H1):
   - Accept optional `node_ceiling: CompressionTier`
   - Cap effective tier: `min(requested, ceiling)` on capability ordering
   - Log `DEBUG` (not `WARN`) when bucket requests higher tier than ceiling — bucket is within rights; node constrains

4. **Implement multi-chunk nvCOMP batch compression** (C3):
   - Accept `NvcompConfig` in `NvcompCompressor::new()`
   - Use `config.batch_size` for `num_chunks`

### Short-Term (Next Sprint)

5. **Implement `NvcompBufferPool`** (H2) — pinned host memory pool for DMA transfers

6. **Add `accel_compression_fallback_total` metric** (H3) — `AtomicU64` on `AccelDispatcher`; ADR-mandated and low-effort

7. **Add `parse_compression_tier()`** (M2) — string-to-enum parser for all five ADR-0007 tier values

8. **Update feature documents** (M1) — sync DoD checklists with current state

### Medium-Term

9. **Add `CompressionTier::None` to `resolve_compression_tier_with_fallback()`** — handle `None` by returning early (no compression, no fallback)

10. **Wire compression into segment lifecycle** (L3) — integrate `Compressor` into `SegmentSealer` and read path

11. **Add Snappy/zstd nvCOMP FFI** (M5) or remove unimplemented variants

### Low Priority

12. Consolidate duplicate SAFETY comments (L1)

13. Improve igzip decompress retry (M3)

14. Update spec §9.6.2, §9.9.1, §9.9.2, §14.1, §14.2 per ADR-0007 migration path (L2)
