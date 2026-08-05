---
audit_date: 2026-08-05
scope: targeted
target_crates: oceanfs-ec, oceanfs-accel, oceanfs-hash
severity_counts:
  critical: 1
  high: 1
  medium: 3
  low: 4
---

# Audit Report: Erasure Coding, Hardware Acceleration, and Hash Subsystem Implementation Completeness

## Summary

The erasure coding (EC), hardware acceleration, and hashing subsystems are
**substantially complete and functional**. The Cauchy Reed-Solomon codec with
GF(2^8) arithmetic, stripe layout, rayon-based parallel encode/decode, all
three acceleration tiers (CPU SIMD, ISA-L, CUDA), the `Compressor` trait with
zstd/igzip/nvCOMP backends, and the BLAKE3 hashing crate all compile, pass
their test suites, and implement the traits specified in the feature docs and
ADRs.

The most significant finding is **dead stub code in `oceanfs-ec/src/isal.rs`**
that exports a bare `IsalEncoder` struct with no trait implementations. The
real ISA-L backend lives in `oceanfs-accel`, making the `oceanfs-ec` stub
confusing and potentially misleading to consumers. The second most important
gap is the **incomplete GPU cooldown/recovery** mechanism, which marks a GPU
as unavailable but has no timer-based auto-recovery path.

---

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `oceanfs-ec/src/isal.rs` lines 14-30 | `IsalEncoder` is a bare struct stub with a `TODO` comment, no trait impls, no actual ISA-L FFI. The `pub use isal::isal::IsalEncoder` export in `lib.rs` line 39 exposes a useless type. The real ISA-L backend is in `oceanfs-accel/src/isal.rs`. Consumers that `use oceanfs_ec::IsalEncoder` will get the dead stub, not the working implementation. | Remove the `isal.rs` module from `oceanfs-ec` entirely, or replace it with a `pub use oceanfs_accel::IsalEncoder` re-export. The EC crate should either own the ISA-L backend or delegate it. Current split is misleading. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `oceanfs-accel/src/cuda/mod.rs` line 239 | GPU cooldown/recovery is incomplete. `mark_unavailable()` sets an `AtomicBool` and logs an error, but there is no timer-based recovery path (spec §9.5.6 requires 60-second cooldown + re-probe). `try_recover_ec_backend()` at dispatcher.rs:354 just clears the flag without any delay or re-verification. | Implement a 60-second cooldown with automatic re-probe. Use `tokio::time::sleep` in a background task, or re-probe on the next encode attempt if more than 60 seconds have elapsed since `mark_unavailable`. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `oceanfs-accel/src/cuda/nvcomp.rs` | `NvcompBufferPool` (pinned/zero-copy host memory pool for DMA transfers) is not implemented. Feature doc marks this as H2 deferred. Without pinned memory, GPU compression pays double-copy penalty (host → bounce → device). Perf rule 1.2 requires arena/buffer pool for segment data. | Implement a pinned memory pool using `cudaHostAlloc` via cudarc or raw FFI. Low priority for correctness but important for GPU compression throughput. |
| M2 | `oceanfs-accel/src/cuda/nvcomp.rs` lines 234-250 | nvCOMP only supports LZ4 codec. The `NvcompCodec` enum has `Lz4`, `Snappy`, and `Zstd` variants, but only LZ4 FFI bindings (`nvcompBatchedLZ4CompressGetTempSize`, etc.) are defined. Snappy and zstd code paths are not implemented. | Add FFI bindings for `nvcompBatchedSnappy*` and `nvcompBatchedZstd*` functions, or document LZ4-only as the initial release scope with remaining codecs planned for later. |
| M3 | `oceanfs-ec/src/gf.rs`, `oceanfs-accel/src/arm_sve.rs`, `oceanfs-accel/src/cuda/mod.rs` | GF(2^8) log/exp tables (256 + 512 bytes) are duplicated across three modules. In oceanfs-ec they are computed via `const fn`, but in arm_sve.rs and cuda/mod.rs they are baked-in `static` arrays (~2 KB each). This increases binary size by ~4 KB and creates a maintenance burden if the primitive polynomial ever changes. | Share a single GF(2^8) table source across all three modules. Options: (a) move tables to `oceanfs-core`, (b) use `lazy_static`/`once_cell` with a single definition, (c) accept the duplication as intentional code isolation (the tables are small and unchanging). |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `oceanfs-accel/src/cuda/nvcomp.rs` line 250 | `num_chunks` is hardcoded to 1 in `NvcompCompressor::compress()`. The nvCOMP batched API is designed for multi-buffer batches but the `Compressor` trait operates on a single `&[u8]`. The `config.batch_size` field is unused for compression batching. | Either remove the misleading `batch_size` config field, or implement true batch compression by buffering multiple `compress()` calls before dispatching to nvCOMP. |
| L2 | `oceanfs-accel/src/dispatcher.rs` line 809 | CUDA probing (`probe_cuda`) returns `true` unconditionally when the `cuda` feature is enabled, without actually checking `CudaDevice::new()`. The real probe happens in `CudaBackend::new()` which handles failure gracefully, but this means `Auto` tier on a system without a GPU will still attempt CUDA first (and fall back). Minor startup time waste (~ms). | Call `CudaDevice::new(0).is_ok()` in `probe_cuda` for a real probe, or document the deferred-probe pattern explicitly. |
| L3 | `oceanfs-accel/src/arm_sve.rs` line 56 | `ArmSveLevel` enum is missing `#[non_exhaustive]` annotation. Per coding.md §1.5, all public enums with planned future variants should be non-exhaustive. Currently it's only `pub` within the crate but exported via the facade. | Add `#[non_exhaustive]` to `ArmSveLevel`. |
| L4 | `oceanfs-hash/Cargo.toml` | `oceanfs-hash` depends on `bytes` but does not use it. The only usage is `HashOutput` which is a `[u8; 32]` wrapper with no `Bytes` usage. The `Cargo.toml` lists `bytes.workspace = true`. | Remove the unused `bytes` dependency from `oceanfs-hash`. |

---

## Coupling Hotspots

| Symbol | Crate | In-Degree | Risk |
|---|---|---|---|
| `Encoder::encode` (trait method) | oceanfs-ec | 63 | **Expected** — central EC interface |
| `Decoder::decode` (trait method) | oceanfs-ec | 49 | **Expected** — central EC interface |
| `AccelDispatcher::new` | oceanfs-accel | 50 | **Expected** — startup entry point |
| `HashOutput::from_bytes` | oceanfs-hash | 68 | **Expected** — hash creation |
| `AccelConfig::default` | oceanfs-core | 51 | **Expected** — config default |

No unexpected coupling hotspots. The `Encoder`/`Decoder` traits are the correct central abstractions.
The dependency graph between the three crates respects the architecture DAG:
`oceanfs-core → oceanfs-ec → oceanfs-accel` and `oceanfs-core → oceanfs-hash`.

---

## Dependency Graph

No DAG violations detected. The dependency flow is:
```
oceanfs-core  (config types, errors, EncodingPlan)
    ├── oceanfs-ec  (traits, Cauchy RS, stripe layout)
    │       └── oceanfs-accel  (dispatcher, backends, compression)
    └── oceanfs-hash  (Hasher, BatchHasher, HashOutput)
```

Feature gates correctly isolate optional subsystems:
- `isa-l` feature in `oceanfs-accel` gates ISA-L encoder/decoder + igzip compression
- `cuda` feature gates CUDA backend + nvCOMP compression
- `arm-sve` feature gates ARM NEON/SVE backend
- `no_cuda_toolkit` and `no_nvcomp` cfgs (set by `build.rs`) allow graceful degradation when CUDA/nvCOMP SDKs are absent

---

## Guideline Violations

| Guideline | Location | Violation |
|---|---|---|
| coding.md §1.5 (`#[non_exhaustive]`) | `oceanfs-accel/src/arm_sve.rs:56` | `ArmSveLevel` enum is public but missing `#[non_exhaustive]` |
| coding.md §7.2 (SAFETY comments) | All crates | **Compliant** — Every `unsafe` block in isal.rs (6 blocks), arm_sve.rs (18 blocks), igzip.rs (9 blocks), cuda/mod.rs (5 blocks), and nvcomp.rs (21 blocks) has a `// SAFETY:` comment. Verified across all files. |
| perf rule 1.2 (arena/buffer pool) | `oceanfs-accel/src/cuda/nvcomp.rs` | `NvcompBufferPool` (pinned memory) is not implemented. Deferred per feature doc. |
| architecture.md §2.3 (feature-gated modules) | All crates | **Compliant** — All optional backends are behind `#[cfg(feature = "...")]` or combined arch+feature gates. Compilation without any features produces a fully functional Tier-0-only system. |

---

## ADR Compliance

| ADR | Status | Notes |
|---|---|---|
| ADR-0006 (Tier Model) | ✅ Compliant | Startup probing (§1), fallback chain with warnings (§2), trait-based pluggability via `Arc<dyn Encoder/Decoder>` (§3), GPU semaphore concurrency (§4), Non-EC acceleration with `Compressor` trait (§5), feature-gated compilation (§6), per-bucket tier selection (§7). All sections verified in `dispatcher.rs`. |
| ADR-0007 (Compression Governance) | ✅ Compliant | Node-level `compression` section in config, `cap_compression_tier()` method at dispatcher.rs:450, two-level governance with ceiling. |
| ADR-0008 (Hash Crate) | ✅ Compliant | `Hasher` and `BatchHasher` traits defined, `Blake3Hasher` and `Blake3BatchHasher` implemented, `HashOutput` in `oceanfs-hash` not `oceanfs-core`, `HashKey` stays in `oceanfs-core`. |

---

## Test Coverage

| Crate | Public Symbols | Tests (unit + integration) | Coverage Assessment |
|---|---|---|---|
| oceanfs-ec | 14 | ~40 unit + proptest + 2 integration | ✅ Strong — proptest round-trip, GF arithmetic properties, stripe layout edge cases |
| oceanfs-accel | 19 | ~78 (with `cuda` feature) | ✅ Strong — dispatcher tiers, fallback chains, cross-backend round-trips, compression dispatch, per-backend unit tests |
| oceanfs-hash | 5 | ~12 unit | ✅ Adequate — streaming, batch, hex display, idempotency |

### Test Gaps
- **No concurrent GPU stress tests.** The feature docs note that concurrent ops stress tests are missing for `CudaBackend`.
- **No 100 MB segment GPU test.** Large-segment GPU encode/decode is not exercised (requires substantial VRAM).
- **Cross-backend tests (ISA-L↔Cauchy, ARM↔Cauchy) exist** in integration test files and pass when applicable hardware/features are available.

---

## Subsystem Status Tables

### EC Core (`oceanfs-ec`)

| Feature | Status | Evidence |
|---|---|---|
| `Encoder` trait | ✅ Complete | traits.rs:19-27 |
| `Decoder` trait | ✅ Complete | traits.rs:47-61 |
| Cauchy RS encode | ✅ Complete | cauchy.rs:171-193, real GF(2^8) matrix ops |
| Cauchy RS decode | ✅ Complete | cauchy.rs:195-261, Gauss-Jordan inversion |
| GF(2^8) arithmetic | ✅ Complete | gf.rs — log/exp tables, gf_mul/gf_div/gf_inv |
| `StripeLayout` / `EncodingPlan` | ✅ Complete | stripe/layout.rs |
| `StripeBatch` (SoA) | ✅ Complete | stripe/batch.rs |
| `ParallelEncoder` (rayon) | ✅ Complete | stripe/parallel.rs — `into_par_iter()` |
| `ParallelDecoder` (rayon) | ✅ Complete | stripe/parallel.rs |
| `ShardData` (bytemuck) | ✅ Complete | shard.rs |
| ISA-L backend in oceanfs-ec | ❌ **Dead stub** | isal.rs — bare struct, TODO, no trait impls |

### CPU SIMD (Tier 0)

| Feature | Status | Evidence |
|---|---|---|
| `CpuEncoder` wrapper | ✅ Complete | tier0.rs — wraps `CauchyEncoder` |
| CPU capability detection | ✅ Complete | tier0.rs:53-81 — SSE4.1/AVX2/AVX-512/NEON |
| Portable fallback | ✅ Complete | Always available via `CauchyEncoder` |

### ISA-L (Tier 1)

| Feature | Status | Evidence |
|---|---|---|
| FFI declarations | ✅ Complete | isal.rs:36-66 — `ec_init_tables`, `ec_encode_data` |
| `IsalTables` precomputation | ✅ Complete | isal.rs:99-135 — Cauchy matrix + FFI call |
| `IsalEncoder` impl `Encoder` | ✅ Complete | isal.rs:213-290 |
| `IsalDecoder` impl `Decoder` | ✅ Complete | isal.rs:341-442 |
| AVX-512 probing | ✅ Complete | isal.rs:142-144 — `is_x86_feature_detected!` |
| Feature gate | ✅ Complete | `#[cfg(all(target_arch = "x86_64", feature = "isa-l"))]` |
| Build.rs libisal linking | ✅ Complete | build.rs:66-93 — pkg-config |
| SAFETY comments | ✅ Complete | 6 `unsafe` blocks, all documented |

### ARM NEON/SVE (Tier 1)

| Feature | Status | Evidence |
|---|---|---|
| NEON GF(2^8) kernel | ✅ Complete | arm_sve.rs:230-250 — `neon_gf_mul_16` with real intrinsics |
| NEON encode | ✅ Complete | arm_sve.rs:261-312 |
| SVE2 encode | ✅ Complete | arm_sve.rs:354-441 — real `svtbl_u8` intrinsics |
| SVE2 decode | ✅ Complete | arm_sve.rs:480-573 |
| SVE → NEON delegation | ✅ Complete | arm_sve.rs:454-462 |
| Portable fallback | ✅ Complete | arm_sve.rs:315-332 |
| `ArmEncoder` struct | ✅ Complete | arm_sve.rs:617 |
| `ArmDecoder` struct | ✅ Complete | arm_sve.rs (after 650) |
| SIMD level probing | ✅ Complete | Runtime SVE2→SVE→NEON detection |
| SAFETY comments | ✅ Complete | 18 `unsafe` blocks, all documented |

### GPU/CUDA (Tier 2)

| Feature | Status | Evidence |
|---|---|---|
| `CudaBackend` struct | ✅ Complete | cuda/mod.rs:179-188 |
| GPU probing | ✅ Complete | cuda/mod.rs:196-226 — `CudaDevice::new()` |
| PTX kernel loading | ✅ Complete | cuda/mod.rs:217-221 + kernels/gf256_encode.ptx |
| CUDA kernel (GF encode) | ✅ Complete | cuda/mod.rs:327-342 — `kernel.launch()` |
| Device memory management | ✅ Complete | cuda/mod.rs:304-308 — `alloc`, htod/dtoh copies |
| Semaphore concurrency | ✅ Complete | cuda/mod.rs:293-295 — `try_acquire` |
| `mark_unavailable` | ✅ Partial | Sets flag, no timer recovery |
| Decode → CPU fallback | ✅ Complete | cuda/mod.rs:377-392 |
| Build.rs + cfg flags | ✅ Complete | build.rs:96-154 |
| GF split-tables on GPU | ✅ Complete | cuda/mod.rs:57-104 |

### AccelDispatcher

| Feature | Status | Evidence |
|---|---|---|
| Startup probing | ✅ Complete | dispatcher.rs:139-277 |
| Tier resolution | ✅ Complete | dispatcher.rs:626-671 |
| Fallback chain | ✅ Complete | dispatcher.rs:632-669, with WARN logs |
| Per-bucket override | ✅ Complete | dispatcher.rs:388-406 |
| Per-tier cache | ✅ Complete | dispatcher.rs:199-229 |
| Runtime fallback | ✅ Partial | `mark_ec_backend_unhealthy` exists, `try_recover` is a no-op |
| `AccelMetrics` | ✅ Complete | metrics.rs — atomic counters |
| Node compression ceiling | ✅ Complete | dispatcher.rs:450-489 — ADR-0007 |
| `Compressor` resolution | ✅ Complete | dispatcher.rs:427-439 |

### Hash Subsystem (`oceanfs-hash`)

| Feature | Status | Evidence |
|---|---|---|
| `Hasher` trait | ✅ Complete | hasher.rs:27-36 |
| `Blake3Hasher` (streaming) | ✅ Complete | hasher.rs:54-90 |
| `BatchHasher` trait | ✅ Complete | batch.rs:27-32 |
| `Blake3BatchHasher` | ✅ Complete | batch.rs:53-74 |
| `HashOutput` | ✅ Complete | hash_output.rs:27-44 |
| Crate separation (ADR-0008) | ✅ Complete | `HashOutput` in oceanfs-hash, `HashKey` stays in oceanfs-core |

### Compression

| Feature | Status | Evidence |
|---|---|---|
| `Compressor` trait | ✅ Complete | compressor.rs:43-81 |
| `ZstdCompressor` | ✅ Complete | compressor.rs:102-153 |
| `IgzipCompressor` (ISA-L) | ✅ Complete | igzip.rs:224-373 — full FFI compress + decompress |
| `NvcompCompressor` (nvCOMP) | ✅ Partial | nvcomp.rs — LZ4 only, num_chunks=1 |
| `NvcompBufferPool` | ❌ Not implemented | Deferred (H2) |
| Node governance (ADR-0007) | ✅ Complete | dispatcher.rs:450-489 |

---

## Top 5 Blocking Gaps

1. **`oceanfs-ec/src/isal.rs` is dead stub code** (Critical) — The `IsalEncoder` type exported from `oceanfs-ec` is a bare struct with no trait implementations. Any consumer importing via `oceanfs_ec::IsalEncoder` gets the useless stub, not the real `oceanfs_accel::IsalEncoder`. Remove or re-export to avoid confusion.

2. **GPU cooldown/recovery is incomplete** (High) — `mark_unavailable()` sets an `AtomicBool` but there is no timer-based recovery. Per spec §9.5.6, a 60-second cooldown with automatic re-probe is required. `try_recover_ec_backend()` just clears the flag with no delay.

3. **nvCOMP `NvcompBufferPool` (pinned memory) not implemented** (Medium) — Without pinned/zero-copy host memory, GPU compression incurs a double-copy penalty. Perf rule 1.2 mandates arena/buffer pools for segment data.

4. **nvCOMP only implements LZ4 codec** (Medium) — The `NvcompCodec` enum has `Snappy` and `Zstd` variants but only LZ4 FFI bindings exist. Either implement the remaining codecs or scope LZ4-only as initial release.

5. **GF(2^8) log/exp tables duplicated across 3 modules** (Medium) — The same 768-byte table is defined in `oceanfs-ec/src/gf.rs` (const fn), `oceanfs-accel/src/arm_sve.rs` (static array), and `oceanfs-accel/src/cuda/mod.rs` (static array). Consolidate to avoid maintenance drift.

---

## Recommendations

1. **Immediately:** Remove or replace `oceanfs-ec/src/isal.rs` — it is misleading dead code.
2. **Before GPU production use:** Implement GPU cooldown with 60-second timer + re-probe.
3. **Before compression production use:** Implement `NvcompBufferPool` for pinned DMA memory.
4. **For completeness:** Add Snappy and zstd FFI bindings to nvCOMP, or document scope.
5. **For maintainability:** Consolidate GF(2^8) tables into a single shared module.
6. **Low effort:** Add `#[non_exhaustive]` to `ArmSveLevel`, remove unused `bytes` dep from `oceanfs-hash`.
