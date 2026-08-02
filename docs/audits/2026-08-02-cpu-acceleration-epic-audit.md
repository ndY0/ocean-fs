---
audit_date: 2026-08-02
scope: targeted
target_crates: oceanfs-accel
severity_counts:
  critical: 4
  high: 5
  medium: 5
  low: 4
---

# Audit Report: CPU Acceleration Backends Epic — Scope vs Implementation Gap Analysis

## Summary

The CPU Acceleration Backends epic (two features: **ISA-L x86 AVX-512 Encoder** and **ARM NEON/SVE Encoder**) has substantial implementation in `oceanfs-accel`. The ISA-L backend has a working FFI integration with comprehensive encode/decode roundtrip tests and cross-backend compatibility verification. The ARM backend has a working NEON encode path and a portable fallback for decode. However, **four critical gaps** exist: (1) the ISA-L backend lacks self-protecting AVX-512 runtime detection at the constructor level, (2) the ISA-L module lacks a `#[cfg(target_arch = "x86_64")]` compile-time gate, (3) the SVE/SVE2 encode kernels are entirely unimplemented (stubs only), and (4) the ARM decode path is never accelerated — it always falls back to portable Cauchy RS. Additionally, observability metrics, runtime fallback mechanisms, and the dedicated integration test files required by the feature specifications are missing. The epic is approximately **65% complete** for ISA-L and **40% complete** for ARM NEON/SVE.

---

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `oceanfs-accel/src/isal.rs:89-136` `IsalEncoder::new()` | ISA-L encoder constructor does **not** perform runtime AVX-512 detection. Spec requires `std::is_x86_feature_detected!("avx512f")` with `new() -> Option<Self>`. Current signature is `new(k, m) -> EcResult<Self>` which unconditionally constructs the backend. On a CPU without AVX-512, the ISA-L FFI call (`ec_init_tables`) will segfault because it tries to execute AVX-512 instructions. The dispatcher's `probe_tier1()` does check for AVX-512, but the backend itself is not self-protecting. | Add CPUID check at constructor entry. Change signature to `fn new(k, m) -> Option<Self>` and return `None` if AVX-512 is absent. Add `pub fn is_available() -> bool` as specified. |
| C2 | `oceanfs-accel/src/isal.rs` (missing) | `IsalEncoder` has no `is_available()` class method. Spec (§Interface) requires `pub fn is_available() -> bool`. The dispatcher does tier-level probing, but individual backends must be independently queriable per ADR-0006 §3. | Add `pub fn is_available() -> bool` that checks `std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("avx512bw")`. |
| C3 | `oceanfs-accel/src/isal.rs:1`, `oceanfs-accel/src/lib.rs:57-58` | ISA-L module is gated **only** on `#[cfg(feature = "isa-l")]` — missing the `#[cfg(target_arch = "x86_64")]` compile-time arch gate. The spec (§Scope) requires both: `#[cfg(all(target_arch = "x86_64", feature = "isa-l"))]`. Compiling with `--features isa-l --target aarch64-unknown-linux-gnu` would declare x86-specific `extern "C"` functions that fail to link. | Wrap the `mod isal` declaration in `lib.rs` with `#[cfg(all(target_arch = "x86_64", feature = "isa-l"))]`. Add matching `#[cfg(target_arch = "x86_64")]` gate in `isal.rs`. |
| C4 | `oceanfs-accel/src/arm_sve.rs` (missing functions) | **SVE2 and SVE encode kernels are unimplemented stubs.** The spec (§Data Flow) requires `encode_sve2()`, `encode_sve()`, `encode_neon()`, and `encode_portable()`. Only `encode_neon()` and the portable fallback are implemented. The SVE2 and SVE capability levels are detected at construction (`ArmSveLevel::Sve2`, `ArmSveLevel::Sve`) but the encode path in `ArmEncoder::encode()` only checks `self.level >= ArmSveLevel::Neon` and calls `neon_encode()` — there is no branch for SVE/SVE2-specific kernels. On Graviton4/SVE2 hardware, the encoder degrades to NEON. | Implement SVE and SVE2 GF(2^8) vectorized multiply kernels using `std::arch::aarch64` intrinsics (`svld1`, `svmla`, `svst1`). Add dispatch in `ArmEncoder::encode()`: `Sve2 => encode_sve2(...)`, `Sve => encode_sve(...)`. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `oceanfs-accel/src/arm_sve.rs:477-486` `ArmEncoder::decode()` | **ARM decode is never accelerated.** The `Decoder` impl for `ArmEncoder` unconditionally delegates to `self.fallback.decode()` (portable Cauchy RS from `oceanfs-ec`). The spec (§ARM Data Flow, §Interface) requires an `ArmDecoder` struct (or at minimum a SIMD-accelerated decode path) that dispatches to SVE/NEON/portable decode kernels. All ARM decode operations currently run at portable speed regardless of SIMD capability. | Either (a) implement SIMD-accelerated decode kernels (SVE2, SVE, NEON) that use the same split-table approach but with a reconstructed decode matrix, or (b) as a lower effort approach, add a NEON-accelerated decode path that mirrors `neon_encode` with an inverted coefficient matrix. |
| H2 | `oceanfs-accel/src/isal.rs:89` | No separate `IsalDecoder` struct exists — `IsalEncoder` implements both `Encoder` and `Decoder`. The spec (§Interface) expects `pub struct IsalDecoder` with separate `fn new(tables: &IsalTables) -> Option<Self>`. While functionally equivalent, this violates the specified API surface. | Either create a dedicated `IsalDecoder` struct (with `Encoder` on `IsalEncoder` and `Decoder` on `IsalDecoder`), or update the feature spec to reflect the combined design. |
| H3 | `oceanfs-accel/src/` (missing module) | **Zero observability metrics implemented.** The spec (§9.8.1) defines 10+ Prometheus metrics: `accel_tier_active`, `accel_encode_duration_seconds`, `accel_decode_duration_seconds`, `accel_bytes_processed_total`, `accel_fallback_total`, `accel_runtime_fallback_total`, `accel_gpu_utilization`, `accel_gpu_memory_bytes`, `accel_gpu_semaphore_wait_seconds`, `accel_compress_duration_seconds`, `accel_hash_duration_seconds`. None of these exist in the codebase. A grep for `accel_fallback_total` and `accel_encode_duration` returned zero results. | Add a `metrics.rs` module with atomic counters and histogram stubs. As a minimum viable implementation, add `accel_encode_duration_seconds` (histogram), `accel_bytes_processed_total` (counter), `accel_fallback_total` (counter labeled by from/to tier), and `accel_runtime_fallback_total` (counter). |
| H4 | `oceanfs-accel/src/dispatcher.rs` (missing) | **No EC tier fallback counter.** The spec (ADR-0006 §2) requires `accel_fallback_total` to be incremented on each fallback event so operators can detect misconfiguration. The dispatcher has a `compression_fallback_count` (line 106) but no equivalent for EC tier fallbacks. The `resolve_ec_tier()` function (line 536-576) emits WARN logs but does not increment any counter. | Add `ec_fallback_count: AtomicU64` to `AccelDispatcher`. Increment in `resolve_ec_tier()` whenever a fallback occurs. Expose via `pub fn ec_fallback_count(&self) -> u64`. |
| H5 | `oceanfs-accel/src/isal.rs:89-96` | Missing `pub(crate) struct IsalTables`. The spec (§Interface) requires a dedicated `IsalTables` struct wrapping precomputed encoding tables (`[u8; 32*k*m]`). The implementation uses raw `Vec<u8>` fields (`encode_tables: Vec<u8>`) and creates temporary tables in `encode()` via `vec![0u8; table_size]` on every call when `m` differs. This loses type safety and incurs repeated allocation. | Wrap the table buffer in a dedicated struct: `pub(crate) struct IsalTables { buf: Vec<u8>, k: u8, m: u8 }`. Add validation that k,m match when reusing. Cache per-(k,m) in a `HashMap` to avoid re-initialization. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `oceanfs-accel/tests/` (missing files) | **Missing integration test files.** The feature specs require `tests/isal_ec_roundtrip.rs` and `tests/arm_ec_roundtrip.rs` at the crate root. These files do not exist. Unit tests for both backends are embedded in their source modules (`#[cfg(test)] mod tests`), which is good for unit coverage but the spec also requires separate integration tests that exercise the public API. | Create `tests/isal_ec_roundtrip.rs` with cross-backend roundtrip tests (ISAL↔Cauchy RS) and AVX-512 detection tests. Create `tests/arm_ec_roundtrip.rs` with cross-kernel roundtrip tests (NEON↔portable). |
| M2 | `oceanfs-accel/src/isal.rs:173-186` `IsalEncoder::encode()` | **Encoding tables are cloned on every encode call when m matches.** Line 185 clones `self.encode_tables` every call: `self.encode_tables.clone()`. This clones a `Vec<u8>` of `32*k*m` bytes every encode. For k=16, m=8 that's 4 KB per call — minor but unnecessary. The spec says "Encode/decode table caching: precomputed tables reused across stripes." The table data is read-only; cloning is wasteful. | Pass `&self.encode_tables` (borrow) instead of cloning. The `ec_encode_data` FFI function takes `*const u8` — a borrow is sufficient. |
| M3 | `oceanfs-accel/Cargo.toml:11` | **Feature dependency mismatch.** The spec and ADR-0006 §6 specify `isa-l = ["dep:isal-rs"]` to depend on a Rust binding crate. The actual implementation uses `isa-l = []` with raw `extern "C"` FFI declarations. No `isal-rs` crate exists in dependencies. This is a design divergence from the spec rather than a bug, but it should be reconciled. | Either (a) add `isal-rs` as a dependency and wrap its safe API (preferred for safety), or (b) update the feature spec and ADR-0006 to reflect the raw FFI approach. |
| M4 | `oceanfs-accel/src/dispatcher.rs` (missing) | **No runtime fallback mechanism.** The spec (ADR-0006 §9.7.1) requires that if an active backend fails at runtime (ISA-L FFI error, GPU device lost), the dispatcher marks it unavailable, falls back to the next tier, increments `accel_runtime_fallback_total`, and retries. The dispatcher caches backends at startup with no mechanism to detect runtime failures or switch backends dynamically. | Add an `Arc<AtomicBool>` or `AtomicU8` flag per backend. On encode/decode error, check if the backend should be marked unavailable. Wrap `encoder.encode()` in a retry loop that falls back through tiers. |
| M5 | `oceanfs-accel/src/arm_sve.rs` (missing) | **No ARM decode kernel.** The portable Cauchy RS decoder from `oceanfs-ec` is used for all ARM decode operations. The spec (§ARM Data Flow) shows `ArmDecoder::decode` dispatching to SVE→NEON→portable decode paths with a reconstructed decode matrix. The NEON encode path has a high-quality split-table implementation; building the corresponding decode path would complete the ARM backend. | Implement `neon_decode()` that mirrors `neon_encode()` but with an inverted coefficient matrix. At minimum, add NEON-accelerated data recovery when the decode matrix is precomputed. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `oceanfs-accel/src/arm_sve.rs:42` | `#![allow(dead_code)]` at module level. This attribute suppresses warnings for SVE structs/functions that are defined but unused (since SVE kernel stubs are incomplete). A properly implemented module should not need this blanket suppression. | Remove `#![allow(dead_code)]` once SVE/SVE2 kernels are implemented. Use targeted `#[allow(dead_code)]` on specific items only if needed. |
| L2 | `oceanfs-accel/src/lib.rs:30-34` | Lib.rs doc comment mentions "GPU cooldown mechanism" and "mark GPU unavailable for 60s" but this mechanism is not implemented anywhere in the accel crate. The CudaBackend has `is_available()` but no cooldown timer. | This is Phase 8 scope (GPU acceleration). Remove the doc reference or move it to the CUDA module docs with a "TODO: Phase 8" marker. |
| M3 | `oceanfs-accel/src/isal.rs:36-66` | The `ec_init_tables` FFI declaration has no return type (`fn ec_init_tables(k: i32, rows: i32, a: *const u8, gftbls: *mut u8)`). The spec mentions "Check return code" but real ISA-L's `ec_init_tables` is actually `void` — there's nothing to check. This is correct but the spec is misleading. | No code change needed. Update the feature spec to note that `ec_init_tables` has no return value. |
| L4 | `oceanfs-accel/src/arm_sve.rs:624-691` | ARM encode/decode roundtrip tests only exercise the portable fallback path because `ArmEncoder::decode()` unconditionally delegates to `self.fallback.decode()`. The NEON encode path is exercised but the decode verification always uses portable Cauchy RS, not a NEON-accelerated decode path. | Once H1 (NEON decode) is implemented, add tests that verify NEON encode → NEON decode roundtrip and cross-kernel compatibility (NEON encode → SVE decode, etc.). |

---

## Coupling Hotspots

| Symbol | Crate | In-Degree | Risk |
|---|---|---|---|
| `CauchyEncoder::new` | oceanfs-ec | ~400+ | Medium — heavily depended on, used as fallback by all backends |
| `AccelDispatcher::new` | oceanfs-accel | N/A (composition root) | Low — single construction at startup |
| `IsalEncoder::new` | oceanfs-accel | low (called from dispatcher) | Low — well-isolated behind feature gate |
| `ArmEncoder::new` | oceanfs-accel | low (called from dispatcher) | Low — well-isolated behind feature gate |

The `oceanfs-accel` crate is well-isolated. The only coupling concern is the reliance on `oceanfs-ec::CauchyEncoder` as the universal fallback, but this is by design (ADR-0006 §3).

---

## Dependency Graph

No DAG violations. `oceanfs-accel` depends on `oceanfs-core` and `oceanfs-ec` — both upstream of it in the architecture. No circular dependencies detected.

---

## Guideline Violations

| Guideline | Location | Violation |
|---|---|---|
| ADR-0006 §1 (Startup Probing, Cached for Lifetime) | `oceanfs-accel/src/isal.rs` | `IsalEncoder::new()` does not perform its own runtime AVX-512 probe; relies on dispatcher to pre-filter. Backend should be self-protecting. |
| ADR-0006 §2 (Fallback Chain) | `oceanfs-accel/src/dispatcher.rs` | No `accel_fallback_total` counter for EC tier fallback events. |
| ADR-0006 §3 (Trait-Based Pluggability) | `oceanfs-accel/src/isal.rs` | No separate `IsalDecoder` struct; encoder and decoder traits are on the same struct. |
| ADR-0006 §6 (Feature-Gated Compilation) | `oceanfs-accel/src/isal.rs`, `lib.rs` | Missing `#[cfg(target_arch = "x86_64")]` gate on ISA-L module. |
| Coding §5.1 (All `pub` items have doc comments with examples) | `oceanfs-accel/src/arm_sve.rs` | `ArmEncoder` doc comment has `//!` module doc but the struct's doc comment references examples in `ignore` blocks. Minor. |
| Coding §4.5 (Unsafe code requires a safety test) | `oceanfs-accel/src/arm_sve.rs:227-246` | NEON intrinsic calls have `// SAFETY:` comments but the safety test (unaligned input) described in the spec is not present. Acceptable since the portable fallback handles all edge cases. |
| Performance §4.3 (Feature-gated SIMD) | `oceanfs-accel/src/arm_sve.rs` | SVE/SVE2 code paths are feature-gated but not implemented. The feature gate exists but the code behind it is incomplete. |

---

## ADR Compliance

| ADR | Status | Notes |
|---|---|---|
| ADR-0006 §1 (Startup Hardware Probing) | ⚠️ Partial | Dispatcher probes correctly for ARM (SVE2→SVE→NEON→Portable) and x86 (AVX-512 check). ISA-L backend itself does not self-probe. |
| ADR-0006 §2 (Fallback Chain with Warnings) | ⚠️ Partial | Fallback chain implemented in dispatcher. WARN logging present. Missing: `accel_fallback_total` counter for EC tier. No runtime fallback mechanism. |
| ADR-0006 §3 (Trait-Based Pluggability) | ⚠️ Partial | All backends implement `Encoder`/`Decoder`. Missing: separate `IsalDecoder` struct per spec. ARM `Decoder` impl always uses portable fallback. |
| ADR-0006 §4 (GPU Concurrency Model) | ✅ N/A | Not in scope for CPU acceleration epic. |
| ADR-0006 §5 (Non-EC Acceleration) | ✅ Compliant | Compression subsystem (zstd, igzip, nvCOMP) is implemented separately. |
| ADR-0006 §6 (Feature-Gated Compilation) | ⚠️ Partial | `isa-l` and `arm-sve` features exist. ISA-L missing arch gate. SVE/SVE2 kernels missing. |
| ADR-0006 §7 (Per-Bucket Tier Selection) | ✅ Compliant | `resolve_encoder_for_tier()` and `resolve_decoder_for_tier()` support per-bucket overrides with fallback. |

---

## Test Coverage

| Component | Public Symbols | Unit Tests | Integration Tests | Coverage Assessment |
|---|---|---|---|---|
| `IsalEncoder` | 1 struct (Encoder+Decoder) | 15+ tests (encode, decode, roundtrip, GF arithmetic, matrix inversion, cross-backend) | No dedicated integration test file | **Good unit coverage.** Missing: integration tests at `tests/isal_ec_roundtrip.rs`. |
| `ArmEncoder` | 1 struct (Encoder+Decoder) | 10 tests (construction, GF arithmetic, table correctness, encode/decode roundtrip) | No dedicated integration test file | **Adequate unit coverage on portable path.** Missing: NEON-specific tests, integration tests at `tests/arm_ec_roundtrip.rs`. |
| `AccelDispatcher` | 1 struct | 20+ tests (tier parsing, resolution, fallback, encode/decode, compressor dispatch) | 4 integration tests in `tests/dispatcher_tiers.rs` + 5 in `tests/accel_dispatch.rs` | **Good coverage.** Tests cover all tier combinations and fallback scenarios. |
| `tier0::CpuEncoder` | 1 struct (internal) | 3 tests (availability, capabilities, roundtrip) | — | **Adequate.** |
| `AccelError` | 1 enum | 5 tests (display formatting, type assertions) | — | **Adequate.** |

**Overall test coverage:** The unit test suite is solid for the implemented paths. The main gaps are (a) no NEON-specific integration tests, (b) no separate integration test files as specified, and (c) no tests for the SVE/SVE2 paths since those kernels don't exist.

---

## What IS Implemented (Completeness Assessment)

### ISA-L x86 AVX-512 Encoder (~65%)

| Requirement | Status | Notes |
|---|---|---|
| `IsalEncoder` struct implementing `Encoder` trait | ✅ Done | Full FFI-based Cauchy RS encode via `ec_encode_data` |
| `IsalEncoder` implementing `Decoder` trait | ✅ Done | Gauss-Jordan matrix inversion + ISA-L SIMD recovery |
| `IsalDecoder` separate struct | ❌ Missing | Combined into `IsalEncoder` |
| FFI binding to ISA-L C library | ✅ Done | Direct `extern "C"` declarations for `ec_init_tables` + `ec_encode_data` |
| Runtime CPUID detection | ❌ Missing | Only in dispatcher, not in backend constructor |
| `#[cfg(feature = "isa-l")]` gate | ✅ Done | Module gate exists |
| `#[cfg(target_arch = "x86_64")]` gate | ❌ Missing | Not present on module declaration or in isal.rs |
| `IsalTables` struct | ❌ Missing | Uses raw `Vec<u8>` instead |
| Table caching across stripes | ⚠️ Partial | Tables cloned every call; no per-(k,m) cache |
| `new() -> Option<Self>` / `is_available()` | ❌ Missing | `new()` always succeeds |
| SAFETY comments on all unsafe | ✅ Done | Every unsafe block has documentation |
| Roundtrip tests (ISAL encode↔decode) | ✅ Done | k=4,m=2 through k=16,m=8 with various erasure patterns |
| Cross-backend tests (ISAL↔Cauchy RS) | ✅ Done | ISAL encode + Cauchy decode, and vice versa |
| Build without features compiles | ✅ Done | Verified |

### ARM NEON/SVE Encoder (~40%)

| Requirement | Status | Notes |
|---|---|---|
| `ArmEncoder` struct implementing `Encoder` trait | ✅ Done | NEON + portable encode paths |
| `ArmEncoder` implementing `Decoder` trait | ⚠️ Partial | Always delegates to portable fallback; no SIMD decode |
| `ArmDecoder` separate struct | ❌ Missing | Combined into `ArmEncoder` |
| NEON GF(2^8) encode kernel | ✅ Done | Split-table lookups with `vtbl1_u8`/`vqtbl1q_u8` |
| SVE encode kernel | ❌ Missing | Detected but not implemented |
| SVE2 encode kernel | ❌ Missing | Detected but not implemented |
| Portable fallback | ✅ Done | Delegates to own log/exp table implementation |
| Runtime SIMD probing | ✅ Done | SVE2→SVE→NEON→Portable, cached at construction |
| `#[cfg(feature = "arm-sve")]` gate | ✅ Done | Module and arch gate correct |
| `ArmSveLevel` enum | ✅ Done | `Portable`, `Neon`, `Sve`, `Sve2` |
| `EncodeTables` precomputation | ✅ Done | Split-table construction for Cauchy matrix |
| SAFETY comments on NEON intrinsics | ✅ Done | Every intrinsic call documented |
| Roundtrip tests | ⚠️ Partial | Only portable decode tested |
| Cross-kernel tests | ❌ Missing | No SVE-specific tests |

---

## Recommendations

### Priority 1 — Fix Critical Gaps (Must Address Before Epic Is "Done")

1. **Add AVX-512 runtime detection to `IsalEncoder::new()`** (C1, C2): Change constructor to `fn new(k, m) -> Option<Self>`, check `is_x86_feature_detected!("avx512f")`, return `None` if unavailable. Add `is_available()` class method.

2. **Add `#[cfg(target_arch = "x86_64")]` gate** (C3): Wrap the ISA-L module in both `lib.rs` and `isal.rs` with the arch gate to prevent linking failures on non-x86 targets.

3. **Implement SVE2 and SVE encode kernels** (C4): The SVE2/SVE capability detection is already done. The missing piece is the actual kernel implementations using `std::arch::aarch64` SVE intrinsics. If full SVE implementation is too large for this epic, at minimum add a NEON-accelerated decode path (H1) so the ARM backend is fully functional at NEON speed.

### Priority 2 — Address High-Impact Gaps

4. **Implement ARM SIMD decode** (H1): Add a NEON-accelerated decode kernel. The split-table approach used for encode can be applied to decode by inverting the coefficient matrix.

5. **Add observability metrics** (H3): At minimum, implement `accel_fallback_total`, `accel_encode_duration_seconds`, and `accel_bytes_processed_total`. These are critical for operators to detect misconfiguration and monitor performance.

6. **Add EC tier fallback counter** (H4): Add `ec_fallback_count: AtomicU64` to the dispatcher.

### Priority 3 — Polish and Completeness

7. **Create dedicated integration test files** (M1): `tests/isal_ec_roundtrip.rs` and `tests/arm_ec_roundtrip.rs`.

8. **Fix table cloning in `IsalEncoder::encode()`** (M2): Pass `&self.encode_tables` instead of cloning.

9. **Reconcile feature dependency** (M3): Either add `isal-rs` as a dependency or update spec.

10. **Add runtime fallback mechanism** (M4): When an active backend returns a recoverable error, fall back through tiers transparently.

11. **Implement ARM decode kernel** (M5, H1): NEON-accelerated decode using the same split-table technique.

---

## Appendix: File Inventory

| File | Lines | Purpose | Status |
|---|---|---|---|
| `oceanfs-accel/src/isal.rs` | 852 | ISA-L x86 AVX-512 encoder + decoder | ✅ Implemented (with gaps C1-C3) |
| `oceanfs-accel/src/arm_sve.rs` | 691 | ARM NEON/SVE encoder + decoder | ⚠️ Partial (NEON encode ✅, SVE/SVE2 ❌, decode ❌) |
| `oceanfs-accel/src/dispatcher.rs` | 1150 | Tier resolution, fallback, per-bucket override | ✅ Implemented |
| `oceanfs-accel/src/tier0.rs` | 128 | CPU SIMD fallback (wraps CauchyEncoder) | ✅ Implemented |
| `oceanfs-accel/src/error.rs` | 121 | AccelError enum | ✅ Implemented |
| `oceanfs-accel/src/compressor.rs` | 230 | Compressor trait + ZstdCompressor | ✅ Implemented (separate epic) |
| `oceanfs-accel/src/igzip.rs` | 656 | ISA-L igzip compression | ✅ Implemented (separate epic) |
| `oceanfs-accel/src/cuda/` | — | CUDA backend + nvCOMP | ✅ Implemented (separate epic, Phase 8) |
| `oceanfs-accel/tests/dispatcher_tiers.rs` | 84 | Integration: tier selection | ✅ Implemented |
| `oceanfs-accel/tests/accel_dispatch.rs` | 123 | Integration: cross-backend dispatch | ✅ Implemented |
| `oceanfs-accel/build.rs` | 177 | ISA-L pkg-config, CUDA probing | ✅ Implemented |
