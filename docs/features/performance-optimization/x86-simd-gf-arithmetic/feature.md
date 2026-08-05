---
feature: "x86 SIMD GF(2^8) Arithmetic"
epic: "performance-optimization"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: gap-closure-epic-6
    reason: "Encoder trait return type changed from Vec<Vec<u8>> to Vec<Bytes> (QW-7 / codebase hygiene) — SIMD implementation should target the new signature"
adr:
  - 0006-hardware-acceleration-tier-model
perf:
  - "5.3 Feature-gated SIMD compilation"
  - "10.6 Conditional platform-specific code paths"
  - "11.4 Criterion benchmarks for hot-path functions"
  - "12.1 SAFETY comments on every unsafe block"
created: 2026-08-05
updated: 2026-08-05
---

# x86 SIMD GF(2^8) Arithmetic

## Summary

Port the ARM NEON split-table SIMD approach for GF(2^8) multiplication to
x86, implementing SSE4.1, AVX2, and AVX-512 vectorized paths with runtime
CPU feature detection. The portable log/exp table lookup path (Tier 0) runs
at ~42ms per 4MB segment encode. The ARM NEON backend achieves ~2ms — a
~20× improvement. This feature brings the same SIMD speedup to x86, covering
the dominant deployment platform. Code lives in `oceanfs-ec/src/gf.rs`
(Tier 0 portable + SIMD) and `oceanfs-accel/src/tier0.rs` (x86 dispatch).
Follows the same split-table algorithmic pattern as the existing
`oceanfs-accel/src/arm_sve.rs` backend.

## Scope

### In Scope

- **SSE4.1 vectorized table lookup path.** Uses `_mm_shuffle_epi8` (PSHUFB)
  for 16-way parallel table lookups, achieving ~1.5× speedup over portable
  log/exp lookup. Targets baseline x86-64 (SSE4.1 is universal on all Intel
  since Nehalem 2008 and AMD since Bulldozer 2013). The split-table approach
  splits GF_LOG/GF_EXP into 4-bit nibble tables for PSHUFB indexing.

- **AVX2 PCLMULQDQ carry-less multiply path.** Uses `_mm_clmulepi64_si128`
  for 16-element GF(2^8) multiply without table lookups. Processes 16 bytes
  per instruction via carry-less multiplication + reduction modulo the
  primitive polynomial. Achieves ~2× speedup over portable. Targets Intel
  Haswell+ (2013) and AMD Excavator+ (2015).

- **AVX-512 VPCLMULQDQ path.** Uses `_mm512_clmulepi64_epi128` (VPCLMULQDQ)
  for 64-element GF(2^8) multiply in one instruction. Achieves ~4× speedup
  over portable. Targets Intel Skylake-X+ (2017), Ice Lake+, and AMD Zen 4+
  (2022). Falls back to AVX2 when AVX-512 not available.

- **Runtime CPU feature detection.** At first GF operation, run `is_x86_feature_detected!`
  (from `std::arch`) for `avx512f`+`vpclmulqdq`, `avx2`+`pclmulqdq`, and
  `sse4.1`. Cache the detected level in a `static AtomicU8` for zero-cost
  dispatch on subsequent calls. Follows the same pattern as BLAKE3's runtime
  detection and ADR-0006 §1.

- **Portable log/exp table fallback preserved.** When no SIMD features are
  detected (or when compiling for a non-x86 target), the existing
  `LOG_TABLE`/`EXP_TABLE` lookup path is the fallback.

- **Unsafe block discipline.** All `unsafe` blocks must have `// SAFETY:`
  comments per guideline §12.1, citing the invariant (valid alignment,
  pointer provenance, feature detection gating).

- **Criterion benchmarks.** Extend `benches/ec_benchmark.rs` to compare:
  - Portable (log/exp table)
  - SSE4.1 (PSHUFB)
  - AVX2 (PCLMULQDQ)
  - AVX-512 (VPCLMULQDQ)
  - ARM NEON (aarch64 cross-reference, if available)
  For segment sizes: 64KB, 1MB, 4MB, 64MB. Report GF ops/sec and encode time.

### Out of Scope (for this feature)

- **ARM NEON/SVE implementation** — already exists in `oceanfs-accel/src/arm_sve.rs`
- **ISA-L FFI path** — already exists in `oceanfs-accel/src/isal.rs`
- **GPU/CUDA path** — separate epic (Phase 8)
- **`Encoder::encode()` return type change** — handled by gap-closure Epic 6
  (codebase-hygiene) / Feature 1 QW-7
- **GF(2^8) table deduplication across crates** (accel H5) — handled by
  gap-closure Epic 6 (codebase-hygiene)
- **Per-decode matrix inversion caching** (accel H3) — separate optimization

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-ec` | New module `src/gf/simd_x86.rs` with `#[cfg(target_arch = "x86_64")]`. Modify `src/gf/mod.rs` to expose `gf_mul_simd` function and runtime dispatch infrastructure. |
| `oceanfs-accel` | Modify `src/tier0.rs` to call the new SIMD GF path from `oceanfs-ec` instead of scalar `gf_mul`. No new module needed (delegates to `oceanfs-ec`). |
| `benches/` | Extend `ec_benchmark.rs` with SIMD vs portable comparison benchmarks. |

## Interface (Public API)

- `pub fn gf_mul_simd(a: &[u8], b: &[u8], dst: &mut [u8])` in `oceanfs-ec::gf`
  — Multiply two byte slices element-wise using the fastest available SIMD
  path. Detects CPU features on first call. Falls back to scalar `gf_mul` loop.
- `pub fn gf_mul_simd_unchecked(a: &[u8], b: &[u8], dst: &mut [u8])` — same
  as above, but assumes caller has verified `dst.len() == a.len() == b.len()`
  and that the slices are SIMD-aligned (32 bytes for AVX2, 64 bytes for AVX-512).
- `pub enum GfSimdLevel { Portable, Sse41, Avx2, Avx512 }` — detected SIMD level
- `impl GfSimdLevel { pub fn detect() -> Self; }` — runtime feature detection
- Internal-only: `static GF_SIMD_LEVEL: AtomicU8` — cached detection result

## Data Flow

```
Segment sealed → ParallelEncoder::encode(segment_data, plan)
  │
  ├─ ... (stripe partitioning, rayon par_iter) ...
  │
  └─ Per stripe: AccelDispatcher::encode(stripe_data, m)
       │
       └─ Tier 0 (CpuEncoder): encode_cauchy(data, parity, k, m)
            │
            └─ Triple-nested loop: rows(m) × bytes(shard_size) × cols(k)
                 │
                 └─ gf_mul(a, b):  // ← THIS IS THE HOT PATH
                      │
                      ├─ [First call] GfSimdLevel::detect()
                      │     ├─ is_x86_feature_detected!("avx512f") && "vpclmulqdq" → Avx512
                      │     ├─ is_x86_feature_detected!("avx2") && "pclmulqdq" → Avx2
                      │     ├─ is_x86_feature_detected!("sse4.1") → Sse41
                      │     └─ else → Portable
                      │     Cache result in GF_SIMD_LEVEL with Ordering::Release
                      │
                      └─ [Every call] match GF_SIMD_LEVEL.load(Ordering::Acquire):
                           ├─ Avx512 → gf_mul_avx512(a, b)  // 64 elements/instruction
                           ├─ Avx2   → gf_mul_avx2(a, b)    // 16 elements/instruction
                           ├─ Sse41  → gf_mul_sse41(a, b)   // 16 elements/PSHUFB (split-table)
                           └─ Portable → LOG[a] + LOG[b] → EXP[sum]  // existing fallback
```

**Expected performance (single stripe, 64KB shard, k=4):**

| Path | GF ops | Time (est.) | vs Portable |
|---|---|---|---|
| Portable (log/exp) | 524,288 | ~2.6 ms | 1.0× |
| SSE4.1 (split-table PSHUFB) | 524,288 | ~1.7 ms | 1.5× |
| AVX2 (PCLMULQDQ) | 524,288 | ~1.3 ms | 2.0× |
| AVX-512 (VPCLMULQDQ) | 524,288 | ~0.65 ms | 4.0× |

**Expected performance (4MB segment, 16 stripes, k=4, m=2):**

| Path | Total encode time (est.) | vs Portable |
|---|---|---|
| Portable | ~42 ms | 1.0× |
| SSE4.1 | ~27 ms | 1.5× |
| AVX2 | ~21 ms | 2.0× |
| AVX-512 | ~10.5 ms | 4.0× |

## Definition of Done

- [ ] **SSE4.1 path:** `#[cfg(target_feature = "sse4.1")]` function `gf_mul_sse41`
  implemented in `oceanfs-ec/src/gf/simd_x86.rs`. Uses PSHUFB split-table 4-bit
  nibble lookup. Unit test verifies correctness against portable `gf_mul`.
- [ ] **AVX2 path:** `#[cfg(target_feature = "avx2")]` function `gf_mul_avx2`
  implemented. Uses `_mm_clmulepi64_si128` carry-less multiply + polynomial
  reduction. Unit test verifies correctness.
- [ ] **AVX-512 path:** `#[cfg(target_feature = "avx512f")]` function `gf_mul_avx512`
  implemented. Uses `_mm512_clmulepi64_epi128`. Unit test verifies correctness.
- [ ] **Runtime detection:** `GfSimdLevel::detect()` implemented with
  `is_x86_feature_detected!`. Result cached in `static AtomicU8` with
  `Ordering::Release`/`Acquire`. First-call detection cost is ~tens of
  nanoseconds (one-time branch).
- [ ] **Portable fallback:** Existing `gf_mul` loop preserved as the fallback
  for `GfSimdLevel::Portable` and non-x86 targets.
- [ ] **SAFETY comments:** Every `unsafe` block in SIMD code has a `// SAFETY:`
  comment citing: (a) feature detection has confirmed the instruction is
  available, (b) pointer alignment meets requirements (16/32/64 bytes),
  (c) buffer lengths are validated.
- [ ] **oceanfs-accel integration:** `tier0.rs` calls `gf_mul_simd` instead of
  `gf_mul` when encoding on x86. Portable path unchanged for non-x86.
- [ ] **Code:** `cargo build --all-targets` succeeds on x86_64. Cross-compilation
  to aarch64 succeeds (portable fallback only — SIMD code `#[cfg]`-gated).
- [ ] **Tests:** All 40+ existing tests in `oceanfs-ec` pass. New tests added:
  `gf_simd_roundtrip` (encode+decode produce identity), `gf_simd_crosscheck`
  (all SIMD levels agree with portable), `gf_simd_edge_cases` (empty input,
  size not multiple of SIMD width).
- [ ] **Docs:** `gf_mul_simd` and `GfSimdLevel` have `# Examples`. Module-level
  doc in `simd_x86.rs` describes the split-table algorithm.
- [ ] **ADR:** ADR-0006 §1 (startup probing) is followed — SIMD detection is
  one-time, cached, not per-operation. Fallback chain (Avx512 → Avx2 → Sse41
  → Portable) matches ADR §2.
- [ ] **Perf:** `cargo bench` in `benches/ec_benchmark.rs` shows 1.5-4.0×
  improvement over portable baseline on supported hardware.
- [ ] **Integration:** `ParallelEncoder` end-to-end test produces identical
  parity output using SIMD vs portable paths. Round-trip encode+decode
  works at all SIMD levels.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and `ignore`-tagged
> doc examples are non-blocking (see `guidelines/coding.md` §9.2.1).
