---
feature: "EC Encode Optimizations"
epic: "performance-optimization"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: performance-optimization
    feature: x86-simd-gf-arithmetic
    reason: "GFNI instructions build on the SIMD dispatch framework established by Feature 2. The runtime feature detection cache (GfSimdLevel) is extended, not replaced."
adr:
  - 0006-hardware-acceleration-tier-model
perf:
  - "2.1 Rayon parallel iterators for EC stripe encode/decode"
  - "10.6 Conditional platform-specific code paths"
  - "11.4 Criterion benchmarks for hot-path functions"
  - "12.1 SAFETY comments on every unsafe block"
created: 2026-08-05
updated: 2026-08-05
---

# EC Encode Optimizations

## Summary

Three erasure-coding-specific optimizations that build on the x86 SIMD
GF(2^8) arithmetic work (Feature 2) to further reduce encode latency.
**GFNI instructions** (Galois Field New Instructions, Intel Ice Lake
2021+ / AMD Zen 4 2022+) perform GF(2^8) multiplication in a single
instruction — eliminating the log/exp table lookup and the SIMD
shuffling entirely. This is the holy grail for EC performance: 1
instruction per byte instead of 4-6. **Cauchy matrix precomputation**
eliminates runtime matrix construction for common (k,m) pairs by
storing the entire encode matrix as `const` arrays at compile time.
**Streaming EC encode** eliminates the seal-time latency spike by
encoding each stripe row as soon as its k data shard bytes are written
to the segment buffer, overlapping encode with write — seal becomes a
no-op. Combined, these three optimizations target the entire EC encode
lifecycle: the GF operation (GFNI), the setup overhead (const matrices),
and the scheduling of encode work (streaming). Code lives in
`oceanfs-ec/src/gf/` (GFNI path), `oceanfs-ec/src/matrix.rs` (const
matrices), and `oceanfs-storage/src/segment/` (streaming encode).

## Scope

### In Scope

- **GFNI instructions for GF(2^8) multiply.** Intel's Galois Field New
  Instructions include `vgf2p8affineqb` (among others), which performs
  GF(2^8) multiplication of 64 elements in a single instruction — no
  table lookup, no SIMD shuffling, no carry-less multiply + polynomial
  reduction. This is the fastest possible GF(2^8) path: one instruction
  per 64 bytes. Implement as a new SIMD level `GfSimdLevel::Gfni` in
  the runtime feature detection cache (extending the `AtomicU8` from
  Feature 2). Detection: `std::is_x86_feature_detected!("gfni")` at
  first GF operation. When GFNI is available, it is the highest
  priority path (above AVX-512 VPCLMULQDQ, since GFNI is both faster
  and simpler). The dispatch order becomes:
  ```
  GFNI → AVX-512 (VPCLMULQDQ) → AVX2 (PCLMULQDQ) → SSE4.1 (PSHUFB) → Portable
  ```
  GFNI requires AVX-512 or AVX2 as a foundation (the instruction
  operates on 512-bit or 256-bit registers depending on the VEX/EVEX
  prefix). The implementation uses `_mm512_gf2p8affine_epi64_epi8` on
  AVX-512 hardware and `_mm256_gf2p8affine_epi64_epi8` on AVX2+GFNI
  hardware (GFNI without AVX-512, e.g., some Ice Lake client SKUs).
  All `unsafe` blocks must have `// SAFETY:` comments per guideline
  §12.1. The implementation lives in
  `oceanfs-ec/src/gf/simd_x86.rs` alongside the existing SSE4.1/AVX2/
  AVX-512 paths.

- **Cauchy matrix precomputation as `const` arrays.** For the most
  common (k, m) pairs — (4,2), (6,3), (8,4), (10,6) — precompute the
  entire Cauchy Reed-Solomon encode matrix at compile time as `const
  [[u8; K]; M]` arrays. The encode matrix is m×k bytes, deterministic
  for a given (k,m) pair, and the Cauchy matrix construction
  (`ec_init_tables` in ISA-L, or the Rust-side `cauchy_matrix()`
  function) is pure computation with no runtime inputs beyond (k,m).
  Eliminating runtime matrix construction removes ~30-100µs of setup
  per segment encode (computing the Cauchy matrix, the Vandermonde
  determinant, and inverting submatrices). The encode function reads
  from a `const` slice instead of a runtime-computed table. For
  uncommon (k,m) pairs, fall back to runtime matrix computation.
  Implementation:
  ```rust
  // Compile-time precomputed encode matrix for (k=4, m=2)
  const CAUCHY_MATRIX_4_2: [[u8; 4]; 2] = [
      [0x01, 0x01, 0x01, 0x01],
      [0x01, 0x02, 0x04, 0x08],
  ];
  // More rows for m=3 would be:
  // [0x01, 0x03, 0x05, 0x0f], etc.
  ```
  The precomputed matrices cover the matrix for encode only (m parity
  rows). The decode matrix is dynamic (depends on which shards are
  available), so decode still uses runtime computation. The `const`
  matrices live in `oceanfs-ec/src/matrix.rs`. A build script
  (`build.rs`) uses `ec_init_tables` or equivalent Rust logic to
  generate the values and writes them as a Rust source file — the
  values are verified by a test that compares `const` matrix × data
  against runtime-computed matrix × data for round-trip correctness.

- **Streaming EC encode.** Instead of encoding a full segment at seal
  time (the current model in the spec), encode each stripe row as soon
  as all k data shard bytes for that row are available in the segment
  buffer. The segment buffer already holds appended blob data organized
  by byte position (the segment is a logical byte array). When byte
  position N is written and `N % (k * strip_size) == 0` (i.e., a stripe
  row boundary is reached), fire off a `rayon::spawn` (or enqueue to a
  bounded `tokio::sync::mpsc` channel feeding a dedicated encode worker
  pool) to encode that stripe row to its m parity shards. The seal
  event becomes a no-op for encoding — all parity shards are already
  computed. It only needs to: (1) finalize any partial last stripe
  (padded to strip_size), (2) collect parity shards into segment
  metadata, (3) distribute shards to replica nodes. This eliminates
  the seal-time latency spike that currently blocks the write path
  when `write_ec_async = false`.
  Tradeoffs:
  - Parity shards must be buffered until the segment is fully sealed
    and distributed. Memory overhead: m × segment_size bytes per active
    segment. For a 4 MB segment with m=2, that's 8 MB per segment —
    acceptable for the active segment pool (4 segments × 8 MB = 32 MB).
  - The segment buffer must track which stripes are "ready" — a bitmap
    or counter per stripe row. A stripe is ready when all k shard bytes
    for that row have been written (a stripe row spans k × strip_size
    bytes in the logical segment).
  - Encoding work is spread evenly across the write lifetime instead
    of concentrated at seal — smoother CPU utilization, no seal-time
    latency spike.
  Implementation: modify `ActiveSegment` in `oceanfs-storage` to
  maintain a `StripeReadiness` bitmap and an `Arc<Mutex<ParityBuffer>>`
  for parity shard accumulation. Spawn encode tasks via rayon when a
  stripe becomes ready. Configurable: `ec_streaming_encode = true`
  (default in write-optimized profiles, where seal latency matters).

### Out of Scope (for this feature)

- **ISA-L FFI path.** Already implemented in `oceanfs-accel/src/isal.rs`.
  This feature's GFNI path is a Rust-native SIMD path, not a replacement
  for ISA-L.
- **GPU/CUDA EC path.** Separate epic (Phase 8).
- **ARM NEON/SVE improvements.** The existing ARM backend
  (`oceanfs-accel/src/arm_sve.rs`) is unchanged. ARM does not have
  GFNI-equivalent instructions (FEAT_GCS is a different feature).
- **Fountain codes, delta encoding, or other algorithmic codec changes.**
  These are architectural changes to the EC codec itself, not
  optimizations of the existing Cauchy RS codec. Out of scope for this
  epic.
- **Per-decode matrix inversion caching** (accel H3) — separate
  optimization.
- **EC stripe layout changes** (SoA memory layout). Already covered by
  perf guideline §6.2; tracked separately.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-ec` | Extend `src/gf/simd_x86.rs` with GFNI paths: `gf_mul_gfni_avx512` (512-bit) and `gf_mul_gfni_avx2` (256-bit for GFNI without AVX-512). Extend `GfSimdLevel` enum with `Gfni` variant. New file `src/matrix.rs` with precomputed Cauchy matrices as `const` arrays. New `build.rs` to generate verified matrix constants. |
| `oceanfs-accel` | Modify `src/tier0.rs` to use precomputed `const` matrices for common (k,m) when available, falling back to runtime matrix construction. |
| `oceanfs-storage` | Modify `src/segment/active.rs` (`ActiveSegment`) to support streaming EC encode: add `StripeReadiness` bitmap, `ParityBuffer`, and logic to detect stripe row completion and spawn encode tasks. Modify `src/segment/sealer.rs` to collect pre-computed parity shards and finalize partial stripes. |
| `oceanfs-core` | New config field: `ec_streaming_encode: bool` (default `true`). |
| `benches/` | Extend `ec_benchmark.rs` with GFNI vs AVX-512 vs AVX2 vs portable comparison. Add benchmark for encode latency with and without streaming (simulated seal-time spike vs spread-out encode). |

## Interface (Public API)

- `pub enum GfSimdLevel` in `oceanfs-ec::gf` — gains `Gfni` variant
  (ordered highest priority). Detection extended to check for
  `std::is_x86_feature_detected!("gfni")`.
- `pub(crate) fn cauchy_encode_matrix(k: u8, m: u8) -> &'static [[u8; K_MAX]; M_MAX]`
  in `oceanfs-ec::matrix` — returns a reference to the precomputed
  `const` matrix for given (k,m), or computes at runtime for uncommon
  pairs. This is `pub(crate)` — consumers in `oceanfs-accel` access
  it via their dependency.
- `pub(crate) struct ParityBuffer` in `oceanfs-storage::segment` —
  accumulates pre-computed parity shards during streaming encode.
  Exposed to the sealer via `pub(super)`.
- No breaking changes to the `Encoder` or `Decoder` traits. GFNI is
  an internal dispatch level, transparent to trait users.

## Data Flow

**GFNI GF(2^8) dispatch (first call → cached):**
```
gf_mul(a, b):
  ├─ [first call] GfSimdLevel::detect()
  │     ├─ is_x86_feature_detected!("gfni") && "avx512f" → Gfni (AVX-512)
  │     ├─ is_x86_feature_detected!("gfni") && "avx2"    → Gfni (AVX2)
  │     ├─ is_x86_feature_detected!("avx512f")            → Avx512 (VPCLMULQDQ)
  │     ├─ is_x86_feature_detected!("avx2")               → Avx2 (PCLMULQDQ)
  │     ├─ is_x86_feature_detected!("sse4.1")             → Sse41 (PSHUFB)
  │     └─ else                                           → Portable
  │     Cache in GF_SIMD_LEVEL (AtomicU8, Release/Acquire)
  │
  └─ [every call] match cached level:
       ├─ Gfni     → gf_mul_gfni_avx512 or gf_mul_gfni_avx2
       ├─ Avx512   → gf_mul_avx512
       ├─ Avx2     → gf_mul_avx2
       ├─ Sse41    → gf_mul_sse41
       └─ Portable → gf_mul_portable
```

**Streaming EC encode:**
```
PUT /bucket/key → WriteCoordinator → ActiveSegment::append(data)
  ├─ write data to segment buffer at position N
  ├─ update StripeReadiness bitmap:
  │     stripe_row = N / (k * strip_size)
  │     byte_in_row = N % (k * strip_size)
  │     shard_idx = byte_in_row / strip_size
  │     if this write completes shard shard_idx for stripe_row:
  │       set bit shard_idx in StripeReadiness[stripe_row]
  ├─ if StripeReadiness[stripe_row].all_set():
  │     └─ spawn encode for stripe_row:
  │          read k shard bytes from segment buffer
  │          encode k → m parity (via AccelDispatcher)
  │          write m parity shards to ParityBuffer[stripe_row]
  └─ return SegmentHandle to client (WAL ack)

Later: Segment sealed (full or timeout)
  ├─ [encode already done for all full stripes]
  ├─ finalize partial last stripe (pad, encode)
  ├─ collect ParityBuffer → SegmentMetadata.parity_shards
  ├─ distribute k+m shards to replica nodes
  └─ [seal complete — no encode latency spike]
```

**Const matrix vs runtime matrix:**
```
AccelDispatcher::encode(data, k, m):
  ├─ match (k, m):
  │     (4,2) | (6,3) | (8,4) | (10,6) →
  │       matrix = CAUCHY_MATRIX_K_M  // &'static [[u8; K]; M]
  │     other →
  │       matrix = compute_cauchy_matrix(k, m)  // runtime
  └─ encode_with_matrix(data, matrix)
```

**Expected GFNI performance (single stripe, 64KB shard, k=4):**

| Path | GF ops | Time (est.) | vs Portable |
|---|---|---|---|
| Portable (log/exp) | 524,288 | ~2.6 ms | 1.0× |
| SSE4.1 (PSHUFB) | 524,288 | ~1.7 ms | 1.5× |
| AVX2 (PCLMULQDQ) | 524,288 | ~1.3 ms | 2.0× |
| AVX-512 (VPCLMULQDQ) | 524,288 | ~0.65 ms | 4.0× |
| **GFNI** (VGF2P8AFFINEQB) | 524,288 | ~0.3 ms | **~8.7×** |

GFNI achieves the maximum theoretical throughput because a single
instruction replaces the entire `gf_mul` body — no table lookups,
no reduction, no shuffling.

## Definition of Done

- [ ] **GFNI path:** `gf_mul_gfni_avx512` and `gf_mul_gfni_avx2`
  functions implemented in `oceanfs-ec/src/gf/simd_x86.rs`. Use
  `_mm512_gf2p8affine_epi64_epi8` (AVX-512+GFNI) and
  `_mm256_gf2p8affine_epi64_epi8` (AVX2+GFNI). `GfSimdLevel::Gfni`
  variant added. Runtime detection checks `gfni` feature and selects
  the appropriate vector width. Unit tests verify correctness against
  portable `gf_mul` for all byte values (0..255) × (0..255) and
  random multi-byte inputs. Every `unsafe` block has `// SAFETY:`.
- [ ] **Const matrices:** Precomputed `const [[u8; K]; M]` arrays for
  (4,2), (6,3), (8,4), (10,6) in `oceanfs-ec/src/matrix.rs`. Build
  script generates and verifies values. `cauchy_encode_matrix(k, m)`
  returns `&'static` reference or computes at runtime. Test verifies
  that `const` matrix × data == runtime-computed matrix × data for
  all supported (k,m) pairs.
- [ ] **Streaming EC encode:** `ActiveSegment` maintains
  `StripeReadiness` bitmap and `ParityBuffer`. Stripe completion
  detection fires on stripe row boundary. Encode tasks dispatched via
  rayon (or channel to encode worker pool). Partial last stripe
  finalized at seal. `ec_streaming_encode` config flag (default
  `true`). Existing segment append tests pass (streaming encode must
  produce identical parity shards to seal-time encode). New tests:
  streaming encode produces correct parity for single-stripe and
  multi-stripe segments; parity buffer memory usage is bounded;
  seal with streaming encode is a no-op.
- [ ] **Code:** `cargo build --all-targets` succeeds on x86_64.
  Cross-compilation to aarch64 succeeds (GFNI paths `#[cfg]`-gated;
  const matrices are platform-independent; streaming encode is
  platform-independent). `cargo build --features nightly` succeeds
  (GFNI requires `std::arch::x86_64` which is stable as of Rust
  1.59 for the instructions used).
- [ ] **Tests:** All 40+ existing tests in `oceanfs-ec` pass. New tests:
  GFNI correctness cross-check vs portable; const matrix round-trip
  (encode+decode); streaming encode vs seal-time encode parity
  equivalence; `GfSimdLevel` priority ordering (GFNI selected when
  available).
- [ ] **Docs:** `GfSimdLevel::Gfni` documented. Module-level docs in
  `simd_x86.rs` describe the GFNI algorithm. `cauchy_encode_matrix()`
  has `# Examples`. Streaming encode design documented in
  `ActiveSegment` module doc.
- [ ] **ADR:** ADR-0006 §1 (startup probing, cached for lifetime)
  satisfied — GFNI is one more cached SIMD level, detected once at
  first GF operation. Fallback chain: Gfni → Avx512 → Avx2 → Sse41
  → Portable. Const matrices are purely a compile-time optimization
  — no runtime impact. Streaming encode does not change the EC trait
  interface — `Encoder::encode()` is still callable on a full stripe.
- [ ] **Perf:** Criterion benchmarks show: GFNI achieves ≥8× speedup
  over portable for GF multiplication; const matrix setup is zero-cost
  (compile-time) vs ~50µs runtime; streaming encode eliminates ≥95%
  of seal-time encode latency (seal latency reduced from ~10ms to
  ~50µs for parity collection only).
- [ ] **Integration:** End-to-end S3 PUT flow exercises streaming encode
  for multi-blob segments. Seal-time latency measured and verified to
  be dominated by parity collection and shard distribution, not encode.
  Round-trip PUT+GET with GFNI-enabled hardware produces correct data.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and `ignore`-tagged
> doc examples are non-blocking (see `guidelines/coding.md` §9.2.1).
