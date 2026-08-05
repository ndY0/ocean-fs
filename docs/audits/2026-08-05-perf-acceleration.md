---
audit_date: 2026-08-05
scope: targeted
target_crates: oceanfs-accel, oceanfs-ec, oceanfs-hash
severity_counts:
  critical: 2
  high: 5
  medium: 8
  low: 6
---

# Audit Report: Acceleration Subsystem Hot Path Performance

## Summary

The acceleration subsystem is architecturally sound — startup probing, cached tier resolution,
`// SAFETY:`-documented unsafe blocks, and a clean fallback chain from GPU → ISA-L → CPU SIMD.
However, the hot path suffers from several performance issues: **`dyn Trait` dispatch** on every
encode/decode call (violates perf rule 6.4), **`Vec<Vec<u8>>` return types** causing per-stripe
heap allocations (violates perf rule 1.1), **no SIMD in the portable GF(2^8) path** (log/exp table
lookup only), **per-decode matrix inversion** with no memoization, and **duplicated GF code** across
three crates. The CUDA probing is a no-op returning `true` unconditionally, and no pinned memory
pool exists for GPU transfers despite the spec mandating one (§9.5.3). ARM NEON/SVE backends are
well-structured but are never exercised on non-aarch64 targets. BLAKE3 hashing is fully compliant.

---

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `oceanfs-accel/src/dispatcher.rs:871-878` | **`dyn Encoder` vtable dispatch on the EC encode hot path.** `AccelDispatcher::encode()` calls `self.encoder.encode(...)` where `encoder: Arc<dyn Encoder>`. Every encode call incurs a vtable lookup + indirect call. Perf rule §6.4 explicitly forbids `dyn Trait` on EC encode/decode — mandates static dispatch via generics. | Replace `Arc<dyn Encoder>` with an enum-based dispatch (`enum EncoderBackend { Cpu(CpuEncoder), Isal(IsalEncoder), Cuda(CudaBackend) }`) that monomorphizes at the call site. Or use a generic `AccelDispatcher<Cpu, Isal, Cuda>` with static dispatch. |
| C2 | `oceanfs-ec/src/traits.rs:27` | **`Encoder::encode()` returns `Vec<Vec<u8>>` — per-stripe heap allocation.** Every encode call allocates `m * shard_size` bytes per stripe. For a 4 MB segment with k=4, m=2, strip_size=64KB: 16 stripes × 2 parity × 64KB = 2 MB of fresh heap allocations per segment. Perf rule §1.1 mandates `Bytes`/`BytesMut`. | Change return type to `Vec<Bytes>` or accept a pre-allocated `&mut [BytesMut]` output buffer. Use a per-thread `BytesMut` pool for parity output. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `oceanfs-ec/src/gf.rs:62-69` | **GF(2^8) multiplication uses log/exp table lookup only — no SIMD path.** `gf_mul` does two static array lookups (`LOG_TABLE`, `EXP_TABLE`) plus an addition. This is ~5-8 cycles per multiply on modern CPUs. AVX2 `PCLMULQDQ` or split-table lookups would process 16-32 elements in the same time. The spec (§9.3.2) describes runtime SIMD dispatch; this is not implemented in `gf.rs`. | Implement `gf_mul_simd` using `#[cfg(target_feature = "avx2")]` with `_mm_clmulepi64_si128` or a split-table approach (as done in `arm_sve.rs`). The ARM backend already has a working split-table NEON implementation — the same algorithm works on x86 with SSE2/AVX2. |
| H2 | `oceanfs-ec/src/cauchy.rs:95-114` | **Triple-nested loop in `encode_cauchy` with `gf_mul` inner.** For k=4, m=2, shard_size=64KB: 524,288 GF multiplies per stripe. With 16 stripes: 8.4M GF multiplies. At ~5ns/GF op (table lookup): ~42ms total. No vectorization — the compiler cannot auto-vectorize the indirect table lookup pattern. | The ISA-L and ARM backends already have efficient SIMD encode paths. The Tier 0 portable path needs split-table SIMD (as H1) to bring the baseline encode time down from ~42ms to ~2ms (20× improvement with 16-byte SIMD chunks). |
| H3 | `oceanfs-ec/src/cauchy.rs:195-260` | **Per-decode matrix inversion via Gauss-Jordan — no memoization.** Every decode call inverts a k×k matrix from scratch: O(k³) GF operations. For k=4 this is ~100 ops; for k=16 it's ~4,000 ops. The inverse depends only on (k, m) and which shard indices are missing — it is deterministic and could be memoized. The ISA-L decoder (`isal.rs:341-442`) and ARM decoder both repeat this. | Cache decoded matrices by `(k, m, missing_indices_bitmask)` in a `HashMap` or fixed-size LRU. For common failure patterns (single missing shard), precompute all k possible inverses at startup. |
| H4 | `oceanfs-accel/src/cuda/mod.rs:807-817` | **CUDA probing is a no-op.** `probe_cuda()` unconditionally returns `true` when the `cuda` feature is enabled, with the comment "treat CUDA as available if the feature is on." The actual probing happens later in `CudaBackend::new()`. This means `AccelDispatcher::new()` falsely resolves the tier to `GpuCuda` when no GPU is present, hiding the fallback at startup. | Call `cudarc::init()` and check `device_count > 0` in `probe_cuda()`. Only return `true` when a GPU is confirmed present. |
| H5 | `oceanfs-accel/src/arm_sve.rs`, `oceanfs-accel/src/cuda/mod.rs` | **Duplicate GF(2^8) tables and portable arithmetic across three files.** `oceanfs-ec/src/gf.rs` has log/exp tables built via compile-time `const fn`. `oceanfs-accel/src/arm_sve.rs` has identical `GF_LOG`/`GF_EXP` static arrays (hardcoded). `oceanfs-accel/src/cuda/mod.rs` has a third copy. Three maintenance points for a single mathematical primitive. | Delete the duplicated tables in `arm_sve.rs` and `cuda/mod.rs`. Re-export `oceanfs_ec::gf::gf_mul`, `gf_inv` etc. Build split-tables from the canonical `gf_mul` in `oceanfs-ec`. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `oceanfs-accel/src/metrics.rs:27-42` | **`AccelMetrics` has six `AtomicU64` fields without cache-line padding.** Multiple threads on different cores increment `bytes_encoded`, `bytes_decoded`, `encode_ops_total`, `decode_ops_total` simultaneously — these atomics share cache lines, causing false-sharing stalls. Perf rule §6.1. | Add `#[repr(align(64))]` to `AccelMetrics` or place each counter in its own cache-line-padded cell (e.g., `CachePadded<AtomicU64>` from `crossbeam-utils`). |
| M2 | `oceanfs-ec/src/stripe/parallel.rs:133,221` | **`Vec<Vec<u8>>` allocation for parity and data shards in `ParallelEncoder::encode()` and `ParallelDecoder::decode()`.** Each call creates `data_shards: Vec<Vec<u8>>` with `k` allocations of `total_stripes * shard_size` bytes. The `StripeBatch` holds interior `Vec<Vec<u8>>`. This is AoS-like (array of heap pointers) rather than true SoA. | Use a single contiguous `BytesMut` allocation and index into it with offset arithmetic. `StripeBatch` should hold `Bytes` slices, not owned `Vec<Vec<u8>>`. |
| M3 | `oceanfs-accel/src/isal.rs:750` | **`Box::leak(Box::new(tables))` for `IsalTables` lifetime management.** The tables are intentionally leaked for `'static` lifetime so they can be shared via `Arc<dyn Encoder>`. The memory is small (~few KB) and allocated once at startup, so the leak is harmless — but it prevents the tables from being dropped if the backend is reconfigured. | Acceptable for now given the tiny fixed size. Document in a code comment that this is intentional and the tables are never freed. |
| M4 | `oceanfs-ec/src/stripe/parallel.rs:135-144` | **`rayon::par_iter()` collects `Vec<Result<Vec<Vec<u8>>>>` per stripe into a Vec, then copies parity into SoA layout.** This creates an intermediate `Vec` of per-stripe results and then copies each parity shard's data into the SoA layout via `copy_from_slice`. The intermediate `Vec` could be avoided by writing directly into the SoA layout from within the parallel closures. | Use `rayon::par_iter().zip()` or an `AtomicUsize` cursor to write parity shards directly into pre-allocated SoA buffers. |
| M5 | `oceanfs-accel/src/dispatcher.rs:872-879` | **Per-operation atomic counter increments in the encode/decode hot path.** `self.metrics.record_encode(byte_count)` calls `fetch_add` on two atomics per encode. While `Ordering::Relaxed` atomics are cheap (~1 cycle on x86), they still cause cache-line contention when multiple cores encode concurrently. | Batch metrics updates: accumulate bytes in a thread-local counter and flush periodically. Or accept the cost — atomics with relaxed ordering are very cheap on x86. |
| M6 | `oceanfs-accel/src/dispatcher.rs:186-188` | **`CpuEncoder` is constructed twice** — once as encoder, once as decoder (since `CpuEncoder` implements both traits). This allocates two `CauchyEncoder` instances with identical `CodecConfig`. | Share a single `Arc<CpuEncoder>` for both roles, since `CpuEncoder` implements both `Encoder` and `Decoder`. |
| M7 | `oceanfs-ec/src/stripe/batch.rs:11-16` | **`StripeBatch` uses `Vec<Vec<u8>>` internally, not fixed-size arrays.** The guideline (§6.2) specifies `data: [[u8; 64KiB]; k]` for compile-time-known shard sizes. In practice, strip sizes vary per bucket config, so compile-time arrays may not be feasible — but the `Vec<Vec<u8>>` versus flat `Vec<u8>` layout matters. | Consider a flat `Vec<u8>` with offset-based indexing: `fn shard_offset(stripe: usize, shard: usize) -> usize`. This gives true SoA without the double-indirection of `Vec<Vec<u8>>`. |
| M8 | `oceanfs-accel/src/cuda/mod.rs` | **GPU encode reads data from `&[&[u8]]`, copies into a flat `Vec<u8>` via `extend_from_slice`, then copies to GPU.** This is a CPU-side data copy before the H→D transfer. For large segments, this doubles the memory bandwidth cost. | Use `cudaMemcpy2D` or staged copies to upload shards directly from their individual buffers without flattening. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `oceanfs-accel/src/dispatcher.rs:139` | **Startup probe runs synchronously.** Probing adds ~50-200ms to node startup (CPUID, CUDA device enumeration). This is acceptable per ADR-0006 §Consequences, but could be made async if startup latency matters. | No change needed — ADR-0006 explicitly chose synchronous probing. Documented tradeoff. |
| L2 | `oceanfs-ec/src/gf.rs:81` | **`gf_div` and `gf_inv` panic on zero input.** The panic message is clear but panicking in library code is discouraged (architecture.md §7.3). These are internal functions called only with validated inputs. | Return `0` for `gf_inv(0)` instead of panicking (or return `Option<Gf8>`). The panic is currently unreachable in practice but is technically a risk. |
| L3 | `oceanfs-hash/src/batch.rs` | **`BatchHasher` hashes chunks sequentially.** The trait provides `hash_chunks(&self, chunks: &[&[u8]]) -> Vec<HashOutput>` but the implementation (`Blake3BatchHasher`) is sequential. For multi-chunk blob verification, rayon parallelism could help. | Add `rayon::par_iter()` to `Blake3BatchHasher::hash_chunks()` when chunk count > 1. |
| L4 | `oceanfs-ec/src/stripe/parallel.rs:98` | **Tokio semaphore permit is dropped at end of method scope.** `let _permit = self.semaphore.as_ref().map(|s| s.acquire());` — the `_permit` holds an acquired-but-incomplete future until dropped. This only works because `acquire()` is called in a non-async context (the methods are sync). The permit is held for the entire encode/decode (including rayon work), which may block other callers unnecessarily. | Use `try_acquire()` and hold the permit only for the duration that shared resource access is needed, not the entire encoding computation. Alternatively, document that this is intentional — the semaphore bounds segment-level concurrency. |
| L5 | `oceanfs-accel/src/dispatcher.rs:149-150` | **Tier 1 probes use `#[cfg(any(feature = "isa-l", feature = "arm-sve"))]`** — mutually exclusive features but both map to `AccelTier::IsaL`. This is by design (ADR-0006 §1 — one Tier 1 per platform), but confusing. | Consider renaming `AccelTier::IsaL` to `AccelTier::CpuSimdTier1` or adding an `ArmSve` variant for clarity. |
| L6 | `oceanfs-accel/src/arm_sve.rs:77-79` | **`ArmSveLevel::from_usize` is missing** — no numeric conversion method. The enum has `#[non_exhaustive]` but no standard conversion utilities. | Add `impl From<u8> for ArmSveLevel` or a `from_u8()` method if needed for config parsing. |

---

## Coupling Hotspots

| Symbol | Crate | In-Degree | Risk |
|---|---|---|---|
| `Encoder::encode` (trait method) | oceanfs-ec | 63 | Medium — trait method with many implementors; changing signature breaks all backends |
| `AccelDispatcher::new` | oceanfs-accel | 50 | High — startup path; every change affects all consumers |
| `Decoder::decode` (trait method) | oceanfs-ec | 49 | Medium — same as encode, mirror risk |
| `AccelConfig::default` | oceanfs-core | 51 | Low — default config, widely used |

The dependency graph respects the DAG constraint (oceanfs-ec → oceanfs-accel → oceanfs-storage). No circular dependencies detected.

---

## Dependency Graph

The crate DAG matches `guidelines/architecture.md §1.1`:
```
oceanfs-hash → oceanfs-core → oceanfs-ec → oceanfs-accel → oceanfs-storage
```
No violations. The `oceanfs-core` purity check shows only `oceanfs-hash` as an internal dependency (per ADR-0008).

---

## Guideline Violations

| Guideline | Location | Violation |
|---|---|---|
| §1.1 | `oceanfs-ec/src/traits.rs:27` | `Encoder::encode()` returns `Vec<Vec<u8>>` — not `Bytes`/`BytesMut` |
| §1.1 | `oceanfs-ec/src/cauchy.rs:101` | `encode_cauchy` allocates parity as `Vec<Vec<u8>>` |
| §1.1 | `oceanfs-ec/src/stripe/parallel.rs:108-111` | `ParallelEncoder::encode` allocates `Vec<Vec<u8>>` for data shards |
| §1.6 | — | No object pool found for EC encode/decode descriptors (`src/ec/`, `src/accel/`) |
| §6.1 | `oceanfs-accel/src/metrics.rs:27` | `AccelMetrics` has multiple `AtomicU64` fields without `#[repr(align(64))]` |
| §6.2 | `oceanfs-ec/src/stripe/batch.rs:12-15` | `StripeBatch` uses `Vec<Vec<u8>>` internally — not `[[u8; 64KiB]; k]` fixed arrays |
| §6.4 | `oceanfs-accel/src/dispatcher.rs:95-96` | `AccelDispatcher` holds `Arc<dyn Encoder>` and `Arc<dyn Decoder>` — dynamic dispatch on encode/decode hot path |

### Compliant Rules

| Guideline | Status | Evidence |
|---|---|---|
| §2.1 | COMPLIANT | `ParallelEncoder::encode()` and `ParallelDecoder::decode()` both use `rayon::par_iter()` for stripe parallelism (`parallel.rs:135-144`, `224-251`) |
| §2.7 | COMPLIANT | `CudaBackend` uses `tokio::sync::Semaphore` with `max_concurrent_ops` (default 1) (`cuda/mod.rs:223,293`) |
| §5.1 | COMPLIANT | Uses upstream `blake3` crate with runtime SIMD detection (`hasher.rs:53-56`) |
| §5.2 | COMPLIANT | `Hasher` trait has `update()` for streaming hash — no full-blob buffering (`hasher.rs:27-36`) |
| §5.3 | COMPLIANT | Platform-specific SIMD paths with `#[cfg(target_arch)]` and portable fallbacks (`arm_sve.rs:44-45`, `tier0.rs:54-81`) |
| §10.6 | COMPLIANT | `#[cfg(target_arch = "x86_64")]` and `#[cfg(target_arch = "aarch64")]` with fallbacks (`tier0.rs:54`, `arm_sve.rs:44`) |
| §12.1 | COMPLIANT | Every `unsafe` block in ISA-L, CUDA, and ARM SIMD has `// SAFETY:` comments (`isal.rs:130`, `arm_sve.rs:231-232`, `cuda/mod.rs`) |

---

## ADR Compliance

| ADR | Status | Notes |
|---|---|---|
| ADR-0006 (§1: Startup probing) | COMPLIANT | Probing runs at `AccelDispatcher::new()`, cached for lifetime. No lazy probing. |
| ADR-0006 (§2: Fallback chain) | COMPLIANT | GpuCuda → IsaL → CpuSimd with WARN logging and atomic fallback counters. |
| ADR-0006 (§3: Trait-based pluggability) | COMPLIANT | All backends implement `Encoder`/`Decoder`. Dispatcher delegates through `Arc<dyn Encoder>`. |
| ADR-0006 (§4: GPU semaphore) | COMPLIANT | `CudaBackend` has `encode_semaphore: Arc<Semaphore>` with configurable permits. |
| ADR-0006 (§5: Non-EC acceleration) | COMPLIANT | BLAKE3 delegates to upstream crate. Compressor trait exists with zstd/igzip/nvcomp backends. |
| ADR-0008 | COMPLIANT | `oceanfs-hash` implemented with `Blake3Hasher`, `Hasher` trait, `HashOutput` in correct crate. |

---

## Test Coverage

| Crate | Public Symbols | Tests | Coverage Assessment |
|---|---|---|---|
| `oceanfs-accel` | 12 (pub types) | 44 unit tests + 4 integration tests | Good — dispatcher, ISA-L, ARM, metrics, error all tested. Roundtrip encode/decode tested per tier. |
| `oceanfs-ec` | 10 (pub types) | 40+ unit tests + proptests | Good — GF arithmetic, Cauchy encode/decode, stripe batch, parallel encode/decode. Cross-backend compatibility tested in ISA-L. |
| `oceanfs-hash` | 4 (pub types) | 8 unit tests | Adequate — basic hash correctness, cloning, idempotency. No streaming multi-chunk test. |

**Gaps:**
- No criterion benchmarks for EC encode/decode (per rule §11.4)
- No GPU kernel integration tests (requires CUDA hardware)
- No NEON/SVE kernel tests on non-aarch64 (compile-only verification)

---

## EC Encode Trace (4MB segment, k=4, m=2, strip_size=64KiB)

```
Segment sealed → ParallelEncoder::encode(segment_data, plan)
  │
  ├─ Tokio semaphore acquire (optional)
  │
  ├─ Allocate data_shards: Vec::with_capacity(4)                      [1 alloc: 16 bytes]
  │   ├─ data_shards[0]: vec![0u8; 16 * 65536]                        [1 alloc: 1,048,576 bytes]
  │   ├─ data_shards[1]: vec![0u8; 16 * 65536]                        [1 alloc: 1,048,576 bytes]
  │   ├─ data_shards[2]: vec![0u8; 16 * 65536]                        [1 alloc: 1,048,576 bytes]
  │   └─ data_shards[3]: vec![0u8; 16 * 65536]                        [1 alloc: 1,048,576 bytes]
  │
  ├─ Copy segment_data into interleaved SoA layout (memcpy)
  │
  ├─ Allocate parity_shards: vec![vec![0u8; 16*65536]; 2]             [3 allocs: ~2,097,152 bytes]
  │
  ├─ Rayon par_iter over 16 stripes → per stripe:
  │   │
  │   ├─ Build stripe_data: Vec<&[u8]> from shard slices              [1 alloc: ~64 bytes]
  │   │
  │   └─ AccelDispatcher::encode(stripe_data, m=2)
  │       │
  │       ├─ Arc::clone (atomic increment)                             [~5 ns]
  │       ├─ dyn Encoder vtable dispatch                               [~5-10 ns]
  │       ├─ metrics.record_encode (2× atomic fetch_add)              [~10 ns]
  │       │
  │       └─ CpuEncoder::encode → CauchyEncoder::encode → encode_cauchy
  │           │
  │           ├─ Allocate parity: (0..2).map(|_| vec![0u8; 65536])    [2 allocs: 131,072 bytes]
  │           │
  │           └─ Triple-nested loop: rows(2) × bytes(65536) × cols(4)
  │               │
  │               └─ gf_mul(a, b): LOG_TABLE[a] + LOG_TABLE[b]        [~5 ns each]
  │                  → EXP_TABLE[sum]                                   [524,288 × per stripe]
  │                                                                     [~2.6 ms per stripe]
  │
  ├─ Collect rayon results: Vec<Result<Vec<Vec<u8>>>>                 [1 alloc: 16 entries]
  │
  ├─ Copy each stripe's parity into SoA parity_shards (memcpy)        [2 MB copied]
  │
  └─ Return StripeBatch { data, parity }

Total allocations: ~14 + 16×(1+2) = ~62 allocations per segment
Total bytes allocated: ~8.3 MB per segment (2× segment size)
Total CPU time: ~42 ms (16 stripes × 2.6 ms) — estimated at ~5ns/GF op
```

**Key observations:**
1. Per-stripe allocation of intermediate parity `Vec<Vec<u8>>` doubles memory usage
2. `gf_mul` is purely table-based — no SIMD vectorization
3. Each stripe's parity is allocated and then copied into SoA — a wasteful intermediate step
4. Vtable dispatch on every stripe's encode call (16 times per segment)

---

## EC Decode Trace (same segment, recovering 1 missing data shard)

```
ReadCoordinator → ParallelDecoder::decode(batch, plan, &[0])
  │
  ├─ Tokio semaphore acquire (optional)
  │
  ├─ Allocate recovered_data: vec![vec![0u8; 16*65536]; 4]            [5 allocs: ~4,194,304 bytes]
  │
  ├─ Rayon par_iter over 16 stripes → per stripe:
  │   │
  │   ├─ Build shards: Vec<Option<&[u8]>> of length k+m                [1 alloc]
  │   │
  │   └─ AccelDispatcher::decode(shards, k=4, m=2)
  │       │
  │       ├─ dyn Decoder vtable dispatch                               [~5-10 ns]
  │       ├─ metrics.record_decode (2× atomic fetch_add)              [~10 ns]
  │       │
  │       └─ CauchyEncoder::decode
  │           │
  │           ├─ Build generator matrix G (4+2)×4                      [1 alloc per decode!]
  │           ├─ Select k surviving rows → sub_matrix                  [1 alloc]
  │           ├─ Gauss-Jordan matrix inversion O(k³)                    [~100-4000 GF ops]
  │           │   ├─ Allocate augmented matrix [A|I]: 4×8              [1 alloc]
  │           │   ├─ Forward elimination + normalization
  │           │   └─ Extract inverse
  │           ├─ Allocate recovered: (0..4).map(|_| vec![0u8; 65536]) [4 allocs]
  │           │
  │           └─ Triple-nested loop: rows(4) × bytes(65536) × cols(4)
  │               └─ gf_mul (same table lookup)                         [~5.2 ms per stripe]
  │
  ├─ Collect rayon results → copy decoded data into SoA
  │
  └─ Return recovered_data

Total allocations: ~14 + 16×(1+1+1+4) = ~126 allocations per segment
Total CPU time: ~83 ms (16 stripes × 5.2 ms) — includes matrix inversion per stripe
```

**Key observations:**
1. **Matrix inversion repeated per stripe** — 16 Gauss-Jordan inversions of the same 4×4 matrix
2. Generator matrix rebuilt per decode (could be cached per (k,m))
3. Same `Vec<Vec<u8>>` allocation pattern as encode

---

## GPU Offload Overhead Breakdown

For a batch of 64 stripes (k=4, m=2, strip_size=64KiB = 16MB total data):

| Step | Operation | Est. Latency | Notes |
|---|---|---|---|
| 1 | Semaphore acquire | ~1 µs | `try_acquire()` — non-blocking atomics |
| 2 | Build GPU encode tables (CPU) | ~10 µs | Cauchy matrix + split-table construction |
| 3 | Flatten data shards (CPU) | ~50 µs | `extend_from_slice` for 4 × 64 × 64KB = 16MB |
| 4 | Allocate device memory (cudaMalloc) | ~100 µs | 16MB input + 8MB output + tables |
| 5 | H→D copy (PCIe 3.0 x16) | ~1,200 µs | 16MB ÷ 13 GB/s ≈ 1.2ms (non-pinned!) |
| 6 | Kernel launch overhead | ~10 µs | CUDA driver scheduling |
| 7 | Kernel execution | ~50 µs | 64 stripes × GF(2^8) in parallel |
| 8 | D→H copy (PCIe 3.0 x16) | ~600 µs | 8MB parity ÷ 13 GB/s |
| 9 | Free device memory | ~50 µs | cudaFree |
| 10 | Semaphore release | ~1 µs | |

**Total GPU overhead: ~2,072 µs (~2.1 ms)**
CPU ISA-L equivalent (same 64 stripes): ~800 µs per spec break-even table.

**Winner: CPU.** The 16MB batch is well below the 100MB `min_segment_size` threshold — correctly filtered out by `should_use_gpu()`. But if the threshold were lowered to 16MB, the GPU would be **2.5× slower** than ISA-L.

**Notes on missing spec features:**
- No pinned memory pool (`GpuBufferPool` from spec §9.5.3). The H→D and D→H transfers use non-pinned host buffers, requiring an internal driver copy (double the effective transfer time).
- No CUDA streams for overlap. The implementation does `stream_synchronize` implicitly through `cudarc`'s synchronous API.
- `CudaBackend::encode()` calls `try_acquire()` (non-blocking) and returns an error if no permit — the caller must handle fallback. This is correct per ADR-0006.

---

## ISA-L FFI Audit

### Table Caching

| Aspect | Status | Details |
|---|---|---|
| Tables constructed once | COMPLIANT | `IsalTables::new(k, m)` precomputes encoding tables via `ec_init_tables` at startup |
| Tables cached per (k,m) | PARTIAL | Only one `IsalTables` instance created with default k=4, m=2. If multiple (k,m) pairs are needed, tables are rebuilt per-encode (see `isal.rs:247-260`) |
| Lifetime management | ACCEPTABLE | `Box::leak` for `'static` — negligible memory (~few KB) |
| Cross-backend compatibility | COMPLIANT | Same Cauchy matrix construction as `CauchyEncoder`; interop tested in `isal.rs:878-940` |

### Data Copies

| Step | Copies | Bytes (per stripe) |
|---|---|---|
| Build Cauchy matrix | 1 | k × m bytes |
| `ec_init_tables` (FFI) | 1 | 32 × k × m bytes |
| Encode: assemble pointer array | 0 (pointer only) | — |
| Allocate parity buffers | 1 | m × shard_size bytes |
| `ec_encode_data` (FFI) | 0 (in-place write to parity) | — |
| Return parity: move `Vec<Vec<u8>>` | 0 (ownership transfer) | — |

**Total copies per encode: 1 allocation (parity output).** The data shards are passed as `&[&[u8]]` — zero copies for input. This is optimal for the ISA-L path.

### Safety

All 5 `unsafe` blocks in `isal.rs` have `// SAFETY:` comments:
- Line 130: `ec_init_tables` — verified buffer sizes, k/m in range
- Line 258: `ec_init_tables` (temporary tables) — same invariants
- Line 278: `ec_encode_data` (encode) — verified pointers, sizes, thread-safety
- Line 414: `ec_init_tables` (decode tables) — verified sizes
- Line 430: `ec_encode_data` (decode) — verified pointers, sizes

---

## Top 5 Bottlenecks

| Rank | Bottleneck | Impact | Fix Complexity |
|---|---|---|---|
| 1 | **No SIMD in Tier 0 GF(2^8) path** — log/exp table lookup only, no vectorization. 524K GF ops per stripe on a scalar path. | ~20× slower than SIMD path | Medium — implement split-table approach in `gf.rs` using `#[cfg(target_feature)]` |
| 2 | **`Vec<Vec<u8>>` allocation per stripe** — 62 allocations per 4MB segment (8.3 MB allocated). Returns owned vectors, no pooling. | ~30% of encode time in allocator | Medium — switch to `Bytes` return types, add parity buffer pool |
| 3 | **`dyn Trait` dispatch per encode/decode** — vtable lookup + indirect call on the hot path. | ~5-10 ns per call, but prevents inlining optimizations | High — requires interface redesign (enum-based dispatch or generic dispatcher) |
| 4 | **Per-decode matrix inversion (no caching)** — Gauss-Jordan repeated for every stripe decode. | k³ GF ops per stripe, repeated 16× | Low — memoize by (k,m,missing_indices_bitmask) |
| 5 | **Duplicate GF code in three crates** — maintenance hazard and potential for drift. | Zero runtime impact, but correctness risk | Low — delete duplicates, re-export from canonical `oceanfs-ec::gf` |

---

## Recommendations (Prioritized)

1. **Implement split-table GF(2^8) SIMD in `oceanfs-ec::gf`** (addresses C2, H1, H2)
   - The algorithm already exists in `arm_sve.rs` for NEON/SVE — port to x86 SSE2/AVX2
   - Use `#[cfg(target_feature = "avx2")]` with `_mm256_xor_si256` + table lookups
   - Expected improvement: 15-20× on GF arithmetic (portable path goes from ~42ms to ~2ms per 4MB segment)

2. **Replace `Vec<Vec<u8>>` return types with `Bytes`/`BytesMut`** (addresses C2, M2)
   - Change `Encoder::encode()` to return `Vec<Bytes>` or write into a pre-allocated `&mut [BytesMut]`
   - Add a per-shard-group `BytesMut` pool for parity output
   - Expected improvement: eliminates ~30% of encode overhead (allocation + copy)

3. **Replace `dyn Encoder`/`dyn Decoder` with static dispatch** (addresses C1)
   - Option A: `enum EncoderBackend { Cpu(CpuEncoder), Isal(IsalEncoder), Cuda(CudaBackend) }` with a `match` at the call site
   - Option B: `ParallelEncoder<Cpu, Isal, Cuda>` generic over backends
   - Expected improvement: eliminates vtable dispatch, enables cross-backend inlining

4. **Cache decoded matrices by `(k, m, missing_indices_bitmask)`** (addresses H3)
   - Precompute inverses for common single-shard-loss patterns at startup
   - Use a bounded LRU cache for uncommon patterns
   - Expected improvement: eliminates O(k³) GF ops per stripe decode (saves ~20% of decode time)

5. **Fix CUDA probing to actually check for GPU** (addresses H4)
   - Call `cudarc::init()` in `probe_cuda()` and verify `device_count > 0`
   - Ensures tier resolution is accurate at startup

6. **Consolidate GF code** (addresses H5)
   - Delete duplicate `GF_LOG`/`GF_EXP` arrays in `arm_sve.rs` and `cuda/mod.rs`
   - Use `oceanfs_ec::gf::gf_mul` and `oceanfs_ec::gf::gf_inv` for table construction
   - Build split-tables from the canonical GF implementation

7. **Add `#[repr(align(64))]` to `AccelMetrics`** (addresses M1)
   - Or wrap each counter in `CachePadded<AtomicU64>`

8. **Implement pinned memory pool for GPU transfers** (addresses spec §9.5.3 gap)
   - Create `GpuBufferPool` with pre-allocated `cudaMallocHost` buffers
   - Recycle pinned buffers to avoid per-operation allocation overhead

9. **Add criterion benchmarks** for EC encode/decode (addresses rule §11.4)
   - `benches/ec_benchmark.rs`: compare CPU SIMD, ISA-L, CUDA backends at 4MB, 64MB, 256MB segment sizes

10. **Add streaming multi-chunk hash test** (addresses L3)
    - Test `Blake3BatchHasher` with rayon parallelism and verify correctness
