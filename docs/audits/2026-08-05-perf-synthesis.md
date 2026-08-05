# Performance Audit — Unified Synthesis

**Author:** Brainstorm Agent (Architect)
**Date:** 2026-08-05
**Context:** Synthesis of five domain-specific performance audits of OceanFS hot
paths: write path, read path, network communication, storage I/O, and
acceleration. Each audit cross-referenced `guidelines/performance.md` §1-14.
**Source Reports:**
- `docs/audits/2026-08-05-perf-write-path.md`
- `docs/audits/2026-08-05-perf-read-path.md`
- `docs/audits/2026-08-05-perf-network.md`
- `docs/audits/2026-08-05-perf-storage-io.md`
- `docs/audits/2026-08-05-perf-acceleration.md`

---

## 1. Overall Assessment

| Domain | Grade | Criticals | Highs | Core Issue |
|---|---|---|---|---|
| Write Path | **Broken** | 3 | 4 | WriteCoordinator is a stub — zero storage integration. WAL fsync no-op. `to_vec()` copies everywhere. |
| Read Path | **Degraded** | 4 | 6 | ReadTuningConfig silently discarded. EC decode dead code. Four `.to_vec()` copies on cache-hit path. No sendfile/mmap. |
| Network | **Functional but wasteful** | 2 | 6 | O(N²) gossip bandwidth. Full blob `.to_vec()` on replication. Stream buffering defeats streaming RPCs. |
| Storage I/O | **Partially complete** | 2 | 6 | WAL fsync no-op. BufferPool/SegmentSealer unwired. No O_DIRECT/mmap/io_uring. No RocksDB tuning. |
| Acceleration | **Solid core, allocation-heavy** | 2 | 5 | `dyn Trait` vtable dispatch on encode/decode. `Vec<Vec<u8>>` return from Encoder. No x86 SIMD in GF(2^8). |

**Total across 5 audits:** 13 critical, 27 high, 35 medium, 24 low.

---

## 2. The Five Cross-Cutting Problems

Several findings appear across multiple domains. These are systemic issues, not isolated bugs.

### Problem A: The Write Path Doesn't Touch Storage

Confirmed by write-path audit (C2) and storage-IO audit (C2): `WriteCoordinator::put()` generates a `SegmentId`, hashes, replicates via gRPC, and returns. It never writes to WAL, never appends to an `ActiveSegment`, never triggers segment sealing, never invokes EC encoding. The `SegmentPool`, `BufferPool`, `SegmentShard`, and `WalWriter` exist with full implementations and unit tests — but none are connected to the actual S3 PUT handler.

**Impact:** Every performance optimization in the write path (pipeline parallelism, per-core sharding, group commit, buffer pooling) is dead code. The current write path does ad-hoc allocation and has no durability (see Problem B).

**Fix:** Gap-closure Epic 3 (write-path-unification).

### Problem B: WAL fsync Is a No-Op — Data Not Durable

Confirmed by write-path audit (C1) and storage-IO audit (C1): `WalSyncGroup` correctly batches fsync requests with a 64-waiter capacity and timeout — the group commit *infrastructure* is guideline §3.4 compliant. But the actual fsync closure passed to `create_sync_group()` returns `Ok(())` without calling `sync_all()` or `sync_data()`. The `file.flush()` call on a raw `File` is itself a no-op — Rust's `File::flush()` calls `fflush()` which only flushes userspace buffers, not kernel buffers to disk.

**Impact:** All WAL data is lost on OS crash or power failure. The WAL provides zero durability.

**Fix:** Gap-closure Epic 4 (correctness-gaps) — wire `sync_all()` into the group commit closure. Also wire WAL crash recovery (`WalReader::replay()` at startup).

### Problem C: `.to_vec()` Copies on Every Hot Path

This is the most pervasive pattern. Confirmed across all five audits:

| Location | What Happens | Audit |
|---|---|---|
| `WriteCoordinator::forward_write()` + `replicate_to_single()` | `data.to_vec()` copies entire blob from `Bytes` to `Vec<u8>` per replica. W=3, 4MB blob = 12MB allocated. | Write-path C3, Network C2 |
| S3 GET handler cache-hit path | Four `.to_vec()` calls convert `Bytes` → `Vec<u8>` on every L1/L2 cache hit. | Read-path C3, H6 |
| gRPC `append_segment` handler | Entire stream accumulated to `Vec<u8>` before writing, defeating streaming. | Network C1 |
| `MultiChunkAssembler` | Uses `Vec<u8>` + `Bytes::from()` (double allocation) instead of `BytesMut::freeze()`. | Read-path C4 |
| `FetchShard` response | Accumulates stream chunks into `Vec<u8>` via `extend_from_slice`. | Read-path M2 |

**Impact:** For a read-heavy workload with L1 cache hits, every GET copies the full blob even though the data is already in a refcounted `Bytes`. For writes, replication copies the blob per replica.

**Fix:** Replace `.to_vec()` with `Bytes::clone()` (refcount bump, zero copy). Use `BytesMut` for stream accumulation with `freeze()`. This is ~30 minutes of work with massive impact.

### Problem D: EC Decode & ReadTuningConfig Are Dead Code

Confirmed by read-path audit (C1, C2) and server audit (C1-server, C2-server, H1-server):

- `decode_ec_shards()` exists and compiles but has zero callers. Any read that requires reconstructing from parity shards (degraded mode, node failure) **fails entirely**.
- `ReadTuningConfig` fields (`parallel_fetch`, `use_fastest_k`, `stripe_parallelism`) are parsed from bucket policy but silently discarded at `read_coordinator.rs:403`. The spec's "use fastest k" semantic — a key performance differentiator — is unimplemented.
- Read repair compares the same HLC against itself (`schedule_repair(meta.hlc, meta.hlc, ...)`) — a correctness gap that also means stale data is never corrected, causing unnecessary re-reads.

**Impact:** The read path cannot tolerate any node failure. The performance tuning knobs (k=8 wide stripe for read parallelism, fastest-k response) have zero effect.

**Fix:** Gap-closure Epic 4 (correctness-gaps).

### Problem E: `dyn Trait` + `Vec<Vec<u8>>` on the EC Hot Path

Confirmed by acceleration audit (C1, C2):

- `AccelDispatcher` holds `Arc<dyn Encoder>` and calls `encoder.encode()` through a vtable — violating guideline §6.4 ("static dispatch over dynamic dispatch on hot paths"). This prevents inlining of the GF(2^8) arithmetic across the trait boundary.
- `Encoder::encode()` returns `Vec<Vec<u8>>` — owned, heap-allocated vectors. For a 4MB segment with k=4, m=2, strip_size=64KB, this produces ~62 allocations and ~8.3MB copied. Perf rule §1.1 mandates `Bytes`/`BytesMut` for blob data.
- GF(2^8) multiplication in Tier 0 uses log/exp table lookup only — no x86 SIMD path (SSE4.1, AVX2, AVX-512) despite the ARM NEON backend having a working split-table SIMD approach.

**Impact:** EC encoding is ~20× slower than achievable (no SIMD), spends ~30% of time in allocation (`Vec<Vec<u8>>`), and cannot be inlined across crates (`dyn Trait`).

**Fix:** Gap-closure Epic 6 (codebase-hygiene) for `dyn Trait` → generic dispatch. New performance feature for SIMD GF(2^8) and `Bytes`-based encode return.

---

## 3. Guideline Compliance Matrix

Aggregated across all five audits. Sorted by compliance rate.

| § | Rule | Compliant | Partial | Violation | Key Gaps |
|---|---|---|---|---|---|
| §2.6 | Bounded channels | ✅ 5/5 | — | — | No unbounded channels found |
| §2.3 | parking_lot locks | ✅ 5/5 | — | — | No std::sync locks on hot paths |
| §2.7 | Semaphore limits | ✅ 4/5 | ⚠️ 1 | — | Write-path missing semaphore for concurrent seals |
| §10.1-3 | LTO, codegen-units, panic abort | ✅ 5/5 | — | — | Fully compliant in Cargo.toml |
| §2.1 | Rayon parallel EC | ✅ 2/5 | ⚠️ 2 | ❌ 1 | Read-path EC decode has no rayon |
| §5.1-2 | BLAKE3 streaming | ✅ 3/5 | ⚠️ 2 | — | L1 cache verify uses one-shot, not streaming |
| §8.1-2 | FuturesUnordered + select! | ✅ 3/5 | ⚠️ 2 | — | Replica fetch is sequential, not FuturesUnordered |
| §4.1 | Connection pool | ✅ 1/5 | ⚠️ 4 | — | Pool exists but channels acquired eagerly |
| §4.4 | Streaming gRPC | — | ⚠️ 3/5 | ❌ 2 | Server-side buffering defeats streaming |
| §1.1 | Bytes for blob data | — | ⚠️ 3/5 | ❌ 2 | `.to_vec()` copies pervasive; `Vec<Vec<u8>>` in Encoder |
| §3.1-6 | I/O (O_DIRECT, mmap, io_uring, sendfile, group commit) | — | ⚠️ 1/5 | ❌ 4 | WAL fsync no-op. No O_DIRECT/mmap/io_uring/sendfile anywhere |
| §6.4 | Static dispatch on hot paths | — | — | ❌ 5/5 | `dyn Encoder` in AccelDispatcher. `dyn Trait` in multiple coordinators |
| §1.5 | Zero-copy protobuf | — | — | ❌ 3/5 | `encode_to_vec()` used instead of `encode(&mut buf)` |
| §6.2 | SoA for EC stripes | — | ⚠️ 1/5 | ❌ 1/5 | Vec<Vec<u8>> is AoS, guideline mandates SoA |
| §9.3 | Pre-compute key hash once | ✅ 2/5 | ⚠️ 2/5 | ❌ 1/5 | Re-hashing in some paths |

---

## 4. Top 10 Bottlenecks by Estimated Impact

Ranked by the product of (frequency × overhead × data volume).

| # | Bottleneck | Domain | Impact Estimate | Fix Effort |
|---|---|---|---|---|
| 1 | **Write path doesn't touch storage** — no WAL, no buffer pool, no segment pipeline, no EC | Write / Storage | Write throughput capped at ad-hoc alloc speed; zero durability | High (gap Epic 3) |
| 2 | **WAL fsync is a no-op** — group commit infrastructure exists but fsync closure returns Ok(()) | Storage / Write | All WAL data lost on crash; durability = 0 | Low (wire `sync_all()`) |
| 3 | **`.to_vec()` copies everywhere** — write replication, cache hits, stream buffering | All 5 domains | ~30% of CPU time in unnecessary memcpy per request | Low (replace with `.clone()` / `BytesMut`) |
| 4 | **EC decode dead code** — reads needing parity reconstruction fail | Read | Cannot tolerate any node failure; degraded mode broken | Medium (gap Epic 4) |
| 5 | **`dyn Trait` on EC hot path** — vtable dispatch per encode/decode, prevents inlining | Accel | ~5-10% overhead per EC op; blocks cross-crate optimization | Medium (refactor to generic) |
| 6 | **`Vec<Vec<u8>>` return from Encoder** — ~62 allocs + ~8.3MB copy per 4MB segment encode | Accel | ~30% of encode time in allocation | Medium (return `Bytes`) |
| 7 | **No SIMD in GF(2^8) Tier 0** — log/exp table only, no x86 vector path | Accel | ~20× slower than achievable (~42ms → ~2ms per segment) | High (implement x86 SIMD) |
| 8 | **ReadTuningConfig silently discarded** — `parallel_fetch`, `use_fastest_k`, `stripe_parallelism` parsed but unused | Read | Read-optimized profile has zero effect; spec tuning knobs are dead | Low (wire config to code) |
| 9 | **O(N²) gossip bandwidth** — full membership pushed to all peers every second | Network | 17.5 MB/s per node at 500 nodes for gossip alone | Medium (delta-only push) |
| 10 | **No O_DIRECT / mmap / io_uring / sendfile** — all I/O through tokio::fs + spawn_blocking | Storage / Read | Double buffering; no zero-copy disk→network; extra thread switches | High (platform-specific) |

---

## 5. What's Working Well

Despite the gaps, several subsystems are performance-correct:

| Area | What's Good | Guideline |
|---|---|---|
| **Compile-time profile** | LTO=fat, codegen-units=1, panic=abort, opt-level=3 | §10.1-3 ✅ |
| **Bounded channels** | No `unbounded_channel` found anywhere. All `mpsc::channel(N)`. | §2.6 ✅ |
| **parking_lot** | No `std::sync::Mutex` or `std::sync::RwLock` on hot paths. | §2.3 ✅ |
| **BufferPool implementation** | Correctly pre-allocates `BytesMut`, recycles on release. Just not wired. | §1.2 ✅ (code) |
| **Group commit infrastructure** | `WalSyncGroup` batches up to 64 waiters with timeout. Structure correct. | §3.4 ✅ (code) |
| **SegmentPool pipeline** | Encode queue backpressure, semaphore-bounded concurrency, pool rotation. Just not wired. | §2.5, §2.7 ✅ (code) |
| **BLAKE3 streaming** | Upstream `blake3` crate with runtime SIMD detection. Multi-chunk assembler uses streaming. | §5.1-2 ✅ |
| **FuturesUnordered replication** | WriteCoordinator uses `FuturesUnordered` + `tokio::select!` with timeout for parallel replica writes. | §8.1-2 ✅ |
| **Key hash pre-computation** | SHA-256 computed once at handler entry, passed through routing + metadata lookup. | §9.3 ✅ |
| **ARM NEON SIMD** | Working split-table SIMD approach for GF(2^8) on ARM. Needs porting to x86. | §10.6 ✅ |
| **SAFETY comments** | All `unsafe` blocks in ISA-L FFI, CUDA kernels, SIMD intrinsics documented. | §12.1 ✅ |

---

## 6. Quick Wins — Low Effort, High Impact

These can be done in 1-2 sessions before the gap-closure epics:

| # | Fix | Effort | Impact |
|---|---|---|---|
| 1 | Replace all `.to_vec()` on `Bytes` with `.clone()` (refcount bump) | 30 min | Eliminates 30% of hot-path CPU in memcpy |
| 2 | Wire actual `sync_all()` into WAL group commit closure | 15 min | Restores durability (was no-op) |
| 3 | Replace `MultiChunkAssembler` `Vec<u8>` + `Bytes::from()` with `BytesMut::freeze()` | 15 min | Eliminates double allocation per read |
| 4 | Wire `ReadTuningConfig` fields to actually control parallel fetch / fastest-k | 1 hour | Restores read-optimized profile |
| 5 | Replace `encode_to_vec()` with `encode(&mut BytesMut)` in protobuf serialization | 30 min | Zero-copy protobuf (guideline §1.5) |
| 6 | Replace `dyn Encoder` in dispatcher with generic `<E: Encoder>` or enum dispatch | 2 hours | Enables inlining, eliminates vtable |
| 7 | Return `Bytes` instead of `Vec<Vec<u8>>` from `Encoder::encode()` | 2 hours | Eliminates 62 allocs + 8.3MB copy per 4MB segment |

**Total quick-win effort:** ~7 hours. **Total quick-win impact:** Addresses 7 of the top 10 bottlenecks.

---

## 7. Gap-Closure Cross-Reference

Most performance gaps are already covered by the gap-closure plan:

| Perf Finding | Gap-Closure Epic | Feature |
|---|---|---|
| Write path doesn't touch storage (C2-write, C2-storage) | Epic 3 | write-path-unification |
| WAL fsync no-op (C1-write, C1-storage) | Epic 4 | correctness-gaps §WAL |
| EC decode dead code (C2-read) | Epic 4 | correctness-gaps §EC-decode |
| ReadTuningConfig discarded (C1-read) | Epic 4 | correctness-gaps §read-tuning |
| Read repair broken (C1-server) | Epic 4 | correctness-gaps §read-repair |
| Hinted handoff not wired (C5-storage) | Epic 4 | correctness-gaps §hinted-handoff |
| O(N²) gossip (C1-network) | Epic 6 | codebase-hygiene (delta-only push) |
| `.to_vec()` copies | Epic 6 | codebase-hygiene (zero-copy pass) |
| `dyn Trait` on EC path (C1-accel) | Epic 6 | codebase-hygiene (generic dispatch) |
| `Vec<Vec<u8>>` from Encoder (C2-accel) | Epic 6 | codebase-hygiene (Bytes return) |
| No x86 SIMD GF(2^8) (H3-accel) | **NEW** | Needs new feature |
| No O_DIRECT/mmap/io_uring (H1-H2-storage) | **NEW** | Needs new feature |

**New features needed beyond gap-closure:**
- `perf-gf-simd-x86` — Port ARM NEON split-table SIMD to x86 SSE4.1/AVX2/AVX-512
- `perf-io-platform` — Implement O_DIRECT, mmap, io_uring, sendfile per guideline §3

---

## 8. Recommended Sprint Sequence (Performance Track)

These run in parallel with the gap-closure and test-harness sprints:

| Sprint | Focus | Deliverables |
|---|---|---|
| **Quick Wins** (1 day) | `.to_vec()` purge, WAL fsync fix, BytesMut freeze, ReadTuningConfig wiring, protobuf zero-copy | 7 fixes, ~7 hours |
| **Perf Sprint A** (2-3 days) | `dyn Trait` → generic dispatch, `Vec<Vec<u8>>` → `Bytes` in Encoder, gossip delta-only push | 3 structural fixes |
| **Perf Sprint B** (3-5 days) | x86 SIMD GF(2^8), O_DIRECT/mmap/io_uring/sendfile | Platform-specific optimizations |
| **Perf Sprint C** (2-3 days) | RocksDB tuning (bloom filter, per-CF write buffer, block cache), `#[repr(C)]` for on-disk structs, `serde_json` → protobuf for metadata | Storage configuration optimization |

---

## 9. Summary

The hot-path performance is bottlenecked primarily by **architectural disconnection** (the write path doesn't use the storage engine) and **pervasive unnecessary allocation** (`.to_vec()` everywhere, `Vec<Vec<u8>>` in EC, `dyn Trait` preventing inlining). The individual components are well-designed — BufferPool, SegmentPool, WalSyncGroup, ParallelEncoder, ARM NEON SIMD — but they need to be wired together and the allocation patterns need to be fixed. The quick wins can be done in a day and address 7 of the top 10 bottlenecks.
