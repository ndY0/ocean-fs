# Performance Optimization — Epic Overview

**Epic:** `performance-optimization`
**Date:** 2026-08-05
**Updated:** 2026-08-05
**Context:** Five domain-specific performance audits on 2026-08-05 identified
**13 critical, 27 high, 35 medium, and 24 low** findings across write path,
read path, network, storage I/O, and acceleration. Many of those findings are
structural gaps already covered by the [gap-closure plan](../gap-closure/README.md).
This epic covers the **genuinely new findings** not covered by gap-closure:
platform-specific optimizations, SIMD acceleration, I/O infrastructure, RocksDB
tuning, and a set of low-effort high-impact quick wins that can ship
immediately.

An architectural brainstorm (2026-08-05) identified **five additional**
optimizations ranging from "one-line change with measurable impact" to
"requires specific hardware and changes the programming model." These are
captured as Features 5–9 below.

## Feature Summary

| # | Feature | Priority | Effort | Blocks | Blocked By |
|---|---|---|---|---|---|
| 1 | [quick-wins](#feature-1-quick-wins) | **critical** | ~7 hours | — | — |
| 2 | [x86-simd-gf-arithmetic](#feature-2-x86-simd-gf-arithmetic) | **high** | 3-5 days | 5, 8 | — |
| 3 | [platform-io-optimizations](#feature-3-platform-io-optimizations) | **high** | 3-5 days | 6, 9 | gap-closure Epic 3 |
| 4 | [rocksdb-tuning](#feature-4-rocksdb-tuning) | **high** | 2-3 days | 6 | — |
| 5 | [rust-low-level-tunings](#feature-5-rust-low-level-tunings) | **high** | ~4 hours | — | Feature 2, gap-closure Epic 6 |
| 6 | [advanced-io-optimizations](#feature-6-advanced-io-optimizations) | **high** | 3-5 days | — | Feature 3, Feature 4, gap-closure Epic 3 |
| 7 | [network-socket-tunings](#feature-7-network-socket-tunings) | **medium** | 2-3 days | — | gap-closure Epic 2 |
| 8 | [ec-encode-optimizations](#feature-8-ec-encode-optimizations) | **high** | 3-5 days | — | Feature 2 |
| 9 | [hardware-offload](#feature-9-hardware-offload) | **medium** | 5-10 days | — | Feature 3, gap-closure Epics 2, 4 |

## Dependency Graph

```
Feature 1 (quick-wins) ──────────── independent, start immediately
Feature 2 (x86-simd-gf) ─────────── independent, start anytime
Feature 4 (rocksdb-tuning) ──────── independent, start anytime
Feature 3 (platform-io) ─────────── depends on gap-closure Epic 3 (write-path-unification)
Feature 5 (rust-low-level) ──────── depends on Feature 2 (SIMD path is the real hot path
                                     for inline hints) + gap-closure Epic 6 (QW-7 return type)
Feature 6 (advanced-io) ─────────── depends on Feature 3 (I/O abstraction) + Feature 4
                                     (RocksDB for mlock) + gap-closure Epic 3
Feature 7 (network-socket) ──────── depends on gap-closure Epic 2 (multi-node connectivity)
Feature 8 (ec-encode-opts) ──────── depends on Feature 2 (SIMD dispatch framework)
Feature 9 (hardware-offload) ────── depends on Feature 3 (I/O abstraction) + gap-closure
                                     Epic 2 (gRPC stable) + gap-closure Epic 4 (WAL wired)
```

## Execution Order

1. **Feature 1** ships immediately (~7 hours). Addresses 7 of the top 10
   bottlenecks from the synthesis. Gates on nothing.
2. **Feature 2, Feature 4, Feature 5** (after gap-closure Epic 6) run in
   parallel — SIMD GF arithmetic, RocksDB tuning, and Rust low-level tunings.
   Feature 5 is small (~4 hours) and can ship in the gap between larger
   features.
3. **Feature 3** starts after gap-closure Epic 3 (write-path-unification)
   lands — the I/O paths require a wired write path to test against.
4. **Feature 6, Feature 7** run after their respective dependencies land.
   Feature 6 builds on Feature 3's I/O infrastructure; Feature 7 requires
   multi-node connectivity from gap-closure Epic 2.
5. **Feature 8** starts after Feature 2 lands — GFNI extends the SIMD
   dispatch framework.
6. **Feature 9** is a v2-class feature: requires stable gRPC data path,
   wired WAL, and I/O abstraction. Scheduled after gap-closure Epics 2-4
   and Feature 3 are complete. Individual offload paths (pmem, QAT, GDS,
   RDMA) can ship incrementally as their Cargo features.

## Relation to Gap-Closure Epics

This epic is **complementary** to [gap-closure](../gap-closure/README.md). The
gap-closure epics address structural correctness gaps (unwired write path,
no-op fsync, dead EC decode, unwired ReadTuningConfig, O(N²) gossip, codebase
hygiene). This epic addresses performance optimizations that either:

- Are genuinely new findings with no overlap (x86 SIMD, platform I/O),
- Are low-effort quick wins spanning multiple gap-closure epics (the 7 fixes
  touch code owned by gap-closure Epics 3, 4, and 6, but are implementation
  details rather than architectural changes), or
- Are configuration/tuning changes requiring no code architecture changes
  (RocksDB tuning).

### What gap-closure already covers (DO NOT DUPLICATE)

| Finding | Covered By |
|---|---|
| Write path doesn't touch storage | gap-closure Epic 3 |
| WAL fsync no-op | gap-closure Epic 4 |
| EC decode dead code | gap-closure Epic 4 |
| ReadTuningConfig silently discarded | gap-closure Epic 4 |
| Read repair broken | gap-closure Epic 4 |
| Hinted handoff not wired | gap-closure Epic 4 |
| O(N²) gossip bandwidth | gap-closure Epic 6 |
| `.to_vec()` copies (pervasive) | gap-closure Epic 6 (zero-copy pass) |
| `dyn Trait` on EC hot path | gap-closure Epic 6 (generic dispatch) |
| `Vec<Vec<u8>>` from Encoder | gap-closure Epic 6 (Bytes return) |

### What this epic covers (new)

| Finding | Feature |
|---|---|
| No x86 SIMD in GF(2^8) Tier 0 | Feature 2: x86-simd-gf-arithmetic |
| No O_DIRECT / mmap / io_uring / sendfile | Feature 3: platform-io-optimizations |
| RocksDB missing bloom filter, per-CF tuning, block cache | Feature 4: rocksdb-tuning |
| Quick wins: 7 fixes across all domains | Feature 1: quick-wins |
| High-allocation EC path; missed GF inlines; cache-miss branch penalty | Feature 5: rust-low-level-tunings |
| fsync latency dominates writes; page cache pollution; background task I/O interference | Feature 6: advanced-io-optimizations |
| gRPC socket latency (delayed ACKs, interrupt wakeup, accept queue contention) | Feature 7: network-socket-tunings |
| GF(2^8) not at theoretical minimum; runtime matrix setup; seal-time encode latency spike | Feature 8: ec-encode-optimizations |
| Optional hardware offload paths: GDS, QAT, pmem, RDMA | Feature 9: hardware-offload |

## Audit Reports Source

| Audit | File | Finding IDs |
|---|---|---|
| Write Path | [`2026-08-05-perf-write-path.md`](../../audits/2026-08-05-perf-write-path.md) | H1, H3, C3-related |
| Read Path | [`2026-08-05-perf-read-path.md`](../../audits/2026-08-05-perf-read-path.md) | C1, C2, C3, C4, M2, H3, H4 |
| Network | [`2026-08-05-perf-network.md`](../../audits/2026-08-05-perf-network.md) | C1, C2, H1, H3, H5, M1 |
| Storage I/O | [`2026-08-05-perf-storage-io.md`](../../audits/2026-08-05-perf-storage-io.md) | H1, H2, H3, H4, H5, H6 |
| Acceleration | [`2026-08-05-perf-acceleration.md`](../../audits/2026-08-05-perf-acceleration.md) | C1, C2, H1, H2, H3, H4, H5 |
| Synthesis | [`2026-08-05-perf-synthesis.md`](../../audits/2026-08-05-perf-synthesis.md) | §6 Quick Wins, §7 Gap-Closure Cross-Reference |

## Guideline Compliance Mapping

| Guideline § | Feature(s) | Description |
|---|---|---|
| §1.1 | Feature 1, 2 | `Bytes`/`BytesMut` for blob data, zero-copy |
| §1.2 | Feature 1 | Arena/buffer pool for segment append (MultiChunkAssembler) |
| §1.3 | Feature 1 | Pre-sized collections (stream accumulation) |
| §1.5 | Feature 1 | Zero-copy protobuf (`encode_to_vec` → `encode(&mut BytesMut)`) |
| §2.1 | Feature 8 | Rayon parallel iterators for EC stripe encode/decode |
| §2.7 | Feature 9 | Tokio semaphore for concurrency limits (QAT, GDS, RDMA) |
| §3.1 | Feature 6 | Sequential-only WAL writes |
| §3.2 | Feature 3 | `O_DIRECT` for segment data files |
| §3.3 | Feature 3 | `mmap` for hot segment reads |
| §3.4 | Feature 6 | Group commit for WAL fsync (optimized: `sync_file_range`+`fdatasync`) |
| §3.5 | Feature 3, 6 | `io_uring` / `tokio-uring` for disk I/O |
| §3.6 | Feature 3 | `sendfile` / `splice` for blob responses |
| §4.1 | Feature 7 | Persistent gRPC connection pool per peer |
| §4.3 | Feature 7 | `TCP_NODELAY` on all sockets |
| §4.4 | Feature 7 | Streaming gRPC for large data transfers |
| §6.4 | Feature 1, 5 | Static dispatch over dynamic dispatch on hot paths |
| §10.1 | Feature 5 | LTO in release profile (inline hints work with fat LTO) |
| §10.6 | Feature 2, 3, 6, 7, 8, 9 | Conditional platform-specific code paths with fallbacks |
| §11.1 | Feature 5 | Atomic counters on hot paths |
| §11.4 | Feature 2, 3, 6, 7, 8, 9 | Criterion benchmarks for hot-path functions |
| §12.1 | Feature 2, 8, 9 | `// SAFETY:` comments on every unsafe block |

---

## Feature 5: Rust Low-Level Tunings

**Priority:** high | **Effort:** ~4 hours | **File:** [rust-low-level-tunings/feature.md](rust-low-level-tunings/feature.md)

Three small, high-leverage Rust-specific optimizations: (1) mimalloc global
allocator replacing the system allocator to reduce allocation pressure in the
EC path (10-20% throughput gain), (2) `#[inline(always)]` on `gf_mul()` and
`gf_add()` to prevent missed inline opportunities across trait boundaries even
with LTO=fat, (3) `likely`/`unlikely` branch hints on the three-tier cache
lookup (L1 → L2 → RocksDB) where the L1-hit path is the 90%+ common case.
Zero architectural changes. Addresses the allocation avalanche identified in
the acceleration audit and the branch-predictor blind spot in the cache path.

**Blocks:** Nothing. **Blocked by:** Feature 2 (SIMD path is the real inline
target), gap-closure Epic 6 (QW-7 return type change affects the inline
context).

---

## Feature 6: Advanced I/O Optimizations

**Priority:** high | **Effort:** 3-5 days | **File:** [advanced-io-optimizations/feature.md](advanced-io-optimizations/feature.md)

Six syscall-level I/O and scheduling refinements building on Feature 3's
platform I/O infrastructure: (1) `sync_file_range` + `fdatasync` replacing
`sync_all` in the WAL group commit (2-3× faster on NVMe), (2) `O_TMPFILE` +
`linkat` for atomic segment writes (zero partial-file window), (3)
`madvise(MADV_SEQUENTIAL)` + `MADV_DONTNEED` on segment reads to prevent page
cache pollution, (4) `ioprio_set(IOPRIO_CLASS_IDLE)` for GC/scrub/anti-entropy
threads so they never compete with client I/O, (5) `SCHED_IDLE` for the same
background threads so they only run in idle CPU time, (6) `mlock` on the
RocksDB block cache as swap defense. All `#[cfg(target_os = "linux")]`-gated.

**Blocks:** Nothing. **Blocked by:** Feature 3 (I/O abstraction), Feature 4
(RocksDB for mlock), gap-closure Epic 3 (write-path-unification).

---

## Feature 7: Network Socket Tunings

**Priority:** medium | **Effort:** 2-3 days | **File:** [network-socket-tunings/feature.md](network-socket-tunings/feature.md)

Three Linux socket options applied to the gRPC data path: (1) `SO_BUSY_POLL`
for low-latency busy-wait polling instead of interrupt-driven wakeups —
eliminates ~5-10µs wakeup latency for small RPCs, (2) `TCP_QUICKACK` to
disable delayed ACKs (up to 500ms saved per RPC round-trip for independent
request-response patterns), (3) `SO_REUSEPORT` to bind N sockets to the same
port (one per tokio worker thread), eliminating accept-queue contention via
kernel 4-tuple-hash connection distribution. Applied to both server accept
and client connect sockets in `oceanfs-network`.

**Blocks:** Nothing. **Blocked by:** gap-closure Epic 2 (multi-node
connectivity must exist before socket options can be benchmarked).

---

## Feature 8: EC Encode Optimizations

**Priority:** high | **Effort:** 3-5 days | **File:** [ec-encode-optimizations/feature.md](ec-encode-optimizations/feature.md)

Three erasure-coding-specific optimizations building on Feature 2: (1) GFNI
instructions (Intel Ice Lake 2021+ / AMD Zen 4 2022+) for GF(2^8) multiply in
a single instruction (`vgf2p8affineqb`) — the holy grail at ~8.7× portable,
(2) compile-time `const` Cauchy encode matrices for common (k,m) pairs
((4,2), (6,3), (8,4), (10,6)) eliminating runtime matrix construction
(~30-100µs saved per segment encode), (3) streaming EC encode that encodes
each stripe row as soon as its k data shard bytes are available in the segment
buffer, overlapping encode with append — seal becomes a no-op for encoding
work. Extends the `GfSimdLevel` dispatch framework from Feature 2.

**Blocks:** Nothing. **Blocked by:** Feature 2 (SIMD dispatch framework
that GFNI extends).

---

## Feature 9: Hardware Offload

**Priority:** medium | **Effort:** 5-10 days | **File:** [hardware-offload/feature.md](hardware-offload/feature.md)

Four hardware-specific optimizations, each feature-gated behind a Cargo
feature so they compile to zero code on standard hardware: (1) **GPU Direct
Storage (GDS)** — DMA segment data directly from NVMe SSD to GPU VRAM (one
DMA instead of SSD→CPU→GPU), saving 50% PCIe bandwidth for CUDA EC encode
(`#[cfg(feature = "gds")]`), (2) **Intel QAT** — hardware-accelerated zstd
compression via QuickAssist Technology, implementing the `Compressor` trait
alongside nvCOMP and CPU zstd (`#[cfg(feature = "qat")]`), following the
ADR-0007 compression governance model, (3) **Persistent memory (pmem)** —
DAX-mapped WAL with `clwb`+`sfence` replacing `fsync`, eliminating the
millisecond-scale fsync bottleneck (`#[cfg(feature = "pmem")]`), (4) **RDMA
(RoCE/InfiniBand)** — one-sided RDMA write/read replacing gRPC streaming for
the data plane (`AppendSegment` and `FetchShard`), a new transport alongside
gRPC (`#[cfg(feature = "rdma")]`). This is a v2-class feature: requires
stable gRPC data path, wired WAL, and I/O abstraction before integration.

**Blocks:** Nothing. **Blocked by:** Feature 3 (I/O abstraction), gap-closure
Epic 2 (gRPC stable), gap-closure Epic 4 (WAL wired). Individual offload
paths can ship incrementally as their Cargo features.
