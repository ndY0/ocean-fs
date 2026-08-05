---
audit_date: 2026-08-05
scope: targeted
target_crates: oceanfs-server (read path), oceanfs-cache, oceanfs-ec (decode path)
severity_counts:
  critical: 4
  high: 6
  medium: 8
  low: 5
---

# Audit Report: Read Path Performance

## Summary

The OceanFS read path has a well-layered cache cascade (L1→L2→L3→ReadCoordinator) and correctly uses `FuturesUnordered` for parallel chunk fetching. However, several **critical gaps** exist: (1) the `ReadTuningConfig` fields (`parallel_fetch`, `use_fastest_k`, `stripe_parallelism`) are parsed but never applied — they are silently discarded, meaning all reads serialize chunk fetches despite the config saying otherwise; (2) `decode_ec_shards()` is dead code with no callers, so reads requiring parity reconstruction **fail unconditionally**; (3) the S3 response path converts `Bytes` to `Vec<u8>` via `.to_vec()` at **four** distinct return points, adding an unnecessary full-blob copy; (4) there is no `sendfile`/`splice`, no `O_DIRECT`, no `mmap`, no `rayon`, no `Semaphore`, and no streaming hash in the HTTP handler — all performance guidelines in §2–§9 are effectively unimplemented on the read path.

---

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `oceanfs-server/src/read/coordinator.rs:403` | `ReadTuningConfig` fields `parallel_fetch`, `use_fastest_k`, and `stripe_parallelism` are read from policy (lines 386–388) but **discarded on line 403**: `let _ = (parallel_fetch, use_fastest_k);`. The `stripe_parallelism` field is logged but never used to create a `Semaphore`. The comment says "consumed for future feature gating." This is a **spec violation**: the spec (§5.3, §8.1) states `read_parallel_fetch` controls parallel vs serial fetch and `read_use_fastest_k` controls "use fastest k" semantics. Neither is honored. All chunk fetches use `FuturesUnordered` unconditionally, which is the correct default, but there is no path that serializes when `parallel_fetch=false` and no early-termination when k-of-m arrive. | Implement the config: when `parallel_fetch=false`, use sequential fetches via a simple loop. When `use_fastest_k=true`, implement a k-of-m fetch pattern (fetch k+m, take first k). When `stripe_parallelism > 0`, wrap decode with `tokio::sync::Semaphore`. |
| C2 | `oceanfs-server/src/read/coordinator.rs:496–509` | `decode_ec_shards()` is `#[allow(dead_code)]` with comment "not yet implemented" at the shard level. It has **zero callers** (confirmed: no call-sites in `assemble_chunks` or `fetch_chunks_with_grpc`). The `SegmentReader::read_chunk` and the gRPC fetch path operate at the **chunk** level, not the **shard** level. When shards are missing (i.e., a node is down), the read path has no mechanism to fetch parity shards or invoke EC decode. Reads requiring parity reconstruction **fail**. | Refactor `assemble_chunks` to operate on shards rather than chunks. For each chunk, fetch k+m shards via `FuturesUnordered`, accept first k, and call `decode_ec_shards()` if any data shards are missing. Wire `decoder` into `ReadCoordinator` via `with_decoder()`. |
| C3 | `oceanfs-server/src/s3_handler/handlers.rs:208,219,228,250` | L1 cache-hit path converts `Bytes` to `Vec<u8>` via `.to_vec()` at four separate return points. This allocates a new `Vec<u8>` and copies the full blob payload, defeating the zero-copy semantics of `Bytes`. A 1 MB cached blob causes a 1 MB copy on every L1 hit. | Use `Body::from(cached_data)` (like line 317) instead of `cached_data.to_vec()` at all four return sites. `Bytes` implements `Into<Body>`. |
| C4 | `oceanfs-server/src/read/assembly.rs:50,142` | `MultiChunkAssembler` uses `Vec<u8>` as its internal accumulation buffer (line 50) and converts to `Bytes::from(self.buffer)` on finalize (line 142). This means the entire blob is accumulated in a `Vec<u8>` (allocating + copying each chunk via `extend_from_slice`) and then **copied again** into a `Bytes` allocation. A 4 MB blob across 4 chunks = ~8 MB of allocation/copy (4 MB Vec + 4 MB Bytes). | Replace `buffer: Vec<u8>` with `buffer: BytesMut`. `BytesMut::extend_from_slice()` appends in-place. On finalize, `buffer.freeze()` yields a `Bytes` with **zero copy**. This also aligns with guideline §1.1. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `oceanfs-server/src/s3_handler/handlers.rs:192–193` | L1 cache-hit BLAKE3 verification uses `blake3::hash(&cached_data)` — a **one-shot hash** that buffers the entire blob. This contradicts guideline §5.2 (streaming hash). For a 1 MB cached blob, this doesn't matter much, but the pattern sets a bad precedent. | Replace with streaming `blake3::Hasher` or, since L1 data is already assembled in `Bytes`, accept the one-shot as a pragmatic trade-off for cached data (add comment justifying). |
| H2 | `oceanfs-server/src/read/coordinator.rs:307` | `get_object()` uses `blake3::hash(&data)` — a one-shot hash of the fully assembled blob. For large multi-segment blobs, this buffers the entire blob in memory before hashing. This contradicts §5.2 (streaming hash). | Move hash verification into `MultiChunkAssembler` (which already does streaming) and remove the one-shot hash from `get_object()`. The assembler should always verify, even when called from `get_object()`. |
| H3 | All read-path modules | **No `sendfile`/`splice`** (§3.6), **no `O_DIRECT`** (§3.2), **no `mmap`** (§3.3), **no `io_uring`** (§3.5). The segment read path (`InMemorySegmentReader`) is entirely in-memory for testing. The gRPC fetch path reads into `Vec<u8>` then copies to `Bytes`. There is no disk I/O path to optimize yet. | When a real disk-backed `SegmentReader` is implemented, ensure it uses `O_DIRECT` for cold segments and `mmap` (via `memmap2`) when `read_cache_segments=true`. Use `sendfile` for the response body when the source is an mmap'd file. |
| H4 | `oceanfs-server/src/read/` | **No rayon usage** (§2.1). The spec and guidelines mandate parallel EC stripe decode using `rayon::par_iter()`. Since EC decode is not invoked on the read path at all (C2), there is naturally no rayon usage. | When C2 is fixed and `decode_ec_shards()` is wired, use `rayon::par_iter()` inside the decode call for multi-stripe segments. |
| H5 | `oceanfs-server/src/read/coordinator.rs:404–422` | The `fetch_chunks_with_grpc` path is gated on `self.pool.is_some() && self.membership.is_some()`. When these are absent, the local `InMemorySegmentReader` is used. However, the **gRPC path iterates replicas sequentially** (fetch.rs lines 199–254): a `for` loop over `replica_set` with no parallelism and no timeout per replica. It tries replica 1, then replica 2, etc. A slow/stuck first replica blocks the entire fetch. | Use `FuturesUnordered` per-chunk across all k+m replicas, or at minimum wrap each replica attempt in `tokio::time::timeout()`. |
| H6 | `oceanfs-cache/src/l2_metadata.rs:148–184` | The L2 metadata cache `get()` method returns `Arc<ObjectMetadata>`, which requires cloning the Arc. This is fine. But when inline data is served from L2 cache in `handlers.rs:239–251`, the inline data is extracted via `inline.clone()` which clones the `Bytes` — this is reference-counted (zero-copy), so it's efficient. However, **line 250**: `inline.clone().to_vec()` — the `.to_vec()` adds an unnecessary copy. | Same fix as C3: use `Body::from(inline.clone())`. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `oceanfs-server/src/read/coordinator.rs:283–311` | `get_object()` does **not** check any caches. It goes directly to `lookup_metadata()` (RocksDB or in-memory store). The cache cascade (L1→L2→L3) is implemented entirely in `handlers.rs`. This means any internal caller of `ReadCoordinator::get_object()` bypasses all three cache layers. The spec (§5.1) describes the read path as having caches integrated into the coordinator, not layered above it. | Consider moving L1/L2/L3 cache checks into `ReadCoordinator` itself, or at minimum document that `ReadCoordinator` is a cache-bypassing low-level API. |
| M2 | `oceanfs-server/src/read/fetch.rs:229–235` | The gRPC `FetchShard` stream accumulates data into a `Vec::new()` (line 229) then converts to `Bytes::from(data)` (line 243). This copies the shard data from Vec to Bytes. Use `BytesMut` as the accumulator: `BytesMut::new()` with `extend_from_slice()`, then `.freeze()` yields `Bytes` zero-copy. This is exactly the M4-server finding. | Replace `Vec::new()`/`extend_from_slice` with `BytesMut::new()`/`extend_from_slice`/`freeze()`. |
| M3 | `oceanfs-server/src/s3_handler/handlers.rs:186–230` | L1 cache-hit path makes **two** L2 lookups: first to get stored hash for verification (line 189–191), and then later the standard L2 check (line 233). If L1 hits but L2 doesn't have the key, the code falls through without attempting L1-only serve (line 223–228 handles this). The flow is correct but the code structure is hard to follow. | Restructure as a single decision tree: L1 hit → verify hash from L2 if available → serve or evict. Document clearly. |
| M4 | `oceanfs-server/src/read/coordinator.rs:431–442` | Read repair (`schedule_repair`) is only triggered when gRPC is enabled, but it passes `meta.hlc` for both `local_hlc` and `remote_hlc` (line 438–439) — meaning it always compares the same value against itself. This makes read repair a no-op. The comment on line 435 acknowledges this: "currently fetches from the first available replica; full multi-replica comparison requires HLC metadata in shard responses." | When multi-replica fetch is implemented, track per-replica HLCs and pass them to `schedule_repair` for comparison. |
| M5 | `oceanfs-server/src/read/coordinator.rs:186` | The EC decoder field is `#[cfg(feature = "ec")]` gated. When `ec` feature is disabled, the decoder is absent and `decode_ec_shards()` is not compiled. This is architecturally correct but means the default build cannot handle parity reads. | Ensure the `ec` feature is enabled by default in `oceanfs-server/Cargo.toml`. |
| M6 | `oceanfs-cache/src/l3_negative.rs:94` | The negative cache Bloom filter uses `std::collections::hash_map::DefaultHasher` for hashing (line 238). This is not cryptographically collision-resistant and varies per process (SipHash with random key). This means the Bloom filter cannot be shared across processes or persisted. The spec says the negative cache can be "rebuilt from metadata every hour" — using `DefaultHasher` means two nodes with the same keys will have different Bloom filter bit patterns. | Use the pre-computed `HashKey` (SHA-256) from the request context instead of re-hashing. This aligns with §9.3 (pre-compute key hash once). |
| M7 | `oceanfs-server/src/read/` | No `tokio::select!` with timeout branches (§8.2). The fetch path uses `FuturesUnordered` but with no overall timeout — it relies on individual chunk timeouts (which are also not fully wired: `_timeout_ms` is unused in `fetch_single_chunk`, line 159). | Wrap `FuturesUnordered` collection in `tokio::time::timeout()` with the configured read timeout. Wire `_timeout_ms` into per-fetch operations. |
| M8 | `oceanfs-server/src/read/fetch.rs:187` | The replica set for a chunk's segment is computed by hashing the segment ID string: `blake3::hash(chunk.segment_id.to_string().as_bytes())`. This re-hashes the segment ID on every fetch. The segment ID could be pre-hashed or the hash could be stored alongside the `ChunkRef`. | Pre-compute segment hash at metadata-write time and store it in `ChunkRef` or `SegmentMetadata`. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `oceanfs-server/src/read/assembly.rs:69` | `MultiChunkAssembler` pre-allocates `Vec::with_capacity(64 * 1024)` — 64 KB. This is a reasonable starting size for small blobs but will reallocate for any blob > 64 KB. | Use the blob's `size` field from `ObjectMetadata` to pre-allocate to exact capacity: `Vec::with_capacity(meta.size as usize)`. |
| L2 | `oceanfs-server/src/read/coordinator.rs:38–39` | `DEFAULT_READ_TIMEOUT_MS` is `#[allow(dead_code)]` — the default timeout is computed but never used. | Remove or wire into the fetch path. |
| L3 | `oceanfs-server/src/read/fetch.rs:122–126` | The `FuturesUnordered` collection uses `Vec::with_capacity(chunk_count)` for `chunk_data` (line 128) but the `futs` FuturesUnordered is created via `.collect()` without a capacity hint. | Use `FuturesUnordered::with_capacity(chunks.len())` for the futures collection. |
| L4 | `oceanfs-server/src/s3_handler/handlers.rs:303–307` | Prefetch hint is spawned as a `tokio::spawn` with an empty adjacent-key list (`&[]`), making it a no-op. The comment says "without key ordering context in GET, we pass an empty adjacent list. The engine skips." | Either wire real adjacent-key detection (range reads, sequential access detection) or remove the prefetch spawn overhead. |
| L5 | All read-path modules | `dyn Trait` usage (e.g., `Arc<dyn SegmentReader>`, `Arc<dyn MetadataOps>`, `Arc<dyn ConflictResolver>`) is extensive in `ReadCoordinator`. This is by design (architecture.md §5.4: dependency injection for testing) but does incur a vtable lookup per call on the hot path (§6.4). | Acceptable per architecture rules; no action needed unless benchmarks show the vtable cost is measurable. |

---

## Read Path Trace

Below is the step-by-step trace of a GET request through the system, annotated with performance observations.

```
GET /{bucket}/{key}
│
├─[handler] S3 handler: get_object()                         [handlers.rs:168]
│  │
│  ├─ Step 0: Hash key → HashKey (SHA-256)                   [handlers.rs:182]
│  │  ✓ Pre-computed once at entry (§9.3 compliant)
│  │
│  ├─ Step 1: L1 Object Cache check                          [handlers.rs:185-230]
│  │  │  cache: DashMap<BucketId, BucketCache>               [l1_object.rs:149]
│  │  │  ✓ DashMap (§2.2 compliant)
│  │  │  ✓ parking_lot internally in tests only (§2.3)
│  │  │  ✓ Atomic counters relaxed (§11.1 compliant)
│  │  │
│  │  ├─ HIT → BLAKE3 verify (one-shot hash)                [handlers.rs:192]
│  │  │  │  ✗ .hash(&cached_data) — not streaming (§5.2 VIOLATION, H1)
│  │  │  │  ✗ .to_vec() — copies Bytes → Vec<u8> (§9.1 VIOLATION, C3)
│  │  │  └─ Return 200 + Vec<u8> body
│  │  │
│  │  └─ MISS → continue
│  │
│  ├─ Step 2: L2 Metadata Cache check                        [handlers.rs:233-252]
│  │  │  cache: DashMap<BucketId, BucketMetadataCache>       [l2_metadata.rs:127]
│  │  │
│  │  ├─ HIT with inline_data → serve directly               [handlers.rs:239-251]
│  │  │  │  ✓ 0 I/O for inline blobs (spec compliant)
│  │  │  │  ✓ Populates L1 cache (line 247)
│  │  │  │  ✗ .to_vec() — copies Bytes → Vec<u8> (C3)
│  │  │  └─ Return 200 + Vec<u8> body
│  │  │
│  │  └─ HIT without inline → metadata cached; fall through
│  │
│  ├─ Step 3: L3 Negative Cache check                        [handlers.rs:259-268]
│  │  │  cache: BloomFilter per bucket                       [l3_negative.rs:128]
│  │  │  ✗ Uses DefaultHasher (not SHA-256 HashKey) (§9.3 VIOLATION, M6)
│  │  │
│  │  ├─ "Definitely absent" → 404
│  │  └─ "Maybe present" → continue
│  │
│  ├─ Step 4: ReadCoordinator.get()                          [coordinator.rs:327]
│  │  │
│  │  ├─ 4a: Ring lookup (replica set)                      [coordinator.rs:284]
│  │  │  │  _replica_set computed but unused
│  │  │
│  │  ├─ 4b: lookup_metadata() → MetadataOps::get_object()   [coordinator.rs:336]
│  │  │  │  ✗ No L2 cache check here — always queries store (M1)
│  │  │  │  → If inline_data present: return Bytes immediately
│  │  │  │     ✓ Inline blob served in 0–1 I/O
│  │  │  │  → Else: proceed to chunk assembly
│  │  │
│  │  ├─ 4c: assemble_chunks()                               [coordinator.rs:373]
│  │  │  │
│  │  │  ├─ Read policy: parallel_fetch, use_fastest_k       [coordinator.rs:386-388]
│  │  │  │  ✗ Values read but DISCARDED on line 403 (C1)
│  │  │  │  ✗ stripe_parallelism logged but no Semaphore    (C1)
│  │  │  │
│  │  │  ├─ fetch_chunks() or fetch_chunks_with_grpc()      [fetch.rs:43-86]
│  │  │  │  │
│  │  │  │  ├─ Inline check (redundant with above)           [fetch.rs:73]
│  │  │  │  │
│  │  │  │  ├─ fetch_all_chunks_parallel()                   [fetch.rs:92]
│  │  │  │  │  │
│  │  │  │  │  ├─ FuturesUnordered per chunk                 [fetch.rs:103]
│  │  │  │  │  │  ✓ Completion-order semantics (§8.1)
│  │  │  │  │  │  ✗ No timeout wrapping (§8.2 VIOLATION, M7)
│  │  │  │  │  │  ✗ No Semaphore bounding (§8.5 VIOLATION)
│  │  │  │  │  │
│  │  │  │  │  ├─ fetch_single_chunk()                       [fetch.rs:156]
│  │  │  │  │  │  │
│  │  │  │  │  │  ├─ [FAST PATH] SegmentReader::read_chunk   [fetch.rs:166]
│  │  │  │  │  │  │  ✓ Local in-memory read (test mode)
│  │  │  │  │  │  │  ✗ No O_DIRECT/mmap (§3.2, §3.3)
│  │  │  │  │  │  │
│  │  │  │  │  │  ├─ Ring lookup for segment replicas        [fetch.rs:187-188]
│  │  │  │  │  │  │  ✗ Re-hashes segment ID each time (M8)
│  │  │  │  │  │  │
│  │  │  │  │  │  └─ [gRPC FALLBACK] Sequential replica iter [fetch.rs:199-254]
│  │  │  │  │  │     │  ✗ Sequential (not parallel) per-chunk (H5)
│  │  │  │  │  │     │  ✗ No per-replica timeout (H5, M7)
│  │  │  │  │  │     │  ✗ Vec::new() → Vec<u8> → Bytes (M2)
│  │  │  │  │  │     │  ✗ No EC decode for missing shards (C2)
│  │  │  │  │  │     │  ✗ No shard-level fetch (chunk-level only)
│  │  │  │  │  │     └─ Return Bytes
│  │  │  │  │  │
│  │  │  │  │  └─ Collect results, order by index            [fetch.rs:128-153]
│  │  │  │  │     ✓ Pre-sized Vec<Option<Bytes>>              [fetch.rs:128]
│  │  │  │  └─ Return Vec<Bytes>
│  │  │  │
│  │  │  ├─ Read repair (if gRPC enabled)                    [coordinator.rs:431-442]
│  │  │  │  ✗ Passes same HLC for local and remote — no-op  (M4)
│  │  │  │
│  │  │  ├─ MultiChunkAssembler                              [assembly.rs:60]
│  │  │  │  │
│  │  │  │  ├─ Streaming BLAKE3: hasher.update(chunk)        [assembly.rs:108]
│  │  │  │  │  ✓ Streaming hash (§5.2 compliant)
│  │  │  │  │
│  │  │  │  ├─ buffer: Vec<u8>.extend_from_slice(chunk)      [assembly.rs:109]
│  │  │  │  │  ✓ extend_from_slice (§9.5 compliant)
│  │  │  │  │  ✗ Vec<u8> not BytesMut (§1.1 VIOLATION, C4)
│  │  │  │  │  ✗ Pre-alloc 64KB static, not blob-size aware  (L1)
│  │  │  │  │
│  │  │  │  └─ finalize(): hasher.finalize() + verify        [assembly.rs:124]
│  │  │  │     ✓ Batch verify via single hasher (§5.4 compliant)
│  │  │  │     → Bytes::from(buffer) — copies Vec → Bytes    [assembly.rs:142]
│  │  │  │       ✗ Extra allocation (C4)
│  │  │  │
│  │  │  └─ Return Bytes
│  │  │
│  │  └─ 4d: BLAKE3 one-shot hash of assembled data          [coordinator.rs:307]
│  │     ✗ Redundant with MultiChunkAssembler's streaming hash (H2)
│  │     ✗ .hash(&data) — one-shot, not streaming (§5.2)
│  │
│  └─ Return GetResult { data: Bytes, hash, ... }
│
├─[handler] Populate caches                                  [handlers.rs:289-296]
│  ├─ L1: object_cache.put(bucket, key, data.clone())
│  │  ✓ Bytes::clone() is ref-counted (zero-copy)
│  └─ L2: metadata_cache.put(bucket, key, metadata.clone())
│
└─[handler] Build HTTP response                               [handlers.rs:312-317]
   ├─ Body::from(result.data) — ✓ Bytes → Body (good)
   └─ ✗ No sendfile/splice for file-backed data (§3.6)
```

---

## Top 5 Bottlenecks

Ranked by estimated performance impact on real workloads:

| Rank | Bottleneck | Location | Impact | Fix Effort |
|------|-----------|----------|--------|------------|
| 1 | **ReadTuningConfig not applied** — `parallel_fetch`, `use_fastest_k`, `stripe_parallelism` are parsed but discarded. All reads use hard-coded behavior. | `coordinator.rs:403` | **CRITICAL**: Without `use_fastest_k`, a single slow node blocks the entire read. Without `stripe_parallelism`, decode parallelism is uncontrolled. | Medium — wire config boolean to control fetch strategy; add Semaphore. |
| 2 | **L1 cache hit copies blob to Vec<u8>** — four `.to_vec()` calls in the hottest path (cache hits). | `handlers.rs:208,219,228,250` | **CRITICAL**: Every L1 hit incurs a full-blob copy. For a 1 MB blob at 10K req/s, this is 10 GB/s of wasted memory bandwidth. | Trivial — replace `.to_vec()` with `Body::from()`. |
| 3 | **MultiChunkAssembler double-allocation** — Vec<u8> buffer + Bytes conversion copies every blob on the chunk-assembly path. | `assembly.rs:50,142` | **HIGH**: Every non-inline read (all blobs > 4KB) copies the full blob twice. | Low — `BytesMut` instead of `Vec<u8>`. |
| 4 | **EC decode dead code** — `decode_ec_shards()` has zero callers. Reads requiring parity fail. | `coordinator.rs:496-509` | **CRITICAL**: Under any node failure, reads of EC-protected data fail. | High — refactor to shard-level fetch + wire decode. |
| 5 | **gRPC fetch sequential per replica** — iterates replicas one at a time with no timeout. | `fetch.rs:199-254` | **HIGH**: A slow first replica adds its full latency to the read. No parallelism benefits from k+m distribution. | Medium — FuturesUnordered for per-replica fetch within each chunk. |

---

## Cache Efficiency Analysis

### Hit/Miss Paths

```
                       GET /bucket/key
                            │
                   ┌────────┴────────┐
                   │   L1 Cache Hit  │  → served from memory (0 I/O)
                   │   (DashMap)     │     ✗ copies Bytes→Vec<u8> (C3)
                   └────────┬────────┘
                            │ MISS
                   ┌────────┴────────┐
                   │   L2 Cache Hit  │
                   │   (DashMap)     │
                   └──┬──────────┬───┘
                      │          │
              inline_data?    chunks only?
                      │          │
              ┌───────┘          └───────┐
              │ served from memory       │ metadata cached;
              │ (0 I/O) ✗ .to_vec()      │ falls through to
              └───────┘                  │ ReadCoordinator
                            │            │
                   ┌────────┴────────┐   │
                   │   L3 Negative   │   │
                   │   (Bloom)       │   │
                   └──┬──────────┬───┘   │
                      │          │       │
                   absent?    maybe?     │
                      │          │       │
                   ┌──┘          └───┐   │
                   │ 404 (0 I/O)     │   │
                   └──┘              │   │
                            ┌────────┴───┴───┐
                            │ ReadCoordinator │
                            │  ┌───────────┐  │
                            │  │ Metadata  │  │  → RocksDB GET (1 I/O)
                            │  │ Lookup    │  │     ✗ No L2 check here (M1)
                            │  └─────┬─────┘  │
                            │        │        │
                            │  inline?→serve  │  → from memory (0 extra I/O)
                            │        │        │
                            │  chunks?        │
                            │  ┌─────┴─────┐  │
                            │  │ Fetch     │  │  → k segment reads (k I/O)
                            │  │ Chunks    │  │     or gRPC FetchShard calls
                            │  └─────┬─────┘  │
                            │        │        │
                            │  ┌─────┴─────┐  │
                            │  │ Assemble  │  │  → BLAKE3 streaming verify
                            │  │ + Verify  │  │     → Bytes::from(Vec) copy (C4)
                            │  └───────────┘  │
                            └─────────────────┘
```

### Redundant Operations
1. **Double BLAKE3**: `MultiChunkAssembler` does streaming hash + verify; then `get_object()` does a separate one-shot `blake3::hash()` (H2).
2. **Double inline check**: `fetch_chunks_inner()` (fetch.rs:73) checks `is_inline()` again even though `get_object()` already handled inline (no harm, just unnecessary branch).
3. **L1+L2 lookup then metadata lookup**: When L1 misses and L2 hits without inline data, the handler falls through to `ReadCoordinator.get()`, which calls `lookup_metadata()` again — a second metadata query for the same key.

---

## Zero-Copy Audit

Every data copy on the read path, traced from wire to client:

| Step | Location | Source | Destination | Copy? | Bytes Copied | Fix |
|------|----------|--------|-------------|-------|-------------|-----|
| 1 | HTTP body read | axum | `Bytes` | No (zero-copy from hyper) | 0 | — |
| 2 | L1 cache `put()` | `Bytes` | `DashMap` entry | No (ref-count clone) | 0 | — |
| 3 | L1 cache `get()` | `DashMap` entry | `Bytes` return | No (ref-count clone) | 0 | — |
| **4** | **L1 hit → response** | `Bytes` | `Vec<u8>` (`.to_vec()`) | **YES** | **full blob** | Use `Body::from()` (C3) |
| **5** | **L2 inline hit → response** | `Bytes` | `Vec<u8>` (`.to_vec()`) | **YES** | **full blob** | Use `Body::from()` (C3) |
| 6 | `MultiChunkAssembler` push | `Bytes` chunk | `Vec<u8>` buffer (`.extend_from_slice`) | Yes (memcpy per chunk) | chunk size | Use `BytesMut` (C4) |
| **7** | **`MultiChunkAssembler` finalize** | `Vec<u8>` buffer | `Bytes` (`.from()`) | **YES** | **full blob** | Use `BytesMut::freeze()` (C4) |
| 8 | L1 cache populate | `Bytes` | `DashMap` entry | No (ref-count clone) | 0 | — |
| 9 | HTTP response from coord. | `Bytes` | `Body` | No (zero-copy via `From`) | 0 | — |
| **10** | **gRPC FetchShard stream** | stream chunks | `Vec::new()` + `extend` | Yes | shard size | Use `BytesMut` (M2) |
| **11** | **gRPC FetchShard final** | `Vec<u8>` | `Bytes::from()` | **YES** | **shard size** | Use `BytesMut::freeze()` (M2) |

**Total copies per read (worst case, L1 miss + multi-chunk assembly):**
- Full blob: 2 copies (Vec→Bytes in assembler + redundant BLAKE3)
- Chunk-level: 1 copy per chunk (Bytes→Vec extend)
- If gRPC fetch: +1 copy per shard (stream→Vec→Bytes)

**Total copies per read (L1 cache hit, current code):**
- Full blob: 1 copy (Bytes→Vec via `.to_vec()`)

**Target (after fixes):**
- L1 hit: 0 copies
- Inline from L2: 0 copies
- Chunk assembly: 0 copies (BytesMut → freeze)
- gRPC fetch: 0 copies (BytesMut → freeze)

---

## Dependency Graph

The read path spans these crates:

```
oceanfs-server (handlers → ReadCoordinator → fetch/assembly/repair)
    ├── oceanfs-cache (ObjectCache, MetadataCache, NegativeCache)
    ├── oceanfs-core (types, proto messages)
    ├── oceanfs-routing (RingCache, hash_key)
    ├── oceanfs-network (ConnectionPool)
    ├── oceanfs-membership (Membership)
    ├── oceanfs-storage (SegmentRpcClient for gRPC)
    └── oceanfs-ec (Decoder trait, only via #[cfg(feature = "ec")])
```

All dependencies flow upward toward `oceanfs-server`, respecting the DAG constraint from `architecture.md` §1.3. No circular dependencies detected on the read path.

---

## Guideline Violations

| Guideline | Location | Violation |
|-----------|----------|-----------|
| §1.1 (Bytes/BytesMut) | `assembly.rs:50` | `Vec<u8>` buffer instead of `BytesMut` (C4) |
| §1.1 (Bytes/BytesMut) | `fetch.rs:229` | `Vec::new()` accumulator for gRPC stream (M2) |
| §1.3 (Pre-sized collections) | `fetch.rs:103` | `FuturesUnordered` created via `.collect()` without capacity hint (L3) |
| §1.3 (Pre-sized collections) | `assembly.rs:69` | Pre-alloc static 64KB instead of blob-size-aware (L1) |
| §2.1 (Rayon parallel iterators) | `read/` | No rayon usage; EC decode not invoked (H4) |
| §2.6 (Bounded channels) | `read/` | No bounded channels used (not applicable to read path directly) |
| §2.7 (Semaphore) | `read/` | `stripe_parallelism` logged but no Semaphore created (C1) |
| §3.2 (O_DIRECT) | `read/` | No disk segment read path implemented yet (H3) |
| §3.3 (mmap) | `read/` | No `mmap` for segment reads (H3) |
| §3.6 (sendfile/splice) | `handlers.rs:208,219,228,250,317` | Response body uses buffer-then-send; no sendfile (H3) |
| §5.2 (Streaming hash) | `handlers.rs:192` | L1 cache verification uses one-shot `blake3::hash()` (H1) |
| §5.2 (Streaming hash) | `coordinator.rs:307` | `get_object()` uses one-shot `blake3::hash()` (H2) |
| §5.4 (Batch verify) | `assembly.rs:108,133` | ✓ Compliant — single hasher for multi-chunk |
| §6.4 (Static dispatch) | `coordinator.rs:175-186` | ✓ Acceptable — `Arc<dyn Trait>` is architectural requirement (§5.4) |
| §7.2 (RwLock for reads) | `coordinator.rs:565` | ✓ `parking_lot::RwLock` in InMemorySegmentReader |
| §8.1 (FuturesUnordered) | `fetch.rs:103` | ✓ Used for chunk fetch; ✗ not used for per-replica fetch (H5) |
| §8.2 (tokio::select!) | `fetch.rs` | ✗ No `select!` with timeout branches (M7) |
| §8.4 (Avoid Box::pin) | `read/` | ✓ No `Box::pin` found on read path |
| §9.1 (Borrowed data) | `handlers.rs:208,219,228,250` | ✗ `.to_vec()` forces copy where `Body::from(Bytes)` would be zero-copy (C3) |
| §9.3 (Pre-compute key hash) | `handlers.rs:182` | ✓ HashKey computed once at entry |
| §9.3 (Pre-compute key hash) | `l3_negative.rs:238` | ✗ DefaultHasher instead of reusing HashKey (M6) |
| §9.5 (extend_from_slice) | `assembly.rs:109` | ✓ Uses `extend_from_slice` |
| §12.1 (SAFETY comments) | `read/` | ✓ No unsafe blocks on read path |
| §13.1 (thiserror) | `error.rs` | ✓ Uses `thiserror::Error` |

---

## ADR Compliance

| ADR | Status | Notes |
|-----|--------|-------|
| ADR-0001 (Segment packing) | Compliant | Read path handles inline + chunk-based reads per tiered sizing. |
| ADR-0006 (GPU acceleration) | N/A | GPU is EC encode/write path; not on read path. |
| ADR-0008 (Hash crate) | Compliant | Uses `blake3` crate; streaming hasher in `MultiChunkAssembler`. |
| ADR-0009 (Storage crate split) | Compliant | Read path uses `SegmentRpcClient` from `oceanfs-storage` via gRPC. |

---

## Test Coverage

| Crate | Key Symbols | Tests | Coverage |
|-------|------------|-------|----------|
| `oceanfs-server::read::coordinator` | `ReadCoordinator`, `get`, `get_object`, `assemble_chunks` | 14 tests | Well-covered: single/multi chunk, hash mismatch, inline, not-found, concurrent reads, full pipeline |
| `oceanfs-server::read::fetch` | `fetch_chunks`, `fetch_chunks_inner`, `fetch_all_chunks_parallel` | 4 tests | Basic coverage: inline, empty, segment reader, missing reader |
| `oceanfs-server::read::assembly` | `MultiChunkAssembler`, `push_chunk`, `finalize` | 6 tests | Good: single/multi chunk, hash mismatch, order error, no-verify, incomplete |
| `oceanfs-server::read::repair` | `schedule_repair`, `perform_read_repair` | 0 tests in module | **Untested**: 0 test functions. The repair module has no unit tests. |
| `oceanfs-cache::l1_object` | `ObjectCache`, `get`, `put`, `invalidate` | 12 tests | Well-covered |
| `oceanfs-cache::l2_metadata` | `MetadataCache`, `get`, `put`, `invalidate` | 8 tests | Well-covered |
| `oceanfs-cache::l3_negative` | `NegativeCache`, `contains`, `insert` | 7 tests | Well-covered |

**Gaps:**
- `oceanfs-server/src/read/repair.rs` has **zero tests**. This is a correctness risk since read-repair logic determines which replica's data is served.
- The gRPC fetch path (`fetch_chunks_with_grpc`) has no integration tests.
- No benchmarks for any read-path functions (contra §11.4).

---

## Recommendations

### Immediate (should fix before production use)

1. **Fix C3** — Replace all four `.to_vec()` calls in `handlers.rs` with `Body::from(cached_data)`. This is a one-line change per site with immediate throughput impact.
2. **Fix C4** — Replace `Vec<u8>` with `BytesMut` in `MultiChunkAssembler`. This eliminates a full-blob copy on every assembled read.
3. **Fix C1** — Wire `ReadTuningConfig` fields into the fetch strategy. At minimum, implement `stripe_parallelism` as a `Semaphore` bound on concurrent decodes, and implement `use_fastest_k` as k-of-m early termination in `FuturesUnordered`.
4. **Fix L1** — Use `ObjectMetadata.size` for `Vec::with_capacity()` in `MultiChunkAssembler::new()`.

### Short-term (next sprint)

5. **Fix C2** — Refactor the read path to support shard-level fetch + EC decode. This is the largest change but essential for durability under node failure.
6. **Fix M2** — Replace `Vec::new()` with `BytesMut` in the gRPC stream fetch accumulator.
7. **Fix H2** — Remove the redundant one-shot `blake3::hash()` from `get_object()`; rely on `MultiChunkAssembler`'s streaming verification.
8. **Fix M7** — Add `tokio::time::timeout()` wrapping around the `FuturesUnordered` collection and per-replica fetch attempts.
9. **Fix M6** — Use the pre-computed `HashKey` (SHA-256) in `NegativeCache` instead of `DefaultHasher`.
10. **Add tests** — Write unit tests for `repair.rs` and integration tests for the gRPC fetch path.

### Medium-term (performance optimization)

11. **Implement §3.2/§3.3/§3.6** — When a real disk-backed `SegmentReader` is added, use `O_DIRECT` for cold reads, `mmap` for hot reads, and `sendfile`/`splice` for HTTP responses from file-backed data.
12. **Add benchmarks** — Criterion benchmarks for `MultiChunkAssembler`, `fetch_chunks`, and end-to-end read path per §11.4.
13. **Wire prefetch** — Either implement actual sequential-access detection for `PrefetchEngine::after_get()` or remove the no-op spawn (L4).

### Long-term (architecture)

14. **Consider moving caches into ReadCoordinator** — The current split (caches in HTTP handler, coordinator unaware) means internal API callers bypass all caching. Either move cache logic into `ReadCoordinator` or clearly document `ReadCoordinator` as the cache-bypassing path.
