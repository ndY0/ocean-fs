---
audit_date: 2026-08-05
scope: targeted
target_crates: oceanfs-server (write path), oceanfs-storage (wal, segment, buffer_pool), oceanfs-ec
severity_counts:
  critical: 3
  high: 4
  medium: 6
  low: 4
---

# Audit Report: Write Path Performance

## Summary

The OceanFS write path demonstrates sound architectural intent — a buffer pool, segment sharding, segment pool pipeline, group-commit WAL, and rayon-parallel EC encoding all exist as implementation components. However, **none of these components are wired into the critical write path**. The `WriteCoordinator::put()` method creates a `SegmentId`, computes a BLAKE3 hash, replicates via gRPC, and returns — it never writes to a WAL, never appends to a segment buffer, and never triggers EC encoding. The WAL's group-commit fsync function is explicitly a no-op, making data durability depend entirely on `file.flush()` (userspace buffer flush). Every replication hop copies the full blob from `Bytes` to `Vec<u8>`. These three issues alone — missing storage integration, no-durability WAL, and per-replication copies — account for the most severe performance and correctness gaps. The EC encoder uses rayon and semaphore-bounded concurrency correctly but constructs data shards as `Vec<Vec<u8>>` with full zero-initialization, violating SoA layout and `Bytes`-backed rules.

---

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `oceanfs-storage/src/wal/writer.rs:220-229` (create_sync_group) | **WAL fsync is a no-op.** The `fsync_fn` closure passed to `WalSyncGroup::new()` contains a comment "No-op for in-memory tests; real fsync happens in append's flush()" and executes `Ok(())` — meaning no `fsync`/`fdatasync` is ever called. The `append()` method calls `file.flush()` (userspace buffer → kernel) but not `file.sync_all()`. File rotation does `sync_all()` at line 175, but during normal operation, data is never durably persisted. **This is a data-loss risk.** Violates §3.4. | Replace the no-op closure with a real `fsync_fn` that calls `file.sync_data()` or `file.sync_all()` on the current WAL file. Implement file handle sharing between `WalWriter` and the `WalSyncGroup` so the flusher task can access the current file. |
| C2 | `oceanfs-server/src/write/coordinator.rs:108-201` (WriteCoordinator::put) | **WriteCoordinator has zero integration with WAL, segment buffer, or EC encoding.** The `put()` method generates a `SegmentId`, computes a BLAKE3 hash, fans out replication, checks quorum, and returns. It never appends data to an `ActiveSegment`, never writes a WAL entry, never invokes EC encoding. The segment ID is generated but the data never enters the storage pipeline. The only persistence is the S3 handler's ad-hoc `std::fs::write` at `handlers.rs:79` (which blocks the async runtime). Violates the architectural design in `guidelines/architecture.md` §1.2 (WriteCoordinator should orchestrate the full pipeline). | Wire the `WriteCoordinator` to the `SegmentPool`, `WalWriter`, and `SegmentSealer`. The write path should be: ring lookup → append to ActiveSegment via SegmentPool → write WAL entry → replicate → await quorum → return. EC encoding should be enqueued via the `SegmentPool`'s bounded encoding channel. |
| C3 | `oceanfs-server/src/write/coordinator.rs:275` (forward_write), `replication.rs:126` (replicate_to_single) | **Full blob copy from `Bytes` to `Vec<u8>` on every replication/forward.** `forward_write()` line 275: `data: req.data.to_vec()`. `replicate_to_single()` line 126: `data: data.to_vec()`. `Bytes` provides zero-copy slicing and refcounted sharing; `to_vec()` allocates a new heap buffer and copies the entire blob payload. For a 4 MB blob replicated to 2 remotes, this is 8 MB of unnecessary allocation + copy. Violates §1.1 and §9.1. | Pass `Bytes` directly to the protobuf request or use `prost::bytes::Bytes` as the wire type for the `data` field. The `SegmentAppendRequest` protobuf should have its `data` field typed as `bytes` (not `Vec<u8>`). If protobuf serialization requires `Vec<u8>`, use `Bytes::into()` which may avoid copy if the Bytes is uniquely owned. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `oceanfs-server/src/grpc/segment_service.rs:84` (append_segment) | **Stream accumulator uses `Vec::new()` without pre-sizing.** `let mut segment_data: Vec<u8> = Vec::new()` grows incrementally as stream chunks arrive via `extend_from_slice()`. The `SegmentAppendRequest` carries `object_size` which is known from the first chunk but never used to pre-allocate. For a 4 MB blob, this causes ~12 reallocations. Violates §1.3. | Read `chunk.object_size` from the first stream message and call `segment_data.reserve(object_size as usize)` before the accumulation loop. |
| H2 | `oceanfs-storage/src/wal/writer.rs:43-51` (WalWriter fields) | **`tokio::sync::Mutex` used on all WAL fields instead of `parking_lot::Mutex`.** `file`, `file_seq`, `position`, and `global_position` are all `Mutex<std::fs::File/u64>` using tokio's async mutex. On the hot append path, this adds `.await` overhead for every lock acquisition. `parking_lot::Mutex` provides user-space synchronization ~5x faster in the uncontended case. Violates §2.3. | Replace `tokio::sync::Mutex` with `parking_lot::Mutex` for all WalWriter fields. The `append()` method is called from async context but the lock hold duration is microseconds (a single `write_all` + `flush` call). Use `parking_lot::Mutex::lock()` (blocking) which is safe on tokio for short critical sections. Alternatively, wrap in `spawn_blocking` if lock duration is uncertain. |
| H3 | `oceanfs-ec/src/stripe/parallel.rs:108-111,133` (ParallelEncoder::encode) | **EC data shards use `Vec<Vec<u8>>` with full zero-initialization instead of SoA `BytesMut`.** `vec![0u8; total_stripes * shard_size]` allocates and zero-fills large buffers. For a 4 MB segment with k=4, m=2, shard_size=64KB, this is 6 × 256 KB = 1.5 MB allocation + zero-init. Violates §1.1 (use `Bytes`/`BytesMut` for blob data) and §6.2 (SoA layout). Note: the logical layout is SoA (shards are contiguous), but the container is nested `Vec`s which adds indirection. | Replace `Vec<Vec<u8>>` with a single flat `BytesMut` allocation partitioned into k+m shard regions. Use `BytesMut::with_capacity(total_size)` and slice views (`&[u8]` references) for per-shard access. Consider `bytemuck::cast_slice` for interpreting shard bytes as `[u8; SHARD_SIZE]` arrays (§9.4). |
| H4 | `oceanfs-server/src/grpc/segment_service.rs:84,90-93` (append_segment) | **Metadata fields use `Vec::new()` without pre-sizing.** `blake3_hash`, `chunk_segment_ids`, `chunk_offsets`, `chunk_lengths` are all `Vec::new()` initialized when their sizes are bounded by the stream's chunk count (typically 1). Each `clone()` at lines 112-115 creates a new allocation. Violates §1.3. | Initialize with `Vec::with_capacity(1)` or use `SmallVec` (§1.4) since the common case is single-chunk writes. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `oceanfs-server/src/write/coordinator.rs:108-201` (put) | **No segment sharding in the write path.** `SegmentShard` exists in `oceanfs-storage/src/segment/shard.rs` with correct hashing logic but is never instantiated or called by the write coordinator. Every write goes through the same code path with no per-core parallelism. Violates §2.5. | Instantiate a `SegmentShard` (or `SegmentPool`) per tier in the composition root (`oceanfs-node`) and inject it into the write coordinator. Hash the connection ID or a per-request counter to select the shard. |
| M2 | `oceanfs-storage/src/segment/sealer.rs:95,130-131` | **SegmentSealer copies full segment data via `to_vec()` and hardcodes EC params to 0.** `active.data().to_vec()` copies the entire `BytesMut` buffer to a `Vec<u8>`. Additionally, `ec_k: 0, ec_m: 0` at lines 130-131 means EC encoding metadata is never populated. Violates zero-copy principle. | Pass the `BytesMut` by value (consume the segment via `into_buffer()`) and use it directly for the file write. Integrate EC encoding by calling `ParallelEncoder::encode()` during sealing and storing the resulting EC parameters. |
| M3 | `oceanfs-storage/src/blob_store.rs:59` (write_blob) | **`file.sync_all()` on every blob write with no batching.** Each PUT triggers a blocking `sync_all()` call, which is a disk barrier (1-10ms on NVMe). There is no group commit or write-behind for blob persistence. | Batch blob writes or use a write-behind flusher thread. Consider using `O_DIRECT` + `fsync` batching, or relying on WAL durability instead of per-blob sync. |
| M4 | `oceanfs-server/src/s3_handler/handlers.rs:79` (put_object) | **Blocking `std::fs::write` on the tokio async runtime.** The S3 handler calls `std::fs::write(&path, &body)` synchronously, blocking the async worker thread for the duration of the disk write. This is an ad-hoc persistence mechanism that bypasses both the WAL and the segment store. | Remove the ad-hoc `std::fs::write`. Persistence should happen through the WAL + segment store pipeline. If temporary disk persistence is needed, use `tokio::fs::write` or `spawn_blocking`. |
| M5 | `oceanfs-storage/src/segment/pool.rs:179` (SegmentPool::append) | **Synchronous `parking_lot::Mutex::lock()` in append path may block tokio runtime.** The `append()` method acquires `parking_lot::Mutex` locks synchronously. While lock hold times are microsecond-scale (a single `extend_from_slice` call), the documentation at line 169 acknowledges the risk and recommends `spawn_blocking` for callers. However, no caller actually wraps it in `spawn_blocking`. | Either: (a) document that callers must use `spawn_blocking`, or (b) provide an `async fn append_async()` wrapper that internally calls `spawn_blocking`. |
| M6 | `oceanfs-storage/src/segment/pool.rs:273` (enqueue_encoding) | **Hardcoded 500ms timeout for EC encoding queue backpressure.** The timeout is a magic number with no configuration. Under sustained load, segments may be deferred indefinitely with only a `tracing::warn!` log. | Make the timeout configurable via `PoolConfig`. Consider a bounded retry mechanism or an overflow-to-disk strategy for deferred encodes. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `oceanfs-server/src/write/coordinator.rs:191,306` | **`SmallVec::new()` with zero inline capacity.** Both occurrences create `smallvec::SmallVec::new()` which starts with 0 stack-allocated elements. Since the result always has exactly 1 `ChunkRef`, this causes a heap allocation for a single-element vector. | Use `smallvec::smallvec![chunk_ref]` or `SmallVec::from_buf([chunk_ref])` to keep the single element on the stack. |
| L2 | `oceanfs-server/src/write/coordinator.rs:252-258` (forward_write), `replication.rs:96-98` | **`target.to_string()` allocates on every error path.** The `ForwardFailed` error variant contains a `String` target. Since `NodeId` likely wraps a small string, consider using `Copy` or `SmallVec` for error types (§13.3). | Derive `Clone` on `NodeId` and store it directly in the error variant instead of converting to `String`. |
| L3 | `oceanfs-storage/src/wal/writer.rs:210-213` (find_latest_file) | **`.append(true)` is correct but `create(true)` is used on the fallback path.** The WAL open logic at line 212 creates a new file with `.create(true).append(true)`, but line 210 uses `.append(true)` for existing files. Both paths honor sequential-only writes (§3.1). No violation; noted for completeness. | N/A — compliant. |
| L4 | `oceanfs-server/src/s3_handler/handlers.rs:208,219,228,250` (get_object) | **`cached_data.to_vec()` on every cache hit in the read path.** The L1 cache stores `Bytes` but `to_vec()` copies to `Vec<u8>` for the HTTP response body. Consider returning `Bytes` directly in the response — axum's `Body::from(Bytes)` is zero-copy. | Use `Body::from(cached_data)` instead of `cached_data.to_vec()`. |

---

## Hot Path Trace

Step-by-step through the write path with performance annotations:

### Step 1: HTTP Handler (`handlers.rs:40-150`)
```
put_object() receives Bytes body from axum (zero-copy from HTTP body)
├── BucketId::new(&bucket)           — inline stack allocation
├── ObjectKey::new(&key)              — inline stack allocation  
├── hash_key()                        — SHA-256 compute (~1μs) ✓ §9.3
├── WriteRequest construction:
│   ├── bucket_id.clone()             — String clone (alloc)
│   ├── key.clone()                   — String clone (alloc)
│   ├── body.clone()                  — Bytes::clone (refcount, ~0 alloc) ✓ §1.1
│   └── policy: Option<Arc<...>>      — Arc clone (refcount)
├── state.write.put(req).await        → WriteCoordinator
├── [POST-WRITE] segment_store.put()  — Bytes::clone (refcount) ✓
├── [POST-WRITE] std::fs::write()     — ❌ BLOCKING disk I/O on async thread (M4)
├── [POST-WRITE] metadata.put_object()— RocksDB write (async via spawn_blocking)
└── [POST-WRITE] cache invalidation   — local + remote RPC calls
```

**Allocation count (critical path):** 2 String clones (BucketId, ObjectKey), 1 Bytes refcount increment.  
**Copies:** 0 on critical path (body is Bytes, passed by refcount).  
**Blocking I/O:** 1 × `std::fs::write` (M4).

### Step 2: WriteCoordinator (`coordinator.rs:108-201`)
```
put() receives WriteRequest
├── ring.lookup(hash_key)             — RingCache lookup (ArcSwap read, ~0 lock) ✓ §2.4
├── [non-local] forward_write()       — gRPC streaming call
│   ├── req.data.to_vec()             — ❌ FULL BLOB COPY Bytes→Vec<u8> (C3)
│   ├── blake3::hash(&req.data)       — BLAKE3 over Bytes (~10μs/MB) ✓ (redundant: hash not forwarded)
│   └── client.append_segment(stream) — gRPC unary (not streaming for single chunk)
├── [local] SegmentId::new()          — UUID generation (random)
├── [local] blake3::hash(&req.data)   — BLAKE3 hash compute ✓ §5.1
├── [local] ❌ NO WAL WRITE           — segment_id generated but no WAL entry (C2)
├── [local] ❌ NO SEGMENT APPEND      — data never enters ActiveSegment (C2)
├── replicate_write()                 → replication module
│   ├── FuturesUnordered              — ✓ parallel fan-out (§8.1)
│   ├── Vec::with_capacity()          — ✓ pre-sized (§1.3)
│   ├── tokio::select! with timeout   — ✓ bounded deadline (§8.2)
│   └── per-target: data.to_vec()     — ❌ FULL BLOB COPY per replica (C3)
├── quorum check                      — local ack counts as 1
└── return WriteResult                — SmallVec::new() (L1: heap alloc for 1 element)
```

**Allocation count (local write):** 1 UUID generation, 1 `SmallVec` heap alloc (L1), 0 BLAKE3 allocs.  
**Allocation count (per remote replica):** 1 × `data.to_vec()` (~4 MB for typical blob) + String allocs for protobuf fields.  
**Lock acquisitions:** 0 (ring_cache uses ArcSwap, membership uses DashMap internally).  
**Integration gaps:** No WAL write, no segment append, no EC encoding (C2).

### Step 3: Replication (`replication.rs:32-79`)
```
replicate_write() receives &[u8] data (already copied from Bytes!)
├── FuturesUnordered::collect()       — ✓ parallel tasks per target
├── tokio::pin!(timeout)              — ✓ stack pinning (§8.4)
├── tokio::select! { biased }         — ✓ timeout + completion race
└── per target: replicate_to_single() — sequential per-target
    ├── membership.address_of()       — DashMap lookup (~O(1), lock-free)
    ├── pool.get_channel(addr)        — ConnectionPool::acquire (parking_lot::Mutex)
    ├── channel.clone()               — tonic Channel clone (Arc refcount)
    ├── data.to_vec()                 — ❌ FULL BLOB COPY (C3)
    ├── req.bucket.to_string()        — String alloc (protobuf field)
    ├── req.key.to_string()           — String alloc (protobuf field)
    ├── segment_id.as_uuid().as_bytes().to_vec() — 16-byte Vec alloc
    ├── vec![0], vec![data.len()]     — 2 small Vec allocs for chunk metadata
    └── client.append_segment(stream) — gRPC call (streaming via tokio_stream::once)
```

**Allocation count (per replica):** 1 × `data.to_vec()` (blob size), 2 × `to_string()` (key names), 3 × small `Vec` allocs (chunk metadata).  
**Copies:** 1 × full blob (protobuf `data` field), key strings.

### Step 4: gRPC Segment Service (`segment_service.rs:78-190`)
```
append_segment() receives Streaming<SegmentAppendRequest>
├── Vec::new() segment_data           — ❌ no pre-sizing (H1)
├── while stream.message()            — accumulate chunks
│   └── segment_data.extend_from_slice() — ✓ batch copy (§9.5)
├── data_store.write_segment_data()   — ❌ dyn dispatch (§6.4 context: acceptable for DI)
├── [metadata] metadata cloning       — Vec::clone() for blake3_hash, chunk_segment_ids, etc.
├── [metadata] ObjectKey::new(&key)   — String alloc
├── [metadata] BucketId::new(&bucket) — String alloc
├── [metadata] md_store.put_object_in_bucket() — RocksDB write (spawn_blocking)
└── return SegmentAppendResponse       — protobuf serialize
```

**Allocation count:** 1 × `Vec<u8>` (growing from 0 to blob size), multiple small `Vec` clones for metadata fields.  
**Reallocations:** ~log₂(blob_size / 16) ≈ 12 for 4 MB (H1).

### Step 5: WAL Writer (when integrated) (`wal/writer.rs:89-130`)
```
append() receives WalEntry
├── entry.to_bytes()                  — Vec<u8> alloc (entry serialization) ✓ necessary
├── position.lock().await             — ❌ tokio::sync::Mutex (H2)
├── file.lock().await                 — ❌ tokio::sync::Mutex (H2)
├── file.write_all(&data)             — sequential write ✓ §3.1
├── file.flush()                      — userspace→kernel (NOT durable)
├── sync_group.submit().await         — oneshot channel registration
└── rx.await                          — wait for group commit
    └── [flusher task] fsync_fn()     — ❌ NO-OP (C1: no fsync/fdatasync called)
```

**Lock acquisitions:** 3 × tokio::sync::Mutex (position, file, global_position).  
**Allocations:** 1 × `entry.to_bytes()` (WalEntry serialization).  
**Durability:** `file.flush()` only — kernel may still lose data on power failure (C1).

### Step 6: Segment Sealer (`sealer.rs:88-151`)
```
seal() receives &mut ActiveSegment + entries
├── active.data().to_vec()            — ❌ FULL SEGMENT COPY BytesMut→Vec<u8> (M2)
├── SegmentIndex::new(entries.to_vec()) — entries clone + index build
├── Blake3Hasher::hash(&data)         — checksum compute (~10μs/MB)
├── SegmentHeader::new() → to_bytes() — Vec<u8> alloc (small)
├── index.to_bytes()                  — Vec<u8> alloc (small)
├── Vec::with_capacity(header+data+index) — ✓ pre-sized (§1.3)
├── extend_from_slice × 3             — ✓ batch copy (§9.5)
├── tokio::fs::write(&path, &file_data) — async disk write ✓
├── metadata.put_segment(meta)        — RocksDB write
│   └── ec_k: 0, ec_m: 0             — ❌ EC not integrated (M2)
└── wal.truncate(wal_pos)             — WAL space reclamation
```

**Allocation count:** 1 × full segment data copy (to_vec), 1 × entries clone, 2 × small Vec for header/index, 1 × pre-sized file buffer.  
**Copies:** Full segment data (BytesMut → Vec<u8>).  
**EC integration:** None (ec_k=0, ec_m=0).

### Step 7: EC Encoder (when integrated) (`ec/stripe/parallel.rs:97-156`)
```
ParallelEncoder::encode() receives &[u8] segment_data + EncodingPlan
├── semaphore.acquire()               — ✓ bounded concurrency (§2.7, §8.5)
├── Vec::with_capacity(k) data_shards — ✓ pre-sized
├── for _ in 0..k:
│   └── vec![0u8; total_stripes * shard_size] — ❌ zero-init alloc per shard (H3)
├── Copy loop: segment→shards         — interleaved copy (AoS→SoA conversion)
├── par_iter() over stripes           — ✓ rayon parallel (§2.1)
│   └── Vec<&[u8]> per stripe        — small Vec alloc per stripe
│       └── encoder.encode(&stripe_data, m8) — GF(2^8) matrix multiply
└── Collect parity results            — copy into pre-allocated parity shards
```

**Allocation count:** k × `vec![0u8; stripes*shard_size]`, m × `vec![0u8; stripes*shard_size]`, stripes × `Vec::with_capacity(k)` (stripe_data slices).  
**Parallelism:** ✓ rayon par_iter.  
**Semaphore:** ✓ bounded.  
**Zero-copy:** ❌ full data copy from segment_data into SoA layout (H3).

---

## Guideline Compliance Matrix

### §1: Memory & Allocation

| Rule | Status | Evidence |
|------|--------|----------|
| §1.1 — `Bytes`/`BytesMut` for blob data | **PARTIAL** | ✓ S3 handler receives `Bytes` from axum. ✓ WriteRequest carries `Bytes`. ✓ ActiveSegment uses `BytesMut`. ❌ `forward_write` and `replicate_to_single` use `data.to_vec()`. ❌ `segment_service` accumulates in `Vec<u8>`. ❌ EC encoder uses `Vec<Vec<u8>>`. |
| §1.2 — Buffer pool for segment append | **COMPLIANT** | `BufferPool` exists, pre-allocates `BytesMut`, recycles on release. `ActiveSegment::new()` acquires from pool. ✓ |
| §1.3 — Pre-sized collections | **PARTIAL** | ✓ `replicate_write` uses `Vec::with_capacity`. ✓ `SegmentSealer` uses `Vec::with_capacity`. ✓ `WalSyncGroup` uses `Vec::with_capacity(64)`. ✓ `SegmentPool` uses `Vec::with_capacity`. ❌ `segment_service` uses `Vec::new()` for accumulated data (H1). ❌ Metadata clone vectors use `Vec::new()`. |
| §1.4 — `SmallVec` for small metadata | **PARTIAL** | ✓ `WriteCoordinator` uses `SmallVec` for `chunks`. ✓ `segment_service` uses `SmallVec` for `chunks`. ❌ `SmallVec::new()` with 0 inline capacity (L1). |
| §1.5 — Zero-copy protobuf deserialization | **VIOLATION** | ❌ The `SegmentAppendRequest.data` field is typed as `Vec<u8>` in prost-generated code, requiring `to_vec()` from `Bytes`. No `Bytes` wire type configured. |
| §1.6 — Object pool for request-context structs | **NOT APPLICABLE** | No request-context pool exists. `WriteRequest` is stack-allocated per request. Pooling could reduce allocator churn but is not critical at current scale. |

### §2: Concurrency & Parallelism

| Rule | Status | Evidence |
|------|--------|----------|
| §2.1 — Rayon parallel for EC | **COMPLIANT** | `ParallelEncoder::encode()` and `ParallelDecoder::decode()` use `rayon::par_iter()`. ✓ |
| §2.2 — `DashMap` for concurrent caches | **NOT ON PATH** | RingCache uses internal data structures (likely DashMap-based). Membership likely uses DashMap. Not directly verifiable from write path code. |
| §2.3 — `parking_lot` locks everywhere | **PARTIAL** | ✓ `BufferPool` uses `parking_lot::Mutex`. ✓ `SegmentShard` uses `parking_lot::Mutex`. ✓ `SegmentPool` uses `parking_lot::Mutex`. ❌ `WalWriter` uses `tokio::sync::Mutex` for all fields (H2). |
| §2.4 — `ArcSwap` for read-mostly data | **NOT ON PATH** | RingCache wraps Ring; unable to verify internal implementation from write path. |
| §2.5 — Sharded segment buffer | **PARTIAL** | ✓ `SegmentShard` exists with correct hashing logic. ❌ Not wired into the write path (M1). `WriteCoordinator::put()` never instantiates or calls `SegmentShard`. |
| §2.6 — Bounded channels | **COMPLIANT** | ✓ `WalSyncGroup` uses `mpsc::channel(1024)`. ✓ `SegmentPool` uses `mpsc::channel(encode_queue_capacity)`. No `unbounded_channel` found. ✓ |
| §2.7 — Semaphore for concurrency limits | **COMPLIANT** | ✓ `ParallelEncoder` uses `tokio::sync::Semaphore`. ✓ `SegmentPool` uses `Arc<Semaphore>` for encode concurrency. |

### §3: I/O

| Rule | Status | Evidence |
|------|--------|----------|
| §3.1 — Sequential-only WAL writes | **COMPLIANT** | ✓ `WalWriter::find_latest_file()` opens with `.append(true)`. `rotate()` opens with `.create(true).append(true)`. No `seek` on write path. `truncate()` uses `SeekFrom::Start(position)` only for truncation (not hot path). |
| §3.2 — `O_DIRECT` for segment data | **NOT IMPLEMENTED** | No `O_DIRECT` usage found. Segment files written via `tokio::fs::write` without direct I/O flags. |
| §3.3 — `mmap` for hot segment reads | **NOT ON WRITE PATH** | Read-path concern; not audited. |
| §3.4 — Group commit for WAL fsync | **CRITICAL VIOLATION** | The `WalSyncGroup` architecture is correct (collects waiters, flushes batch), but the `fsync_fn` passed to it is a **no-op** (C1). No `fsync`/`fdatasync` is ever called on the WAL file during normal operation. |
| §3.5 — `io_uring` / `tokio-uring` | **NOT IMPLEMENTED** | No `tokio-uring` usage found. All disk I/O uses `tokio::fs` or synchronous `std::fs`. |
| §3.6 — `sendfile` / `splice` for blob responses | **NOT ON WRITE PATH** | Read-path concern; not audited. |

### §4: Networking

| Rule | Status | Evidence |
|------|--------|----------|
| §4.1 — Persistent gRPC connection pool | **COMPLIANT** | ✓ `ConnectionPool::get_channel(addr)` used in `forward_write()` and `replicate_to_single()`. Channels are pooled per peer. |
| §4.2 — HTTP/2 multiplexing | **NOT VERIFIED** | S3 handler uses axum; HTTP/2 support depends on axum server configuration (not visible from handler code). |
| §4.3 — `TCP_NODELAY` | **NOT VERIFIED** | Socket configuration not visible from write path code. |
| §4.4 — Streaming gRPC for large data | **PARTIAL** | ✓ `SegmentRpc::AppendSegment` is declared as client-streaming. ❌ Replication sends single-chunk streams (`tokio_stream::once(request)`) — effectively unary behavior with streaming overhead. |
| §4.5 — Adaptive per-operation timeouts | **COMPLIANT** | ✓ `OperationTimeouts::default().wal_write_ms` used for replication timeout. Timeout configuration exists in `oceanfs-core/src/timeouts.rs`. |

### §5: Hashing & Checksums

| Rule | Status | Evidence |
|------|--------|----------|
| §5.1 — BLAKE3 with runtime SIMD | **COMPLIANT** | ✓ Uses `blake3` crate directly. Runtime SIMD detection is automatic. |
| §5.2 — Streaming hash | **NOT APPLICABLE** | `put()` hashes the full `Bytes` in memory (`blake3::hash(&req.data)`). Streaming would only matter if data arrived in chunks before being assembled. Current architecture has full body in memory before hashing — acceptable. |
| §5.3 — Feature-gated SIMD | **NOT APPLICABLE** | BLAKE3 handles SIMD internally. |
| §5.4 — Batch verify for multi-chunk | **NOT ON WRITE PATH** | Read-path concern; not audited. |

### §6: Data Structures & Memory Layout

| Rule | Status | Evidence |
|------|--------|----------|
| §6.1 — Cache-line alignment for atomics | **NOT VERIFIED** | No `#[repr(align(64))]` found in write path atomics. |
| §6.2 — SoA layout for EC stripe data | **PARTIAL** | ✓ Logical layout is SoA (data shards are contiguous arrays). ❌ Container is `Vec<Vec<u8>>` with indirection and per-shard allocation (H3). A true SoA would be a single flat allocation with computed offsets. |
| §6.3 — `#[repr(C)]` for on-disk structures | **NOT VERIFIED** | `SegmentHeader`, `SegmentIndexEntry` have `to_bytes()` methods but internal representation not verified for `#[repr(C)]`. |
| §6.4 — Static dispatch on hot paths | **COMPLIANT** | ✓ `WriteCoordinator` uses concrete types (no `dyn`). ✓ `ParallelEncoder<E: Encoder>` uses generics for static dispatch. ✓ Replication uses concrete `SegmentRpcClient`. ❌ `SegmentGrpcService` uses `Arc<dyn SegmentDataStore>` for DI — acceptable per architecture guidelines §4.1. |
| §6.5 — `BTreeMap` for ordered access | **NOT ON PATH** | Ring routing uses hash-based lookup, not ordered. |

### §7: Locking Discipline

| Rule | Status | Evidence |
|------|--------|----------|
| §7.1 — Minimize lock hold duration | **PARTIAL** | ✓ `SegmentPool::append()` acquires lock, does `extend_from_slice` (fast), releases. ❌ `WalWriter::append()` holds two locks simultaneously (file + position) for the full write + flush duration. |
| §7.2 — `RwLock` when reads ≥ 10× | **NOT APPLICABLE** | No `RwLock` found on write path. All mutable state uses `Mutex`. |
| §7.3 — Explicit lock guard drop | **PARTIAL** | ✓ `SegmentPool::append()` at line 207: `drop(seg_guard)` before state transition. ✓ `replicate_write` at line 115: `drop(pooled)` after channel clone. ❌ `WalWriter` relies on scope-bound drop. |
| §7.4 — Lock ordering documented | **PARTIAL** | ✓ `ParallelEncoder` documents "semaphore → encoder/decoder internal state". ❌ `WalWriter` acquires `file`, `position`, and `global_position` locks in `append()` with no documented ordering. |

### §8: Async Patterns

| Rule | Status | Evidence |
|------|--------|----------|
| §8.1 — `FuturesUnordered` for parallel fetches | **COMPLIANT** | ✓ `replicate_write()` uses `FuturesUnordered` for parallel replica fan-out. |
| §8.2 — `tokio::select!` with timeout | **COMPLIANT** | ✓ `replicate_write()` uses `tokio::select! { biased }` with timeout branch. ✓ `WalSyncGroup` uses `tokio::select!` for first-waiter-or-timeout. |
| §8.3 — `spawn` vs `spawn_blocking` | **PARTIAL** | ✓ `WalSyncGroup` uses `tokio::spawn` for the flusher task. ✓ `metadata/store.rs` uses `spawn_blocking` for RocksDB. ❌ `SegmentPool::enqueue_encoding()` at line 276: `handle.block_on()` — blocks the calling thread (which is already on tokio if called from async context) with `tokio::time::timeout`. Should be `spawn` instead. |
| §8.4 — Avoid `Box::pin` on hot paths | **COMPLIANT** | ✓ `replicate_write()` uses `tokio::pin!(timeout)`. ✓ `WalSyncGroup` uses `tokio::pin!(timeout)`. No `Box::pin` found in write path. |
| §8.5 — Bounded semaphore for task concurrency | **COMPLIANT** | ✓ `ParallelEncoder` uses `Arc<Semaphore>`. ✓ `SegmentPool` uses `Arc<Semaphore>`. |

### §9: Zero-Copy / No-Allocation

| Rule | Status | Evidence |
|------|--------|----------|
| §9.1 — Accept borrowed data | **PARTIAL** | ✓ S3 handler receives `Bytes` from axum. ✓ `ActiveSegment::append()` accepts `&[u8]`. ❌ `replicate_to_single()` accepts `&[u8]` but immediately copies to `Vec` (C3). |
| §9.2 — `&str` over `String` | **PARTIAL** | ✓ `put_object()` uses `&bucket`/`&key` for BucketId/ObjectKey construction. ❌ Protobuf field construction forces `to_string()` for `bucket_id` and `object_key` fields. |
| §9.3 — Pre-compute key hash once | **COMPLIANT** | ✓ `put_object()` computes `hash_key()` at handler entry point. `WriteRequest` carries `hash_key: HashKey`. |
| §9.4 — `bytemuck` for zero-copy EC | **PARIALLY COMPLIANT** | ✓ `oceanfs-ec` exports `cast_shard_slice`, `cast_shard_slice_mut`, `ShardPod`. ❌ `ParallelEncoder` does not use these helpers; manually copies data into `Vec<Vec<u8>>` (H3). |
| §9.5 — `extend_from_slice` for batch writes | **COMPLIANT** | ✓ `SegmentSealer::seal()` uses `extend_from_slice` for header, data, index. ✓ `ActiveSegment::append()` uses `extend_from_slice`. ✓ `segment_service` uses `extend_from_slice`. |

---

## Top 5 Bottlenecks (Ranked by Impact)

| # | Bottleneck | Impact | Fix |
|---|-----------|--------|-----|
| 1 | **No WAL/segment integration in write path (C2)** | Every PUT bypasses the storage pipeline entirely. No durability, no EC protection, no segment packing. The system works as an in-memory store with ad-hoc disk writes. | Wire `WriteCoordinator` → `SegmentPool` → `WalWriter` → `SegmentSealer`. |
| 2 | **WAL fsync is a no-op (C1)** | All data written through the WAL is lost on power failure. `file.flush()` only moves data to kernel buffers; `sync_all()` on rotation is too infrequent. | Implement real `fsync_fn` that calls `sync_data()` or `sync_all()` on the current WAL file. |
| 3 | **Per-replication `Bytes→Vec<u8>` copy (C3)** | Every remote replica receives a full heap copy of the blob. For a 4 MB blob replicated to 2 nodes: 8 MB of allocation + memcpy per PUT. At 10K PUT/s, that's ~80 GB/s of unnecessary memory traffic. | Use `Bytes` as the protobuf wire type for `SegmentAppendRequest.data`. If prost requires `Vec<u8>`, use `Bytes::into()` to avoid copy when uniquely owned. |
| 4 | **SegmentService Vec<u8> accumulation without pre-sizing (H1)** | `Vec::new()` + incremental `extend_from_slice` causes ~12 reallocations for a 4 MB blob. `object_size` is available from the first stream chunk but not used. | Call `segment_data.reserve(object_size as usize)` after reading the first chunk. |
| 5 | **EC encoder full allocation with zero-init (H3)** | `vec![0u8; total_stripes * shard_size]` for k+m shards wastes memory bandwidth on zero-fill that is immediately overwritten by the copy loop. For 4 MB segments with k=4,m=2: 1.5 MB zero-init per encode. | Use `BytesMut::with_capacity()` which allocates without zero-fill, or use `Vec::with_capacity()` + `resize()` for the exact needed size without zero-init. |

---

## Allocation Hotspot List

Every allocation identified on the write path critical path (per PUT):

| Step | Allocation | Size | Type |
|------|-----------|------|------|
| S3 handler | `bucket_id.clone()` | ~bucket name length | String |
| S3 handler | `key.clone()` | ~key name length | String |
| WriteCoordinator | `SegmentId::new()` | 16 bytes (UUID) | SegmentId |
| WriteCoordinator | `SmallVec::new()` → heap for 1 element | 32 bytes | ChunkRef vec (L1) |
| **Replication × N** | `data.to_vec()` | **blob size (up to 4 MB)** | **Vec&lt;u8&gt; (C3)** |
| Replication × N | `bucket.to_string()` | ~bucket name length | String |
| Replication × N | `key.to_string()` | ~key name length | String |
| Replication × N | `segment_id.as_uuid().as_bytes().to_vec()` | 16 bytes | Vec&lt;u8&gt; |
| Replication × N | `vec![0]`, `vec![data.len()]` | 8+ bytes each | Vec&lt;u64/u32&gt; |
| SegmentService | `Vec::new()` → grows to blob size | **blob size (up to 4 MB)** | **Vec&lt;u8&gt; (H1)** |
| SegmentService | metadata Vec clones (×4) | small | Vec&lt;u8&gt; clones |
| SegmentSealer | `data.to_vec()` | segment size | Vec&lt;u8&gt; (M2) |
| SegmentSealer | `entries.to_vec()` | n × entry size | Vec&lt;SegmentIndexEntry&gt; |

**Estimated allocation volume per PUT (1 replica, 4 MB blob):** ~12 MB allocated, ~4 MB of which is zero-copy avoidable through `Bytes` integration.

---

## Lock Contention Analysis

### Lock Acquisitions on Write Path

| Lock | Type | Path | Contention Risk |
|------|------|------|-----------------|
| RingCache internal | ArcSwap (assumed) | `put()` → `ring.lookup()` | **Low** — wait-free reads per §2.4 |
| Membership internal | DashMap (assumed) | `replicate_to_single()` → `address_of()` | **Low** — sharded locking per §2.2 |
| ConnectionPool internal | Mutex (parking_lot) | `get_channel()` | **Medium** — all replications contend on pool acquire. Pool should have N channels per peer. |
| WalWriter.file | tokio::sync::Mutex | `append()` → `write_all` + `flush` | **Medium-High** — all concurrent appends serialize on file write. Group commit amortizes fsync but not `write_all`. |
| WalWriter.position | tokio::sync::Mutex | `append()` → position update | **Medium** — held simultaneously with file lock. |
| WalWriter.global_position | tokio::sync::Mutex | `append()` → global position update | **Low** — brief atomic update. |
| SegmentPool.current_index | parking_lot::Mutex | `append()` → slot selection | **Low** — microseconds, round-robin increment. |
| PoolSlot.segment | parking_lot::Mutex | `append()` → `extend_from_slice` | **Low-Medium** — per-slot contention depends on shard distribution. With N=4 shards, 25% of writes contend per slot. |
| BufferPool.free | parking_lot::Mutex | `acquire()`/`release()` | **Low** — only during segment creation/destruction, not per-write. |

### Risk Summary

The highest contention risk is on the `WalWriter.file` lock — all writes serialize on this single mutex. This is inherent to append-only WAL semantics (only one writer at a time can append). Mitigation: (1) use `parking_lot::Mutex` instead of `tokio::sync::Mutex` (H2), (2) the group commit design correctly amortizes fsync cost across appends, but the `write_all` call is still serialized. Alternative: per-core WAL shards with a global sequence number counter for ordering.

---

## Dependency Graph

The write path crosses the following crate boundaries per the DAG in `guidelines/architecture.md`:

```
oceanfs (binary)
  → oceanfs-node (composition root)
    → oceanfs-server (S3Handler, WriteCoordinator)
      → oceanfs-routing (RingCache) ✓
      → oceanfs-membership (Membership) ✓
      → oceanfs-network (ConnectionPool) ✓
      → oceanfs-cache (cache invalidation RPC) ✓
    → oceanfs-storage ← ❌ NOT WIRED (SegmentPool, WalWriter, BufferPool unused by server)
    → oceanfs-ec ← ❌ NOT WIRED (ParallelEncoder unused by server)
```

**Violation:** The DAG specifies `oceanfs-server` depends on `oceanfs-storage` and `oceanfs-ec` via traits in `oceanfs-core`. However, the current `WriteCoordinator` does not use any storage or EC types. The composition root (`oceanfs-node`) should inject these dependencies into `WriteCoordinator`, but does not.

---

## ADR Compliance

| ADR | Status | Notes |
|-----|--------|-------|
| ADR-0009 (Merkle tree moved to durability) | **COMPLIANT** | `SegmentSealer::seal()` at line 108: `merkle_root: None` with comment referencing ADR-0009. Merkle tree computation is deferred to `oceanfs-durability`. ✓ |
| ADR-0001 (segment packing) | **NOT VERIFIED** | Segment packing implies multiple blobs per segment. `WriteCoordinator::put()` creates one segment per blob with `SegmentId::new()`. No segment packing logic exists. This may be intentional for Phase 2 but contradicts the ADR. |

---

## Test Coverage

| Module | Public Symbols | Tests | Coverage Notes |
|--------|---------------|-------|----------------|
| `write::coordinator` | WriteCoordinator, WriteRequest | 9 test functions | ✓ Good coverage: local write, forwarding, quorum (met/unmet), HLC, empty ring, fan-out. |
| `write::replication` | replicate_write (pub(crate)) | 2 test functions | Low coverage: empty targets, unknown node. Missing: successful replication test, partial failure, timeout. |
| `wal::writer` | WalWriter | 3 test functions | Low coverage: append, truncate, sync. Missing: rotation, group commit integration, recovery. |
| `wal::sync` | WalSyncGroup (pub(crate)) | 2 test functions | Good coverage: batch flush, timeout. |
| `segment::buffer` | ActiveSegment (pub(crate)) | 8 test functions | Good coverage: append, offsets, full, tiers, buffer lifecycle. |
| `segment::pool` | SegmentPool (pub(crate)) | 8 test functions | Good coverage: creation, append, rotation, concurrency, backpressure. |
| `segment::shard` | SegmentShard (pub(crate)) | 4 test functions | Good coverage: routing, distribution, count validation. |
| `segment::sealer` | SegmentSealer | 4 test functions | Good coverage: conditions (full, timeout, empty), seal output. |
| `ec::stripe::parallel` | ParallelEncoder, ParallelDecoder | 5 test functions | Good coverage: roundtrip, padding, missing shards, semaphore. |
| `s3_handler::handlers` | put_object, get_object, etc. | 11 test functions | Good coverage: 200, etag, roundtrip, data match, cache cascade. |

**Gaps:** No integration test covering the full write path (S3 handler → segment append → WAL → seal → EC). The existing tests are unit tests that mock or bypass the storage layer. Cross-crate integration tests in `oceanfs-node/tests/` exist but need to be verified for write-path coverage.

---

## Recommendations

### Immediate (Critical)

1. **Wire the storage pipeline into WriteCoordinator (C2).** Inject `SegmentPool`, `WalWriter`, and `SegmentSealer` into `WriteCoordinator`. The `put()` method should: resolve shard → append to `ActiveSegment` via `SegmentPool` → write `WalEntry` → replicate → check quorum → return.
2. **Implement real WAL fsync (C1).** Replace the no-op `fsync_fn` with a function that calls `sync_data()` or `sync_all()` on the current WAL file. The flusher needs access to the file handle — consider `Arc<Mutex<File>>` shared between `WalWriter` and `WalSyncGroup`.
3. **Eliminate `Bytes→Vec<u8>` copies (C3).** Configure prost to use `bytes::Bytes` as the wire type for `SegmentAppendRequest.data`. If not possible, use `Bytes::try_into_vec()` or `Bytes::into()` when the `Bytes` is uniquely owned to avoid the copy.

### High Priority

4. **Pre-size the segment_service accumulator (H1).** Parse `object_size` from the first stream chunk and call `segment_data.reserve()`.
5. **Replace `tokio::sync::Mutex` with `parking_lot::Mutex` in WalWriter (H2).** Short critical sections are safe for blocking mutexes on tokio.
6. **Convert EC encoder to use flat `BytesMut` allocation (H3).** Pre-allocate a single `BytesMut` of `(k+m) * stripes * shard_size` bytes, compute shard offsets, and use slice views for the encode loop.

### Medium Priority

7. **Wire segment sharding into WriteCoordinator (M1).** Instantiate `SegmentShard` per tier in composition root, inject into coordinator, hash connection ID for shard selection.
8. **Integrate EC encoding into SegmentSealer (M2).** After sealing, enqueue the segment for EC encoding via the `SegmentPool`'s bounded encode channel.
9. **Remove ad-hoc `std::fs::write` from S3 handler (M4).** Persistence should flow through the storage pipeline.

### Low Priority

10. **Use `smallvec![]` macro for single-element chunks (L1).** Avoids heap allocation for single-chunk results.
11. **Make EC encode queue timeout configurable (M6).** Add to `PoolConfig`.
12. **Return `Bytes` directly from L1 cache for HTTP responses (L4).** Uses `Body::from(Bytes)` which is zero-copy.
