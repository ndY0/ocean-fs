---
audit_date: 2026-08-05
scope: targeted
target_crates: oceanfs-storage, oceanfs-core
severity_counts:
  critical: 2
  high: 6
  medium: 7
  low: 5
---

# Audit Report: Storage I/O Performance

## Summary

The storage I/O layer has a well-designed architecture that correctly models
the spec's three-tier metadata store (objects, segments, deletions CFs), an
append-only WAL with group-commit infrastructure, a buffer pool for segment
buffers, and per-core segment sharding. **However, two critical correctness
gaps make the system not durable today:** WAL fsync is a no-op (data is
never actually persisted to disk), and the BufferPool+SegmentSealer are
constructed but never wired to consumers, so the write path allocates
per-request. Beyond these, the I/O layer uses only standard `std::fs` /
`tokio::fs` — missing `O_DIRECT`, `mmap`, `io_uring`, and `sendfile`
optimizations that would be required for production throughput. The
compile-time profile is well-configured (LTO=fat, codegen-units=1,
panic=abort).

---

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `crates/oceanfs-storage/src/wal/writer.rs:220-230` (`create_sync_group`) | **WAL fsync is a no-op — data never persisted to disk.** The closure passed to `WalSyncGroup::new()` contains `Ok(())` with the comment `// No-op for in-memory tests; real fsync happens in append's flush().` But `file.flush()` (line 108) on a raw `std::fs::File` is a no-op — Rust's `File` has no userspace buffer; `flush()` only applies to `BufWriter`. The actual `fsync`/`fdatasync` syscall is never invoked. The group-commit infrastructure (§3.4) correctly collects waiters but calls an empty fsync function. This means **all data written to the WAL is lost on OS crash or power failure** — the data sits in the kernel page cache only. | Replace the no-op closure with `file.sync_all()` (or `file.sync_data()` for fdatasync). Since the sync group operates on a background task, it must hold an `Arc<File>` or channel to the file handle. Alternatively, call `file.sync_all()` directly in `append()` after `write_all` and skip the group-commit path for correctness-first implementation. |
| C2 | `crates/oceanfs-node/src/node.rs:201,210` (confirmed by prior audit H3) | **BufferPool and SegmentSealer constructed but never wired.** `_buffer_pool` and `_sealer` are created with underscore prefixes because they are unused. The write path (`route_write` in `segment/route_write.rs`) creates ad-hoc `ActiveSegment` instances from a locally-constructed `BufferPool` — not the one the node constructs. Every segment append allocates a fresh `BytesMut` buffer instead of recycling from the pool. Perf rule §1.2 (arena/buffer pool for segment append buffers) is violated despite BufferPool being implemented. | Wire the `BufferPool` from node construction into the write coordinator / active segment lifecycle. Pass it as `Arc<BufferPool>` to the segment pool and `SegmentShard`. Wire `SegmentSealer` into the active segment lifecycle for seal-on-full/seal-on-timeout. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `crates/oceanfs-storage/src/blob_store.rs:55-60`, `segment/sealer.rs:116-125` | **No `O_DIRECT` or `mmap` for segment data files (§3.2, §3.3).** `BlobStore::write_blob()` opens files with plain `File::create()`. `SegmentSealer::seal()` uses `tokio::fs::write()` which buffers the entire segment in memory and then writes through the OS page cache. Neither path sets `O_DIRECT` (to bypass page cache for large segments) nor uses `mmap` (for zero-copy reads when `read_cache_segments=true`). | For the write path: open segment files with `OpenOptions::new().write(true).create(true).custom_flags(libc::O_DIRECT)` when segment caching is disabled. For the read path: branch on `read_cache_segments` config — use `memmap2::Mmap` when enabled, `O_DIRECT` reads when disabled. Align buffers to the block device sector size (typically 512 or 4096 bytes). |
| H2 | All storage I/O paths | **No `io_uring` / `tokio-uring` for disk I/O (§3.5).** All disk I/O (WAL writes, segment reads/writes, RocksDB I/O) uses `std::fs` (synchronous blocking) wrapped in `tokio::task::spawn_blocking` or `tokio::fs`. The feature doc (`docs/features/phase-1-storage-engine/wal-write-ahead-log.md:44`) lists `tokio-uring` as planned, and the audit report (`docs/audit-report.md:37`) flags this gap. `tokio-uring` is not present in any `Cargo.toml`. | Add `tokio-uring` as an optional dependency. Feature-gate with `#[cfg(all(target_os = "linux", feature = "io-uring"))]`. Implement a `DiskIo` abstraction that selects `tokio-uring` on Linux 5.1+ and falls back to `tokio::fs` (current path) on other platforms. The highest-impact targets: WAL writes (sync bottleneck), segment reads (bulk throughput). |
| H3 | `crates/oceanfs-storage/src/wal/entry.rs:26-42` | **`WalEntry` NOT `#[repr(C)]` (§6.3).** The doc comment explicitly documents a binary layout: 72 bytes with specific field offsets. However the struct derives only `Debug, Clone, PartialEq, Eq` — no `#[repr(C)]`. Rust's default struct layout can reorder fields and insert arbitrary padding. The `to_bytes()`/`from_bytes()` implementations manually serialize fields in a specific order, which works as long as the conversion is done through those methods, but the struct itself has no guaranteed memory layout. `SegmentHeader` (in `segment/header.rs`) has the same issue — doc-commented binary layout with 76-byte size, but no `#[repr(C)]`. | Add `#[repr(C)]` to both `WalEntry` and `SegmentHeader`. This guarantees field order and padding match the documented binary layout. Consider also using `bytemuck` (§9.4) for zero-copy casting if the structs derive `Pod` and `Zeroable`. |
| H4 | `crates/oceanfs-storage/src/metadata/store.rs:49-77` | **RocksDB configuration missing performance-critical settings.** The database opens with `Options::default()` and only sets `create_if_missing`, `create_missing_column_families`, compression (`Zstd`), and block cache size. Missing: **(a) Bloom filter policy** — the `objects` CF has no bloom filter, so every GET for a non-existent key must probe all SST files. Per the spec, the negative (Bloom) cache is a separate L3 construct — but RocksDB's own bloom filter would accelerate metadata lookups before reaching the application-level negative cache. **(b) Write buffer size per CF** — only the default is used. The `objects` CF (frequent writes) should have a larger write buffer than `segments` or `deletions`. **(c) `set_max_open_files`** — no value set; RocksDB defaults to -1 (unlimited), which keeps all SST file descriptors open. For a node with thousands of SST files this can exhaust fd limits. **(d) No compaction style tuning** — `optimize_level_style_compaction` is called but only with memtable size; no universal compaction for write-heavy CFs. | **(a)** Add bloom filter to `objects` CF: `block_opts.set_bloom_filter(10.0, false)` with 10 bits per key. **(b)** Set `cf_opts.set_write_buffer_size()` per CF — larger for `objects` (64MB default, 256MB for write-heavy). **(c)** Set `opts.set_max_open_files(4096)` or derive from ulimit. **(d)** Evaluate universal compaction for `deletions` CF (append-mostly, tombstone-heavy). |
| H5 | `crates/oceanfs-storage/src/metadata/store.rs:96,119,227,304` | **Metadata serialization uses `serde_json` on hot write/read paths (§1.5).** Every `put_object`, `put_segment`, and batch write calls `serde_json::to_vec()` to serialize metadata into JSON. Every `get_object`/`get_segment` calls `serde_json::from_slice()`. JSON adds ~2-5× size overhead vs a compact binary format (protobuf or custom), and parsing JSON on every metadata read burns CPU. `SegmentIndex::to_bytes()` (in `segment/index.rs:78`) explicitly acknowledges this with a comment: "replace with a compact binary format in production." | Use protobuf for metadata serialization. `ObjectMetadata` and `SegmentMetadata` already have protobuf definitions in `oceanfs-core/proto/`. Generate Rust types with `prost` and serialize with `Message::encode()`/`Message::decode()`. This would also satisfy §1.5 (zero-copy protobuf deserialization with `Bytes`-backed types) — though metadata values are small enough that the allocation overhead of JSON may dominate more than the wire format itself. |
| H6 | `crates/oceanfs-storage/src/wal/writer.rs:39-52` | **WalWriter has 3 `tokio::sync::Mutex` fields on the same struct — potential false sharing.** `file`, `file_seq`, `position`, and `global_position` are all individually wrapped in `Mutex`. The `append()` method acquires `file.lock()` and `position.lock()` simultaneously (lines 103-104), and `rotate()` holds `file`, `seq`, and `position` simultaneously (lines 170-172). These are on the same struct, so the mutex state counters sit on adjacent cache lines. With multiple tokio tasks contending on the WAL, false sharing can bounce cache lines between cores. Perf rule §6.1 requires `#[repr(align(64))]` for mutable atomics on hot paths. | Either **(a)** consolidate the three position-related fields into a single `Mutex<(File, u64, u64)>` — reducing lock count from 4 to 2, or **(b)** add `#[repr(align(64))]` to `WalWriter` with `#[allow(unused)]` padding fields, though this only helps if the Mutex internals (which use atomics) are the hot path. The simplest fix is consolidation — the file, position, and file_seq are always modified together. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `crates/oceanfs-storage/src/segment/sealer.rs:116-125` | **`tokio::fs::write` on seal path allocates entire segment in memory.** The sealer constructs a `Vec<u8>` containing header + data + index, then writes it in one call. For a 4 MB standard segment, this is one 4 MB allocation on top of the existing `BytesMut` buffer. The data is copied from the `ActiveSegment::data()` slice into this allocation, doubling memory usage during seal. | Write the segment file in three `write_all` calls (header, data, index) to a pre-opened `File`, rather than assembling a contiguous buffer. This avoids the double-allocation and enables O_DIRECT writes (H1) since each part can be aligned independently. |
| M2 | `crates/oceanfs-storage/src/wal/writer.rs:103-121` | **No lock ordering documentation (§7.4).** `append()` acquires `file.lock()` and `position.lock()` simultaneously (lines 103-104) — or rather, sequentially since these are async mutexes. `rotate()` acquires `file`, `seq`, and `position` in that order. The ordering is consistent (file → position → global_position) but is **not documented** as a module-level comment. A future maintainer adding a code path that acquires them in a different order could introduce deadlocks. | Add a lock ordering comment at the top of `wal/writer.rs`: `// LOCK ORDER: file → position → global_position (file_seq is only held in rotate, never with position)` |
| M3 | All storage read paths | **No `sendfile` / `splice` for blob serving (§3.6).** The existing blob serving path for inline blobs reads from RocksDB and copies to the HTTP response buffer. For segment-stored blobs, data is read from `BlobStore::read_blob()` into a `Vec<u8>` and then copied to the response. There is no fd-to-socket zero-copy path (sendfile). | On Linux, detect when the response source is a file descriptor and use `tokio::io::copy` between the file and the socket, which internally uses `sendfile` when both ends support it. This requires the segment data file to remain open during the read (not opened per-request). |
| M4 | `crates/oceanfs-storage/src/metadata/store.rs:187-208` | **`list_objects` does prefix scan with manual `take_while` — no RocksDB prefix extractor.** The iterator uses `IteratorMode::From(prefix_key, Forward)` which starts at the correct key but then manually checks `key.starts_with(&prefix_key)` on every entry. This means the iterator scans all keys beyond the prefix until it finds a non-matching key. For a bucket with millions of objects, scanning past the prefix to find the boundary adds latency. | Configure RocksDB's `prefix_extractor` on the `objects` CF to use fixed-length prefix extraction (first N bytes of the key, where N = bucket prefix length + 1). This enables RocksDB's prefix bloom filter and automatically stops iteration at the prefix boundary via `SeekForPrev`. |
| M5 | `crates/oceanfs-storage/src/metadata/store.rs:406-470` | **Async wrappers use `spawn_blocking` without justification comment (§8.3).** `put_object_async`, `get_object_async`, and `delete_object_async` all wrap synchronous RocksDB calls in `tokio::task::spawn_blocking`. Per §8.3, every `spawn_blocking` must have a comment explaining why no async alternative exists. RocksDB's Rust bindings do not provide an async API (it's a C++ library), so this is justified — but the justification comment is missing. | Add a comment on each `spawn_blocking` call: `// RocksDB C++ bindings are synchronous; no async alternative exists.` |
| M6 | `crates/oceanfs-storage/src/metadata/store.rs:64-72` | **All three column families share identical options.** `objects`, `segments`, and `deletions` all get the same `Zstd` compression, same block cache, same block-based table options. The `deletions` CF is append-mostly (tombstones are written and eventually GC'd) — it would benefit from a different compaction style. The `objects` CF has the highest read volume. | Tune per-CF: `objects` gets bloom filter + larger block cache priority. `deletions` gets universal compaction + smaller write buffer (since it's append-mostly with infrequent reads). `segments` gets default settings. |
| M7 | `crates/oceanfs-storage/src/segment/index.rs:34` | **`SegmentIndex` backed by `BTreeMap` — correct per §6.5 but uses JSON serialization (§1.5).** The `BTreeMap<u64, SegmentIndexEntry>` provides O(log n) lookup by offset, which is correct for ordered access. However `to_bytes()`/`from_bytes()` use `serde_json`, adding ~3-5× size overhead vs a compact binary encoding (e.g., a simple length-prefixed entry list). | Replace JSON serialization with a binary format. Each `SegmentIndexEntry` is fixed-size (8+4+32=44 bytes). Serialize as: `[u32 count][entry*count]` — no per-field framing overhead. This also enables mmap-based zero-copy parsing when loading the index. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `crates/oceanfs-storage/src/segment/shard.rs:36` | **`parking_lot::Mutex` not annotated "unfair" (§7.5).** `SegmentShard::segments` uses `Vec<parking_lot::Mutex<ActiveSegment>>` without an `// unfair` comment at construction. Per §7.5, throughput matters more than starvation-prevention fairness. | Add `// unfair` comment at the `Mutex::new()` call site. |
| L2 | Workspace `Cargo.toml` | **No `target-cpu=native` or PGO workflow (§10.4-10.5).** The release profile is well-configured (LTO=fat, codegen-units=1, panic=abort, opt-level=3, strip=symbols), but there is no CI job for `target-cpu=native` builds or a PGO script. The guidelines call for a `scripts/pgo.sh` and a dedicated CI job. | Create `scripts/pgo.sh` with the three-step workflow (compile with profile-generate, run benchmark workload, compile with profile-use). Add a `release-native` CI job with `RUSTFLAGS="-C target-cpu=native"`. |
| L3 | `crates/oceanfs-storage/src/wal/entry.rs:78-88` | **`WalEntry::to_bytes()` uses `Vec::with_capacity` — good, but does not use `bytemuck` (§9.4).** The manual `copy_from_slice` calls are correct but verbose. `bytemuck` could provide zero-copy casting from `&WalEntry` to `&[u8; 72]` if the struct were `#[repr(C)]` and derived `Pod`. | After adding `#[repr(C)]` (H3), derive `bytemuck::Pod` and `bytemuck::Zeroable` on `WalEntry`. Then `to_bytes()` becomes `bytemuck::bytes_of(&self).to_vec()` and `from_bytes()` becomes `bytemuck::try_from_bytes(data)`. |
| L4 | `crates/oceanfs-storage/src/segment/shard.rs:62-63` | **`Vec::with_capacity` used correctly (§1.3).** `SegmentShard::new()` pre-sizes the segments Vec with `Vec::with_capacity(count)`. This is compliant. | No action needed — noted as a positive finding. |
| L5 | `crates/oceanfs-storage/src/segment/sealer.rs:149` | **WAL truncate on seal deletes ALL WAL entries, not just the sealed segment's.** `seal()` calls `self.wal.truncate(self.wal.global_position().await)` which truncates the WAL file at its current end position — effectively deleting all entries. This is correct only if the WAL contains only entries for the single segment being sealed. In a production system with concurrent active segments, this would truncate entries belonging to other active segments. | Track per-segment WAL positions. When sealing segment S, truncate only the entries belonging to S, leaving other segments' entries intact. This requires WAL entries to carry segment IDs (they already do) and a per-segment position tracker. |

---

## Guideline Compliance Matrix

### I/O (§3)

| Rule | Status | Evidence |
|---|---|---|
| §3.1 Sequential-only WAL writes | **COMPLIANT** | `wal/writer.rs:180` opens with `.append(true)`. No `SeekFrom::Start` calls except in `truncate()` which seeks to a valid position. |
| §3.2 `O_DIRECT` for segment data files | **VIOLATION** | `blob_store.rs:57` opens with `File::create()` only. `sealer.rs:125` uses `tokio::fs::write`. No `O_DIRECT` anywhere. |
| §3.3 `mmap` for hot segment reads | **VIOLATION** | No `memmap2` or `mmap` usage in the codebase. Segment reads copy data into `Vec<u8>`. |
| §3.4 Group commit for WAL fsync | **PARTIAL** | Infrastructure exists (`WalSyncGroup` in `wal/sync.rs`) with correct batching logic, but the actual fsync function is a no-op (C1). |
| §3.5 `io_uring` / `tokio-uring` | **VIOLATION** | Not present in any `Cargo.toml`. No feature gate. All I/O uses `std::fs` / `tokio::fs`. |
| §3.6 `sendfile` / `splice` for blob responses | **VIOLATION** | No fd-to-socket zero-copy path. Reads go through userspace buffers (M3). |

### Memory & Allocation (§1)

| Rule | Status | Evidence |
|---|---|---|
| §1.1 `Bytes`/`BytesMut` for blob data | **COMPLIANT** | `ActiveSegment::buffer` is `BytesMut`. `ObjectMetadata::inline_data` is `Option<bytes::Bytes>`. No `Vec<u8>` on segment hot path. |
| §1.2 Buffer pool for segment append buffers | **PARTIAL** | `BufferPool` is implemented and well-tested. But C2: node constructs `_buffer_pool` (unused), write path allocates ad-hoc. |
| §1.3 Pre-sized collections | **COMPLIANT** | `Vec::with_capacity()` used in `BufferPool::new()`, `SegmentShard::new()`, segment pool slots, `cf::encode_object_key`. |
| §1.4 `SmallVec` for small metadata | **COMPLIANT** | `ObjectMetadata::chunks` uses `SmallVec<[ChunkRef; 4]>`. `SegmentMetadata::storage_locations` uses `SmallVec<[NodeId; 16]>`. |
| §1.5 Zero-copy protobuf deserialization | **VIOLATION** | Metadata uses `serde_json` for (de)serialization, not protobuf. H5 tracks this. |

### Data Structures & Memory Layout (§6)

| Rule | Status | Evidence |
|---|---|---|
| §6.1 Cache-line alignment for mutable atomics | **VIOLATION** | `WalWriter` lacks `#[repr(align(64))]` despite having multiple `Mutex` fields on the same struct (H6). |
| §6.2 SoA layout for EC stripe data | **NOT APPLICABLE** | EC stripe encoding is in `oceanfs-ec` crate, outside this audit's scope. See `oceanfs-ec/src/stripe/batch.rs`. |
| §6.3 `#[repr(C)]` for on-disk structures | **VIOLATION** | `WalEntry` and `SegmentHeader` have documented binary layouts but no `#[repr(C)]` (H3). |
| §6.5 `BTreeMap` for segment blob index | **COMPLIANT** | `SegmentIndex::entries` is `BTreeMap<u64, SegmentIndexEntry>`. Correct per spec for offset-ordered lookup. |

### Locking (§7)

| Rule | Status | Evidence |
|---|---|---|
| §7.4 Lock ordering documented | **VIOLATION** | `wal/writer.rs` holds up to 3 Mutexes in `append()` and `rotate()` — no lock ordering comment (M2). |
| §7.5 Default-unfair `parking_lot::Mutex` | **PARTIAL** | Used correctly in `SegmentShard`, `SegmentPool`, `PoolSlot` — but not annotated `// unfair` (L1). |

### Zero-Copy (§9)

| Rule | Status | Evidence |
|---|---|---|
| §9.4 `bytemuck` for byte-to-struct casting | **NOT USED** | `WalEntry` and `SegmentHeader` manually serialize with `copy_from_slice`. `bytemuck` is a workspace dependency but not used in storage crate. |
| §9.5 `extend_from_slice` for batch writes | **COMPLIANT** | `ActiveSegment::append()` uses `self.buffer.extend_from_slice(data)`. Segment sealer uses `extend_from_slice` for assembling header+data+index `Vec`. |

### Compile-Time (§10)

| Rule | Status | Evidence |
|---|---|---|
| §10.1 LTO in release profile | **COMPLIANT** | `Cargo.toml:116`: `lto = "fat"`. |
| §10.2 Single codegen unit | **COMPLIANT** | `Cargo.toml:117`: `codegen-units = 1`. |
| §10.3 Panic abort in release | **COMPLIANT** | `Cargo.toml:118`: `panic = "abort"`. |
| §10.4 `target-cpu = "native"` | **NOT IMPLEMENTED** | No CI job or script for native CPU builds (L2). |
| §10.5 PGO workflow | **NOT IMPLEMENTED** | No `scripts/pgo.sh` or CI job (L2). |

### Async Patterns (§8)

| Rule | Status | Evidence |
|---|---|---|
| §8.3 `spawn_blocking` justified | **PARTIAL** | Metadata async wrappers use `spawn_blocking` for RocksDB (necessary) but without justification comment (M5). |

---

## RocksDB Configuration Audit

| Parameter | Current Value | Recommended | Rationale |
|---|---|---|---|
| Compression | `Zstd` (all CFs) | `Zstd` (all CFs) | **Good.** Zstd provides the best compression ratio for metadata, which is important for inline blob storage efficiency. |
| Block cache | 128 MB (default), single shared cache | 128-512 MB, shared | **Adequate.** 128 MB is reasonable for metadata-heavy workloads. Should be configurable per deployment. |
| Bloom filter | **Not configured** | 10 bits/key on `objects` CF | **Missing.** Without bloom filter, every GET for a non-existent key does an SST file probe. Critical for HEAD/GET 404 performance. |
| Write buffer (memtable) | 64 MB (default), single value | 64-256 MB, per-CF | **Adequate for now.** Larger write buffers reduce write stalls on the `objects` CF under high write throughput. |
| Max open files | **Not set** (default -1 = unlimited) | 4096 or derived from ulimit | **Risky.** Default -1 keeps all SST file descriptors open. On a long-running node with thousands of SST files, this can exhaust system fd limits. |
| Compaction style | Leveled (default) | Leveled for `objects`/`segments`; Universal for `deletions` | **Acceptable.** Universal compaction for the `deletions` CF would reduce write amplification since tombstones are write-once, GC'd-later. |
| Parallelism | `num_cpus::get()` | `num_cpus::get()` | **Good.** RocksDB background compactions and flushes scale with core count. |
| Write buffer manager | **Not configured** | Consider if total memtable memory exceeds available RAM | **Not critical.** Only needed if memory pressure forces RocksDB to flush too aggressively. |
| WAL (RocksDB's own) | Default (enabled) | Keep enabled | **Good.** RocksDB's internal WAL protects against process crashes. OceanFS's WAL sits above this. |
| Column family differentiation | **None — all CFs identical** | Per-CF tuning | **Missing.** See M6. |
| Serialization format | `serde_json` | Protobuf or custom binary | **Suboptimal.** JSON adds 2-5× size overhead and parsing cost vs binary. See H5. |

---

## WAL Performance Analysis

### Fsync Strategy

The group-commit infrastructure (`WalSyncGroup` in `wal/sync.rs`) is correctly structured:
- Bounded channel (`mpsc::channel(1024)`) collects fsync requests
- First waiter triggers a batch; subsequent waiters within the same window are drained with `try_recv`
- Batch max size is 64 waiters
- Timeout at `batch_timeout_ms` (default 5ms)

However, the actual fsync function passed to the sync group is a **no-op** (C1).
The comment in `create_sync_group` claims "real fsync happens in append's flush()"
but `std::fs::File::flush()` on a raw `File` is a no-op — it only applies to
`BufWriter`. The data is written to the kernel page cache but never flushed to
the storage device.

### Durability Assessment

| Event | Data Safety |
|---|---|
| Process crash | ✅ WAL data in kernel page cache survives (same kernel). WAL replay on restart recovers. |
| OS crash / kernel panic | ❌ Data in page cache lost. No fsync means no durability. |
| Power failure | ❌ Same as OS crash. |
| Disk failure | ❌ Without fsync, even normally-durable data may be in a partial-write state. |

**Effective throughput with no-op fsync:** ~∞ (since no actual disk barrier occurs).
**Expected throughput with real fsync (NVMe):** ~50-100 MB/s for file.sync_all() per batch, or ~10,000-20,000 appends/sec at 5ms batch timeout with 64-entry batches.

### Throughput Estimate (with real fsync)

Assuming:
- NVMe SSD with ~100µs fsync latency
- 5ms batch window
- 64 entries/batch max
- Group commit amortization

| Scenario | Fsync calls/sec | Appends/sec | Latency per append |
|---|---|---|---|
| Per-write fsync (no batching) | ~10,000 | ~10,000 | ~100µs (fsync) + write |
| Batch of 1 (timeout hits) | ~200 | ~200 | ~5ms (batch timeout) |
| Batch of 64 (full batch) | ~200 | ~12,800 | ~5ms / 64 ≈ 78µs amortized |
| Optimized (io_uring + SQ polling) | ~50,000 | ~320,000 | ~2µs |

---

## Segment I/O Analysis

### File Open Flags

| Operation | Current Flags | Required Flags (§3.2) | Gap |
|---|---|---|---|
| `BlobStore::write_blob` | `File::create()` (read/write, no special flags) | `O_DIRECT` (unless read_cache_segments) | No O_DIRECT |
| `BlobStore::read_blob` | `File::open()` (read-only) | `O_DIRECT` or `mmap` based on config | No branch |
| `SegmentSealer::seal` | `tokio::fs::write` (creates temp, writes, renames) | `O_DIRECT` | No O_DIRECT, double-buffer (M1) |
| `WalWriter` | `OpenOptions::new().append(true)` | Append mode is correct (§3.1) | ✅ Compliant |

### Data Layout

The segment data format is:
```
[SegmentHeader: 76 bytes] [blob data ...] [SegmentIndex: JSON bytes]
```

The header has a documented binary layout (76 bytes) but no `#[repr(C)]` (H3).
The index is JSON-serialized (H5/M7). There's no SoA layout consideration since
segment data is opaque blob concatenation — EC striping is a higher layer.

### Access Patterns

- **Writes:** Sequential append to in-memory `BytesMut` → on seal, copied to a single `Vec<u8>` → written via `tokio::fs::write`. No random I/O on write path. ✅
- **Reads:** `BlobStore::read_blob` reads entire segment file into `Vec<u8>` — no range read. For a 4 MB segment where the caller only needs bytes 1000-1100, this reads the entire 4 MB. ❌
- **Index reads:** The entire segment file is read and parsed to extract the index. No separate index file or header-only read. ❌

---

## Buffer Allocation Audit

### Where Buffers Come From

| Allocation Site | Type | Size | Frequency | Pooled? |
|---|---|---|---|---|
| `ActiveSegment::new` (buffer.rs:78) | `BytesMut` | `chunk_size` (64KB) | Per active segment creation | ✅ Via `BufferPool::acquire()` |
| `ActiveSegment::append` (buffer.rs:121) | In-place `extend_from_slice` | Variable | Per PUT | ✅ No allocation — in-place append |
| `SegmentSealer::seal` (sealer.rs:121) | `Vec<u8>` | header+data+index (~4 MB) | Per seal | ❌ Fresh allocation |
| `BlobStore::read_blob` (blob_store.rs:76) | `Vec<u8>` | Full segment size | Per read | ❌ Fresh allocation |
| `BlobStore::write_blob` (blob_store.rs:57) | Write-through (no buffer) | — | Per write | N/A |
| `WalEntry::to_bytes` (entry.rs:79) | `Vec<u8>` | 72 bytes | Per append | ❌ Fresh allocation (but 72 bytes is negligible) |

### Recycling Status

The `BufferPool::release()` method clears and returns buffers to the free pool.
However, C2 means this path is never exercised in production — the node's
`BufferPool` is never wired. The segment pool tests and unit tests exercise the
buffer pool, but the actual write path constructs its own ad-hoc `ActiveSegment`
with a locally-created `BufferPool` that goes out of scope after the test.

---

## Top 5 Bottlenecks (Ranked by Impact)

### 1. WAL fsync is a no-op (C1) — No Durability

**Impact:** Data loss on OS crash/power failure. This is a correctness bug, not
just a performance issue. All WAL writes land only in the kernel page cache with
no disk barrier.

**Fix:** Wire `file.sync_all()` into the `WalSyncGroup`'s closure. Requires
passing an `Arc<File>` (or the file descriptor) to the sync group's background
task.

**Throughput impact:** Adding real fsync will reduce WAL throughput from
effectively infinite to ~50-100 MB/s (NVMe). Group commit amortization will
keep per-append latency low (sub-100µs at batch sizes of 64).

### 2. BufferPool not wired (C2) — Per-Request Allocation

**Impact:** Every active segment creation allocates a new `BytesMut` buffer
(64KB-4MB). Under load with 32 active segment pools cycling, this could mean
~100+ MB of allocations per second, stressing the allocator.

**Fix:** Wire the `BufferPool` from `Node::start()` into the segment pool and
`SegmentShard`, replacing ad-hoc constructions.

### 3. No O_DIRECT / mmap for segment I/O (H1) — Double-Buffering

**Impact:** Every segment write and read goes through the OS page cache. For a
4 MB segment write, this means: (1) userspace → kernel page cache copy, (2) page
cache → disk. The page cache then holds 4 MB of segment data that is unlikely to
be re-read before eviction. For reads under `read_cache_segments=false`, this
pollutes the page cache and evicts hot metadata/WAL data.

**Fix:** Add `O_DIRECT` to segment file opens, with aligned buffers. Add `mmap`
path when `read_cache_segments=true`.

### 4. JSON serialization on metadata hot path (H5) — CPU Overhead

**Impact:** Every metadata PUT/GET serializes/deserializes JSON. For a write-heavy
workload (10K PUTs/sec), this is 10K JSON encodes/sec. JSON encoding is ~10-100×
slower than protobuf encoding for structured data.

**Fix:** Switch to protobuf (already defined in `oceanfs-core/proto/`) for
metadata serialization. This also satisfies §1.5.

### 5. WalEntry / SegmentHeader not #[repr(C)] (H3) — Correctness Risk

**Impact:** Without `#[repr(C)]`, the Rust compiler can reorder fields and insert
padding arbitrarily. The current `to_bytes()`/`from_bytes()` methods manually
specify field order, so serialized data is correct — but if anyone ever uses
`std::mem::transmute` or `bytemuck` on these structs, they'd get garbage. This
is a latent correctness bug waiting to happen.

**Fix:** Add `#[repr(C)]` to both structs. Add `bytemuck::Pod`/`Zeroable` derives
for zero-copy casting.

---

## Recommendations (Prioritized)

1. **Immediate (blocking correctness):** Fix C1 — add real `fsync`/`fdatasync` to the WAL sync group. This is the only finding that makes the system not durable. Without it, the WAL provides zero crash protection.

2. **High priority (integration):** Fix C2 — wire `BufferPool` and `SegmentSealer` into the write path via `Node::start()`. This unblocks the `final-integration-read-write-end-to-end` feature and eliminates per-request allocation.

3. **High priority (correctness):** Fix H3 — add `#[repr(C)]` to `WalEntry` and `SegmentHeader`. This is a one-line change per struct with zero behavior change (since `to_bytes`/`from_bytes` are correct) but prevents future misuse.

4. **Performance (I/O):** Fix H1 — add `O_DIRECT` flag to segment data file opens, with buffer alignment. Branch on `read_cache_segments` config for `mmap`. This eliminates double-buffering for large segments.

5. **Performance (metadata):** Fix H5 — switch from `serde_json` to protobuf for metadata serialization. This reduces CPU overhead and storage size by 2-5×.

6. **Performance (RocksDB):** Fix H4 — add bloom filter to `objects` CF, set `max_open_files`, tune write buffer per CF. These are configuration changes with no code modifications needed.

7. **Performance (async I/O):** Fix H2 — add `tokio-uring` feature gate for Linux disk I/O. This is the highest-effort recommendation but provides the largest throughput gain (2-5× for WAL writes, 3-10× for segment reads).

8. **Medium priority:** Fix M1 (avoid double-allocation on seal), M2 (document lock ordering), M4 (prefix extractor for RocksDB LIST), M5 (add spawn_blocking justification comment), M7 (binary segment index format).

9. **Low priority:** Fix L1 (unfair annotation), L2 (native CPU build + PGO), L3 (bytemuck for WalEntry), L5 (per-segment WAL truncation).
