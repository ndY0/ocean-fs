# Disk-Backed Segment Reader — Design Document

**Date:** 2026-08-08
**Author:** Architect (brainstorm agent)
**Context:** Platform I/O Optimizations feature (docs/features/performance-optimization/platform-io-optimizations/feature.md)
**Status:** Proposed

---

## 1. Problem Summary

The production read path is entirely in-memory. `InMemorySegmentReader`
(`HashMap<SegmentId, Bytes>` in `RwLock`) stores all segment data permanently
in RAM. At startup, `node.rs:572-583` preloads all segment data from `BlobStore`.
On PUT, newly-written data is inserted into the same map (`node.rs:578`).

The four Platform I/O Optimization types (`SegmentFileCache`, `DiskIo`,
`DirectIoBuf`, `SegmentFileBody`) are built and tested in
`oceanfs-storage/src/io/` but have **zero consumers**. The segment files are
written via O_DIRECT (good), but never read back through the configured I/O
mode. Memory grows without bound. Cache eviction does not exist.

**Goal:** A disk-backed `SegmentReader` implementation that replaces
`InMemorySegmentReader` in production, integrates the four I/O types, respects
`read_cache_segments` config, and enables zero-copy sendfile responses.

---

## 2. Where the New Reader Lives

**Decision:** New module `oceanfs-storage/src/io/segment_reader.rs`

**Rationale:**

1. **Proximity to the I/O infrastructure.** The reader composes
   `SegmentFileCache`, `DiskIo`, `DirectIoBuf`, and optionally
   `SegmentFileBody`. All four live in `oceanfs-storage/src/io/`. Colocating
   the reader keeps the I/O strategy in one module tree.

2. **Crate dependency DAG compliance.** `oceanfs-storage` depends on
   `oceanfs-core`. `oceanfs-server` depends on `oceanfs-storage`. The new
   reader imports `SegmentReader` from `oceanfs-server` — this is a
   **valid edge** because `storage → server` does not exist in the DAG.

   Wait — that's a problem. `oceanfs-server` depends on `oceanfs-storage`,
   not the reverse. If the reader lives in `oceanfs-storage`, it cannot
   implement `SegmentReader` (defined in `oceanfs-server`).

   **Resolution:** Move the `SegmentReader` trait from
   `oceanfs-server/src/read/coordinator.rs:153-165` into
   `oceanfs-storage/src/io/segment_reader.rs` (or a sibling module). The
   trait is a storage-level abstraction — "read a chunk from a segment" —
   not a server concern. The trait is consumed by `oceanfs-server` (read
   coordinator), but its definition rightfully belongs in the crate that
   provides its implementations.

   Per `guidelines/architecture.md` §2.1: "Traits in the consuming crate"
   is the general rule, with the exception that "traits fundamental to the
   domain and consumed by many crates may live in `oceanfs-core`." The
   `SegmentReader` trait is consumed by one crate (`oceanfs-server`) and
   needs implementations in `oceanfs-storage` — placing it in `oceanfs-core`
   avoids a circular dep while keeping the architecture clean:
   - `oceanfs-core` defines `SegmentReader` trait
   - `oceanfs-storage` provides `DiskSegmentReader` impl
   - `oceanfs-server` imports the trait from `oceanfs-core` and accepts
     `Arc<dyn SegmentReader>` at construction time

   **Alternative considered (trait in storage, re-exported):** Define the
   trait in `oceanfs-storage` and have `oceanfs-server` depend on it.
   Rejected because `oceanfs-server` already depends on `oceanfs-storage`
   — this is the simpler path. But it does mean the trait lives in
   `storage`, not "the consuming crate." The architecture guidelines
   explicitly permit this for `oceanfs-core` (§2.1 exception); the same
   reasoning applies here since `oceanfs-storage` is the natural home for
   a segment-reader trait.

   **Final placement of trait:** `oceanfs-storage/src/io/segment_reader.rs`
   defines `pub trait SegmentReader`. The `DiskSegmentReader` struct and
   impl live in the same file. The existing `InMemorySegmentReader` in
   `oceanfs-server/src/read/coordinator.rs:1270` is moved/migrated to
   `oceanfs-storage` alongside the trait (or kept in server for test use
   only, implementing the relocated trait).

   **Impact on `oceanfs-server` consumers:**
   - `ReadCoordinator.segment_reader: Option<Arc<dyn SegmentReader>>`
     changes import from `crate::SegmentReader` to `oceanfs_storage::SegmentReader`
   - `InMemorySegmentReader` import changes or is migrated

3. **Module file:** `oceanfs-storage/src/io/segment_reader.rs`. Re-exported
   from `oceanfs-storage/src/io/mod.rs`:
   ```rust
   pub mod segment_reader;
   pub use segment_reader::{DiskSegmentReader, SegmentReader};
   ```

---

## 3. Trait / Interface Design

### 3.1 Trait Evolution

The current `SegmentReader` trait at `coordinator.rs:153-165`:

```rust
pub trait SegmentReader: Send + Sync {
    fn read_chunk(
        &self,
        segment_id: &SegmentId,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, String>;
}
```

This is adequate for the read path's current need (fetch bytes → assemble).
However, for zero-copy sendfile integration, the caller (S3 handler) needs to
know whether the data came from disk (so it can use `SegmentFileBody`) or from
memory (so it uses `Body::from(Bytes)`).

**Decision: Extend the return type, not the trait signature.**

Add a new enum `SegmentReadSource` alongside the trait:

```rust
// In oceanfs-storage/src/io/segment_reader.rs

/// Describes the data source for a segment chunk read.
///
/// Used by upper layers to choose the response body strategy:
/// - `Memory` → `Body::from(Bytes)` (zero-copy from Bytes)
/// - `MmapBacked` → `SegmentFileBody` (mmap-backed, for sendfile path)
/// - `DirectIo` → `SegmentFileBody` or `Body::from(Bytes)` (both fine)
#[derive(Debug, Clone)]
pub enum SegmentReadSource {
    /// Data was served from an in-memory cache (HashMap, L1, inline metadata).
    /// The `Bytes` owns its data.
    Memory,
    /// Data was sliced from an mmap region backed by a segment file.
    /// The `Bytes` shares the mmap's `Arc` — zero additional allocation.
    /// The `segment_id` and `file_path` are available for metrics/logging.
    MmapBacked {
        segment_id: SegmentId,
        file_path: PathBuf,
    },
    /// Data was read from disk via O_DIRECT or buffered I/O into a
    /// temporary buffer.
    DirectIo {
        segment_id: SegmentId,
        file_path: PathBuf,
    },
}
```

The `DiskSegmentReader::read_chunk` returns `(Bytes, SegmentReadSource)` instead
of just `Bytes`. The `InMemorySegmentReader` always returns `Memory`.

But this changes the trait signature — all callers must be updated. The
`fetch_single_chunk` function at `fetch.rs:396-407` calls
`reader.read_chunk(&chunk.segment_id, chunk.offset, chunk.length)` and
returns `Result<Bytes>`. We need to thread the source through.

**Decision: Keep the trait simple; add a companion method.**

```rust
pub trait SegmentReader: Send + Sync {
    /// Reads a chunk of data from a segment.
    fn read_chunk(
        &self,
        segment_id: &SegmentId,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, String>;

    /// Returns the source metadata for the most recent `read_chunk` call.
    ///
    /// Default returns [`SegmentReadSource::Memory`].
    /// Stateful readers (DiskSegmentReader) override this to return
    /// file-backed information for sendfile integration.
    fn last_read_source(&self, segment_id: &SegmentId)
        -> SegmentReadSource
    {
        let _ = segment_id;
        SegmentReadSource::Memory
    }
}
```

**Rejected alternative (change return type):** Changing `read_chunk` to
return `(Bytes, SegmentReadSource)` forces every call site to destructure
the tuple, even when only `Bytes` is needed. The `fetch_single_chunk`
function at `fetch.rs:410` currently does `Ok(data)` — it neither knows
nor cares about the source. The companion method is opt-in: only the top
level (S3 handler) queries it.

**Rejected alternative (wrap in ReadResult immediately):** The coordinator's
`ReadResult::data` could change from `Bytes` to an enum. But this couples the
coordinator to transport concerns (HTTP body choice). The coordinator should
return data + metadata; the handler decides presentation.

### 3.2 `DiskSegmentReader` Struct

```rust
/// Disk-backed segment reader implementing [`SegmentReader`].
///
/// Routes reads through the configured I/O backend based on
/// [`IoReadMode`], resolved from `NodeConfig::read_cache_segments`
/// (with per-bucket override from `BucketPolicy::read_tuning`).
///
/// ## I/O Mode Selection
///
/// | `IoReadMode` | Read Path                                   |
/// |--------------|---------------------------------------------|
/// | `Mmap`       | `SegmentFileCache::get_or_map()` → `&mmap` slice → `Bytes` |
/// | `Direct`     | `DirectIoBuf` → `DiskIo::read()` → `Bytes` |
/// | `Buffered`   | `tokio::fs::File::read_at()` → `Bytes`     |
///
/// ## Memory Bounds
///
/// Memory is bounded by the `SegmentFileCache` (max mmap entries × segment
/// size) plus temporary `DirectIoBuf` allocations (per-read, returned to
/// pool). There is no unbounded HashMap — unlike `InMemorySegmentReader`.
pub struct DiskSegmentReader {
    /// The configured read mode, resolved at construction.
    read_mode: IoReadMode,
    /// The disk I/O backend (io_uring or tokio::fs).
    disk_io: Arc<DiskIo>,
    /// Optional LRU cache of memory-mapped segment files.
    /// Only used when `read_mode == Mmap`.
    mmap_cache: Option<Arc<SegmentFileCache>>,
    /// Optional buffer pool for O_DIRECT reads.
    /// Only used when `read_mode == Direct`.
    direct_buf_pool: Option<Arc<DirectBufPool>>,
    /// Base directory for segment files.
    segment_dir: PathBuf,
    /// Tracks the source of the most recent read, keyed by segment_id.
    /// Used by `last_read_source()`.
    last_source: parking_lot::Mutex<HashMap<SegmentId, SegmentReadSource>>,
}
```

**Construction (in `oceanfs-node/src/node.rs`, around line 567):**

```rust
// Replace:
// let segment_reader = Arc::new(oceanfs_server::InMemorySegmentReader::new());

// With:
let io_mode = IoReadMode::from_config(config.read_cache_segments);
let mmap_cache = if io_mode == IoReadMode::Mmap {
    Some(Arc::new(SegmentFileCache::new(config.segment_cache_max_entries)))
} else {
    None
};
let segment_reader: Arc<dyn oceanfs_storage::SegmentReader> = Arc::new(
    DiskSegmentReader::new(
        io_mode,
        disk_io.clone(),
        mmap_cache,
        config.data_dir.join("segments"),
    )
);
```

### 3.3 Config Additions

`NodeConfig` (at `oceanfs-core/src/config/node.rs:110`) already has:

```rust
pub read_cache_segments: bool,     // default: false
pub io_uring_enabled: bool,        // default: cfg!(linux)
```

**New field needed:**

```rust
/// Maximum number of segment files to keep memory-mapped.
/// Only meaningful when `read_cache_segments = true`.
/// Default: 64 segments. Set to 0 to cache all (unbounded — not recommended).
#[serde(default = "default_segment_cache_max_entries")]
pub segment_cache_max_entries: usize,  // default: 64
```

---

## 4. Cache Hierarchy

### 4.1 Where the Segment Cache Fits

```
Client GET request
       │
       v
┌──────────────────────────────────────────────────────────────────────┐
│ S3 GET Handler (handlers.rs:200)                                      │
│                                                                       │
│  1. L1 Object Cache (oceanfs-cache::ObjectCache)                     │
│     Hit → verify BLAKE3 → Body::from(cached_bytes) → 200             │
│     Miss ↓                                                            │
│  2. L2 Metadata Cache (oceanfs-cache::MetadataCache)                 │
│     Hit → inline_data? → yes → Body::from(inline) → 200             │
│            no → chunk list (continue)                                 │
│     Miss ↓                                                            │
│  3. L3 Negative Cache → "absent" → 404                                │
│     Not absent ↓                                                      │
│  4. ReadCoordinator::get_object()                                     │
│       ├─ lookup_metadata() → RocksDB                                  │
│       └─ assemble_chunks()                                            │
│            └─ for each chunk:                                         │
│                 └─ segment_reader.read_chunk(segment_id, offset, len) │
│                      │                                                │
│                      v                                                │
│       ┌─────────────────────────────────────────────────────────────┐ │
│       │ L0 Segment File Cache  (NEW — DiskSegmentReader)            │ │
│       │                                                              │ │
│       │  IoReadMode::Mmap:                                          │ │
│       │    SegmentFileCache::get_or_map(segment_id, &path)          │ │
│       │      Hit → Arc<Mmap> → &[u8] slice → Bytes::from(slice)    │ │
│       │      Miss → memmap2::Mmap::map(&file) → cache → Bytes      │ │
│       │                                                              │ │
│       │  IoReadMode::Direct:                                        │ │
│       │    DirectIoBuf → DiskIo::read(path, &buf, offset)           │ │
│       │    → Bytes::copy_from_slice(&buf[...len])                   │ │
│       │                                                              │ │
│       │  IoReadMode::Buffered:                                      │ │
│       │    tokio::fs::File::read_at(path, &mut buf, offset)         │ │
│       │    → Bytes::from(buf)                                       │ │
│       └─────────────────────────────────────────────────────────────┘ │
│                                                                       │
│  5. MultiChunkAssembler → streaming BLAKE3 → Bytes                   │
│  6. Return GetResult { data: Bytes, ... }                             │
│  7. Populate L1 / L2 caches                                           │
│  8. Response: SegmentFileBody or Body::from(Bytes)                   │
└──────────────────────────────────────────────────────────────────────┘
```

### 4.2 L0 vs L1 vs L2 Cache

| Layer | What it caches | Granularity | Eviction | Hit benefit |
|-------|---------------|-------------|----------|-------------|
| **L0 (Segment)** | Raw segment file bytes (mmap) | Per segment (4-64 MB) | LRU on segment access | Skips disk read + EC decode for all blobs in that segment |
| **L1 (Object)** | Assembled + verified blob payloads | Per object (any size) | LRU + TTL | Skips metadata lookup + chunk assembly + hash verify |
| **L2 (Metadata)** | `ObjectMetadata` entries | Per object | LRU + TTL | Skips RocksDB metadata lookup |

The L0 and L1 caches are **complementary**, not redundant:
- **L1 stores post-assembly blobs.** A 1 KB blob from a 4 MB segment costs
  O(1) memory but delivers ~0 I/O on re-read. 10,000 hot blobs = ~10 MB in L1.
- **L0 stores pre-assembly segments.** A single mmap entry (4 MB) serves
  **all** blobs within that segment. If 100 blobs from the same segment are
  read, L0 saves 100 × (disk metadata lookup + segment read). Memory cost:
  4 MB.

**Eviction interaction:** When L1 evicts a blob, the blob can still be served
from L0 (segment mmap) on next access — the cost is chunk assembly + hash
verify, but no disk I/O. When L0 evicts a segment (mmap unmapped), next
access re-maps the file from disk — the cost is a page fault.

---

## 5. Eviction and Memory Bounds

### 5.1 Segment File Cache (mmap mode)

The `SegmentFileCache` at `mmap.rs:48-53` is a bounded LRU:
- `max_entries` (default 64 from new config field `segment_cache_max_entries`)
- On insert when full: evict oldest-access entry (linear scan of Vec)
- Evicted `Arc<Mmap>` handles held by in-flight readers remain valid until
  dropped by those readers

**Memory implication:** 64 segments × 4 MB default target size = 256 MB.
Tunable via config.

### 5.2 Direct I/O Buffers (direct mode)

`DirectIoBuf` allocations are per-read, short-lived. After `read_chunk`
returns, the buffer is either:
- Recycled into a pool (`DirectBufPool` — a simple `ArrayQueue<DirectIoBuf>`)
- Dropped (the memory is unmapped)

The pool caps at `max_pooled` (default 8). Each buffer is page_size-aligned,
matching the requested chunk size (max: 4 MB for standard segments).

**Memory implication:** 8 × 4 MB = 32 MB worst case, with buffers recycled.

### 5.3 In-Memory Fallback (Buffered mode)

Standard `tokio::fs` reads allocate a temporary `Vec<u8>` → `Bytes::from`.
The buffer is dropped after the read. No long-lived memory beyond what the
caller holds (assembler buffer, L1 cache).

### 5.4 No Unbounded Growth

Unlike `InMemorySegmentReader` (`HashMap<SegmentId, Bytes>` with no eviction),
`DiskSegmentReader` has no persistent, unbounded data structure. All
allocations are bounded by cache size (mmap) or pool caps (direct buffers).

---

## 6. Sendfile Integration

### 6.1 How the GET Handler Chooses the Body Type

Currently at `handlers.rs:317-354`:

```rust
match state.read.get(req).await {
    Ok(result) => {
        // ...
        (StatusCode::OK, headers, Body::from(result.data)).into_response()
    }
    // ...
}
```

`result.data` is `Bytes`. `Body::from(Bytes)` creates a standard axum body.
This is already zero-copy from userspace perspective (Bytes is ref-counted),
but the HTTP body is served through `write()` syscalls that copy from
userspace to kernel socket buffer.

**Decision: Extend `GetResult` with source metadata.**

Add a field to `GetResult` (at `coordinator.rs:74-83`):

```rust
pub struct GetResult {
    pub data: Bytes,
    pub metadata: ObjectMetadata,
    pub cache_hit: CacheHitLevel,
    pub hash: HashOutput,
    /// Whether the data was backed by a disk segment file.
    /// When `Some`, the handler SHOULD use `SegmentFileBody`
    /// for the HTTP response (enabling potential sendfile path).
    pub segment_source: Option<SegmentReadSource>,
}
```

The `assemble_chunks` method propagates the source from the segment reader.
When all chunks come from the same mmap-backed segment, the source is
`MmapBacked { segment_id, file_path }`. The handler then wraps in
`SegmentFileBody`:

```rust
// handlers.rs:317-354, modified:
match state.read.get(req).await {
    Ok(result) => {
        let body: http_body::Body = match result.segment_source {
            Some(SegmentReadSource::MmapBacked { .. }) |
            Some(SegmentReadSource::DirectIo { .. }) => {
                // Use file-backed body for potential sendfile optimization
                SegmentFileBody::new(result.data, 0, result.data.len() as u64)
            }
            _ => {
                Body::from(result.data)
            }
        };
        (StatusCode::OK, headers, body).into_response()
    }
}
```

### 6.2 The Trigger Condition

`SegmentFileBody` is used when:
1. The blob is not inline (has chunk references → segment on disk)
2. The segment reader is a `DiskSegmentReader` (not `InMemorySegmentReader`)
3. The `SegmentReadSource` is `MmapBacked` or `DirectIo`

When L1 cache hit: source is `Memory` → `Body::from(Bytes)`.
When L2 inline data: source is `Memory` → `Body::from(Bytes)`.
When segment file read: source is `MmapBacked`/`DirectIo` → `SegmentFileBody`.

### 6.3 Current Limitation of SegmentFileBody

`SegmentFileBody` currently wraps `Bytes`, not a file descriptor. It does
not perform a `sendfile(2)` syscall — the actual sendfile would require
holding a `tokio::fs::File` or raw fd. For this iteration, the benefit is:
1. **Correctness:** Signals to the HTTP layer that the body is file-backed
   (useful for future sendfile optimization, metrics)
2. **Zero-copy Bytes:** The `Bytes` was sliced from `Arc<Mmap>`, so no
   data copy occurred during chunk read
3. **Size hint:** `SegmentFileBody::size_hint` provides exact content-length,
   which axum uses for `Content-Length` header

Full `sendfile(2)` integration is deferred to a follow-up feature that would
extend `SegmentFileBody` to hold a `tokio::fs::File` and use platform-specific
sendfile in `poll_frame`. This design leaves the door open.

---

## 7. Config Flow

### 7.1 From Config to Reader

```
oceanfs.toml
  read_cache_segments = true          (NodeConfig, core/config/node.rs:110)
  segment_cache_max_entries = 64      (NodeConfig, new field)
  
  [bucket.my-bucket]
  read_cache_segments = false         (BucketPolicy, server/bucket_config.rs:309)
       │
       v
node.rs:567 — construction
  let effective_cache = bucket_policy.read_tuning.read_cache_segments
      .unwrap_or(config.read_cache_segments);
  let io_mode = IoReadMode::from_config(effective_cache);
       │
       v
DiskSegmentReader::new(io_mode, disk_io, mmap_cache, segment_dir)
       │
       v
ReadCoordinator::with_segment_reader(segment_reader)
       │      coordinator.rs:253
       v
fetch_single_chunk() → reader.read_chunk()  (fetch.rs:410)
       │
       v (Bytes returned)
assemble_chunks() → MultiChunkAssembler  (coordinator.rs:1044)
       │
       v (final Bytes + SegmentReadSource)
get_object() → GetResult { data, segment_source, ... }
       │
       v
handlers.rs:get_object() → segment_source? → Body::from() or SegmentFileBody
```

### 7.2 Per-Bucket Override

`BucketPolicy::read_tuning.read_cache_segments` (`Option<bool>`) already
exists at `bucket_config.rs:309`. When `Some(true)` or `Some(false)`, it
overrides the node-level default. The effective value is resolved:

```rust
// In node.rs, during construction:
fn resolve_read_mode(
    config: &NodeConfig,
    bucket_policy: Option<&BucketPolicy>,
) -> IoReadMode {
    let cache_segments = bucket_policy
        .and_then(|p| p.read_tuning.read_cache_segments)
        .unwrap_or(config.read_cache_segments);
    IoReadMode::from_config(cache_segments)
}
```

**But:** The `DiskSegmentReader` is constructed once per node (single
instance shared across all buckets). Per-bucket overrides mean the reader
needs to support **mode switching per read**.

**Decision: Mode is resolved per-read, not per-reader.**

Add a method to accept per-read hint:

```rust
impl DiskSegmentReader {
    /// Reads a chunk, optionally overriding the configured read mode.
    /// 
    /// `mode_override`: when `Some`, overrides the reader's default mode
    /// for this call. Used for per-bucket `read_cache_segments` overrides.
    pub async fn read_chunk_with_mode(
        &self,
        segment_id: &SegmentId,
        offset: u64,
        length: u32,
        mode_override: Option<IoReadMode>,
    ) -> Result<(Bytes, SegmentReadSource), String> {
        let mode = mode_override.unwrap_or(self.read_mode);
        // ...
    }
}
```

But this doesn't fit the `SegmentReader` trait. The trait method is
synchronous and doesn't take a mode parameter. Options:

1. **Extend the trait** — add a `read_chunk_with_mode` default method.
   Rejected: complicates the trait for all implementors.

2. **One reader per bucket** — construct a `DiskSegmentReader` per bucket.
   Rejected: wasteful, mmap caches would be siloed.

3. **Mode stored in the reader, updated on config reload** — the reader
   holds an `ArcSwap<IoReadMode>` that is updated when bucket config changes.
   But this changes the mode for ALL concurrent reads, not per-bucket.

4. **Pass mode through ReadRequest** — the coordinator already receives
   `policy: Option<Arc<BucketPolicy>>` in `ReadRequest` (coordinator.rs:55).
   The coordinator resolves the mode and passes it down to the segment reader
   via a wrapper or the reader's internal state.

**Decision: Option 4 — Thread mode through the read path.**

The `fetch_single_chunk` function at `fetch.rs:396` already receives no
bucket policy. We change `ReadCoordinator::assemble_chunks` (coordinator.rs:909)
to pass `read_cache_segments` to the fetch layer, which passes it to the
segment reader. The `DiskSegmentReader` stores a per-thread `Cell<IoReadMode>`
or accepts mode in a side-channel.

**Simpler approach:** The `DiskSegmentReader` exposes a
`set_per_call_mode(IoReadMode)` method. Before calling `read_chunk`, the
caller sets the mode. The reader uses a thread-local or a mutex-guarded
`current_mode` field. For the common case (node-level default), the mode
is never changed.

Actually, the cleanest approach given the trait constraint:

```rust
// Add to DiskSegmentReader:
impl DiskSegmentReader {
    /// Per-read mode override for the next `read_chunk` call.
    /// Resets after the call. Thread-safe through the Mutex.
    pub fn set_next_mode(&self, mode: IoReadMode) {
        *self.next_mode_override.lock() = Some(mode);
    }
}
```

In `assemble_chunks`, before chunk reads:

```rust
// coordinator.rs, in assemble_chunks():
let mode_override = policy
    .and_then(|p| p.read_tuning.read_cache_segments)
    .map(IoReadMode::from_config);

if let Some(mode) = mode_override {
    if let Some(reader) = self.segment_reader.as_ref() {
        // Downcast or method call — this requires the reader to be DiskSegmentReader
        // OR add a set_read_mode method to the SegmentReader trait
    }
}
```

**Rejected — downcasting is a code smell.** Better approach: add an
optional `ReadOptions` parameter to the trait via a new method.

**Final Decision: Use a companion trait for mode-aware readers.**

```rust
/// Optional extension for segment readers that support per-read mode overrides.
pub trait SegmentReaderExt: SegmentReader {
    /// Sets the I/O read mode for the next `read_chunk()` call.
    /// The mode is consumed (reset to default after the call).
    fn set_read_mode(&self, mode: IoReadMode);
}
```

`DiskSegmentReader` implements both `SegmentReader` and `SegmentReaderExt`.
The coordinator checks `if let Some(ext) = reader.as_any().downcast_ref::<dyn SegmentReaderExt>()`.
This is acceptable because it's a single, well-defined extension point.

**Even simpler:** Skip per-read mode complexity for v1. Use **node-level
config only** for the `DiskSegmentReader` mode. Per-bucket `read_cache_segments`
is documented as "overrides node-level, but requires a node restart" or is
implemented in v2. The feature doc already marks this as optional
(`#[serde(default)]` with `Option<bool>`).

This aligns with the feature's Definition of Done item:
> "Config: `NodeConfig` gains `read_cache_segments: bool`"

Per-bucket override is a stretch item, not a blocker.

---

## 8. Migration from InMemorySegmentReader

### 8.1 Strategy: Replace, Don't Wrap

`InMemorySegmentReader` at `coordinator.rs:1270-1322` is a HashMap with no
eviction. Its purpose was:
1. Fast local reads during single-node testing
2. Simplicity during early development

With `DiskSegmentReader`, reads go to disk via the configured mode. For
single-node operation, this works identically — the segment files exist on
the local filesystem.

**Production path:**
```
node.rs:568
  // OLD:
  // let segment_reader = Arc::new(InMemorySegmentReader::new());

  // NEW:
  let segment_reader: Arc<dyn SegmentReader> = Arc::new(
      DiskSegmentReader::new(io_mode, disk_io, mmap_cache, segment_dir)
  );
```

**Test path:**
`InMemorySegmentReader` is kept in `oceanfs-storage` (moved from `oceanfs-server`)
for use in unit tests, benchmark scaffolding, and headless mode. It remains a
valid `SegmentReader` implementation.

### 8.2 Startup Preload Removal

The preload loop at `node.rs:570-584` iterates `blob_store.list_blobs()` and
calls `segment_reader.put()` for each. This loads ALL segment data into RAM.

With `DiskSegmentReader`, this loop is **removed**. Segment data is read from
disk on demand via the configured I/O mode. If `read_cache_segments = true`,
the `SegmentFileCache` warms up organically as reads occur. No startup
preload penalty.

### 8.3 PUT Path Change

Currently, `InMemorySegmentReader` is also used on PUT at `node.rs:578`:
`segment_reader.put(*id, data)` stores newly-written data in the HashMap so
subsequent GETs can find it before the segment sealer writes it to disk.

With `DiskSegmentReader`, the PUT path writes to disk (via `SegmentSealer`),
and the GET path reads from disk. The reader does not have a `put` method.
The data is available on disk immediately after `SegmentSealer::seal()`.
There's a brief window between ack-ing the client and the seal completing
— during this window, a GET would get a miss. This is handled by the
existing write-path design (async EC encoding, seal happens post-ack).

**For the transitional period** (before `SegmentSealer` is fully wired):
Keep the `InMemorySegmentReader` as a **decorator** around `DiskSegmentReader`:
- `put` writes to the in-memory store
- `read_chunk` checks in-memory first, falls back to disk

This is a temporary bridge until Epic 3 (write-path-unification) is complete.

```rust
// In node.rs, transitional wiring:
pub struct BridgingSegmentReader {
    memory: InMemorySegmentReader,     // fast path for recent writes
    disk: DiskSegmentReader,           // persistent path for sealed segments
}

impl SegmentReader for BridgingSegmentReader {
    fn read_chunk(&self, segment_id: &SegmentId, offset: u64, length: u32)
        -> Result<Bytes, String>
    {
        // Try in-memory first (recent writes not yet sealed to disk)
        if let Ok(data) = self.memory.read_chunk(segment_id, offset, length) {
            return Ok(data);
        }
        // Fall back to disk
        self.disk.read_chunk(segment_id, offset, length)
    }
}
```

Once Epic 3 is complete (segment sealer integration), the bridge is removed
and `DiskSegmentReader` is used directly.

---

## 9. Crate Impact Summary

| Crate | Change |
|-------|--------|
| `oceanfs-core` | New config field: `NodeConfig::segment_cache_max_entries` (usize, default 64) |
| `oceanfs-storage` | New module: `src/io/segment_reader.rs` with `DiskSegmentReader`, `SegmentReader` trait (moved from server), `SegmentReadSource` enum, `SegmentReaderExt` trait |
| `oceanfs-storage` | New: `DirectBufPool` (bounded pool of `DirectIoBuf` for O_DIRECT reads) |
| `oceanfs-storage` | Modify: `io/mod.rs` re-exports `segment_reader` module |
| `oceanfs-server` | Modify: `read/coordinator.rs` — remove `SegmentReader` trait definition, import from `oceanfs_storage`; remove `InMemorySegmentReader`; add `segment_source` to `GetResult` |
| `oceanfs-server` | Modify: `read/fetch.rs` — propagate `SegmentReadSource` from reader → coordinator |
| `oceanfs-server` | Modify: `s3_handler/handlers.rs` — use `SegmentFileBody` when source is file-backed |
| `oceanfs-node` | Modify: `node.rs` — construct `DiskSegmentReader` instead of `InMemorySegmentReader`; remove startup preload loop (or keep behind transitional bridge) |

**No new crate.** All changes are contained within existing crates.

**DAG validation:** `oceanfs-storage` gains a new module that depends on
existing `oceanfs-storage` I/O types. `oceanfs-server` already depends on
`oceanfs-storage`. No new crate dependency edges. No cycles introduced.

---

## 10. Open Questions

1. **`SegmentReader` trait home:** Should it live in `oceanfs-core` or
   `oceanfs-storage`? The design proposes `oceanfs-storage` since it's the
   natural home for storage-level abstractions. `oceanfs-core` is for
   truly cross-cutting types. The trait is only consumed by `oceanfs-server`
   (which already depends on `oceanfs-storage`).

2. **Per-bucket mode override:** Deferred to v2 or requires a clean
   trait extension design. The proposed `SegmentReaderExt::set_read_mode`
   is a workable v2 path. Does this need an ADR?

3. **`DirectBufPool` design:** The pool for `DirectIoBuf` recycling needs
   a capacity and a thread-safe implementation. A lock-free `ArrayQueue` of
   pre-allocated buffers with a semaphore for backpressure. This is a small
   implementation detail — does it warrant its own sub-feature?

4. **Segment file path resolution:** The `DiskSegmentReader` needs to know
   the filesystem path for a given `SegmentId`. Currently segment files are
   managed by `SegmentSealer` / `BlobStore`. The path convention
   (`{data_dir}/segments/{segment_id}.dat` or similar) needs to be
   confirmed. The `SegmentFileCache::get_or_map` already takes a `&Path`.

5. **Async in the trait:** Currently `SegmentReader::read_chunk` is
   synchronous (returns `Result<Bytes, String>`). The `DiskSegmentReader`
   needs async I/O (`tokio::fs::File::read_at`). This requires either:
   - Making `read_chunk` async (changing the trait)
   - Using `tokio::task::block_in_place` or a `Handle::block_on` inside
     the sync method
   
   The current code at `fetch.rs:410` calls `reader.read_chunk()` which
   is synchronous. Making the trait async would ripple through
   `fetch_single_chunk`, `fetch_chunks`, `assemble_chunks`, `get_object`,
   and the S3 handler (which is already async). This is the proper path
   but has significant blast radius.

6. **`DiskIo` path integration:** `DiskIo::read()` at `uring.rs:85-103`
   takes a `&Path` and opens a new `tokio::fs::File` per read call. For
   the segment reader, this means one `open` + `read` + `close` per chunk.
   For mmap mode, the `SegmentFileCache` amortizes this to one `mmap` call
   per segment. For direct/buffered, consider opening the file once and
   holding a handle — or rely on the OS page cache to make the reopen cheap.
   This is a performance tuning concern, not a correctness concern.

---

## 11. Rejected Alternatives

### Alternative A: Wrap InMemorySegmentReader with a disk fallback
**Rejected because:** The in-memory store has no eviction and would continue
to grow unbounded. Wrapping it behind a disk fallback hides the memory leak
rather than fixing it. The design must bound memory usage.

### Alternative B: Keep `SegmentReader` trait in `oceanfs-server`, impl in storage
**Rejected because:** `oceanfs-storage` cannot depend on `oceanfs-server`
(DAG violation — server depends on storage, not the reverse). Moving the
trait to `oceanfs-storage` (or `oceanfs-core`) is the only DAG-compliant
path.

### Alternative C: Return a `ReadHandle` enum from read_chunk
Instead of `Bytes`, return `ReadHandle::Memory(Bytes) | ReadHandle::File(File, offset, len)`.
**Rejected because:** This forces the assembler to handle the File variant
(open, seek, read into buffer), duplicating I/O logic. The assembler only
needs `&[u8]` — it shouldn't care where the bytes came from. The source
metadata travels alongside, not inside, the data.

### Alternative D: Per-bucket DiskSegmentReader instances
**Rejected because:** Would duplicate the mmap cache across buckets,
potentially mapping the same segment file N times if N buckets reference it.
A shared cache is more memory-efficient. Node-level mode with per-bucket
override (via `set_read_mode`) is the right granularity.

---

## 12. References

| File | Line(s) | Description |
|------|---------|-------------|
| `crates/oceanfs-server/src/read/coordinator.rs` | 153-165 | `SegmentReader` trait definition (to be moved) |
| `crates/oceanfs-server/src/read/coordinator.rs` | 171-195 | `ReadCoordinator` fields including `segment_reader` |
| `crates/oceanfs-server/src/read/coordinator.rs` | 909-1053 | `assemble_chunks()` — chunk fetch + assembly |
| `crates/oceanfs-server/src/read/coordinator.rs` | 313-353 | `get_object()` — returns `GetResult` with `data: Bytes` |
| `crates/oceanfs-server/src/read/coordinator.rs` | 1270-1322 | `InMemorySegmentReader` (to be replaced in production) |
| `crates/oceanfs-server/src/read/fetch.rs` | 396-427 | `fetch_single_chunk()` — calls `reader.read_chunk()` |
| `crates/oceanfs-server/src/s3_handler/handlers.rs` | 317-354 | GET handler — currently `Body::from(result.data)` |
| `crates/oceanfs-server/src/bucket_config.rs` | 292-321 | `ReadTuningConfig` with `read_cache_segments` |
| `crates/oceanfs-node/src/node.rs` | 567-584 | Construction of `InMemorySegmentReader` + preload |
| `crates/oceanfs-core/src/config/node.rs` | 103-117 | `NodeConfig::read_cache_segments`, `io_uring_enabled` |
| `crates/oceanfs-storage/src/io/mod.rs` | 54-86 | `IoReadMode` enum and `from_config()` |
| `crates/oceanfs-storage/src/io/mmap.rs` | 48-164 | `SegmentFileCache` — bounded LRU of `Arc<Mmap>` |
| `crates/oceanfs-storage/src/io/uring.rs` | 40-218 | `DiskIo` — io_uring / tokio::fs dispatcher |
| `crates/oceanfs-storage/src/io/direct.rs` | 28-122 | `DirectIoBuf` — page-aligned O_DIRECT buffer |
| `crates/oceanfs-storage/src/io/sendfile.rs` | 56-112 | `SegmentFileBody` — `http_body::Body` impl |
| `docs/features/performance-optimization/platform-io-optimizations/feature.md` | 1-224 | Platform I/O feature spec |
| `docs/spec.md` | 389-468 | Read path & caching spec (§5) |
| `guidelines/architecture.md` | 14-38 | Crate DAG |
| `guidelines/architecture.md` | 80-109 | Cross-crate coupling rules §2.1-2.2 |
