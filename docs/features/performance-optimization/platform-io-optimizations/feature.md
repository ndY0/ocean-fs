---
feature: "Platform I/O Optimizations"
epic: "performance-optimization"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: gap-closure-epic-3
    reason: "write-path-unification must wire the segment pipeline before platform I/O paths can be exercised and tested end-to-end"
adr:
  - 0001-segment-packing
perf:
  - "3.2 O_DIRECT for segment data files"
  - "3.3 mmap for hot segment reads"
  - "3.5 io_uring / tokio-uring for disk I/O"
  - "3.6 sendfile / splice for blob responses"
  - "10.6 Conditional platform-specific code paths"
  - "11.4 Criterion benchmarks for hot-path functions"
created: 2026-08-05
updated: 2026-08-05
---

# Platform I/O Optimizations

## Summary

Implement the four I/O performance guidelines (§3.2-3.6) that are currently
entirely unimplemented: `O_DIRECT` for segment data files, `mmap` for hot
segment reads, `io_uring`/`tokio-uring` for true async disk I/O on Linux,
and `sendfile`/`splice` for zero-copy blob responses from disk to network.
All platform-specific code is `#[cfg(target_os = "linux")]`-gated with
portable `tokio::fs` fallbacks per guideline §10.6. Code lives in a new
`oceanfs-storage/src/io/` module. This feature gates on gap-closure Epic 3
(write-path-unification) because the I/O paths cannot be tested until the
write path is wired end-to-end.

## Scope

### In Scope

- **§3.2: `O_DIRECT` for segment data files.** When `read_cache_segments = false`
  (write-optimized profile), open segment data files with
  `OpenOptions::new().custom_flags(libc::O_DIRECT)` on Linux. Bypass the OS
  page cache for large segment reads/writes. Requires aligned buffers: the
  data buffer, offset, and I/O length must all be multiples of the logical
  block size (typically 512 bytes). Implement a `DirectIoBuf` wrapper that
  allocates page-aligned memory (via `memmap2` or `posix_memalign`) for
  O_DIRECT-compatible buffers. Fall back to buffered I/O on non-Linux or
  when alignment cannot be guaranteed.

- **§3.3: `mmap` for hot segment reads.** When `read_cache_segments = true`
  (read-optimized profile), map frequently-accessed segment shard files with
  `memmap2::Mmap`. Zero-copy reads from the kernel page cache — data is
  faulted in on first access and evicted under memory pressure. The `Mmap`
  region is accessed as `&[u8]` via `bytemuck` for zero-copy slice
  operations. Fall back to `tokio::fs::read` when mmap is not available
  (e.g., on filesystems that don't support it) or when `read_cache_segments`
  is false.

- **§3.5: `io_uring` / `tokio-uring` for disk I/O.** On Linux 5.1+ with
  `io_uring` support, use `tokio-uring` for all disk I/O operations: WAL
  writes, segment reads/writes, flush/fsync. Implement a `DiskIo` enum-based
  abstraction:
  ```rust
  pub enum DiskIo {
      Uring(tokio_uring::IoUring),
      #[cfg(not(target_os = "linux"))]
      TokioFs,
  }
  impl DiskIo {
      pub async fn read(&self, path: &Path, buf: &mut [u8], offset: u64) -> Result<usize>;
      pub async fn write(&self, path: &Path, buf: &[u8], offset: u64) -> Result<()>;
      pub async fn sync(&self, file: &File) -> Result<()>;
      pub async fn open(&self, path: &Path, opts: OpenOptions) -> Result<File>;
  }
  ```
  Feature-gated behind `#[cfg(feature = "io-uring")]` in `Cargo.toml`.
  `tokio-uring` is an optional dependency. On systems without io_uring
  (macOS, older Linux kernels), `DiskIo::TokioFs` wraps `tokio::fs` as the
  fallback.

- **§3.6: `sendfile` / `splice` for blob responses.** When serving segment
  data from disk to an HTTP response, use `sendfile(2)` on Linux to copy
  data directly from the file descriptor to the socket — bypassing the
  userspace buffer entirely. In the HTTP handler, detect when the response
  body source is a file-backed `mmap` or file descriptor, and use
  `tokio::io::copy` which internally uses `sendfile` on Linux when both ends
  support it. For axum/tower integration: implement a `SegmentFileBody` that
  wraps a `tokio::fs::File` + offset/length and implements `http_body::Body`
  using `sendfile`. For non-file responses (inline blobs from cache/memory),
  continue using `Body::from(Bytes)` (zero-copy from memory).

- **Criterion benchmarks.** Add `benches/io_benchmark.rs` comparing:
  - `O_DIRECT` vs buffered read/write for 64KB, 1MB, 4MB segment sizes
  - `mmap` vs `tokio::fs::read` for random offset reads within a 4MB segment
  - `tokio-uring` vs `tokio::fs` for sequential write throughput
  - `sendfile` vs `read`+`write` for 4MB blob response path

- **Portable fallbacks.** All platform-specific code must have `#[cfg(not(target_os = "linux"))]`
  fallbacks per §10.6. Non-Linux platforms use `tokio::fs` for all I/O.

### Out of Scope (for this feature)

- **WAL fsync fix** — handled by Feature 1 QW-2 (this feature provides the
  io_uring sync path as an additional optimization once the fsync is wired)
- **RocksDB I/O tuning** — handled by Feature 4 (rocksdb-tuning). RocksDB
  manages its own I/O; this feature only addresses OceanFS-managed file I/O.
- **Write path architectural wiring** — handled by gap-closure Epic 3. This
  feature adds I/O paths that the wired write path will exercise.
- **EC encode/decode I/O** — acceleration audit I/O is CPU/GPU, not disk.
- **WAL group commit architecture** — already implemented (write-path audit
  §3.4). This feature optionally adds io_uring-based fsync to the group
  commit path.
- **`target-cpu=native` / PGO** — compile-time optimizations §10.4-10.5,
  tracked separately.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | New module `src/io/` with `direct.rs` (O_DIRECT), `mmap.rs` (mmap), `uring.rs` (io_uring), `sendfile.rs` (sendfile/splice), `mod.rs` (DiskIo facade). |
| `oceanfs-storage` | Modify `src/segment/sealer.rs` to use `O_DIRECT`/`mmap` based on config. Modify `src/segment/reader.rs` to use `mmap` when enabled. Modify `src/wal/writer.rs` to optionally use io_uring for writes. |
| `oceanfs-server` | Modify `src/s3_handler/handlers.rs` to use `SegmentFileBody` (sendfile) for file-backed segment responses instead of `Body::from(Vec<u8>)`. |
| `oceanfs-core` | New config fields: `read_cache_segments: bool`, `io_uring_enabled: bool` (Linux-only). Extend `NodeConfig` and `BucketPolicy`. |

## Interface (Public API)

- `pub struct DirectIoBuf` — page-aligned buffer for O_DIRECT I/O. Implements
  `AsRef<[u8]>`, `AsMut<[u8]>`, `Deref<Target=[u8]>`. Allocates via
  `posix_memalign` or `memmap2` anonymous mapping.
- `pub enum DiskIo` — I/O backend dispatcher (see Scope §3.5). Selects
  `Uring` or `TokioFs` based on platform + config.
- `pub enum IoReadMode { Direct, Buffered, Mmap }` — read strategy, resolved
  from config.
- `pub struct SegmentFileBody` — implements `http_body::Body` for sendfile-based
  responses. Wraps a `tokio::fs::File` + byte range.
- `impl DiskIo` — async `read`, `write`, `sync`, `open` methods.

## Data Flow

**O_DIRECT write path (write-optimized profile):**
```
Segment sealed → SegmentSealer::seal()
  ├─ read_cache_segments? → false
  ├─ DirectIoBuf::with_capacity(segment_size)  // page-aligned allocation
  ├─ copy segment data into DirectIoBuf (aligned)
  ├─ DiskIo::write(&file, &aligned_buf, 0)?    // O_DIRECT via io_uring or libc
  └─ DiskIo::sync(&file)?                       // fdatasync via io_uring or libc
```

**mmap read path (read-optimized profile):**
```
GET /bucket/key → ReadCoordinator → segment read
  ├─ read_cache_segments? → true
  ├─ SegmentFileCache::get(segment_id)?
  │     ├─ cache hit → return Arc<Mmap>       // zero-copy, already mapped
  │     └─ cache miss → memmap2::Mmap::map(&file)?
  │           ├─ insert into SegmentFileCache (bounded LRU)
  │           └─ return Arc<Mmap>
  ├─ read segment header from &mmap[0..76]
  ├─ lookup blob: &mmap[blob_offset..blob_offset+blob_len]
  └─ return Bytes slice from mmap             // zero-copy to response
```

**sendfile response path:**
```
GET /bucket/key → S3 handler → response body
  ├─ blob stored in segment file (not inline)
  ├─ SegmentFileBody::new(file, offset, length)
  │     └─ impl http_body::Body
  │           └─ poll_frame():
  │                 └─ sendfile(file_fd, socket_fd, offset, length)
  │                       // kernel-space copy: disk page cache → socket buffer
  │                       // NO userspace buffer involved
  └─ Response::new(SegmentFileBody)
```

## Definition of Done

- [ ] **O_DIRECT:** `DirectIoBuf` wrapper implemented. Segment file opens use
  `custom_flags(libc::O_DIRECT)` when `read_cache_segments = false`.
  `SegmentSealer::seal()` writes through O_DIRECT when configured. Buffer
  alignment validated at test time. Fallback to buffered I/O on non-Linux.
- [ ] **mmap:** `memmap2::Mmap` used for segment reads when
  `read_cache_segments = true`. `SegmentFileCache` (bounded LRU, `DashMap`-based)
  caches open `Arc<Mmap>` handles. Zero-copy read path: mmap region → `&[u8]`
  → `Bytes` response (via `Bytes::from` copying only the needed slice).
- [ ] **io_uring:** `DiskIo` enum implemented with `Uring` variant
  (`#[cfg(feature = "io-uring")]`). `tokio-uring` added as optional dependency.
  `DiskIo::TokioFs` fallback on non-Linux or when feature disabled. WAL writer
  optionally uses io_uring for `write_all` + `sync_all`. Criterion benchmarks
  show throughput improvement.
- [ ] **sendfile:** `SegmentFileBody` implements `http_body::Body` using
  `sendfile` syscall on Linux. S3 GET handler selects `SegmentFileBody` when
  the blob source is a file-backed segment (not inline/memory). Non-Linux
  fallback uses buffered `tokio::io::copy`.
- [ ] **Config:** `NodeConfig` gains `read_cache_segments: bool` (default
  `false`) and `io_uring_enabled: bool` (Linux default `true`, macOS ignored).
  `BucketPolicy` gains `read_cache_segments` override.
- [ ] **Code:** `cargo build --all-targets` succeeds on Linux (with and without
  `io-uring` feature). Cross-compilation to macOS succeeds (io_uring feature
  disabled, tokio::fs fallback). No `#[cfg(target_os)]` causes dead-code
  warnings on either platform.
- [ ] **Tests:** All existing segment/wal tests pass. New tests: `DirectIoBuf`
  alignment + read/write; `DiskIo` fallback selection; `SegmentFileBody`
  sendfile vs read fallback (integration test serving a real file); mmap
  read + cache eviction; O_DIRECT write + read roundtrip.
- [ ] **Docs:** Module-level doc in `src/io/mod.rs` describes the I/O
  strategy selection logic. `DiskIo` has `# Examples`. `SegmentFileBody` doc
  explains the sendfile optimization.
- [ ] **ADR:** ADR-0001 (segment packing) constraints satisfied — segment
  I/O paths work for both small (64KB) and standard (4MB) segment sizes.
- [ ] **Perf:** Criterion benchmarks in `benches/io_benchmark.rs` show:
  O_DIRECT write within 5% of buffered write throughput; mmap random read
  ~2× faster than `tokio::fs::read`; io_uring write throughput ≥ buffered
  `tokio::fs` throughput; sendfile response time ~30% faster than `read`+`write`
  for 4MB blobs.
- [ ] **Integration:** End-to-end S3 PUT/GET flow exercises all four I/O paths
  when configured. Segment written via O_DIRECT, read via mmap, served via
  sendfile — end-to-end zero-copy from disk to network.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and `ignore`-tagged
> doc examples are non-blocking (see `guidelines/coding.md` §9.2.1).
