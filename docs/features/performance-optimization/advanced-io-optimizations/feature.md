---
feature: "Advanced I/O Optimizations"
epic: "performance-optimization"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: performance-optimization
    feature: platform-io-optimizations
    reason: "O_DIRECT, mmap, and io_uring infrastructure from Feature 3 provides the I/O abstraction layer that these advanced syscall-level optimizations plug into."
  - epic: gap-closure-epic-3
    reason: "write-path-unification must wire the segment pipeline before WAL and segment I/O paths can be tested end-to-end."
adr:
  - 0001-segment-packing
perf:
  - "3.1 Sequential-only WAL writes"
  - "3.4 Group commit for WAL fsync"
  - "3.5 io_uring / tokio-uring for disk I/O"
  - "10.6 Conditional platform-specific code paths"
  - "11.4 Criterion benchmarks for hot-path functions"
created: 2026-08-05
updated: 2026-08-05
---

# Advanced I/O Optimizations

## Summary

Six syscall-level I/O and scheduling optimizations that build on the
platform I/O infrastructure (Feature 3: O_DIRECT, mmap, io_uring,
sendfile) to further reduce latency and CPU overhead. Each is a targeted
improvement to a specific I/O path — WAL fsync, segment write, segment
read, background task scheduling, and block cache pinning. None change
the architecture. Combined they address the dominant remaining I/O
bottlenecks: fsync latency (1-10ms on NVMe), page cache pollution from
segment reads, and background-task interference with foreground I/O.
All platform-specific code is `#[cfg(target_os = "linux")]`-gated with
portable fallbacks per guideline §10.6. Code lives primarily in
`oceanfs-storage` (WAL, segment I/O) and `oceanfs-node` (thread
scheduling).

This feature **builds on** Feature 3 (platform-io-optimizations) which
establishes the `DiskIo` abstraction and `O_DIRECT`/`mmap`/`io_uring`
paths. Where Feature 3 provides the infrastructure, Feature 6 applies
syscall-level refinements: replacing `sync_all` with cheaper
`sync_file_range`+`fdatasync`, using `O_TMPFILE` for atomic segment
creation, adding `madvise` hints on segment reads, and setting I/O/CPU
scheduling classes on background threads.

## Scope

### In Scope

- **`sync_file_range` + `fdatasync` for WAL group commit.** The WAL
  group-commit path (wired by Feature 1 QW-2) currently calls
  `file.sync_data()` or `file.sync_all()`. `sync_all` flushes both
  data and inode metadata (file size, mtime) — two disk barriers. For
  an append-only WAL where the file already exists and metadata updates
  are deferred, `sync_file_range(fd, offset, 0,
  SYNC_FILE_RANGE_WRITE)` (start write-out of dirty pages in the
  specified range, non-blocking) followed by `fdatasync(fd)` (flush
  data only, skip inode metadata) is cheaper. On NVMe, measured at
  2-3× faster than full `sync_all` because it saves one disk barrier.
  This replaces the current `sync_all()` call in the group-commit
  closure with the Linux-specific path, falling back to `sync_data()`
  on non-Linux. Implementation: the `WalSyncGroup` flusher task opens
  the WAL file with an additional `File` handle for the sync path,
  tracks the byte range written since last sync, and calls
  `sync_file_range` + `fdatasync` on that range. After sync, update
  the persisted offset watermark.

- **`O_TMPFILE` + `linkat` for atomic segment writes.** On Linux 3.11+,
  the segment write sequence can be simplified: instead of:
  create temp file → write data → fsync → `rename()` (two directory
  operations, a window where a partial file is visible), use:
  `open("/segment/dir", O_TMPFILE | O_WRONLY, mode)` (creates an
  unnamed, invisible file) → write data → fsync →
  `linkat(AT_FDCWD, "/proc/self/fd/N", dirfd, "segment-name",
  AT_SYMLINK_FOLLOW)` (atomically links the unnamed file into the
  directory). Benefits: one fewer directory operation, zero window
  where a partial file is visible to readers, and the segment name
  never exists until the data is fully durable. The `SegmentSealer`
  gains an `AtomicSegmentWrite` strategy that uses `O_TMPFILE` when
  available, falling back to the `rename`-based path on older kernels
  or non-Linux. Feature-detection: probe `O_TMPFILE` support once at
  startup by attempting to create a test `O_TMPFILE` in the segment
  data directory.

- **`madvise(MADV_SEQUENTIAL)` + `MADV_DONTNEED` on segment reads.**
  When serving a GET that reads a full segment from disk (mmap or
  buffered read path): (1) before reading, call `madvise(addr, len,
  MADV_SEQUENTIAL)` on the mapped region — tells the kernel to do
  aggressive read-ahead and treat the access pattern as a single
  forward scan, (2) after reading and serving the response, call
  `madvise(addr, len, MADV_DONTNEED)` — tells the kernel to eagerly
  evict these pages from the page cache. Segment reads are large
  (64 KB to 4 MB), sequential, and not re-read frequently per segment
  (hot segments are cached by L1 object cache, not re-read from disk).
  Without `MADV_DONTNEED`, the kernel keeps the full segment in the
  page cache, eventually evicting hot metadata, WAL pages, and L1/L2
  cache entries that should be resident. The `madvise` calls go in
  the `SegmentReader` path, gated on `read_cache_segments = false`
  (when mmap caching is disabled, eviction after read is correct;
  when caching is enabled, skip `MADV_DONTNEED`). `#[cfg(target_os =
  "linux")]` gated; no-op on other platforms.

- **`ioprio_set(IOPRIO_CLASS_IDLE)` for GC/scrub/anti-entropy threads.**
  Set the I/O scheduling class to `IOPRIO_CLASS_IDLE` for all
  background task threads: garbage collection, segment compaction,
  active scrubbing, Merkle tree anti-entropy exchange, and hinted
  handoff delivery. Threads with `IOPRIO_CLASS_IDLE` only receive disk
  I/O bandwidth when no other thread (client reads/writes, WAL syncs,
  segment writes) wants it. Without this, a scrub cycle scanning
  hundreds of segments can spike GET latency by competing for NVMe
  command slots and disk bandwidth. Implementation: after spawning
  each background task thread, call `libc::ioprio_set(
  libc::IOPRIO_WHO_PROCESS, 0, libc::IOPRIO_PRIO_VALUE(
  libc::IOPRIO_CLASS_IDLE, 0))`. This is a per-thread (actually
  per-process for these threads, since each background task is its own
  tokio task on a thread) setting. `#[cfg(target_os = "linux")]`
  gated.

- **`SCHED_IDLE` for GC/scrub/anti-entropy threads.** Same as above but
  for CPU scheduling. Set the thread's scheduling policy to
  `SCHED_IDLE` via `libc::sched_setscheduler(0, libc::SCHED_IDLE,
  &param)`. These threads only execute when no other thread wants the
  CPU — they literally run in idle CPU time. The background task
  spawning code in `oceanfs-node/src/node.rs` applies this along with
  `ioprio_set`. Combined, background tasks cannot steal either CPU
  cycles or disk bandwidth from client-facing work. `#[cfg(target_os =
  "linux")]` gated. Note: `SCHED_IDLE` requires `CAP_SYS_NICE` or
  running as root. Document this as a deployment requirement or
  gracefully degrade if `set_scheduler` fails with `EPERM`.

- **`mlock` for RocksDB block cache.** Pin the RocksDB block cache in
  physical RAM using `mlock(2)` to prevent the kernel from swapping it
  under memory pressure. On a storage node, swapping the block cache
  is worse than OOM — it turns microsecond L3 cache lookups into
  millisecond disk reads, cascading into request timeouts and cluster
  instability. This is a defense-in-depth measure: even if the node is
  under memory pressure and the kernel decides to swap anonymous pages,
  the block cache stays resident. Implementation: after RocksDB is
  opened, obtain a reference to the block cache via
  `rocksdb::BlockBasedOptions::block_cache()` and call `libc::mlock()`
  on the cache's memory region. If `mlock` fails (e.g., `mlock` limit
  reached, `CAP_IPC_LOCK` not held), log a `WARN` and continue without
  pinning — the system operates correctly, just without the swap-defense
  guarantee. `#[cfg(target_os = "linux")]` gated. Document the
  `CAP_IPC_LOCK` capability requirement in deployment docs.

### Out of Scope (for this feature)

- **O_DIRECT, mmap, io_uring, sendfile infrastructure.** Already covered
  by Feature 3 (platform-io-optimizations).
- **WAL fsync wiring.** Already covered by Feature 1 QW-2. This feature
  replaces the `sync_all` call with a cheaper equivalent after it is
  wired.
- **RocksDB I/O tuning (compaction, WAL, flushes).** Already covered
  by Feature 4 (rocksdb-tuning).
- **`mlock` for non-RocksDB memory.** The L1 object cache and L2
  metadata cache live in userspace (DashMap/BTreeMap backed by heap).
  Pinning those is a separate consideration — they are explicitly
  sized by the operator and the kernel's LRU page reclaim should keep
  them resident under normal operation.
- **NUMA-aware memory placement for block cache.** Separate optimization
  requiring `libnuma` and `numa_alloc_onnode`. This feature only
  addresses swap defense.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | New module `src/io/atomic_write.rs` (`O_TMPFILE` + `linkat` segment write). Modify `src/io/mod.rs` for `madvise` integration in segment reader. Modify `src/wal/sync.rs` to use `sync_file_range` + `fdatasync` instead of `sync_all`. |
| `oceanfs-node` | Modify `src/node.rs` background task spawning to apply `ioprio_set(IOPRIO_CLASS_IDLE)` and `SCHED_IDLE` to GC, scrub, anti-entropy, and hinted-handoff threads. |
| `oceanfs-storage` | Modify `src/metadata/store.rs` (RocksDB initialization) to call `mlock` on the block cache after open. |
| `oceanfs-core` | New config fields: `wal_use_sync_file_range: bool` (default `true` on Linux), `background_io_class_idle: bool` (default `true` on Linux), `background_cpu_sched_idle: bool` (default `true` on Linux), `mlock_block_cache: bool` (default `true` on Linux). |

## Interface (Public API)

- `pub enum SegmentWriteMode { Rename, Tmpfile }` — strategy for atomic
  segment writes. Resolved from kernel version at startup.
- `pub(crate) fn apply_background_io_class(thread_name: &str)` in
  `oceanfs-node::node` — applies `ioprio_set(IOPRIO_CLASS_IDLE)` to
  the calling thread. No-op on non-Linux.
- `pub(crate) fn apply_background_cpu_sched(thread_name: &str)` in
  `oceanfs-node::node` — applies `SCHED_IDLE` to the calling thread.
  No-op on non-Linux.
- No new public types exposed outside the crate boundaries. All
  optimizations are internal quality-of-implementation improvements.

## Data Flow

**WAL sync with `sync_file_range` + `fdatasync`:**
```
WalWriter::append() → WalSyncGroup::submit()
  ├─ [N concurrent waiters batched, max 5ms or 64 waiters]
  ├─ [flusher task wakes]
  ├─ sync_file_range(fd, last_synced_offset, bytes_written,
  │                   SYNC_FILE_RANGE_WRITE)  // start write-out (non-blocking)
  ├─ fdatasync(fd)                             // flush data pages only
  ├─ update last_synced_offset
  └─ wake all N waiters (oneshot::Sender::send)
```
Benefit: `fdatasync` is one disk barrier vs `sync_all`'s two.
`sync_file_range` initiates write-out asynchronously so the disk
controller can pipeline the write with the subsequent `fdatasync`.

**Atomic segment write with `O_TMPFILE`:**
```
Segment sealed → SegmentSealer::write(shards)
  ├─ for each shard:
  │     fd = open(segment_dir, O_TMPFILE | O_WRONLY, 0644)
  │     // fd is unnamed — not visible in directory
  │     pwrite(fd, shard_data, 0)
  │     fdatasync(fd)
  │     linkat(AT_FDCWD, "/proc/self/fd/{fd}", dirfd, "segment-{id}.shard",
  │            AT_SYMLINK_FOLLOW)
  │     // NOW the file exists — atomically
  │     close(fd)
  └─ update segment metadata in RocksDB
```
Contrast with `rename`-based path:
```
  create temp file → write → fsync → rename → [window: partial visible] → close
```

**Segment read with `madvise` hints:**
```
GET /bucket/key → ReadCoordinator → segment read (non-cached mmap path)
  ├─ mmap segment file
  ├─ madvise(addr, len, MADV_SEQUENTIAL)     // kernel: aggressive read-ahead
  ├─ read blob data from &mmap[offset..offset+len]
  ├─ serve response
  ├─ madvise(addr, len, MADV_DONTNEED)       // kernel: evict these pages ASAP
  └─ munmap
```

**Background thread scheduling:**
```
Node startup:
  ├─ spawn GC task
  │     └─ [inside task] apply_background_io_class("gc")
  │     └─ [inside task] apply_background_cpu_sched("gc")
  ├─ spawn scrub task
  │     └─ [inside task] apply_background_io_class("scrub")
  │     └─ [inside task] apply_background_cpu_sched("scrub")
  ├─ spawn anti-entropy task
  │     └─ [inside task] apply_background_io_class("anti-entropy")
  │     └─ [inside task] apply_background_cpu_sched("anti-entropy")
  └─ spawn hinted-handoff task
        └─ [inside task] apply_background_io_class("hinted-handoff")
        └─ [inside task] apply_background_cpu_sched("hinted-handoff")
```

## Definition of Done

- [ ] **`sync_file_range` + `fdatasync`:** WAL group commit flusher uses
  `sync_file_range` + `fdatasync` on Linux. Track the `last_synced_offset`
  watermark in the `WalSyncGroup`. Fall back to `sync_data()` on non-Linux.
  WAL durability tests pass (power-loss simulation, crash-recovery test).
  Criterion benchmark: WAL append+sync latency reduced by ≥30% vs `sync_all`
  on NVMe.

- [ ] **`O_TMPFILE`:** `AtomicSegmentWrite` enum and `Tmpfile` variant
  implemented in `oceanfs-storage/src/io/atomic_write.rs`. Startup probe
  tests `O_TMPFILE` support. Segment writes use the `Tmpfile` path when
  available. Segment durability test: verify that a crash between `write`
  and `linkat` leaves no partial file visible (the unnamed file is
  automatically cleaned by the kernel). Fall back to `rename` path on
  older kernels.

- [ ] **`madvise` hints:** Segment reader calls `madvise(MADV_SEQUENTIAL)`
  before read and `madvise(MADV_DONTNEED)` after read when
  `read_cache_segments = false`. No-op on non-Linux. Integration test:
  verify that after a 4 MB segment read, the page cache usage does not
  increase (pages are evicted). Benchmark: segment read latency unchanged
  or slightly improved; system page cache hit rate for metadata/WAL
  improves under concurrent read+write load.

- [ ] **`ioprio_set`:** All background task threads call
  `ioprio_set(IOPRIO_CLASS_IDLE)` at task start. `#[cfg(target_os =
  "linux")]` gated. Integration test (manual, requires `ionice`): verify
  with `ionice -p <tid>` that background threads show `idle` scheduling
  class. Foreground threads remain at `best-effort`.

- [ ] **`SCHED_IDLE`:** All background task threads call
  `sched_setscheduler(SCHED_IDLE)` at task start. Captures and logs
  `EPERM` gracefully if capability not held. `#[cfg(target_os =
  "linux")]` gated. Verify with `chrt -p <tid>` that background threads
  show `SCHED_IDLE`.

- [ ] **`mlock`:** After RocksDB opens, call `mlock` on the block cache
  memory region. Log `WARN` if `mlock` fails (e.g., `CAP_IPC_LOCK` not
  held). Verify with `/proc/<pid>/status VmLck` that the block cache
  size appears in locked memory. Swapping test (manual): under memory
  pressure (e.g., `stress-ng --vm 4`), verify that the block cache
  remains resident while anonymous pages are swapped.

- [ ] **Config:** New config fields added to `oceanfs-core` types with
  sensible defaults: `wal_use_sync_file_range = true`,
  `background_io_class_idle = true`, `background_cpu_sched_idle = true`,
  `mlock_block_cache = true`. All configurable per node only, not per bucket.

- [ ] **Code:** `cargo build --all-targets` succeeds on Linux.
  Cross-compilation to macOS succeeds (all `#[cfg]` gates correctly
  exclude Linux-specific code).

- [ ] **Tests:** All existing WAL, segment, and node tests pass. New tests:
  `sync_file_range` + `fdatasync` vs `sync_all` latency comparison (criterion),
  `O_TMPFILE` atomicity test (kill -9 during write → no partial file),
  `madvise` page cache eviction test, `ioprio_set`/`SCHED_IDLE` capability
  handling test.

- [ ] **Docs:** Module-level docs in `src/io/atomic_write.rs` and
  `src/wal/sync.rs` explain the optimization. Deployment docs note
  `CAP_SYS_NICE` (for `SCHED_IDLE`) and `CAP_IPC_LOCK` (for `mlock`)
  requirements.

- [ ] **ADR:** ADR-0001 (segment packing) constraints satisfied — segment
  write paths work for both small (64 KB) and standard (4 MB) segment
  sizes. `O_TMPFILE` path handles all segment sizes identically.

- [ ] **Perf:** Criterion benchmarks show: WAL sync latency reduced
  ≥30% on NVMe with `sync_file_range`+`fdatasync`; segment write latency
  unchanged (no regression from O_TMPFILE); segment read page cache
  pollution reduced (fewer metadata/WAL evictions under concurrent load).

- [ ] **Integration:** End-to-end S3 PUT (exercises WAL sync + atomic
  segment write) and GET (exercises madvise hints) pass. Background task
  scheduling verified via `ps` and `/proc` inspection.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and `ignore`-tagged
> doc examples are non-blocking (see `guidelines/coding.md` §9.2.1).
