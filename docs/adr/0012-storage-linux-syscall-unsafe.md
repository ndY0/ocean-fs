# ADR-0012: Extend `unsafe` in `oceanfs-storage` for Linux Syscall Wrappers

**Status:** Proposed
**Date:** 2026-08-08
**Deciders:** architecture team

---

## Context

[ADR-0011] authorized `unsafe` code in `oceanfs-storage` **only** for
`memmap2::Mmap` segment I/O, with an explicit clause:

> If a future requirement needs additional `unsafe` in `oceanfs-storage`,
> a new ADR is required.

The [Advanced I/O Optimizations] feature introduces four new categories
of `unsafe` in `oceanfs-storage`, all for well-known Linux syscall
wrappers that improve WAL throughput, segment write atomicity, page
cache management, and background thread scheduling. Each category:

- Is `#[cfg(target_os = "linux")]`-gated with portable fallbacks on
  non-Linux platforms per performance guideline §10.6.
- Has `#[allow(unsafe_code)]` on each individual `unsafe` block with
  `// SAFETY:` comments documenting the invariants.
- Involves well-known, simple syscalls with trivial safety invariants:
  valid file descriptors, advisory-only hints the kernel may ignore,
  no shared mutable state.
- Is already implemented in the working tree — this ADR retroactively
  approves the unsafe usage and amends the scope established by
  [ADR-0011].

The four categories are:

1. **`sync_file_range` + `fdatasync`** — WAL group-commit range-sync
   (file: `crates/oceanfs-storage/src/wal/sync.rs`).
2. **`open(O_TMPFILE)` + `linkat`** — atomic segment writes invisible
   to readers until fully synced
   (file: `crates/oceanfs-storage/src/io/atomic_write.rs`).
3. **`madvise(MADV_SEQUENTIAL, MADV_DONTNEED)`** — page cache hints
   for segment reads
   (files: `crates/oceanfs-storage/src/io/segment_reader.rs`,
   `crates/oceanfs-storage/src/io/mmapped.rs`).
4. **`ioprio_set(IOPRIO_CLASS_IDLE)` + `sched_setscheduler(SCHED_IDLE)`**
   — background thread I/O and CPU scheduling hints
   (file: `crates/oceanfs-storage/src/io/sched.rs`).

Without ADR approval, these `unsafe` blocks are technically in violation
of [ADR-0011]'s scoping clause, even though the code follows all other
safety guidelines (§7.2 `deny + allow` pattern, §12.1 `// SAFETY:`
comments). This ADR retroactively extends the scope to cover them.

The architecture guideline §7.2 already states (as amended by ADR-0011):

> Limited to mmap operations; new unsafe use-cases require a new ADR.

This ADR fulfills that requirement.

> **Note on `mlock`:** The [Advanced I/O Optimizations] feature also
> includes `mlock` for the RocksDB block cache, which uses `unsafe`
> via `libc::mlock`. That category is **not** covered by this ADR — it
> lives in `oceanfs-storage/src/metadata/` and has different safety
> considerations (physical memory pinning, `CAP_IPC_LOCK` requirements,
> potential for resource exhaustion). If `mlock` is to be permitted in
> `oceanfs-storage`, it requires its own ADR.

## Decision

**Extend the scope of permitted `unsafe` in `oceanfs-storage` to cover
four categories of Linux syscall wrappers**, as detailed below. The
existing `#![deny(unsafe_code)]` crate-level lint, per-item
`#[allow(unsafe_code)]` annotation pattern, and `// SAFETY:` comment
requirement are unchanged.

Amend architecture guideline §7.2 to add the four categories to the
`oceanfs-storage` entry's scope note, replacing "mmap segment I/O only"
with the broader enumeration below.

### Category 1: WAL Range-Sync via `sync_file_range` + `fdatasync`

**Location:** `crates/oceanfs-storage/src/wal/sync.rs`

**Syscalls:**
- `libc::sync_file_range(fd, offset, length, SYNC_FILE_RANGE_WRITE)` —
  initiates write-back of dirty pages in the specified byte range
  (non-blocking).
- `std::fs::File::sync_data()` — flushes data pages only, skipping
  inode metadata (file size, mtime). On Linux this maps to `fdatasync(2)`.

**Safety invariants:**
- `fd` is a valid, open file descriptor obtained from
  `std::fs::File::as_raw_fd()`. The file is opened by the `WalSyncGroup`
  flusher and is guaranteed to remain open for the lifetime of the
  function call.
- `offset` and `length` describe the byte range written since the last
  sync, tracked by a `last_synced_offset` watermark. The range is
  bounded by the actual bytes written via `pwrite`/`write`.
- `SYNC_FILE_RANGE_WRITE` is advisory — it starts write-out but does
  not wait for completion. It cannot cause data corruption even if
  called with invalid parameters (the kernel returns `EINVAL` or
  `EIO`).

**Fallback:** On non-Linux platforms, the function calls
`file.sync_data()` directly, skipping `sync_file_range`. The
`offset`/`length` pair provides no benefit without the syscall, but
the fallback is correct: `sync_data()` flushes all dirty data pages.

**Why `unsafe` is needed:** `libc::sync_file_range` is an `unsafe`
FFI call. There is no safe Rust wrapper in the standard library.

### Category 2: Atomic Segment Writes via `open(O_TMPFILE)` + `linkat`

**Location:** `crates/oceanfs-storage/src/io/atomic_write.rs`

**Syscalls:**
- `libc::open(dir, O_RDONLY | O_DIRECTORY)` — opens the segment
  directory (used to obtain `dir_fd` for `linkat`).
- `std::fs::OpenOptions::new().custom_flags(libc::O_TMPFILE)` — creates
  an unnamed, invisible temporary file in the segment directory. The
  file has no directory entry until linked.
- `libc::linkat(AT_FDCWD, "/proc/self/fd/{fd}", dir_fd, filename,
  AT_SYMLINK_FOLLOW)` — atomically links the unnamed `O_TMPFILE` inode
  into the directory under its final segment filename.
- `libc::close(dir_fd)` — closes the directory file descriptor.

**Safety invariants:**
- **`open(O_TMPFILE)`:** The `O_TMPFILE` flag is used with
  `std::fs::OpenOptions` on a valid directory path. The created file is
  unnamed — no other process or thread can open it by name. The file
  descriptor is exclusively owned by the calling function and dropped
  at scope exit (both on success and panic via `Drop`).
- **`open(O_RDONLY | O_DIRECTORY)`:** Opens a directory for reading
  only. The returned `fd` is used solely as a directory file descriptor
  for `linkat`. It is closed (via `libc::close`) before the function
  returns. The path is a valid directory that is guaranteed to exist
  (created at node startup).
- **`linkat`:** `dir_fd` is a valid directory file descriptor.
  `"/proc/self/fd/{fd}"` refers to the open file description of the
  `O_TMPFILE` inode — this magic path is guaranteed to resolve as long
  as the fd is open (which it is, until the `linkat` completes).
  `filename_c` is a valid, null-terminated C string for the target
  segment filename. `AT_SYMLINK_FOLLOW` resolves the `/proc/self/fd`
  symlink to the actual inode. If `linkat` fails, the `O_TMPFILE` inode
  is cleaned up by the kernel when the last fd is closed (on `Drop`).

**Fallback:** `SegmentWriteMode::Rename` uses the traditional path:
create temp file → write → fsync → `std::fs::rename`. This is portable
and works everywhere. The mode is selected at startup via
`SegmentWriteMode::probe()`, which tests `O_TMPFILE` support by
attempting to create a test file and checking for `EOPNOTSUPP`,
`EINVAL`, or `ENOENT`.

**Why `unsafe` is needed:** `libc::linkat`, `libc::open` (raw fd),
`libc::close`, and `OpenOptionsExt::custom_flags()` with
`libc::O_TMPFILE` are all `unsafe` FFI boundaries. There is no safe
Rust standard library API for `O_TMPFILE` or `linkat`.

### Category 3: Page Cache Hints via `madvise(MADV_SEQUENTIAL, MADV_DONTNEED)`

**Location:** `crates/oceanfs-storage/src/io/segment_reader.rs`
(functions `madvise_sequential` and `madvise_dontneed`),
`crates/oceanfs-storage/src/io/mmapped.rs`.

**Syscalls:**
- `libc::madvise(addr, len, MADV_SEQUENTIAL)` — hints that the mapped
  region will be accessed sequentially, enabling aggressive kernel
  read-ahead.
- `libc::madvise(addr, len, MADV_DONTNEED)` — hints that the mapped
  region will not be accessed again soon, enabling eager page cache
  eviction (used only when `read_cache_segments = false`).

**Safety invariants:**
- `addr` points to a valid memory-mapped region of `len` bytes,
  obtained from `memmap2::Mmap::as_ptr()` or `mmap` return value.
- Both `MADV_SEQUENTIAL` and `MADV_DONTNEED` are **purely advisory
  hints**. The kernel may ignore them. They cannot cause undefined
  behavior — if the address is invalid, the kernel returns `EINVAL`
  or `ENOMEM` (an error code, not a signal or fault). There is no
  path from these flags to memory corruption, use-after-free, or
  data races.
- `MADV_DONTNEED` is called **only** after the read is complete and the
  data has been served to the client. The mapped pages are no longer
  referenced by any Rust code. If the kernel evicts them, subsequent
  reads will page-fault and re-read from disk — correct behavior for
  non-cached segments.

**Fallback:** Both functions are conditionally compiled:
`#[cfg(target_os = "linux")]`. On non-Linux platforms, the callers skip
the `madvise` calls entirely. Read behavior is unchanged — no hints
are provided, and the kernel uses its default page cache policy.

**Why `unsafe` is needed:** `libc::madvise` is an `unsafe` FFI call.
There is no safe Rust wrapper in the standard library.

### Category 4: Background Thread Scheduling via `ioprio_set` + `sched_setscheduler`

**Location:** `crates/oceanfs-storage/src/io/sched.rs`

**Syscalls:**
- `libc::ioprio_set(IOPRIO_WHO_PROCESS, 0, IOPRIO_CLASS_IDLE)` — sets
  the I/O scheduling class of the calling thread to idle, meaning it
  only receives disk bandwidth when no other thread wants it.
- `libc::sched_setscheduler(0, SCHED_IDLE, &param)` — sets the CPU
  scheduling policy of the calling thread to idle, meaning it only
  executes when no other runnable thread exists.

**Safety invariants:**
- Both syscalls operate on **the calling thread only** (pid=0 means
  "current thread"). They cannot affect other threads or processes.
- Both are **advisory scheduling hints**. The kernel may choose to
  schedule the thread differently under load. They cannot cause
  undefined behavior, data corruption, or deadlocks.
- `IOPRIO_CLASS_IDLE` requires no special privileges — any thread can
  lower its own I/O priority.
- `SCHED_IDLE` requires `CAP_SYS_NICE`. The code catches `EPERM`
  gracefully: it logs an info message and continues with normal CPU
  scheduling. The function signature documents this in its doc comment.
- No shared mutable state is accessed. The calls are made once at
  thread startup before any work begins.

**Fallback:** Both functions are `#[cfg(target_os = "linux")]`-gated
with no-op stubs on non-Linux platforms. The function signature accepts
a `thread_name: &str` for logging — on non-Linux, the name is unused
(accepted and discarded to avoid `unused_variable` warnings). The
callers (in `oceanfs-node` background task spawning) call both functions
unconditionally.

**Why `unsafe` is needed:** `libc::ioprio_set` and
`libc::sched_setscheduler` are `unsafe` FFI calls. There are no safe
Rust wrappers in the standard library.

### Scope Boundaries

This ADR permits `unsafe` in `oceanfs-storage` for the following
purposes **only**:

1. Memory-mapped segment I/O via `memmap2::Mmap` (already authorized
   by [ADR-0011]).
2. WAL range-sync via `sync_file_range` + `fdatasync` (Category 1).
3. Atomic segment writes via `open(O_TMPFILE)` + `linkat` (Category 2).
4. Page cache hints via `madvise(MADV_SEQUENTIAL, MADV_DONTNEED)`
   (Category 3).
5. Background thread scheduling via `ioprio_set(IOPRIO_CLASS_IDLE)` +
   `sched_setscheduler(SCHED_IDLE)` (Category 4).

It does **not** authorize:

- Raw pointer manipulation, `transmute`, `MaybeUninit` shenanigans,
  or inline assembly.
- FFI bindings to any library other than `libc` for the listed syscalls.
- `unsafe` in any other `oceanfs-*` crate (they remain
  `#![forbid(unsafe_code)]` unless authorized by their own ADR).
- Any new syscall not listed in these four categories.
- `mlock` for the RocksDB block cache — that requires a separate ADR.

### Enforcement

The existing enforcement mechanisms are unchanged and apply to all five
categories (mmap + four syscall categories):

1. **Crate-level:** `#![deny(unsafe_code)]` in
   `oceanfs-storage/src/lib.rs`. All `unsafe` blocks are errors by
   default.
2. **Per-item override:** Each `unsafe` block must be preceded by
   `#[allow(unsafe_code)]`, making all unsafe sites auditable via
   `grep -r "allow(unsafe_code)" crates/oceanfs-storage/src/`.
3. **Safety comment:** Each `unsafe` block must carry a
   `// SAFETY:` comment (enforced by `clippy::undocumented_unsafe_blocks`
   at the crate level).
4. **CI audit:** The CI check that verifies each crate's `lib.rs`
   lint attribute already accepts `deny(unsafe_code)` for
   `oceanfs-storage` (as amended by ADR-0011). No CI changes needed.

## Consequences

### Positive

- **Four high-value I/O optimizations become ADR-compliant.** The code
  is already implemented and follows all safety guidelines; this ADR
  removes the policy violation and makes the implementation
  architecturally sound.
- **WAL fsync latency reduced 2-3× on NVMe.** Category 1
  (`sync_file_range` + `fdatasync`) saves two disk barriers per group
  commit vs. `sync_all`.
- **Zero-window atomic segment writes.** Category 2 (`O_TMPFILE` +
  `linkat`) eliminates the window where a partial segment file is
  visible to readers, and reduces directory operations per write.
- **Page cache pollution reduced.** Category 3 (`madvise`) keeps hot
  metadata and WAL pages in cache by eagerly evicting cold segment
  pages after read — without it, large segment reads (up to 4 MB each)
  evict more valuable data from the page cache.
- **Background tasks cannot starve foreground I/O or CPU.** Category 4
  (`ioprio_set` + `SCHED_IDLE`) ensures GC, scrub, heal, and
  anti-entropy threads only consume idle resources — preventing the
  "scrub cycle spikes GET latency" class of production incidents.
- **Compiled-away on non-Linux.** All four categories are
  `#[cfg(target_os = "linux")]`-gated with correct portable fallbacks.
  Non-Linux builds are completely unaffected — zero cost, zero risk.

### Negative

- **Unsafe surface expands further within `oceanfs-storage`.** The
  crate now has `unsafe` in up to six files (mmap, sync, atomic_write,
  segment_reader, mmapped, sched) instead of one (mmap). Code review
  surface grows proportionally.
- **Risk of scope creep increases.** With five authorized unsafe
  categories, the temptation to add "one more syscall" in
  `oceanfs-storage` grows. Mitigation: this ADR explicitly closes the
  scope — any new syscall requires its own ADR. CI can be extended to
  enforce a whitelist of permitted `unsafe` sites if needed.
- **`SCHED_IDLE` requires `CAP_SYS_NICE`.** Deployment must grant
  this capability or accept degraded background scheduling. The code
  gracefully handles `EPERM`, so this is a deployment concern, not a
  correctness concern. Documented in the function's doc comment.

### Neutral

- **Architecture guideline §7.2 must be updated.** The
  `oceanfs-storage` entry's scope note must change from "mmap segment
  I/O only" to enumerate all five categories. This is a one-paragraph
  edit.
- **No CI changes.** The existing `deny` + `allow` enforcement is
  already in place for `oceanfs-storage`.
- **No new crates.** All four categories are implemented inside
  `oceanfs-storage` — the unsafe is confined to the crate that owns
  the relevant invariants (segment lifecycle for atomic writes, WAL
  lifecycle for range-sync, mmap region ownership for madvise, thread
  ownership for scheduling).

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Move all syscall wrappers to a new `oceanfs-syscall` crate** | Isolates all unsafe FFI to one crate; `oceanfs-storage` stays `#![forbid(unsafe_code)]` | Adds a 15th crate to the workspace; the safety invariants are intrinsically tied to `oceanfs-storage` types (`SegmentHandle`, `WalSyncGroup`, mmap region lifetimes); the `syscall` crate would need to accept raw `fd`, raw pointers, and `CString` — it cannot enforce OceanFS-specific invariants at the type level, making it strictly less safe; the crate boundary would be artificial (thin wrappers around libc functions) and provide no architectural benefit beyond moving the `unsafe` keyword to a different file | The safety argument for each syscall depends on OceanFS-specific invariants (segment immutability, WAL append-only semantics, mmap region lifetimes, thread ownership). A generic syscall crate cannot enforce these — it would accept raw integers and pointers, pushing the burden of safety onto callers without the compiler's help. Keeping the unsafe at the call site where the invariant lives is the correct granularity. |
| **Use safe wrappers from the `nix` or `rustix` crate instead of `libc`** | No `unsafe` in OceanFS source; the wrapper crate handles the FFI | The `nix` crate wraps many syscalls but not all — `O_TMPFILE` via `custom_flags` still requires `unsafe` in `OpenOptionsExt`; `rustix` is still experimental and doesn't cover `sync_file_range` ergonomically; adds two large dependencies for thin wrappers around 4 syscalls; the "someone else handles unsafe" argument is false — the OceanFS invariants (segment sealing, WAL offset tracking) must still be correctly maintained regardless of who writes the `unsafe` block | The safety invariants are OceanFS-specific. A wrapper crate adds dependency weight without reducing the reasoning burden on reviewers. If a syscall wrapper from `nix` or `rustix` becomes the standard ecosystem choice and covers all four categories with safe APIs, this decision can be revisited. Today, no single crate covers all four cleanly. |
| **Reject these four categories and require them to remain in a separate crate** | `oceanfs-storage` stays as constrained as possible | Four separate small crates (or one `oceanfs-syscall` crate) create compilation overhead and artificial boundaries; the performance benefits are lost if the alternative is "don't implement them" | The performance benefits are substantial and well-motivated (ADR-0011 already established the precedent that storage I/O performance justifies scoped `unsafe`). The syscall wrappers are simpler and safer than the mmap case (advisory hints vs. memory aliasing), so if mmap was approved, these should be too. |
| **Amend ADR-0011 rather than creating ADR-0012** | Fewer ADRs in the index; one document covers all `oceanfs-storage` unsafe | ADR-0011 explicitly says "new ADR required" for additional categories; a single ADR covering five categories becomes long and harder to reference individually; ADR-0011 is already referenced by the architecture guideline §7.2 with a scope note — adding four more categories to the same ADR would make the scope note unwieldy | ADR-0011 deliberately limited its scope to mmap. Extending it would violate its own scope clause. A separate ADR keeps each decision focused and individually supersedable — if one syscall category is later deprecated, only ADR-0012 needs updating. |

## References

- [ADR-0011: Relax `unsafe_code` in `oceanfs-storage` for mmap Segment I/O](0011-storage-mmap-unsafe.md) — precedent and scope clause
- [Feature: Advanced I/O Optimizations](../features/performance-optimization/advanced-io-optimizations/feature.md) — feature specification for all four categories
- [Architecture guideline §7.2: Unsafe Code Policy](../../guidelines/architecture.md#72-unsafe-code-policy) — current permitted-crates list
- [Performance guideline §10.6: Conditional platform-specific code paths](../../guidelines/performance.md#106-conditional-platform-specific-code-paths)
- [Performance guideline §12.1: `// SAFETY:` comments on every unsafe block](../../guidelines/performance.md#121-safety-comments-on-every-unsafe-block)
- [Performance guideline §3.4: Group commit for WAL fsync](../../guidelines/performance.md#34-group-commit-for-wal-fsync)
- `sync_file_range(2)` — Linux man page
- `open(2)` `O_TMPFILE` — Linux man page (kernel 3.11+)
- `linkat(2)` `AT_SYMLINK_FOLLOW` — Linux man page
- `madvise(2)` `MADV_SEQUENTIAL`, `MADV_DONTNEED` — Linux man page
- `ioprio_set(2)` `IOPRIO_CLASS_IDLE` — Linux man page
- `sched(7)` `SCHED_IDLE` — Linux man page

[ADR-0011]: 0011-storage-mmap-unsafe.md
[Advanced I/O Optimizations]: ../features/performance-optimization/advanced-io-optimizations/feature.md
