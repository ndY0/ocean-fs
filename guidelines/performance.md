# OceanFS — Performance Guidelines

**Version:** 0.2.0 — Draft
**Date:** 2026-07-30

---

## Philosophy

Every rule in this document is **best-effort mandatory**. Implementers are
expected to follow each rule. Deviations are permitted, but **must be
justified in code review** with a rationale (benchmark data showing the rule
is harmful in context, platform constraint, or correctness requirement).

Rules are organized by category. Each rule includes:
- **The rule** — what to do.
- **Why** — the performance rationale.
- **How to verify** — how to check compliance (clippy lint, manual review,
  benchmark).

---

## 1. Memory & Allocation

### 1.1 Use `bytes::Bytes` / `BytesMut` for blob data

Never use `Vec<u8>` on hot paths carrying blob payloads.

**Why:** `Bytes` provides zero-copy slicing, shared ownership via reference
counting, and no allocation on clone. `BytesMut` avoids reallocation when
appending. This eliminates the dominant allocation cost in a blob store.

**Verify:** Manual review. Grep for `Vec<u8>` in `src/storage/` and
`src/net/`. Any occurrence on a hot path must be justified.

### 1.2 Arena / buffer pool for segment append buffers

Pre-allocate and recycle `BytesMut` buffers for active segment writing. Use
a bounded pool — not a per-write allocation.

**Why:** Every blob write appends to a segment buffer. Without a pool,
this is one allocation per PUT. With a pool, allocation is amortized to
pool initialization.

**Verify:** Check that `src/storage/segment/` uses a `BufferPool` type that
returns buffers to the pool on segment seal.

### 1.3 Pre-size collections with known capacity

Always use `Vec::with_capacity(n)`, `HashMap::with_capacity(n)`,
`String::with_capacity(n)` when the final size is known or bounded.

**Why:** Avoid reallocation cascades. A `Vec::new()` + N× `push()` causes
log₂(N) reallocations and copies. Pre-sizing does one allocation.

**Verify:** Clippy lint: `clippy::slow_vector_initialization`. Additional
manual review for `HashMap` and `String` usage.

### 1.4 Use `SmallVec` for small metadata structures

Use `smallvec::SmallVec<[u8; 16]>` or similar for metadata structures
where the common case has few elements (e.g., chunk lists, node lists,
error stacks).

**Why:** Avoids heap allocation entirely for the common case. A 4-element
chunk list fits on the stack. Only rare multi-segment blobs allocate.

**Verify:** Grep for `smallvec` usage in `src/metadata/`. Any
variable-length collection in struct definitions should use `SmallVec`.

### 1.5 Zero-copy protobuf deserialization

Use `prost` with `bytes::Bytes` as the wire type. Never copy protobuf
fields into intermediate `String` or `Vec<u8>`.

**Why:** `prost` with `Bytes`-backed types means deserialization borrows
from the read buffer. Copying doubles memory usage and kills throughput
for metadata-heavy operations.

**Verify:** Manual review of all `prost` message types. Fields must be
`Bytes` (not `Vec<u8>`) and `String` (which is already zero-copy in
prost).

### 1.6 Object pool for request-context structs

Pool frequently-created, short-lived structs such as request contexts,
EC encode descriptors, and shard fetch handles. Use `object-pool` or
a lock-free `ArrayQueue`-based pool.

**Why:** Allocating and dropping these per request creates malloc/free
churn visible in perf profiles. A pool recycles them.

**Verify:** Grep for `pool` or `Pool` in `src/`. Hot-path service
handlers should acquire from a pool, not allocate.

---

## 2. Concurrency & Parallelism

### 2.1 Rayon parallel iterators for EC stripe encode/decode

All stripes within a segment are independent. Use
`rayon::par_iter()` / `par_iter_mut()` to encode or decode them.

**Why:** A 4 MB segment with k=4 has ~16 stripes, each requiring a
GF(2⁸) matrix multiply. Running these sequentially wastes cores.
Rayon work-stealing saturates all cores with zero coordination
overhead.

**Verify:** Grep for `rayon` in `src/ec/`. All stripe loops must use
parallel iteration unless the segment has exactly 1 stripe.

### 2.2 `dashmap` for concurrent caches

Use `dashmap::DashMap` for metadata cache, routing cache, and any
other read-heavy, concurrently-accessed map.

**Why:** Sharded internal locking — 4× to 8× the effective concurrency
of `RwLock<HashMap>`. Reads never block reads. Writes lock only their
shard.

**Verify:** Grep for `DashMap` in cache module source files. No
`Mutex<HashMap>` or `RwLock<HashMap>` in cache hot paths.

### 2.3 `parking_lot::RwLock` everywhere

Replace all `std::sync::RwLock` and `std::sync::Mutex` with
`parking_lot::RwLock` and `parking_lot::Mutex`.

**Why:** parking_lot uses user-space synchronization (atomic spin +
yield) instead of kernel futex contention. ~5× faster in the
uncontended case. Avoids system call overhead on every lock
acquisition.

**Verify:** Clippy lint: forbid `std::sync::Mutex` and
`std::sync::RwLock` at workspace level (`clippy.toml`). Confirmed
with `grep -r "std::sync::Mutex" src/` returning zero.

### 2.4 `ArcSwap` for read-mostly shared data

Use `arc_swap::ArcSwap` (or `ArcSwapOption`) for data that is read
by many concurrent tasks and written only on config change or ring
topology update — specifically ring state, bucket policies, and
connection pool references.

**Why:** Wait-free reads. Writers never block readers because the old
`Arc` is swapped atomically. Readers always see a consistent, fully-
initialized snapshot. The penalty is that writers pay an `Arc::clone`
— acceptable since writes happen orders of magnitude less often.

**Verify:** Grep for `ArcSwap` in `src/routing/` and `src/config/`.
Ring topology and bucket config references must use it.

### 2.5 Sharded segment buffer per worker thread

Hash the request's connection ID or a per-task counter to select one
of N active segment groups. Each group has independent active segment
pools.

**Why:** Without sharding, all concurrent PUTs contend on a single
segment's append lock. With N shards, contention is divided by N. On
a 32-core machine with 8 shards, lock contention becomes nearly
invisible.

**Verify:** Check that `SegmentShard` or equivalent type exists in
`src/storage/segment/` and that the write path uses
`shard_index = hash(connection_id) % shard_count`.

### 2.6 Bounded channels for inter-task communication

Use `tokio::sync::mpsc::channel(bound)` — never unbounded channels —
for all work queues: EC encoding queue, heal queue, scrub work
distribution, gossip message queues.

**Why:** Unbounded channels permit unbounded memory growth under load
spikes. Bounded channels enforce backpressure — producers slow down
when consumers are saturated. This is the foundation of stable
performance under overload.

**Verify:** Grep for `tokio::sync::mpsc`. No occurrence of
`unbounded_channel`. All channel creations specify a capacity.

### 2.7 Tokio semaphore for concurrency limits

Wrap finite resources (GPU device, disk bandwidth, in-flight EC
encodes) with `tokio::sync::Semaphore`. Acquire a permit before
consuming the resource; release on completion.

**Why:** Prevents resource exhaustion. Without limits, a burst of 1000
simultaneous segment seals could trigger 1000 concurrent EC encodes,
exhausting memory and thrashing the GPU. A semaphore bounds
concurrency to the optimal parallelism for the hardware.

**Verify:** Search for `Semaphore` in `src/ec/` and `src/storage/`.
EC encoding, GPU offload, and heal operations must acquire permits.

---

## 3. I/O

### 3.1 Sequential-only WAL writes

The write-ahead log is append-only. Never seek within the WAL file.
Open with `std::fs::OpenOptions::new().append(true)`.

**Why:** Sequential writes saturate disk bandwidth (500+ MB/s on NVMe).
Random writes drop to 50-100 MB/s. The WAL is the synchronous
bottleneck in the write path — every microsecond saved here reduces
client latency.

**Verify:** File open in `src/storage/wal/` must use `.append(true)`.
No `seek` or `SeekFrom` on WAL file handles.

### 3.2 `O_DIRECT` for segment data files

Open segment shard data files with `O_DIRECT` (or equivalent on the
target platform). Bypass the OS page cache for segment data.

**Why:** Segment data is large (4 MB+) and not re-read frequently per
segment. Caching it in the OS page cache evicts hot metadata and WAL
data that benefit more from caching. `O_DIRECT` avoids double-
buffering (kernel + userspace).

Exception: when `read_cache_segments=true`, use `mmap` instead (see
rule 3.3).

**Verify:** File open in `src/storage/segment/data.rs` must set
`custom_flags(libc::O_DIRECT)` when the config does not enable segment
caching.

### 3.3 `mmap` for hot segment reads

When `read_cache_segments=true` (read-optimized profile), map
frequently-accessed segment shard files with `memmap2::Mmap`.

**Why:** Zero-copy reads from the kernel page cache. Data is faulted in
on first access and evicted under memory pressure — no userspace
buffer management needed. The page cache already implements LRU; don't
reimplement it.

**Verify:** Segment read path in `src/storage/segment/` must branch on
config flag and use `memmap2` when enabled.

### 3.4 Group commit for WAL fsync

Batch multiple concurrent fsync requests into a single fsync call.
Maintain a list of pending sync waiters; on each fsync completion,
wake all waiters whose data was flushed.

**Why:** Each `fsync` is a disk barrier (1-10ms on NVMe). If 100
concurrent PUTs each fsync individually, that's 100× latency. Group
commit amortizes that to 1 fsync per batch.

**Verify:** `src/storage/wal/sync.rs` must implement a group commit
mechanism with a flusher task that collects waiters.

### 3.5 `io_uring` / `tokio-uring` for disk I/O

On Linux 5.1+, use `tokio-uring` for all disk I/O: WAL writes,
segment reads/writes, RocksDB I/O (if supported). Fall back to
`tokio::fs` on older kernels or non-Linux platforms.

**Why:** True async disk I/O without a thread pool. `io_uring` submits
I/O requests to the kernel via shared ring buffers — no thread
switching, no work stealing, no syscall overhead per I/O.

**Verify:** Feature-gated: `#[cfg(target_os = "linux")]` uses
`tokio-uring`; fallback uses `tokio::fs`. Grep for `tokio-uring` and
confirm the fallback path exists.

### 3.6 `sendfile` / `splice` for blob responses

When serving blob data from disk to a network socket, use
`sendfile(2)` or `splice(2)` to copy data directly from the file
descriptor to the socket — never read into a userspace buffer first.

**Why:** Avoids a kernel→userspace→kernel copy. For a 1 GB blob, this
saves 1 GB of memory bandwidth and CPU time spent on `read` + `write`.

**Verify:** HTTP response body for GET must detect when the source is
a file-backed `mmap` or fd and use `tokio::io::copy` with the fd —
which internally uses `sendfile` on Linux if both ends support it.

---

## 4. Networking

### 4.1 Persistent gRPC connection pool per peer

Maintain a pool of `N` gRPC channels per peer node. Acquire a channel
from the pool for each RPC call; return it on completion. Never
create a new channel per RPC.

**Why:** TLS handshake + HTTP/2 SETTINGS negotiation is ~5ms per
connection. Reusing channels amortizes that to zero on all calls after
the first. Connection pool also provides automatic load balancing
across the pooled channels.

**Verify:** `src/net/pool.rs` must implement a `ConnectionPool` type
with `acquire()`/`release()`. No `Endpoint::connect()` in hot
RPC call sites.

### 4.2 HTTP/2 multiplexing for client API

The S3-compatible HTTP server must use HTTP/2 with stream multiplexing.
Many concurrent GETs/PUTs from one client share a single TCP connection.

**Why:** Reduces connection establishment overhead for clients making
many concurrent requests. Also avoids per-connection TLS handshake
cost.

**Verify:** Server configuration in `src/server/` must enable HTTP/2
(`hyper` with `serve_connection` or `axum` with h2 feature).

### 4.3 `TCP_NODELAY` on all sockets

Set `TCP_NODELAY` (disable Nagle's algorithm) on every TCP socket —
both server accept sockets and client gRPC connections.

**Why:** Nagle's algorithm delays small sends to coalesce them. In a
distributed storage system, small metadata ACKs and gossip messages
must arrive with minimal latency. Coalescing small writes is the
application's job (rule 3.4), not the kernel's.

**Verify:** Socket setup in `src/server/` and `src/net/` must include
`.set_nodelay(true)`. Grep for `TCP_NODELAY` or `set_nodelay`.

### 4.4 Streaming gRPC for large data transfers

Use gRPC client streaming for `AppendSegment` and server streaming for
`FetchShard`. Never send multi-megabyte data in a single unary RPC
payload.

**Why:** Streaming overlaps data transfer with processing. The receiver
begins computing on the first chunk before the last chunk arrives.
Unary RPCs require the entire payload to be buffered in memory on both
ends before processing starts.

**Verify:** Protobuf service definition in `proto/` must declare
`stream` on request for `AppendSegment` and on response for
`FetchShard`.

### 4.5 Adaptive per-operation timeouts

Set timeouts per operation type, not per connection:
- WAL write ack: 100-500ms
- Metadata read (cache miss): 10-50ms
- Segment shard fetch: 1-30s (depends on size)
- EC encode: 1-60s (depends on segment size)
- Gossip ping: 1-5s

**Why:** A single global timeout is either too short for large
operations (causing spurious failures) or too long for small ones
(causing slow error detection). Per-operation timeouts detect failures
at the appropriate granularity.

**Verify:** `src/config/timeouts.rs` or per-operation config structs
must define distinct timeout durations. Hard-coded constants with
operation-specific names.

---

## 5. Hashing & Checksums

### 5.1 BLAKE3 with runtime SIMD detection

Use the `blake3` crate. It auto-detects AVX-512, AVX2, SSE4.1, or
NEON at program startup and selects the fastest implementation. Never
force a specific implementation at compile time unless targeting a
known architecture for a release binary.

**Why:** The `blake3` crate achieves ~4 GB/s/core on AVX-512. The
portable C implementation is ~10× slower. Runtime detection means
the same binary runs optimally everywhere.

**Verify:** `Cargo.toml` depends on `blake3` (not a manual
implementation). No `#[cfg]` forcing a specific implementation.

### 5.2 Streaming hash — never buffer the full blob

Use `blake3::Hasher::update()` in a streaming loop — never collect
the entire blob into one contiguous buffer before hashing.

**Why:** For a 100 GB blob, buffering before hashing requires 100 GB
of memory and blocks the client until the full body is received.
Streaming hashing uses constant memory and overlaps hashing with
network I/O.

**Verify:** The hash computation in the write path (also the read
verification path) must call `.update()` on chunks as they arrive,
not `.collect()` → `.hash()` on a fully assembled buffer.

### 5.3 Feature-gated SIMD compilation

Use `#[cfg(target_feature = "avx512f")]` or equivalent for optional
SIMD acceleration in areas not covered by `blake3` (e.g., custom
checksum verification loops, memcpy alternatives).

**Why:** Compile-time dispatch avoids runtime feature detection
overhead when the binary is compiled for a known target. The main
binary ships with runtime detection (rule 5.1); auxiliary tooling
benefits from compile-time.

**Verify:** Feature-gated SIMD code must have a portable fallback
path. Grep for `#[cfg(target_feature` to audit usage.

### 5.4 Batch verify for multi-chunk reads

When reading a blob that spans multiple chunks (from different
segments), compute the BLAKE3 hash over the concatenated chunk data
in a single hasher — not one hash per chunk. Compare the result
against the stored per-blob hash.

**Why:** A single hash call handles all chunk data. N separate hashes
call the compression function N times for the same number of input
bytes, with additional initialization and finalization overhead.

**Verify:** `src/hash/` or the read verification path must show a
loop that calls `hasher.update(chunk_data)` for each chunk, then
`hasher.finalize()` once.

---

## 6. Data Structures & Memory Layout

### 6.1 Cache-line alignment for mutable atomics

`#[repr(align(64))]` on structs containing atomics that are
frequently mutated from different threads — per-core counters,
shard cursors, WAL write position.

**Why:** False sharing: when two cores write to different fields on
the same cache line (64 bytes on x86, 128 bytes on Apple Silicon),
the cache coherency protocol bounces the line between cores,
destroying throughput. Aligned padding isolates each field to its
own cache line.

**Verify:** Grep for `#[repr(align(64))]` on struct definitions with
`Atomic*` fields. Any such struct without alignment must justify
that the atomics are only accessed from a single thread or are
read-only.

### 6.2 SoA layout for EC stripe data

Store EC stripe data as a Struct of Arrays:
```
// NOT this (AoS):
// Vec<StripeRow { data: [u8; 64KiB], parity: [u8; 64KiB] }>

// THIS (SoA):
// struct StripeBatch {
//     data: [[u8; 64KiB]; k],
//     parity: [[u8; 64KiB]; m],
// }
```

**Why:** EC encoding walks columns of the matrix — each shard byte is
computed from the same offset across all k input shards. SoA layout
places all shard[i] bytes contiguously, giving sequential memory
access during encode/decode. AoS would scatter reads across cache
lines.

**Verify:** `src/ec/stripe.rs` layout. Data and parity must be stored
as separate contiguous arrays.

### 6.3 `#[repr(C)]` for all on-disk / on-wire structures

Any struct serialized to disk (WAL entries, segment headers) or sent
over the network (protobuf-adjacent structs) must be `#[repr(C)]`.

**Why:** Rust's default representation has no guaranteed field order
or padding. `#[repr(C)]` gives a stable, predictable layout. Not
necessary for protobuf-generated types (prost handles this), but
needed for any manually-laid-out binary format.

**Verify:** Grep for all `#[repr(C)]` on structs in `src/storage/`
and `src/net/packet.rs` or equivalent. Binary-serialized structs
without it are a correctness bug.

### 6.4 Static dispatch over dynamic dispatch on hot paths

No `Box<dyn Trait>` or `&dyn Trait` in functions called on the
read path, write path, EC encode/decode, or hash verification.
Use generics with `impl Trait` or `where` bounds instead.

**Why:** Dynamic dispatch adds a vtable lookup (pointer chase + branch)
per trait method call. Monomorphization via generics eliminates this
entirely — the compiler generates a specialized version of the
function with the concrete type inlined.

**Verify:** Clippy lint: `clippy::type_complexity` warns on complex
types. Manual review: grep for `dyn ` and `Box<dyn` in hot-path
source files. Any occurrence must be justified.

### 6.5 `BTreeMap` over `HashMap` for ordered access

Use `BTreeMap` for the DHT ring lookup (ordered by hash range),
segment blob index (ordered by offset), and any data structure that
benefits from locality or range queries.

**Why:** `BTreeMap` provides O(log n) with excellent cache locality
(nodes are contiguous arrays). `HashMap` provides O(1) average but
with a random-access memory pattern that thrashes the cache on
large maps. For ring lookups (binary search on sorted ranges),
`BTreeMap` is the correct data structure.

**Verify:** Grep for `BTreeMap` in `src/routing/` and
`src/storage/segment/index.rs`. Ring routing must use ordered
maps.

---

## 7. Locking Discipline

### 7.1 Minimize lock hold duration

Structure code as: (1) compute data outside the lock,
(2) acquire lock, (3) commit results, (4) release lock.

**Why:** Lock contention scales non-linearly with hold duration.
Reducing hold time from 100µs to 10µs means 10× more requests
before contention becomes the bottleneck. Computing outside the
lock is always possible when the result is independent of shared
state.

**Verify:** Manual review of every lock scope. The body of every
`lock()` block must contain only reads/writes to shared state —
no computation, no allocation, no I/O. Use `drop(guard)` to make
the scope explicit.

### 7.2 `RwLock` when reads ≥ 10× writes

Use `RwLock` instead of `Mutex` when the data structure is read
at least 10× more often than written. In OceanFS, this covers:
bucket config, ring topology, connection pool, segment state.

**Why:** `RwLock` allows concurrent reads. Under read-heavy load,
throughput scales with the number of readers. `Mutex` serializes
all access, capping throughput at 1/lock_time.

**Verify:** At definition site, choose `RwLock` when the access
pattern is read-dominant. Include a comment with the approximate
read:write ratio if not obvious.

### 7.3 Explicit lock guard drop

Use `drop(lock_guard)` to release a lock early, rather than
relying on implicit scope-bound drop.

**Why:** Implicit drop at the end of a scope can hold a lock across
unrelated code that follows. Explicit `drop()` makes the critical
section visible to the reviewer and ensures no accidental extension.

**Verify:** Manual review. Any lock guard held past its last use
of shared state is a violation.

### 7.4 Lock ordering documentation

For any pair of locks `A` and `B` that may be held simultaneously
by any code path, document the required acquisition order as a
module-level comment in the file that holds both.

**Why:** Without a documented order, concurrent code paths risk
deadlocks (thread 1 holds A, waits for B; thread 2 holds B, waits
for A). Documented ordering + debug-assertion checks prevent this.

**Verify:** Any file that acquires multiple locks must have a
comment block:
```
// LOCK ORDER: segment_lock → metadata_lock → connection_pool
```
Debug builds should assert the order with a thread-local lock
stack or tiered lock identifiers.

### 7.5 Default-unfair `parking_lot::Mutex`

Use `parking_lot::Mutex` in default (unfair) mode. Only enable fair
locking when strict FIFO ordering is required for correctness.

**Why:** Fair locking guarantees first-to-wait gets the lock. This
requires queue management on every unlock, reducing throughput by
10-20%. For a storage system, throughput matters more than
starvation-prevention fairness — lock hold times are microsecond-
scale so starvation is empirically negligible.

**Verify:** `parking_lot::Mutex::new(value)` not
`parking_lot::Mutex::fair(value)`. Annotate with `// unfair` comment
at construction.

---

## 8. Async Patterns

### 8.1 `FuturesUnordered` for parallel shard fetches

When fetching k of k+m shards for a read, spawn all fetch futures
into a `futures::stream::FuturesUnordered`. Collect results until
k completed successfully.

**Why:** `FuturesUnordered` polls all futures concurrently and yields
results in completion order — not submission order. This naturally
implements "use fastest k" without any explicit scheduling. The
client gets data as soon as k shards respond, even if the remaining
nodes are slow.

**Verify:** Grep for `FuturesUnordered` in `src/storage/read.rs` or
equivalent. Shard fetch must use it.

### 8.2 `tokio::select!` with timeout branches

Use `tokio::select!` for any operation with a deadline or fallback
path — not nested `tokio::time::timeout` calls.

**Why:** `select!` cancels remaining branches when one completes,
preventing resource leaks. Nested timeouts keep the inner future
alive past the outer deadline. `select!` with a `tokio::time::sleep`
branch is the canonical pattern for time-bounded operations.

**Verify:** Grep for `tokio::select!` in `src/`. Any timeout or
cancellation logic must use it.

### 8.3 `spawn` vs `spawn_blocking`

Use `tokio::task::spawn` for all async work. Only use
`tokio::task::spawn_blocking` for truly blocking CPU-only work
that has no async equivalent (e.g., a third-party C library call
that blocks the thread).

**Why:** `spawn_blocking` reserves a dedicated thread from a limited
pool (default 512). Overusing it starves genuinely blocking
operations. Disk I/O, network I/O, and EC computation all have
async paths — use those.

**Verify:** Grep for `spawn_blocking`. Every occurrence must be
justified: annotated with a comment explaining why no async
alternative exists.

### 8.4 Avoid `Box::pin` on hot async paths

Use `pin!` macro (stack pinning) or `tokio::pin!` for
self-referential async state. Avoid `Box::pin` inside functions
called on the read/write path.

**Why:** `Box::pin` allocates. The `pin!` macro pins to the stack,
which is free. In an async function that is called per-request,
avoiding one allocation per call is a measurable throughput win.

**Verify:** Grep for `Box::pin` in `src/storage/` and `src/net/`.
Replaceable occurrences should use stack pinning.

### 8.5 Bounded semaphore for task concurrency

Apply a `tokio::sync::Semaphore` (or `Semaphore::new(bound)`) before
spawning tasks for parallel work (EC encode, heal, scrub). Await a
permit before `spawn`.

**Why:** Without a bound, a burst of work spawns thousands of tasks,
each competing for CPU, memory, and I/O bandwidth. The semaphore
limits concurrency to the value that maximizes throughput for that
workload.

**Verify:** Grep for `Semaphore` in `src/ec/`, `src/heal/`,
`src/scrub/`. All task-spawning loops must acquire a permit first.

---

## 9. Zero-Copy / No-Allocation Hot Paths

### 9.1 Accept borrowed data, never require ownership

Internal APIs on the hot path must accept `&[u8]`, `impl AsRef<[u8]>`,
or `Bytes` (shared reference) — never `Vec<u8>` (owned). The caller
decides allocation strategy.

**Why:** Requiring `Vec<u8>` forces the caller to allocate even when
it already has the data in a `Bytes` or borrowed slice. This creates
unnecessary copies.

**Verify:** Manual review of function signatures in `src/storage/`
and `src/net/rpc.rs`. Parameters carrying blob or metadata data must
be references or shared-ownership types.

### 9.2 `&str` over `String`; `Cow<str>` only when ownership needed

Function parameters should be `&str`. Use `Cow<'_, str>` only when
the function sometimes needs to modify or own the string.

**Why:** Every `String` parameter that receives a `&str` argument
forces an allocation (`"literal".to_string()`). `&str` is a fat
pointer — zero allocation.

**Verify:** Clippy lint: `clippy::ptr_arg` (warns on `&String` and
`&Vec`). Manual review for `String` parameters that are always used
as `&str`.

### 9.3 Pre-compute key hash once

Compute `SHA-256(object_key)` once at the HTTP handler entry point.
Pass the hash alongside the key through routing, metadata lookup,
and segment lookup.

**Why:** Re-hashing the same key in each layer burns CPU for no
benefit. A key hash is deterministic and stable for the request's
lifetime.

**Verify:** `HashKey` or equivalent type exists in request context.
Functions that route or look up objects accept the pre-computed hash.

### 9.4 `bytemuck` for zero-copy byte-to-struct casting

Use `bytemuck::from_bytes` / `bytemuck::cast_slice` when
interpreting EC shard data (`&[u8]`) as arrays of GF(2⁸) elements
(`&[u8; 64KiB]`) — no copy.

**Why:** EC encoding operates on array-of-array data. Copying from
`Vec<Vec<u8>>` into structured types adds allocation and memcpy.
`bytemuck` reinterprets the bytes in place when the layout is
guaranteed `Pod`.

**Verify:** `src/ec/` must use `bytemuck` for shard data access.
Raw `std::mem::transmute` is forbidden; `bytemuck` provides
compile-time safety checks.

### 9.5 `extend_from_slice` for known batch sizes

When writing multiple blobs into a segment or assembling stripe
data, use `.extend_from_slice(known_slice)` rather than N
individual `.push()` calls.

**Why:** `extend_from_slice` calls `memcpy` once per batch. N
individual `push` calls check capacity N times and call `memcpy`
N times. For a segment with 1000 small blobs, this matters.

**Verify:** Clippy lint: `clippy::extend_with_drain`. Manual review:
loops that call `.push()` inside a body where `.extend_from_slice()`
before the loop is possible.

---

## 10. Compile-Time Optimizations

### 10.1 LTO in release profile

Enable link-time optimization in the release profile:
`lto = "fat"` in `Cargo.toml` `[profile.release]`.

**Why:** Cross-crate inlining eliminates function-call overhead
across crate boundaries. In a multi-crate workspace, Rust does
not inline across crates without LTO. Observed wins: 10-20%
throughput for metadata-heavy workloads.

**Verify:** `Cargo.toml` workspace root must contain:
```toml
[profile.release]
lto = "fat"
```

### 10.2 Single codegen unit in release

Set `codegen-units = 1` in `[profile.release]`.

**Why:** Multiple codegen units compile in parallel but prevent
cross-unit optimization. Single-unit takes longer to compile
(acceptable for release builds) but enables the optimizer to see
the entire program.

**Verify:** `Cargo.toml`:
```toml
[profile.release]
codegen-units = 1
```

### 10.3 Panic abort in release

Set `panic = "abort"` in `[profile.release]`.

**Why:** Removes unwind tables and landing pads from the binary.
Smaller binary, fewer branches, no unwind code in hot paths.
Aborting on panic is acceptable for a storage system where
correctness is paramount — a panic is an unrecoverable bug.

**Verify:** `Cargo.toml`:
```toml
[profile.release]
panic = "abort"
```

### 10.4 `target-cpu = "native"` for deployment builds

Use `-C target-cpu=native` when compiling a binary for a specific
deployment machine. Not for distributable binaries.

**Why:** Enables all CPU features available on the build machine
(AVX-512, BMI2, etc.). Distributable binaries must compile for a
baseline target.

**Verify:** CI pipeline must have a dedicated "release-native" job
with `RUSTFLAGS="-C target-cpu=native"`. The distributable build
pipeline must not set this.

### 10.5 PGO workflow

Profile-Guided Optimization: compile with `-Cprofile-generate`,
run a representative benchmark workload, recompile with
`-Cprofile-use`.

**Why:** The compiler uses runtime branch probabilities and hot-path
data to reorder code for better instruction cache utilization.
Typical wins: 5-15% throughput on hot paths.

**Verify:** Presence of a `scripts/pgo.sh` or CI job that performs
the three-step PGO workflow. Document which benchmark workload to
use.

### 10.6 Conditional platform-specific code paths

Use `#[cfg(target_arch = "x86_64")]` and `#[cfg(target_arch =
"aarch64")]` to select platform-specific implementations with
fallbacks.

**Why:** Different platforms have different SIMD instruction sets
(AVX-512 vs NEON vs SVE). One implementation path tuned for the
target beats runtime dispatch for known-deployment scenarios.

**Verify:** Grep for `#[cfg(target_arch` in `src/`. Any architecture-
specific module must have a portable fallback in `src/fallback/` or
equivalent.

---

## 11. Instrumentation & Profiling

### 11.1 Atomic counters on hot paths

All latency histograms, request counters, cache hit/miss counters,
bytes-transferred counters on hot paths must use `AtomicU64` or
`AtomicUsize` with `Ordering::Relaxed` (memory-order is not required
for stats).

**Why:** Mutex-guarded counters add lock contention to the hot path.
Atomics with relaxed ordering compile to a single `INC` instruction
on x86 — effectively free.

**Verify:** Grep for `AtomicU64` and `AtomicUsize` in `src/metrics/`
or equivalent. No `Arc<Mutex<Counter>>` on hot-path metrics.

### 11.2 `tracing` span discipline

Use `tracing::instrument` spans on service entry points (HTTP
handlers, RPC service implementations). Avoid spans inside hot
loops (EC encode inner loop, shard fetch loop, hash update loop).

**Why:** Span creation allocates and incurs branching overhead.
Placing spans at operation boundaries gives full request tracing
without slowing the inner hot path.

**Verify:** Grep for `#[instrument]` and `span!` macro in `src/`.
No spans inside `for` loops that process >10 items.

### 11.3 Feature-gated profiling hooks

Gate all profiling integration (pprof, dhat, flamegraph hooks) behind
a Cargo feature `profiling`. Core code must compile and run without
this feature.

**Why:** Profiling adds overhead (sampling, allocation tracking).
Feature gating means the production binary carries no profiling cost.

**Verify:** `#[cfg(feature = "profiling")]` guards on all profiling
calls. `Cargo.toml` defines `profiling` as a non-default feature.

### 11.4 Criterion benchmarks for hot-path functions

Every hot-path function must have a criterion benchmark: EC encode,
EC decode, BLAKE3 hash (various sizes), metadata lookup, WAL append,
segment index lookup, shard fetch.

**Why:** Without benchmarks, performance regressions are invisible
until production. Criterion provides statistical comparison against
a baseline, detecting regressions of <1%.

**Verify:** `benches/` directory with `ec_benchmark.rs`,
`hash_benchmark.rs`, `storage_benchmark.rs`, etc. Each benchmark
named after the function it tests.

### 11.5 CI performance regression detection

CI must run all criterion benchmarks against the main branch
baseline. A regression >3% (configurable) fails the CI check.

**Why:** Automated regression detection catches performance bugs
before merge. Without it, throughput can degrade incrementally
across many PRs.

**Verify:** CI configuration (`.github/workflows/` or equivalent)
includes a job running `cargo bench` and comparing against a stored
baseline. `critcmp` or `codspeed` integration.

---

## 12. Safety & Correctness Under Performance Constraints

### 12.1 `// SAFETY:` comments on every unsafe block

Every `unsafe { ... }` block must be preceded by a `// SAFETY:`
comment citing the invariant that makes it sound.

**Why:** The unsafe blocks in a performance-oriented codebase (SIMD
intrinsics, bytemuck casts, manual memory layout) are the highest-
risk code. Documenting the invariants makes auditing possible.

**Verify:** Clippy lint: `clippy::undocumented_unsafe_blocks`.
Code review must reject undocumented unsafe.

### 12.2 CI: clippy with denied warnings

`cargo clippy -- -D warnings` must pass in CI. All clippy lints are
denied. Individual allows require a `#[allow(clippy::lint_name)]`
with a comment.

**Why:** Clippy catches 80% of the rules in this document
automatically. Treating warnings as errors prevents backsliding.

**Verify:** CI configuration runs `cargo clippy --all-targets --all-features -- -D warnings`.

### 12.3 CI: ASAN, TSAN, UBSAN

Test suite must pass under AddressSanitizer, ThreadSanitizer, and
UndefinedBehaviorSanitizer. Run nightly:
```
RUSTFLAGS="-Z sanitizer=address" cargo +nightly test
RUSTFLAGS="-Z sanitizer=thread" cargo +nightly test
RUSTFLAGS="-Z sanitizer=undefined" cargo +nightly test
```

**Why:** Catches buffer overflows, use-after-free, data races, and
UB that compiler optimizations may exploit. Critical for a system
that handles arbitrary binary data.

**Verify:** CI configuration includes three separate sanitizer jobs.
All pass.

### 12.4 Loom models for lock-free structures

The ring cache, metadata cache, buffer pool, and connection pool
must have `loom` model-based tests that verify correctness under
all possible thread interleavings.

**Why:** Lock-free data structures are notoriously hard to reason
about. Loom systematically explores all interleavings, finding
subtle bugs that traditional testing misses.

**Verify:** Test files in `tests/loom/` or `#[cfg(loom)]` modules.
CI runs `RUSTFLAGS="--cfg loom" cargo test --test loom`.

---

## 13. Error Handling Performance

### 13.1 `thiserror` for library error types

Use `thiserror::Error` derive macro for all error types in library
crates. Never `Box<dyn Error>` on hot paths.

**Why:** `Box<dyn Error>` allocates on creation. `thiserror` generates
thin `enum` variants with `#[error("...")]` — the error type is a
plain enum on the stack. Zero allocation for error creation on the
common error path (e.g., key not found, checksum mismatch).

**Verify:** Grep for `derive(Error)` in `src/`. Grep for
`Box<dyn Error>` in `src/` — allowed only at the application
boundary (HTTP handlers, main).

### 13.2 `anyhow` / `eyre` only at application boundary

Use `anyhow::Result<T>` or `eyre::Result<T>` only in the HTTP layer
and `main.rs`. Storage engine, EC, networking, and routing layers
return concrete error types.

**Why:** `anyhow` boxes errors. Libraries returning concrete types
allow callers to match and handle specific errors without allocating.
The application boundary wraps concrete errors into `anyhow` for
convenient propagation to the top-level handler.

**Verify:** Grep for `anyhow` and `eyre` usage in `src/`. Must not
appear in `src/storage/`, `src/ec/`, `src/net/`, `src/routing/`,
`src/hash/`.

### 13.3 `Copy + Clone` on error enums where possible

Error enums that only contain integer codes or thin references should
derive `Copy, Clone`. This avoids clone/allocation overhead when
propagating errors through multiple layers.

**Why:** Propagating an error through 3 layers of `?` operator should
not allocate or clone. `Copy` means the error is passed by value in
a register.

**Verify:** Check `#[derive(Debug, Clone, Copy, Error)]` (or similar)
on error enums. Large error variants (containing `String` or `Vec`)
may not derive `Copy`.

---

## 14. Review Checklist

Every code review must verify:

- [ ] No `Vec<u8>` on hot paths (1.1)
- [ ] No `std::sync::Mutex` or `std::sync::RwLock` (2.3)
- [ ] No unbounded channels (2.6)
- [ ] WAL is append-only (3.1)
- [ ] No `Box<dyn Error>` in library code (13.1)
- [ ] No `anyhow` below application boundary (13.2)
- [ ] Every `unsafe` has `// SAFETY:` (12.1)
- [ ] Lock ordering documented for multi-lock code paths (7.4)
- [ ] Explicit `drop(guard)` for early lock release (7.3)
- [ ] Bounded semaphore before task spawning loops (8.5)
- [ ] Pre-sized collections where capacity is known (1.3)
- [ ] `FuturesUnordered` for parallel shard fetches (8.1)

For any guideline not followed, the PR description must include a
`## Performance Deviations` section with the format:

```
### Rule X.Y: [Rule Name]
**Deviation:** [What was done instead]
**Justification:** [Benchmark data or reason]
```
