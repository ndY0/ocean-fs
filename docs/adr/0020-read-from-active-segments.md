# ADR-0020: Read from Active (Unsealed) Segment Buffers

**Status:** Accepted
**Date:** 2026-08-11
**Deciders:** architect (brainstorm)

---

## Context

OceanFS uses a pool of active segments to decouple write latency from
seal-time I/O. The pipeline is:

```
PUT → ActiveSegment buffer (RAM) → seal worker (async) → disk file
```

Data is acknowledged to the client (HTTP 200) as soon as it lands in
an active segment buffer and the hinted-handoff WAL entry is written.
Segment sealing — flushing the filled buffer to disk — happens asynchronously
via a background tokio task.

**The gap:** a GET arriving after a successful PUT but before the seal
worker has flushed the segment to disk returns HTTP 500. The
`DiskSegmentReader` only reads from sealed segment files; it has no
visibility into the active pool buffers.

This gap was historically masked because most e2e tests wrote objects
smaller than the 4 KB `inline_threshold_bytes`, which are stored as
`inline_data` in object metadata (no segment allocation). The first test
to exercise the segment path with objects > 4 KB exposed the gap.

A secondary race condition was discovered in the seal worker: the blob
index entry (`record_blob_entry`) was recorded *after* the WAL write
(`write_wal_entry`), which is an async yield point. If the seal worker
scheduled between the two, it would find an empty entry map and silently
skip the seal — leaving the segment neither in the pool nor on disk.

### Constraints

- `SegmentReader` trait already exists (`oceanfs_storage::io::SegmentReader`)
  with one method: `read_chunk(segment_id, offset, length) -> Result<Bytes, String>`.
- The `ReadCoordinator` uses `Option<Arc<dyn SegmentReader>>`; any
  fix must work through this existing abstraction.
- The segment pool uses `parking_lot::Mutex` for append operations;
  any read path must not introduce deadlock or significant contention.
- Must not require changing the `SegmentReader` trait signature (no
  API break for implementors).

---

## Decision

### 1. `SegmentPool::try_read()` — pool-level read

Add a synchronous `try_read(segment_id, offset, length) -> Option<Bytes>`
method to `SegmentPool` that:

- Iterates over all pool slots.
- Acquires the same `parking_lot::Mutex` used by `append()` to access
  each slot's `SegmentBuffer`.
- If a matching `segment_id` is found, copies the `[offset, offset+length)`
  range via `Bytes::copy_from_slice`.
- Returns `None` if no active segment in this pool matches the requested id.

The lock hold time is microsecond-scale (a single memcpy), identical to
the append fast path. No deadlock risk — only one mutex is held at a time.

### 2. `PoolFallbackReader` — composite SegmentReader

A new struct in `oceanfs_storage::io` implementing the existing
`SegmentReader` trait:

```rust
pub struct PoolFallbackReader {
    pools: Vec<Arc<SegmentPool>>,
    fallback: Arc<dyn SegmentReader>,
}
```

`read_chunk` logic:
1. Try each pool in order via `pool.try_read()`.
2. If no pool match, delegate to `self.fallback.read_chunk()`.

This is a transparent adapter — no changes to `ReadCoordinator`, `fetch.rs`,
or any consumer of the `SegmentReader` trait.

### 3. Wiring in node composition root

In `oceanfs_node::Node::start()`, clone the `Arc<SegmentPool>` handles
before they are consumed by `WriteCoordinator::new()`. Wrap them
together with the existing `DiskSegmentReader` in a `PoolFallbackReader`,
then pass the composite to `ReadCoordinator::with_segment_reader()`.

### 4. Write coordinator race fix

Move `record_blob_entry()` calls **before** `write_wal_entry().await`
in the `SizeTier::Small` and `SizeTier::Standard` arms of
`WriteCoordinator::put()`. The blob index entry is now visible in the
`DashMap` before any async yield point, guaranteeing the seal worker
sees it when processing the seal request.

### Scope

- **In:** reads from active segment buffers in both Small and Standard pools.
- **Out:** Multi-tier segments (not yet exercised by any e2e test; their
  blob entry recording is a separate gap), inline objects (already
  served from metadata), EC recovery integration.
- **Out:** A standalone `try_read` for the `InMemorySegmentReader` path
  (not needed — that reader already holds all data in a HashMap).

---

## Consequences

### Positive

- **Correctness.** Acknowledged writes are immediately readable. The
  read-after-write semantic expected by S3 clients is now honoured.
- **Zero API break.** No changes to `SegmentReader` trait, `ReadCoordinator`,
  `fetch.rs`, or any downstream consumer.
- **Minimal overhead.** The pool check is a synchronous memcpy under
  the same mutex used by append. Falls through to disk-backed read when
  the segment has been sealed, incurring no additional cost on the
  common path.
- **Fixes race.** The `record_blob_entry` reorder eliminates the
  silent seal-skip bug that could cause permanent data loss for
  recently written segments.

### Negative

- **Tightens pool coupling.** The read path now holds a reference to the
  write-side segment pools. If the pool API changes (e.g., slot layout
  refactor), `try_read` must be updated in lockstep. Mitigated by the
  fact that `try_read` lives in the same crate (`oceanfs-storage`) as
  `SegmentPool`.
- **No zero-copy.** `try_read` performs a `Bytes::copy_from_slice()`
  rather than sharing the underlying buffer. This is intentional —
  the active segment buffer is a `BytesMut` that cannot be shared
  without freezing (which would destroy the active segment).
  The copy is bounded by chunk size (typically ≤ 4 MB), and the
  common path hits the disk-backed reader (sealed segments), which
  can use mmap for zero-copy.

### Neutral

- **Maintenance burden.** ~95 lines across 3 files. The
  `PoolFallbackReader` is a thin adapter with no complex logic.
- **Test coverage needed.** `SegmentPool::try_read()` needs unit tests
  for the found/not-found paths. `PoolFallbackReader` needs a test
  verifying the fallback chain.

---

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **A. Sync seal on PUT** | Simplest path; no pool coupling | Destroys write throughput — defeats purpose of async seal pool; tail latency becomes seal I/O latency (~ms) | Violates performance requirement §2.5 (pipeline parallelism) |
| **B. Read from WAL** | Data already on disk for crash recovery | WAL is sequential, not indexed by `(segment_id, offset)`; full scan required to find a single chunk | Unacceptable latency for hot-path reads |
| **C. Inline-data fallback** | No new code; works for small objects | Only covers objects ≤ 4 KB; segment path still broken for larger blobs | Doesn't solve the general problem |
| **D. Force pool to expose `Bytes` refcount** | Zero-copy reads | Would require freezing the active buffer, destroying the segment; incompatible with append-after-read | Architecturally unsound |

---

## References

- `oceanfs-storage/src/segment/pool.rs` — `SegmentPool::try_read()`
- `oceanfs-storage/src/io/segment_reader.rs` — `PoolFallbackReader`
- `oceanfs-server/src/write/coordinator.rs` — race fix in `put()`
- `oceanfs-node/src/node.rs` — wiring in `Node::start()`
- Spec §4.2 (Pipeline Parallelism), §4.4 (Failure Handling During Write)
- ADR-0009 (Storage Crate Split) — established the `SegmentReader` trait
