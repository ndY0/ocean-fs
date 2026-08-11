# ADR-0021: Sealing-Data Set — Closing the Read-After-Write Gap During Segment Seal

**Status:** Accepted
**Date:** 2026-08-11
**Deciders:** architect (brainstorm)

---

## Context

ADR-0020 (Read from Active Segment Buffers) closed most of the
read-after-write gap by teaching `SegmentPool::try_read()` to scan active
(append-mode) slots. A GET arriving after a successful PUT can now read
the unsealed buffer directly from the pool, without waiting for the
asynchronous seal worker to flush the segment to disk.

However, ADR-0020 only covers the window while a segment is in an active
slot. The write pipeline has a second window that it did not address:

```
append() → segment full → dequeue from slot → enqueue for sealing → disk write
                         ^                                                   ^
                         |--- DEAD ZONE: segment unreachable ----------------|
```

When a segment buffer fills during `append()`, the pool:
1. Transitions the slot to `PoolSlotState::Sealing`.
2. **Dequeues** the segment from the slot (`slot.segment.lock().take()`).
3. Freezes the buffer (`seg.into_buffer().freeze()`) and enqueues it on a
   bounded `mpsc` channel for the background seal worker.
4. Installs a fresh `ActiveSegment` in the now-idle slot.

Between steps 2 and 4, the segment data is unreachable from two directions:

- **`try_read()`** — the segment is no longer in any slot, so the slot scan
  returns `None`.
- **`DiskSegmentReader`** — the seal worker has not yet written the file
  to disk.

This violates the "read-after-write gap is closed" guarantee documented in
the storage architecture. The window is bounded by seal I/O latency
(typically 10–100 ms for a full segment), but during that window every GET
for a recently written blob returns HTTP 500.

The constraint is that the seal must remain **asynchronous** — blocking
the pool slot until the seal completes (a "sync seal on PUT") would reduce
write throughput by approximately 50%, violating the pipeline parallelism
requirement in spec §4.2.

### Forces

- The seal is asynchronous; the pool cannot block on disk I/O.
- The segment data is already in memory (as `Bytes`) at dequeue time.
- The dequeue operation is infrequent (once per ~64 MB of data written).
- Read load (GET requests) can be bursty and must not contend with writes.

---

## Decision

### Sealing-Data Set on `SegmentPool`

Add a "sealing-data set" — a `parking_lot::RwLock<HashMap<SegmentId, Bytes>>`
field on `SegmentPool` — that retains a reference-counted clone of the
segment's full buffer at dequeue time:

```rust
// oceanfs-storage/src/segment/pool.rs
struct SegmentPool {
    // ...
    sealing_data: RwLock<HashMap<SegmentId, Bytes>>,
}
```

**Lifecycle:**

1. **Insert (dequeue time):** When `append_to_next_available()` detects a
   full segment, it freezes the buffer, clones the resulting `Bytes`
   (atomic ref-count increment, not a data copy), and inserts it into
   `sealing_data`:
   ```rust
   let seg_data = seg.into_buffer().freeze();
   self.sealing_data.write().insert(seg_id, seg_data.clone());
   self.enqueue_seal(seg_id, seg_data, seg_tier, parity);
   ```
   The `Bytes` is shared: one clone goes to the sealing-data set, the
   other goes to the seal worker via the channel.

2. **Read (try_read):** `SegmentPool::try_read()` checks the sealing-data
   set after scanning active slots and before returning `None`:
   ```rust
   if let Some(seg_data) = self.sealing_data.read().get(&segment_id) {
       // ... copy/slice the requested range ...
   }
   ```
   This is a lock-free `RwLock` read (`parking_lot`'s `read()` does not
   block unless a writer holds the lock) plus a `HashMap::get()`. The
   check is added to the existing `try_read` path that already performs
   per-slot `Mutex` locks — it is not a new contention source.

3. **Remove (seal complete):** After the seal worker writes the segment to
   disk, it calls `SegmentPool::remove_seal_buffer(segment_id)` to free
   the held `Bytes` reference:
   ```rust
   pub fn remove_seal_buffer(&self, segment_id: SegmentId) {
       self.sealing_data.write().remove(&segment_id);
   }
   ```

4. **Cleanup (channel full):** If the seal channel is at capacity
   (`try_send` returns `Full`), the entry is removed from the
   sealing-data set to prevent a `Bytes` leak:
   ```rust
   Err(mpsc::error::TrySendError::Full(_)) => {
       self.sealing_data.write().remove(&segment_id);
       tracing::warn!(..., "seal queue full; seal deferred, sealing-data entry removed");
   }
   ```

### Design Rationale

| Choice | Rationale |
|---|---|
| **`Bytes::clone()`** | Atomic ref-count increment — zero-copy data sharing between the pool and seal worker. The `Bytes` buffer is immutable after `freeze()`. |
| **`parking_lot::RwLock`** | Writes (insert, remove) happen once per segment lifecycle (~every 64 MB of data). Reads happen on every GET request. `parking_lot`'s `RwLock` gives lock-free shared reads — the read path never blocks on the write path. |
| **`HashMap<SegmentId, Bytes>`** | O(1) lookup by segment ID. The map only grows to a small steady-state size (see below). |
| **Set lives on `SegmentPool`** | `SegmentPool` is the only type that witnesses the active→sealing transition. No external orchestration needed. |

### Memory Bound

The steady-state memory overhead is bounded by the number of segments
that can be in-flight through the seal channel at one time:

```
pending_segments ≤ active_pool_size × shard_count × num_tiers
                 ≤ 2 × 4 × 2 = 16 segments

max_memory ≤ pending_segments × segment_capacity
           ≤ 16 × 64 MB = ~1 GB (worst case)
```

In practice, the seal channel is drained quickly, and only 1–2 segments
are typically pending simultaneously (~64–128 MB). The `try_send` failure
path (channel full) further bounds this by clearing sealing-data entries
when the channel cannot accept new work.

### Scope

- **In:** Sealing-data set on `SegmentPool` for the dequeue→disk-write window.
  Insert on dequeue, check in `try_read`, remove after successful seal.
- **Out:** Eviction of stale entries beyond what the channel-full cleanup
  already handles. A seal worker crash or permanent stall could accumulate
  entries up to the channel capacity; this is an accepted risk (see
  Consequences: Negative).

---

## Consequences

### Positive

- **Read-after-write gap fully closed.** A GET request arriving at any
  point after a successful PUT — during active append, during the
  dequeue window, or after disk seal — will find the data. This satisfies
  the S3-compatible read-after-write semantic and the storage architecture
  guarantee.
- **Minimal overhead on the read path.** `try_read()` already acquires
  per-slot `Mutex` locks. The additional work — one `RwLock::read()`
  (lock-free in the common case) and one `HashMap::get()` — is
  negligible compared to the existing slot scan.
- **Zero-copy data sharing.** `Bytes::clone()` increments an atomic
  ref-count. The actual 64 MB buffer is shared between the pool and the
  seal worker. No data copy occurs.
- **Write path unaffected.** The write path inserts into the map under
  a write lock, but this happens exactly once per segment fill (~every
  64 MB) — an isolated hot spot that does not contend with the read path.
- **Graceful degradation on backpressure.** When the seal channel is
  full, the sealing-data entry is cleaned up immediately. The worst case
  is a temporary gap for that one segment (acceptable: the channel being
  full means the system is already overloaded).

### Negative

- **Unbounded accumulation on seal worker stall.** If the seal worker
  thread panics or stalls permanently, sealing-data entries accumulate
  up to the channel capacity (`active_pool_size × shard_count × num_tiers`
  segments). `Bytes` refs are not leaked (the channel also holds refs),
  but memory is not reclaimed until the seal worker recovers or the
  process restarts. This is an accepted risk: a stalled seal worker is
  a process-fatal condition regardless.
- **Cross-crate lifecycle method.** `remove_seal_buffer()` is called
  from `oceanfs-server/src/write/coordinator.rs`, not from within
  `oceanfs-storage`. This means the seal completion notification flows
  from the server crate back into the storage crate. Per
  `guidelines/architecture.md` §4.1, `oceanfs-server` may import concrete
  crates and call their public methods — this is not a dependency
  inversion violation. However, it means the `SegmentPool` API surface
  grew by one method specifically for the seal worker's benefit, which
  is a moderate API-design concession.

### Neutral

- **Maintenance surface.** ~40 lines of code across `pool.rs` (field
  declaration, insert in `append_to_next_available`, check in `try_read`,
  `remove_seal_buffer` method, cleanup in `enqueue_seal`) plus 6 lines
  in `coordinator.rs` (post-seal removal call). The logic is simple and
  well-localized.
- **`sealing_data` is private.** The field is not exposed through the
  crate's public API — only `try_read()` and `remove_seal_buffer()`
  interact with it. This preserves the encapsulation of `SegmentPool`.

---

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **A. Delay slot rotation until seal completes** | Simplest mental model; no additional state | Blocks the pool slot during disk I/O (10–100 ms). With 2 slots per pool, this reduces write throughput by ~50% — defeats the purpose of the active pool. | Violates spec §4.2 (Pipeline Parallelism). Seal latency would appear on the write critical path. |
| **B. In-memory segment cache with async callback** | Decouples the read path from the pool; no cross-crate `remove_seal_buffer` | Requires an async notification channel from seal worker → cache invalidation. Adds two new types (cache, callback) and async coordination for a window that lasts ~10–100 ms. More complex than the problem warrants. | The sealing-data set is simpler: it piggybacks on the existing `SegmentPool` lifecycle with no async machinery. |
| **C. Accept the gap; rely on client retry** | Zero code changes | Produces transient HTTP 500 errors visible to users. Violates the documented read-after-write guarantee. S3 clients expect immediate read-after-write for new objects; retry loops at the client layer are not an acceptable substitute. | The gap is an architectural defect, not a feature. Closing it is required for correctness. |
| **D. Store sealing entries in a separate dedicated struct** | Cleaner separation of concerns; `SegmentPool` API doesn't grow | Requires a shared handle (`Arc<SealingDataSet>`) passed to both the pool and the seal worker. The pool must call `insert`, and the seal worker must call `remove`. This is structurally equivalent to the chosen approach but adds a new type for no practical benefit. | `SegmentPool` is already the single witness to the active→sealing transition. Adding a separate type adds indirection without reducing coupling. |

---

## References

- `crates/oceanfs-storage/src/segment/pool.rs` — `sealing_data` field (line 141), insert in `append_to_next_available` (line 346), check in `try_read` (line 312), `remove_seal_buffer` (line 451), cleanup in `enqueue_seal` (line 378)
- `crates/oceanfs-server/src/write/coordinator.rs` — `remove_seal_buffer()` call after successful seal (lines 637, 641)
- ADR-0020: Read from Active (Unsealed) Segment Buffers — introduced `try_read()`; this ADR closes the remaining gap after dequeue
- ADR-0009: Storage Crate Split — established the `SegmentReader` trait and `oceanfs-storage` as the segment lifecycle owner
- `guidelines/architecture.md` §4.1 — composition root and cross-crate construction rules
- Spec §4.2: Pipeline Parallelism — requires asynchronous sealing to avoid blocking writes
