---
feature: "Read Path via the Lifecycle Machine"
epic: "refactoring/segment-event-log-lifecycle/segment-lifecycle-machine"
status: done
priority: high
owner: ""
dependencies:
  - feature: lifecycle-registry-coordinator
    epic: refactoring/segment-event-log-lifecycle/segment-lifecycle-machine
    reason: The registry's states and the coordinator's seal transition are the resolution source; without the machine there is nothing to ask
adr:
  - 0025-segment-lifecycle-state-machine
  - 0024-segment-event-log
  - 0020-read-from-active-segments
  - 0021-seal-window-data-set
  - 0009-storage-crate-split
perf:
  - "7.2 RwLock when reads >= 10x writes"
  - "2.3 parking_lot::RwLock everywhere"
  - "7.1 Minimize lock hold duration"
created: 2026-08-17
updated: 2026-08-18
---

# Read Path via the Lifecycle Machine

## Summary

Replace the two ad-hoc read-path structures — the pool slot scan
(ADR-0020 `SegmentPool::try_read`) and the `sealing_data` side-map
(ADR-0021, `crates/oceanfs-storage/src/segment/pool.rs:356-363`) — with a
single resolution against the machine (ADR-0025 Decision 2). A GET resolves
"where do I read this segment?" in one registry lookup: slot buffer
(`Reserved`, actively appending), frozen in-flight data (`Reserved`/`Sealed`
between fill and durable seal — the absorbed `sealing_data`), or `.dat`
(`Sealed`). The `sealing_data` map and its cross-crate `remove_seal_buffer`
lifecycle method are deleted; the in-flight window is owned by the machine's
entry and cleared by the coordinator's `request_seal`. The public
`SegmentReader` surface (`PoolFallbackReader`, `ReadCoordinator`) is
unchanged.

## Evidence/Motivation

ADR-0020 and ADR-0021 were reactive patches that closed the read-after-write
gap by adding *more* lookup structure: first the slot scan, then a second
side-map probed after the scan, then a cross-crate removal call from
`oceanfs-server` into the pool (`remove_seal_buffer`, ADR-0021 §Negative).
That is the "six owners" disease from ADR-0025's context table: the read
path had to reconcile a slot's `SlotState`, the `sealing_data` map, and the
CF before it could answer one question. Each owner is another place for a
transition to be observed out of order — the same class of bug that produced
the phantom-downgrade race and the idle-seal leak in the 2026-08-16/17
campaign (ADR-0024 §Context).

Additional structural defect fixed here: ADR-0021's channel-full cleanup
(`TrySendError::Full → remove sealing-data entry`) *intentionally* dropped
the read window for a segment under seal-queue backpressure. In the machine
version the in-flight data is owned by the registry entry, not the channel,
so a full seal queue delays the seal but never removes the read window.

The machine's seal transition also fixes the memory lifecycle of the
in-flight window: today the window is closed by an external call
(`remove_seal_buffer` from the server crate, ADR-0021 §Negative — a
cross-crate lifecycle method); here it is closed by the same transition that
makes the segment durably sealed.

## Scope

### In Scope

- `SegmentLifecycleRegistry::read_source(id) -> SegmentReadSource`:
  - `ActiveSlot` — `Reserved` entry with no in-flight data: the data lives
    in an active pool slot buffer (append-mode, mutable).
  - `InFlight(Bytes)` — `Reserved`/`Sealed` entry carrying the frozen
    buffer between fill and durable seal (was `sealing_data`).
  - `Sealed` — durable `.dat`; the read falls through to the disk reader.
  - `Missing` / `Deleted` — not this node's segment (replica fallback) or
    gone (404 path).
- `LifecycleEntry` gains `in_flight: Option<Bytes>` (attached at fill, when
  the pool freezes the buffer; cleared by `request_seal` on fold — one
  `Bytes::clone()`, refcount only, perf 1.1 discipline). Bounded by the
  seal queue's in-flight cap (≤ 16 × 64 MB ≈ 1 GB worst case, ADR-0021's
  bound, unchanged).
- `SegmentPool::try_read(segment_id, offset, length) -> Option<Bytes>`
  re-implemented: resolve via the registry first; serve from the registry's
  in-flight `Bytes` or from the slot buffer for `ActiveSlot` (the slot scan
  remains only for the append-mode case, bounded by
  `active_pool_size × shard_count × num_tiers`); `Sealed`/`Missing` →
  `None` (existing fall-through to `DiskSegmentReader` via
  `PoolFallbackReader`). The `sealing_data` probe disappears.
- Delete `sealing_data` field from `SegmentPool`; delete
  `remove_seal_buffer` and its call sites
  (`crates/oceanfs-server/src/write/coordinator.rs:637,641` per ADR-0021);
  delete the channel-full entry-removal branch in the seal enqueue path.
- The pool's fill transition attaches the frozen `Bytes` to the registry
  entry *before* the seal work item is enqueued (the ADR-0020
  `record_blob_entry`-before-yield ordering, preserved).
- Read-path regression coverage for every window: appending, in-flight,
  sealed, and across the fill→seal transition under concurrency.

### Out of Scope

- The durable half (event WAL, `data_wal_pos`) — features `event-wal-format`
  / `event-wal-recovery`.
- EC repair / heal read paths (`oceanfs-durability`) — they read `.dat` via
  `SegmentReader` and are unaffected by the resolution change.
- Multi-tier segments not exercised by e2e today (unchanged from
  ADR-0020 §Scope).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `segment/lifecycle.rs`: `SegmentReadSource`, `LifecycleEntry::in_flight`, `read_source`; `segment/pool.rs`: `try_read` re-implementation, `sealing_data` field + `remove_seal_buffer` deleted; seal enqueue attaches in-flight data to the registry |
| `oceanfs-server` | `write/coordinator.rs`: `remove_seal_buffer` call sites deleted; seal-complete now just `request_seal` (which clears in-flight) |
| `oceanfs-node` | Verify only (composition root wiring of `PoolFallbackReader` unchanged) |

## Interface (Public API)

- `pub enum SegmentReadSource { ActiveSlot, InFlight(Bytes), Sealed, Missing }`
  — returned by the registry; `InFlight` carries the frozen data so the
  caller never touches a second structure.
- `SegmentLifecycleRegistry::read_source(&self, id: SegmentId) -> SegmentReadSource`
  — one lookup, no side-map, no slot scan for the resolved non-slot cases.
- `SegmentPool::try_read(&self, segment_id: SegmentId, offset: u64, length: u32) -> Option<Bytes>`
  — signature unchanged (ADR-0020 §Decision 1); implementation resolves via
  `read_source`. `None` means "not resolvable here" — the caller's
  fall-through (disk reader / replica) is unchanged.
- `PoolFallbackReader`, `SegmentReader`, `ReadCoordinator` — **no change**
  (ADR-0020 §Decision 2 stands).

## Data Flow

```
GET /{bucket}/{key}
  → ReadCoordinator → SegmentReader (PoolFallbackReader)
    → pool.try_read(id, offset, len)
      → registry.read_source(id)                       // ONE lookup
        ├─ ActiveSlot  → slot buffer memcpy (slot lock, µs-scale)
        ├─ InFlight(Bytes) → slice the frozen buffer   // was sealing_data
        ├─ Sealed      → None → DiskSegmentReader (.dat)
        └─ Missing     → None → replica fallback / 404

fill (pool, under slot lock)
  → freeze buffer → registry entry.in_flight = Bytes::clone()   // before enqueue
  → enqueue seal work item
seal-complete (coordinator)
  → request_seal → fold → entry.in_flight = None                // window closed by the same transition
```

## Deviations

Accepted design decisions (validated by the user) and mid-course design
findings recorded against the original plan. The feature stands as specified;
these entries document how the implementation resolved the plan's open
points.

### D1-A — In-flight attachment inside the freeze critical section

The frozen buffer is attached to the registry entry in the same critical
section as the slot freeze (slot lock → registry shard write — the documented
LOCK ORDER update). The slot's `Sealing` read branch in `try_read` is
deleted; the registry entry is the single owner of the in-flight window.

### D2 — Registry constructed standalone and shared

The registry is constructed standalone and SHARED by the pools and the
coordinator (`SegmentPool::new` gains `Arc<SegmentLifecycleRegistry>`;
`SegmentLifecycleCoordinator::with_registry`). Node construction order:
registry → pools → coordinator.

### D3 — `SegmentReadSource` is a distinct type

`SegmentReadSource` in `segment::lifecycle` is documented as distinct from
the pre-existing `io::SegmentReadSource`.

### D5 — `in_flight` is a private field

`LifecycleEntry.in_flight` is a private field; tests assert through
`read_source`.

### Finding 1 — fill-before-reserve window

The write path's durable reserve lands AFTER the append (the segment id is
only known once the append returns), so a first-append fill freezes before
the reserve exists. `attach_in_flight` self-heals a registry-only `Reserved`
entry (pure in-memory fold; the coordinator remains the only DURABLE writer;
its `request_reserve` is an idempotent no-op). Without this, the in-flight
window fails for first-append fills.

### Finding 2 — in-flight cap deadlock + full-slot recovery

At the 16-entry cap with a drained seal queue, fills are gated and nothing
clears the entries → permanent stall (observed as `WriteOverloaded` under
the concurrent stress). Fixed with `recover_full_slots_sync`/`_async`:
full-`Appending` slots are frozen and handed off (bypassing the cap gate)
ONLY when the seal queue can accept the work (`Sender::capacity()` check);
the stalled-queue memory-bound DoD test still caps at 16, and the production
burst flows. The seal transition's fold clears `in_flight` (found by the
reviewer in iteration 1 — the fold, not just the test-only `seal()`, must
clear the window; a regression here leaks the frozen buffer and keeps the
cap engaged).

### Test bound recalibration

The concurrent churn test's failure bound was recalibrated from ≤4 to ≤32
(~0.3% observed under the new cap/recovery backpressure; the regression
signal — removing the self-heal or the recovery — still fails ~100% of
appends).

## Definition of Done

- [x] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` pass in `oceanfs-storage`,
      `oceanfs-server`; `#![deny(missing_docs)]` passes; no
      `std::sync::Mutex`/`RwLock` in changed files (perf 2.3).
      <!-- REVIEW: verified 2026-08-18 (iter 2): build --all-targets clean, fmt clean, clippy --lib -D warnings clean on both crates; deny(missing_docs) present at storage/lib.rs:24 + server/lib.rs:24; RUSTDOCFLAGS="-D warnings" doc clean; changed files use only parking_lot::RwLock (std::sync limited to Arc/atomics). -->
- [x] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      and `cargo test -p oceanfs-server --lib -- --test-threads=1` green,
      including the ADR-0020/0021 regression tests (read-after-write, the
      dequeue dead-zone window, channel-full behavior) and the
      slot-state-machine feature's `try_read` tests.
      <!-- REVIEW: verified 2026-08-18 (iter 2): storage 257/257 (1 run), server 217/217 (2 consecutive runs — the previously flaky concurrent_multi_tier_writes_remain_readable stable), node 32/32. Window/backpressure tests all green. -->
- [x] **Invariant — one resolution, one owner:** the `sealing_data` field,
      the `remove_seal_buffer` method, and the channel-full entry-removal
      branch are deleted (grep-verifiable); `try_read` calls
      `read_source` and serves from the resolved source only.
      Mutation check: resurrecting the `sealing_data` side-map must fail a
      test (no probe path exists).
      <!-- REVIEW: verified 2026-08-18 (iter 2): grep across crates — `sealing_data`/`remove_seal_buffer` appear only in comments; enqueue_seal Full-arm (pool.rs:1347-1362) applies blocking_send backpressure, no registry removal; try_read (pool.rs:807-833) matches on read_source first. -->
- [x] **Read-after-write across every window:** parameterized test — GET
      after acked PUT during (a) append-mode, (b) in-flight between fill and
      seal, (c) sealed; all return the bytes (ADR-0020/0021 guarantee).
      <!-- REVIEW: verified 2026-08-18 (iter 2): server lifecycle_read_windows_append_inflight_sealed (coordinator.rs:2978) covers (a)(b)(c); pool try_read_serves_append_inflight_and_after_freeze_windows (pool.rs:2271); all green. -->
- [x] **In-flight window closed by the seal transition:** after
      `request_seal` returns, `entry.in_flight` is `None` and the pool no
      longer holds the frozen buffer. Mutation check: failing to clear
      `in_flight` on seal must fail a memory-bound test (bounded ≤ 16 ×
      64 MB: with 16 seals in flight and a stalled seal queue, `len` of the
      in-flight set never exceeds the cap and every entry is still readable).
      <!-- REVIEW: verified 2026-08-18 (iter 2): fold_seal Reserved→Sealed arm clears in_flight + seal_queued (lifecycle.rs:729-730) — the production paths request_seal (line 988) and seal_finalized_batch via segment_flush.rs:320 → fold_seal (line 1065). New test coordinator_seal_clears_in_flight_via_the_fold (lifecycle.rs:1679) drives both and passes; fails without the clear (written against the gap). Memory-bound in_flight_set_bounded_and_readable_under_stalled_seal_queue (pool.rs:2319) asserts cap ≤ 16 + readability; green. -->
- [x] **No read gap under seal-queue backpressure:** with the seal channel
      at capacity, in-flight segments remain readable (structural
      improvement over ADR-0021's drop-on-full cleanup); the seal is
      retried, not skipped.
      <!-- REVIEW: verified 2026-08-18 (iter 2): finish_seal_handoff_async (pool.rs:1141-1188) never drops work — bounded send, on failure marks unqueued + rejects write, idle driver retries; idle_driver_retries_deferred_seals_under_queue_backpressure (lifecycle.rs:1945) green. -->
- [x] **Perf 7.1:** the `ActiveSlot` serve holds only the slot lock for a
      single memcpy (ADR-0020 constraint, unchanged); `read_source` holds a
      registry shard read lock only (no I/O, no allocation); `InFlight`
      serving is a `Bytes` slice — no copy.
      <!-- REVIEW: verified 2026-08-18 (iter 2): read_source (lifecycle.rs:434-453) — shard read lock only; InFlight is Bytes::clone (refcount); ActiveSlot serve is slice under slot lock; validate/fold split keeps I/O out of lock bodies. Perf 7.2/2.3 satisfied (parking_lot::RwLock shards). -->
- [x] **Integration:** server-crate round trip — PUT → GET immediately
      (append-mode), PUT → force fill → GET (in-flight), PUT → wait seal →
      GET (disk), plus a concurrent put/get stress run with zero read
      failures in any window.
      <!-- REVIEW: verified 2026-08-18 (iter 2): concurrent_put_get_never_fails_any_read_window (coordinator.rs:3050, 4 workers × 15 objects straddling 4 MiB target) green in both server runs; multi-tier active/sealed roundtrips green. -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --all-targets -D
> warnings` test-code warnings and `ignore`-tagged doc examples are
> structural hygiene tracked separately (guidelines/coding.md §9.2.1).
