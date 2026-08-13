---
feature: "Segment-Pool Slot State Machine Unification"
epic: "refactoring"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: gap-closure/pool-backpressure-and-buffer-recycling
    reason: This refactor must preserve that feature's semantics (bounded backpressure, self-heal, buffer recycling); its tests and DoD gates become this feature's regression suite
adr:
  - 0020-read-from-active-segments
  - 0021-seal-window-data-set
perf:
  - "7.1 Minimize lock hold duration"
  - "2.3 parking_lot::RwLock everywhere"
created: 2026-08-13
updated: 2026-08-13
---

# Segment-Pool Slot State Machine Unification

## Summary

Refactor `PoolSlot` (crates/oceanfs-storage/src/segment/pool.rs) from a
two-lock split — `state: Mutex<PoolSlotState>` + `segment:
Mutex<Option<SegmentBuffer>>` (pool.rs:81–83) — into a single lock holding
one state enum that owns the segment. Today the nominal `PoolSlotState {
Idle, Appending, Sealing }` (pool.rs:71–78) under-represents the real
lifecycle: `Sealing` spans two distinct sub-states (with-segment between
`set_state(Sealing)` and `take()`, and with-`None` between `take()` and
re-activation). The two-lock split makes the transitions non-atomic and
created three TOCTOU windows that had to be patched reactively across
`gap-closure/read-path-integrity-under-load` and
`gap-closure/pool-backpressure-and-buffer-recycling`. Merging state and
segment under one lock makes "state and segment are consistent" a
structural invariant: the three windows disappear by construction, the
backpressure wait machinery collapses toward a plain predicate wait, and
the reviewer-flagged letter-of-7.1 deviation in `try_activate_slot`
(allocation under the slot lock) is fixed by the same change.

## Evidence/Motivation

Post-implementation review of `gap-closure/pool-backpressure-and-buffer-recycling`
(status: done). Today `PoolSlot` is TWO locks:

- `state: Mutex<PoolSlotState>` — pool.rs:82
- `segment: Mutex<Option<SegmentBuffer>>` — pool.rs:83
- `PoolSlotState { Idle, Appending, Sealing }` — pool.rs:71–78

The real lifecycle is a 4-state machine:

```
Appending → Sealing-with-segment (between set_state(Sealing) and take())
          → Sealing-with-None    (between take() and re-activation)
          → Appending
```

The nominal 3-state enum cannot express the two `Sealing` sub-states, and
the two-lock split means no single lock witnesses a consistent
(state, segment) pair. This created THREE distinct TOCTOU windows, each
patched reactively across the two gap-closure features:

1. **state-check → segment-lock window** (`append_with_hook`,
   pool.rs:283–299): the appender reads `Appending`, then locks the segment
   mutex and finds `None` — a concurrent filler already moved the slot to
   `Sealing` and took the segment. Previously an immediate error; now a
   retry through `append_to_next_available_with_hook`.
2. **append → SegmentFull window** (pool.rs:301–309): the segment filled
   concurrently between the state check and the lock acquisition →
   `Error::SegmentFull` → retry on the next slot.
3. **Preempted-filler window** — the measured 10 ms deadlock: both slots
   `Sealing`-with-`None`, zero activations, because the filling thread was
   descheduled after `take()` and before re-activation. Fixed by the
   waiters' self-heal in `append_to_next_available_with_hook`
   (pool.rs:465–470): an exhausted waiter calls `try_activate_slot()`
   itself before waiting.

Each window is patched with an extra retry/heal branch; the result is
three overlapping defensive layers that are individually necessary and
collectively hard to reason about.

Reviewer-flagged perf deviation (recorded in
pool-backpressure-and-buffer-recycling, "Known pre-existing observations"
#2): `try_activate_slot` holds the slot's segment lock across
`SegmentBuffer::new` (pool.rs:537–556) — allocation on miss inside the
lock, a letter-of-perf-7.1 violation.

The invariant "state and segment are consistent" should be structural,
not enforced by retries.

## Design & Scope

### Single-lock slot state machine

Replace the two locks with ONE `parking_lot::Mutex<SlotState>` per slot:

```rust
pub(crate) enum SlotState {
    Appending(SegmentBuffer), // actively accepting writes
    Sealing(Bytes),           // frozen, between take and hand-off
    Idle,                     // no segment (brief, post-build)
}
```

Transition methods, each a single lock acquisition:

- `take_for_sealing() -> Option<SegmentBuffer>` — atomically moves
  `Appending` → `Sealing`; returns the buffer when a seal must be
  enqueued. The frozen `Bytes` stays in the slot (or is immediately moved
  to `sealing_data` — see Open Question 3), so the segment is never
  unreachable (ADR-0021 read window).
- `install_replacement(SegmentBuffer)` — the caller builds the replacement
  **outside** the lock (fixes the letter-of-7.1 deviation); installation
  is a single pointer swap, shrinking the `Sealing`-with-`None` transit
  window from "allocation time" to "pointer move". This is the atomic
  slot swap.
- `try_append(&[u8]) -> Result<...>` — append + fill-check + hook
  invocation under ONE lock acquisition (see ordering guarantee below).

Windows 1–3 disappear by construction:

- **Window 1:** state check and segment access are the same lock — a slot
  that reports `Appending` always has its buffer.
- **Window 2:** append and fill-check are one critical section; no
  concurrent filler can interleave.
- **Window 3:** a preempted filler can no longer strand a
  `Sealing`-with-`None` slot: the frozen `Bytes` remains in the slot until
  the replacement is installed, and the transit is a pointer move.

### Backpressure machinery simplification

`append_to_next_available_with_hook` (pool.rs:412–496) becomes: scan slots
for `Appending`, else wait on the existing condvar (`slot_activation`,
pool.rs:161) with the same bounded budget — `SLOT_ACTIVATION_WAIT` = 10 ms
and `SLOT_ACTIVATION_WAIT_SLICE` = 1 ms (pool.rs:37, 44) — with the wait
predicate reduced to "any slot Appending". Whether the self-heal remains
an explicit fallback is Open Question 2; either way the semantics it
protects (a parked pool must never block forever) must be preserved.

### Must-preserve semantics (regression requirements)

- **`append_with_hook` ordering guarantee:** the hook runs under the slot
  lock, BEFORE the fill-triggered seal enqueue
  (read-path-integrity-under-load Defect 2; consumed at
  write/coordinator.rs:250–256, 267–271). The new `try_append` keeps this
  airtight.
- **Backpressure:** bounded wait + refresh-on-wake + self-heal;
  `SLOT_ACTIVATION_WAIT`/`SLOT_ACTIVATION_WAIT_SLICE` keep working.
- **`try_read`** serves both appending segments and `sealing_data`
  (ADR-0020; pool.rs:373–396).
- **Buffer recycling** via `release_buffer`
  (pool-backpressure-and-buffer-recycling; pool.rs:612–614, consumed at
  coordinator.rs:693–706).
- **Public API surface:** `take_seal_rx`, `seal_semaphore`,
  `remove_seal_buffer`, `active_count`/`slot_count` (pool.rs:585, 590,
  600, 354, 360).

### Out of Scope

- No change to seal-queue `try_send` semantics (drop-on-full stays).
- No change to `SealingWork`'s channel shape.
- No `append_with_hook` signature change — `oceanfs-server` callers
  (`write/coordinator.rs`) untouched.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `segment/pool.rs`: `SlotState` enum, single-lock `PoolSlot`, transition methods, allocation-outside-lock activation, simplified wait loop; tests updated plus new state-transition tests. |
| `oceanfs-server` | No signature change expected — re-run `append_with_hook` callers/round-trip tests in `write/coordinator.rs`. |
| `oceanfs-node` | Verify only (composition root untouched). |

## Definition of Done

- [ ] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` on oceanfs-storage +
      oceanfs-server, `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`
      clean
- [ ] **Tests:** `cargo test -p oceanfs-storage -- --test-threads=1` and
      `cargo test -p oceanfs-server --lib -- --test-threads=1` green
      (PIPELINE.md §4.6 RocksDB SIGABRT caveat)
- [ ] **Tests:** the three backpressure tests pass with their current
      gates: `append_waits_for_slot_reactivation`,
      `append_self_heals_when_all_slots_are_parked`,
      `concurrent_churn_never_exhausts_slots` ≤ 4 failures / 3200 appends
      (pool.rs:948, 994, 1012)
- [ ] **Tests:** the `append_with_hook` round-trip tests in oceanfs-server
      pass
- [ ] **Review:** mutation re-verification of the three original TOCTOU
      windows — the reviewer re-introduces each window (split state/segment
      reads, interleavable fill-check, preempted filler) and shows the
      tests fail, and confirms the unified design makes each
      un-representable (checklist item)
- [ ] **Integration:** seed-42 30 s load test PASS with
      `manifest_integrity` 0 mismatches and the node log clean of
      `no appending segment available in pool`

## Open Questions

1. Keep the `PoolSlotState` name or rename to `SlotState`? (The enum now
   carries the segment itself, so "state" under-describes the holding
   variant.)
2. Does the self-heal become a plain predicate wait ("any slot
   Appending"), or remain an explicit stranded-slot re-activation
   fallback?
3. Does `SealingWork.segment_data` stay `Bytes` — with the frozen `Bytes`
   moved from the slot into the work item at enqueue while `sealing_data`
   keeps its read-window clone — or does the slot retain the frozen
   `Bytes` until `remove_seal_buffer`?
