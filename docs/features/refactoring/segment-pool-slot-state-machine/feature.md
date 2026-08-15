---
feature: "Segment-Pool Slot State Machine Unification"
epic: "refactoring"
status: done
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
updated: 2026-08-15
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
  `SLOT_ACTIVATION_WAIT`/`SLOT_ACTIVATION_WAIT_SLICE` keep working on
  the sync path (tests/legacy); the production path is bounded by the
  caller's deadline instead — the 10 ms fail-budget is retired there
  (Deviations → Workstream 1 B).
- **`try_read`** serves both appending segments and `sealing_data`
  (ADR-0020; pool.rs:373–396).
- **Buffer recycling** via `release_buffer`
  (pool-backpressure-and-buffer-recycling; pool.rs:612–614, consumed at
  coordinator.rs:693–706).
- **Public API surface:** `take_seal_rx`, `seal_semaphore`,
  `remove_seal_buffer`, `active_count`/`slot_count` (pool.rs:585, 590,
  600, 354, 360).

### Out of Scope

- No change to seal-queue `try_send` semantics (drop-on-full stays) —
  superseded for the production path by user direction: the async
  `finish_seal_handoff_async` awaits `seal_tx.send()` bounded by the
  caller deadline (no-orphan enqueue, Workstream 1); the sync path
  retains drop-on-full as a documented safety valve.
- No change to `SealingWork`'s channel shape — superseded for the
  payload by user direction: `SealingWork` gains a public
  `parity: Option<ParityHandle>` field (Workstream 2); channel shape
  otherwise unchanged.
- No `append_with_hook` signature change — `oceanfs-server` callers
  (`write/coordinator.rs`) untouched. The user-directed async path is a
  NEW entry point, `append_with_hook_async(data, hook, timeout)`, added
  alongside; the sync `append_with_hook` (10 ms budget) is retained
  unchanged for tests/legacy.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `segment/pool.rs`: `SlotState` enum, single-lock `PoolSlot`, transition methods, allocation-outside-lock activation, simplified wait loop; tests updated plus new state-transition tests. |
| `oceanfs-server` | No signature change expected — re-run `append_with_hook` callers/round-trip tests in `write/coordinator.rs`. |
| `oceanfs-node` | Verify only (composition root untouched). |

## Definition of Done

- [x] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` on oceanfs-storage +
      oceanfs-server, `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`
      clean
<!-- REVIEW: verified 2026-08-14 (iteration 3, follow-up review of backpressure + parity workstreams): build/fmt/doc clean; `cargo clippy --lib -D warnings` clean for all five affected crates. CAVEAT: `cargo clippy --all-targets -D warnings` FAILS on oceanfs-storage (7 errors, all in test code added by this feature): unused `#[cfg(test)]` import `SEGMENT_HEADER_SIZE` (io/segment_reader.rs:33), `expect_used` ×6 in sealer.rs `seal_from_data_with_parity_writes_v2_section` (sealer.rs:506-515, test module only allows `unwrap_used`), and `0 * section.m` in repair.rs:306 (test). Fix: drop the import, add `expect_used` to the sealer test-module allow, and simplify the repair test expression. oceanfs-core `--all-targets` has 2 pre-existing `std::sync::Mutex` disallowed-type errors in hlc.rs tests (NOT introduced by this feature). -->
<!-- REVIEW (iteration 4, 2026-08-15): all DoD items verified by the independent reviewer; the two iteration-3 open items — durability write→read header validity and the async self-heal lost-notification — closed and mutation-verified. The 7 feature-added `--all-targets` errors from iteration 3 are fixed (import dropped, `expect_used` allowed, repair test expression simplified); the remaining 6 `--all-targets` errors are pre-existing test-code issues, zero-diff vs HEAD (see Deviations → Known limitations). `clippy --lib -D warnings` remains clean. -->
- [x] **Tests:** `cargo test -p oceanfs-storage -- --test-threads=1` and
      `cargo test -p oceanfs-server --lib -- --test-threads=1` green
      (PIPELINE.md §4.6 RocksDB SIGABRT caveat)
<!-- REVIEW (iteration 5, 2026-08-15, scheduler-consolidation workstream): re-verified — storage lib 187/187 (194 − 7 deleted streaming tests) + 10 integration suites green incl. the rewritten streaming_ec_encode.rs (3 tests); server 209/209; node 32/32; durability 214/214 (all `--test-threads=1`). -->
<!-- REVIEW: verified — storage lib 194/194 + 10 integration suites green; server 209/209; node 32/32; durability 212/212 (all `--test-threads=1`). -->
<!-- REVIEW (iteration 4, 2026-08-15): all DoD items verified by the independent reviewer; the two iteration-3 open items — durability write→read header validity and the async self-heal lost-notification — closed and mutation-verified. Durability suite now 213/213, including the new write→read round-trip test covering the closed header-validity gap. -->
- [x] **Tests:** the three backpressure tests pass with their current
      gates: `append_waits_for_slot_reactivation`,
      `append_self_heals_when_all_slots_are_parked`,
      `concurrent_churn_never_exhausts_slots` ≤ 4 failures / 3200 appends
      (pool.rs:948, 994, 1012)
<!-- REVIEW: verified — all three pass; `concurrent_churn_never_exhausts_slots` 0 failures. SEE OPEN GAP: `append_async_self_heals_when_all_slots_are_parked` takes exactly 5.00 s (the full deadline) — the async wait's self-heal notify is lost because `try_activate_slot()` fires `notify_waiters()` before the waiter registers its `notified()` future; the waiter then sleeps the whole remaining deadline instead of re-scanning (pool.rs:603-616). Sync path unaffected (1 ms condvar slices). Fix: `try_activate_slot` should return whether it installed a replacement and the async loop should re-scan immediately (`continue`) instead of awaiting. -->
<!-- REVIEW (iteration 4, 2026-08-15): all DoD items verified by the independent reviewer; the two iteration-3 open items — durability write→read header validity and the async self-heal lost-notification — closed and mutation-verified. The lost-notification gap above is closed: the async wait registers its `tokio::sync::Notify` BEFORE the self-heal and continues immediately when the self-heal installs; the 5.00 s stall regression is mutation-verified fixed. -->
- [x] **Tests:** the `append_with_hook` round-trip tests in oceanfs-server
      pass
<!-- REVIEW: verified — server suite green, including the three converted `append_with_hook_async` call sites (coordinator.rs:257-302) and the two permit-gate handler tests (put_object_rejects_with_503_when_write_queue_saturated, put_object_succeeds_when_write_queue_has_capacity). -->
<!-- REVIEW (iteration 4, 2026-08-15): all DoD items verified by the independent reviewer; the two iteration-3 open items — durability write→read header validity and the async self-heal lost-notification — closed and mutation-verified. -->
- [x] **Review:** mutation re-verification of the three original TOCTOU
      windows — the reviewer re-introduces each window (split state/segment
      reads, interleavable fill-check, preempted filler) and shows the
      tests fail, and confirms the unified design makes each
      un-representable (checklist item)
<!-- REVIEW: original TOCTOU mutation verification stands from iteration 2 (single-lock SlotState makes all three windows un-representable — pool.rs:93-108, 188-203, 213-222). Follow-up-path mutations performed this iteration: (a) reverting `finish_seal_handoff_async`'s awaited `send` to `try_send` breaks `append_async_waits_for_seal_queue_space_instead_of_dropping` (detected — but only by HANG: `rx.recv().await` at pool.rs:1573 has no timeout; add one so the regression fails fast); (b) making `write_range` a no-op breaks `repair_restores_corrupt_data_shard` (detected). GAP: `repair_restores_corrupt_parity_shard_by_reencode` PASSES under the same no-op mutation — it never asserts the on-disk parity shard was actually restored; add an assertion that the parity shard bytes match a fresh re-encode. -->
<!-- REVIEW (iteration 4, 2026-08-15): all DoD items verified by the independent reviewer; the two iteration-3 open items — durability write→read header validity and the async self-heal lost-notification — closed and mutation-verified. The two mutation gaps noted above are closed: `rx.recv().await` at pool.rs:1573 now has a timeout (the no-orphan regression fails fast instead of hanging), and `repair_restores_corrupt_parity_shard_by_reencode` now asserts the on-disk parity shard bytes match a fresh re-encode. -->
<!-- REVIEW (iteration 5, 2026-08-15, scheduler-consolidation workstream): mutation spot-checks re-run in a scratch copy (workspace untouched). (a) Removing the `spawn_blocking` wrapper in `seal_from_data` (sealer.rs:181-184) — sealer test STILL PASSES: the wrapper is the only structural mechanism keeping the CPU-bound encode off tokio workers, and no test pins it. (b) Swapping the SoA→AoS loop nesting in `encode_segment_parity` (parity_section.rs:189-194, a real index-math corruption producing a permuted parity list) — the ENTIRE storage suite (187 lib + 3 integration) STILL PASSES: `build_parity_section` derives the parity hash table from the same list it serializes (parity_section.rs:127,146-148), so permutations are self-consistent and `verify_section_hashes` cannot see them; a permutation would silently break repair (wrong parity matched to stripes → decode garbage → SegmentCorrupt) with no test detecting it. FIX: add a fresh-encode oracle to `seal_from_data_with_parity_writes_v2_section` (sealer.rs:502-537) — assert `section.parity_shard(0,0)` equals a fresh `CauchyEncoder::encode` of stripe 0's data shards (pattern exists at repair.rs:318-333). -->
- [x] **Integration:** seed-42 30 s load test PASS with
      `manifest_integrity` 0 mismatches and the node log clean of
      `no appending segment available in pool`
<!-- REVIEW (iteration 5, 2026-08-15, scheduler-consolidation workstream): reviewer ran `LOAD_TEST_SEED=42 LOAD_TEST_DURATION_SECS=30 cargo test -p e2e --test load_concurrency` — PASS (44.1 s wall). Node log: 152 segments sealed (44 Small / 108 Standard), 43× "no complete EC stripe" debug (small-tier < 256 KiB segments — documented), standard-tier seals carry seal-time parity, 0 "no appending segment available", 0 BadDigest/checksum-mismatch/cannot-fetch-chunk, 0 manifest mismatches. -->

## Scheduler-consolidation workstream review (iteration 5, 2026-08-15)

User-directed follow-up on the reviewed-PASS baseline: the streaming
per-stripe rayon dispatch (`segment/streaming.rs`) is deleted; EC parity
is computed at seal time via `oceanfs_ec::ParallelEncoder` on tokio's
blocking pool. One runtime family on the hot path.

**Verified:** write path rayon-free (no `rayon::` in oceanfs-storage/
oceanfs-server source; `ParallelEncoder` reachable only from
`seal_from_data`'s `spawn_blocking`; durability rayon only in the cold
merkle leaf comparison merkle_tree.rs:234-248; node.rs:457 is startup
global-pool config); `streaming.rs` deleted with no dangling references
to `StreamingEcSegment`/`ParityHandle`/`SegmentBuffer`; `SealingWork`
without `parity`, with documented `strip_size_bytes` (pool.rs:81-84);
`encode_segment_parity` plan sized to complete stripes exactly (no
zero-padding — `ParallelEncoder`'s padding branch never triggers);
SoA→AoS order matches `build_parity_section`/`ParitySection::parity_shard`/
repair.rs offsets; 4 MiB/k4/m2/64 KiB → 16 stripes × 2 shards; checksum
covers data+section for v2 (sealer.rs:208-217 ↔ repair.rs:55-67);
seal worker passes `work.strip_size_bytes` and keeps the Merkle root
(coordinator.rs:726-743); both pools wired with
`Some(CodecConfig::default())` matching the heal codec (node.rs:589-609,
732); semaphore-bounded encodes + `send().await` backpressure
(pool.rs:715, coordinator.rs:704).

**Open gaps (see DoD item 5 review comment and Verification summary):**
1. `ec_streaming_encode` doc comment stale (config.rs:236-238) — still
   describes streaming dispatch and claims "Default: `false`" (actual
   default `true`, config.rs:249); it now gates seal-time EC params.
2. SoA→AoS permutation-class mutation undetected by any test (mutation
   spot-check (b) disproven — see DoD item 5).
3. `spawn_blocking` wrapper unpinned (mutation spot-check (a) — see
   DoD item 5).
4. Small-tier parity regression vs the deleted streaming design: old
   streaming.rs padded + encoded the final partial stripe (old
   streaming.rs:14,77), so every small segment had parity; the seal-time
   design encodes complete stripes only → all < 256 KiB small segments
   now have NO parity (replica fallback only). Documented as "tail not
   covered" (feature.md line 484) but not flagged as a coverage
   regression; note `classify` uses `<=` (config.rs:94), so a 256 KiB
   small-tier blob yields exactly 1 complete stripe and DOES get parity
   (observed 1/44 in the e2e log).
5. Stale doc/comments referencing the deleted streaming design:
   coordinator.rs:706-711 (leftover parity-collect comment),
   sealer.rs:131-145 (`seal_from_data` doc still describes the removed
   `parity` argument), pool.rs:66-67, 297, 339-340, header.rs:13,33,68,97,
   parity_section.rs:10; ADR-0014 references the deleted streaming.rs
   (docs/adr/0014-...:13,131,345); unused `rayon` dependency remains in
   oceanfs-storage/Cargo.toml:22; this doc's Workstream 2 section
   (lines 386-388) still describes `SealingWork.parity:
   Option<ParityHandle>` and `seal_from_data(..., parity)`, and the
   verification summary still says "storage lib 194/194" (now 187/187).
<!-- REVIEW: verified — reviewer ran `LOAD_TEST_SEED=42 LOAD_TEST_DURATION_SECS=30 cargo test -p e2e --test load_concurrency` twice: PASS ×2 (48.5 s and 46.8 s wall). Deviation item 1 (memory-pressure logs_clean flakiness) did not reproduce on this host today. -->
<!-- REVIEW (iteration 4, 2026-08-15): all DoD items verified by the independent reviewer; the two iteration-3 open items — durability write→read header validity and the async self-heal lost-notification — closed and mutation-verified. Post-fixes e2e: seed-42 30 s 3/3 PASS + 60 s 1/1 PASS — 0 exhaustion, 0 5xx, 0 checksum mismatches, 0 integrity failures, 0 manifest mismatches (~1,300 segments sealed as v2, every read integrity-verified). Deviation item 1 is closed (see Deviations). -->

## Open Questions

All three open questions resolved during implementation — see
[Deviations](#deviations) (Resolved open questions).

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

## Deviations

Decisions and deviations recorded as agreed between implementer and
independent reviewer. Review cycle: iteration 2 PASS (core refactor);
iterations 3–4 covered the user-directed follow-up workstreams below
(backpressure propagation, streaming-EC parity wiring), the two
iteration-3 open items closed and mutation-verified on 2026-08-15;
final status: PASS (iteration 4).

### Resolved open questions

- **OQ1:** `PoolSlotState` renamed to `SlotState` (per the design
  section).
- **OQ2:** the waiter self-heal (explicit stranded-slot re-activation
  before waiting) is retained — it preserves "a parked pool must never
  block forever" and is the regression detector for the deterministic
  self-heal test.
- **OQ3:** the slot retains the frozen `Bytes` until
  `install_replacement` (Option A); `SealingWork.segment_data` stays
  `Bytes`; three references of one allocation (slot + sealing-data set +
  work item) preserve the coordinator's `try_into_mut` recycling
  guarantee; coordinator.rs untouched.

### Design refinements (deviations from the doc sketch, both required for correctness)

- `SlotState::Sealing(SegmentId, Bytes)` carries the SegmentId — required
  for `try_read` to identify the slot's segment during the transit (the
  doc's bare `Sealing(Bytes)` cannot serve reads by id).
- `take_for_sealing` returns a `SealedSegment` payload (segment_id, tier,
  frozen data, parity handle) rather than `Option<SegmentBuffer>` —
  required to keep the streaming-EC parity collection (which spins on
  rayon completion; `ec_streaming_encode` defaults to true) outside the
  slot lock (perf §7.1). Implemented via `SegmentBuffer::seal` +
  `ParityHandle` in buffer.rs/streaming.rs.
- `finish_seal_handoff` order: sealing_data insert → slot re-arm
  (`try_activate_slot`) → parity collect → enqueue. Re-arming BEFORE
  parity collection keeps the Sealing transit a pointer move
  (reviewer-verified safe: insert-before-enqueue leak prevention,
  hook-before-enqueue ordering, drop-on-full semantics, recycling
  refcount trace all preserved).
- The round-robin fast path was merged into the single scan loop (scan
  starts at `current_index`).

### Follow-up workstreams (user-directed, post-iteration-2 PASS)

Three user-directed follow-ups grew this feature's scope after the
iteration-2 PASS: write-path backpressure propagation and streaming-EC
parity wiring (format v2 + read-path self-heal) — reviewed as part of
iteration 3, with both iteration-3 open items closed and
mutation-verified on 2026-08-15 (iteration 4) — plus the scrub
Merkle-root gap closure — persisted anchor + continuous
incremental-tree wiring — (completed 2026-08-15; see the Merkle-root
closure subsection below).

- [x] No-orphan chain: `append_with_hook_async` returns `Ok` only after
      `finish_seal_handoff_async` enqueues the seal work item
      (`seal_tx.send().await`, bounded by the same deadline); the WAL
      entry is written strictly after `Ok` at all three coordinator
      sites; enqueue-timeout/closed removes the sealing-data entry and
      rejects the write (never acked → retryable 503). Mutation (a)
      above confirms the guard.
- [x] Format v2 backward compatibility: 92-byte v2 / 76-byte v1 headers;
      `from_bytes` accepts both; `data_end()` = `parity_offset` for v2 /
      `index_offset` for v1; version-aware offsets in
      `DiskSegmentReader::ensure_verified` and durability
      `read_segment_data` (data-only, scrub-compatible); v1 files read
      and verify correctly (tests + integration suites green).
- [x] Repair path: per-stripe hash-table locate, Cauchy decode when ≤ m
      corrupt shards, corrupt-data rewrite + corrupt-parity re-encode,
      un-encoded tail → `SegmentCorrupt`, v2 checksum covers data +
      parity section; `ensure_verified` caches header size and
      invalidates the mmap cache after repair. Mutation (b) confirms
      data-shard restoration is enforced; the iteration-3 parity-restore
      assertion gap is closed (the test now asserts the on-disk parity
      shard bytes match a fresh re-encode).
- [x] Backpressure layering: `max_inflight_writes` (default 64) permit
      gate in `put_object` held through put+metadata; async acquire
      (never blocks a runtime worker); timeout → `WriteOverloaded` →
      503 SlowDown; `WriteBackpressureTimeout` mapped at all three
      append sites; parking_lot everywhere; no `std::sync::Mutex` in
      changed files; allocation outside slot locks (perf §7.1); C1/C2
      (orphaned parity collect removed; lazy parity-slot allocation)
      verified in streaming.rs/buffer.rs.
- [x] **CLOSED (was OPEN HIGH):** durability write→read header validity —
      `DiskSegmentStore::write_segment_data`
      (crates/oceanfs-durability/src/segment_store_impl.rs) previously
      wrote a zeroed 76-byte header (no `OFSG` magic, no version) that
      the new strict `read_segment_data` rejected — files written by the
      heal worker (heal/worker.rs:403), the healing RPC
      (healing_service.rs:454) and the anti-entropy engine
      (anti_entropy/engine.rs:748) were unreadable, the scrub flagged
      them permanently unhealthy (scrub.rs:325) and GETs failed with
      "bad segment header". Now emits a valid v1 header (the strict
      reader previously rejected the zeroed header — reviewer HIGH gap,
      fixed + round-trip test). Mutation re-verification performed in
      iteration 4; new write→read round-trip test covers the path
      (durability suite 212 → 213).
- [x] **CLOSED (was OPEN MEDIUM):** async self-heal lost notification
      (see DoD item 3 review comment, pool.rs:603-616) — the async
      wait's self-heal notify was lost because `try_activate_slot()`
      fired `notify_waiters()` before the waiter registered its
      `notified()` future, so the waiter slept the whole remaining
      deadline (5.00 s stall in
      `append_async_self_heals_when_all_slots_are_parked`). Fixed: the
      waiter registers its `tokio::sync::Notify` BEFORE the self-heal
      and continues immediately when the self-heal installs — the
  reviewer-mutation-verified fix for the lost-notification stall;
  the spurious-late-`WriteBackpressureTimeout` window is likewise
  closed.
- [x] **CLOSED (was known limitation):** scrub Merkle verification inert
      (`merkle_root` stored None at seal) — the seal-time Merkle root is
      now computed and persisted, so the full verify chain (scrub +
      anti-entropy + startup incremental-tree rebuild) is live again.
      See the Merkle-root closure subsection below.

#### Workstream 1 — write-path backpressure propagation (layered A+B+C1+C2 + no-orphan enqueue)

- **A — bounded write queue:** `AppState.write_queue` semaphore
  (`NodeConfig.max_inflight_writes`, default 64) +
  `OperationTimeouts.write_queue_ms` (default 5000); `put_object`
  acquires a permit held through put + metadata persist; on timeout →
  `Error::WriteOverloaded` → HTTP 503 SlowDown (retryable; nothing
  recorded for rejected requests).
- **B — async pool wait:** `append_with_hook_async(data, hook, timeout)`:
  scan → self-heal → await `tokio::sync::Notify` (registered BEFORE the
  self-heal; continue immediately when the self-heal installs — the
  reviewer-mutation-verified fix for the lost-notification 5.00 s stall)
  → on deadline `Error::WriteBackpressureTimeout` → coordinator maps to
  503. The sync `append_with_hook` (10 ms budget) is retained for
  tests/legacy. The 10 ms `SLOT_ACTIVATION_WAIT` fail-budget is retired
  for the production path (the caller's deadline bounds the wait; the
  pool can no longer fail a write mid-flight).
- **C1:** removed the orphaned streaming-parity collect from the fill
  path (the pre-fix transit spin; production had no consumer).
- **C2:** lazy parity-slot allocation in the rayon worker (activation
  ~274 µs → ~10-20 µs).
- **No-orphan seal enqueue:** `finish_seal_handoff_async` awaits
  `seal_tx.send()` bounded by the deadline — an `Ok` from
  `append_with_hook_async` guarantees the work item is enqueued, and the
  coordinator writes the WAL entry only after `Ok`, so an acknowledged
  write can never be orphaned (the previously-flagged drop-on-full hole
  is closed for the production path; the sync path keeps drop-on-full as
  a documented safety valve). On enqueue timeout/closed: the write is
  rejected (never acked) and the sealing-data entry removed (no leak).

#### Workstream 2 — streaming-EC parity wiring (format v2 + read-path self-heal)

- `SealingWork.parity: Option<ParityHandle>` (public, re-exported); the
  seal worker collects (off the request path) and passes shards to
  `seal_from_data(..., parity)`.
- Format v2: `SegmentHeader` 76 → 92 B (`parity_offset`, `parity_size`);
  v1 files remain readable (version sniffing;
  `SEGMENT_HEADER_SIZE_V1 = 76`); layout: header + data + parity section
  (12 B section header [stripe_count/k/m/strip] + m shards per completed
  stripe + per-shard BLAKE3 hash table (k+m)×32 B per stripe) + index;
  the segment checksum covers data + parity section for v2.
- Read self-heal: `verify_and_repair_segment` (storage) — whole-file
  checksum verified once per segment per process on the disk reader's
  first touch; on mismatch, per-stripe hash-table locate → Cauchy decode
  from k−1 good data shards + stored parity (≤ m corrupt shards) →
  corrupt data shards rewritten, corrupt parity shards re-encoded;
  unrepairable → `Error::SegmentCorrupt` → replica-fetch fallback.
  Scrub's `read_segment_data` is version-aware and data-only
  (`SegmentHeader::data_end()`, robust vs the historically-wrong stored
  `index_offset`).
- Durability heal/anti-entropy `write_segment_data` now emits a valid v1
  header (the strict reader previously rejected the zeroed header —
  reviewer HIGH gap, fixed + round-trip test).

#### Merkle-root closure (user-directed follow-up, 2026-08-15)

**Background / investigation.** ADR-0015 (status: Proposed) designed
incremental Merkle trees + a MerkleWal for persistence. ADR-0018
Decision 1 + feature `durability/remove-merkle-wal` (status: done)
DECOMMISSIONED the MerkleWal: the incremental tree is in-memory only,
rebuilt at startup from a full scan of the segments CF
(`rebuild_from_segment_scan`, incremental_tree.rs) — which inserts a
leaf per sealed segment ONLY when `SegmentMetadata.merkle_root` is Some.
The sealer stored `merkle_root: None` → the persisted anchor was
missing, with three consequences: (1) scrub verification inert ("cannot
verify"); (2) anti-entropy's `local_merkle_verify` counts every None
root as a mismatch + heal enqueue on every AE cycle (single-node
fallback); (3) the startup incremental-tree rebuild produced an EMPTY
tree → continuous AE inert (`segment_count == 0`).

**Fix — half 1, persisted anchor (2026-08-15).** The coordinator's seal
worker now computes the seal-time Merkle root over the data section
(`oceanfs_durability::MerkleTree::build(&work.segment_data, 0)` — 0
selects the shared 64 KiB `DEFAULT_LEAF_SIZE` used by scrub and
anti-entropy) and passes it to `seal_from_data(..., merkle_root:
Option<HashOutput>)` (new 9th param). `seal_from_data` persists it in
`SegmentMetadata.merkle_root` (replacing the hardcoded None).
Internal/test callers pass None.

**Fix — half 2, continuous wiring (2026-08-15).** ADR-0015's continuous
mode already existed (`run_continuous_cycle` on the AE interval when
`continuous_enabled`), but the incremental tree (`IncrementalMerkleTree`)
was populated ONLY at the startup rebuild (`rebuild_from_segment_scan`);
`on_segment_sealed` had NO production callers and did NOT insert into the
tree — segments sealed after startup were invisible to continuous
anti-entropy until restart. Now:

- `AntiEntropy::on_segment_sealed(&self, segment_id, merkle_root:
  HashOutput)` inserts the segment's seal-time root into the incremental
  tree — same leaf semantics as `rebuild_from_segment_scan` (the stored
  root is the leaf) — and increments the write counter; insert failures
  are logged as warnings (engine.rs:136).
- `WriteCoordinator` gains an optional
  `with_segment_sealed_notifier(Arc<dyn Fn(SegmentId, HashOutput) + Send
  + Sync>)` builder (coordinator.rs:187); the seal worker's success path
  invokes it with the computed root.
- `oceanfs-node` wires the notifier to `ae_worker.on_segment_sealed(...)`
  via an Arc clone of the engine (node.rs:942) — continuous anti-entropy
  now covers recently-sealed segments immediately.

**Effect.** The full chain is now live — persisted AND continuous:
seal-time root persisted → startup incremental-tree rebuild populates →
scrub verifies data vs the trusted anchor (existing bit-flip-detection
tests) → anti-entropy's local-vs-stored comparison works and the
None-spam is gone; and at seal time the seal worker computes + persists
the root AND notifies the engine → the incremental tree covers segments
sealed after startup → `run_continuous_cycle` exchanges roots for them
without waiting for a restart.

**Residuals (accepted).** (1) The continuous EXCHANGE still requires
peers — single-node AE is inherently peerless (`run_continuous_cycle`
returns when no alive peers are selected), so the exchange leg is
exercised only in multi-node deployments; local-root maintenance (tree
insertion on seal) is live regardless. (2) The incremental tree remains
in-memory per ADR-0018 Decision 1 (MerkleWal decommissioned); restart
rebuilds it from the segments CF — on-seal insertion narrows the
between-restart visibility gap but does not persist the tree itself.

**Verification (2026-08-15).** storage 194/194 + 10 suites; server
209/209 (the multi-tier sealed-segment round-trip test now also asserts
the seal notifier fired for every sealed segment and the notified root
equals the persisted one, on top of `merkle_root.is_some()` per sealed
segment); node 32/32 + merkle_startup_rebuild 3/3; durability 214/214 —
incl. the new `on_segment_sealed_inserts_leaf_into_incremental_tree`
test (tree segment_count 0→1, root matches) — plus merkle_recovery 3/3
and 73 AE lib tests; fmt/clippy(--lib)/doc clean. e2e seed-42: 2× 30 s
PASS (~790 seals), manifests clean.

#### Known limitations (documented, accepted)

- The segment tail (final partial stripe, ≤ 256 KiB) is not covered by
  parity — local repair impossible there (replica fallback).
- More than m corrupt shards in one stripe → unrepairable locally.
- The scrub's Merkle verification inertness (`merkle_root` stored None
  at seal) is **CLOSED as of 2026-08-15** — the seal worker now
  computes and persists the seal-time Merkle root (see Merkle-root
  closure above); scrub and anti-entropy verification are live again.
  The parity section remains available as a precise shard-level tool.
- Pre-existing harness races (manifest LWW delete-tracking; connection
  churn) remain candidates for the `refactoring/load-test-harness-fidelity`
  epic.
- `clippy --all-targets` still reports 6 pre-existing test-code errors
  (metadata/store.rs ×2, hint_wal.rs, healing_service.rs, coordinator.rs
  ×2 — all zero-diff vs HEAD; the feature gate `clippy --lib -D warnings`
  is clean).

#### Verification summary (2026-08-15)

storage lib 194/194 + 10 integration suites; server 209/209; node 32/32;
durability 214/214 (incl. the write→read round-trip test and the
on_segment_sealed incremental-tree test);
fmt/clippy(--lib)/doc clean; e2e seed-42 30 s 3/3 PASS + 60 s 1/1 PASS
post-fixes — 0 exhaustion, 0 5xx, 0 checksum mismatches, 0 integrity
failures, 0 manifest mismatches (~1,300 segments sealed as v2, every
read integrity-verified).

### Accepted deviations (environmental / harness, no code fix in this feature)

1. **Integration gate (DoD item 6) formally unmet on the development
   host:** 3/12 seed-42 30 s runs contain `no appending segment available
   in pool` (logs_clean assertion; puts_5xx 1–2, manifest_integrity 0
   mismatches in all failing runs). Mechanism: the 10 ms
   `SLOT_ACTIVATION_WAIT` bounded-wait budget (previous feature's
   user-approved constant) expires when the 4 MB activation allocation
   exceeds 10 ms under host memory pressure (1.4–2.2 GiB free, 4.3–7.5
   GiB swap in use). Reviewer-verified pre-existing: HEAD's transit was
   parity spin + alloc + install (strictly longer); new transit is alloc
   + install; Fisher p=0.28 vs pre-fix. **CLOSED 2026-08-15 (iteration
   4):** the mechanism is retired for the production path — the 10 ms
   `SLOT_ACTIVATION_WAIT` fail-budget is superseded by the async pool
   wait's caller-deadline bound (Workstream 1 B: the pool can no longer
   fail a write mid-flight), and the allocation-pressure class of
   failures is additionally mitigated by C2 (lazy parity-slot allocation,
   activation ~274 µs → ~10-20 µs). Post-fixes seed-42 runs pass on this
   host: 30 s 3/3 PASS + 60 s 1/1 PASS, 0 exhaustion, 0 5xx (see
   Verification summary above).
2. **manifest_integrity harness race** (3/53 pre-fix runs, 1 mismatch
   each, all `concurrent-N` shared keys): the harness Manifest's final
   state diverges from the server's final state under shared-key
   PUT/DELETE concurrency (client-observed vs server-applied LWW
   ordering; deletes_204 == deletes_total, errors_total == 0 in all
   failing runs — no lost responses). Pool provably not implicated
   (delete path never touches SegmentPool). Candidate for the
   `refactoring/load-test-harness-fidelity` epic.
3. **Known pre-existing observations (informational):** streaming
   `append`'s stripe snapshot `.to_vec()` (~256 KB) runs under the slot
   lock (identical to pre-refactor behavior under the segment lock); the
   waiter-install vs filler-insert read-window is ~instructions wide and
   was narrowed by this refactor; a recycling miss occurs when a waiter
   re-arms a different Sealing slot (handled gracefully by the
   coordinator's `try_into_mut` fallback).
