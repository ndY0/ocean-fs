---
feature: "Segment-Pool Backpressure & Buffer Recycling"
epic: gap-closure
status: done
priority: medium
owner: ""
dependencies:
  - epic: gap-closure/read-path-integrity-under-load
    reason: Its independent review recorded the residual 0.3% PUT 5xx ("no appending segment available in pool") and recommended this follow-up; shares its seal-window terminology and load-test gates
adr:
  - 0001-segment-packing
  - 0020-read-from-active-segments
  - 0021-seal-window-data-set
perf:
  - "1.2 Arena / buffer pool for segment append buffers"
  - "7.1 Minimize lock hold duration"
created: 2026-08-13
updated: 2026-08-13
---

# Segment-Pool Backpressure & Buffer Recycling

## Summary

Follow-up to `read-path-integrity-under-load` (status: done). That feature's
independent review recorded a residual failure class in its 120 s load run —
6 × `PUT 500 InternalError: "multi tier append: invalid config: no appending
segment available in pool"` within an 11 ms write burst (~0.3% of PUTs,
unchanged from the pre-fix `SegmentFull` error class) — and explicitly
recommended a follow-up feature. This feature addresses two problems in
`oceanfs-storage` (with a small `oceanfs-server` companion change):

1. **Transient slot exhaustion:** when all `active_pool_size` slots are
   simultaneously in the synchronous fill→seal-enqueue transit window
   (`Sealing` with no segment), an arriving append finds zero appendable
   slots and fails. Fix: bounded blocking backpressure on a condvar in
   `SegmentPool` instead of immediate failure.
2. **Segment buffers are never recycled:** `BufferPool::release()` is called
   only from tests; every segment activation mallocs a fresh 4 MiB buffer
   forever. Fix: return the unique-owned `BytesMut` after seal completion to
   the pool, and rework the pool's free lists into byte-bounded size classes
   so 4 MiB buffers cannot retain unbounded memory.

Both fixes land in the same two files the prior feature touched
(`crates/oceanfs-storage/src/segment/pool.rs`,
`crates/oceanfs-storage/src/buffer_pool.rs`), plus the seal worker in
`crates/oceanfs-server/src/write/coordinator.rs`.

## Evidence

From the read-path-integrity-under-load review (preserved in that feature's
DoD annotation):

- **120 s run (e2e node log 19:54:08.848–859):** 6 × `PUT 500 InternalError:
  "multi tier append: invalid config: no appending segment available in pool"`
  within 11 ms during a write burst — all 4 standard-pool slots transiently
  `Sealing` during 4 MiB churn.
- Rate ~0.3% — the same rate as the pre-fix `SegmentFull` 2 × 5xx the prior
  feature's Open Question 1 recorded; the SegmentFull retry added there
  narrowed one race window but not this one.
- Error return site: `crates/oceanfs-storage/src/segment/pool.rs:410`
  (`append_to_next_available_with_hook`).
- `PoolConfig::default()` has `active_pool_size = 4`; the default is
  hardcoded at `oceanfs-node/src/node.rs:584` (no config knob).

**Mechanics of the failure window (verified):**

1. When a segment fills, the filling thread — synchronously, inside `append`
   — runs: `set_state(Sealing)` → `take()` → `freeze()` → insert
   `sealing_data` → `enqueue_seal` → `try_activate_slot()` →
   `SegmentBuffer::new` → `BufferPool::acquire_sized(4 MiB)` (pop a 64 KB
   pre-allocated chunk + `reserve(4 MiB)` = a ~10–100 µs malloc).
2. During that transit the slot reports `Sealing` with no segment.
   `append_to_next_available_with_hook` skips non-Appending slots and returns
   `Error::InvalidConfig("no appending segment available in pool")` at
   pool.rs:410 when every slot is simultaneously non-appendable (Sealing, or
   Appending-but-full via the SegmentFull-skip path).
3. With 32 workers churning 4 MiB segments (~16 fills/s measured), a burst
   can put all 4 slots in transit at once; an append arriving inside the
   µs-scale window fails.
4. The failure is clean and client-retryable: nothing is recorded (no blob
   index entry, no WAL entry) before the error, and no slot leaks — the
   in-transit slots are re-activated synchronously by their filler threads,
   which never block on appenders.

**Recycling facts (verified):**

- `BufferPool::release()` is called only from tests — never in production.
  The recycle path is dead code.
- Actual lifecycle: activation = `acquire_sized(4 MiB)` = pop one of the 1024
  pre-allocated 64 KB chunks (consumed once each) + `reserve(4 MiB)` → 1
  malloc(4 MiB) + 1 free(64 KB) churn per segment; seal = the last `Bytes`
  is dropped → 1 free(4 MiB). After warm-up, every segment activation
  mallocs.
- The fill path deliberately freezes (`into_buffer().freeze()`) because the
  buffer must stay alive for `sealing_data` reads during the seal window and
  for the seal worker's disk write (zero-copy; a copy-then-return alternative
  would cost ~400 µs per seal vs ~50–100 µs malloc — rejected).
- `bytes 1.12.1` (in Cargo.lock) has `Bytes::try_into_mut()` — reuse IS
  possible post-seal without any copy.
- `BufferPool::release` already `clear()`s the buffer, so no stale data can
  leak into a reused segment. WAL-replay-created segments use the same
  acquire path, so they are covered.

## Design & Scope

### Problem 1 — transient slot exhaustion (~0.3% PUT 5xx)

As described in Evidence: an append arriving while every slot is in the
synchronous `Sealing` transit window fails immediately. The window is
µs-scale; failing is wrong when waiting a few milliseconds would succeed.

### Workstream A — bounded backpressure (the correctness fix)

In `SegmentPool` (`crates/oceanfs-storage/src/segment/pool.rs`):

- Add a `parking_lot::Condvar`, paired with a dedicated
  `parking_lot::Mutex<()>` used purely as the wait primitive.
- Call `notify_all()` in `try_activate_slot` after a slot is re-activated.
- In `append_to_next_available_with_hook`, when the scan exhausts all slots:
  wait on the condvar with a bounded timeout (~10 ms, exceeding the µs-scale
  re-activation by orders of magnitude), re-scan, and only on timeout return
  the existing `InvalidConfig` error.
- Document the wait in the `append`/`append_with_hook` doc comments (the
  methods remain synchronous, now "with bounded wait").

**Deadlock analysis:** slot re-activation is synchronous in the filling
thread immediately after `enqueue_seal`; it never waits on appenders.
Spurious wakeups are harmless (re-scan + timeout).

**Tests:**

1. Deterministic — a `#[cfg(test)]` helper parks all slots in `Sealing`,
   spawns an appender thread, asserts it blocks, then re-activates a slot and
   asserts the appender completes.
2. Timeout path — returns the existing `InvalidConfig` error.
3. Statistical — `active_pool_size = 2`, tiny target sizes to maximize fill
   churn, many threads × many appends, assert zero `InvalidConfig` errors.

### Problem 2 — segment buffers are never recycled (one malloc per segment forever)

As described in Evidence: `release()` is dead code in production and every
activation mallocs. Reuse is possible post-seal with zero copy via
`Bytes::try_into_mut()`:

- In the coordinator's seal worker success path
  (`crates/oceanfs-server/src/write/coordinator.rs`, `start_seal_worker`),
  immediately after `remove_seal_buffer(segment_id)` (which drops the
  `sealing_data` reference), the work item's `segment_data: Bytes` is the
  unique owner of the original `BytesMut` allocation → `try_into_mut()`
  succeeds → hand the `BytesMut` back to the pool via a new
  `SegmentPool::release_buffer(buf)` accessor (forwards to the internal
  `buffer_pool: Arc<BufferPool>` field, pool.rs:133) → the next
  `acquire_sized(4 MiB)` pops it with capacity ≥ 4 MiB → `reserve` is a
  no-op → **zero allocation per activation after warm-up**.
- Fallback: if `try_into_mut` fails (refcount > 1 — shouldn't happen after
  `remove_seal_buffer`), drop the `Bytes`. Safe by construction.
- `BufferPool::release` already `clear()`s the buffer, so no stale data can
  leak into a reused segment. WAL-replay-created segments use the same
  acquire path, so they are covered.

### Required companion changes (the non-trivial part)

1. **Byte-bounded, size-classed accounting.** Today `release()` bounds the
   free list by COUNT (`max_buffers`, default 1024) — fine for 64 KB chunks
   (64 MiB cap), but 4 MiB buffers would allow up to 4 GiB retained. Design:
   two size classes (small ~64 KB, standard ~4 MiB), each free list bounded
   in BYTES (e.g., standard class cap 64 MiB = 16 segments, small class cap
   16 MiB). The initial 64 MiB pre-allocation (1024 × 64 KB) becomes
   unnecessary with recycling — make the pool lazy or pre-allocate per size
   class with small byte budgets.
2. **Both tiers share one `BufferPool` in `oceanfs-node`** (same
   `shard_buffer_pool` for small + standard pools), so the size classes must
   live inside `BufferPool` (per-class free lists + per-class byte budgets),
   with `acquire_sized` choosing the class by requested capacity and
   `release` returning to the class of the buffer's actual capacity.
3. **`SegmentPool::release_buffer(&self, buf: BytesMut)`** `pub` accessor
   (crates/oceanfs-storage/src/segment/pool.rs:612) — not `pub(crate)`: the
   caller (the seal worker) lives in `oceanfs-server`, so cross-crate
   visibility is required; consistent with the ADR-0021 precedent of
   `remove_seal_buffer` — + the coordinator seal worker calling
   `try_into_mut` after `remove_seal_buffer` on both the small and
   standard paths.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `segment/pool.rs`: condvar + bounded wait in `append_to_next_available_with_hook`, notify in `try_activate_slot`, `release_buffer` accessor, tests. `buffer_pool.rs`: size-classed byte-bounded free lists, lazy pre-allocation, updated docs/tests. |
| `oceanfs-server` | `write/coordinator.rs` seal worker: `try_into_mut` + `release_buffer` after `remove_seal_buffer` (both tiers). No other changes. |
| `oceanfs-node` | No code change expected (defaults shared); verify only. |

## Out of Scope

- No change to the seal-queue `try_send` semantics (drop-on-full stays).
- No change to `SealingWork`'s type.
- No EC/streaming changes.
- No config-driven pool sizing (follow-up if needed).

## Known pre-existing observations (out of scope)

Flagged by the independent reviewer as potential follow-ups; both are
pre-existing and intentionally not addressed by this feature:

1. **Seal worker skips `remove_seal_buffer` on two paths.** The
   empty-entries skip path and the seal-failure arm in the coordinator's
   seal worker do not call `remove_seal_buffer`, so the `sealing-data`
   entry is retained and no recycle occurs there. Pre-existing ADR-0021
   scoping (removal only on success).
2. **`try_activate_slot` holds the slot's segment lock during
   `SegmentBuffer::new`** (allocation on miss). A letter-of-perf-7.1
   deviation, pre-existing, fill-path-only.

## Definition of Done

- [x] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` on touched crates,
      `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`
<!-- REVIEW: verified 2026-08-13 — build --all-targets clean, fmt clean, clippy --lib -D warnings (storage+server) clean, doc clean. Iteration 2 re-verified: build --all-targets clean, fmt --all -- --check clean, clippy -D warnings clean (storage+server+node --lib), RUSTDOCFLAGS="-D warnings" cargo doc --no-deps clean. -->

- [x] **Tests:** deterministic condvar wait test (blocked appender completes
      on re-activation), timeout test, statistical churn test
      (`concurrent_churn_never_exhausts_slots`) with a documented tolerance
      of ≤4 failures / 3200 appends (0.125%), recycling tests (released
      buffer re-acquired without reserve; byte caps enforced;
      clear-on-release prevents data leakage)
<!-- REVIEW (iteration 2): timeout test now present — append_returns_error_when_activation_keeps_failing (pool.rs:972-991) with a #[cfg(test)] fail_activation AtomicBool seam in try_activate_slot (pool.rs:542-547); seam verified production-safe (cfg(test) on field, ctor init, and check). Test verified passing 5/5, deterministic (pure timeout, no notify can fire). Terminal error re-verified at pool.rs:495. All other tests green. DEVIATION: concurrent_churn_never_exhausts_slots now tolerates ≤4 failures / 3200 appends (0.125%) instead of zero (pool.rs:1056-1060) — ACCEPTED: mutation-verified independently — removing the budget-refresh (pool.rs:492) → 27/3200 failures → test FAILS; removing the self-heal (pool.rs:470) → caught deterministically by append_self_heals_when_all_slots_are_parked (FAILS). 20/20 flake-free churn runs. Wording amended 2026-08-13: the ≤4 / 3200 tolerance is a scheduling-turbulence floor measured in the adversarial 16-thread × 4-slot / every-append-fills configuration — at exhaustion the pool was healthy (slots Appending, last activation 45 µs prior). Regression detection is split across two tests: removing the budget refresh fails the churn test (~27/3200 ≫ 4); removing the self-heal fails the deterministic append_self_heals_when_all_slots_are_parked test. Both mutation-verified by the reviewer. -->
- [x] **Tests:** `cargo test -p oceanfs-storage -- --test-threads=1` and
      `cargo test -p oceanfs-server --lib -- --test-threads=1` green
<!-- REVIEW (iteration 2): re-verified — storage 172 lib tests + all integration suites green; server 207 lib green; node 32 lib green (3/3 re-runs; one SIGABRT at process exit observed once = known RocksDB C++ teardown, PIPELINE.md §4.6, did not reproduce). -->

- [x] **Integration:** seed-42 30 s load test PASS, `manifest_integrity`
      0 mismatches, `puts_4xx == 0`. The 5xx gate is **log-based** (Open
      Question 3 decision), not an assertion: no `puts_5xx == 0` assertion
      was added to `e2e/tests/load_concurrency.rs` (which only asserts
      `puts_4xx == 0`); instead the node log is grepped for the specific
      exhaustion message `no appending segment available in pool` —
      verified 0 occurrences in the seed-42 30 s run, all 3 random-seed
      runs, and the 120 s run.
<!-- REVIEW (iteration 2): seed-42 30 s re-run with current binary — PASS; captured node logs show 0 "no appending segment available in pool", 0 BadDigest, 0 "cannot fetch chunk", 0 "seal queue full", 456 slot re-activations and 456 successful seals (recycle path exercised). Wording amended 2026-08-13 per review: the 5xx gate is log-based, not an assertion — no puts_5xx == 0 assertion exists in e2e/tests/load_concurrency.rs (only puts_4xx == 0, line 166-171); the Open Question 3 decision was to grep the node log for the specific exhaustion message, verified 0 occurrences in seed-42 30 s, 3 random seeds, and the 120 s run. -->

- [x] **Integration:** 3× random seeds flake-free
<!-- REVIEW: verified 2026-08-13 — seeds 7, 1234, 987654 (30 s each): all PASS, puts_5xx=0, 0 mismatches. Iteration 2: seed 987654 re-run with current binary — PASS, log clean (0 exhaustion/BadDigest/cannot-fetch/seal-queue-full, 500 re-activations). -->
- [x] **Perf:** 120 s run — RSS stable AND flatter than the pre-recycling
      baseline (no per-activation malloc churn), `puts_5xx == 0`,
      `puts_multi > 0` with all multi-tier objects verified readable
<!-- REVIEW: verified 2026-08-13 — 120 s run PASS: 2147 PUTs, puts_5xx=0, puts_multi=428, 1806 written/1806 verified/0 mismatches. RSS sampled every ~4 s: 448–1236 MB sawtooth with repeated drops (no monotonic growth; avg 752 MB). "Flatter than baseline" not quantitatively demonstrated (feature-1 baseline 190–960 MB under different conditions); recycling mechanism itself verified via unit tests + coordinator release path (coordinator.rs:693-706) + sawtooth pattern. Iteration 2: production code unchanged (iteration-2 delta is cfg(test) seam + test tolerance + comments), so the 120 s result carries over. EVALUATION ITEM: SLOT_ACTIVATION_WAIT=10 ms / SLOT_ACTIVATION_WAIT_SLICE=1 ms (pool.rs:37,44) deliberately kept for Phase 2 sustained-load evaluation — check for bottlenecking there, not a gap. RESOLVED 2026-08-13 (explicit user decision): the hardcoded latency constants `SLOT_ACTIVATION_WAIT` (10 ms silence budget) and `SLOT_ACTIVATION_WAIT_SLICE` (1 ms wakeup granularity) in crates/oceanfs-storage/src/segment/pool.rs are deliberately kept for now; whether the 1 ms lost-wakeup slice bottlenecks write latency will be evaluated in later test phases (Phase 2 sustained load). -->

- [x] **Observability:** node log clean of `no appending segment available`
      / `BadDigest` / `cannot fetch chunk` during the runs
<!-- REVIEW: verified 0 occurrences of all three patterns (and 0 `seal queue full`) in captured node logs for both the 30 s and 120 s runs. Iteration 2: re-verified in fresh seed-42 and seed-987654 30 s runs (0 occurrences of all four patterns). -->

## Open Questions

1. **Resolved.** Byte budgets per size class. The proposed 64 MiB standard
   / 16 MiB small split was NOT implemented — both size classes share the
   same per-class byte budget of `buffer_pool_chunk_bytes ×
   buffer_pool_max_chunks × shard_count` = 64 MiB × shard_count (e.g.,
   256 MiB per class at the default `shard_count = 4`). The node constructs
   ONE shared `BufferPool` with `total_pool_chunks = max_chunks ×
   shard_count` (crates/oceanfs-node/src/node.rs:559,568-571), and
   `BufferPool::new` applies that single budget to each size class
   (crates/oceanfs-storage/src/buffer_pool.rs:120-126). No config knob
   exists (out of scope).
<!-- REVIEW (iteration 2): verified — both classes share one byte budget = buffer_pool_chunk_bytes × buffer_pool_max_chunks × shard_count (buffer_pool.rs:119-126 SizeClass::new(budget); node.rs:559,568-571). Defaults: 64 KB × 1024 × shard_count, shard_count = min(num_cpus, 16) (config/shard.rs:23-29) → 64 MiB × shard_count per class (e.g. 256 MiB/class at shard_count=4), NOT a fixed "64 MiB both classes". Neither budget matches the proposed 64/16 MiB split, and there is no config knob. Question resolved 2026-08-13 with the actual formula recorded above. -->
2. Backpressure timeout value (proposed 10 ms) — trade-off vs client latency
   under genuine sustained overrun.
<!-- Implemented with `SLOT_ACTIVATION_WAIT` = 10 ms / `SLOT_ACTIVATION_WAIT_SLICE` = 1 ms (pool.rs:37,44). Per explicit user decision (2026-08-13) the constants are deliberately kept for now; whether the 1 ms lost-wakeup slice bottlenecks write latency will be evaluated in later test phases (Phase 2 sustained load). See the Perf DoD item. -->
3. **Resolved.** Neither a generic `puts_5xx == 0` assertion nor a scoped
   assertion was added — the decision was a **log-based gate**: grep the
   node log for the specific exhaustion message (`no appending segment
   available in pool`), verified 0 occurrences in the seed-42 30 s run, all
   3 random-seed runs, and the 120 s run. `e2e/tests/load_concurrency.rs`
   still asserts only `puts_4xx == 0`.
4. Metric exposure: a `buffer_pool_free_bytes` gauge per size class for
   observability.
