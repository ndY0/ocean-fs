---
feature: "Compaction as a State Machine"
epic: "refactoring/segment-event-log-lifecycle/segments-cf-removal"
status: done
priority: high
owner: ""
dependencies:
  - feature: segments-cf-removal
    epic: refactoring/segment-event-log-lifecycle/segments-cf-removal
    reason: "Order SWAPPED at landing (user-validated; Deviations D1): this feature landed BEFORE segments-cf-removal. The spec's original dependency order contradicted itself — it deleted the segments CF while the compactor still wrote it. The compactor's move onto the machine needs only the EPIC-2 machinery (coordinator + event WAL); compaction recovery (rows 7-9) needs only the objects CF. Landed order: compaction-state-machine → segments-cf-removal → startup-rebuild-from-machine."
  - feature: lifecycle-registry-coordinator
    epic: refactoring/segment-event-log-lifecycle/segment-lifecycle-machine
    reason: The compactor requests transitions from the coordinator; it never writes state or events itself (ADR-0025 Decision 4)
  - feature: event-wal-format
    epic: refactoring/segment-event-log-lifecycle/segment-event-log
    reason: The durable milestones are SealEvent(new) and DeleteEvent(old); the format's event types and data_wal_pos are the machine's compaction vocabulary
adr:
  - 0025-segment-lifecycle-state-machine
  - 0024-segment-event-log
  - 0023-metadata-store-native-replacement-path
perf:
  - "7.1 Minimize lock hold duration"
  - "1.1 bytes BytesMut for blob data"
  - "1.3 Pre-size collections with known capacity"
created: 2026-08-17
updated: 2026-08-18
---

# Compaction as a State Machine

## Summary

Turn the GC compactor (`crates/oceanfs-durability/src/gc/segment_compactor.rs`)
into a state machine with five crash-relevant milestones whose durable
checkpoints are events (ADR-0025 Decision 4):

```
Copying       → new .dat being written (no durable event yet)
NewSealed     → SealEvent(new) appended          [durable]
ObjectsMoved  → PutObject(new refs) committed    [RocksDB]
OldDeleted    → DeleteEvent(old) appended        [durable]
OldRemoved    → old .dat unlinked
```

The compactor **requests** each transition from the `SegmentLifecycleCoordinator`
and never writes state or events itself. The coordinator enforces ADR-0024's
compaction ordering (new `.dat` → `SealEvent(new)` → `PutObject(new)` →
`DeleteEvent(old)` → unlink old), so the metadata-only-compaction and
BadDigest defects become structurally impossible: a new segment cannot exist
without a durable `.dat` (its `SealEvent` carries the full repacked
metadata), and the old segment cannot disappear before objects point at the
new one. Crash recovery is a fold + one objects-CF read; crash-window rows
7–9 are added to the fault-injection matrix.

## Evidence/Motivation

Two of the four 2026-08-16/17 load-test defects live in this compactor:

- **Metadata-only compaction** — the compactor created a new segment id and
  remapped object chunks to it **without ever writing the new segment's
  `.dat`** (`segment_compactor.rs`), because it had no data store and no
  lifecycle discipline; crash-recovery mismatches resulted. With the
  machine, "the new segment is real" is exactly "its `SealEvent` is
  durable", and the coordinator's `seal()` requires the `.dat` fsync to
  precede the event (crash-window row 7 proves the ordering: kill between
  fsync and event → adopted, never lost; kill before fsync → `Copying`, no
  event, old segment intact).
- **Compression-ref corruption on repack (BadDigest after restart)** — the
  compactor hardcoded `compressed: false` on repacked `ChunkRef`s, so reads
  of repacked compressed objects returned raw compressed bytes. The
  machine's `seal()` API takes the full repacked metadata
  (`compressed` + `logical_length` + checksum per chunk), so a repack that
  drops compression state cannot produce a valid `SealEvent`.

The precedent is the slot-state-machine feature: structural invariants beat
reactive patches. The compactor's ordering was folklore ("the compactor
knows the sequence"); now it is the coordinator's transition API.

## Scope

### In Scope

- `CompactionState` machine + milestones in
  `crates/oceanfs-durability/src/gc/compaction_recovery.rs` (a sibling
  module of `segment_compactor.rs`), with the coordinator as the only
  transition executor:
  - `Copying` → write new `.dat` (no durable event; crash here leaves
    nothing — old segment untouched).
  - `NewSealed` → `coordinator.request_seal(new_id, full repacked metadata,
    repacked_from: Some(old_id))` — only after the new `.dat` fsync
    returns.
  - `ObjectsMoved` → `put_object(new refs)` committed in the objects CF
    (RocksDB; the one cross-store hop — ordering by construction, not
    atomicity).
  - `OldDeleted` → `coordinator.request_delete(old_segment_id)`.
  - `OldRemoved` → unlink old `.dat` (issued only after `request_delete`
    returns durable).
- ChunkRef repack discipline: every repacked `ChunkRef` carries the source
  chunk's `compressed` + `logical_length` + checksum verbatim; the repack
  output feeds `request_seal`, so a corrupt repack is rejected at the
  transition (seal-time metadata validation).
- Crash recovery per milestone (rows 7–9, added to the fault-injection
  matrix):
  - Row 7 (NewSealed → ObjectsMoved): new sealed, objects→old → new `.dat`
    orphan → reaper.
  - Row 8 (ObjectsMoved → OldDeleted): objects→new, old sealed → old
    segment sealed-orphan → reaper (`request_delete`).
  - Row 9 (OldDeleted → OldRemoved): old deleted, `.dat` present → old
    `.dat` orphan → sweep.
  - Recovery input: fold (tells which new segments are sealed and which old
    are deleted) + **one objects-CF read** (do objects point at new or
    old?) — exactly the ADR-0025 Decision 4 recovery shape.
- Compactor call sites updated: GC orchestrates the machine per compaction
  unit; the coordinator's transition API is the only state/event writer
  reachable from `segment_compactor.rs`.

### Out of Scope

- Compaction *policy* (what to compact, when) — unchanged.
- Objects-CF storage (stays RocksDB; `PutObject` remains a RocksDB write).
- Rows 1–6 of the crash matrix (feature `event-wal-recovery`) and the
  node-level matrix gate (feature `startup-rebuild-from-machine`).
- Any direct event appends or registry writes by the compactor — the DoD
  makes that unrepresentable.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | `gc/segment_compactor.rs`: `CompactionState` machine, milestone transitions via the coordinator, repack discipline; `gc/compaction_recovery.rs`: recovery for rows 7–9 |
| `oceanfs-storage` | `segment/lifecycle.rs`: seal-time metadata validation (repack fields) at `request_seal`; no other change |
| `oceanfs-node` | Composition root: GC wired with the coordinator handle |

## Interface (Public API)

Shipped as of the implementation (verified 2026-08-18; the pre-implementation
sketches are recorded in the Deviations section):

- `pub enum CompactionState { Copying, NewSealed, ObjectsMoved, OldDeleted, OldRemoved }`
  — the compactor's own progress (in-memory; observability + tests); the
  durable checkpoints are the events, not this enum — crash recovery never
  reads it, it reads the fold.
- `pub struct CompactionUnit { pub old_segment_id: SegmentId, pub new_segment_id: SegmentId, pub tier: SizeTier, pub ec_k: u8, pub ec_m: u8 }`
  — both types live in `gc/compaction_recovery.rs` and are `pub`.
- `SegmentLifecycleCoordinator` (existing, from `lifecycle-registry-coordinator`):
  `pub async fn request_seal(&self, id: SegmentId, metadata: SegmentMetadata, repacked_from: Option<SegmentId>) -> Result<(), TransitionError>`
  — the `SealEvent` is built **inside** the coordinator (from the arguments
  plus the registry entry's recorded `data_wal_pos`); the compactor never
  constructs an event. The seal carries the full repacked metadata, so a
  repack that drops compression state cannot produce a valid `SealEvent`.
  When `repacked_from` is set, the tier/EC shape is cross-checked against
  the source segment's registry entry and a mismatch is rejected **before
  any durable write** (`lifecycle.rs:1844-1856`).
- `pub trait ObjectLookup: Send + Sync { fn is_referenced(&self, segment_id: SegmentId) -> Result<bool>; }`
  — the one objects-CF read per unit; tests use an instrumented double that
  counts reads (DoD assertion).
- `pub fn recover_incomplete_compactions(registry: &SegmentLifecycleRegistry, objects: &dyn ObjectLookup) -> Result<Vec<CompactionRecoveryAction>>`
  — fold + one objects-CF read per unit →
  `{ FinishOldDeletion(SegmentId), SweepNewOrphan(SegmentId), SweepOldDat(SegmentId) }`.
  `Result`-wrapped, **no `None` variant**: complete units yield an empty
  vec, and `SweepOldDat` is idempotent.

## Data Flow

```
GC selects a segment
  → Copying:      write new .dat (streaming, BytesMut discipline)
  → fsync new .dat
  → NewSealed:    coordinator.request_seal(new_id, full metadata, Some(old_id))
  → ObjectsMoved: put_object(new refs) [RocksDB]
  → OldDeleted:   coordinator.request_delete(old)
  → OldRemoved:   unlink old .dat

crash at any milestone
  → fold events
  → one objects-CF read (new refs committed?)
  → row 7/8/9 recovery action (reaper / sweep)
```

## Deviations

Accepted deviations agreed between implementer and independent reviewer
(iteration 2, verdict **PASS**, 0 items incomplete). The shipped shapes
below are the code as verified 2026-08-18; each spec sketch that drifted
during implementation is recorded here rather than silently edited out.

### D1 — Feature order swap (user-validated)

Landed order: **compaction-state-machine → segments-cf-removal →
startup-rebuild-from-machine** — not segments-cf-removal first, as the
spec's original dependency order had it. The original order contradicted
itself: it deleted the segments CF while the compactor still wrote it
(`batch_write(PutSegment(new) + DeleteSegment(old))` was the documented
migration surface). The compactor's move onto the machine needs only the
EPIC-2 machinery (coordinator + event WAL); compaction recovery (rows 7–9)
needs only the objects CF (one `is_referenced` read per unit). The
`dependencies:` frontmatter note is amended accordingly.

### D2 — Interface shape: `request_seal`

The spec sketched `request_seal(evt: SealEvent)`; the shipped signature is
`pub async fn request_seal(&self, id: SegmentId, metadata: SegmentMetadata,
repacked_from: Option<SegmentId>) -> Result<(), TransitionError>`
(`lifecycle.rs:1833`) — the `SealEvent` is built inside the coordinator
from the arguments plus the registry's `last_data_wal_pos`; the compactor
never constructs an event. When `repacked_from` is set, tier/EC are
cross-checked against the source segment's registry entry and a mismatch is
rejected **before any durable write** (`lifecycle.rs:1844-1856`).

### D3 — Event format: compaction marker in the SealEvent payload

The compaction marker rides in the `SealEvent` payload's formerly-reserved
flags byte (bit 0 = `SEAL_FLAG_REPACKED_FROM`) plus an optional 16-byte
segment id: payload 48 → 64 bytes (`SEAL_PAYLOAD_SIZE` 48,
`MAX_PAYLOAD_SIZE` 64). Old unmarked records decode unchanged (`flags = 0`,
payload length 48).

### D4 — Checkpoint format v2

Checkpoint format bumped to **v2**: Sealed entries carry `repacked_flag(1)`
+ `repacked_from(16)`. Version-1 checkpoints are rejected — none deployed
(the format landed with EPIC 2), so no legacy files need reading
(`event_checkpoint.rs` module doc).

### D5 — Recovery actions: `Result`-wrapped, no `None`

`recover_incomplete_compactions(registry, &dyn ObjectLookup) ->
Result<Vec<CompactionRecoveryAction>>` — `Result`-wrapped (the objects-CF
read can fail), no `None` variant: complete units yield an empty vec, and
`SweepOldDat` is idempotent (a missing file is `Ok(0)`).

### D6 — Pre-NewSealed marker loss (accepted gap, rows 3 and 9)

A compaction unit killed before its `SealEvent` is adopted by the data-WAL
pass (crash-window row 3) carries **no** `repacked_from` marker — the
adoption path doesn't know the unit; the unreferenced replacement is then
reaped by the general orphan scan. The fully-dead path's crash residue
(deleted + `.dat` present) has no marker either — the `.dat` sweep is
dispatched by `startup-rebuild-from-machine` (the reaper has no `.dat`
scan today; pre-existing gap pinned by storage row 5).

### D7 — Test seam: `cfg(test)` stall

A `cfg(test)` stall seam (`stall_seam`: `STALL_AT`/`REACHED` atomics) in
`gc/segment_compactor.rs` enables in-process kill-at-milestone fault
injection for rows 7–9; the production build carries no seam.

### D8 — Fully-dead path unlinks through the coordinator

The fully-dead path now performs delete-then-unlink **through the
coordinator + shard store** (`request_delete` durable → shard sweep).
Previously the `.dat` file leaked — nothing unlinked it.

### D9 — Seal-time shape validation

`request_seal` rejects a repacked seal whose tier/EC contradict the source
segment (the shape never changes across a repack) — no durable write on
rejection (`TransitionError::DurableWriteFailed` before any event append).

## Definition of Done

- [x] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` pass in `oceanfs-durability`,
      `oceanfs-storage`, `oceanfs-node`; `#![deny(missing_docs)]` passes.
      **Verified (2026-08-18, iteration 2):** build `--all-targets` clean
      for all three crates (no warnings — the `dead_code Harness::crash`
      and unused-import warnings are removed), `fmt --check` clean, clippy
      `--lib -D warnings` clean on all three,
      `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` clean on all three,
      `#![deny(... missing_docs ...)]` present in
      durability/storage/node `lib.rs` (lines 23/24/21). Note: clippy
      `--all-targets -D warnings` reports test-code warnings — non-gating
      per the lint note below.
- [x] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      and `-p oceanfs-storage --lib -- --test-threads=1` green; the
      existing GC/compactor suites pass against the machine-backed store.
      **Verified (2026-08-18, iteration 2):** durability lib 233 passed
      (incl. 9 `gc::compaction_crash` tests), storage lib 322 passed
      (`--test-threads=1`); durability integration suites green
      (`gc_compaction` 5, `orphan_reaper` 7, `anti_entropy` 14,
      `distributed_scrub` 5, `merkle_recovery` 3); node lib 32 passed +
      node tests green; server lib 217 passed.
- [x] **Invariant — compactor never writes state or events
      (ADR-0025 Decision 4):** `segment_compactor.rs` contains no
      `put_segment`/deleted-marker writes and no event-log/registry writes;
      every milestone transition goes through the coordinator
      (grep-verifiable + mutation check: a compactor-side state write must
      fail a test).
      **Verified:** production code (lines 1–448) has zero
      `put_segment`/`delete_segment`/`event_wal`/`registry`/`fold_` calls;
      the struct holds no `EventWal`/registry handles (only
      `Arc<SegmentLifecycleCoordinator>`, `dyn MetadataStore` for
      `PutObject`-only `batch_write`, `data_store`, `shard_store`).
      Mutation detection: a direct `SealEvent`-before-`.dat` write breaks
      `kill_before_dat_write_...` (`dropped_empty_reserves == 1` assert,
      `compaction_crash.rs:322`); a delete-before-move breaks row 8's
      old-Sealed assert (`compaction_crash.rs:486`).
- [x] **Invariant — ADR-0024 compaction ordering:** the coordinator
      rejects `request_seal(new)` before the new `.dat` fsync returns
      (test: ordering enforced by API shape + row 7 fault injection); the
      old `.dat` unlink is issued only after `request_delete(old)` returns
      durable (row 9). The full five-milestone sequence is exercised by one
      integration test with kills at each milestone.
      **Verified:** compactor order — `write_segment_data`
      (`segment_compactor.rs:255`) before `request_seal` (:292);
      `request_delete` (:351) before `delete_shards` (:358) and on the
      fully-dead path (:143–147). Ordering pinned by
      `kill_between_dat_write_and_seal` (`adopted_segments == 1`) and
      row-9 (`SweepOldDat`) tests; kills at all five stall seams (1–5) in
      `compaction_crash.rs`.
- [x] **Invariant — metadata-only compaction unrepresentable:** a
      compaction unit cannot reach `NewSealed` without a durable new
      `.dat` (crash before fsync → fold shows only `Copying`; the old
      segment is untouched and objects still point at it). Mutation check:
      emitting `SealEvent(new)` before the fsync must fail the crash test.
      **Verified:**
      `kill_before_dat_write_folds_copying_and_drops_the_reserve`
      (`compaction_crash.rs:293`) asserts reserve dropped + old Sealed +
      objects→old + no actions;
      `kill_between_dat_write_and_seal_adopts_the_new_dat` (:335) asserts
      `adopted_segments == 1` (a pre-fsync `SealEvent` would flip the fold
      to Sealed → assert fails).
- [x] **Invariant — BadDigest unrepresentable:** repacked `ChunkRef`s
      preserve `compressed` + `logical_length` + checksum; the regression
      test compacts a compressed object, restarts, and reads it back with a
      matching digest. Mutation check: hardcoding `compressed: false` on
      repack (the original defect) must fail the read-back test.
      **Verified:** repack copies length/compressed/logical_length verbatim
      (`segment_compactor.rs:190-204`);
      `repacked_compressed_chunk_reads_back_with_matching_digest`
      (`compaction_crash.rs:597`) asserts `chunk.compressed` (line 641 —
      hardcoded `false` fails here), logical_length/length equality,
      verbatim bytes, and blake3 digest match after compaction+restart; the
      uncompressed variant (`compaction_crash.rs:746`) pins the
      verbatim-bytes path with `compressed: false`.
- [x] **Invariant — crash-window rows 7–9 are fault-injection tests:**
      kill between NewSealed/ObjectsMoved/OldDeleted/OldRemoved; assert
      recovery lands in the table's folded state with the correct action
      (row 7 → new `.dat` orphan → reaper; row 8 → old sealed-orphan →
      reaper `request_delete`; row 9 → old `.dat` orphan → sweep), and
      reads resolve correctly after recovery (objects → new or old per
      milestone).
      **Verified:** `stall_seam` (`segment_compactor.rs:895-916`) arms
      kills at milestones 1–5; row 7/8/9 tests (`compaction_crash.rs:387 /
      454 / 506`) drive via `drive_to_milestone` + `advance_to`, abort the
      task + drop harness (true crash), reboot, fold + recover, assert
      folded state (marker/Sealed/Deleted), exact action vector, and read
      resolution; dispatch end-states asserted via coordinator
      `request_delete` + shard sweep.
- [x] **Recovery = fold + one objects-CF read:** `recover_incomplete_compactions`
      performs exactly one read of the objects CF per unit (assert in test
      via an instrumented lookup trait); no per-chunk scans.
      **Verified:** one `is_referenced` call per marked unit
      (`compaction_recovery.rs:140`); `CountingLookup` asserts
      reads == units (tests at :238, :253, :272, :297, :315-319).
- [x] **Perf 7.1:** no registry/coordinator lock held during `.dat` copy or
      encode (compute outside the lock); repack buffers are `Bytes`/`BytesMut`
      (perf 1.1), pre-sized per chunk (perf 1.3).
      **Verified:** repack/merkle/encode run outside any lock (compactor
      holds no registry lock; coordinator validate→I/O→fold split,
      `lifecycle.rs:37-41`); repacked is `BytesMut::with_capacity`
      (`segment_compactor.rs:174`), `chunk_remap`
      `HashMap::with_capacity` (:169), ops `Vec::with_capacity` (:309);
      recovery collects units under read guards then reads objects-CF
      outside (`compaction_recovery.rs:128-140`).
- [x] **Integration:** GC → compaction → restart → scrub → read-back chain
      green on compressed and uncompressed objects; AE root for the new
      segment matches the machine's `Sealed` entry.
      **Verified (2026-08-18, iteration 2):** all chain steps green.
      GC→compaction: `full_gc_cycle_compacts_segment` (durability
      `tests/gc_compaction.rs:151`, machine-backed harness).
      compaction→restart→read-back compressed:
      `repacked_compressed_chunk_reads_back_with_matching_digest`
      (`compaction_crash.rs:597`) — flags preserved verbatim, digest match,
      machine root == recomputed root == CF mirror root.
      compaction→restart→scrub→read-back:
      `post_compaction_segment_scrubs_healthy_against_the_machine_root`
      (`compaction_crash.rs:688`) — real `crate::scrub::ScrubWorker::
      scrub_segment` against the machine's `Sealed` entry metadata;
      healthy && !merkle_mismatch && !skipped && bytes_scanned > 0;
      read-back digest match. uncompressed compaction→restart→read-back:
      `repacked_uncompressed_chunk_reads_back_with_matching_digest`
      (`compaction_crash.rs:746`) — verbatim bytes + digest. Scrub step is
      pinned on the uncompressed path (Merkle check is format-agnostic:
      root over raw `.dat` bytes vs machine root).

> **Lint & Doc Examples (non-gating):** `cargo clippy --all-targets -D
> warnings` test-code warnings and `ignore`-tagged doc examples are
> structural hygiene tracked separately (guidelines/coding.md §9.2.1).
