---
feature: "Compaction as a State Machine"
epic: "refactoring/segment-event-log-lifecycle/segments-cf-removal"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: segments-cf-removal
    epic: refactoring/segment-event-log-lifecycle/segments-cf-removal
    reason: The compactor's crash rows finish through the machine-based reaper and retention; consumers must be on the machine before compaction recovery is deterministic
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
updated: 2026-08-17
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
  `crates/oceanfs-durability/src/gc/segment_compactor.rs` (or a sibling
  module), with the coordinator as the only transition executor:
  - `Copying` → write new `.dat` (no durable event; crash here leaves
    nothing — old segment untouched).
  - `NewSealed` → `coordinator.request_seal(SealEvent { new_segment_id,
    full repacked metadata, data_wal_pos of the new segment's entries,
    merkle_root computed at seal })` — only after the new `.dat` fsync
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
| `oceanfs-durability` | `gc/segment_compactor.rs`: `CompactionState` machine, milestone transitions via the coordinator, repack discipline; recovery module for rows 7–9 |
| `oceanfs-storage` | `segment/lifecycle.rs`: seal-time metadata validation (repack fields) at `request_seal`; no other change |
| `oceanfs-node` | Composition root: GC wired with the coordinator handle |

## Interface (Public API)

- `pub enum CompactionState { Copying, NewSealed, ObjectsMoved, OldDeleted, OldRemoved }`
  — the compactor's own progress (in-memory); the durable checkpoints are
  the events, not this enum.
- `pub struct CompactionUnit { pub old_segment_id: SegmentId, pub new_segment_id: SegmentId, pub tier: SizeTier, pub ec_k: u8, pub ec_m: u8 }`
- `SegmentLifecycleCoordinator` (existing, from `lifecycle-registry-coordinator`):
  `request_seal(evt: SealEvent)` — extended with seal-time validation of
  the repacked metadata (`compressed`/`logical_length` consistency vs the
  source `ChunkRef`s when a `repacked_from: Option<SegmentId>` marker is
  set on the event).
- `pub fn recover_incomplete_compactions(registry: &SegmentLifecycleRegistry, objects_cf: &dyn ObjectLookup) -> Vec<CompactionRecoveryAction>`
  — fold + one objects-CF read → `{ FinishOldDeletion, SweepNewOrphan, SweepOldDat, None }`.

## Data Flow

```
GC selects a segment
  → Copying:      write new .dat (streaming, BytesMut discipline)
  → fsync new .dat
  → NewSealed:    coordinator.request_seal(SealEvent(new, full metadata))
  → ObjectsMoved: put_object(new refs) [RocksDB]
  → OldDeleted:   coordinator.request_delete(old)
  → OldRemoved:   unlink old .dat

crash at any milestone
  → fold events
  → one objects-CF read (new refs committed?)
  → row 7/8/9 recovery action (reaper / sweep)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` pass in `oceanfs-durability`,
      `oceanfs-storage`, `oceanfs-node`; `#![deny(missing_docs)]` passes.
- [ ] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      and `-p oceanfs-storage --lib -- --test-threads=1` green; the
      existing GC/compactor suites pass against the machine-backed store.
- [ ] **Invariant — compactor never writes state or events
      (ADR-0025 Decision 4):** `segment_compactor.rs` contains no
      `put_segment`/deleted-marker writes and no event-log/registry writes;
      every milestone transition goes through the coordinator
      (grep-verifiable + mutation check: a compactor-side state write must
      fail a test).
- [ ] **Invariant — ADR-0024 compaction ordering:** the coordinator
      rejects `request_seal(new)` before the new `.dat` fsync returns
      (test: ordering enforced by API shape + row 7 fault injection); the
      old `.dat` unlink is issued only after `request_delete(old)` returns
      durable (row 9). The full five-milestone sequence is exercised by one
      integration test with kills at each milestone.
- [ ] **Invariant — metadata-only compaction unrepresentable:** a
      compaction unit cannot reach `NewSealed` without a durable new
      `.dat` (crash before fsync → fold shows only `Copying`; the old
      segment is untouched and objects still point at it). Mutation check:
      emitting `SealEvent(new)` before the fsync must fail the crash test.
- [ ] **Invariant — BadDigest unrepresentable:** repacked `ChunkRef`s
      preserve `compressed` + `logical_length` + checksum; the regression
      test compacts a compressed object, restarts, and reads it back with a
      matching digest. Mutation check: hardcoding `compressed: false` on
      repack (the original defect) must fail the read-back test.
- [ ] **Invariant — crash-window rows 7–9 are fault-injection tests:**
      kill between NewSealed/ObjectsMoved/OldDeleted/OldRemoved; assert
      recovery lands in the table's folded state with the correct action
      (row 7 → new `.dat` orphan → reaper; row 8 → old sealed-orphan →
      reaper `request_delete`; row 9 → old `.dat` orphan → sweep), and
      reads resolve correctly after recovery (objects → new or old per
      milestone).
- [ ] **Recovery = fold + one objects-CF read:** `recover_incomplete_compactions`
      performs exactly one read of the objects CF per unit (assert in test
      via an instrumented lookup trait); no per-chunk scans.
- [ ] **Perf 7.1:** no registry/coordinator lock held during `.dat` copy or
      encode (compute outside the lock); repack buffers are `Bytes`/`BytesMut`
      (perf 1.1), pre-sized per chunk (perf 1.3).
- [ ] **Integration:** GC → compaction → restart → scrub → read-back chain
      green on compressed and uncompressed objects; AE root for the new
      segment matches the machine's `Sealed` entry.

> **Lint & Doc Examples (non-gating):** `cargo clippy --all-targets -D
> warnings` test-code warnings and `ignore`-tagged doc examples are
> structural hygiene tracked separately (guidelines/coding.md §9.2.1).
