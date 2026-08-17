---
feature: "Event WAL Recovery — Fold, data_wal_pos Seek, Crash-Window Fault-Injection Matrix"
epic: "refactoring/segment-event-log-lifecycle/segment-event-log"
status: proposed
priority: critical
owner: ""
dependencies:
  - feature: event-wal-format
    epic: refactoring/segment-event-log-lifecycle/segment-event-log
    reason: The fold consumes EventWal records and positions; the format's read_from/TornRecord semantics are the fold's input contract
  - feature: lifecycle-registry-coordinator
    epic: refactoring/segment-event-log-lifecycle/segment-lifecycle-machine
    reason: The fold rebuilds the registry through the coordinator's transition API; recovery-appended SealEvents go through the same single writer
adr:
  - 0024-segment-event-log
  - 0025-segment-lifecycle-state-machine
  - 0018-durability-wal-consolidation
perf:
  - "3.1 Sequential-only WAL writes"
  - "1.3 Pre-size collections with known capacity"
  - "7.1 Minimize lock hold duration"
created: 2026-08-17
updated: 2026-08-17
---

# Event WAL Recovery — Fold, data_wal_pos Seek, Crash-Window Fault-Injection Matrix

## Summary

Implement deterministic crash recovery for the machine: `state =
fold(events)` (ADR-0024 §Consequences, ADR-0025 Decision 3). On startup the
event log is replayed in position order into the registry through the
coordinator's transition API; the data WAL is treated as a **seekable pool
of blob bytes** — replayed only for segments the fold says were `Reserved`
but not yet sealed, with `data_wal_pos` making both the replay filter and
the retention/sweep boundary exact position comparisons instead of CF-set
membership (`cleanup_old_wal_files` / `file_contains_live_entries`,
`crates/oceanfs-storage/src/wal/replay.rs:309,426` are re-pointed at the
machine). This feature also delivers the **fault-injection test matrix** for
crash-window rows 1–6 of ADR-0025 §Crash-window table: kill the process at
each milestone, assert recovery lands in exactly the folded state. Rows 7–9
(compaction) are added by `compaction-state-machine`; the full nine-row
matrix is gated at node level by `startup-rebuild-from-machine`.

## Evidence/Motivation

Today's recovery is heuristic: `node.rs` "adopts" interrupted seal commits
(`crates/oceanfs-node/src/node.rs:1017-1078`, guidance anchor 995-1067) by
scanning for `.dat`-without-CF entries and recomputing roots, and the WAL
cleanup derives entry liveness from a CF scan
(`file_contains_live_entries`, `replay.rs:426`). The 2026-08-16/17 campaign
showed where that ends:

- **Phantom-downgrade race** — the CF-derived `durable_or_deleted` set
  (`replay.rs:349-465`) protected every file whose segment the CF said was
  unsealed; a downgraded entry protected files forever (~3.8 GB/hour leak).
  With the fold, "sealed" is a durable event; a protected file is a
  contradiction the machine cannot express.
- **Metadata-only compaction** — recovery could not distinguish "new
  segment exists" from "new segment has data"; with the fold, a segment is
  real only if its `SealEvent` is, and `SealEvent` requires a durable
  `.dat` (verified by crash-window rows 2/3/7).
- **BadDigest repack** — a recovery-time mismatch class (compression refs
  corrupted on repack); the fold + seal-with-full-metadata makes the stored
  `merkle_root`/`ChunkRef` fields travel through `seal()` (enforced in
  `compaction-state-machine`).

The crash-window table (ADR-0025 §Crash-window table) is the acceptance
contract: every row becomes a fault-injection test. This is not
documentation; it is the test matrix.

## Scope

### In Scope

- `SegmentLifecycle::rebuild_from_events(registry_seed, events) -> Result<Registry>`
  — the fold: apply `SegmentEvent`s in `EventWalPos` order through the
  coordinator's transition API (`reserve` / `seal` / `delete`); `delete`
  evicts the entry (O(live) bound); any transition error (e.g. `seal` on a
  missing id) is a corruption error → startup fails loudly with the record
  position.
- Data-WAL pass for `Reserved`-unsealed segments (the only data-WAL
  reconstruction source, ADR-0024 Decision 1):
  - Replay entries whose `segment_id` ∈ reserved-unsealed set (entries carry
    `segment_id`, `wal/entry.rs:52-56`); rebuild the buffer; `.dat` fsync;
    re-seal via `request_seal` (recomputed `merkle_root` + last-entry
    `data_wal_pos`).
  - Empty reserves (reserved, zero data entries) are **dropped** — idle-seal
    of an empty segment never happens (ADR-0024 retention; crash-window
    row 1).
  - A data entry whose `segment_id` has no `ReserveEvent` is a corruption
    signal (the reserve-before-entry invariant is by construction);
    recovery logs the position and sweeps the entry, never replays it.
- Retention/sweep via position, not CF membership: an entry at position `p`
  of segment `S` is garbage iff `S`'s `SealEvent.data_wal_pos ≥ p` (or `S`
  is `Deleted`). `cleanup_old_wal_files` / `file_contains_live_entries`
  re-pointed at the registry + event positions; the CF-derived
  `durable_or_deleted` scan is deleted.
- **Dual-read verification (phase 2):** after the fold, the registry is
  compared against the CF mirror (`get_segment`/deleted markers); any
  divergence fails startup with a structured error (this is the phase-2
  safety net that `segments-cf-removal` deletes).
- **Fault-injection matrix, rows 1–6** (ADR-0025 §Crash-window table) as
  table-driven tests in `crates/oceanfs-storage/tests/` (or the node-level
  suite where restart is required):
  - Row 1: kill between `ReserveEvent` and first `DataEntry` → folded
    `Reserved`, empty → drop the reserve.
  - Row 2: kill after data entries, before `.dat` fsync → `Reserved`-unsealed
    → seek data WAL, replay entries, re-seal.
  - Row 3: kill after `.dat` fsync, before `SealEvent` → `Reserved`-unsealed
    (`.dat` orphan) → adopt: recompute root, append `SealEvent` (no re-seal
    I/O).
  - Row 4: kill after `SealEvent`, before data-WAL sweep → `Sealed` →
    `.dat` authoritative; entries ≤ `data_wal_pos` swept.
  - Row 5: kill after `DeleteEvent`, before `.dat` unlink → `Deleted` →
    `.dat` orphan → reaper sweeps.
  - Row 6 (`DeleteEvent` after unlink): **never allowed** — a test asserts
    the machine cannot emit the unlink-before-delete sequence (API shape +
    attempt test); this row's "folded state" is a compile/API error, not a
    runtime state.
- Corruption handling: torn tail (`TornRecord` from `read_from`) truncates
  the fold at the last good record; mid-log corruption (checksum failure
  followed by good records) is an error → startup aborts (a torn mid-log
  record is disk corruption, not a crash window).

### Out of Scope

- Checkpoint load/snapshot (feature `event-wal-checkpoint`) — in this
  feature the fold starts at the earliest retained event; the checkpoint
  feature provides the seed registry + start position.
- Compaction crash rows 7–9 (feature `compaction-state-machine`).
- Node-level startup wiring and deletion of the adoption heuristic
  (feature `startup-rebuild-from-machine`).
- Removing the CF mirror (feature `segments-cf-removal`).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `segment/lifecycle.rs`: `rebuild_from_events` + data-WAL pass; `wal/replay.rs`: `cleanup_old_wal_files`/`file_contains_live_entries` re-pointed at the machine; new `tests/crash_matrix.rs` (rows 1–6) |
| `oceanfs-node` | Startup calls `rebuild_from_events` (adoption heuristic still present until `startup-rebuild-from-machine`; wiring changes here are additive) |
| `oceanfs-durability` | Verify only (trait boundary unchanged) |

## Interface (Public API)

- `pub struct RebuildOutcome { pub folded_segments: usize, pub dropped_empty_reserves: usize, pub re_sealed_segments: usize, pub adopted_segments: usize, pub swept_entries: u64 }`
  — the fold is observable; every crash-window row asserts a specific
  outcome vector.
- `SegmentLifecycle::rebuild_from_events(&self, events: impl Iterator<Item = Result<(EventWalPos, SegmentEvent)>>) -> Result<RebuildOutcome>`
  — fold only (no data-WAL pass; used with the seed from a checkpoint).
- `SegmentLifecycle::rebuild_with_data_wal(&self, seed: Option<(Registry, EventWalPos)>, events: ..., data_wal: &WalStore) -> Result<RebuildOutcome>`
  — full startup recovery: fold → data-WAL pass → re-seal.
- `pub fn entry_is_garbage(entry: &LifecycleEntry, pos: &DataWalPos) -> bool`
  — the position rule: `entry.state == Sealed && entry.data_wal_pos >= *pos`
  or `entry.state == Deleted`; the sweep boundary, unit-testable.

## Data Flow

```
crash → restart
  → (phase 2: no checkpoint yet) fold events from position 0
  → dual-read verify vs CF mirror (phase 2)          // divergence = fail
  → data-WAL pass:
      for each Reserved-unsealed segment:
        if .dat exists (row 3): recompute root → append SealEvent   // adopt
        else (row 2): replay entries (segment_id filter) → fsync → SealEvent
      empty reserves (row 1): drop
  → machine ready
  → WAL cleanup: entry garbage iff sealed_pos ≥ entry_pos | deleted
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` pass in `oceanfs-storage`,
      `oceanfs-node`; `#![deny(missing_docs)]` passes.
- [ ] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      green plus the new crash-matrix suite (rows 1–6, below).
- [ ] **Invariant — crash-window rows 1–6 are fault-injection tests:**
      each row drives the coordinator to its milestone, kills the process
      (or replays the on-disk state), rebuilds, and asserts (a) the folded
      state exactly matches the table's "Folded state" column, (b) the
      recovery action column is performed, (c) the `RebuildOutcome` vector
      matches. Row 6 is asserted unrepresentable: the unlink-before-delete
      sequence cannot be expressed through the API (compile-level) and an
      attempted test fails.
- [ ] **Invariant — fold is deterministic and order-exact:** the same event
      sequence folded twice yields identical registries; fold of the
      reserve→data→seal→delete sequence reproduces the CF mirror
      (dual-read); a mid-log corruption aborts with the record position
      (never silent partial fold).
- [ ] **Invariant — reserve-before-entry (ADR-0024 Decision 1):** recovery
      of row 1 proves no data entry exists without its reserve; a mutation
      that lets a data entry precede its reserve in the WAL must fail the
      fold (corruption path) or the coordinator-order test.
- [ ] **Invariant — `data_wal_pos` seek & sweep:** after row 4, the sweep
      removes exactly the sealed segment's entries ≤ `data_wal_pos` and
      keeps every other entry; mutation check: an off-by-one `data_wal_pos`
      (too small) leaves live entries protected (bounded-protection test
      fails), too large sweeps a live entry (recovery test fails).
- [ ] **Perf 1.3/7.1:** the fold pre-sizes the registry (live-segment
      estimate from the CF mirror/checkpoint seed); the fold's lock bodies
      contain only map ops; the data-WAL pass streams sequentially
      (perf 3.1) — no per-entry seeks.
- [ ] **Integration:** the reserved-unsealed re-seal path end-to-end —
      crash mid-window, restart, the affected objects are readable and
      their segments `Sealed` with matching `merkle_root` (node-level test
      here or in `startup-rebuild-from-machine`).

> **Lint & Doc Examples (non-gating):** `cargo clippy --all-targets -D
> warnings` test-code warnings and `ignore`-tagged doc examples are
> structural hygiene tracked separately (guidelines/coding.md §9.2.1).
