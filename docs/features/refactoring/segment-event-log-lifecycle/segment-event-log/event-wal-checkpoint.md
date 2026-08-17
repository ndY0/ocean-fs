---
feature: "Event WAL Checkpoint — Byte-Threshold Snapshot + Truncate"
epic: "refactoring/segment-event-log-lifecycle/segment-event-log"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: event-wal-recovery
    epic: refactoring/segment-event-log-lifecycle/segment-event-log
    reason: The checkpoint is a folded-registry snapshot; the fold (rebuild_from_events) is the code that consumes the events after the snapshot — checkpointing must be speced against a proven fold
  - feature: event-wal-format
    epic: refactoring/segment-event-log-lifecycle/segment-event-log
    reason: bytes_since/EventWalPos/read_from are the trigger and coverage inputs; truncation operates on the format's files
adr:
  - 0024-segment-event-log
  - 0025-segment-lifecycle-state-machine
  - 0023-metadata-store-native-replacement-path
perf:
  - "3.1 Sequential-only WAL writes"
  - "7.1 Minimize lock hold duration"
  - "1.3 Pre-size collections with known capacity"
created: 2026-08-17
updated: 2026-08-17
---

# Event WAL Checkpoint — Byte-Threshold Snapshot + Truncate

## Summary

Give the event log its own GC (ADR-0024 Decision 3): an atomic snapshot of
the folded registry in **our own format** — temp file + fsync + rename +
directory fsync — triggered **only** by a byte threshold on the event log
(`event_wal_checkpoint_bytes`, default 64 MB), after which events older
than the snapshot are truncated. The checkpoint is the on-disk state
snapshot that eventually replaces the `segments` CF (ADR-0025 Decision 3)
and it bounds startup replay: fold cost is capped by the threshold, not by
lifetime event volume. Startup becomes: load latest checkpoint (ms) →
append-fold events after it → machine ready. The threshold is the *only*
trigger — there is no time-based fallback (ADR-0024 Decision 4's "Why a
byte threshold, not rotation" rationale applies to the trigger as well).

## Evidence/Motivation

The event log grows with every seal — ~1.4M events/day at sustained load
(ADR-0024 Decision 3). Unbounded growth means unbounded startup replay and
unbounded disk; a rotation cadence checkpoints on wall clock even when the
log is tiny (wasted I/O) and defers checkpointing past the bounded-replay
guarantee when events spike. The byte threshold makes checkpoint cost a
direct function of replay cost: replay after checkpoint is always ≤
`event_wal_checkpoint_bytes` regardless of workload shape. At TB scale this
matters (the design is deliberately not derived from the 75 GB load-test
box): a quiet cluster generates almost no events; a delete-heavy or
compaction-heavy one generates thousands.

The 2026-08-16/17 leak class (WAL files pinned forever, ~3.8 GB/hour) is
also the reason checkpointing is a *first-class* mechanism, designed in from
day one (ADR-0024 Decision 3) rather than a later retrofit: the event log's
retention rule is "retained until checkpointed", and the checkpoint's
`DeleteEvent`-eviction (deleted segments' history is entirely garbage) is
what keeps the snapshot O(live segments) — the same bound as the registry
(ADR-0025 Decision 5: ~500 MB at 10 TB).

## Scope

### In Scope

- `segment/event_checkpoint.rs` (or in `segment/event_wal.rs`):
  - `write_checkpoint(registry, up_to: EventWalPos) -> Result<CheckpointInfo>`:
    serialize live entries (`Reserved`/`Sealed` with full metadata; `Deleted`
    entries are already evicted — a `DeleteEvent` makes the segment's
    entire history garbage), plus `merkle_root`/`data_wal_pos` for `Sealed`
    entries (retention needs `data_wal_pos` to survive checkpointing).
    Atomic: write `checkpoint-{pos}.tmp` → fsync file → rename to
    `checkpoint-{pos}` → fsync directory.
  - `load_checkpoint(dir) -> Result<Option<(Registry, EventWalPos)>>` —
    newest valid checkpoint; returns the covered position so the fold
    starts after it. Checksummed + versioned (own format, no RocksDB).
  - `truncate_before(pos: EventWalPos)`: delete event files fully covered
    by `pos`, truncate the straddling file at `pos`'s offset. **Never**
    truncates events at/after `pos`.
  - Trigger: after each `append`, if `bytes_since(last_checkpoint_pos) ≥
    event_wal_checkpoint_bytes` → checkpoint (asynchronously off the append
    path; new appends during the checkpoint land after `up_to` and are
    folded on top at startup — exactly-once by position coverage).
  - Crash safety of checkpointing itself (ADR-0024 §Negative lists this
    surface): crash during temp write → old checkpoint + full fold (orphan
    `.tmp` cleaned at startup); crash after rename before truncate → new
    checkpoint + fold of events after it (events ≤ `up_to` are covered by
    the snapshot and ignored — idempotent); truncate never exceeds `up_to`.
  - Metrics: `oceanfs_event_wal_checkpoint_bytes`, `oceanfs_event_wal_truncated_bytes`.
- Startup integration (additive here; node-level gating in
  `startup-rebuild-from-machine`): load latest checkpoint → fold events
  after it → data-WAL pass (from `event-wal-recovery`).

### Out of Scope

- The fold itself and the data-WAL pass (feature `event-wal-recovery`).
- Dropping the `segments` CF and pointing consumers at the checkpoint
  (feature `segments-cf-removal`).
- Checkpointing object metadata — objects stay in RocksDB; the checkpoint
  is segment lifecycle state only.
- Time-based or rotation-based checkpoint triggers — explicitly rejected
  (ADR-0024 Decision 4); the byte threshold is the only trigger.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `segment/event_checkpoint.rs`: snapshot format (versioned, checksummed), `write_checkpoint`/`load_checkpoint`/`truncate_before`; `segment/event_wal.rs`: trigger wiring + `bytes_since` consumption |
| `oceanfs-core` | `event_wal_checkpoint_bytes` config (default 64 MB) |
| `oceanfs-node` | Composition root: checkpoint dir + startup load (additive until `startup-rebuild-from-machine`) |

## Interface (Public API)

- `pub struct CheckpointInfo { pub covered_pos: EventWalPos, pub entries: usize, pub bytes: u64 }`
- `pub fn write_checkpoint(&self, registry: &SegmentLifecycleRegistry, up_to: EventWalPos) -> Result<CheckpointInfo>`
- `pub fn load_checkpoint(&self) -> Result<Option<(Registry, EventWalPos)>>`
  — newest valid checkpoint + its covered position.
- `pub fn truncate_before(&self, pos: EventWalPos) -> Result<()>`
- `pub fn last_checkpoint_pos(&self) -> Option<EventWalPos>`
- `pub fn needs_checkpoint(&self, config: &EventWalConfig) -> bool`
  — `bytes_since(last_checkpoint_pos) >= event_wal_checkpoint_bytes`.

**Snapshot format** (own format, versioned, checksummed; perf 6.3
discipline):

```
checkpoint-{file_seq}-{offset}:
  magic        [4]   = b"CHK\1"
  version      [1]   = 1
  covered_pos  [12]  EventWalPos covered by this snapshot
  entry_count  [4]   LE
  entries      [entry_count]  segment_id(16) + state(1) + metadata(serialized SegmentMetadata)
                              + data_wal_pos(12, Sealed only)
  crc32        [4]   over all preceding bytes
```

## Data Flow

```
append(Seal) → durable (event group)
  → if needs_checkpoint(bytes_since(last) >= threshold):
      (async) write_checkpoint(registry, latest_pos)   // tmp → fsync → rename → dir fsync
      → truncate_before(covered_pos)                   // delete covered files, trim straddling file
crash → startup
  → load_checkpoint → fold events after covered_pos     // replay ≤ threshold bytes
  → machine ready
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` pass in `oceanfs-storage`,
      `oceanfs-node`; `#![deny(missing_docs)]` passes; `event_checkpoint.rs`
      contains no RocksDB dependency (the snapshot is plain files in our
      own format — the CF replacement, ADR-0025 Decision 3).
- [ ] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      green; unit tests for snapshot round trip, checksum/version
      rejection, truncation boundaries, trigger arithmetic.
- [ ] **Invariant — threshold-only trigger:** checkpoints occur **only**
      when `bytes_since(last_checkpoint_pos) ≥ event_wal_checkpoint_bytes`.
      Test: a long-idle event log (no appends for hours in test time)
      produces zero checkpoint files; a burst past the threshold produces
      exactly one. Mutation check: adding a time-based trigger must fail a
      test.
- [ ] **Invariant — replay bound:** after a checkpoint, startup fold reads
      at most `event_wal_checkpoint_bytes` (test: 10× event volume, one
      checkpoint → fold cost independent of total volume; assert the bytes
      read at startup).
- [ ] **Invariant — checkpoint atomicity (snapshot-vs-WAL ordering):**
      fault-injection: kill during temp write → startup recovers from the
      old checkpoint + full fold, orphan `.tmp` removed; kill after rename
      before truncate → startup loads the new snapshot and folds only
      events after `covered_pos` (idempotent — re-folding covered events
      is impossible by construction: the fold starts at `covered_pos`).
- [ ] **Invariant — truncation never cuts live events:** `truncate_before`
      never removes or trims events at/after `up_to`; the straddling file
      is trimmed exactly at the covered offset. Mutation check: truncating
      past `covered_pos` must fail the post-restart fold test.
- [ ] **Invariant — `DeleteEvent` history is garbage:** deleted segments do
      not appear in the snapshot (registry eviction), and no post-checkpoint
      fold can resurrect them; the snapshot stays O(live segments)
      (~500 MB bound at 10 TB, ADR-0025 Decision 5 — asserted by a
      checkpoint-size test at 100K live entries).
- [ ] **Perf 7.1/1.3:** the snapshot serializes a registry snapshot taken
      outside the shard locks (entries copied under short read guards, then
      serialized lock-free); collections pre-sized by `entry_count`.
- [ ] **Integration:** full cycle — drive the event log past the
      threshold, restart, assert (a) machine state equals pre-crash state
      (dual-read vs CF mirror in phase 2), (b) WAL retention still correct
      after checkpoint truncation (sealed segments' entries swept by
      `data_wal_pos` from the snapshot, not from deleted events).

> **Lint & Doc Examples (non-gating):** `cargo clippy --all-targets -D
> warnings` test-code warnings and `ignore`-tagged doc examples are
> structural hygiene tracked separately (guidelines/coding.md §9.2.1).
