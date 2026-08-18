---
feature: "Segment Event WAL — Format, Checksums, Rotation, Own Fsync Group"
epic: "refactoring/segment-event-log-lifecycle/segment-event-log"
status: done
priority: critical
owner: ""
dependencies:
  - feature: lifecycle-registry-coordinator
    epic: refactoring/segment-event-log-lifecycle/segment-lifecycle-machine
    reason: The coordinator is the only appender; the registry fold consumes every appended event. Phase 2 swaps the CF side-effect for an event append — the machine API must be stable first
adr:
  - 0024-segment-event-log
  - 0025-segment-lifecycle-state-machine
  - 0018-durability-wal-consolidation
  - 0023-metadata-store-native-replacement-path
perf:
  - "3.1 Sequential-only WAL writes"
  - "3.4 Group commit for WAL fsync"
  - "6.3 #[repr(C)] for all on-disk / on-wire structures"
  - "7.1 Minimize lock hold duration"
created: 2026-08-17
updated: 2026-08-18
---

# Segment Event WAL — Format, Checksums, Rotation, Own Fsync Group

## Summary

Build the durable half of the design (ADR-0024 Decisions 1, 2, 4): a
dedicated, project-owned, append-only **event WAL** at
`crates/oceanfs-storage/src/segment/event_wal.rs` — plain files, checksummed
records, its **own** `WalSyncGroup` instance — that becomes the single
source of truth for segment lifecycle transitions. This is ADR-0025
migration phase 2: the coordinator's durable side-effect switches from the
RocksDB `segments` CF write to an event append; the CF is kept as a
**derived mirror** (dual-read verification, consumed by the recovery
feature). Record schema is exactly ADR-0024's:
`ReserveEvent { segment_id, tier, ec_k, ec_m }`,
`SealEvent { segment_id, tier, ec_k, ec_m, merkle_root, data_wal_pos }`,
`DeleteEvent { segment_id }`. Ordering between the two logs is by position
reference (`data_wal_pos`), never by a shared sequence number (ADR-0024
Decision 2). The event log is **plain files with the project's own WAL
discipline** — it is not, and cannot be, a RocksDB column family
(ADR-0023 direction).

## Evidence/Motivation

The four 2026-08-16/17 load-test defects were all ordering failures between
two stores with no shared order (ADR-0024 §Context). The CF is a secondary
store written by convention; the event log makes the transition the unit of
durability:

- **Phantom-downgrade race** — `register_phantom_before_wal`
  (`crates/oceanfs-server/src/write/coordinator.rs:447-...`) exists because
  the CF write and the WAL append have no causal link. With events, the
  Reserve is a durable record appended before the first `DataEntry`; there
  is no second write to race (enforced by the coordinator, feature
  `lifecycle-registry-coordinator`, and verified by the recovery feature's
  fault-injection rows).
- **Missing idle seal** — the same leak mechanism; the event log makes
  "sealed" a durable, position-ordered fact the WAL cleanup can consult
  without a CF scan (`cleanup_old_wal_files` /
  `file_contains_live_entries`, `crates/oceanfs-storage/src/wal/replay.rs:309,426`).
- **Metadata-only compaction** — the compactor emitted a new segment id
  without any durable artifact. Once `SealEvent(new)` is the only way a
  segment becomes real, a metadata-only segment is impossible
  (fully enforced by feature `compaction-state-machine`).

Format and durability decisions are inherited from the proven data-WAL
machinery — `WalWriter`/`WalSyncGroup` (`crates/oceanfs-storage/src/wal/{writer,sync}.rs`,
`WalSyncGroup` at `sync.rs:105-124`), CRC32 per record (the `WalEntry` CRC
discipline, `wal/entry.rs:17-88,244-263`) — one more instance of a proven
component, not a new fsync discipline (ADR-0024 Decision 4). ADR-0018's
"fewer WAL domains" rule is deliberately extended: this domain *replaces* a
RocksDB CF (net external durability domains: −1) and is the single ordering
authority, not a parallel data path — the DoD below pins that rationale
next to every ADR-0018 reference.

## Scope

### In Scope

- `segment/event_wal.rs`:
  - `SegmentEvent` record family + on-disk framing (below).
  - `EventWal` writer: append-only files under `{data_dir}/event-wal/`,
    sequential-only I/O (perf 3.1), rotation at
    `event_wal_file_size_bytes` (default 64 MB; retention is the
    checkpoint's job — feature `event-wal-checkpoint`).
  - Own fsync group: a dedicated `WalSyncGroup` instance with its own
    batch window `event_wal_fsync_batch_timeout_ms` (default 50 ms — wider
    than the data path's 5 ms default, per ADR-0024 Decision 4: events are
    sparse, and a seal already pays a `.dat` fsync). The event group's
    waiter list, batch window, and backpressure are fully independent of
    the data group's.
  - `data_wal_pos` capture: the coordinator records the data-WAL position
    (file sequence + offset) of each appended entry per segment and embeds
    the **last** entry's position in the `SealEvent`.
  - Config: `EventWalConfig` in `oceanfs-core` (`event_wal_dir`,
    `event_wal_file_size_bytes`, `event_wal_fsync_batch_timeout_ms`,
    `event_wal_checkpoint_bytes` — the last consumed by the checkpoint
    feature).
  - Metrics: `oceanfs_event_wal_bytes`, `oceanfs_event_wal_files`,
    `oceanfs_event_wal_append_count` (perf 11.1 discipline).
- Coordinator wiring (phase 2): `request_reserve` / `request_seal` /
  `request_delete` append the event (durable via the event group) and then
  fold into the registry. The CF write becomes a derived-mirror write
  performed **after** the event append (dual-read verification surface).
- Append-return positions: `EventWal::append` returns the record's
  `EventWalPos` so the recovery fold and the checkpoint can track coverage.

### Out of Scope

- Fold-based recovery and the fault-injection matrix (feature
  `event-wal-recovery`).
- Checkpoint/truncate (feature `event-wal-checkpoint`).
- Removing the `segments` CF and moving consumers (feature
  `segments-cf-removal`).
- Object metadata in the event log — **objects stay in RocksDB** (confirmed
  ADR-0024 §Scope); the event log covers segment lifecycle only. No drift
  toward full event sourcing.
- A global sequence number across the two logs — rejected by ADR-0024
  Decision 2; position references only.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | New `segment/event_wal.rs`; `segment/lifecycle.rs` coordinator gains the event-appender arm; `wal/sync.rs` untouched (reused) |
| `oceanfs-core` | `EventWalConfig`, `event_wal_fsync_batch_timeout_ms` etc. in config |
| `oceanfs-server` | Verify only (coordinator API unchanged) |
| `oceanfs-node` | Composition root: open the `EventWal`, pass it to the coordinator |

## Interface (Public API)

- `pub struct DataWalPos { pub file_seq: u32, pub offset: u64 }` — position
  of a data-WAL entry; carried by `SealEvent`.
- `pub struct EventWalPos { pub file_seq: u32, pub offset: u64 }` — position
  of an event record in the event log.
- `pub struct ReserveEvent { pub segment_id: SegmentId, pub tier: SizeTier, pub ec_k: u8, pub ec_m: u8 }`
- `pub struct SealEvent { pub segment_id: SegmentId, pub tier: SizeTier, pub ec_k: u8, pub ec_m: u8, pub merkle_root: HashOutput, pub data_wal_pos: DataWalPos }`
  — the full repacked metadata travels through `seal()` (the BadDigest
  defect is impossible because `merkle_root` and the compression-derived
  fields are seal inputs, not compactor afterthoughts).
- `pub struct DeleteEvent { pub segment_id: SegmentId }`
- `pub enum SegmentEvent { Reserve(ReserveEvent), Seal(SealEvent), Delete(DeleteEvent) }`
- `pub struct EventWal`:
  - `pub fn open(dir: PathBuf, config: &EventWalConfig) -> Result<Self>`
  - `pub async fn append(&self, evt: SegmentEvent) -> Result<EventWalPos>`
    — durable on return (group-commit fsync through the event group).
  - `pub fn latest_pos(&self) -> EventWalPos`
  - `pub fn read_from(&self, pos: EventWalPos) -> EventWalReader`
    (`impl Iterator<Item = Result<(EventWalPos, SegmentEvent)>>` — stops
    with `Err(TornRecord)` at the first bad tail record; consumed by
    recovery).
  - `pub fn bytes_since(&self, pos: EventWalPos) -> u64` — the checkpoint
    trigger input.
  - `pub fn register_metrics(&self, registrar: &dyn MetricRegistrar)`
- `pub struct EventWalConfig` (oceanfs-core): fields above.

**On-disk record framing** (fixed header + payload + CRC32, aligned with
the `WalEntry` discipline; perf 6.3 — explicit byte layout, no
repr-padding surprises):

```
EventRecord:
  magic        [4]   = b"EVL\1"
  version      [1]   = 1
  kind         [1]   0=Reserve, 1=Seal, 2=Delete
  reserved     [2]   = 0
  payload_len  [4]   LE, payload size
  segment_id   [16]
  payload      [payload_len]   tier(1) + ec_k(1) + ec_m(1) + reserved(1)
                               | + merkle_root(32) + data_wal_pos(12) for Seal
  crc32        [4]   over all preceding bytes
```

## Data Flow

```
seal worker (after .dat fsync returns)
  → coordinator.request_seal(SealEvent { ..., merkle_root, data_wal_pos })
    → event_wal.append(Seal)                 // durable via EVENT fsync group
    → registry.fold(Seal)                    // Sealed
    → (phase 2) CF mirror write sealed_at    // derived, verified not authoritative

WAL cleanup (phase 2, dual-read)
  → cleanup_old_wal_files consults registry/event log (not the CF scan)
```

## Deviations

Accepted deviations agreed between implementer and independent reviewer
(one review iteration, PASS; no blocking gaps). The reviewer's only
regression-class note — `latest_pos` transient regression across rotation —
was fixed by the implementer with a packed-atomic publish plus a regression
test; the remaining notes below are recorded as accepted, no fix.

### D1 — Server touch beyond "Verify only"

`WalWriter::append` return type changed `u64` → `DataWalPos`
(`oceanfs-storage`); `SegmentSealer::append_wal_entry` added;
`oceanfs-server`'s `write_wal_entry` switched exactly one line to it
(`coordinator.rs`: `wal_writer().append` → `sealer.append_wal_entry`).
The Crate Impact row "oceanfs-server: Verify only" is interpreted as
"lifecycle coordinator `request_*` API unchanged" — the `request_*`
signatures are untouched.

### D2 — `SealEvent` built inside the coordinator

`request_reserve` / `request_seal` / `request_delete` signatures are
unchanged; the `SealEvent` is built inside the coordinator from the
metadata plus the registry entry's recorded `data_wal_pos`
(`registry.last_data_wal_pos` at seal).

### D3 — Failure semantics

- Event append failure → `TransitionError::DurableWriteFailed`, no fold.
- CF mirror write failure after a successful event append → warn + still
  fold (the event log is authoritative; the CF is a derived mirror).
- Absent `data_wal_pos` at seal → `(0,0)` sentinel (documented).
- Absent `merkle_root` at seal when the event WAL is wired →
  `DurableWriteFailed` (a fabricated root would corrupt recovery).

### D4 — `with_event_wal(Arc<EventWal>)` builder

New builder on the coordinator; existing constructors keep phase-1
behavior (CF-only) so all pre-existing tests compile unchanged; the
production node (`oceanfs-node` composition root) wires the EventWal.

### Perf 6.3 — Hand-laid explicit byte layout

A `#[repr(C)]` struct was not used for the on-disk event record; instead
the record is laid out explicitly by hand, following the `WalEntry`
discipline (documented at `event_wal.rs:24-42`; precedent
`wal/entry.rs`) — the crate's accepted precedent, fully deterministic
layout with no repr-padding surprises.

### Accepted LOW note (no fix) — `bytes_since` transient over-count

`bytes_since` can transiently over-count across rotation by the last
record. Safe direction for a byte-threshold checkpoint trigger (fires
early, never late); under-count is impossible.

### Accepted LOW note (no fix) — `wal/writer.rs` import placement

`wal/writer.rs` imports `DataWalPos` from `segment/event_wal.rs` —
intentional placement per the feature's Interface section; no module
cycle.

### Pre-existing test-only fix — `FAIL_SYNC` seam scoping

`io/segment_flush`'s `FAIL_SYNC` test seam was scoped to the failure
test's filename (the shared static leaked across tests on clean main
under `--test-threads=1`). Test-only; no production behavior change.

## Definition of Done

- [x] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` pass in `oceanfs-storage`,
      `oceanfs-node`; `#![deny(missing_docs)]` passes; `event_wal.rs`
      contains no RocksDB dependency (grep-verifiable — the event log is
      plain files, ADR-0023 direction).
<!-- REVIEW: verified 2026-08-18 — build (--all-targets, incl. oceanfs-server + oceanfs-core), fmt --check, fresh clippy -D warnings (storage/node/core), RUSTDOCFLAGS="-D warnings" cargo doc all pass. event_wal.rs contains "RocksDB" only in doc comments (lines 12, 19) — zero imports/code dependency. -->
- [x] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      green; new unit tests cover every `pub` item (append/read round trip,
      torn tail, checksum, rotation, position monotonicity).
<!-- REVIEW: verified 2026-08-18 — 286 lib tests pass (29 event_wal + phase-2 sequence test). Coverage: round trip, mid-log read, concurrent monotonic positions, latest_pos, durable-on-return reopen, rotation at threshold + reopen-resume + keep-old-files, 3 torn-tail variants (header/payload/checksum), clean-boundary EOF, bytes_since incl. rotation span, 4 own-fsync-group fault-injection tests, metrics registration. -->
- [x] **Invariant — checksummed records:** every record round-trips
      byte-exact; a single flipped byte anywhere in a record fails CRC and
      `read_from` surfaces `TornRecord` (stop-at-first-bad-tail semantics,
      consumed by recovery); records are never silently skipped or
      truncated mid-file.
<!-- REVIEW: verified 2026-08-18 — framing matches the spec exactly (magic b"EVL\1", version 1, kinds 0/1/2, 2 reserved, payload_len LE, 16-byte segment_id, tier/ec_k/ec_m/reserved (+merkle_root 32 + file_seq 4 + offset 8 for Seal), crc32 over header+payload; header 28 B, record sizes 36/80/32 asserted by test). Flipped-byte/header, bad magic, unknown kind, unknown tier, truncation at 3 cut points all rejected. -->
- [x] **Invariant — `data_wal_pos` correctness:** unit test — N appends to
      a segment, then seal; `SealEvent.data_wal_pos` equals the position of
      the N-th (last) data entry. Mutation check: off-by-one corruption of
      `data_wal_pos` must fail the sweep-boundary test (recovery feature).
<!-- REVIEW: verified 2026-08-18 — event_wal_phase2_full_sequence_folds_and_mirrors (lifecycle.rs tests) appends 3 WalEntries via sealer.append_wal_entry and asserts evt.data_wal_pos == last_pos returned by the writer. Mutation check belongs to event-wal-recovery (out of scope here). -->
- [x] **Invariant — own fsync group (ADR-0024 Decision 4):** the event
      group is a separate `WalSyncGroup` instance; tests with injected
      blocking fsync functions prove (a) a stalled data group does not
      delay event fsyncs, (b) a stalled event group does not delay data
      fsyncs, (c) the event batch window is governed by
      `event_wal_fsync_batch_timeout_ms` only. The data group's waiter list
      never contains an event append.
<!-- REVIEW: verified 2026-08-18 — EventWal::create_sync_group instantiates its own WalSyncGroup (event_wal.rs:690-734); stalled_data_group_does_not_delay_event_appends, stalled_event_group_does_not_delay_data_group, event_batch_window_governs_fsync_cadence (asserts 1 fsync round for 8 concurrent appends @ 200 ms window), independent_sync_groups_have_independent_waiter_lists all pass. -->
- [x] **Invariant — append-only (perf 3.1):** no seek or rewrite on the
      event WAL files; rotation opens a new file at `event_wal_file_size_bytes`;
      `read_from` is the only read path.
<!-- REVIEW: verified 2026-08-18 — append path is write_all at position under the write mutex (event_wal.rs:530-566); rotate() opens a new file and never deletes (retention = checkpoint feature); read_from is the only reader. -->
- [x] **ADR-0018 compliance note:** every reference to ADR-0018 states the
      replacement rationale — the event log replaces a RocksDB CF (net
      external durability domains −1) and is the single ordering authority,
      not a parallel data path (ADR-0024 §Consequences).
<!-- REVIEW: verified 2026-08-18 — the only ADR-0018 reference in the new code is the module-doc compliance note at event_wal.rs:17-22, which states the full rationale. -->
- [x] **ADR-0024 Decision 2:** there is no sequence counter shared with the
      data WAL anywhere in the code (grep-verifiable); all cross-log
      ordering is via `DataWalPos`.
<!-- REVIEW: verified 2026-08-18 — grep for global_seq/shared_seq/sequence_counter/global_sequence: no matches; cross-log ordering is only DataWalPos carried in SealEvent (recorded per append via sealer.append_wal_entry → coordinator.record_data_wal_pos → registry.last_data_wal_pos at seal). -->
- [x] **Integration:** a reserve→data→seal→delete sequence through the
      coordinator produces exactly three events whose replay fold
      reproduces the registry exactly (dual-read: CF mirror matches); the
      seal's `data_wal_pos` matches the WAL writer's returned position.
<!-- REVIEW: verified 2026-08-18 — event_wal_phase2_full_sequence_folds_and_mirrors passes: 3 events in order, folded registry equals live registry (state + len), CF mirror reflects the delete, data_wal_pos == writer position. Node-level wiring verified: node.rs opens EventWal, with_event_wal, register_metrics; node_lifecycle integration test passes. -->
- [x] **D1–D4 declared deviations:** D1 — WalWriter::append returns
      DataWalPos; server switch is exactly one line (coordinator.rs
      write_wal_entry: wal_writer().append → sealer.append_wal_entry); the
      oceanfs-storage-api WalWriter trait keeps u64 offset (maps
      pos.offset). D2 — request_* signatures unchanged; SealEvent built in
      coordinator from metadata + registry.last_data_wal_pos. D3 — event
      append failure → DurableWriteFailed (no fold); CF mirror failure →
      warn + still fold; absent data_wal_pos → (0,0) sentinel; absent
      merkle_root with event wal wired → DurableWriteFailed. D4 —
      with_event_wal builder; phase-1 constructors keep CF-only behavior
      (all pre-existing lifecycle tests pass). Perf 6.3 — hand-laid byte
      layout documented at event_wal.rs:24-42, consistent with the
      WalEntry precedent (wal/entry.rs). All accepted. Full record in the
      Deviations section above.

> **Lint & Doc Examples (non-gating):** `cargo clippy --all-targets -D
> warnings` test-code warnings and `ignore`-tagged doc examples are
> structural hygiene tracked separately (guidelines/coding.md §9.2.1).
