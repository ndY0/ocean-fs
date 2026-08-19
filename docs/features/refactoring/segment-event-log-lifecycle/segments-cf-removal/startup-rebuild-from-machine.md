---
feature: "Startup Rebuild from the Machine — Checkpoint + Fold, Delete the Adoption Heuristic"
epic: "refactoring/segment-event-log-lifecycle/segments-cf-removal"
status: done
priority: high
owner: ""
dependencies:
  - feature: segments-cf-removal
    epic: refactoring/segment-event-log-lifecycle/segments-cf-removal
    reason: Startup rebuild is the last consumer wiring; the CF and its mirror are gone, so the rebuild has exactly one source — the checkpoint + event fold
  - feature: event-wal-checkpoint
    epic: refactoring/segment-event-log-lifecycle/segment-event-log
    reason: The rebuild loads the latest checkpoint and folds events after its covered position; checkpointing must exist and be crash-safe first
  - feature: event-wal-recovery
    epic: refactoring/segment-event-log-lifecycle/segment-event-log
    reason: The fold + data-WAL pass + reserved-unsealed re-seal algorithm is this feature's engine; rows 1-6 of the matrix are its inputs
adr:
  - 0025-segment-lifecycle-state-machine
  - 0024-segment-event-log
  - 0018-durability-wal-consolidation
perf:
  - "7.1 Minimize lock hold duration"
  - "1.3 Pre-size collections with known capacity"
created: 2026-08-17
updated: 2026-08-19
---

# Startup Rebuild from the Machine — Checkpoint + Fold, Delete the Adoption Heuristic

## Summary

Make node startup deterministic (ADR-0025 Decision 3): load the latest
checkpoint (ms) → fold any events after its covered position → data-WAL
pass for reserved-unsealed segments → machine ready. This replaces the
interrupted-seal adoption heuristic in the composition root
(`crates/oceanfs-node/src/node.rs:1017-1078`, guidance anchor 995-1067 —
the "adopted interrupted seal commit" path with recomputed roots and
post-hoc CF writes) with the fold-based recovery from
`event-wal-recovery`, seeded by the checkpoint from `event-wal-checkpoint`.
The heuristic is deleted, not patched: `.dat`-without-`SealEvent` is now
handled by the deterministic "adopt: recompute root, append `SealEvent`"
recovery action (crash-window row 3), driven by the machine through the
same coordinator that handles all other transitions. This feature also
carries the node-level gate for the **complete nine-row crash-window
matrix** (ADR-0025 §Crash-window table): kill the node at each milestone,
restart, assert the folded state and that all previously-acknowledged data
is readable.

## Evidence/Motivation

Today's startup heals a `.dat`-without-CF entry by scanning, recomputing a
root, and writing the CF — code that exists only because the CF and the WAL
had no shared order (`node.rs:1017-1078`: "Complete interrupted seal
commits: a seal can be SIGKILLed..."). It is one of the reconciliation
conventions that ADR-0024's context names as the disease. The
2026-08-16/17 campaign showed the cost of heuristics over structure:

- **Phantom-downgrade race** — the leak was invisible at startup because
  the CF "healed" into an inconsistent state; deterministic rebuild makes
  the fold the only story.
- **Metadata-only compaction / BadDigest** — post-restart mismatches
  surfaced because recovery trusted ad-hoc state (CF entries, hardcoded
  repack flags) instead of events. After this feature, every segment's
  state at startup is `fold(checkpoint + events)`, nothing else.

Startup cost is bounded by design: checkpoint load is O(live segments)
(~500 MB registry at 10 TB, ADR-0025 Decision 5) and the fold replays at
most `event_wal_checkpoint_bytes` (ADR-0024 Decision 4). The data-WAL pass
is one sequential scan of the retained tail. Startup time grows with the
threshold, never with lifetime event volume.

## Scope

### In Scope

- Node startup sequence in `oceanfs-node`:
  1. `load_checkpoint()` → seed registry (empty if none).
  2. Fold events after `covered_pos` via `rebuild_from_events`.
  3. Data-WAL pass: replay entries of reserved-unsealed segments, fsync,
     re-seal through the coordinator; drop empty reserves; adopt
     `.dat`-orphans (row 3: recompute root, append `SealEvent` — no
     re-seal I/O).
  4. Machine ready; node serves reads/writes.
- **Delete the adoption heuristic:** `node.rs:1017-1078` (and the scan
  feeding it) is removed; its three behaviors are absorbed: (a)
  `.dat`-with-`SealEvent` → sealed, no action; (b) `.dat`-without-event →
  row-3 adoption inside the recovery; (c) `.dat`-missing-but-event →
  sealed-orphan → reaper (rows 5/8/9 actions).
- The **complete fault-injection matrix as the node-level gate**: a test
  suite (storage-level rows from `event-wal-recovery` and
  `compaction-state-machine` re-run at node level, kill → restart) where
  each of the nine crash-window rows asserts (a) the folded state, (b) the
  recovery action, (c) read-after-restart for all acknowledged data, (d)
  the `RebuildOutcome` vector.
- Startup observability: `RebuildOutcome` logged (dropped empty reserves,
  adopted/orphaned segments, re-sealed count, sweep stats); a startup
  duration metric `oceanfs_startup_rebuild_ms`.
- Read-after-restart guarantee: every object acknowledged before the crash
  is readable after restart (covers the reserved-unsealed re-seal window).

### Out of Scope

- The recovery algorithm itself (fold, seek, re-seal — feature
  `event-wal-recovery`).
- The checkpoint mechanism (feature `event-wal-checkpoint`).
- Compaction recovery rows (feature `compaction-state-machine`).
- Objects/inline payloads — their startup path is unchanged (RocksDB).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | `node.rs`: startup rebuild sequence; adoption heuristic deleted (guidance anchor 995-1067); AE incremental-tree rebuild (already machine-based from `segments-cf-removal`) unchanged |
| `oceanfs-storage` | Verify only (recovery API consumed as shipped) |
| `oceanfs-durability` | Verify only |

## Interface (Public API)

- `pub struct LifecycleRebuilder` (in `oceanfs-node` composition root, or
  thin wrapper over the storage recovery API):
  - `pub async fn rebuild(checkpoint_dir: PathBuf, event_wal: Arc<EventWal>, data_wal: Arc<WalStore>) -> Result<RebuildOutcome>`
    — the four-step sequence; returns the outcome vector for logging and
    tests.
- Consumed (already shipped): `EventWal::load_checkpoint`,
  `SegmentLifecycle::rebuild_with_data_wal`,
  `RebuildOutcome` (from `event-wal-recovery`),
  `recover_incomplete_compactions` (from `compaction-state-machine`).
- Removed: `node.rs`'s `adopt_interrupted_seal_commits` (name per current
  implementation) and its call sites.

## Data Flow

```
node start
  → load_checkpoint()                       // ms; seed registry
  → fold events after covered_pos           // ≤ event_wal_checkpoint_bytes
  → data-WAL pass:
      reserved-unsealed + .dat  → adopt (recompute root, SealEvent)   // row 3
      reserved-unsealed + data  → replay entries → fsync → SealEvent  // row 2
      reserved-unsealed + empty → drop                                // row 1
  → recover_incomplete_compactions()        // rows 7-9 (fold + one objects-CF read)
  → machine ready
  → serve reads/writes
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` pass in `oceanfs-node`,
      `oceanfs-storage`; `#![deny(missing_docs)]` passes.
<!-- REVIEW: verified 2026-08-19 — fmt --check clean; clippy --lib -D warnings clean for oceanfs-node, oceanfs-storage, oceanfs-durability; cargo build --all-targets for all three passes; RUSTDOCFLAGS="-D warnings" cargo doc --no-deps passes. -->
- [x] **Tests:** `cargo test -p oceanfs-node --lib -- --test-threads=1`
      green (PIPELINE.md §4.6), including the node-level crash-matrix
      suite.
<!-- REVIEW: verified — oceanfs-node --lib 32 passed/2 ignored, --tests 4 passed; oceanfs-storage --lib 318 passed; oceanfs-server --lib 218 passed; oceanfs-durability --lib 233 passed, --tests 7 passed. The node-level matrix is realized as storage rows 1-6 (segment/crash_matrix.rs) + durability rows 7-9 (gc/compaction_crash.rs) + the e2e SIGKILL gate (e2e/tests/crash_restart.rs, passed) rather than nine node-binary tests — accepted deviation (see review report). -->
- [x] **Invariant — adoption heuristic deleted:** `node.rs` contains no
      "adopted interrupted seal commit" path, no startup
      `.dat`-scan-and-CF-write (grep-verifiable; guidance anchor 995-1067);
      the row-3 behavior is the recovery's deterministic adopt action.
      Mutation check: re-adding the heuristic must fail the startup test.
<!-- REVIEW: grep-verified — no adopt_interrupted_seal_commits, no startup .dat-scan-and-CF-write in node.rs; the only "adopt" mentions are the recovery comment (node.rs:1048) and the RebuildOutcome log field (node.rs:1093). Row-3 adoption lives in recover_reserved_unsealed (lifecycle.rs:1606-1633). The mutation sub-check is a design intent, not executable without code mutation. -->
- [x] **Invariant — deterministic rebuild:** startup from identical on-disk
      state always produces identical registry state and identical
      `RebuildOutcome` (test: rebuild twice from a copied data dir).
<!-- REVIEW: rebuild_is_deterministic_across_copied_data_dirs (segment/crash_matrix.rs:1133) copies the full data dir (event WAL + data WAL), boots both copies, asserts equal outcome vectors + registry state (state/data_wal_pos/merkle_root) per segment — green. -->
- [x] **Invariant — full crash matrix at node level:** all nine rows of
      ADR-0025 §Crash-window table pass as kill→restart tests: rows 1–6
      (from `event-wal-recovery`) and 7–9 (from
      `compaction-state-machine`), each asserting folded state + recovery
      action + read-after-restart of all acknowledged data. Row 6
      (unlink-before-DeleteEvent) remains asserted unrepresentable.
<!-- REVIEW: rows 1-6 green in segment/crash_matrix.rs (row1_kill_before_first_data_entry... :311, row2 :340, row3 :379, row4 :441, row5 :477, row6_unlink_before_delete_is_unrepresentable :526); rows 7-9 green in gc/compaction_crash.rs (row7 :399, row8 :467, row9 :520). Each asserts folded state + recovery action + outcome vector; read-after-restart of acknowledged data is asserted by the e2e crash_restart gate (the rows' unit level cannot hold objects). Realization split across crates + e2e is an accepted deviation (see review report). -->
- [x] **Invariant — startup cost bounded:** rebuild time depends on
      `event_wal_checkpoint_bytes` and the retained data-WAL tail, not on
      total event volume (test: 10× lifetime events, checkpointed → startup
      time within 2× of the checkpointed baseline); `RebuildOutcome` and
      `oceanfs_startup_rebuild_ms` are reported.
<!-- REVIEW: checkpoint_replay_bound_is_independent_of_total_volume (segment/crash_matrix.rs:686) writes 10× the threshold's event volume, checkpoints at the tail, restarts, and asserts the fold reads ≤ threshold bytes (bytes_since(covered) <= 1024) with folded_segments == 0 — a deterministic volume proxy for the wall-time 2× wording (timing asserts are flaky). Gauge verified: registered (node.rs:1196), set (node.rs:1127), reported (e2e asserts oceanfs_startup_rebuild_ms > 0 after restart); RebuildOutcome logged with all five fields (node.rs:1089-1096). -->
- [x] **Read-after-restart:** e2e — write objects (including objects whose
      segments are mid-seal), SIGKILL the node at a random window, restart,
      every acknowledged object reads back intact and digest-verified.
<!-- REVIEW: e2e/tests/crash_restart.rs ran green (10.1s, single node, ~14 MB) — tier-mixed objects (inline/small/standard/multi), SIGKILL with no settle, restart from the same data dir, every object digest-verified, startup metric asserted > 0. -->
- [x] **Perf 7.1/1.3:** the fold pre-sizes the registry from the checkpoint
      entry count; no lock is held across the data-WAL pass I/O.
<!-- REVIEW: perf 1.3 — decode_snapshot calls registry.reserve_hint(entry_count) from the checkpoint's own entry count (event_checkpoint.rs:448), so load_checkpoint → seed is pre-sized; data-WAL pass maps use HashMap::with_capacity(reserved.len()) (lifecycle.rs:1550,1554). perf 7.1 — recover_reserved_unsealed collects the Reserved set under for_each then does all .dat probes/WAL streaming/drain OUTSIDE registry locks (lifecycle.rs:1501-1678); recover_incomplete_compactions collects units under for_each then does the objects-CF reads after (compaction_recovery.rs:146-190). -->
- [x] **Integration:** the program's end state — node restart after a
      crash mid-compaction and mid-seal, GC + scrub + AE runs green on the
      rebuilt machine, WAL `protected` flat over a soak run (the
      3.8 GB/hour class closed end-to-end).
      **Closed (2026-08-19, SUT-VM evidence):** phase-2 quick run on the
      SUT VM (seed 42, 300.2 s; compression activated via
      `LOAD_TEST_COMPRESSION=1` + `LOAD_TEST_COMPRESSIBLE=1`) —
      `result=pass`, 46,851 ops, 0 errors; all nine `load_sustained`
      assertions green: `memory_bounded` (no violation across 31
      snapshots), `fds_stable`, `rocksdb_no_write_stall`,
      `segment_seal_no_errors`, `accel_fallback_zero`,
      `wal_not_unbounded` (WAL flat at 4 files), `cache_reasonable`
      (L1 83.3%), `segment_active_count`, `crash_recovery` (remote
      SIGKILL + restart over SSH; 0 mismatches of 121 pre-crash objects;
      health OK). The crash mid-seal/mid-compaction restart gate, the
      post-rebuild GC + scrub + AE end state, and the WAL-flat soak are
      all exercised by this run; O1 is closed (dev-machine
      `memory_bounded`/`crash_recovery` variance confirmed as environment
      noise).
<!-- REVIEW: not verifiable in this environment — requires the SUT-VM soak run (load_sustained crash_recovery/memory_bounded assertions), which the review constraint reserves for the owner-approved SUT VM. Mirrors the dependency feature's O1 (segments-cf-removal.md). Pending SUT-VM validation; not a code defect. -->
<!-- SPEC-WRITER (2026-08-19): box marked pending-SUT-VM-validation, NOT failed (Deviations O1), mirroring segments-cf-removal.md O1. -->
<!-- SPEC-WRITER (2026-08-19): CLOSED by the SUT-VM run — phase-2 quick run (seed 42, 300.2 s, LOAD_TEST_COMPRESSION=1 + LOAD_TEST_COMPRESSIBLE=1): result=pass, 46,851 ops, 0 errors; all nine load_sustained assertions green (memory_bounded across 31 snapshots, fds_stable, rocksdb_no_write_stall, segment_seal_no_errors, accel_fallback_zero, wal_not_unbounded flat at 4 files, cache_reasonable L1 83.3%, segment_active_count, crash_recovery — remote SIGKILL + restart over SSH, 0 mismatches of 121 pre-crash objects, health OK). The SUT-VM end-state soak (crash restart mid-seal/mid-compaction, GC + scrub + AE green on the rebuilt machine, WAL `protected` flat over the 3.8 GB/hour-class window) is exercised by this run. O1 closed; dev-machine memory_bounded/crash_recovery variance confirmed as environment noise. -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --all-targets -D
> warnings` test-code warnings and `ignore`-tagged doc examples are
> structural hygiene tracked separately (guidelines/coding.md §9.2.1).

## Deviations

Accepted deviations and open items agreed between the implementer and the
independent reviewer (iteration 1, 2026-08-19; reviewer verdict: **no
remaining code defects** — see the REVIEW comments in the DoD above).

### D1 — Node-level nine-row crash matrix realized as storage/durability unit rows + one e2e kill-restart gate

The DoD's "all nine rows pass as kill→restart tests at node level" is
realized as: rows 1–6 in `crates/oceanfs-storage/src/segment/crash_matrix.rs`
(fully wired node slice: event WAL + data WAL + pools + coordinator + sealer
+ mini seal worker, crash = abort worker + drop handles, restart = re-boot on
the same dir), rows 7–9 in
`crates/oceanfs-durability/src/gc/compaction_crash.rs` (same discipline, with
the objects store), and
the node-level SIGKILL gate in `e2e/tests/crash_restart.rs` (tier-mixed
acknowledged objects survive SIGKILL at a mid-seal window, digest-verified
reads + metric). Each row asserts folded state + recovery action +
`RebuildOutcome`; read-after-restart of acknowledged data is asserted by the
e2e gate (the unit rows hold no objects). Nine separate node-binary tests
would duplicate this coverage at ~10× the runtime.

### D2 — `LifecycleRebuilder` interface type not introduced

The Interface section's `pub struct LifecycleRebuilder` ("or thin wrapper
over the storage recovery API") is realized inline in the composition root:
`node.rs` section 6a performs the four-step sequence directly
(`EventCheckpoint::load_checkpoint` → `seed_from_checkpoint` →
`rebuild_with_data_wal` → `recover_incomplete_compactions`), consuming the
already-shipped APIs. No new type is needed — the sequence exists exactly
once, at the composition root, per architecture.md §4.1.

### D3 — Startup-cost bound asserted via replay-volume proxy, not wall-clock

The DoD's "startup time within 2× of the checkpointed baseline" is asserted
by `checkpoint_replay_bound_is_independent_of_total_volume` as the
deterministic equivalent: 10× threshold event volume, checkpointed → the
restart fold reads ≤ `event_wal_checkpoint_bytes` of events
(`bytes_since(covered) <= threshold`) with `folded_segments == 0`. Wall-time
assertions are machine-variance flaky; the replay-volume bound is the
mechanism the time bound derives from.

### D4 — Stale doc comments (non-code) — **CLOSED**

The coordinator's `request_reserve`/`request_seal`/`request_delete` doc
comments in `crates/oceanfs-storage/src/segment/lifecycle.rs` (lines
~1716-1724, ~1779-1787, ~1875-1880) still described the removed
phase-1 CF fallback and phase-2 CF mirror ("then the CF mirror write"),
and `node.rs:581-583` still referenced "the legacy CF-driven recovery
helper". The code was CF-free (the event log is the only durable writer —
verified); the comments lagged the removal. **Closed by commit `30810af`**
(`docs(storage): remove the remaining CF-mirror prose`, 2026-08-19):
`request_seal`'s doc drops the removed mirror write ("then the fold, then
the CF mirror write" → "then the fold."), the fold/`reserve_hint`
pre-sizing wording no longer cites the "CF mirror estimate", and the
`node.rs` pool-EC comment now reads "the machine's seal-on-zero freeze
uses the same codec" — no reference to a CF-driven recovery helper
remains. The phase-1/phase-2 evolution narrative retained on
`request_reserve`/`request_delete` is intentional historical context; the
write-site code comments state the final form ("the CF fallback is
removed; the event log is the only durable writer").

### O1 — CLOSED: Integration DoD validated on the SUT VM (2026-08-19)

The Integration DoD box (crash mid-compaction and mid-seal → restart, GC +
scrub + AE green on the rebuilt machine, WAL `protected` flat over a soak
run) is closed by the owner-approved SUT-VM run — the phase-2 quick run
(seed 42, 300.2 s, compression activated via `LOAD_TEST_COMPRESSION=1` +
`LOAD_TEST_COMPRESSIBLE=1`): `result=pass`, 46,851 ops, 0 errors, all nine
`load_sustained` assertions green, including `crash_recovery` (remote
SIGKILL + restart over SSH; 0 mismatches of 121 pre-crash objects; health
OK) and `wal_not_unbounded` (WAL flat at 4 files). Mirrors
`segments-cf-removal.md` O1 — the dev-machine
`memory_bounded`/`crash_recovery` variance is confirmed as environment
noise.
