---
feature: "Startup Rebuild from the Machine — Checkpoint + Fold, Delete the Adoption Heuristic"
epic: "refactoring/segment-event-log-lifecycle/segments-cf-removal"
status: proposed
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
updated: 2026-08-17
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

- [ ] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` pass in `oceanfs-node`,
      `oceanfs-storage`; `#![deny(missing_docs)]` passes.
- [ ] **Tests:** `cargo test -p oceanfs-node --lib -- --test-threads=1`
      green (PIPELINE.md §4.6), including the node-level crash-matrix
      suite.
- [ ] **Invariant — adoption heuristic deleted:** `node.rs` contains no
      "adopted interrupted seal commit" path, no startup
      `.dat`-scan-and-CF-write (grep-verifiable; guidance anchor 995-1067);
      the row-3 behavior is the recovery's deterministic adopt action.
      Mutation check: re-adding the heuristic must fail the startup test.
- [ ] **Invariant — deterministic rebuild:** startup from identical on-disk
      state always produces identical registry state and identical
      `RebuildOutcome` (test: rebuild twice from a copied data dir).
- [ ] **Invariant — full crash matrix at node level:** all nine rows of
      ADR-0025 §Crash-window table pass as kill→restart tests: rows 1–6
      (from `event-wal-recovery`) and 7–9 (from
      `compaction-state-machine`), each asserting folded state + recovery
      action + read-after-restart of all acknowledged data. Row 6
      (unlink-before-DeleteEvent) remains asserted unrepresentable.
- [ ] **Invariant — startup cost bounded:** rebuild time depends on
      `event_wal_checkpoint_bytes` and the retained data-WAL tail, not on
      total event volume (test: 10× lifetime events, checkpointed → startup
      time within 2× of the checkpointed baseline); `RebuildOutcome` and
      `oceanfs_startup_rebuild_ms` are reported.
- [ ] **Read-after-restart:** e2e — write objects (including objects whose
      segments are mid-seal), SIGKILL the node at a random window, restart,
      every acknowledged object reads back intact and digest-verified.
- [ ] **Perf 7.1/1.3:** the fold pre-sizes the registry from the checkpoint
      entry count; no lock is held across the data-WAL pass I/O.
- [ ] **Integration:** the program's end state — node restart after a
      crash mid-compaction and mid-seal, GC + scrub + AE runs green on the
      rebuilt machine, WAL `protected` flat over a soak run (the
      3.8 GB/hour class closed end-to-end).

> **Lint & Doc Examples (non-gating):** `cargo clippy --all-targets -D
> warnings` test-code warnings and `ignore`-tagged doc examples are
> structural hygiene tracked separately (guidelines/coding.md §9.2.1).
