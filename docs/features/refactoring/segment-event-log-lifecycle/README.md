---
feature: "Segment Event Log & Lifecycle Machine — Program Coordination"
epic: "refactoring/segment-event-log-lifecycle"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: refactoring/segment-pool-slot-state-machine
    reason: Single-lock SlotState precedent must be complete; its semantics (slot-scan read, backpressure, append-with-hook ordering) become the regression suite for the lifecycle machine
adr:
  - 0024-segment-event-log
  - 0025-segment-lifecycle-state-machine
  - 0023-metadata-store-native-replacement-path
  - 0018-durability-wal-consolidation
  - 0020-read-from-active-segments
  - 0021-seal-window-data-set
  - 0009-storage-crate-split
perf:
  - "7.1 Minimize lock hold duration"
  - "2.3 parking_lot::RwLock everywhere"
created: 2026-08-17
updated: 2026-08-17
---

# Segment Event Log & Lifecycle Machine — Program Coordination

> **This is the coordination document for the 3-epic program.** If you are
> implementing any feature under
> `refactoring/segment-event-log-lifecycle/`, read this first — it tells
> you where your work sits in the whole, what must exist before you
> start, and what must not regress while you work. The per-feature docs
> are the authority for your feature; this document is the map.

---

## Summary

A production-grade redesign of segment lifecycle handling. Today segment
state is scattered across six structures (pool slot, sealing-data map,
seal queue, flush coordinator, RocksDB `segments` CF, WAL-cleanup set)
with ordering enforced by convention — a bug class that produced four
data-integrity/WAL failures in the 2026-08-16/17 load-test campaign
(see ADR-0024 §Context). The program replaces the scatter with:

- **ADR-0024** — a dedicated **event WAL** (Reserve/Seal/Delete events,
  `data_wal_pos` position refs) as the single source of truth for
  segment lifecycle; the data WAL becomes a seekable pool of bytes.
- **ADR-0025** — an in-memory **`SegmentLifecycle` machine** owned by a
  single coordinator, with typed transitions (downgrades are compile
  errors); the RocksDB `segments` CF is removed.

The whole program is **three phases of migration** (machine → events →
CF removal). Each phase lands green before the next begins.

---

## The Program at a Glance

```
refactoring/segment-event-log-lifecycle/
├── spec-writer-guidance.md          ← design handoff (context, not spec)
├── segment-lifecycle-machine/       ← EPIC 1 — ADR-0025 phase 1
│   ├── lifecycle-registry-coordinator.md   [critical]   ⬅ START HERE
│   └── lifecycle-read-path.md
├── segment-event-log/               ← EPIC 2 — ADR-0024 + ADR-0025 phase 2
│   ├── event-wal-format.md                  [critical]
│   ├── event-wal-recovery.md                [critical]
│   └── event-wal-checkpoint.md
└── segments-cf-removal/             ← EPIC 3 — ADR-0025 phase 3
    ├── segments-cf-removal.md
    ├── compaction-state-machine.md
    └── startup-rebuild-from-machine.md
```

## Dependency Graph (implementation order)

```
EPIC 1 (machine)
  lifecycle-registry-coordinator ──► lifecycle-read-path
EPIC 2 (events)                      ▲
  lifecycle-registry-coordinator ──► event-wal-format ──► event-wal-recovery ──► event-wal-checkpoint
EPIC 3 (CF removal)                  ▲                        ▲                        ▲
  all five earlier features ───────► segments-cf-removal ──► compaction-state-machine
                                     │                        │
                                     └──────────┬─────────────┘
                                                ▼
                                     startup-rebuild-from-machine   (full 9-row matrix gate)
```

**The golden rule: EPIC 1 completes before EPIC 2 starts, EPIC 2 before
EPIC 3.** The CF-removal features depend on the machine's API being
stable; do not begin `segments-cf-removal` while `lifecycle-registry-`
`coordinator` is still changing shape.

---

## What Each Epic Does — and What It Kills

| Epic | Phase | Kills (by construction) | Delivers |
|---|---|---|---|
| **1. Machine** | ADR-0025 p1 | Phantom-downgrade race; idle-seal gap; scattered writers | Registry + coordinator; all writers routed through it; CF write becomes coordinator's side-effect (behavior unchanged) |
| **2. Events** | ADR-0024 + p2 | WAL-leak class (protection derives from events, not CF scans); non-deterministic recovery | Event WAL + own fsync group; fold-based recovery with `data_wal_pos` seek; byte-threshold checkpoint; CF becomes derived mirror (dual-read) |
| **3. CF removal** | ADR-0025 p3 | Metadata-only compaction; BadDigest repack; adoption heuristics; RocksDB `segments` CF | Compactor as state machine; startup rebuild from machine; consumers (GC/scrub/AE/reaper) on the machine; CF + deleted-markers gone |

Every feature's DoD is phrased as **mutation checks**: resurrecting the
downgrade write, the CF write, the pre-fsync SealEvent, the
`sealing_data` side-map, the `compressed: false` hardcode, or the
adoption heuristic must **fail a test**. "Unrepresentable, not patched."

---

## The Invariants — the Program's Contract

These six invariants appear in every relevant DoD. They are the
acceptance bar for the whole program:

1. **Reserve before data** — `ReserveEvent` precedes the first
   `DataEntry` of its segment.
2. **Seal causal order** — last `DataEntry` → `.dat` fsync →
   `SealEvent` (the worker cannot append the event before the fsync
   returns; `data_wal_pos` makes it checkable).
3. **Delete before unlink** — `DeleteEvent` durable **before** `.dat`
   removal. Row 6 of the crash-window table ("Sealed, file missing") is
   **unrepresentable**, not recoverable.
4. **Compaction chain** — new `.dat` → `SealEvent(new)` →
   `PutObject(new)` → `DeleteEvent(old)` → unlink old. Every crash
   window between milestones is safe.
5. **No downgrade** — a transition may never move a segment from a
   higher state to a lower one; enforced by the transition API shape.
6. **One durable writer** — the event log is the only durable writer of
   segment state. The CF is a derived mirror (epic 2) then gone
   (epic 3); it is never written independently.

## The Crash-Window Table (test contract)

ADR-0025 §Crash-window table has 9 rows. Distribution:

| Rows | Owner feature |
|---|---|
| 1–6 (reserve/seal/delete windows) | `event-wal-recovery` |
| 7–9 (compaction windows) | `compaction-state-machine` |
| Full 9-row matrix, node-level kill→restart | `startup-rebuild-from-machine` (the final gate) |

The matrix is the test contract, not documentation. A feature is not
done until its rows are automated fault-injection tests.

---

## Key Design Decisions to Respect (do not re-litigate)

- **Machine lives in `oceanfs-storage`** (`src/segment/lifecycle.rs`),
  not `oceanfs-durability` — ADR-0025 Decision 1 + "Domain placement".
  Durability consumers read through the existing trait boundary.
- **Event log is plain files with its own `WalSyncGroup`** — not
  RocksDB, not the data WAL's group — ADR-0024 Decision 4.
- **Checkpointing triggers on a byte threshold only** (no time-based
  fallback) — ADR-0024 Decision 3.
- **`data_wal_pos` position refs, not a global sequence number** —
  ADR-0024 Decision 2.
- **Objects stay in RocksDB.** The event log covers segment lifecycle
  only; do not drift toward full event sourcing.
- **Memory bound is TB-scale** (~500 MB at 10 TB, O(live segments)).
  Spec a registry-size gauge; never derive the design from the load-test
  box.

## What an Implementer Should Do When Picking Up a Feature

1. Read this document (you are here).
2. Read the feature doc's `adr:` frontmatter and the cited ADR sections.
3. Read the guidance document's §3 invariants and §5 pitfalls (they
   apply to every feature).
4. Identify your **inputs** (the features listed in `dependencies:`
   frontmatter — they must be done) and your **outputs** (who consumes
   you).
5. Identify your **regression suite**: the features listed in
   `dependencies:` are not just prerequisites — their tests become your
   safety net. In particular, `segment-pool-slot-state-machine`'s
   semantics (slot-scan read, backpressure, append-with-hook ordering)
   are the baseline the machine must preserve.
6. Land green: build, tests, clippy, fmt, docs — per PIPELINE.md.

## Current Status (2026-08-17)

- ADR-0024, ADR-0025: **proposed**, committed, indexed.
- All 8 feature specs: **proposed**, committed, indexed.
- Interim mitigation already landed and deployed: phantom check-then-
  write guard (`coordinator.rs`), idle-seal sweep (`pool.rs`), compactor
  data persistence + compression-ref preservation, tombstone-carried
  chunks, bucket-aware GC, scrub NotFound skip, reaper cadence. These
  are the **pre-program state** — the machine must preserve their
  behaviors as the regression suite, not regress them.
- **Next action:** begin EPIC 1, feature 1 (`lifecycle-registry-
  coordinator`).

## References

- ADR-0024 (event log), ADR-0025 (machine) — the authority
- `spec-writer-guidance.md` — the design handoff
- ADR-0023 (de-RocksDB direction), ADR-0018 (WAL domain rule),
  ADR-0020/0021 (read-path machinery absorbed), ADR-0009 (crate split)
- `docs/features/refactoring/segment-pool-slot-state-machine/feature.md`
  — the precedent; its semantics are the machine's regression baseline
- The 8 feature docs under this directory — the work breakdown
