---
title: "Spec-Writer Guidance — Segment Event Log & Lifecycle Machine (ADR-0024/0025)"
purpose: "Handoff hints for the spec-writer dispatching on ADR-0024 and ADR-0025. Read BEFORE drafting any feature specs. Not itself a spec."
status: guidance
date: 2026-08-17
related:
  - adr: 0024-segment-event-log
  - adr: 0025-segment-lifecycle-state-machine
---

# Spec-Writer Guidance: Segment Event Log & Lifecycle Machine

You are about to craft feature specs for **ADR-0024 (Segment Event Log)**
and **ADR-0025 (Segment Lifecycle State Machine)**. These two ADRs are
one design split in two: the event log is the *durable* half, the machine
is the *runtime enforcement* half. They must be speced as a coherent
program, not two independent features — the migration in ADR-0025 §
Migration is the spine of the whole effort.

This document is a **handoff hint, not a spec**. It tells you what to
preserve, what to avoid, and where the danger lies. The ADRs are the
authority; if anything here conflicts with them, the ADRs win — and tell
the implementer.

---

## 1. The core idea you must not lose

**Segment lifecycle transitions (reserved/sealed/deleted) become ordered,
durable events in a dedicated event WAL. A single in-memory state machine
(owned by one coordinator) enforces transitions by construction. The
RocksDB `segments` CF is removed.**

Everything else in the specs is detail. If a feature spec you write does
not make this spine obvious, rewrite it.

## 2. The bug class this kills (put this in every feature's Motivation)

The four 2026-08-16/17 load-test bugs were all *ordering* defects between
the segment WAL and the segments CF:

1. Phantom-downgrade race → WAL leak (~3.8 GB/hour, disk-full < 1 day)
2. Missing idle seal → same leak mechanism
3. Metadata-only compaction → crash-recovery mismatches
4. Compression-ref corruption on repack → BadDigest after restart

Every feature spec should reference at least one of these concrete
failures as its motivation. They are the acceptance bar: the design must
make each one *unrepresentable*, not just patched.

## 3. Mandatory invariants — they appear in EVERY relevant DoD

These are non-negotiable. Each feature's Definition of Done must include
the invariants it touches, in testable form:

| Invariant | Where it lives |
|---|---|
| `ReserveEvent` before first `DataEntry` | ADR-0024 §Decision 1 |
| last `DataEntry` → `.dat` fsync → `SealEvent` (seal causal order) | ADR-0024 §Decision 1 |
| `DeleteEvent` durable **before** `.dat` unlink | ADR-0024 §Decision 1 |
| Compaction: new `.dat` → `SealEvent(new)` → `PutObject(new)` → `DeleteEvent(old)` → unlink old | ADR-0024 §Decision 1 |
| No transition may downgrade (Sealed → unsealed is a compile error) | ADR-0025 §Decision 1 |
| `data_wal_pos` correctness (recovery seeks by it) | ADR-0024 §Decision 2 |
| Event log is the only durable writer of segment state; CF is never written independently (until removed) | ADR-0025 §Decision 1/3 |
| Crash-window table: every row must have a test | ADR-0025 §Crash-window table |

The crash-window table is the single most important test contract in the
whole design. **Every row must become a fault-injection test** (kill at
each milestone, assert the folded state). Do not spec the table as
"documentation" — spec it as a test matrix.

## 4. The migration spine — spec features in THIS order

ADR-0025 §Migration defines three phases. The specs must follow them:

1. **Machine first** (typed registry + coordinator; CF writes remain as
   the coordinator's side-effect; all writers go through the coordinator)
2. **Events second** (event WAL, position refs, fold-based recovery; CF
   as derived mirror with dual-read verification)
3. **CF removal third** (drop `segments` + deleted-markers CFs; move
   GC/scrub/AE/reaper/WAL-retention consumers onto the machine;
   checkpointing; fault-injection matrix; delete the adoption heuristic)

Each phase must land green (build, tests, clippy, fmt) before the next.
**Do not spec phase 3 before phase 1 exists** — the consumers' move
(GC/scrub/AE) depends on the machine's API being stable.

Suggested epic/feature decomposition (your call to refine, but keep the
dependency order):

- Epic: `segment-lifecycle-machine` (ADR-0025 phases 1)
  - feature: `lifecycle-registry-coordinator` — registry, typed
    transitions, coordinator ownership, writers routed through it
  - feature: `lifecycle-read-path` — try_read via registry; absorb
    ADR-0020/0021 machinery
- Epic: `segment-event-log` (ADR-0024 + ADR-0025 phase 2)
  - feature: `event-wal-format` — records, checksums, rotation, group
    commit reuse
  - feature: `event-wal-recovery` — fold algorithm, data_wal_pos seek,
    crash-window fault-injection matrix
  - feature: `event-wal-checkpoint` — snapshot + truncate (the event
    log's GC)
- Epic: `segments-cf-removal` (ADR-0025 phase 3)
  - feature: `segments-cf-removal` — drop CFs, move consumers
  - feature: `compaction-state-machine` — compactor as machine
    (ADR-0025 §Decision 4)
  - feature: `startup-rebuild-from-machine` — replaces interrupted-seal
    adoption

## 5. Pitfalls to steer the implementer away from

- **Do NOT spec a dual-write reconciliation layer.** If a spec says
  "keep CF and event log in sync with a check", it has missed the point.
  The event log is the source of truth; the CF is either a derived mirror
  (phase 2, read-only) or gone (phase 3).
- **Do NOT spec "narrow the race" fixes.** Every DoD must be phrased as
  "transition X is impossible by construction", never "the window is
  small".
- **Do NOT spec a global sequence number** across the two logs.
  ADR-0024 §Decision 2 chose position references. If a spec reinvents a
  shared counter, it contradicts the ADR.
- **Do NOT spec the event log in RocksDB.** Plain files with the
  project's own WAL discipline (ADR-0024 §Considered Alternatives). This
  is part of the de-RocksDB direction (ADR-0023).
- **Do NOT let compaction bypass the machine.** The compactor requests
  transitions from the coordinator; it never writes state or events
  itself (ADR-0025 §Decision 4). The BadDigest and metadata-only defects
  are the proof of what happens otherwise.
- **Do NOT spec object metadata into the event log.** Objects stay in
  RocksDB (confirmed scope). The event log covers segment lifecycle
  only. If a spec drifts toward full event sourcing, pull it back.
- **Do NOT forget the memory bound is TB-scale, not load-test-scale.**
  The registry at 10 TB is ~500 MB — state that cost explicitly in the
  specs (ADR-0025 §Decision 5) and spec a registry-size gauge.

## 6. What each spec must reference

- The two ADRs (0024, 0025) — mandatory `adr:` frontmatter
- The concrete code anchors (they will move during the work, but they
  anchor the "before" state):
  - `crates/oceanfs-server/src/write/coordinator.rs:661-693`
    (register_phantom_before_wal — the race)
  - `crates/oceanfs-storage/src/wal/replay.rs:349-465`
    (cleanup_old_wal_files / file_contains_live_entries)
  - `crates/oceanfs-storage/src/segment/pool.rs:108-123, 356-363`
    (SlotState, sealing_data)
  - `crates/oceanfs-durability/src/gc/segment_compactor.rs`
  - `crates/oceanfs-node/src/node.rs:995-1067` (adoption heuristic)
- The precedent: `docs/features/refactoring/segment-pool-slot-state-machine/feature.md`
  (structural invariants beat reactive patches — model the "before/after
  and why" framing on it)
- ADR-0023 (de-RocksDB direction), ADR-0018 (WAL domain rule),
  ADR-0020/0021 (read-path machinery being absorbed)

## 7. The tone

These specs are for a **production product that must run for days**.
Every DoD should read like it was written by someone who has already
seen the leak graphs. Precision over prose; invariants over intentions;
test matrices over examples. If a feature's DoD cannot be verified by a
machine, it is not done.

---

*Handoff complete. The ADRs (0024/0025) are the authority; this document
is the compass. If you find yourself unsure whether a spec detail
belongs, re-read ADR-0024 §Decision and ADR-0025 §Decision and ask
"which invariant does this serve?"*
