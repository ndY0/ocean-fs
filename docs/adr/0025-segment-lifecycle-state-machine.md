# ADR-0025: Segment Lifecycle State Machine — In-Memory Registry, Single Coordinator, Removal of the Segments CF

**Status:** Proposed
**Date:** 2026-08-17
**Deciders:** OceanFS architecture team

---

## Context

ADR-0024 introduces a dedicated segment event log as the source of truth
for lifecycle transitions. This ADR decides *how those transitions are
enforced, owned, and consumed at runtime* — and, consequently, what
happens to the `segments` column family.

Today, segment state is owned by six different structures:

| State aspect | Owner | Location |
|---|---|---|
| Accepting writes / frozen for seal | `PoolSlot::SlotState` (`Appending`/`Sealing`/`Idle`) | `pool.rs:108-123` |
| Read window during fill→disk | `sealing_data: RwLock<HashMap<SegmentId, Bytes>>` | `pool.rs:356-363` |
| Awaiting the seal worker | `SealingWork` + mpsc queue | `pool.rs:68-100, 322` |
| Temp-file → fsync → rename → metadata | Flush coordinator registrations | `segment_flush.rs:200-319` |
| Durable truth (`sealed_at`) | `segments` CF (RocksDB) | `store.rs:623` |
| Entry protection / sweepability | WAL cleanup's `durable_or_deleted` set | `replay.rs:349-465` |

There is **no single place that answers "what state is this segment
in?"** — the answer requires querying the slot, the sealing-data map, the
work queue, and the CF, and reconciling them. Every bug fixed in the
2026-08-16/17 campaign (see ADR-0024 §Context) was a failure of one of
these owners to observe another's transition. Conventions ("must run
under the slot lock", "must be registered before the WAL entry") encode
transitions as *ordering folklore*, not enforced structure.

The existing `refactoring/segment-pool-slot-state-machine` feature
(unifying `PoolSlot` under one lock) is the precedent: merging state and
segment under one lock made "state and segment are consistent" a
structural invariant. This ADR applies the same principle at the next
level of granularity — the whole segment lifecycle, across pool, seal
worker, compactor, reaper, GC, and recovery.

### Forces

- **Enforcement by design, not convention.** "Every participant follows
  the ordering" is not enforceable; "there is one participant" is.
- **The live segment set is bounded.** At production scale (TBs across
  multiple drives), live sealed segments are ~170K/TB ≈ **1.7M segments
  at 10 TB**. At ~300 B of `SegmentMetadata` per entry, the registry is
  **~50–500 MB** — bounded, proportional to live data, and cheap compared
  to the correctness it buys. (Deliberately *not* derived from the 75 GB
  load-test box; the bound is stated at TB scale.)
- **Compaction is a workflow with crash-relevant milestones.** It must
  be a state machine too, with the same discipline as the lifecycle.
- **Cross-store atomicity does not exist.** The event log and the
  objects CF (RocksDB) cannot be batched. Ordering across them must be
  by construction (ADR-0024's invariant table), with every crash window
  enumerated and safe.
- **The `segments` CF has exactly one job** — answer "is this segment
  sealed/deleted?" durably. The event log + checkpoint does that job
  better, in our own format, without RocksDB (ADR-0023 direction).

---

## Decision

### Decision 1: A single in-memory `SegmentLifecycle` registry, owned by a single coordinator

A new module — `crates/oceanfs-storage/src/segment/lifecycle.rs` —
defines the machine. It lives **in the storage domain**
(`oceanfs-storage`), not in a dedicated crate:

- The machine's primary writers — the segment pool, the seal worker's
  persistence path, and the WAL writer — are all in `oceanfs-storage`;
  the coordinator must sit beside them, not across a crate boundary.
- `oceanfs-durability` consumers (GC, scrub, AE, orphan reaper) call
  into it read-only through the existing `MetadataStore`-style boundary;
  ADR-0009's direction (trait-in-consuming-crate) is preserved — the
  consumers keep their trait, the machine is the implementation.
- A dedicated `oceanfs-lifecycle` crate would add a boundary with no
  counterpart on the other side: nothing outside storage owns segment
  lifecycle, so there is no second consumer class to justify the split.

**Domain placement (storage vs durability).** The machine is durability
*state* owned by the storage *domain* — the same relationship the data
WAL already has (durability machinery owned by storage,
`crates/oceanfs-storage/src/wal/`). Considered placing it in
`oceanfs-durability` and rejected:

- **Semantic pull.** Lifecycle transitions, crash recovery, and the
  event log are durability-flavored; the `oceanfs-durability` name
  points at them. But that crate is the *background-maintenance* home
  (GC, scrub, AE, reaper — ADR-0017's "look up a column family + act"
  tasks), not the WAL home. ADR-0009 already assigned **segment
  lifecycle ownership to `oceanfs-storage`** (recorded in ADR-0021
  §References).
- **Dependency direction is decisive.** `oceanfs-durability` depends on
  `oceanfs-storage`; storage cannot call into durability without a
  cycle. The machine's primary writers — the pool's fill→Sealing
  transition and the sealer's seal-complete — happen in storage's
  critical sections and must record the event exactly there; and the
  read path consults the machine on every GET via `try_read` (storage).
  Keeping it in-crate avoids a hot-path trait boundary into another
  crate.
- **Consumers are unaffected.** GC, scrub, AE, and the reaper read the
  machine through the existing trait boundary — they already depend on
  storage today. Nothing new crosses a crate edge on a hot path.

```rust
enum SegmentState {
    Reserved,          // ReserveEvent appended; no data yet (or data in flight)
    Sealed,            // SealEvent appended; .dat durable
    Deleted,           // DeleteEvent appended; .dat unlinked (or in progress)
}

struct LifecycleEntry {
    state: SegmentState,
    metadata: SegmentMetadata,   // full metadata: tier, ec_k/m, merkle_root
    data_wal_pos: Option<u64>,   // from SealEvent
}

// Registry: sharded RwLock<HashMap<SegmentId, LifecycleEntry>>, per-segment lock
```

**The coordinator (a single owner — a new `SegmentLifecycleCoordinator`
type, or the existing write coordinator extended) is the ONLY writer of
the registry and the ONLY appender of events.** The seal worker, the
compactor, the orphan reaper, and GC do not touch state directly; they
*request* transitions, and the coordinator validates, appends the
event, folds it, and updates the registry. This makes "every participant
follows the ordering" a non-problem: there is one participant.

**Typed transitions** — the registry exposes only valid methods:

```rust
fn reserve(&self, id, tier, ec) -> Result<()>;   // only on absent/Reserved
fn seal(&self, id, SealEvent) -> Result<()>;     // only on Reserved; takes fsync result + data_wal_pos
fn delete(&self, id) -> Result<()>;              // only on Reserved|Sealed
fn get(&self, id) -> Option<LifecycleEntry>;
```

Illegal transitions (e.g., `seal` on a `Sealed` id, `delete` on
`Deleted`, any downgrade) are **compile-time rejected** by the enum + API
shape. The phantom-downgrade race becomes unrepresentable.

### Decision 2: The read path asks the machine

`SegmentPool::try_read()` (ADR-0020/0021 machinery) is replaced by a
single lookup on the registry: "where do I read this segment?" →
slot buffer (`Reserved`/`Sealed` in-flight), frozen `Sealing` data, or
`.dat` (`Sealed`). The `sealing_data` side-map and the slot-scan +
sealing-data probe collapse into the registry's `Sealed`/in-flight
states. ADR-0021's `sealing_data` set is absorbed as the data attached to
the in-flight state — same mechanism, owned by the machine instead of a
side map.

### Decision 3: The `segments` CF is removed

The `segments` column family (and its deleted-markers CF) are deleted
from RocksDB. Consumers of `list_segments()`/`get_segment()` move to the
machine:

- WAL retention (ADR-0024 §Retention) — consults the event log /
  checkpoint, not the CF;
- GC liveness — enumerates via the machine;
- scrub — reads `merkle_root` from the machine's `Sealed` entries;
- anti-entropy — same (the incremental Merkle tree rebuilds from the
  machine at startup, replacing the `segments`-CF scan that ADR-0018
  Decision 1 established);
- orphan reaper — transitions to `delete()` on the coordinator.

RocksDB keeps the `objects` and `deletions` CFs only (confirmed scope:
objects stay in RocksDB). This is the segment-state slice of ADR-0023's
Phase 2; it does not replace the objects store.

**Startup reconstruction** is deterministic: load checkpoint (ADR-0024
Decision 3) → fold events after it → machine ready. No interrupted-seal
heuristics.

### Decision 4: Compaction is a state machine

The compactor becomes a machine with five crash-relevant milestones,
whose durable checkpoints are events:

```
Copying       → new .dat being written (no durable event yet)
NewSealed     → SealEvent(new) appended          [durable]
ObjectsMoved  → PutObject(new refs) committed    [RocksDB]
OldDeleted    → DeleteEvent(old) appended        [durable]
OldRemoved    → old .dat unlinked
```

The compactor requests each transition from the coordinator; the
coordinator enforces ADR-0024's compaction ordering. Crash recovery is a
fold + one objects-CF read (see crash-window table). The compactor's
`ChunkRef` repack preserves `compressed` + `logical_length` (the
BadDigest defect is impossible because the machine's `seal()` API takes
the full repacked metadata).

### Decision 5: Memory bound at production scale

The registry holds one entry per **live** segment (Reserved or Sealed,
not yet Deleted). At ~300 B/entry:

| Live segments | Registry RAM |
|---|---|
| 170K (1 TB) | ~50 MB |
| 1.7M (10 TB) | ~500 MB |
| 17M (100 TB) | ~5 GB |

The bound is **O(live segments), not O(lifetime writes)** — it scales
with data, not time, because deleted segments are evicted from the
registry on `DeleteEvent`. At the cost profile above (hundreds of MB for
TB-scale production), this is accepted: it eliminates an entire class of
corruption bugs for the price of RAM that is small relative to the
segment buffers (16 × 64 MB ≈ 1 GB) already in flight.

---

## Crash-window table (acceptance criteria)

| Crash between | Folded state | Recovery action |
|---|---|---|
| `ReserveEvent` → first `DataEntry` | Reserved, empty | Drop the reserve (idle-seal never seals empty) |
| `DataEntries` → `.dat` fsync | Reserved-unsealed | Seek data WAL by `data_wal_pos`, replay entries, re-seal |
| `.dat` fsync → `SealEvent` | Reserved-unsealed (`.dat` orphan) | Adopt: recompute root, append `SealEvent` (matches today's behavior, avoids re-seal I/O) |
| `SealEvent` → data-WAL sweep | Sealed | `.dat` authoritative; data entries swept by retention rule |
| `DeleteEvent` → `.dat` unlink | Deleted | `.dat` orphan → reaper sweeps |
| `.dat` unlink → `DeleteEvent` | **Sealed, file missing** | **Never allowed** — DeleteEvent must be durable *before* unlink (by construction) |
| Compaction: `NewSealed` → `ObjectsMoved` | New sealed, objects→old | New `.dat` orphan → reaper |
| Compaction: `ObjectsMoved` → `OldDeleted` | Objects→new, old sealed | Old segment sealed-orphan → reaper |
| Compaction: `OldDeleted` → `OldRemoved` | Old deleted, `.dat` present | Old `.dat` orphan → sweep |

Every window is safe because the coordinator's transition API *cannot*
emit a transition out of order. The acceptance criteria include a
**fault-injection test matrix**: kill the process at each milestone and
assert recovery lands in exactly the folded state.

---

## Consequences

### Positive

- **One owner, typed transitions.** The entire bug class (ordering
  between pool/seal-worker/compactor/CF/WAL) becomes unrepresentable.
- **The read path simplifies.** ADR-0020/0021 machinery collapses into
  one registry lookup; the `sealing_data` side-map disappears as a
  separate structure.
- **RocksDB shrinks.** The `segments` + deleted-markers CFs disappear;
  only objects + deletions remain. Directly serves ADR-0023's direction
  and de-risks the eventual native store.
- **Recovery is deterministic.** No adoption heuristics; the crash
  window table is the test contract.
- **Compaction gains the same discipline** as the lifecycle; the
  metadata-only and BadDigest defects are structurally impossible.

### Negative

- **Blast radius.** Every `list_segments()`/`get_segment()` consumer
  moves to the machine: GC liveness, scrub, AE (incl. the startup tree
  rebuild), orphan reaper, WAL retention, WAL replay, compactor. Wide,
  mechanical, and must be staged (migration below).
- **Cross-store ordering is now explicit.** The event log ↔ objects CF
  ordering (compaction) cannot be atomic; correctness rests on the
  invariant table + fault-injection tests.
- **New correctness surface.** The machine + checkpoint + event fold is
  new crash-recovery code; it must earn the same trust RocksDB's CF had
  (ADR-0023's correctness-asymmetry concern applies to the machine, not
  to the event log's plain-file format).
- **Memory is real.** Hundreds of MB at TB scale — accepted, but must be
  metric-visible (registry size gauge) and bounded by the same
  byte-budget discipline as the rest of the project.

### Neutral

- `MetadataStore` trait's segment methods become read-only consumers of
  the machine; the trait boundary (ADR-0009) is unchanged.
- The hot write path is unchanged except the Reserve event append; the
  seal/delete/compaction paths carry the new event appends.

---

## Migration (three phases)

1. **Machine first.** Introduce the registry + coordinator with typed
   transitions; keep CF writes as the coordinator's durable side-effect
   for now (no behavior change, but all writers go through the
   coordinator). This alone kills the phantom-downgrade race and the
   idle-seal gap.
2. **Events second.** Swap the CF writes for event appends; add the
   event log, position refs, and the fold-based recovery. Keep the CF as
   a derived mirror during this phase (dual-read verification).
3. **CF removal third.** Remove the `segments` CF and deleted-markers;
   move GC/scrub/AE/reaper/WAL-retention consumers onto the machine;
   add checkpointing; run the fault-injection matrix; delete the
   adoption heuristic.

Each phase lands green (build, tests, clippy, fmt) before the next.

---

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Keep the CF as a derived mirror (dual-write)** | Smaller first step; consumers unchanged | Every transition written twice with a reconciliation rule — the disease being cured; doubles the migration's eventual removal work | Rejected — the ADR lands the CF removal as phase 3 of the migration, not as a permanent dual-write |
| **Full native store now (ADR-0023 Phase 2 entire)** | One rewrite, no intermediate states | Objects + inline payloads + tombstones are a much larger correctness surface; conflicts with "consolidate current work first" | Rejected — this ADR is deliberately the segment-state slice only |
| **Machine in `oceanfs-durability`** | Near the consumers (GC, scrub, AE) | The pool + seal worker (the machine's core writers) are in `oceanfs-storage`; ownership would cross the crate boundary in the wrong direction | Rejected — the machine lives with its primary writers in `oceanfs-storage`, exposed read-only to `oceanfs-durability` consumers |
| **Dedicated `oceanfs-lifecycle` crate** | Clean ownership boundary; machine is a first-class crate | Adds a crate boundary with no counterpart on the other side: nothing outside storage owns segment lifecycle, so no second consumer class justifies the split; the coordinator would sit far from the pool and seal worker it coordinates | Rejected — Decision 1 places the machine in `oceanfs-storage/src/segment/lifecycle.rs`; consumers keep their trait boundary (ADR-0009), the machine is the implementation |
| **Keep six owners, add tests** | Zero refactor risk | Tests prove the folklore; they do not enforce it; the campaign showed the folklore fails under load | Rejected — enforcement by design is the requirement |

---

## References

- ADR-0024 (Segment Event Log — companion ADR; event schema, ordering
  invariants, retention, checkpointing)
- ADR-0023 (Metadata Store native replacement path — this ADR is its
  segment-state slice; objects stay in RocksDB)
- ADR-0018 (WAL consolidation — its Decision 1's segments-CF startup
  scan is superseded by machine rebuild)
- ADR-0020 / ADR-0021 (read path machinery absorbed by Decision 2)
- ADR-0009 (crate boundaries; machine placement)
- `crates/oceanfs-storage/src/segment/pool.rs:108-123, 356-363` —
  today's distributed state (SlotState, sealing_data)
- `crates/oceanfs-server/src/write/coordinator.rs:661-693` —
  `register_phantom_before_wal` (the race this design eliminates)
- `crates/oceanfs-storage/src/io/segment_flush.rs:200-319` — flush
  coordinator (the machine's I/O executor underneath)
- `crates/oceanfs-durability/src/gc/segment_compactor.rs` — becomes a
  state machine (Decision 4)
- `crates/oceanfs-durability/src/{scrub,anti_entropy,gc,orphan_reaper}` —
  consumers moving to the machine (Decision 3)
- `docs/features/refactoring/segment-pool-slot-state-machine/feature.md`
  — the precedent: structural invariants beat reactive patches
- Spec §4.2 (Pipeline Parallelism) — async sealing preserved
- `guidelines/architecture.md` §4.1 — composition-root and cross-crate
  construction rules
