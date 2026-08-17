---
feature: "Segment Lifecycle Registry & Coordinator"
epic: "refactoring/segment-event-log-lifecycle/segment-lifecycle-machine"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: refactoring/segment-pool-slot-state-machine
    reason: Single-lock SlotState precedent must be complete; its semantics (slot-scan read, backpressure, append-with-hook ordering) become this feature's regression suite
adr:
  - 0025-segment-lifecycle-state-machine
  - 0024-segment-event-log
  - 0009-storage-crate-split
perf:
  - "7.1 Minimize lock hold duration"
  - "7.2 RwLock when reads >= 10x writes"
  - "2.3 parking_lot::RwLock everywhere"
  - "7.4 Lock ordering documentation"
  - "1.3 Pre-size collections with known capacity"
  - "11.1 Atomic counters on hot paths"
created: 2026-08-17
updated: 2026-08-17
---

# Segment Lifecycle Registry & Coordinator

## Summary

Build the runtime half of the design (ADR-0025 Decision 1) in
`crates/oceanfs-storage/src/segment/lifecycle.rs`: an in-memory
`SegmentLifecycleRegistry` (sharded `parking_lot::RwLock<HashMap<SegmentId,
LifecycleEntry>>`) holding exactly one entry per live segment, and a single
`SegmentLifecycleCoordinator` that is the **only writer** of segment lifecycle
state. This is ADR-0025 migration phase 1: the RocksDB `segments` CF write
remains as the coordinator's durable side-effect (no behavior change), but the
pool, the seal worker, the reaper, and GC stop touching state directly — they
*request* transitions. The phantom-downgrade race and the idle-seal gap die
here, by construction, before any event log exists.

## Evidence/Motivation

The 2026-08-16/17 load-test campaign produced four ordering defects between
the segment WAL and the segments CF; two of them are killed by this feature
alone, and the other two by the follow-on features in this program:

1. **Phantom-downgrade race** — `register_phantom_before_wal`
   (`crates/oceanfs-server/src/write/coordinator.rs:447-...`, guidance anchor
   661-693) contains an explicit `// Do NOT downgrade an already-sealed
   segment` guard with a pre-CF-read retry. That comment *is* the patch: the
   seal worker (a separate task) can persist `sealed_at: Some` between the
   append and the phantom write, and the phantom then downgrades the sealed
   entry to unsealed; nothing ever re-seals it and the WAL cleanup protects
   its files forever — measured `protected` growth 17 → 45 in 30 min
   (~3.8 GB/hour, disk-full in < 1 day on the 75 GB SUT). The fix here is not
   a better guard; the guard becomes unrepresentable because there is no
   second writer to race: `reserve()` on an already-`Sealed` id is rejected by
   the transition API, and no API exists that can write a lower state over a
   higher one.
2. **Missing idle seal** — the pool sealed only on `is_full()`
   (`crates/oceanfs-storage/src/segment/pool.rs:108-123`, `SlotState`), and
   the sealer's `seal_timeout_ms` logic
   (`crates/oceanfs-storage/src/segment/sealer.rs:40-77,162`) was never
   driven, so a partially-filled segment that stopped receiving writes stayed
   `Reserved` forever and pinned its WAL files (same leak mechanism). The
   coordinator owns the idle-seal timer here: every `Reserved` entry that
   stops receiving writes for `seal_timeout_ms` is sealed.

ADR-0025's context table lists six owners of segment state (slot, sealing
map, work queue, flush coordinator, CF, WAL-cleanup set). This feature
collapses the *writers* of that state to one participant. "Every participant
follows the ordering" becomes "there is one participant".

## Scope

### In Scope

- `crates/oceanfs-storage/src/segment/lifecycle.rs`: `SegmentState`,
  `LifecycleEntry`, `SegmentLifecycleRegistry`, `SegmentLifecycleCoordinator`,
  `TransitionError`, `SegmentMetadata` re-use (tier, ec_k/m, merkle_root).
- Sharded registry: N shards (config `lifecycle_registry_shards`, default 64),
  each `parking_lot::RwLock<HashMap<SegmentId, LifecycleEntry>>`; shard chosen
  by `SegmentId` hash. Reads (GET-path resolution, GC/scrub enumeration) never
  block each other; writes are once-per-lifecycle (fill / seal / delete).
- Typed transitions with the ADR-0025 method shapes:
  `reserve` (absent | `Reserved`), `seal` (`Reserved` only, takes the full
  sealed metadata incl. seal-time `merkle_root`), `delete`
  (`Reserved` | `Sealed`), `get`, `for_each`, `len`, `mem_estimate_bytes`.
  Illegal transitions return `TransitionError` variants
  (`AlreadyReserved`, `AlreadySealed`, `AlreadyDeleted`, `NotReserved`,
  `Missing`) and never mutate state. There is **no method that assigns a
  lower state** — downgrade is not expressible.
- `SegmentLifecycleCoordinator` as the single writer:
  - `request_reserve` / `request_seal` / `request_delete`, each:
    validate (registry) → durable side-effect (phase 1: `put_segment` /
    deleted-marker CF write via `MetadataStore`) → fold into the registry.
  - The durable write and the registry fold are strictly ordered; the fold
    happens only after the durable side-effect returns.
  - Idle-seal driver: a `Reserved` entry with no appends for
    `seal_timeout_ms` → `request_seal` (empty segments are dropped by the
    reserve-adoption rule in the recovery feature; an idle partial segment is
    sealed).
- Route every existing CF writer through the coordinator:
  - pool fill → phantom registration (`register_phantom_before_wal` callers)
  - seal worker persistence path (`segment_flush.rs:200-319` seal-complete)
  - orphan reaper / delete-path deleted-marker writes
- Registry-size gauge: `oceanfs_lifecycle_registry_entries` (count) and
  `oceanfs_lifecycle_registry_bytes_estimate` (entries ×
  `mem_estimate_bytes()`), registered via the existing `register_metrics`
  path (perf 11.1).

### Out of Scope

- The event WAL, `data_wal_pos`, and fold-based recovery (features
  `event-wal-format` / `event-wal-recovery` in this program).
- Read-path resolution via the machine (feature `lifecycle-read-path`).
- Removing the `segments` CF (feature `segments-cf-removal`, phase 3).
- Compaction as a state machine (feature `compaction-state-machine`).
- Object metadata; anything outside segment lifecycle stays in RocksDB.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | New module `segment/lifecycle.rs`; `segment/pool.rs` + `segment/sealer.rs` call the coordinator instead of the metadata store; `wal/replay.rs` `cleanup_old_wal_files` still reads the CF in this phase (unchanged) |
| `oceanfs-server` | `write/coordinator.rs` phantom registration + seal-complete call `lifecycle_coordinator.request_*`; `register_phantom_before_wal` deleted |
| `oceanfs-durability` | `gc/orphan_reaper.rs` delete-path routed through the coordinator (trait boundary unchanged) |
| `oceanfs-node` | Composition root constructs the coordinator and injects it into server + durability |

## Interface (Public API)

All items in `oceanfs_storage::segment::lifecycle` (re-exported from the
crate facade):

- `pub enum SegmentState { Reserved, Sealed, Deleted }` — the only states;
  no sub-states, no "sealed but downgradable" representation.
- `pub struct LifecycleEntry { pub state: SegmentState, pub metadata: SegmentMetadata }`
  — the full metadata lives with the state (tier, ec_k/ec_m, `merkle_root`
  filled at seal). `data_wal_pos` is added by `event-wal-format`.
- `pub struct SegmentLifecycleRegistry` — sharded map; all methods take
  `&self`:
  - `pub fn reserve(&self, id: SegmentId, metadata: SegmentMetadata) -> Result<(), TransitionError>`
    — `Ok` only when absent or already `Reserved` (idempotent re-reserve);
    on a `Sealed`/`Deleted` id → `Err(AlreadySealed|AlreadyDeleted)`, no
    mutation.
  - `pub fn seal(&self, id: SegmentId, metadata: SegmentMetadata) -> Result<(), TransitionError>`
    — `Reserved` → `Sealed` only; `Err(AlreadySealed)` / `Err(Missing)`
    otherwise.
  - `pub fn delete(&self, id: SegmentId) -> Result<(), TransitionError>` —
    `Reserved` | `Sealed` → `Deleted`; `Err(AlreadyDeleted)` otherwise. The
    entry is evicted after a configurable grace (default: immediate), keeping
    the registry O(live segments).
  - `pub fn get(&self, id: SegmentId) -> Option<LifecycleEntry>`
  - `pub fn for_each(&self, f: impl FnMut(SegmentId, &LifecycleEntry))` —
    snapshot enumeration for GC liveness / scrub / AE consumers (phase 3).
  - `pub fn len(&self) -> usize`
  - `pub fn mem_estimate_bytes(&self) -> u64`
- `pub struct SegmentLifecycleCoordinator` — the single writer:
  - `pub async fn request_reserve(&self, id: SegmentId, tier: SizeTier, ec_k: u8, ec_m: u8) -> Result<()>`
  - `pub async fn request_seal(&self, id: SegmentId, metadata: SegmentMetadata) -> Result<()>`
  - `pub async fn request_delete(&self, id: SegmentId) -> Result<()>`
  - `pub fn registry(&self) -> &SegmentLifecycleRegistry`
  - `pub async fn seal_idle_segments(&self)` — the idle-seal driver tick.
- `pub enum TransitionError { Missing, AlreadyReserved, AlreadySealed, AlreadyDeleted, NotReserved, DurableWriteFailed }`
- `pub fn shard_count(config: &LifecycleConfig) -> usize` (config in
  `oceanfs-core`).

Memory bound (ADR-0025 Decision 5 — stated at TB scale, not load-test
scale): ~300 B/entry × ~170K live segments/TB → **~50 MB at 1 TB, ~500 MB at
10 TB (1.7M segments), ~5 GB at 100 TB**. The bound is O(live segments), not
O(lifetime writes): `delete()` evicts. The gauge makes the actual cost
visible continuously.

## Data Flow

```
PUT /{bucket}/{key}
  → WriteCoordinator::put()
    → pool slot append (fill path)
      → coordinator.request_reserve(id, tier, ec)      // durable BEFORE first DataEntry
        → registry.reserve (validate) → CF put_segment (durable) → fold
    → write_wal_entry(...)                             // first DataEntry AFTER Ok
  → (async) seal worker: fsync .dat
    → coordinator.request_seal(id, full metadata + merkle_root)
      → registry.seal (validate) → CF sealed_at write (durable) → fold

GC / reaper / delete path
  → coordinator.request_delete(id)
    → registry.delete (validate) → CF deleted-marker (durable) → fold
```

Invariant enforcement points (ADR-0024 §Decision 1 orderings, phase-1
form): `request_reserve` returns `Ok` **before** the write path may append
the first `DataEntry`; `request_seal` is called only after the `.dat` fsync
returns (the seal worker's operation sequence); `request_delete` completes
before the `.dat` unlink is issued by the reaper.

## Definition of Done

- [ ] **Code:** `cargo build --all-targets`, `cargo fmt --check`, and
      `cargo clippy --lib -- -D warnings` pass in `oceanfs-storage`,
      `oceanfs-server`, `oceanfs-node`, `oceanfs-durability`;
      `#![deny(missing_docs)]` passes; no `std::sync::Mutex`/`RwLock` in
      changed files (perf 2.3).
- [ ] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      and `cargo test -p oceanfs-server --lib -- --test-threads=1` green
      (PIPELINE.md §4.6 RocksDB caveat), including the precedent feature's
      regression suite (slot-state-machine, backpressure, append-with-hook
      ordering).
- [ ] **Invariant — no downgrade (ADR-0025 Decision 1):** unit tests cover
      every illegal transition (`reserve` on `Sealed`/`Deleted`, `seal` on
      `Sealed`/`Deleted`/missing, `delete` on `Deleted`) and assert
      `Err(...)` **and** an unchanged registry. Mutation check: re-adding a
      state-downgrading write (a `seal_at: None` re-write over `Sealed`)
      must fail a test — the phantom-downgrade race is unrepresentable.
- [ ] **Invariant — reserve before first DataEntry:** the coordinator's
      `request_reserve` returns `Ok` only after the durable CF write; the
      write path calls it before `write_wal_entry`. Mutation check: moving
      the registration after the WAL append must fail the
      crash-recovery test (kill after first entry → segment present).
- [ ] **Invariant — coordinator is the only writer:** `grep`-verifiable —
      every `put_segment` / deleted-marker CF write outside
      `segment/lifecycle.rs` is gone; `register_phantom_before_wal` and its
      call sites are deleted.
- [ ] **Idle-seal (leak regression):** a partially-filled segment with no
      further writes is sealed within `seal_timeout_ms` (sealer.rs:40-77
      config honored); an empty segment is NOT sealed (dropped at recovery
      per ADR-0024 retention). Mutation check: disabling the idle driver
      must fail the test.
- [ ] **Memory bound + gauge:** registry `len()` equals the live-segment
      count after seal/delete churn; `mem_estimate_bytes()` ≤ ~350 B ×
      `len()`; the gauge reports both; a churn test (10K reserve→seal→delete)
      ends with `len() == 0` (O(live), not O(lifetime)).
- [ ] **Perf 7.1:** no I/O, allocation, or computation under a shard lock —
      the lock bodies contain only map reads/writes; the durable CF write
      and the fold are separate critical sections (validate → release →
      durable → fold). Perf 7.4: LOCK ORDER documented in `lifecycle.rs`
      (registry shard is a leaf; never held while acquiring a slot lock).
- [ ] **Integration:** a write→seal→read round trip through the coordinator
      (server crate boundary) plus a concurrent put/seal stress test with
      zero `Sealed`-to-`Reserved` downgrades observed in a poisoned
      registry probe.

> **Lint & Doc Examples (non-gating):** `cargo clippy --all-targets -D
> warnings` test-code warnings and `ignore`-tagged doc examples are
> structural hygiene tracked separately (guidelines/coding.md §9.2.1).
