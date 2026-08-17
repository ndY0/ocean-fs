---
feature: "Segment Lifecycle Registry & Coordinator"
epic: "refactoring/segment-event-log-lifecycle/segment-lifecycle-machine"
status: done
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

- [x] **Code:** `cargo build --all-targets`, `cargo fmt --check`, and
      `cargo clippy --lib -- -D warnings` pass in `oceanfs-storage`,
      `oceanfs-server`, `oceanfs-node`, `oceanfs-durability`;
      `#![deny(missing_docs)]` passes; no `std::sync::Mutex`/`RwLock` in
      changed files (perf 2.3).
      <!-- REVIEW (iteration 2, PASS): build/fmt/clippy/-D warnings verified clean on all 5 crates; no std::sync locks in changed files (grep); missing_docs clean. RUSTDOCFLAGS="-D warnings" cargo doc --no-deps clean — the 4 broken intra-doc links flagged in iteration 1 (lifecycle.rs:50-51 gauge names, sealer.rs:112 + segment_flush.rs:12 seal_finalized_batch links) were fixed (gauge names escaped as code spans, seal_finalized_batch links converted to code spans). Verified iteration 2, PASS. -->
- [x] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      and `cargo test -p oceanfs-server --lib -- --test-threads=1` green
      (PIPELINE.md §4.6 RocksDB caveat), including the precedent feature's
      regression suite (slot-state-machine, backpressure, append-with-hook
      ordering).
      <!-- REVIEW: verified — storage lib 247/0, server lib 215/0, node lib 32/0, durability lib 219/0, core 191/0; storage integration 10 files green, server 7 files green, durability 5 files green, node 14 files green, all --test-threads=1. -->
- [x] **Invariant — no downgrade (ADR-0025 Decision 1):** unit tests cover
      every illegal transition (`reserve` on `Sealed`/`Deleted`, `seal` on
      `Sealed`/`Deleted`/missing, `delete` on `Deleted`) and assert
      `Err(...)` **and** an unchanged registry. Mutation check: re-adding a
      state-downgrading write (a `seal_at: None` re-write over `Sealed`)
      must fail a test — the phantom-downgrade race is unrepresentable.
      <!-- REVIEW: verified — lifecycle.rs tests cover all illegal transitions with unchanged-registry asserts (reserve_on_sealed_returns_already_sealed_and_does_not_mutate:925, seal_on_sealed_returns_already_sealed_and_does_not_mutate:963, seal_on_deleted:976, seal_on_missing:987, delete_on_deleted:1016, delete_on_missing:1026); coordinator-level request_reserve_on_sealed_is_rejected_without_downgrade:1268 asserts CF+registry both unchanged; server-level poisoned-probe stress test concurrent_put_seal_stress_never_downgrades_registry (write/coordinator.rs:2853) probes every sealed CF id with request_reserve→Err(AlreadySealed) + unchanged CF/registry. A downgrade write would fail request_reserve_on_sealed_is_rejected_without_downgrade (CF sealed_at assert). No method assigns a lower state — downgrade not expressible. -->
- [x] **Invariant — reserve before first DataEntry:** the coordinator's
      `request_reserve` returns `Ok` only after the durable CF write; the
      write path calls it before `write_wal_entry`. Mutation check: moving
      the registration after the WAL append must fail the
      crash-recovery test (kill after first entry → segment present).
      <!-- REVIEW (iteration 2, PASS): verified — request_reserve (lifecycle.rs:687) is validate→put_segment→fold, Ok only after CF write; request_reserve_before_wal (write/coordinator.rs:684) precedes write_wal_entry at all 3 sites (458→463, 494→501, 540→543); round-trip test lifecycle_write_seal_read_roundtrip_through_coordinator:2774 asserts registry entry + CF phantom at PUT return. MUTATION CHECK ADDED (iteration 2): wal_recovery.rs replay_recovers_segment_reserved_before_crash (kill + replay after the first entry, before any WAL write) verifies the reserved segment survives — PASSED. Reviewer nuance (documented): the literal mutation phrasing — a test that reorders the registration and asserts failure — is not discriminatorily satisfiable given D3 (replay-side reserve: replay reserves every rebuilt segment before its first replayed entry, so the segment is present regardless of put-path ordering). The ordering is instead load-bearing through the three structural call sites plus the deterministic request_reserve_on_sealed_is_rejected_without_downgrade test. -->
- [x] **Invariant — coordinator is the only writer:** `grep`-verifiable —
      every `put_segment` / deleted-marker CF write outside
      `segment/lifecycle.rs` is gone; `register_phantom_before_wal` and its
      call sites are deleted.
      <!-- REVIEW: verified with documented exceptions — register_phantom_before_wal deleted (only doc references remain); s3_handler/handlers.rs phantom put_segment block removed (line 171-177); flush path routed via seal_finalized_batch (segment_flush.rs:320); reaper delete via request_delete (orphan_reaper.rs:161); replay reserve via request_reserve (replay.rs:114). Remaining production CF writers: segment_compactor.rs:96 delete_segment, node.rs:1094 adoption put_segment, heal/worker.rs:414 metadata refresh — all three documented as intentional phase-1 exceptions in segments-cf-removal.md §Known remaining CF writers (user-approved); all other put_segment/delete_segment hits are #[cfg(test)] mocks or the store impl itself. DoD's literal "every put_segment outside lifecycle.rs is gone" is satisfied for the three in-scope writers (phantom, seal-complete, reaper delete). -->
- [x] **Idle-seal (leak regression):** a partially-filled segment with no
      further writes is sealed within `seal_timeout_ms` (sealer.rs:40-77
      config honored); an empty segment is NOT sealed (dropped at recovery
      per ADR-0024 retention). Mutation check: disabling the idle driver
      must fail the test.
      <!-- REVIEW: verified — coordinator.seal_idle_segments (lifecycle.rs:839) sweeps wired pools via sweep_idle_segments (pool.rs:988), which uses slot.try_seal_idle (pool.rs:231, empty + idle_for checks); with_idle_seal wired in node.rs:684 with seal_config.seal_timeout_ms; server tick at write/coordinator.rs:908-920 (interval = timeout/4). Tests: seal_idle_segments_seals_partially_filled_segment (lifecycle.rs:1454) + seal_idle_segments_does_not_seal_empty_segment (1479) call the tick directly — disabling the driver (making the tick a no-op) fails the first (assert_ne on fresh segment id); server round-trip test relies on the idle driver for a below-fill segment (would time out if disabled). Empty-never-sealed covered by try_seal_idle's is_empty check + test. -->
- [x] **Memory bound + gauge:** registry `len()` equals the live-segment
      count after seal/delete churn; `mem_estimate_bytes()` ≤ ~350 B ×
      `len()`; the gauge reports both; a churn test (10K reserve→seal→delete)
      ends with `len() == 0` (O(live), not O(lifetime)).
      <!-- REVIEW: verified — churn_10k_reserve_seal_delete_ends_empty (lifecycle.rs:1115) ends len()==0; mem_estimate_is_bounded_by_350_bytes_per_entry (1095); delete_with_default_grace_evicts_immediately (1033) + len_counts_live_entries_across_shards (1081); gauges oceanfs_lifecycle_registry_entries + oceanfs_lifecycle_registry_bytes_estimate created in new() (603-612), updated after every fold via update_gauges, registered via register_metrics (852) and wired at node.rs:1255; test register_metrics_registers_lifecycle_gauges (1405). -->
- [x] **Perf 7.1:** no I/O, allocation, or computation under a shard lock —
      the lock bodies contain only map reads/writes; the durable CF write
      and the fold are separate critical sections (validate → release →
      durable → fold). Perf 7.4: LOCK ORDER documented in `lifecycle.rs`
      (registry shard is a leaf; never held while acquiring a slot lock).
      <!-- REVIEW: verified — validate_* methods take only read locks and return before any I/O; durable put_segment/batch_write happens with no shard lock held; fold_* re-acquire write locks after the durable write (request_reserve:694-709, request_seal:730-736, request_delete:754-760, seal_finalized_batch:778-827 — validate phase → one batch_write → fold phase). LOCK ORDER comment at lifecycle.rs:31-44 documents shard as leaf, never held while acquiring slot locks; seal_idle_segments holds no registry locks while pools sweep (which take slot locks). parking_lot::RwLock used (perf 2.3/7.2). -->
- [x] **Integration:** a write→seal→read round trip through the coordinator
      (server crate boundary) plus a concurrent put/seal stress test with
      zero `Sealed`-to-`Reserved` downgrades observed in a poisoned
      registry probe.
      <!-- REVIEW: verified — lifecycle_write_seal_read_roundtrip_through_coordinator (write/coordinator.rs:2774) does PUT→registry+CF phantom→seal worker→Sealed in both→disk read-back with BLAKE3 verify; concurrent_put_seal_stress_never_downgrades_registry (2853) runs 16 concurrent PUTs + seal worker, waits all Sealed, probes every CF id with registry/CF agreement + poison request_reserve→Err(AlreadySealed) + unchanged stores. Both green under --test-threads=1. -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --all-targets -D
> warnings` test-code warnings and `ignore`-tagged doc examples are
> structural hygiene tracked separately (guidelines/coding.md §9.2.1).

## Deviations

Accepted deviations from the original feature intent, agreed between the
implementer and the user (documented in the implementation conversation).
Each keeps a load-bearing property of the design (single writer, no
downgrade, reserve-before-data, leak-free idle seal) while adjusting the
mechanism or the phase-1 boundary.

### D1-A — Idle-seal driver: coordinator-owned timer with pool-driven detection

The idle-seal driver is a coordinator-owned timer whose detection is driven
by the pools: the pools' slots are the idle detectors, and
`seal_idle_segments()` ticks their sweeps (`sweep_idle_segments` →
`try_seal_idle`). This replaces the earlier notion of per-entry timestamps
maintained inside the registry. Zero hot-path cost: the sweep runs only on
the timer tick, and no registry lock is held while the pools sweep (which
take slot locks).

### D2 — Remaining CF writers outside the coordinator (phase-1 exceptions)

The GC compactor's `PutSegment`/`DeleteSegment` writes and the node-startup
interrupted-seal adoption `put_segment` remain outside the coordinator until
phase 3 (recorded in `segments-cf-removal.md` §"Known remaining CF writers").
The heal worker's post-repair metadata refresh (`merkle_root` invalidation,
no lifecycle state change) also remains, documented in the same section.
All are user-approved phase-1 exceptions; the DoD's only-writer item is
satisfied for the three in-scope writers (phantom, seal-complete, reaper
delete).

### D3 — WAL replay reserves every rebuilt segment before its first replayed entry

`replay_wal` and `replay_queued_segment` gained a lifecycle param and reserve
each rebuilt segment through the coordinator before the segment's first
replayed entry is written. The reserve-before-first-DataEntry ordering thus
holds across recovery, not just the live put path — and it is why the DoD's
literal mutation phrasing for that invariant is not discriminatorily
satisfiable (see the reserve-before-data DoD note above).

### D8 — Startup `seed_from_metadata_store`

At node startup the registry is populated from the `segments` CF — pure
registry folds, no CF writes — so the coordinator is the complete single
writer over pre-existing data (the reaper's `request_delete` validates
against the seeded registry).

### Ordering refinement — `request_reserve` precedes the pool append

`request_reserve` precedes the pool append, not only the WAL entry — required
because the fill-triggered seal work item is enqueued during the append and
the seal path validates `Reserved`-only. The seal worker also performs an
idempotent reserve-on-miss (through the coordinator) to close the
drain-vs-reserve race.

### Interface details

- `LifecycleEntry` has `pub` fields per the feature's Interface section
  (coding.md §1.4 exception for documented public data structs).
- `seal` on a `Deleted` entry returns `NotReserved` (documented transition
  outcome, covered by the unit tests for illegal transitions).

### Pre-existing test defect fixed (test-gate unblock)

`pool_handles_segment_full_with_seal_queue_not_draining` hung forever:
`blocking_send` on a full queue with an undrained receiver. Verified
pre-existing on HEAD (not introduced by this feature); fixed by draining the
queue on a background thread. Required to unblock the storage test gate.

