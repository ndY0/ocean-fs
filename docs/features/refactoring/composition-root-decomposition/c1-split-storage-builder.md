---
feature: "c1: Extract StorageModule Builder from Node::start"
epic: "refactoring/composition-root-decomposition"
status: done
priority: critical
owner: ""
dependencies:
  - feature: none (first in epic)
    reason: No composition-root dependencies; carries the legacy-mode precondition from ADR-0031. Review #64 (B2) is fixed in wave-0/1 f1; review #35 (B1) is DEFERRED from wave-0/1 and closed by this feature's NodeLeaveHandler deletion (DECISION 2026-09-04).
adr:
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
  - 0029-storage-pools-disk-resilience
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# c1: Extract StorageModule Builder from Node::start

## Summary

Extract the storage subsystem construction out of
`Node::start()` (`node.rs:565-1830` sections 0–6c, 11, 6a/6b recovery)
into a dedicated module `crates/oceanfs-node/src/modules/storage.rs`
exposing a plain builder:

```rust
pub struct StorageModule {
    pub registry: Arc<PoolRegistry>,          // the boot registry (§0)
    pub paths: PoolPaths,                     // role-pinned dirs (deviation §Interface)
    pub metadata_store: Arc<RocksDbMetadataStore>, // opened inline §1, owned here
    pub accel: Arc<AccelDispatcher>,          // probed inline §2, owned here
    pub wal_writer: Arc<WalWriter>,
    pub lifecycle_registry: Arc<SegmentLifecycleRegistry>,
    pub event_wal: Arc<EventWal>,
    pub event_checkpoint: Arc<EventCheckpoint>,
    pub lifecycle: Arc<SegmentLifecycleCoordinator>,
    pub sealer: Arc<SegmentSealer>,
    pub data_store: Arc<dyn oceanfs_durability::SegmentDataStore>, // ONE shared DiskSegmentStore
    pub shard_store: Arc<dyn oceanfs_durability::SegmentShardStore>, // ONE shared DiskSegmentShardStore
    pub segment_replicator: Arc<crate::segment_replicator::SegmentReplicator>,
    pub segment_reader: Arc<dyn oceanfs_storage::io::SegmentReader>,
    pub remap_alias: Arc<SegmentRemapAlias>,
    pub io_observer: Arc<IoObserver>,
    // Write-path pools exposed for the inline write-coordinator/metrics
    // consumers (deviation §Interface): shard_buffer_pool, shard_small,
    // shard_standard, segment_pool_small, segment_pool_standard,
    // active_pools, startup_rebuild_gauge
}

impl StorageModule {
    pub async fn build(
        config: &NodeConfig, paths: &PoolPaths, registry: Arc<PoolRegistry>,
        metadata_store: Arc<RocksDbMetadataStore>, accel: Arc<AccelDispatcher>,
        ring_cache: Arc<RingCache>, membership: Arc<Membership>,
        pool: Arc<ConnectionPool>,
    ) -> Result<Self, String>;
    pub async fn run_startup_recovery(&self) -> Result<(), String>; // node.rs 6a/6b sections
}
```

> **Interface deviations (approved 2026-09-04, plan review Q2/Q4).** The
> documented two-argument `build(config, paths)` cannot construct the
> replicator — it needs the network handles built at §4/§5; the actual
> signature takes them (plus the §0–§2 prelude) as arguments. The struct
> drops `wal_reader` (no persisted object — opened locally inside
> recovery) and `pool_registry_for_server` (the same `Arc` as `registry`),
> and adds `paths`, the write-path pools, `active_pools` and
> `startup_rebuild_gauge` (all consumed by the still-inline
> coordinator/metrics code). `run_startup_recovery` is called at the
> position the inline §6a/§6b blocks used to occupy (after the §7–§11
> material still inline in `start()`).

## Scope

### In Scope
- Move construction code (not behavior) into `modules/storage.rs`.
- **Consolidate the store instances to TWO shared instances** — one
  `DiskSegmentStore` + one `DiskSegmentShardStore` shared by the
  replicator, AE, GC, heal, scrub, healing-service, segment-service, and
  repair paths (reviews #57/#59/#60 identify the sprawl at
  `node.rs:1005,1059,1112,1253,1291,2142` today). NOTE: this is the c1
  precondition. The final **one** unified store (single trait/impl via
  ADR-0032) is the store-unification epic's f3 end state — do NOT merge
  `DiskSegmentStore` and `DiskSegmentShardStore` here. See
  `refactoring/store-unification/f2-f3` and roadmap §4.
- Apply ADR-0031: no `config.storage.pools.is_empty()` branches in the
  builder; pools are mandatory. (The legacy removal *epic* `legacy-mode-removal`
  owns the deep delegacy of the store structs; sequence its f2 before
  store-unification f2 per roadmap §4.)
  **DISPOSITION (approved 2026-09-04, c1 plan review Q1):** c1 is a pure
  move — the two `pools.is_empty()` branches move with the code into
  `modules/storage.rs` unchanged and die there when legacy-mode-removal
  **f1** (boot enforcement: core `validate` + `PoolRegistry::from_config`
  refusal + branch deletion) lands in the same train, immediately after
  c1. c1's DoD "pools-mandatory boot refusal" test is delivered by that f1
  slice (c1's gate runs with f1 merged). See
  `legacy-mode-removal/README.md` landing order.
- The leave handler: c1 supersedes `NodeLeaveHandler` per review #34 —
  **delete the handler**. That deletion is the authoritative close for
  wave-0/1 f1 B1 (the fixed-76-byte-header bug, review #35), which
  wave-0/1 **deferred** to this deletion (DECISION 2026-09-04; see
  `review-wave-0-1/f1-correctness-bug-batch.md`). c1 must record B1's
  closure in both docs. **B1 CLOSED 2026-09-04**: the handler (struct,
  impls, §18 construction, `Node.leave_handler` field, leave-handler test
  groups) is deleted and `shutdown()` step 1 calls `membership.leave(None)`.
- Keep the accel probe, ring/routing construction, lifecycle registry,
  pools, sealer, event-WAL + checkpoint, I/O reader construction.

### Out of Scope
- Durability worker construction (c2), server/handler construction (c3),
  network/gRPC construction (c4) — later features in this epic.
- ADR-0017 scheduler — separate epic after this epic.
- Review #64 (B2) — fixed in wave-0/1 f1, not here. Review #35 (B1) —
  closed by THIS feature's `NodeLeaveHandler` deletion (deferred from
  wave-0/1, DECISION 2026-09-04; see the leave-handler note in Scope).
- Merging the two store impls — store-unification epic (ADR-0032).
- Any subsystem behavior change beyond store consolidation.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | New `modules/storage.rs`; `node.rs` slims by ~900 lines; `Node` struct fields replaced by `StorageModule` |

## Interface (Public API)

- `modules/storage.rs` is `pub(crate)`; only `Node::start` consumes it.
- `StorageModule` exposes the `Arc`s listed above (already what `start()`
  needs downstream for server/durability/network builders).

## Data Flow

```
Node::start
  → validate_config (unchanged)
  → §0–§5 inline (registry, paths, metadata, accel, ring, membership, pool)
  → StorageModule::build(&config, &paths, registry, metadata_store,
  │     accel, ring_cache, membership, pool)   // lifecycle registry + event
  │     //   WAL + ckpt, pools + sealer, two shared stores (one each type),
  │     //   replicator, disk reader + wrap, remap alias
  → §7–§11 durability/server material (still inline, consumes storage.*)
  → StorageModule::run_startup_recovery(&self)  // event fold + data-WAL +
  │     (recovery + startup replication pass)     //   compaction recovery
  → (later) DurabilityModule::build(durability components using storage.*)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` passes; `node.rs` reduced by
      the storage construction sections. (REVIEW 2026-09-04: verified —
      workspace build green; node.rs 4499 → 3474 lines, −1025 net. (Round 2
      2026-09-04: recount after the AE-marker reword at node.rs:720-727
      added 3 lines.))
- [x] **Tests:** existing node tests pass (boot, write/read, shutdown);
      new tests: builder returns a consistent `StorageModule` (exactly two
      shared store instances — one `DiskSegmentStore`, one
      `DiskSegmentShardStore`); pools-mandatory boot refusal without
      `[storage.pools]` (ADR-0031 — **delivered by legacy-mode-removal f1
      in the same landing train**, see Scope DISPOSITION).
      (REVIEW 2026-09-04: existing tests green — 68 lib incl. the two new
      `modules/storage.rs` tests, 96 integration across 28 files; two-store
      test present. The pools-mandatory boot-refusal test does NOT exist in
      c1: `modules/storage.rs:257,290` still carry the two
      `config.storage.pools.is_empty()` legacy branches verbatim. RESOLVED
      by the Scope DISPOSITION: f1 (boot enforcement) deletes them and
      ships the refusal test; c1's gate runs with f1 merged.)
- [x] **Docs:** `#![deny(missing_docs)]` passes; builder fields documented.
      (REVIEW 2026-09-04: verified — RUSTDOCFLAGS="-D warnings" doc build
      green; every `StorageModule` field + both methods documented.)
- [x] **ADR:** ADR-0031 satisfied — no legacy branches in the builder by
      the time the c1 gate closes: the branches move with the code into
      `modules/storage.rs` and are deleted by legacy-mode-removal f1 in the
      same landing train (Scope DISPOSITION); review #64 (B2) owned by
      wave-0/1 f1, not here; review #35 (B1) closed by this feature's
      `NodeLeaveHandler` deletion (deferred from wave-0/1, DECISION
      2026-09-04).
      (REVIEW 2026-09-04: B2 correctly absent (fixed in wave-0/1 f1); B1
      closure verified — handler, impls, §18 construction, `leave_handler`
      field, 4 test groups deleted; shutdown step 1 calls
      `membership.leave(None)` (node.rs:2384); closure recorded in c1 +
      f1 docs. "No legacy branches in the builder" met only via the f1
      deferral — the two `config.storage.pools.is_empty()` branches moved
      verbatim into `StorageModule::build` (modules/storage.rs:257, 290)
      pending f1; RESOLVED by the Scope DISPOSITION above.
      (Round 2 2026-09-04: disposition verified — c1 Scope + DoD bullets
      and legacy-mode-removal/README.md epic-DoD bullet (README:164-170)
      both record the same approved pure-move-then-f1/f2 train, and the
      branches still sit at modules/storage.rs:257,290 as documented.
      (Round 3 2026-09-04: round-2 residual cross-doc gaps re-verified
      FIXED in the working tree — legacy README Summary (README:48-51)
      and dependency edge (README:93-96) now read "c1 never *keeps* …
      c1 lands first as a pure move (the branches pass through verbatim),
      f1/f2 delete them in the same landing train", reconciled with the
      README epic-DoD DISPOSITION (README:169-175); f1-boot-enforcement.md
      re-pointed to post-c1 homes — Summary f1:30-31
      (modules/storage.rs:256-257 data_pools, :290 SealConfig.registry),
      In-Scope oceanfs-node section f1:96-104 (branch edits + comment
      blocks :251-255, f8 note above :290), §0-comment re-point f1:108-110
      (node.rs:375-386), Data Flow f1:148 (node.rs:342), Crate Impact row
      f1:130 (modules/storage.rs). All cited coordinates verified against
      the tree: node.rs:342 start, :375-383 §0 comment, :386 map; branch
      snippet at modules/storage.rs:256-257 matches code verbatim; None
      arm at :290 matches. No sentence in the legacy README or f1 doc
      still asserts c1 leaves zero legacy branches before f1/f2 merge.
       Residual LOW findings (round 3, in other epics' docs — not c1 DoD
       gates): f1:106 cited the "storage pool registry: {e}" map at
       node.rs:389-392, where the actual map_err is node.rs:385-387 (:386);
       legacy README:180's References anchor "node.rs:830" was stale
       post-c1 — the anchored "[review] a node without a data pool…"
       marker moved with the pure move to modules/storage.rs:240-242
       (branch :256-257). Both residuals subsequently FIXED in the docs
       (2026-09-04): f1-boot-enforcement.md now cites the map at
       node.rs:385-387 (map_err verified at node.rs:386); legacy
       README:180 now anchors the moved marker at modules/storage.rs:240.
       Re-verified against the working tree. No outstanding findings —
       reviewer PASS, iteration 3.)))
- [x] **Perf:** no new locks on hot paths; the 8→2 store consolidation
      removes duplicate fs layers (single read/write path per type).
      (REVIEW 2026-09-04: verified — exactly two store-construction sites
      in crates/oceanfs-node/src, both modules/storage.rs:397,403; all
      eight former sites now consume clones; stores are stateless resolver
      wrappers so sharing is behavior-neutral; no locks added in builder.)
- [x] **Integration:** `cargo test -p oceanfs-node --lib -- --test-threads=1`
      green; e2e write/read green. (REVIEW 2026-09-04: verified — lib 68
      passed; integration 96 passed incl. node_lifecycle,
      read_write_roundtrip, segment_replication, tiered_routing_e2e,
      e2e_single_node; clippy -D warnings clean; fmt clean.)

## Migration path

Pure move — no crate-boundary change, no public API break. `Node::start`
calls `StorageModule::build(...).await?` where the sections used to be.
Land green, then c2 proceeds.
