---
feature: "c1: Extract StorageModule Builder from Node::start"
epic: "refactoring/composition-root-decomposition"
status: proposed
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
    pub registry: Arc<oceanfs_storage::PoolRegistry>,
    pub lifecycle: Arc<SegmentLifecycleCoordinator>,
    pub lifecycle_registry: Arc<SegmentLifecycleRegistry>,
    pub event_wal: Arc<EventWal>,
    pub event_checkpoint: Arc<EventCheckpoint>,
    pub sealer: Arc<SegmentSealer>,
    pub segment_reader: Arc<dyn oceanfs_storage::io::SegmentReader>,
    pub data_store: Arc<dyn oceanfs_durability::SegmentDataStore>, // ONE per type
    pub shard_store: Arc<dyn oceanfs_durability::SegmentShardStore>, // ONE per type (fused later in ADR-0032)
    pub segment_replicator: Arc<crate::segment_replicator::SegmentReplicator>,
    pub wal_writer: Arc<WalWriter>,
    pub wal_reader: WalReader,
    pub metadata_store: Arc<RocksDbMetadataStore>,
    pub remap_alias: Arc<SegmentRemapAlias>,
    pub io_observer: Arc<IoObserver>,
    pub accel: Arc<AccelDispatcher>,
    pub pool_registry_for_server: Arc<PoolRegistry>,
}

impl StorageModule {
    pub async fn build(config: &NodeConfig, paths: &PoolPaths) -> Result<Self, String>;
    pub async fn run_startup_recovery(&self) -> Result<(), String>; // node.rs 6a/6b sections
}
```

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
- The leave handler: c1 supersedes `NodeLeaveHandler` per review #34 —
  **delete the handler**. That deletion is the authoritative close for
  wave-0/1 f1 B1 (the fixed-76-byte-header bug, review #35), which
  wave-0/1 **deferred** to this deletion (DECISION 2026-09-04; see
  `review-wave-0-1/f1-correctness-bug-batch.md`). c1 must record B1's
  closure in both docs.
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
  → StorageModule::build(&config, &paths)      // pool registry, metadata,
  │     metadata → RocksDbMetadataStore         //   lifecycle, pools, sealer,
  │     lifecycle registry + event WAL + ckpt    //   data_store + shard_store
  │     pools + sealer + two shared stores          //   (one each type)
  → StorageModule::run_startup_recovery()       // event fold + data-WAL +
  │     (recovery + startup replication pass)     //   compaction recovery
  → (later) DurabilityModule::build(durability components using storage.*)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` passes; `node.rs` reduced by
      the storage construction sections.
- [ ] **Tests:** existing node tests pass (boot, write/read, shutdown);
      new tests: builder returns a consistent `StorageModule` (exactly two
      shared store instances — one `DiskSegmentStore`, one
      `DiskSegmentShardStore`), pools-mandatory boot refusal without
      `[storage.pools]` (ADR-0031).
- [ ] **Docs:** `#![deny(missing_docs)]` passes; builder fields documented.
- [ ] **ADR:** ADR-0031 satisfied (no legacy branches in the builder);
      review #64 (B2) owned by wave-0/1 f1, not here; review #35 (B1)
      closed by this feature's `NodeLeaveHandler` deletion (deferred from
      wave-0/1, DECISION 2026-09-04).
- [ ] **Perf:** no new locks on hot paths; the 8→2 store consolidation
      removes duplicate fs layers (single read/write path per type).
- [ ] **Integration:** `cargo test -p oceanfs-node --lib -- --test-threads=1`
      green; e2e write/read green.

## Migration path

Pure move — no crate-boundary change, no public API break. `Node::start`
calls `StorageModule::build(...).await?` where the sections used to be.
Land green, then c2 proceeds.
