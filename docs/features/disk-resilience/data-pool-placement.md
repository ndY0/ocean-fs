---
feature: "Storage Pools: Multi-Data-Pool Segment Store"
epic: "disk-resilience"
status: proposed
priority: high
owner: ""
dependencies: ["pool-runtime", "placement-policy"]
adr: [0029]
perf: [1.3, 2.3, 7.2]
created: 2026-08-22
updated: 2026-08-22
---

# Storage Pools: Multi-Data-Pool Segment Store

## Summary

The core storage-engineering feature of Phase A: sealed segments spread
across the node's `data` pools. The single-root assumption that pervades the
segment path today — `SealConfig.data_dir` (node.rs:644-671),
`DiskSegmentStore::new(segment_dir)` and `DiskSegmentShardStore::new(segment_dir)`
for GC (node.rs:754-763) — becomes a pool-aware store: segment files live
under one of several data-pool roots, and the segment→pool mapping persists
on the segment's own metadata record.

**Corrected persistence model (code-grounded):** there is NO `segments` CF in
RocksDB — ADR-0025 Decision 3 removed it (`store.rs:237-244`; only `objects`
and `deletions` CFs exist). Segment lifecycle state lives in
`SegmentMetadata` (`oceanfs-core::types::metadata.rs:139`) persisted via the
**event WAL + checkpoint + lifecycle registry** (`node.rs:1159-1181`
rebuild path). Therefore `pool_id` rides `SegmentMetadata` and its existing
durable path — nothing new to persist, no RocksDB schema change.

## Scope

### In Scope

- `oceanfs-core`:
  - `SegmentMetadata` gains `pool_id: u32` (serde default 0). Legacy records
    deserialize with 0 = the legacy root; no migration needed. The field
    flows through the existing event-WAL serialization automatically (the
    event log already serializes `SegmentMetadata`).
- `oceanfs-storage`:
  - `SealConfig` gains `data_pools: Vec<Arc<StoragePool>>` (replaces the
    single `data_dir` for segment writes when pools exist; legacy mode keeps
    `data_dir` semantics). The sealer consults `PlacementPolicy` (f3) to
    choose the target pool **once per new segment** (at segment creation,
    not per blob append) and stamps the segment's `pool_id` on the
    `SegmentMetadata` it writes through the lifecycle coordinator (the
    reserve → event append → seal path, ADR-0025/ADR-0024).
  - **Event-WAL recovery** (`wal/replay.rs` replay + `rebuild_with_data_wal`,
    node.rs:1170-1181): recovered/rebuild `SegmentMetadata` keeps `pool_id`
    as serialized (legacy events → 0). The replay fold must not drop the
    field.
- `oceanfs-durability` (the two store constructors observed at
  node.rs:754-763):
  - `DiskSegmentStore::new(data_pools: Vec<Arc<StoragePool>>, legacy_dir:
    PathBuf, pool_id_for: Arc<dyn Fn(&SegmentId) -> Option<u32>>)` and
    `DiskSegmentShardStore::new(...)` — a pool-aware path resolver:
    `resolve(segment_id) -> PathBuf` looks up `pool_id` via the injected
    resolver, then joins the pool root (legacy mode: empty pool list →
    `legacy_dir` for every `segment_id`, today's behavior byte-for-byte).
    **The `SegmentDataStore` trait signature is unchanged**
    (`read/write_segment_data(segment_id)`, anti_entropy/merkle_tree.rs:35)
    — the store resolves pool_id internally via the resolver, which the
    node backs with the lifecycle registry's `SegmentMetadata.pool_id`.
  - **GC unlink/delete** (`request_delete`/compaction) holds
    `SegmentMetadata` (which carries `pool_id`) and passes the pool_id
    alongside `SegmentId` into the store's unlink call — the store keeps a
    `resolve_with_pool(pool_id, segment_id)` fast path for callers that
    already hold the metadata (no resolver call).
  - The stores never touch RocksDB — the resolver is the only metadata
    dependency (keeps the durability → storage dependency one-directional
    and the store hot-path free of RocksDB lookups).
- Anti-entropy + healing already read through `SegmentStore`
  (`SegmentDataStore` trait, `anti_entropy/merkle_tree.rs:35`); they inherit
  multi-root resolution without signature changes (their `read/write_segment_data`
  goes through the pool-aware store).
- Node wiring (node.rs:644-671, 754-763): pass the registry's data pools +
  legacy segments dir into the sealer and the two GC stores.
- Tests:
  - unit (core): `SegmentMetadata` serde round-trip with `pool_id` present
    and default-0 legacy;
  - unit (storage): sealer stamps `pool_id` per segment; placement policy
    drives the target (2 data pools, weight split); event-WAL round-trip
    preserves `pool_id`;
  - unit (durability): `resolve()` returns legacy dir for pool_id 0 and the
    right pool root for pool_id 1..n; unlink removes from the correct root;
  - restart: seal segments onto 2 pools → re-open store + registry → all
    segments still resolve (the event-WAL records carry `pool_id`; nothing
    is rebuilt — this test proves persistence, not reconstruction);
  - legacy regression: no pools → identical layout + behavior as before.

### Out of Scope

- Health-aware placement (Degraded/Dead pool exclusion at write time) —
  Phase B; the policy's eligibility filter is already stubbed in f3.
- Re-replication / repair of segments on a dead pool — Phase B.
- Drain/rebalance of segments across pools — Phase C.
- WAL-pool-loss registry rebuild from on-disk `.dat` scan — Phase B (B7).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | `SegmentMetadata::pool_id` field |
| `oceanfs-storage` | `SealConfig` pool-aware; sealer target selection; event-WAL recovery preserves `pool_id` |
| `oceanfs-durability` | `DiskSegmentStore`/`DiskSegmentShardStore` pool-aware path resolution |
| `oceanfs-node` | wire pools into sealer + GC stores |

## Interface (Public API)

- `SegmentMetadata::pool_id: u32` — durable segment→pool mapping (serde
  default 0 = legacy root).
- `SealConfig::data_pools: Vec<Arc<StoragePool>>` — placement targets.
- `DiskSegmentStore::new(data_pools, legacy_dir, pool_id_for: Arc<dyn Fn(&SegmentId) -> Option<u32>>)`.
- `DiskSegmentShardStore::new(data_pools, legacy_dir, pool_id_for)`.
- Stores resolve by `segment_id` internally (trait unchanged); GC passes
  the pool_id it already holds via `resolve_with_pool(pool_id, segment_id)`.

## Data Flow

```
sealer: new segment ──▶ PlacementPolicy.select_data_pool (once per segment)
   ├─ file written under pool root
   └─ SegmentMetadata{..., pool_id} ──▶ lifecycle reserve/event append (durable)
reader / AE / heal ──▶ store.resolve(pool_id, segment_id) ──▶ pool root
GC (tombstone processing) ──▶ passes pool_id from held SegmentMetadata
   └─ store.unlink(pool_id, segment_id) ──▶ pool root
restart ──▶ event-WAL recovery fold ──▶ registry entries keep pool_id
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` in `oceanfs-core`,
      `oceanfs-storage`, `oceanfs-durability`, `oceanfs-node`
- [ ] **Tests:** all listed green (stamp, resolve, unlink, event-WAL
      persistence, restart, legacy byte-for-byte)
- [ ] **Docs:** `# Examples` on pub items; rustdoc clean
- [ ] **ADR:** ADR-0029 §D1 (pool-granular ownership in the data plane)
      satisfied — the segment→pool mapping is the ownership table, persisted
      via the event-WAL (the only durable segment-state path, ADR-0024/25)
- [ ] **Perf:** 1.3 (pool-id read is a field on an already-held struct — no
      extra lookup), 2.3/7.2 (resolve is a plain join over Arc<StoragePool>
      snapshots — no locks in the read/unlink paths)
- [ ] **Integration:** a 2-data-pool node runs a small e2e write+read+
      delete cycle; GC runs; all segment I/O lands on pool roots; a restart
      resolves every segment to the same root; legacy node passes the same
      scenario on `data_dir`

## Deviations (accepted)

- **Legacy records default `pool_id = 0`** (the legacy root) rather than a
  migration pass. Correct by construction: pool_id 0 only exists when no
  pools are configured, so resolution is unambiguous.
- **Original spec referenced a "segments CF row"** — corrected during
  Phase-B preparation after code grounding: the segments CF was removed by
  ADR-0025 (store.rs:237-244); `pool_id` rides `SegmentMetadata` + event-WAL
  instead, which is the same durability guarantee with less machinery.
