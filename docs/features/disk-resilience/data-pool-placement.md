---
feature: "Storage Pools: Multi-Data-Pool Segment Store"
epic: "disk-resilience"
status: done
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
  default 0 = the first data pool in pool mode / the legacy root in legacy
  mode — see Deviations for the corrected f2 id scheme).
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

- [x] **Code:** `cargo build --all-targets` in `oceanfs-core`,
      `oceanfs-storage`, `oceanfs-durability`, `oceanfs-node`
      (verified by review 2026-08-22: all four crates + `oceanfs` binary
      build clean; `cargo fmt --all -- --check` clean)
- [x] **Tests:** all listed green (stamp, resolve, unlink, event-WAL
      persistence, restart, legacy byte-for-byte, legacy checkpoint)
      (verified by re-review 2026-08-22: stamp (`seal_stamps_pool_id_and_writes_to_selected_pool_root`),
      resolve (`resolve_uses_pool_id_to_pick_root`), unlink
      (`shard_store_lists_and_unlinks_across_pool_roots`), event-WAL
      persistence (`seal_event_pool_id_roundtrips`,
      `seal_event_repacked_with_pool_id_roundtrips`,
      `legacy_seal_record_decodes_pool_id_zero`), restart
      (`restart_fold_preserves_pool_ids_and_resolution`), legacy regression
      (`legacy_node_roundtrip_on_data_dir_segments`), and the new legacy
      checkpoint test
      (`legacy_v2_checkpoint_decodes_with_pool_id_zero`) — all green;
      legacy-checkpoint fix verified: `CHECKPOINT_VERSION` bumped to 3
      (event_checkpoint.rs:80), `LEGACY_CHECKPOINT_VERSION = 2` (:85),
      `decode_snapshot` accepts both (:460) and decodes v2 blobs through
      the explicit 7-field `LegacySegmentMetadata` shape (:438-447, field
      order verified byte-identical to the committed pre-f5 struct via
      `git show HEAD`) mapping to pool_id 0 (:490-506); pre-f5 committed
      format confirmed v2 (`git show HEAD:...event_checkpoint.rs:75`);
      full suites re-run green (storage 351 lib + 45 doc, durability 241
      lib, node 38 lib + integration, all `--test-threads=1`; checkpoint
      suite 11/11, pool_id/wire-format/sealer/restart 8/8, e2e 2/2 in
      12.75s))
      <!-- REVIEW: verified 2026-08-22 re-review — all previously-failed
      checkpoint paths now pass. Remaining LOW: the module doc block in
      event_checkpoint.rs:22-23,45-48 still describes the snapshot
      version as "= 2" and does not mention v3/legacy-v2 — cosmetic doc
      drift only, no behavioral impact. -->
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
      (verified by review: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`
      clean on all four crates; `# Examples` on the new pub items
      (`resolve_pool_root`, `PoolIdResolver`, `select_from_pools`,
      `with_data_pools`, stores' `new`) and updated `SegmentMetadata`
      example)
- [x] **ADR:** ADR-0029 §D1 (pool-granular ownership in the data plane)
      satisfied — the segment→pool mapping is the ownership table, persisted
      via the event-WAL (the only durable segment-state path, ADR-0024/25)
      (verified by re-review 2026-08-22: `pool_id` rides `SealEvent` (flags
      bit 1 + 4-byte payload, length-discriminated decode, legacy records →
      0) and the fold stamps it on `SegmentMetadata` (lifecycle.rs:1447);
      the bincode checkpoint snapshot is now backward-compatible via
      CHECKPOINT_VERSION 3 + a v2 legacy reader (see Tests item); no
      Ceph-OSD / node-granular / probe-blind rejected alternatives
      re-implemented)
- [x] **Perf:** 1.3 (pool-id read is a field on an already-held struct — no
      extra lookup), 2.3/7.2 (resolve is a plain join over Arc<StoragePool>
      snapshots — the registry lookup runs once per segment per process)
      (verified by re-review 2026-08-22: 1.3 pre-sized candidate vec in
      `select_from_pools` (placement.rs:149) and GC's held-metadata fast
      path `delete_shards_with_pool` (no resolver call); 2.3 parking_lot
      used throughout; 7.2 `resolve_pool_root` is a lock-free find over the
      pool snapshot. The read path now caches per-segment pool ids:
      `DiskSegmentReader::pool_id_cache` (segment_reader.rs:157) — the
      sharded registry read lock + full `LifecycleEntry` clone runs ONCE
      per segment per process; subsequent reads hit the cache (legacy mode
      short-circuits with no lock at all, segment_reader.rs:294-295).
      Residual: a tiny uncontended parking_lot mutex guards the cache
      lookup per read — the expensive registry lock is gone from the hot
      path; the guard temporaries are statement-scoped (dropped before the
      resolver call and the second lock, :301-309), so the earlier
      self-deadlock (guard held across a re-lock) is structurally
      impossible; pool_id is immutable per segment (migration is Phase C),
      so cache staleness cannot occur; cache growth is bounded by the
      segment count, matching `last_source`/`verified_headers`.)
      <!-- REVIEW: verified 2026-08-22 re-review — pool_id cache in
      place, no residual locking hazard found (statement-scoped guards,
      no re-lock, parking_lot only). DoD claim amended from "no locks" to
      "registry lookup once per segment per process" (the cache lookup
      itself takes a short parking_lot mutex). e2e + node suite green,
      so the fixed deadlock does not regress. -->
- [x] **Integration:** a 2-data-pool node runs a small e2e write+read+
      delete cycle; GC runs; all segment I/O lands on pool roots; a restart
      resolves every segment to the same root; legacy node passes the same
      scenario on `data_dir`
      (verified by re-review 2026-08-22: `cargo test -p oceanfs-node --test
      data_pool_placement -- --test-threads=1` — 2 passed in 12.75s;
      pool-mode e2e `two_data_pool_node_roundtrip_gc` (renamed): PUT×6
      64 KiB → GET round-trip → `.dat` on pool roots only (legacy
      `data_dir/segments` has 0) → DELETE + GC (1s) unlinks every pool-root
      `.dat`; legacy e2e `legacy_node_roundtrip_on_data_dir_segments` same
      scenario on `data_dir/segments`. Restart coverage is the
      storage-level `restart_fold_preserves_pool_ids_and_resolution`
      (per this doc's own prescription). The module docstring now points to
      that storage-level test; the startup compaction-recovery sweep also
      uses the pool-aware shard store (`reaper_shard_store.delete_shards`,
      node.rs:1278 — was `remove_file(segment_dir/...)`). **LOW remains:
      the e2e test's own docstring (data_pool_placement.rs:115-118) still
      claims "a RESTART resolves everything again from the event-WAL
      records" while the body performs no restart.**)

## Deviations (accepted)

- **`pool_id` semantics corrected: 0 = the first data pool in pool mode,
  not the legacy root.** The original draft's "0 = legacy root, correct by
  construction: pool_id 0 only exists when no pools are configured" is
  wrong given the f2 id scheme: `PoolRegistry::from_config` assigns
  config-order indices 0..n (pool/mod.rs:739-740), so with pools
  configured the first data pool HAS id 0. The sealer therefore stamps the
  real pool id (never 0-as-legacy in pool mode; the no-eligible fallback
  is the first data pool, sealer.rs:349-359) and `resolve_pool_root`
  (pool/mod.rs:92-98) is a plain id lookup — the legacy dir is used only
  when no pools are configured (empty `data_pools`) or the id is unknown.
  The node passes an EMPTY pool list in legacy mode so the legacy layout
  is byte-for-byte (node.rs:671-672; the registry's implicit data pool is
  a runtime fallback, not a placement target — discovered via a runtime
  bug where legacy seals wrote to `data_dir` instead of
  `data_dir/segments`). Consequence: a node with pre-existing legacy
  segments that ADDS pools (config migration) resolves those old segments
  to pool 0's root, not the legacy dir — such migration is Phase C
  (drain/rebalance), out of scope here.
  <!-- REVIEW: correction verified justified — the draft's "0 = legacy,
  unambiguous" is incoherent with f2's committed 0-based config-order ids
  (a segment on the first data pool would be stamped 0 and misresolve to
  the legacy root). The corrected scheme is unambiguous within each mode:
  legacy mode → empty pools → legacy dir for every id; pool mode → real
  pool ids. The durability test `resolve_uses_pool_id_to_pick_root`
  (segment_store_impl.rs:150-152) has a stale comment saying "pool_id 0 →
  legacy dir" while its assertion (and the code) resolves pool_id 0 to the
  registered pool — fix the comment. -->
- **Legacy event-WAL records decode with `pool_id = 0`** (the first data
  pool in pool mode / legacy dir in legacy mode) rather than a migration
  pass. The SealEvent wire format is extended backward-compatibly: flags
  bit 1 (`SEAL_FLAG_POOL_ID`) + 4-byte pool_id appended only when non-zero;
  the four length variants (48/52/64/68) are flag+length discriminated,
  legacy records (48/64 bytes) decode to pool_id 0, and pool_id-0 records
  are byte-identical to the pre-f5 format.
- **Original spec referenced a "segments CF row"** — corrected during
  Phase-B preparation after code grounding: the segments CF was removed by
  ADR-0025 (store.rs:237-244); `pool_id` rides `SegmentMetadata` + event-WAL
  instead, which is the same durability guarantee with less machinery.
- **The bincode event-WAL checkpoint is backward-compatible via a version
  bump + legacy reader (implemented).** The pre-f5 checkpoint (bincode
  `SegmentMetadata` without `pool_id`) fails `bincode::deserialize`
  (`UnexpectedEof` — bincode 1.x does not honor `#[serde(default)]` for
  missing trailing fields). `CHECKPOINT_VERSION` is bumped to 3; v2
  snapshots decode through the explicit 7-field `LegacySegmentMetadata`
  shape → `pool_id 0` (event_checkpoint.rs:438-506), covered by
  `legacy_v2_checkpoint_decodes_with_pool_id_zero`. Without this, a pre-f5
  checkpoint would be discarded and the node would fold from `(0,0)` over a
  log truncated at the last checkpoint — silent registry state loss on
  upgrade.
- **f3 amendment: `PlacementPolicy::select_from_pools`.** f3's policy
  selected from the registry's pools by `StoragePoolId`; the sealer
  already holds the candidate snapshot, so the policy gained the
  slice-based `select_from_pools(&self, pools: &[Arc<StoragePool>]) ->
  Option<Arc<StoragePool>>` (placement.rs:147), which the sealer calls
  once per new segment (sealer.rs:348-359) — no registry round-trip in the
  seal path.
- **`SegmentShardStore::list_segment_files` now returns the owning pool**:
  `Vec<(SegmentId, i64, u32)>` (segment id, mtime, pool id;
  garbage_collector.rs:510). The orphan reaper needs the owning root to
  unlink segments that have no registry entry (the injected resolver
  cannot answer for unregistered segments). The new
  `delete_shards_with_pool(pool_id, segment_id)` fast path
  (garbage_collector.rs:592) serves callers that already hold the pool id
  (GC tombstone processing, compaction cleanup) without a resolver call.
- **In-process node restart is blocked by a pre-existing
  seal-worker-not-joined leak** (RocksDB lock held after shutdown) — a
  pre-f5 server defect unrelated to pool placement. Restart coverage is
  therefore at the storage level: `restart_fold_preserves_pool_ids_and_resolution`
  folds a fresh registry from the event WAL, per this doc's own
  prescription (data_pool_placement.rs:115-121).
- **Compaction/heal writes for not-yet-registered segments resolve to
  pool 0** (the first data pool in pool mode): the resolver returns `None`
  for a segment whose seal never became durable, `unwrap_or(0)` lands on
  pool 0's root when pools are configured (segment_store_impl.rs:56,
  :150-153), and the compaction `cleanup_reserved_new` unlink matches with
  `delete_shards_with_pool(0, …)` — write and unlink are consistent
  (segment_compactor.rs:402-407).
