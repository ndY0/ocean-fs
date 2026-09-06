---
feature: "Metadata-Pool Loss Recovery (Fresh Store + Range Rebuild)"
epic: "disk-resilience-healing"
status: proposed
priority: high
owner: ""
dependencies: ["failure-state-machine", "re-replication-worker"]
adr: [0029, 0035]
perf: [1.3, 7.1]
created: 2026-08-22
updated: 2026-09-06
---

# Metadata-Pool Loss Recovery (Fresh Store + Range Rebuild)

## Summary

ADR-0029 §D7's second durability-critical path, updated to the validated
g8 decisions (D4-D6). Metadata-pool loss is a **catastrophic local event,
not cluster data loss**: the node serves nothing, peers route around it
**immediately** (no SWIM suspicion timeout), and the node rebuilds a fresh
objects CF **and** deletions CF from a live peer over its owned ring
ranges, then rejoins serving.

**Key correctness fact (kept from the original, still code-grounded): NO
re-replication is needed.** Segment data files (`.dat`) live on the data
pools and the segment lifecycle registry lives in the event WAL on the wal
pool (`modules/storage.rs:338-361`, `pool_paths.rs:76-83`) — both intact
when only the metadata pool dies. What is lost is exactly the RocksDB
`objects` + `deletions` column families (`metadata/store.rs:309-316` opens
these two CFs only). The rebuild restores the index over data the node
already holds; peers must route around the node (it cannot serve) but must
NOT re-replicate its segments (they are not lost — re-replication would
duplicate data and waste capacity). g8 shares ADR-0035's peer-fetch
substrate but does **not** consume the replicated segment lifecycle state:
segment lifecycle state is untouched by this feature.

The residual RF=2 window is explicit (kept): if the live peer serving the
range rows is also down AND this node's metadata is gone, reads of those
objects cannot be served from this node (its index is gone) — but the data
is not destroyed; the window closes when either node recovers. True data
loss requires the data pools themselves to die (Phase B data-pool path).

## Scope

### In Scope

- Unavailability signal — **peer side (new wire surface; audit C2)**:
  peers cannot route around a metadata-dead node today. Locally,
  `Node::node_unavailable()` (`node.rs:571-573`) is derived from
  `PoolRegistry::node_serves_requests()` (`pool/mod.rs:1167-1171`) and
  already gates reads/writes with a retryable 503
  (`read/coordinator.rs:549`, `write/coordinator.rs:469` — audit M5: the
  local half is done). But `NodeManifest` has only per-pool fields
  (`membership/manifest.rs:151-158`), peer filters count data-pool health
  only (`routing_cache.rs:280-339`, `can_accept_writes`), and SWIM keeps
  the node Alive (probes are socket-only, ADR-0028) — so a metadata-dead
  node with healthy data pools remains a valid read candidate and write
  target indefinitely. g8 adds:
  - a **NEW node-level `node_unavailable` field to `NodeManifest`**
    (Rust struct + proto; wire surface below);
  - a **peer routing rule**: read candidates and write targets exclude a
    node whose manifest sets `node_unavailable`, regardless of data-pool
    health — peers route around immediately, with no SWIM suspicion
    timeout;
  - node wiring: set the flag when the metadata pool goes Dead, clear it
    after the rebuild completes. The manifest change bumps the owning
    entry's `version` (ADR-0028 D3) and disseminates on the existing
    gossip path. No g3 announcement is sent: the node's segments are
    intact, so there is nothing to re-replicate.
  - Local behavior while unavailable (already present, exercised here):
    S3 API + read path reject with 503 (read/coordinator.rs:549,
    write/coordinator.rs:469).
- Fresh-store rebuild (replacement/remount):
  - `RocksDbMetadataStore::open` creates a fresh DB on a missing/empty dir
    (`create_if_missing`, `metadata/store.rs:273-319`). Today the store is
    opened exactly once at boot (`node.rs:274-278`) — there is no runtime
    reopen, and no test covers "root wiped while the process is down,
    reopen yields a fresh DB" or a live-remount reopen (audit L1). This
    feature adds the runtime reopen path and those tests.
  - **Objects + deletions rebuild (g8 D5/D6)**:
    - new owned-ring-range enumeration: compute the ranges of vnode
      positions the local node owns (new abstraction — today `VnodeRange`
      exists only as the degenerate add/remove return value,
      `core/types/node.rs:83-96`, `routing/ring.rs:89-134`, and what a
      node owns is per-segment `storage_locations`, not object-key ring
      arcs — audit H5);
    - for each owned range, `ListObjectsInRange(range)` on a live peer
      streams **both `ObjectRow`s AND `DeletionRow`s** (g8 D6): deletions
      must be rebuilt so the recovering node's byte-accounting reaper/GC
      are not blinded — `orphan_reaper.rs:41-62` and
      `liveness_tracker.rs:11-49` read only the local deletions CF, and
      an empty deletions CF makes `dead(0) >= total` undecidable so
      reclamation leaks rather than merely delays
      (`orphan_reaper.rs:181-185`); audit M3;
    - `ObjectRow` = bucket + key + everything the objects CF stores (the
      `ObjectMetadata` row: chunks + inline payload + size + BLAKE3 + HLC,
      `core/types/metadata.rs:83-115`). Inline objects' payloads ride the
      row bytes — no data movement accompanies the rebuild;
    - `DeletionRow` = bucket + key + the deletions-CF record
      (`DeadChunkRecord`: `kind` = Tombstone | Supersede, `captured_at`,
      `hlc`, dead chunks — `core/types/metadata.rs:325-339`), restoring
      the TTL-aged accounting feed
      (`collect_aged_dead_chunk_records`, `liveness_tracker.rs:11-49`);
    - the peer streams rows whose ring hash falls in the range; the
      recovering node folds them into its fresh CFs idempotently on
      retry. The chunk refs point at segments the node ALREADY holds (its
      own `.dat` files are intact), so NO data movement accompanies the
      rebuild and segment lifecycle state is never written.
  - After the rebuild: clear `node_unavailable`; the node rejoins the
    read/write path.
- Metrics (fresh names — audit M4: none are registered today):
  - `oceanfs_metadata_unavailable_seconds` (gauge) — local time since the
    metadata pool went Dead;
  - `oceanfs_metadata_rebuild_objects_total` (counter) — ObjectRows
    folded;
  - `oceanfs_metadata_rebuild_deletions_total` (counter) — DeletionRows
    folded;
  - `oceanfs_metadata_rebuild_range_errors_total` (counter) — failed /
    retried range streams.
  - Peers observe the excluded population through the routing filters
    consuming the `node_unavailable` field (g6's existing routing metrics
    are the signal; no new peer metric required).
- Tests:
  - unit: owned-ring-range enumeration (vnode positions for the local node
    cover every key the ring assigns it; ranges disjoint across the
    ring);
  - unit: `NodeManifest` `node_unavailable` round-trips through proto;
    the routing rule excludes a manifest with the flag even when it
    reports healthy data pools (read candidate AND write target);
  - unit: rebuild fold (streamed ObjectRow + DeletionRow → CF writes,
    idempotent on retry);
  - unit: byte-accounting not blinded (a fold without DeletionRows leaves
    a fully-dead segment un-reapable; with the rows restored, the aged
    record is reclaimed);
  - integration (local 3-node): kill the metadata pool on A → A's
    manifest sets `node_unavailable` → peers route reads/writes around A
    immediately (no SWIM timeout; asserted on routing decisions, not
    suspicion); NO re-replication traffic observed (the re-replication
    metric stays flat); A reopens a fresh store on the replaced root; A
    rebuilds objects + deletions from a peer over its owned ranges; all
    pre-kill keys are readable from A again AND a pre-kill delete's dead
    bytes are reclaimed by A's own reaper post-rebuild; cluster data
    intact.

### Out of Scope

- WAL-pool loss recovery (g7) — consumes the replicated lifecycle state
  (ADR-0035 D2); g8 does not touch segment lifecycle state.
- Re-replication of the node's segments (explicitly NOT needed — data
  pools and the registry are intact; peers only route around).
- Tombstone reconstruction by local invention — deletions rows are pulled
  from a peer over the same range stream (g8 D6), never synthesized.
- Segment self-description (Phase C, deferred mitigation).
- Registry / event-WAL / checkpoint recovery (the wal pool survives).

## Crate Impact

| Crate | Change |
|---|---|
| `proto/oceanfs/healing.proto` | (to add) `ListObjectsInRange` RPC + `ObjectRow` / `DeletionRow` messages |
| `proto/oceanfs/membership.proto` | (to add) `NodeManifest.node_unavailable` field |
| `oceanfs-membership` | `NodeManifest` gains `node_unavailable` (`from_pools`, `to_proto`/`from_proto`) |
| `oceanfs-node` | Manifest set/clear on metadata status; owned-ring-range enumeration; rebuild fold; routing rule consumes the flag |
| `oceanfs-durability` | Healing-service `ListObjectsInRange` handler (streams objects + deletions rows) |
| `oceanfs-storage` | Runtime metadata-store reopen on a replaced root (currently boot-only open, `node.rs:274-278`) |

## Interface (Public API)

- `NodeManifest::with_node_unavailable(bool)` / `node_unavailable()` — the
  node-level field (membership crate).
- `pub fn owned_vnode_ranges(ring: &RingCache, self_id: &NodeId) ->
  Vec<VnodeRange>` — the new owned-range enumeration (node-side helper).
  Note: this deliberately does NOT reuse "ObjectLookup"-flavored naming —
  `ObjectLookup` is the compaction-recovery point-read trait
  (`gc/compaction_recovery.rs:76-91`), unrelated to a peer-driven rebuild
  (audit M2).
- `pub fn rebuild_metadata_from_peer(peer, ranges, metadata_store) ->
  Result<u64>` — the fold.
- `pub struct MetadataRecoveryOutcome { rebuilt_objects: u64,
  rebuilt_deletions: u64, unavailable_secs: f64 }`.

New wire surface (both marked **to add**):

- `proto/oceanfs/healing.proto` — the healing service surface is fixed
  today (`healing.proto:67-108`); no object-listing RPC exists (audit H4,
  confirmed gap):
  - `rpc ListObjectsInRange(ObjectRangeRequest) returns (stream
    MetadataRow)` — server-streaming. `ObjectRangeRequest` = the caller's
    owned range (32-byte inclusive `start` / exclusive `end` vnode
    positions, matching the `VnodeRange` type). `MetadataRow` = oneof
    `ObjectRow` | `DeletionRow`; the stream carries BOTH live object rows
    AND deletions-CF records over the range (g8 D5/D6).
  - `ObjectRow` = bucket id + object key + the objects-CF row (chunk refs,
    inline payload, size, BLAKE3 hash, HLC, created_at).
  - `DeletionRow` = bucket id + object key + the deletions-CF record
    (`kind` Tombstone | Supersede, `captured_at`, `hlc`, dead chunks).
- `proto/oceanfs/membership.proto` — the `NodeManifest` message today
  carries only `incarnation` + `pools` (`membership.proto:67-70`); add a
  node-level field (e.g. `bool node_unavailable = 3;`) mirroring the Rust
  struct at `membership/manifest.rs:151-158`. Peers treat it as a hard
  read/write routing exclusion independent of per-pool status.

## Data Flow

```
metadata pool Dead ──▶ node_unavailable = true in NodeManifest (NEW field)
   └─ peers: routing rule excludes the node immediately (no SWIM timeout)
   └─ local: S3/read 503 via node_serves_requests()
        (read/coordinator.rs:549, write/coordinator.rs:469); NO re-replication (segments intact)
replacement/remount ──▶ RocksDbMetadataStore::open on the replaced root
        (create_if_missing; runtime reopen — NEW, currently boot-only)
   └─ owned_vnode_ranges(ring) ──▶ ListObjectsInRange(live peer)
        ──▶ stream MetadataRow
             ├─ ObjectRow   ──▶ fold into the objects CF (idempotent; chunk refs → intact local .dat)
             └─ DeletionRow ──▶ fold into the deletions CF (reaper/GC byte accounting not blinded)
   └─ rebuild done ──▶ node_unavailable = false ──▶ serve again
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` in `oceanfs-node`,
      `oceanfs-durability`, `oceanfs-membership`, `oceanfs-storage`
      (+ proto regen)
- [ ] **Tests:** all listed green (ranges, manifest flag + routing rule,
      rebuild fold, byte-accounting, 3-node integration)
- [ ] **Docs:** `# Examples` on pub items; rustdoc clean
- [ ] **ADR:** ADR-0029 §D7 (metadata loss: catastrophic locally,
      cluster-safe while RF healthy) + ADR-0035 D2 boundary (g8 pulls
      object/deletion rows from a peer but never touches segment lifecycle
      state) satisfied; the corrections below documented
- [ ] **Perf:** 1.3 (range vec + row batching pre-sized), 7.1 (rebuild is
      a one-time recovery path; the range stream never buffers a whole
      range in memory)
- [ ] **Integration:** the epic's metadata-pool-kill DoD — node serves
      nothing, peers route around without SWIM timeout (node-level
      `node_unavailable` flag), fresh store rebuilds objects + deletions
      from a peer, node rejoins, zero cluster data loss, zero
      re-replication traffic

## Deviations (accepted)

- **`node_unavailable` is a NEW manifest field, not an existing one.** The
  original (2026-08-22) spec asserted "the node sets `node_unavailable` in
  its manifest (f6)" as if the field existed. It does not: `NodeManifest`
  carries only per-pool fields (`membership/manifest.rs:151-158`) and peer
  routing counts data-pool health only (`routing_cache.rs:280-339`) —
  audit C2/M5. The manifest field + peer routing rule are new wire
  surface specified here.
- **Deletions rows ARE rebuilt (g8 D6).** The original spec accepted
  tombstone loss ("orphan reaper covers"). Audit M3 shows the reaper/GC
  are byte-accounting consumers of the local deletions CF
  (`orphan_reaper.rs:41-62`, `liveness_tracker.rs:11-49`): with an empty
  deletions CF a fully-dead segment never reaches `dead >= total`
  (`orphan_reaper.rs:181-185`) and reclamation leaks. The rebuild stream
  now carries ObjectRows AND DeletionRows (tombstones + supersedes),
  restoring the accounting feed.
- **Range enumeration is new owned-ring machinery.** `VnodeRange` is only
  a degenerate add/remove return type (`types/node.rs:83-96`,
  `ring.rs:89-134`); routing is per-key `Ring::lookup` and ownership is
  per-segment `storage_locations`. The owned-range enumeration over vnode
  positions is a new abstraction (audit H5).
- **`ListObjectsInRange` is confirmed new wire surface.** No object-listing
  RPC or handler exists (the healing surface is fixed,
  `healing.proto:67-108`); `WalEntry` carries no bucket/key, so the WAL
  cannot rebuild objects (audit H4).
- **Runtime store reopen is new.** The fresh-open primitive
  (`create_if_missing`, `metadata/store.rs:273-319`) exists, but the store
  is opened once at boot (`node.rs:274-278`); wiped-root reopen and
  live-remount reopen are untested (audit L1) — this feature adds them.
- **Corrected from the brainstorm: no re-replication on metadata loss.**
  Kept from the original: peers only route around the node while it
  rebuilds its index; the earlier brainstorm "re-replicate the node's
  ranges onto healthy targets" wording is superseded.
- **Old code anchors replaced throughout.** `node.rs:480/558-564`,
  `store.rs:169-182/201-247` and `wal/entry.rs:52-79` referenced
  pre-composition-root locations; all anchors are now the module/audit
  locations cited inline (audit M1).
