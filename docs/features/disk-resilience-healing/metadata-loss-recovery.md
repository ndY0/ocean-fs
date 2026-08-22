---
feature: "Metadata-Pool Loss Recovery (Fresh Store + Rebuild)"
epic: "disk-resilience-healing"
status: proposed
priority: high
owner: ""
dependencies: ["failure-state-machine", "re-replication-worker"]
adr: [0029]
perf: [1.3, 7.1]
created: 2026-08-22
updated: 2026-08-22
---

# Metadata-Pool Loss Recovery (Fresh Store + Rebuild)

## Summary

ADR-0029 §D7's second durability-critical path: metadata-pool loss is a
**catastrophic local event, not cluster data loss** — the node serves
nothing, peers route around it WITHOUT SWIM suspicion timeouts (the
manifest's metadata-pool status drives it), and the node rebuilds a fresh
objects CF from peers, then rejoins serving.

**Key correctness fact (code-grounded): NO re-replication is needed on
metadata-pool loss.** Segment data files (`.dat`) live on the data pools
(Phase A f5), and the segment lifecycle registry survives in the event-WAL
on the WAL pool (Phase A f4) — both intact when only the metadata pool
dies. What is lost is only the objects CF (key → chunk refs) and
tombstones. The node's copies of every object's data are still on its own
disks; the rebuild restores the index over them. Peers must route around
the node (it cannot serve) but must NOT re-replicate its segments (they
are not lost — re-replicating would duplicate data and waste capacity).

The residual RF=2 window is explicit: if the peer holding the second copy
is also down AND this node's metadata is gone, reads of those objects
cannot be served from this node (its index is gone) — but the data is not
destroyed; the window closes when either node recovers. True data loss
requires the data pools themselves to die (Phase B data-pool path).

## Scope

### In Scope

- `oceanfs-node` metadata-dead handling:
  - **Unavailability signal (pinned)**: on metadata-pool Dead (g2), the
    node sets `node_unavailable` in its manifest (f6). Peers' g6 routing
    skips it immediately (no SWIM suspicion timeout — probes are
    socket-only, ADR-0028, so SWIM alone would keep it Alive forever).
    No g3 announcement is sent: the node's segments are intact, so there
    is nothing to re-replicate.
  - Local behavior while unavailable: S3 API + read path reject with 503
    (g6 enforces; the node cannot serve anything — its objects CF is gone).
- Fresh-store rebuild (the recovery path, on replacement/remount):
  - `RocksDbMetadataStore::open` already creates a fresh DB on an empty
    dir (`create_if_missing`, store.rs:201-247) — the node reopens onto
    the replaced root with NO special migration.
  - **Objects rebuild (pinned)**: the node enumerates its owned ring
    ranges (ring cache, node.rs:480) and for each range asks a live peer
    to stream the object rows it holds in that range — **new data-plane
    RPC `ListObjectsInRange(range) -> stream<ObjectRow>`** on the healing
    service (the code-grounding gap: no object-listing RPC exists today;
    `WalEntry` carries no keys, wal/entry.rs:52-79, so the WAL cannot
    rebuild objects either).
    - `ObjectRow` = bucket + key + chunk refs + HLC — everything the
      objects CF stores;
    - the peer streams rows whose ring hash falls in the range (it holds
      replicas for its own vnodes; the recovering node takes the rows for
      ranges IT owns);
    - the node writes the rows into its fresh objects CF — the chunk refs
      point at segments the node ALREADY holds (its own `.dat` files are
      intact), so NO data movement accompanies the rebuild;
    - tombstones (deletions CF) are NOT rebuilt: lost tombstones degrade
      GC reclamation (dead bytes unreachable → orphan reaper covers,
      store.rs:169-182) — a documented, accepted loss (correctness intact,
      space reclamation delayed).
  - After rebuild: `node_unavailable` clears; the node rejoins the
    read/write path.
- Metrics: `oceanfs_metadata_rebuild_objects_total`, `oceanfs_metadata_unavailable_seconds`.
- Tests:
  - unit: range enumeration (ring vnode ranges for the local node);
  - unit: rebuild fold (streamed ObjectRows → objects CF writes,
    idempotent on retry);
  - integration (local 3-node): kill the metadata pool on A → peers route
    reads/writes around A immediately (no SWIM timeout); NO
    re-replication traffic observed (`oceanfs_ranges_re_replicated_total`
    stays flat); A rebuilds from a peer; all keys readable from A again;
    cluster data intact.

### Out of Scope

- WAL-pool loss recovery (g7).
- Tombstone reconstruction (accepted loss, orphan reaper covers).
- Segment self-description (Phase C, deferred mitigation).
- Re-replication of the node's segments (explicitly NOT needed — data
  pools and the registry are intact; peers only route around).

## Crate Impact

| Crate | Change |
|---|---|
| `proto/oceanfs/healing.proto` | `ListObjectsInRange` RPC + `ObjectRow` |
| `oceanfs-durability` | Healing service range-listing handler |
| `oceanfs-node` | Unavailability flow, fresh-store reopen, rebuild fold |

## Interface (Public API)

- `pub fn owned_ranges(ring: &RingCache, self_id: &NodeId) -> Vec<VnodeRange>`
  (node-side helper).
- `pub fn rebuild_objects_from_peer(peer, ranges, metadata_store) -> Result<u64>`
  — the fold.
- `pub struct MetadataRecoveryOutcome { rebuilt_objects: u64, unavailable_secs: f64 }`.

## Data Flow

```
metadata pool Dead (g2) ──▶ node_unavailable in manifest (f6)
   └─ peers: g6 routing skips the node immediately (no SWIM timeout)
   └─ local: S3/read 503 (g6); NO re-replication (segments intact)
replacement/remount ──▶ RocksDbMetadataStore::open (fresh, create_if_missing)
   └─ owned_ranges(ring) ──▶ ListObjectsInRange(peer) ──▶ ObjectRow stream
        └─ fold into objects CF (idempotent; chunk refs point at local .dat)
   └─ rebuild done ──▶ node_unavailable clears ──▶ serve again
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` in `oceanfs-node`,
      `oceanfs-durability`, `oceanfs-network` (proto regen)
- [ ] **Tests:** all listed green (ranges, rebuild fold, 3-node
      integration)
- [ ] **Docs:** `# Examples` on pub items; rustdoc clean
- [ ] **ADR:** ADR-0029 §D7 (metadata loss: catastrophic locally,
      cluster-safe while RF healthy) satisfied; the "no re-replication"
      correction documented below
- [ ] **Perf:** 1.3 (range vec + row batching pre-sized), 7.1 (rebuild is
      a one-time recovery path)
- [ ] **Integration:** the epic's metadata-pool-kill DoD — node serves
      nothing, peers route around without SWIM timeout, fresh store
      rebuilds from peers, node rejoins, zero cluster data loss, zero
      re-replication traffic

## Deviations (accepted)

- **New `ListObjectsInRange` RPC is required** — identified in the
  Phase-B code grounding: no object-listing path exists (the read path is
  key-addressed; AE compares Merkle roots per segment, not object rows).
  This is the one new wire surface in Phase B.
- **Tombstones are not rebuilt** — their loss delays GC reclamation only
  (orphan reaper covers); documented as an accepted recovery tradeoff.
- **Corrected from the brainstorm: no re-replication on metadata loss.**
  The brainstorm said peers "re-replicate the node's ranges onto healthy
  targets"; the code-grounding shows the segments and the lifecycle
  registry are on OTHER pools (data/wal) and survive — the node's copies
  are intact, so re-replication would duplicate data. Peers only route
  around the node while it rebuilds its index. The earlier
  "re-replication-in" wording in the brainstorm §2.9 is superseded by
  this correction.
