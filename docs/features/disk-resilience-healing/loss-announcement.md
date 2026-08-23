---
feature: "Loss Announcement + Compaction Remap (Targeted Push)"
epic: "disk-resilience-healing"
status: done
priority: high
owner: ""
dependencies: ["failure-state-machine", "sealed-segment-replication"]
adr: [0029]
perf: [1.3, 2.6, 7.1]
created: 2026-08-22
updated: 2026-08-23
---

# Loss Announcement + Compaction Remap (Targeted Push)

## Summary

ADR-0029 §D4's fast path, extended by the owner-approved Option A for
GAP-1 (compaction metadata-remap propagation). Two announcement kinds on
the healing service, both targeted pushes with bounded retries:

1. **Loss announcement** — when a data pool is confirmed Dead, the node
   announces the affected **segment set** (derived in g2 from the
   lifecycle registry's `pool_id`) to exactly the peers that hold
   replicas — `SegmentMetadata.storage_locations − self`. Peers
   cross-check their own hold-set and enqueue re-replication (g5). The
   announcement is an event, not state: it never rides the NodeManifest.
2. **Compaction remap** — when the owner compacts `S → S'`, it tells the
   holders of `S` so they re-point their OWN object rows. The owner's
   compaction rewrites only its RocksDB (`ObjectsMoved`); without
   propagation, peers' metadata silently diverges until reads reference a
   segment that exists nowhere (GAP-1 — the `45c8` read failure). The
   remap carries the **chunk-remap table** because the repacked layout is
   not offset-preserving.

The periodic reconciliation loop (g4) is the MANDATORY pull failsafe that
runs regardless of whether any announcement arrived (ADR-0029 §D4).

## Scope

### In Scope

- **Wire (healing.proto — data-plane; the healing service already rides
  `grpc_listen_addr`):**
  - `LossAnnouncement { origin: NodeId, pool_id: u32, segments: repeated
    SegmentId }` + `AnnounceLoss(LossAnnouncement) → LossAck
    { accepted: uint32 }`.
  - `SegmentRemap { origin: NodeId, old_segment_id: SegmentId,
    new_segment_id: SegmentId, chunks: repeated RemappedChunk }` +
    `AnnounceRemap(SegmentRemap) → RemapAck { applied: bool }`.
  - `RemappedChunk { old_offset: u64, length: u32, new_offset: u64 }` —
    one entry per live chunk repacked from the old segment into the new.
- **`oceanfs-node` (announce module):**
  - On data-pool Dead (g2 event): build the announcement from
    `derive_affected_segments`; **fan-out (pinned)**: for each affected
    segment, targets = `union(storage_locations − self)` — NOT the whole
    cluster, NOT `ring.lookup`. Deliver via the existing `ConnectionPool`
    + `HealingRpcClient`.
  - On compaction (the compactor's `compaction_remap_notifier` fires
    after `ObjectsMoved` commits): fan the remap out to
    `storage_locations(old) − self`.
  - Retry policy: 3 attempts at 500 ms spacing (mirrors the hint
    delivery retry), then drop (g4 reconciliation is the safety net).
  - `Node::remap_alias()`, `Node::pending_repairs()` accessors.
- **Compactor (segment_compactor.rs):**
  - `with_compaction_remap_notifier(old, new, chunk_table)` — fired
    AFTER `ObjectsMoved` commits (the owner's metadata is authoritatively
    at the new id, and the old `.dat` is still present, so a peer that
    has not processed the remap can still fetch the old segment from the
    owner via the read fallback — no read window). The chunk table is
    built from the compactor's existing `chunk_remap` HashMap.
- **`oceanfs-durability` healing service:**
  - `AnnounceLoss` handler: for each announced segment, verify local
    hold-set (lifecycle registry contains the segment AND
    `storage_locations` contains origin); enqueue a `ReRepRequest` into
    the repair sink (g5 — NOT the heal queue; heal repairs EC shard
    corruption, re-replication restores copies); ack the count.
  - `AnnounceRemap` handler: verify hold-set + origin → record the alias
    + chunk table (`SegmentRemapAlias`) → batch-rewrite locally-persisted
    object rows through the chunk table → delete the stale replica
    (durable `request_delete` + unlink, ADR-0024 invariant 3) → ack.
- **Alias map (`oceanfs-core::SegmentRemapAlias`):** receiver-side
  `old → (new, chunk table)` consulted by the append handler when
  persisting replicated metadata — a LATE chunk ref referencing a
  segment the local GC already compacted away is translated at write time
  (the `45c8` mechanism closed at the push level).
- Metrics: `oceanfs_announcements_tx_total`,
  `oceanfs_announcements_rx_total`, `oceanfs_announcements_accepted`.
- Tests:
  - unit (node): fan-out targets == union(storage_locations − self);
    self excluded; dedup; retries bounded (fan-out derivation).
  - unit (durability): `AnnounceLoss` acks only held+verified segments;
    un-held/spoofed origins not enqueued; no sink → acks nothing.
    `AnnounceRemap` re-points rows + records alias; rejects spoofed
    remaps. Compactor fires the remap notifier with the right ids +
    chunk table.
  - unit (core): alias resolve/insert/remove/evict.
  - integration (local 3-node): (1) kill data pool on A → B/C receive
    exactly the segments they hold and enqueue repairs (visible via
    `pending_repairs()`); (2) **GAP-1 closure**: DELETE + GC compaction →
    every surviving object readable byte-identical through A, B, AND C.

### Out of Scope

- Reconciliation (g4) — the mandatory safety net, announcement-
  independent; documented as the failsafe that catches whatever the push
  missed (including the metadata-repair primitive: detect "metadata
  references a segment on no live holder" and re-point it).
- Re-replication execution (g5) — the announcement only enqueues.
- Node-unavailability announcements (metadata Dead) — g8 owns that
  payload; this feature is pool-loss (data) only.
- Deterministic repack ids (Option C) — rejected (content divergence
  makes a deterministic id with divergent bytes worse than divergent ids).

## Crate Impact

| Crate | Change |
|---|---|
| `proto/oceanfs/healing.proto` | `LossAnnouncement`, `LossAck`, `SegmentRemap`, `RemappedChunk`, `RemapAck`; `AnnounceLoss` + `AnnounceRemap` RPCs |
| `oceanfs-core` | `SegmentRemapAlias` (+ `RemappedChunk`) — shared receiver-side map |
| `oceanfs-durability` | Healing service handlers; compactor remap notifier; repair-sink trait + `ReRepRequest` |
| `oceanfs-server` | Append handler translates late chunk refs through the alias |
| `oceanfs-node` | `announce` module (fan-out + retry); composition-root wiring |

## Interface (Public API)

- `oceanfs_node::announce::announce_pool_loss(origin, pool_id, segments,
  targets, pool, membership, attempts, retry_delay, metrics) -> Result<()>`
  — `metrics: Option<&AnnounceMetrics>` increments the tx counter (None
  in tests; implemented as the 9th parameter — superset of the original
  spec).
- `oceanfs_node::announce::announce_segment_remap(origin, old, new,
  chunks, targets, pool, membership, attempts, retry_delay, metrics) -> Result<()>`.
- `oceanfs_node::announce::AnnounceMetrics` — `new()`,
  `register_metrics()`, `record_delivery()`.
- `oceanfs_node::announce::derive_fan_out_targets(segments, self_id) ->
  Vec<NodeId>` — the pinned union-minus-self fan-out.
- `oceanfs_durability::healing_service::{ReRepRequest, RepairSink}`.
- `oceanfs_core::{SegmentRemapAlias, RemappedChunk}`.
- `Node::remap_alias()`, `Node::pending_repairs()`,
  `Node::try_recv_repair()`.

## Data Flow

```
data pool Dead ──▶ derive_affected_segments(registry, pool_id)
   └─ targets = union(storage_locations − self) ──▶ AnnounceLoss → HealingRpcClient (3×500ms)
peer handler: verify local hold ──▶ enqueue ReRepRequest (g5) ──▶ LossAck{count}
   └─ announcement dropped after retries ──▶ g4 reconciliation catches up

compactor ObjectsMoved ──▶ compaction_remap_notifier(old, new, chunks)
   └─ targets = storage_locations(old) − self ──▶ AnnounceRemap → HealingRpcClient (3×500ms)
peer handler: verify hold+origin ──▶ alias.insert ──▶ repoint_objects ──▶ request_delete + unlink ──▶ RemapAck
   └─ late append ──▶ alias.resolve(chunk) ──▶ persisted with the new ref (GAP-1 closed)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` in `oceanfs-node`,
      `oceanfs-durability`, `oceanfs-core`, `oceanfs-server`,
      `oceanfs-storage`; proto regen
- [x] **Tests:** all listed green (derivation, fan-out, verify-enqueue,
      remap re-point + reject, alias, compactor notifier)
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
- [x] **ADR:** ADR-0029 §D4 (targeted push: RF-bounded fan-out, compact
      affected set, event-not-state) satisfied; GAP-1 closure at the
      push level + g4 failsafe documented
- [x] **Perf:** 1.3 (pre-sized segment vec — `Vec::with_capacity(chunk_remap.len())`
      at segment_compactor.rs, `HashMap::with_capacity` at healing_service.rs;
      `derive_affected_segments`/`derive_fan_out_targets` use `Vec::new()` — rare
      paths with unknown counts, acceptable), 2.6 (bounded repair queue,
      bounded retries), 7.1 (announcement path is rare; the derive reads
      the registry snapshot once, no lock held during RPC; the append
      handler's alias resolve is one short read-lock per chunk — resolved
      per chunk, each acquire/release bounded, never held across I/O)
- [x] **Integration:** 3-node local cluster — (1) the killed-pool
      announcement reaches exactly the replica holders and
      `pending_repairs()` rises by the held count; (2) after
      DELETE + GC compaction, surviving objects are byte-identical when
      read through A, B, AND C (the GAP-1 closure assertion)

## Deviations (accepted)

- **No ack-guaranteed delivery.** The announcements are best-effort push
  with bounded retries; correctness never depends on them because g4
  reconciles independently. (Same tradeoff as hinted-handoff delivery.)
- **The remap handler does NOT wait for the new segment's push to land
  before re-pointing.** The owner's remap fires after `ObjectsMoved`,
  and the new segment's push is enqueued at `NewSealed` (before it); a
  peer that re-points to `S'` before `S'.dat` arrives locally serves via
  the read path's gRPC fallback to the owner (which holds `S'`). The
  alias records `S → S'` regardless.
- **Option C (deterministic repack id) rejected.** Two nodes compacting
  "the same" segment produce DIFFERENT bytes (different dead sets at
  different times, different metadata arrival), so a deterministic id
  would collide with divergent content — the push receiver's merkle
  check would reject one. Worse than divergent ids; not implemented.
- **Stale-replica deletion is best-effort on the receiver.** If the
  `request_delete`/unlink fails, the receiver's own GC reclaims the
  stale replica via the fully-dead path (its rows were re-pointed away).

## Known Gaps (handoff to g4)

- **g4 MUST add the metadata-repair primitive**: detect "metadata
  references a segment on no live holder" and re-point it (the pull
  failsafe for whatever the push missed — a late append that raced both
  the remap AND the receiver's re-point scan, a remap that lost to a
  partition, a remap to a node that was down through all retries).
- **g4 MUST recompute live copies against the CURRENT ring**, treating
  `storage_locations` as intent, not truth (GAP-5).
- **g5 drains `Node::repair_rx`** (the bounded channel behind the
  repair sink).
