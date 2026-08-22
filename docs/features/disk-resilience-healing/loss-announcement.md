---
feature: "Loss Announcement (Targeted Push)"
epic: "disk-resilience-healing"
status: proposed
priority: high
owner: ""
dependencies: ["failure-state-machine"]
adr: [0029]
perf: [1.3, 7.1]
created: 2026-08-22
updated: 2026-08-22
---

# Loss Announcement (Targeted Push)

## Summary

ADR-0029 §D4's fast path: when a data pool is confirmed Dead, the node
announces the affected **segment set** (derived in g2 from the lifecycle
registry's `pool_id`) to exactly the peers that hold replicas — the
`storage_locations` field of each affected segment's `SegmentMetadata`
(types/metadata.rs:151). Peers cross-check their own hold-set and enqueue
re-replication (g5). The announcement is an event, not state: it never
rides the NodeManifest.

## Scope

### In Scope

- Wire (data-plane proto — the announcement is replica-to-replica, not
  membership-plane state; it rides the existing gRPC server on
  `grpc_listen_addr`, node.rs:483-510):
  - `oceanfs.healing` proto extension (the healing service already carries
    `FetchShardRequest`): new `LossAnnouncement` message:
    `{ origin: NodeId, pool_id: u32, segments: repeated SegmentId }`
    (compact: segment ids are 16 bytes; a pool with 10k segments → 160 KB
    — bounded and rare).
  - New RPC on the healing service: `AnnounceLoss(LossAnnouncement) ->
    LossAck { accepted: uint32 }` — receiver verifies it actually holds a
    replica of each announced segment before acking.
- `oceanfs-node`:
  - On data-pool Dead (g2 event): build the announcement from
    `derive_affected_segments` (g2);
  - **fan-out (pinned)**: for each affected segment, the replica set is
    `SegmentMetadata.storage_locations` MINUS self. The union of those
    sets is the target list — NOT the whole cluster, NOT `ring.lookup`
    (the ring maps key hashes, not segments). Deliver via the existing
    `ConnectionPool` (data plane) + `HealingRpcClient`.
  - Retry policy: the announcement is a fast path — 3 attempts at 500ms
    spacing (mirrors the hint delivery retry, node.rs:1789-1792), then
    drop (the reconciliation loop g4 is the safety net).
- `oceanfs-durability` healing service:
  - `AnnounceLoss` handler: for each announced segment, verify local
    hold-set (lifecycle registry contains the segment AND
    `storage_locations` contains origin); enqueue a repair request into
    the ReRepWorker's queue (g5 — NOT the heal queue; heal repairs EC
    shard corruption, re-replication restores copies) for each verified
    segment; ack the count.
- Metrics: `oceanfs_announcements_tx_total`, `oceanfs_announcements_rx_total`,
  `oceanfs_announcements_accepted`.
- Tests:
  - unit (node): affected-set derivation from a registry with mixed
    `pool_id`s; fan-out targets == union(storage_locations - self);
    self excluded; retries bounded;
  - unit (durability): handler acks only segments the receiver holds;
    un-held segments not enqueued;
  - integration (local 3-node): kill data pool on node A → nodes B/C
    receive exactly the segments they hold and enqueue repairs (visible
    via heal-queue depth).

### Out of Scope

- Reconciliation (g4) — the safety net, announcement-independent.
- Re-replication execution (g5) — the announcement only enqueues.
- Node-unavailability announcements (metadata Dead) — g8 owns that payload;
  this feature is pool-loss (data) only.

## Crate Impact

| Crate | Change |
|---|---|
| `proto/oceanfs/healing.proto` | `LossAnnouncement`, `AnnounceLoss` RPC |
| `oceanfs-durability` | Healing service handler; heal-queue integration |
| `oceanfs-node` | Announcement construction + fan-out |

## Interface (Public API)

- `oceanfs_node::announce::announce_pool_loss(pool_id, segments, targets,
  pool: &ConnectionPool) -> Result<()>` — the fan-out primitive.
- `oceanfs_durability::healing_service::handle_announce_loss(...)` —
  verify + enqueue.

## Data Flow

```
g2: data pool Dead ──▶ derive_affected_segments(registry, pool_id)
   └─ for each segment: targets ∪= storage_locations - self
   └─ AnnounceLoss{pool_id, segments} → HealingRpcClient (3×500ms)
peer handler: verify local hold ──▶ enqueue repair (g5) ──▶ LossAck{count}
   └─ announcement dropped after retries ──▶ g4 reconciliation catches up
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` in `oceanfs-node`,
      `oceanfs-durability`, `oceanfs-network` (proto regen)
- [ ] **Tests:** all listed green (derivation, fan-out, verify-enqueue,
      retries)
- [ ] **Docs:** `# Examples` on pub items; rustdoc clean
- [ ] **ADR:** ADR-0029 §D4 (targeted push: RF-bounded fan-out, compact
      affected set, event-not-state) satisfied
- [ ] **Perf:** 1.3 (pre-sized segment vec), 7.1 (announcement path is
      rare; the derive reads the registry snapshot once, no lock held
      during RPC)
- [ ] **Integration:** 3-node local cluster — the killed-pool announcement
      reaches exactly the replica holders and the heal queue depth rises
      by the held-segment count

## Deviations (accepted)

- **No ack-guaranteed delivery.** The announcement is best-effort push
  with bounded retries; correctness never depends on it because g4
  reconciles independently. (Same tradeoff as hinted-handoff delivery —
  event-driven fast path + periodic sweep.)
