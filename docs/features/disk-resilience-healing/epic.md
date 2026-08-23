---
epic: "disk-resilience-healing"
status: proposed
priority: high
created: 2026-08-22
updated: 2026-08-22
---

# Disk Resilience — Phase B: Failure Semantics & Healing — Epic Plan

Epic: `disk-resilience-healing`
ADR: [ADR-0029](../adr/0029-storage-pools-disk-resilience.md)
Brainstorm: [disk-resilience-pools](../../brainstorm/disk-resilience-pools.md)
Depends on: [disk-resilience](../features-dir-not-applicable) (Phase A epic:
pools config, registry, manifest gossip, routing cache, segment→pool mapping)

## Goal

Turn the pool *status* fields from Phase A into live failure semantics:
detect disk degradation/failure (trend-based, tech-aware), drive the
Healthy → Degraded → Dead state machine with role-aware consequences,
propagate losses via targeted announcements + a periodic reconciliation
safety net, heal under-replication with a re-replication worker, and recover
from WAL-pool and metadata-pool loss. The cluster must survive a disk death
without evicting the node or storming its whole ring share.

## Code-grounding corrections (from the Phase-B preparation read)

These findings change the Phase-A-era design in the brainstorm/ADR and are
binding for this epic's features:

1. **The reconciliation loop is NEW machinery.** The "reuse the hint-sweep
   skeleton" claim was wrong: the 5s hint sweep (node.rs:1740-1817) is a
   per-node hint *drain* (select! on interval + Alive events), not a
   per-range replica-count scan. Only the *pattern* (interval + event
   select + bounded retries) carries over.
2. **The segment is the announcement/reconciliation unit — not a key
   range.** The ring has no range abstraction (ring.rs:16-47: vnode
   positions are `[u8;32]`, `lookup` is per-key-hash). But
   `SegmentMetadata.storage_locations` (types/metadata.rs:151) already
   lists the replica set per segment, and `pool_id` (Phase A f5) identifies
   the owning pool. "Ranges R on pool P" = "segments with pool_id P".
3. **Replica-fetch machinery exists and is reusable.** `HealWorker` already
   fetches full segments from peers via `HealingRpcClient::fetch_shard`
   (heal/worker.rs:431-515; healing_service.rs:855) and writes through the
   `SegmentDataStore` trait (anti_entropy/merkle_tree.rs:35-53). B5/B7/B8
   build on these, not on new RPCs — except one (g8's object listing).
4. **Heal's distributed fetch iterates ALL alive nodes**, not ring-targeted
   replicas (heal/worker.rs:439-444; ring_cache is a dev-dependency of
   durability). B5's capacity-aware target selection must be injected as a
   trait from the node layer (which holds the manifest cache, f7).
5. **The objects CF cannot be rebuilt from the WAL.** `WalEntry`
   (wal/entry.rs:52-79) carries segment position + data + HLC, but NO
   bucket/key — object→chunk mapping exists only in the objects CF
   (store.rs:237-244: only `objects` + `deletions` CFs remain). B8 needs a
   new peer RPC to list object rows in a ring range.
6. **Segment state survives metadata-pool loss.** Segment lifecycle lives in
   the event-WAL + checkpoint + registry (ADR-0024/25; node.rs:1159-1181),
   pinned to the WAL pool (Phase A f4). A metadata-pool death loses only
   objects/tombstones CFs — segments and their `pool_id` mapping survive.

## Feature DAG

```
g1 disk-io-observability
 └── g2 failure-state-machine
      ├── sealed-segment-replication   (corrective backbone — g3's premise)
      │    ├── g3 loss-announcement    (+ compaction remap, Option A)
      │    ├── g4 reconciliation
      │    └── g6 routing-manifests
  g4 ──→ g5 re-replication-worker
  g3 + g5 ──→ g7 wal-loss-recovery
  g2 + g5 ──→ g8 metadata-loss-recovery
```

Implementation order: **g1 → g2 → sealed-segment-replication → g3 → g4 →
g5 → g6 → g7 → g8**. The backbone (discovered while scoping g3) is the
data-replication prerequisite: object bytes are only durable on the
segment ring after a sealed segment's full data section is pushed to its
replicas and `storage_locations` is stamped. g3's fan-out targets
(`storage_locations − self`) are real only because of it. After the
backbone, g3/g4/g6 are independent; g5 needs g4 (repair requests); g7
needs g3 (own loss announcement) + g5 (fetch machinery); g8 needs g2
(metadata-dead detection) + g5 (reuses its fetch/worker pattern — but NO
re-replication runs; see the feature's correction note).

| # | Feature | Touches | Depends on |
|---|---|---|---|
| g1 | `disk-io-observability` — DiskIo abstraction, FaultyIo, signal buckets, trend detector, tech profiles | storage, core | f2, f3 (Phase A) |
| g2 | `failure-state-machine` — status transitions, role consequences, write rejection | storage, node, server | g1 |
| — | `sealed-segment-replication` — data-replication backbone: seal-time push, storage_locations, needs set (corrective; discovered mid-epic) | node, server, storage, durability, routing | g2 |
| g3 | `loss-announcement` — segment-set announcement + compaction remap, targeted fan-out | node, durability, core | g2, sealed-segment-replication |
| g4 | `reconciliation` — 5s repair loop, risk-prioritized queue, metadata-repair primitive (GAP-1 failsafe) | node, durability | g2, sealed-segment-replication |
| g5 | `re-replication-worker` — repair execution, capacity-aware targets | durability, node | g4 |
| g6 | `routing-manifests` — read/write path filters, hint target preference | node, server | g2, f7 (Phase A) |
| g7 | `wal-loss-recovery` — fresh WAL, registry rebuild, catch-up from replicas | durability, node, storage | g3, g5 |
| g8 | `metadata-loss-recovery` — unavailability, fresh store, objects rebuild (NO re-replication) | node, server, storage | g2, g5 |

## Acceptance bar (epic DoD)

- [ ] ADR-0029 D3 (typed failure semantics + role consequences), D4 (push
      announcement + periodic reconciliation), D5 (routing on manifests),
      D6 (RF-urgency pacing), D7 (WAL/metadata loss recovery) implemented
      for Phase B.
- [ ] A Degraded data pool routes reads/writes around it (no re-replication
      storm); a Dead data pool triggers a targeted announcement to
      `storage_locations` holders and re-replication restores RF.
- [ ] Reconciliation restores RF **even when announcements are suppressed**
      (safety-net test), within the 5s-tick + repair bound for single-copy
      segments.
- [ ] wal-pool kill: writes rejected (write_degraded) + reads continue;
      replacement with a fresh WAL resumes writes after catch-up — no data
      loss (verified via objects-CF-driven missing-segment enumeration).
- [ ] metadata-pool kill: node serves nothing, peers route around without
      SWIM suspicion timeouts, fresh store rebuilds objects from peers
      (NO re-replication — data pools + registry are intact), node
      rejoins serving — zero cluster data loss, zero re-replication
      traffic.
- [ ] No node eviction storm: SWIM state stays Alive throughout disk-level
      events (probes are socket-only, ADR-0028).
- [ ] All Phase A suites stay green (regression); clippy/fmt/rustdoc clean
      across affected crates.
