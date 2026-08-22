---
feature: "Re-Replication Worker"
epic: "disk-resilience-healing"
status: proposed
priority: high
owner: ""
dependencies: ["reconciliation"]
adr: [0029]
perf: [1.3, 2.7, 8.1, 8.5]
created: 2026-08-22
updated: 2026-08-22
---

# Re-Replication Worker

## Summary

The repair executor of ADR-0029 §D4/D5/D6: a worker that consumes
re-replication requests (from g3's announcements and g4's reconciliation),
fetches the missing segment from a live holder via the existing
`HealingRpcClient::fetch_shard` (healing_service.rs:855), writes it through
the pool-aware `SegmentDataStore` (Phase A f5), and updates
`SegmentMetadata.storage_locations` so the new copy is accounted. Target
selection for *placement* is capacity-aware via peer manifests (f7);
concurrency is bounded by a semaphore (HealWorker pattern, heal/worker.rs:8-12).

## Scope

### In Scope

- `oceanfs-durability::repair`:
  - `struct ReRepWorker` — mirrors HealWorker's shape:
    - bounded queue (mpsc, perf 2.6) + `tokio::sync::Semaphore`
      (perf 2.7/8.5, `max_concurrent_repairs`, default 16);
    - `process(repair_request)`: fetch segment data from a live holder
      (iterate `storage_locations − self` via `fetch_segment_from_replicas`,
      heal/worker.rs:431-515 — the existing H3 fetch, reused) → write via
      `SegmentDataStore::write_segment_data` → update `storage_locations`.
    - **placement**: the write goes through the pool-aware store which
      selects a data pool via `PlacementPolicy` (Phase A f3) — the worker
      picks the *target node* (via manifests), the node's own store picks
      the *pool*.
  - **target-node selection (pinned, injected)**: a trait
    `RepairTargetSelector` implemented in `oceanfs-node`:
    `fn pick_repair_target(&self, source: &SegmentId, holders: &[NodeId]) -> Option<NodeId>`
    — filters candidates by manifest health (excludes nodes with
    `write_degraded` / no Healthy data pool, f7 cache) and prefers the
    node with the most free data-pool capacity (manifests'
    `capacity_free_bytes`). The durability crate never touches manifests
    directly (ring_cache is a dev-dependency there today — same boundary,
    heal/worker.rs:86-88).
  - **storage_locations update (pinned)**: through the lifecycle
    coordinator's existing durable write path — the same
    `request_refresh_metadata` the HealWorker uses post-repair
    (heal/worker.rs:417-420) is extended to carry the new location set, so
    the event-WAL remains the only durable writer (ADR-0024/25).
- Node wiring: build the worker with `RepairTargetSelector` over the
  manifest cache (f7); wire g3's announcement handler and g4's
  reconciliation into the same queue.
- Metrics: `oceanfs_ranges_re_replicated_total`,
  `oceanfs_repair_queue_depth{priority}`, `oceanfs_repair_failures_total`.
- Tests:
  - unit: worker processes a request end-to-end with an in-memory data
    store (fetch from a mock holder, write, locations updated);
  - unit: selector — excludes write_degraded / no-Healthy-pool candidates;
    prefers most-free-capacity; falls back to any live holder when all
    filtered out;
  - unit: concurrency bound (semaphore limits in-flight ops);
  - integration (local 3-node): kill a data pool on A → B/C re-replicate
    their held segments (announcement + reconciliation both drive it);
    `storage_locations` converges to include the repair target; the
    cluster serves reads of the affected keys without data loss.

### Out of Scope

- Announcement (g3) and reconciliation (g4) — the worker only executes.
- WAL/metadata-loss recovery (g7/g8) — those use the worker's fetch
  primitive but have their own recovery flows.
- EC shard repair (existing HealWorker/scrub path — unchanged).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | New `repair` module (worker, selector trait); lifecycle locations-update |
| `oceanfs-node` | Selector impl over manifest cache; wiring into announcements + reconciliation |

## Interface (Public API)

- `pub struct ReRepWorker` — `new(config, data_store, selector, lifecycle, pool, membership)`, `run(shutdown)`.
- `pub trait RepairTargetSelector: Send + Sync` — `pick_repair_target(...)`.
- `pub struct RepairRequest { segment_id: SegmentId, holders: Vec<NodeId>, reason: RepairReason }` — `RepairReason::{Announcement, Reconciliation}`.

## Data Flow

```
g3 announcement / g4 reconciliation ──▶ RepairRequest ──▶ bounded queue
   └─ semaphore ──▶ process:
        fetch from holders (fetch_shard, heal/worker.rs pattern)
        └─ selector.pick_repair_target (manifests: health + free capacity)
        └─ write via SegmentDataStore (PlacementPolicy picks the pool)
        └─ lifecycle.request_refresh_metadata(locations += target)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` in `oceanfs-durability`,
      `oceanfs-node`
- [ ] **Tests:** all listed green (end-to-end, selector rules, concurrency,
      3-node integration)
- [ ] **Docs:** `# Examples` on pub items; rustdoc clean
- [ ] **ADR:** ADR-0029 §D5 (routing hint — selector consults manifests),
      §D6 (repair pacing via bounded concurrency + priority) satisfied
- [ ] **Perf:** 2.7/8.5 (semaphore-bounded concurrency), 8.1
      (FuturesUnordered for parallel holder fetch attempts), 1.3
      (pre-sized fetch buffers), 2.6 (bounded queue backpressure)
- [ ] **Integration:** the epic's "re-replication restores RF" DoD — a
      killed data pool's segments return to RF via announcement AND via
      reconciliation alone (both paths tested)

## Deviations (accepted)

- **`storage_locations` update rides `request_refresh_metadata`** rather
  than a new lifecycle event. The coordinator's refresh path already
  exists and is durable; extending its payload avoids a new event type
  while keeping the event-WAL the single durable writer.
