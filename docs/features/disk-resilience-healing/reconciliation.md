---
feature: "Periodic Reconciliation (Pull Safety Net)"
epic: "disk-resilience-healing"
status: proposed
priority: high
owner: ""
dependencies: ["failure-state-machine"]
adr: [0029]
perf: [1.3, 2.6, 7.1]
created: 2026-08-22
updated: 2026-08-22
---

# Periodic Reconciliation (Pull Safety Net)

## Summary

ADR-0029 §D4's mandatory safety net: a per-node 5s-tick loop (the
hint-sweep *pattern* from node.rs:1740-1817, but new machinery) that scans
the segments this node holds, computes the **live replica count** for each
from `SegmentMetadata.storage_locations` ∩ alive members (minus
unavailable nodes, per their manifests), and enqueues repair for any
segment below RF — risk-prioritized (single-copy segments first). It does
NOT depend on any announcement having arrived; it is a repair loop, not a
detection loop (failed repairs retry next tick).

## Scope

### In Scope

- `oceanfs-durability::reconcile`:
  - `struct ReconciliationLoop` — 5s tick (`tokio::time::interval`,
    matching `hint_delivery_sweep_sec` default 5):
    - input: lifecycle registry (held segments + their `storage_locations`
      + `pool_id`), membership view (alive nodes), manifest cache (node
      availability — a node whose metadata pool is Dead serves nothing and
      must not count as a live copy, g8).
    - **live-copy computation (pinned)**: for segment S,
      `live(S) = |storage_locations(S) ∩ alive_nodes|` — a node whose
      metadata pool is Dead still HOLDS S's data (data pools intact, g8),
      so it counts as a live copy for RF purposes; it is merely
      *unservable*, which is a routing concern (g6), not a durability
      concern. Re-replication is only for genuinely lost copies (Dead
      data pool, g3/g5).
      Note: this is a *belief* (metadata says who holds S), not disk
      truth; disk-truth verification is scrub/anti-entropy's job, out of
      scope.
    - risk-prioritized work queue: `BinaryHeap` keyed by
      `(live(S), last_attempt(S))` — single-copy segments (live=1) always
      drain first, double-copy next, healthy (≥ RF) never enqueued.
    - repair request → heal queue (g5) with retry-on-failure semantics
      (re-enqueue next tick — the loop keeps state per segment to avoid
      hot-looping: a failed segment is retried at most once per N ticks,
      N configurable, default 3).
  - **announcement independence**: the loop runs regardless of
    announcements; it is the complete safety net when announcements are
    suppressed (network partition, announcer crash).
- Node wiring: spawn the loop in node.rs beside the hint delivery watcher;
  register the membership event stream (`membership.subscribe()`) to wake
  the loop early on Alive/Dead changes (same select! pattern as the hint
  sweep, node.rs:1799-1817).
- Metrics: `oceanfs_ranges_under_replicated` (gauge, current count by
  live-copy class), `oceanfs_reconcile_scan_ms`, `oceanfs_repair_enqueued_total`.
- Tests:
  - unit: live-copy computation (alive ∩ locations − unavailable);
    priority ordering (single-copy before double-copy before healthy);
    retry pacing (failed segment not hot-looped);
  - unit: a suppressed-announcement scenario (no g3 events at all) —
    a segment under RF is still enqueued within `5s + tick` (the
    safety-net guarantee);
  - integration (local 3-node): kill a data pool on A; B/C's
    reconciliation enqueues repairs for their held segments even when the
    announcement channel is disabled.

### Out of Scope

- Announcement (g3) — the fast path; this loop does not send any.
- Re-replication execution (g5) — this loop only computes + enqueues.
- Scrub/anti-entropy disk-truth verification (existing machinery, unchanged).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | New `reconcile` module (loop, priority queue, live-copy calc) |
| `oceanfs-node` | Spawn loop + event wake-up wiring |

## Interface (Public API)

- `pub struct ReconciliationLoop` — `new(registry, membership, manifest_cache,
  heal_sender, config)`; `run(shutdown_token)`.
- `pub fn live_copy_count(segment: &SegmentMetadata, alive: &HashSet<NodeId>,
  unavailable: &HashSet<NodeId>) -> usize` — pure, unit-tested.
- `pub struct ReconcileConfig` — `tick_secs` (5), `retry_after_ticks` (3).

## Data Flow

```
membership events / 5s tick ──▶ scan held segments (registry snapshot)
   └─ live(S) = |storage_locations ∩ alive − unavailable|
       ├─ live < RF ──▶ priority queue (live-count first)
       │    └─ heal_sender.enqueue (g5) ──▶ re-replication
       └─ live ≥ RF ──▶ skip
failed repair ──▶ retry after retry_after_ticks (never hot-loop)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` in `oceanfs-durability`,
      `oceanfs-node`
- [ ] **Tests:** all listed green (live-copy calc, priority, retry pacing,
      suppressed-announcement safety net)
- [ ] **Docs:** `# Examples` on pub items; rustdoc clean
- [ ] **ADR:** ADR-0029 §D4 (periodic reconciliation, repair loop,
      announcement-independent) + §D6 (risk-prioritized pacing) satisfied
- [ ] **Perf:** 1.3 (pre-sized scan vec), 2.6 (bounded heal queue
      backpressure), 7.1 (registry snapshot once per tick; the loop holds
      no locks during RPC-free compute)
- [ ] **Integration:** 3-node local cluster — with announcements disabled,
      a killed data pool's segments are re-replicated to RF within the
      `5s + repair` bound (the epic's safety-net DoD item)

## Deviations (accepted)

- **Reconciliation counts metadata belief, not disk truth.** A node listed
  in `storage_locations` but missing the file (e.g., torn write) is counted
  live until scrub/AE catches it. Disk-truth verification stays with the
  existing scrub/anti-entropy machinery; the reconciliation loop's job is
  replica-count restoration, not integrity.
