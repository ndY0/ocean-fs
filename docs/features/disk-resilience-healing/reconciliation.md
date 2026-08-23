---
feature: "Periodic Reconciliation (Pull Safety Net)"
epic: "disk-resilience-healing"
status: done
priority: high
owner: ""
dependencies: ["failure-state-machine", "sealed-segment-replication", "loss-announcement"]
adr: [0029]
perf: [1.3, 2.6, 7.1]
created: 2026-08-22
updated: 2026-08-23
---

# Periodic Reconciliation (Pull Safety Net)

## Summary

ADR-0029 §D4's mandatory safety net: a per-node loop that restores RF
**independently of any announcement having arrived**. It is a repair
loop, not a detection loop — failed repairs retry next tick.

**Design (user-approved — scan-free):**

- **Event-driven wake, not a full scan.** A bounded, risk-prioritized
  work queue is processed per tick. The queue is populated by
  *events* — a node died / a node's data pools all went Dead (its
  manifest change rides membership gossip) — which identify the affected
  segments through a **HolderIndex** (node → segments listing it in
  `storage_locations`), maintained incrementally (O(RF) per stamp, never
  a full scan). A node dying touches exactly the segments that listed it.
- **Completeness without sampling.** A slow drift scan (hourly,
  configurable) does a full pass — every segment is checked, just not
  every tick (ADR-0029 §D4: "healthy ranges at slow background cadence
  (drift detection)").
- **Risk-prioritized queue.** Single-copy segments (live=1) drain
  first, double-copy (live=2) next, healthy (≥ RF) never enqueued.
- **Retry pacing.** A failed repair is retried at most once per
  `retry_after_ticks` (3) — never hot-looped.
- **Job B (read-driven dangling-metadata repair).** When a read fails
  because the object's metadata references a segment on no live holder
  (a compaction remap the g3 push missed — GAP-1 failsafe), the read
  path fetches the object's CURRENT metadata from its ring replicas and
  re-points locally, then retries once. Zero scans, zero sampling — the
  repair happens exactly where and when a dangling reference is
  observed (ADR-0029 §D5: the error path is the truth).

## Scope

### In Scope

- `oceanfs-durability::reconcile`:
  - `struct ReconciliationLoop` — event wake (`membership.subscribe()`),
    5s tick (`tokio::time::interval`), hourly drift scan:
    - input: lifecycle registry (held segments + their `storage_locations`
      + `pool_id`), membership view (alive nodes + manifests).
    - **live-copy computation (pinned)**: for segment S,
      `live(S) = |storage_locations(S) ∩ alive − unavailable|` — a node
      whose metadata pool is Dead still HOLDS S's data (data pools
      intact, g8), so it counts as a live copy; a node whose data pools
      are ALL Dead (or a Left/Dead member) does not. Note: metadata
      belief, not disk truth — scrub/AE verify disk contents (out of
      scope).
    - risk-prioritized work queue: `BinaryHeap` keyed by live count
      (single-copy first).
    - repair request → the g3 repair sink (g5) with retry-on-failure
      semantics (retried at most once per `retry_after_ticks`).
  - `HolderIndex` — reverse `node_id → segments` index, maintained
    incrementally from the lifecycle coordinator's
    `set_storage_locations` notifier (the SINGLE choke point) + boot
    build + drift rebuild.
  - **announcement independence**: the loop runs regardless of
    announcements (proven by the integration test with
    `announcements_enabled=false`).
- `oceanfs-storage`: `SegmentLifecycleCoordinator` gains an optional
  `storage_locations_notifier` (fired after every holder-set commit).
- `oceanfs-server` (Job B, read-driven repair):
  - `Error::SegmentUnavailable` — raised by the fetch path when a
    segment is unreachable on every holder.
  - `ReadCoordinator::repair_dangling_metadata` — on that error, fetch
    the object's current metadata from its ring replicas, write the
    re-pointed rows locally, retry the read once.
- Node wiring: spawn the loop in node.rs; wire the
  `storage_locations` notifier; register the membership event stream;
  `announcements_enabled` config (default true — disabling it proves
  the safety net is independent of the push).
- Metrics: `oceanfs_ranges_under_replicated` (gauge — the current
  under-replicated population awaiting repair, i.e. the work-queue
  depth), `oceanfs_reconcile_scan_ms` (gauge, last drift scan
  duration), `oceanfs_repair_enqueued_total` (counter).
- Tests:
  - unit: live-copy computation (alive ∩ locations − unavailable);
    holder index record/replace; priority ordering (single-copy
    first); retry pacing (a hot-looping segment is not re-enqueued
    within `retry_after_ticks`); drift-scan index build; Job B
    detector + metadata-response conversion.
  - integration (local 3-node, `announcements_enabled=false`): kill a
    data pool on A; B/C's reconciliation enqueues repairs for their
    held segments even when the announcement channel is disabled.

### Out of Scope

- Announcement (g3) — the fast path; this loop does not send any.
- Re-replication execution (g5) — this loop only computes + enqueues.
- Scrub/anti-entropy disk-truth verification (existing machinery, unchanged).
- Full fetch-from-owner metadata rebuild (the corner where the owner is
  also unreachable) — g8's metadata-loss rebuild machinery.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | New `reconcile` module (loop, HolderIndex, priority queue, live-copy calc) |
| `oceanfs-storage` | `SegmentLifecycleCoordinator::set_storage_locations` notifier |
| `oceanfs-server` | `Error::SegmentUnavailable` + `ReadCoordinator::repair_dangling_metadata` (Job B) |
| `oceanfs-node` | Spawn loop + event wake-up wiring + `announcements_enabled` config |

## Interface (Public API)

- `oceanfs_durability::reconcile::ReconciliationLoop` — `new(registry,
  membership, repair_sink, self_id, rf, config)`; `run(shutdown_token)`;
  `enqueue(segment_id)`; `on_storage_locations(id, locations)`;
  `pending_len()`; `holder_index()`; `register_metrics()`.
- `oceanfs_durability::reconcile::ReconcileConfig` — `tick_secs` (5),
  `retry_after_ticks` (3), `drift_scan_secs` (3600),
  `max_batch_per_tick` (256).
- `oceanfs_durability::reconcile::live_copy_count(segment, alive,
  unavailable) -> usize` — pure, unit-tested.
- `oceanfs_durability::reconcile::{HolderIndex, NoopRepairSink}`.
- `oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::{
  with_storage_locations_notifier, set_storage_locations_notifier}`.
- `oceanfs_server::Error::SegmentUnavailable`.
- `NodeConfig::announcements_enabled` (default true).
- `Node::reconciliation()` accessor.

## Data Flow

```
membership event (node Dead / data pools all Dead) ──▶ HolderIndex[node] ──▶ enqueue affected
5s tick ──▶ process bounded batch (risk-priority: live=1 first)
   └─ live(S) < RF ──▶ repair_sink.enqueue (g5) ──▶ re-replication
   └─ failed repair ──▶ retry after retry_after_ticks (never hot-loop)
hourly drift scan ──▶ rebuild index ──▶ full pass (completeness, not sampling)

read fails (SegmentUnavailable) ──▶ repair_dangling_metadata ──▶ GetObjectMetadata from owner
   └─ owner reports current chunks ──▶ write locally ──▶ retry read once
   └─ owner unreachable ──▶ original error (g8 rebuild)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` in `oceanfs-durability`,
      `oceanfs-node`, `oceanfs-server`, `oceanfs-storage`
<!-- REVIEW (iter 2, re-run): `cargo build --all-targets` passes; only
     warning is the known pre-existing dead-code `test_hint_wal_implements_wal_writer_trait`
     (crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs:848, untouched by this
     feature). -->
- [x] **Tests:** all listed green (live-copy calc, holder index,
      priority, retry pacing, drift scan, Job B, suppressed-announcement
      safety net)
<!-- REVIEW (iter 2, re-run): durability lib 253, node lib 59 (2 ignored), server lib
     232 — all green. New unit tests: live_copy_counts_intersection_minus_unavailable,
     holder_index_records_and_replaces, priority_orders_single_copy_first,
     retry_pacing_prevents_hot_loop, drift_scan_builds_index_from_registry
     (reconcile.rs:747,766,783,819,899), dangling_segment_error_detector,
     object_metadata_from_response_preserves_chunks_and_hlc (coordinator.rs:1800,1811). -->
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
<!-- REVIEW (iter 2, re-run): RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
     -p oceanfs-core -p oceanfs-storage -p oceanfs-durability -p oceanfs-node passes.
     oceanfs-server rustdoc still fails ONLY on the pre-existing RING_PROBE_HASHES
     (admin.rs:267) + HintObjectApplier (write/coordinator.rs:1879) errors, both in
     untouched files. -->
- [x] **ADR:** ADR-0029 §D4 (periodic reconciliation, repair loop,
      announcement-independent) + §D6 (risk-prioritized pacing) satisfied
<!-- REVIEW (iter 2, re-verified): 5s tick + bounded risk-prioritized queue
     (single-copy first) + hourly drift scan; repair loop (failed repairs retried
     after retry_after_ticks=3); announcement independence proven by the integration
     test (announcements_enabled=false); §D6 no-timer-window urgency via queue
     priority, not deferral timers. -->
- [x] **Perf:** 1.3 (pre-sized vectors), 2.6 (bounded queue + bounded
      batch per tick), 7.1 (registry snapshot once per pass; no locks
      held across RPC/await; the holder-index notifier fires outside the
      shard lock)
<!-- REVIEW (iter 2): 1.3 — batch/pre-pass Vec::with_capacity (reconcile.rs:623,646),
     2.6 — in_queue dedup bounds the queue + max_batch_per_tick=256 + bounded 1024
     repair channel (node.rs:924); 7.1 — membership_snapshot once per drift pass
     (reconcile.rs:596), no parking_lot guard held across repair_sink.enqueue().await
     (all guards scoped in the sync pre-pass), notifier fires after drop(guard)
     outside the shard write lock (lifecycle.rs:2161-2169). The prior drift-scan
     re-entrancy gap (enqueue→registry.get inside for_each while it holds the shard
     read lock) is FIXED: the drift scan computes live_copy_count from the already-held
     entry and calls enqueue_with_live (reconcile.rs:599-606), which never re-enters
     the registry (453-460); the public enqueue still computes live via registry.get
     for the event-wake path (445-448, 567). -->
- [x] **Integration:** 3-node local cluster — with announcements
      disabled, a killed data pool's segments are detected as
      under-replicated and repairs are enqueued (the safety-net
      guarantee; execution is g5)
<!-- REVIEW (iter 2, re-run): `cargo test -p oceanfs-node --test reconciliation
     --test loss_announcement -- --test-threads=1` passes (1 + 1 tests, ~39s total):
     announcements disabled on all 3 nodes, A's data pool killed → B/C
     pending_repairs grow ≥ owner_segment_count. Sibling suite
     (loss_announcement, segment_replication 3, failure_state_machine 2,
     routing_cache, io_observer_wiring, io_observer_faulty) all green.
     fmt/clippy: cargo fmt --check clean; lib clippy -D warnings clean on all 5
     crates; `cargo clippy -p oceanfs-node --all-targets -- -D warnings` clean.
     Pre-existing failures unchanged (verified): durability all-targets clippy
     dead-fn test_hint_wal_implements_wal_writer_trait (hint_wal.rs:848); server
     rustdoc RING_PROBE_HASHES + HintObjectApplier (3 errors incl. summary);
     swim_death_detection_within_timeout (grpc_services.rs:560). -->

## Deviations (accepted)

- **Reconciliation counts metadata belief, not disk truth.** A node
  listed in `storage_locations` but missing the file (e.g., torn write)
  is counted live until scrub/AE catches it.
- **Job B is read-driven, not scan-driven (user-approved).** The g4 doc
  originally read "the loop must detect dangling metadata"; detecting it
  by scanning all object rows is the terabytes-per-tick anti-pattern.
  Instead the read path repairs exactly the observed dangling reference
  (zero scans, zero sampling, one round-trip per failing read). The
  owner-down corner is g8's metadata-loss rebuild.
- **`storage_locations` is intent, not truth (GAP-5).** The loop
  recomputes live copies against the current membership+manifest view;
  after a repair lands, g5 MUST stamp the post-repair holder set on the
  owner's registry entry (documented handoff) so `storage_locations`
  does not accumulate dead entries / miss new ones over slow data
  migration — the read path already finds data via the segment ring, so
  object rows never go stale from migration.
- **Announcements can be disabled (`announcements_enabled=false`)** —
  proving the reconciliation loop is the independent safety net.

## Known Gaps (handoff)

- **g5 drains `Node::repair_rx`** and **stamps the post-repair holder
  set** on the owner's registry entry (the migration stale-reference
  fix).
- **g8 owns the owner-unreachable metadata rebuild** (the corner Job B
  defers to).
