---
feature: "Durability Scheduler (ADR-0017) — Implementation Coordination"
epic: "refactoring/durability-scheduler"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: refactoring/composition-root-decomposition
    reason: c2-split-durability-builder must exist first — the ADR-0017 scheduler wrapper and task adaptors are constructed and registered inside DurabilityModule (roadmap wave 2 ① before ③)
  - epic: refactoring/store-unification
    reason: ADR-0032 (roadmap wave 2 ② before ③) delivers the unified SegmentDataStore the scheduler-registered tasks operate against; adaptors are written against oceanfs_storage_api::SegmentDataStore (its f1/f2/f3), so store unification lands before scheduler wiring
adr:
  - 0017-durability-task-abstraction
  - 0032-unify-segment-data-access
  - 0025-segment-lifecycle-state-machine
created: 2026-09-04
updated: 2026-09-04
---

# Durability Scheduler — Program Coordination

> **This is the coordination document for the ADR-0017 scheduler epic.** If
> you are implementing any feature under `refactoring/durability-scheduler/`,
> read this first — it tells you where your work sits in the whole, what must
> exist before you start, what is in/out of the scheduler and why, and what
> must not regress while you work. The per-feature docs are the authority for
> your feature; this document is the map.

## Summary

ADR-0017 (2026-08-09) proposed a `DurabilityTask` trait + `DurabilityScheduler`
to unify the durability background tasks (GC, orphan reaper, scrub, AE, heal):
one global concurrency semaphore, per-task interval loops, keyspace-fraction
round-robin partitioning, skip-if-still-running, error tolerance, and unified
`durability_*` metrics. **The ADR was never implemented.**

Today's reality (verified 2026-09-04): every background task runs its own
interval loop + `CancellationToken` inside
`crates/oceanfs-node/src/node.rs::spawn_background_tasks` (node.rs:3265-3545:
GC 3299-3321, AE 3337-3363, scrub 3373-3405, orphan reaper 3413-3434, heal
3464-3473), and each subsystem owns its own semaphore / concurrency control
(e.g. `HealWorker`'s `max_concurrent_heals` semaphore; the review anchor
`healing_service.rs:1030` creates a fresh `Semaphore(16)` per RPC call).
The code has also moved on from the storage-era layout the ADR described:
segments live in the ADR-0025 lifecycle registry/coordinator + ADR-0024 event
WAL, AE uses an incremental Merkle tree (ADR-0015), and ADR-0032 unifies
segment data access into one store.

This epic implements the ADR's *intent* against that reality: a
`DurabilityTask` trait whose implementations register with the state/deps
they actually need (adaptors capture the worker + its `Arc` deps), a
`DurabilityScheduler` that owns the interval loops / concurrency limit /
metrics, keyspace-fraction plumbing, and composition-root wiring in the
`DurabilityModule` builder (c2). The trait + scheduler live in
`crates/oceanfs-durability` (ADR-0005: trait in the consuming crate).

## Scope decision — which tasks are in / out of the scheduler

| Task | Today (verified) | In scheduler? | Why |
|---|---|---|---|
| GC (`GarbageCollector`, `gc/garbage_collector.rs`) | Interval loop in node.rs:3299; `run_cycle(metadata, &registry)` (garbage_collector.rs:232) | **Yes** | True interval task (ADR-0017 finding #21) |
| Orphan reaper (`OrphanReaper`, `gc/orphan_reaper.rs`) | Interval loop in node.rs:3413; `run_cycle()` (orphan_reaper.rs:120) | **Yes** | True interval task |
| Scrub (`ScrubCoordinator`, `scrub.rs`) | Interval loop in node.rs:3373; `run_cycle(registry, data_store)` (scrub.rs:708) | **Yes** | True interval task |
| Anti-entropy (`AntiEntropy`, `anti_entropy/engine.rs`) | Interval loop in node.rs:3337 dispatching `run_continuous_cycle()`/`run_cycle()` | **Yes** | Interval cadence, but **not** keyspace-sharded and **not** `concurrent_cycles` — keeps its ADR-0015 continuous/sampling internals intact |
| Heal worker (`HealWorker`, `heal/worker.rs`) | **Queue-driven**: `run(shutdown)` drains `HealQueue` (worker.rs:178); own `max_concurrent_heals` semaphore | **No** | Not interval-based. The ADR-era "heal interval" (spec §6.5) no longer exists as a loop; forcing a queue worker into an interval poll would be a redesign. Its own semaphore already bounds concurrency. Leave as-is; note in f1 |
| Reconciliation (`ReconciliationLoop`, `reconcile.rs`) | Event-driven (membership subscription) + bounded ticks + hourly drift scan; `run(self: Arc<Self>, shutdown)` (reconcile.rs:534) | **No** | Event+wake model; ADR-0017 §Considered-C rejected event-driven scheduling. Out with a clear note |
| Hinted handoff prune / delivery (`hinted_handoff/`) | Prune interval loop in node.rs:3486; delivery event+wake (`HintedHandoffManager`) | **No** | ADR-0017 explicitly excluded hint delivery; prune is internal WAL lifecycle owned by the manager, not a durability scan |
| Segment compactor (`gc/segment_compactor.rs`) | Runs **inside** `GarbageCollector::run_cycle` under `max_concurrent_compactions` | **No** (part of GC) | Not separately scheduled today; it is GC's compaction phase |
| `healing_service.rs:1030` per-RPC `Semaphore(16)` | gRPC request handler concurrency | **No** | Per-RPC, event-driven; not a background cycle. (It is the review anchor that motivated the global-semaphore idea; the scheduler bounds *background cycle* I/O, not RPC handler concurrency) |

## Feature DAG

```
[epic gates — roadmap wave 2 ① then ② before ③]
  refactoring/composition-root-decomposition/c2-split-durability-builder
  refactoring/store-unification/ f1-unify-trait → f2-single-impl → f3-single-instance-wiring
                                    │
                                    ▼
docs/features/refactoring/durability-scheduler/
  README.md                       ← this document (map)
  ├── f1-durability-task-trait    [critical]  DurabilityTask trait + adaptors (GC/orphan/scrub/AE)
  │        │
  │        ▼
  ├── f2-scheduler                [critical]  DurabilityScheduler engine: global semaphore,
  │        │                                  interval loops, skip/overrun, timeout, error
  │        │                                  tolerance, shutdown, durability_* metrics
  │        ▼
  ├── f3-keyspace-sharding        [medium]    keyspace_fraction round-robin mechanism;
  │                                           GC/orphan stay full-space (scan-reality constraint,
  │                                           orphan_reaper.rs:297 O(n) — MUST NOT get worse)
  │        │
  │        ▼
  └── f4-scheduler-wiring         [high]      build + register tasks + spawn scheduler in
                                              DurabilityModule (c2); delete the four per-task
                                              loops from node.rs; [durability] config; metrics
```

Ordering rules:

1. **c1 → c2 (composition root) then store-unification f1→f2→f3 then this
   epic.** The roadmap (§4) sequences wave 2 ① → ② → ③. f4 assumes
   `DurabilityModule` exists and the unified `oceanfs_storage_api::SegmentDataStore`
   is injected by `StorageModule`. Do not build `.dat` writers or scheduler
   wiring against the pre-unification store.
2. **f1 → f2 → f3 → f4 within this epic.** f2 needs the trait; f3 needs the
   trait's `keyspace_fraction` + the scheduler's rotation cursor; f4 wires the
   completed adaptors + scheduler into the node. f1–f3 are crate-level work in
   `oceanfs-durability` (no node dependency) and can land while c2/store
   unification are still finishing; f4 is the only feature that touches
   `oceanfs-node`.
3. **f3 may be re-scoped** by whoever implements it — see f3's scope section.
   It is deliberately the smallest feature: it exists to *not* make the
   full-space scans worse while shipping the partition mechanism.

## Reconciliation with ADR-0017 (decisions from 2026-09-04 triage)

The ADR's literal design is adjusted in the following ways, all recorded in
the per-feature docs:

1. **No `(metadata, segments)` on the trait.** The scheduler does not know or
   care what a task scans. Each adaptor owns the real deps
   (`Arc<dyn MetadataStore>` + `Arc<SegmentLifecycleRegistry>` for GC,
   `Arc<SegmentLifecycleRegistry>` + `Arc<dyn SegmentDataStore>` for scrub,
   etc.) and the trait is `async fn run_cycle(&self, window) -> Result<u64>`.
2. **GC and orphan reaper keep `keyspace_fraction = 1.0`.** Today GC runs
   `process_tombstones` over `list_tombstones_all()` +
   `list_objects_all_with_bucket()` + a full registry scan, and the reaper
   builds a full referenced set via `metadata.list_objects_all()`
   (orphan_reaper.rs:294-313; the `[review]` O(n) block at :297). The
   `MetadataStore` API has **no range-scan method**, so a per-cycle fraction
   cannot bound the dominant whole-store scans; ADR-0017's "GC at 0.1 = 10×
   cheaper cycles" premise does not hold on today's scan shape. Naive sharding
   would multiply the O(objects) scans 10× — the feature MUST NOT do that.
3. **AE is not keyspace-sharded.** ADR-0015's incremental tree + sampling is a
   different model; AE registers with `concurrent_cycles=false`,
   `keyspace_fraction=1.0`, and its existing continuous/full dispatch is
   preserved verbatim inside the adaptor. Do NOT force the tree into shards.
4. **Metrics keep domain counters.** Existing per-task counters
   (`gc_cycles_total`, `ae_segments_compared_total`,
   `scrub_segments_checked_total`, `orphan_segments_reaped_total`, …) stay —
   they carry domain detail. The scheduler *adds* the generic `durability_*`
   cycle metrics from ADR-0017 §4. Registration moves to one place
   (`DurabilityModule`, per c2 scope), which is what addresses finding #19's
   "constructors pollute" complaint at the wiring level.

## Epic-level DoD (ADR-0017 acceptance)

- [ ] `crates/oceanfs-durability` exposes `DurabilityTask` and
      `DurabilityScheduler`; adaptors exist for GC, orphan reaper, scrub, and
      AE; heal/reconciliation/hint-delivery are NOT wrapped (documented in
      this README + f1).
- [ ] The scheduler owns the interval loops: `grep -n "tokio::time::interval"`
      in `crates/oceanfs-node/src/node.rs` no longer matches gc/ae/scrub/
      orphan-reaper blocks (heal + hint-prune loops remain, they are
      queue/internal).
- [ ] One global concurrency semaphore bounds concurrently-running cycles
      across all registered tasks (`max_concurrent_tasks`); slow cycles do
      not queue back-to-back (skip/overrun accounting).
- [ ] Unified metrics registered once: `durability_cycle_total{task,status}`,
      `durability_cycle_duration_seconds{task}`,
      `durability_items_processed_total{task}`,
      `durability_cycle_skipped_total{task,reason}`, and
      `durability_scheduler_backlog`.
- [ ] GC + orphan reaper scan behavior is byte-for-byte unchanged versus
      today (full-space passes; same `segments_scanned` counts); the O(n)
      object-list concern (orphan_reaper.rs:297) is not worsened and is
      documented in f3.
- [ ] Config: new `[durability]` `max_concurrent_tasks` / `task_timeout_sec`
      in `NodeConfig`; individual task intervals continue to come from their
      existing `NodeConfig` fields (`gc_interval_sec`, `ae_interval_sec`,
      `scrub_interval_sec`, `orphan_reaper_interval_sec`) — no config
      relocation (ADR-0017 Neutral).
- [ ] Green: `cargo build --all-targets`; `cargo test -p oceanfs-durability
      --lib -- --test-threads=1`, `-p oceanfs-node --lib -- --test-threads=1`,
      and the node integration tests `durability_wiring`, `scrub_cycle`,
      `orphan_reaper`, `gc_compaction`, `read_write_roundtrip`,
      `node_lifecycle` (all `--test-threads=1` per PIPELINE.md §4.6) pass.
- [ ] Node boots and the e2e write/read scenario is green with the scheduler
      running GC/orphan/scrub/AE on their configured intervals.

## References

- ADR-0017 (this epic's decision), ADR-0032 (unified store — substrate),
  ADR-0025 (lifecycle registry/coordinator — the machine tasks read),
  ADR-0015 (AE incremental tree — why AE is not sharded), ADR-0005
  (trait in consuming crate)
- Review triage roadmap: `docs/features/refactoring/review-2026-09-roadmap.md`
  §2 Theme 2, §3 wave 2 ③ (anchors `healing_service.rs:1030`,
  `garbage_collector.rs:160,192`, `health.rs:83`, `node.rs:1932,2620`,
  `write/coordinator.rs:382`)
- Composition root: `docs/features/refactoring/composition-root-decomposition/`
  (c2 = `modules/durability.rs`, "ADR-0017 scheduler lands here as the
  wrapper"; c5 moves remaining spawns)
- Store unification: `docs/features/refactoring/store-unification/README.md`
