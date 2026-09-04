---
feature: "c2: Extract DurabilityModule Builder"
epic: "refactoring/composition-root-decomposition"
status: done
priority: high
owner: ""
dependencies:
  - feature: c1-split-storage-builder
    epic: refactoring/composition-root-decomposition
    reason: Durability workers consume the single shared store and lifecycle registry from StorageModule
adr:
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# c2: Extract DurabilityModule Builder

## Summary

Extract construction of the durability workers (`Node::start()` §7,
7b–7d: GC, AE + merkle tree, scrub, reaper, heal pipeline, reconcile
loop, re-replication worker + dispatcher, op timeouts) into
`modules/durability.rs`. Returns a `DurabilityModule` bundle owning the
workers (one `Arc` per background worker + the shared cross-worker
handles), built against `StorageModule`'s **single** store (consolidated
in c1). The extraction scope is §7 only: hinted handoff + its manager
stay in `start()` §11 (c5 territory) and `remap_alias` is a c1
`StorageModule` field — see Accepted Deviations D1.

> The struct sketch below is the **pre-implementation** plan
> (2026-09-04), kept as the deviation anchor. The shipped
> `DurabilityModule` differs from it in five user-approved points
> (field list, `heal` owner type, metrics registration, `build`
> signature, spawn surface) — each is recorded once in **Accepted
> Deviations** at the end of this document.

```rust
pub struct DurabilityModule {
    pub gc: Arc<GarbageCollector>,
    pub ae: Arc<AntiEntropy>,
    pub scrub: Arc<ScrubCoordinator>,
    pub reaper: Arc<OrphanReaper>,
    pub heal_worker: HealWorker,
    pub reconciliation: Arc<ReconciliationLoop>,
    pub rep_worker: Arc<ReRepWorker>,
    pub repair_dispatcher: Arc<crate::repair::RepairDispatcher>,
    pub hinted_handoff_manager: Arc<HintedHandoffManager>,
    pub op_timeouts: Arc<OperationTimeouts>,
    // (ADR-0017 scheduler lands here in a later epic as the wrapper)
}
```

## Scope

### In Scope
- Move durability-worker construction out of `start()` unchanged (pure
  move), wiring each worker to the c1 shared store/lifecycle.
- Register the workers' metrics in one place (the node's central registry
  is passed in).
- Delete the now-dead 4+ extra `DiskSegmentStore::new`/`ShardStore::new`
  calls in the durability section (they disappear with c1 consolidation).

### Out of Scope
- Server/handler construction (c3).
- ADR-0017 scheduler implementation (separate epic; this module is where
  its wrapper will live).

## Definition of Done

- [x] `node.rs` shrinks by the durability sections; workers constructed in
      `modules/durability.rs`.
<!-- REVIEW: verified — node.rs 3738 → 3468 lines (−270); the §7 inline block (node.rs:513-524) is replaced by the DurabilityModule::build call (node.rs:518-524); all 12 workers/handles are constructed only in modules/durability.rs::build (durability.rs:97-377); modules/mod.rs:9 declares the module. -->
- [x] Node boots; durability background tasks run on schedule; metrics
      registered.
<!-- REVIEW: verified — e2e crash_restart/wal_recovery/segment_lifecycle/garbage_collection/rewrite_leak_test/cluster_lifecycle(4/4) all green locally; node integration suites (gc_compaction, scrub_cycle, reconciliation, re_replication, etc.) green; metrics registered once via the single module call durability.register_metrics(&*metrics) at node.rs:888 (§12 — see D3); global heal queue init (durability.rs:340) still precedes every enqueue_heal caller (workers spawned §16, after build). -->
- [x] No new `DiskSegmentStore`/`DiskSegmentShardStore` construction
      outside `StorageModule`.
<!-- REVIEW: verified — only production sites are modules/storage.rs:385/388 (c1); durability.rs constructs no stores; remaining occurrences are pre-existing #[cfg(test)] fixtures in oceanfs-durability. -->
- [x] Node tests green.
<!-- REVIEW: verified — oceanfs-node lib 66/66 (incl. 2 new modules::durability tests — the third, the weak build_initializes_the_global_heal_queue smoke test, was removed as LOW-2 below; re-verified 66 passed after both LOW fixes), all 30 integration files green (165 tests), oceanfs-durability lib 265/265; fmt/clippy (-D warnings) clean; RUSTDOCFLAGS="-D warnings" cargo doc clean. -->

## Accepted Deviations

All five deviations were approved by the user BEFORE implementation
(2026-09-04). The independent reviewer returned **PASS (0 blocking
gaps; 2 LOW items, both since fixed by the implementer)**:

1. The `Node` struct doc comment now reflects the c2 module layout
   (`Node::start()` builds the durability bundle via
   `DurabilityModule::build`; §16 spawns from `durability.*` clones).
2. The weak `build_initializes_the_global_heal_queue` smoke test was
   removed — global heal-queue fidelity (init at durability.rs:340
   precedes every `enqueue_heal` caller) is covered by node boot/e2e
   runs through the scrub/GC heal-enqueue paths.

Node lib re-verified after both fixes: **66 passed** (see DoD).

- **D1 — extraction scope is §7 only; the plan's `remap alias` and
  `hinted_handoff_manager` entries are stale.** The pre-implementation
  Summary enumerated "… op timeouts, remap alias" and the sketch above
  lists `hinted_handoff_manager`; neither ships in the module — hinted
  handoff + its manager stay in `Node::start()` §11 (they are c5
  background-spawn-extraction territory, not §7), and `remap_alias` is
  a `StorageModule` field (c1). The shipped `DurabilityModule`
  carries **12 pub(crate) fields**: `gc`, `ae`, `scrub`, `reaper`,
  `heal` (D2), `reconciliation`, `rep_worker`, `repair_dispatcher`,
  `op_timeouts`, plus `ec_decoder`, `codec_config` and
  `announce_metrics` — the last three are exposed because §8–§17
  consumers need them (the read coordinator's codec/decoder path and
  the §16b loss-announcer's transmit counters).
- **D2 — `heal` is `Arc<HealWorker>`; the sketch's plain
  `heal_worker` value is superseded.** `HealWorker::run` changed
  `self` → `&self` (oceanfs-durability/src/heal/worker.rs): the queue
  receiver is interior-mutable and is taken once through the queue, so
  the worker runs from behind an `Arc` (the node's §16 spawn passes
  `durability.heal.clone()`); a second concurrent `run` observes `None`
  and exits.
- **D3 — metrics registration is a module method; the registry is not
  hoisted.** The module does not own a metrics registry (the plan's
  "owning … metrics" wording): `DurabilityModule::register_metrics(&self,
  registrar: &dyn MetricRegistrar)` registers every worker's counters in
  one call, invoked once from the node's §12 block
  (`durability.register_metrics(&*metrics)` at node.rs:888). The central
  `MetricsRegistry` creation was NOT hoisted into the module — §12 keeps
  creating it and registering every other subsystem bundle as before.
  The method is idempotent (covered by the
  `register_metrics_covers_all_workers` test).
- **D4 — build signature.** `pub(crate) async fn build(config:
  &NodeConfig, storage: &StorageModule, membership: Arc<Membership>,
  pool: Arc<ConnectionPool>) -> Result<Self, String>` — async because
  the AE merkle rebuild scans the lifecycle registry; `membership` and
  `pool` are passed because the workers and the compaction-remap
  closure need them (still owned by `Node::start()` — c4 re-homes
  them).
- **D5 — `spawn_background_tasks` keeps its parameter list.** Only the
  `heal_worker` parameter's type changed (plain `HealWorker` →
  `Arc<HealWorker>`; call site passes `durability.heal.clone()`). The
  rest of the spawn-surface rework (module-bundle parameters, moving
  the spawns into the modules) is c5's job, not §7's.
