---
feature: "c2: Extract DurabilityModule Builder"
epic: "refactoring/composition-root-decomposition"
status: proposed
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

Extract construction of the durability workers (node.rs sections 7, 7b–7d:
GC, AE + merkle tree, scrub, reaper, heal pipeline, reconcile loop,
re-replication worker + dispatcher, op timeouts, remap alias) into
`modules/durability.rs`. Returns a `DurabilityModule` bundle owning the
workers and metrics, all built against `StorageModule`'s **single** store
(consolidated in c1).

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

- [ ] `node.rs` shrinks by the durability sections; workers constructed in
      `modules/durability.rs`.
- [ ] Node boots; durability background tasks run on schedule; metrics
      registered.
- [ ] No new `DiskSegmentStore`/`DiskSegmentShardStore` construction
      outside `StorageModule`.
- [ ] Node tests green.
