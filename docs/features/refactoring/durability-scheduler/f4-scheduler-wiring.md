---
feature: "f4: Scheduler Wiring in the Durability Builder"
epic: "refactoring/durability-scheduler"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: f1-durability-task-trait
    epic: refactoring/durability-scheduler
    reason: The adaptors (GcTask/OrphanTask/ScrubTask/AeTask) are constructed and registered here
  - feature: f2-scheduler
    epic: refactoring/durability-scheduler
    reason: The DurabilityScheduler engine is instantiated, registered with metrics, and spawned here
  - feature: f3-keyspace-sharding
    epic: refactoring/durability-scheduler
    reason: Registration honors f3's decision (GC/orphan at keyspace_fraction 1.0) and its guard
  - feature: c2-split-durability-builder
    epic: refactoring/composition-root-decomposition
    reason: "DurabilityModule (crates/oceanfs-node/src/modules/durability.rs) is where the ADR-0017 scheduler wrapper lives (c2 doc: the ADR-0017 scheduler lands here in a later epic as the wrapper)"
  - feature: c1-split-storage-builder
    epic: refactoring/composition-root-decomposition
    reason: StorageModule provides the single lifecycle registry + metadata store the adaptors capture
  - feature: f3-single-instance-wiring
    epic: refactoring/store-unification
    reason: The single unified Arc<dyn oceanfs_storage_api::SegmentDataStore> from StorageModule is injected into the scrub worker the ScrubTask wraps (ADR-0032 D4)
  - feature: f2-single-impl
    epic: refactoring/store-unification
    reason: GC/AE/heal data paths use the unified storage impl before the scheduler drives them more frequently
adr:
  - 0017-durability-task-abstraction
  - 0032-unify-segment-data-access
  - 0025-segment-lifecycle-state-machine
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f4: Scheduler Wiring in the Durability Builder

## Summary

Wire ADR-0017 into the running node. Inside `DurabilityModule::build`
(`crates/oceanfs-node/src/modules/durability.rs`, from c2) construct the four
task adaptors (f1), construct the `DurabilityScheduler` (f2) with the new
`[durability]` config values, register the tasks, and register the unified
metrics — then delete the four per-task interval loops from
`node.rs::spawn_background_tasks` (GC node.rs:3299-3321, AE 3337-3363, scrub
3373-3405, orphan reaper 3413-3434) and drive them from the scheduler's
single spawn. Intervals continue to come from their existing `NodeConfig`
fields (`gc_interval_sec`, `ae_interval_sec`, `scrub_interval_sec`,
`orphan_reaper_interval_sec`) per ADR-0017's "no config relocation". Add the
new scheduler-level config (`max_concurrent_tasks`, `task_timeout_sec`) as a
new `[durability]` `DurabilityConfig` in `oceanfs-core`.

## Scope

### In Scope
- New `DurabilityConfig` in `oceanfs-core`:
  `max_concurrent_tasks: usize` (default 2 — semaphore permits) and
  `task_timeout_sec: u64` (default 3600); serde defaults; `NodeConfig` gains
  `#[serde(default)] pub durability: crate::DurabilityConfig`.
- `DurabilityModule::build` constructs and registers:
  `GcTask`, `OrphanTask`, `ScrubTask`, `AeTask` with intervals read from
  `config.gc_interval_sec`, `config.orphan_reaper_interval_sec`,
  `config.scrub_interval_sec`, `config.ae_interval_sec`, and the deps from
  `StorageModule` (metadata store, lifecycle registry, single
  `oceanfs_storage_api::SegmentDataStore`).
- `DurabilityModule` owns the scheduler: fields
  `scheduler: Arc<DurabilityScheduler>`, `scheduler_cancel:
  CancellationToken`, and a `spawn_scheduler(&self)` entry point that spawns
  the loops (per c2/c5 "modules expose their own spawn" — no `tokio::spawn`
  inside `Node::start()`).
- Metrics: scheduler `durability_*` metrics (f2) + the existing per-worker
  metrics are all registered in `DurabilityModule::build` against the node's
  central `oceanfs_server::admin::MetricsRegistry` (c2 already centralizes
  worker-metric registration here).
- `node.rs` `spawn_background_tasks` slimming: remove the gc/ae/scrub/orphan
  interval loops and their `apply_background_io_class` /
  `apply_background_cpu_sched` prologues (the scheduler applies niceness per
  task name at spawn, honoring `config.background_io_class_idle` /
  `background_cpu_sched_idle`); keep the heal worker's queue-driven spawn and
  the hinted-handoff prune loop untouched.
- `BackgroundTasks`: replace `gc`/`gc_cancel`, `anti_entropy`/`ae_cancel`,
  `scrub`/`scrub_cancel`, `orphan_reaper`/`reaper_cancel` with
  `durability_scheduler: Option<JoinHandle<()>>` + `scheduler_cancel`;
  `Node::shutdown` (node.rs:3107) cancels `scheduler_cancel` instead of the
  four per-task tokens.
- Health integration: existing task-health signals that relied on per-task
  join-handle liveness keep working through the scheduler's metrics
  (`durability_cycle_total` moving per task) — see the review anchor
  `health.rs:83` note in the README.

### Out of Scope (for this feature)
- The engine/trait/adaptor work (f1-f3).
- Removing the heal, reconciliation, hint-prune, or hint-delivery spawns
  (queue/event-driven — not scheduler tasks; README scope table).
- Moving the four tasks' interval *values* into `[durability]` or into worker
  configs (ADR-0017 Neutral; intervals stay in their current `NodeConfig`
  fields).
- The c5 background-spawn extraction (that epic moves the *remaining* spawns
  into `modules/background.rs`); this feature only removes the scheduler-managed
  loops and adds the scheduler handle where `spawn_background_tasks` builds
  `BackgroundTasks`.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New `src/config/durability.rs` (`DurabilityConfig` + serde defaults); `config/mod.rs` re-export; `NodeConfig.durability` field (config/node.rs, near the other background-item fields) |
| `oceanfs-durability` | None (f1-f3 landed the types); optionally a `DurabilityConfig`→scheduler-args mapping helper is acceptable here |
| `oceanfs-node` | `modules/durability.rs` (c2) builds + registers tasks and spawns the scheduler; `node.rs::spawn_background_tasks` drops four loops; `BackgroundTasks` struct + `Node::shutdown` updated |

## Interface (Public API)

```rust
// oceanfs-core: crates/oceanfs-core/src/config/durability.rs
/// Scheduler-level durability configuration (ADR-0017 §2).
#[derive(Debug, Clone)]
pub struct DurabilityConfig {
    /// Global semaphore permits — bounds total concurrent durability I/O
    /// across all registered tasks. Default 2.
    #[serde(default = "default_max_concurrent_tasks")]
    pub max_concurrent_tasks: usize,
    /// Maximum duration of a single cycle before it is timed out
    /// (seconds). Default 3600. 0 disables the timeout.
    #[serde(default = "default_task_timeout_sec")]
    pub task_timeout_sec: u64,
}
// NodeConfig (config/node.rs) gains:
//   #[serde(default)] pub durability: crate::DurabilityConfig,

// oceanfs-node: modules/durability.rs (post-c2)
pub struct DurabilityModule {
    // ...workers from c2...
    /// ADR-0017 scheduler wrapper. Constructed in build(); spawned by
    /// spawn_scheduler() once the node is ready to start background work.
    pub scheduler: Arc<oceanfs_durability::DurabilityScheduler>,
    pub scheduler_cancel: CancellationToken,
    pub scheduler_handle: Option<JoinHandle<()>>,
}

impl DurabilityModule {
    /// Registers GcTask/OrphanTask/ScrubTask/AeTask and the durability_*
    /// metrics. Intervals come from `config.<task>_interval_sec`.
    // (performed inside build(); signature per c2's builder shape)
    pub fn build(cfg: &NodeConfig, storage: &StorageModule) -> DurabilityModule;

    /// Spawns the scheduler loops (f2). No tokio::spawn inside Node::start().
    pub fn spawn_scheduler(&mut self, shutdown: CancellationToken);
}
```

Wiring values (preserving today's cadence exactly):

| Adaptor | Worker | Interval source (`NodeConfig`) | Deps captured |
|---|---|---|---|
| `GcTask` | `Arc<GarbageCollector>` (post-c1: with the shared data store/lifecycle/shard store) | `config.gc_interval_sec` | `metadata_store`, `lifecycle_registry` |
| `OrphanTask` | `Arc<OrphanReaper>` | `config.orphan_reaper_interval_sec` | — (reaper holds its deps) |
| `ScrubTask` | `Arc<ScrubCoordinator>` | `config.scrub_interval_sec` | `lifecycle_registry`, `storage.data_store` |
| `AeTask` | `Arc<AntiEntropy>` | `config.ae_interval_sec` | — (AE holds registry/store/tree) |

All registered with `keyspace_fraction = 1.0` per f3; the f3 guard stays
in the adaptors.

## Data Flow

```
NodeConfig { durability: { max_concurrent_tasks: 2, task_timeout_sec: 3600 },
             gc_interval_sec, scrub_interval_sec, orphan_reaper_interval_sec,
             ae_interval_sec, ... }
   │
   ▼
DurabilityModule::build(cfg, &storage)                 // c2 module
   ├─ GcTask / OrphanTask / ScrubTask / AeTask::new(...)
   ├─ DurabilityScheduler::new(max_concurrent_tasks, timeout, Some(&metrics))
   ├─ scheduler.register(Arc<dyn DurabilityTask> × 4)
   └─ scheduler.register_metrics(&metrics)             // durability_*
   │
   ▼
Node::start(): after the join/readiness gate
   └─ durability.spawn_scheduler(scheduler_cancel)     // one task per loop
   │
   ▼
BackgroundTasks { durability_scheduler: Some(handle), scheduler_cancel }
   ... existing heal/hint/reconcile spawns unchanged ...
   │
   ▼
Node::shutdown(): scheduler_cancel.cancel(); await handle
   (replaces gc_cancel/ae_cancel/scrub_cancel/reaper_cancel cancels)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds.
- [ ] **Config tests:** new `DurabilityConfig` defaults (2 / 3600) and TOML
      parsing (`max_concurrent_tasks`, `task_timeout_sec`, and 0 = timeout
      disabled) are covered in `crates/oceanfs-core` config tests.
- [ ] **Tests:** `cargo test -p oceanfs-node --lib -- --test-threads=1`
      (PIPELINE.md §4.6) passes. `grep -n "tokio::time::interval"` in
      `crates/oceanfs-node/src/node.rs` matches no gc/ae/scrub/orphan-reaper
      block.
- [ ] **Integration (all `--test-threads=1`):**
      `cargo test -p oceanfs-node --test durability_wiring`,
      `--test scrub_cycle`, `--test orphan_reaper`, `--test gc_compaction`,
      `--test read_write_roundtrip`, `--test node_lifecycle` pass with the
      scheduler driving the four tasks.
- [ ] **Boot/e2e:** a node boots with the scheduler running; GC/orphan/scrub/
      AE each move `durability_cycle_total{task}` on their configured
      intervals (visible on the metrics endpoint); shutdown cancels the
      scheduler token and the node exits cleanly.
- [ ] **Metrics:** `durability_cycle_total`, `durability_cycle_duration_seconds`,
      `durability_items_processed_total`, `durability_cycle_skipped_total`,
      `durability_scheduler_backlog` are registered once; no duplicate
      registration warnings from the central registry.
- [ ] **ADR:** ADR-0017 §2 config (`max_concurrent_tasks`, `task_timeout_sec`)
      lands under `[durability]`; task intervals are not relocated;
      heal/reconciliation/hint delivery remain outside the scheduler (README
      scope table is the reference).
- [ ] **Docs:** every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes in touched crates.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).
