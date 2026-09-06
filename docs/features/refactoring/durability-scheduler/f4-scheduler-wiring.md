---
feature: "f4: Two-Tier Budget + Scheduler Wiring"
epic: "refactoring/durability-scheduler"
status: in_progress
priority: high
owner: ""
dependencies:
  - feature: f1-durability-task-trait
    epic: refactoring/durability-scheduler
    reason: The adaptors (GcTask/OrphanTask/ScrubTask/AeTask) are constructed and registered here
  - feature: f2-scheduler
    epic: refactoring/durability-scheduler
    reason: DurabilityBudget + DurabilityScheduler are instantiated, injected into the Tier-0 workers, registered with metrics, and spawned here
  - feature: f3-keyspace-sharding
    epic: refactoring/durability-scheduler
    reason: Registration honors f3's decision (GC/orphan at keyspace_fraction 1.0) and its guard
  - feature: c2-split-durability-builder
    epic: refactoring/composition-root-decomposition
    reason: "DurabilityModule (crates/oceanfs-node/src/modules/durability.rs) is where the ADR-0017 budget + scheduler wrapper lives (c2 doc: the ADR-0017 scheduler lands here in a later epic as the wrapper)"
  - feature: c1-split-storage-builder
    epic: refactoring/composition-root-decomposition
    reason: StorageModule provides the single lifecycle registry + metadata store the adaptors capture
  - feature: f3-single-instance-wiring
    epic: refactoring/store-unification
    reason: The single unified Arc<dyn oceanfs_storage_api::SegmentDataStore> from StorageModule is injected into the scrub worker the ScrubTask wraps (ADR-0032 D4)
  - feature: f2-single-impl
    epic: refactoring/store-unification
    reason: GC/AE/heal data paths use the unified storage impl before the scheduler drives them
adr:
  - 0017-durability-task-abstraction
  - 0032-unify-segment-data-access
  - 0025-segment-lifecycle-state-machine
perf: []
created: 2026-09-04
updated: 2026-09-06
---

# f4: Two-Tier Budget + Scheduler Wiring

> **FINAL STATE (2026-09-06):** implementation COMPLETE + independent review
> verdict **PASS** (iteration 2); status intentionally **`in_progress`** — the
> **Boot/e2e** DoD item below is a cloud-harness-only external gate that has
> not been executed (PIPELINE.md §6) and must not be marked done until it is.
> Everything locally verifiable is green and recorded `[x]`: fmt, `cargo build
> --all-targets`, clippy `-D warnings` (core/storage/durability/node),
> rustdoc `-D warnings`, lib tests (core 231, storage 458, durability 276
> incl. 19 scheduler tests, node 66), and the node integration suite
> `durability_wiring` / `scrub_cycle` / `orphan_reaper` / `gc_compaction` /
> `read_write_roundtrip` / `node_lifecycle` (all `--test-threads=1`). This is
> the **sole remaining gate** for the whole epic — once the harness e2e runs
> green, flip this doc (and the epic README) to `done` and close the Boot/e2e
> item. Accepted deviations vs the original spec wording are recorded inline
> under Definition of Done below (items 3–4 of the epic's recorded
> deviations).

## Summary

Wire the amended ADR-0017 into the running node. Inside `DurabilityModule`
(`crates/oceanfs-node/src/modules/durability.rs`):

1. Construct the **shared `DurabilityBudget`** (f2) from `[durability]`
   config and hand it to the Tier-0 workers and the healing gRPC service so
   their private concurrency gates are **replaced** by Tier-0 acquisitions
   (single gate).
2. Construct the four task adaptors (f1) and the `DurabilityScheduler` (f2),
   register the tasks + metrics.
3. Delete the four per-task interval loops from `DurabilityModule::spawn_loops`
   (GC, AE, scrub, orphan reaper) and drive them from the scheduler's single
   spawn, which acquires Tier-1 permits.
4. Remove the io/cpu niceness prologues and the
   `background_io_class_idle` / `background_cpu_sched_idle` config fields
   (ADR-0017 amendment — helpers deleted).
5. Add `[durability]` `DurabilityConfig` in `oceanfs-core` and remove the
   now-superseded `heal_parallel_segments`.

Intervals continue to come from their existing `NodeConfig` fields
(`gc_interval_sec`, `ae_interval_sec`, `scrub_interval_sec`,
`orphan_reaper_interval_sec`) per ADR-0017's "no config relocation".

## Scope

### In Scope
- New `DurabilityConfig` in `oceanfs-core` (`src/config/durability.rs`):
  - `repair_max_active: usize` — Tier-0 permits. Default 16. **Single gate**
    for heal ops + re-rep pulls + inbound hint batches.
  - `housekeeping_max_active: usize` — Tier-1 permits. Default 2.
  - `task_timeout_sec: u64` — per-cycle timeout. Default 3600; 0 disables.
  - serde defaults; `NodeConfig` gains
    `#[serde(default)] pub durability: crate::DurabilityConfig`.
- Remove from `NodeConfig`: `heal_parallel_segments` (replaced by
  `durability.repair_max_active`), `background_io_class_idle`,
  `background_cpu_sched_idle` (helpers removed).
- `DurabilityModule::build`:
  - constructs `Arc<DurabilityBudget>` from config;
  - constructs `GcTask`, `OrphanTask`, `ScrubTask`, `AeTask` with intervals
    from `config.<task>_interval_sec` and deps from `StorageModule`;
  - constructs `Arc<DurabilityScheduler>` with the budget + `task_timeout`;
  - registers the four tasks (all `keyspace_fraction = 1.0`, f3 guard);
  - stores the budget Arc in a field the server builder can reach (see
    "Healing service" below).
- Tier-0 worker wiring (their private gates were deleted in f2):
  - `HealWorker` constructed with the budget; each heal op acquires a Tier-0
    permit (`modules/durability.rs` heal pipeline section).
  - `ReRepWorker` constructed with the budget; each pull/write acquires a
    Tier-0 permit.
  - Healing gRPC service (built in the c3/c5 server builder): the hint-batch
    handler acquires a Tier-0 permit per batch; the per-RPC `Semaphore(16)`
    as a cross-RPC gate is deleted. An intra-batch fetch cap remains inside
    the permit as within-operation parallelism (same rule as heal shard
    fetch / scrub batch concurrency).
- `DurabilityModule` owns the scheduler: fields `scheduler:
  Arc<DurabilityScheduler>`, `scheduler_cancel: CancellationToken`,
  `scheduler_handle: Option<JoinHandle<()>>`, and a `spawn_scheduler(&self,
  shutdown)` entry point (c5: modules expose their own spawn — no
  `tokio::spawn` inside `Node::start()`).
- Metrics: budget metrics + scheduler `durability_cycle_*` metrics (f2) +
  the existing per-worker metrics all registered against the node's central
  `oceanfs_server::admin::MetricsRegistry`.
- `spawn_loops` slimming (`modules/durability.rs`): remove the gc/ae/scrub/
  orphan interval loops + their `apply_background_io_class` /
  `apply_background_cpu_sched` prologues; keep the heal worker's queue-driven
  run, the hint-prune loop, the delivery watcher, reconciliation, and the
  re-rep worker + dispatcher spawns (unchanged apart from the Tier-0 wiring).
- `BackgroundTasks`: replace `gc`/`gc_cancel`, `anti_entropy`/`ae_cancel`,
  `scrub`/`scrub_cancel`, `orphan_reaper`/`reaper_cancel` with
  `durability_scheduler: Option<JoinHandle<()>>` + `scheduler_cancel`;
  shutdown cancels `scheduler_cancel` instead of the four per-task tokens.
- Health integration: task-health signals that relied on per-task
  join-handle liveness keep working through `durability_cycle_total` moving
  per task.

### Out of Scope (for this feature)
- The engine/trait/adaptor/budget work (f1–f3) — f4 only wires them.
- Removing the heal, reconciliation, hint-prune, hint-delivery, re-rep, or
  dispatcher spawns (queue/event-driven — not scheduler tasks).
- Moving the four tasks' interval *values* into `[durability]` (ADR-0017
  Neutral).
- Device-level io-class separation (explicitly rejected by the amendment).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New `src/config/durability.rs` (`DurabilityConfig` + serde defaults); re-export; `NodeConfig.durability` field; remove `heal_parallel_segments`, `background_io_class_idle`, `background_cpu_sched_idle` + their defaults/tests |
| `oceanfs-durability` | (f1–f3 landed the types + Tier-0 client changes); no further change here beyond optional config→budget mapping helper |
| `oceanfs-node` | `modules/durability.rs`: build budget + scheduler, register tasks, wire Tier-0 into heal/re-rep, delete the four loops + niceness prologues, expose `spawn_scheduler`; `modules/server.rs`: pass the budget into `HealingGrpcService`; `node.rs`: `BackgroundTasks` fields + shutdown |
| `oceanfs-storage` | `src/io/sched.rs` + `io/mod.rs` re-export: delete `apply_background_io_class` / `apply_background_cpu_sched` |

## Interface (Public API)

```rust
// oceanfs-core: crates/oceanfs-core/src/config/durability.rs
/// Scheduler/budget-level durability configuration (ADR-0017 amendment).
#[derive(Debug, Clone)]
pub struct DurabilityConfig {
    /// Tier-0 (repair) permits — bounds concurrent heal ops + re-rep
    /// pulls + inbound hint batches node-wide. Default 16.
    #[serde(default = "default_repair_max_active")]
    pub repair_max_active: usize,
    /// Tier-1 (housekeeping) permits — bounds concurrent scheduled
    /// cycles (GC/orphan/scrub/AE). Default 2.
    #[serde(default = "default_housekeeping_max_active")]
    pub housekeeping_max_active: usize,
    /// Maximum duration of a single Tier-1 cycle (seconds). Default
    /// 3600. 0 disables the timeout.
    #[serde(default = "default_task_timeout_sec")]
    pub task_timeout_sec: u64,
}
// NodeConfig (config/node.rs): #[serde(default)] pub durability: crate::DurabilityConfig,

// oceanfs-node: modules/durability.rs
pub(crate) struct DurabilityModule {
    // ...workers from c2...
    /// Two-tier admission budget (ADR-0017 amendment) shared by the
    /// scheduler (Tier-1) and heal/re-rep/hint-apply (Tier-0). Also
    /// handed to the server builder for the healing gRPC service.
    pub(crate) budget: Arc<oceanfs_durability::DurabilityBudget>,
    /// ADR-0017 scheduler wrapper. Constructed + tasks registered in
    /// build(); spawned by spawn_loops (c5 module-owned spawn pattern).
    pub(crate) scheduler: Arc<oceanfs_durability::DurabilityScheduler>,
}

// The spawn handle + cancellation token live on `BackgroundTasks`
// (node.rs), exactly like the other module-owned loops:
//   background.durability_scheduler: Option<JoinHandle<()>>
//   background.scheduler_cancel: CancellationToken
// `DurabilityModule::spawn_loops` spawns `self.scheduler` and records
// both on `bg`. No tokio::spawn inside Node::start().
```

Wiring values (preserving today's cadence exactly):

| Adaptor | Worker | Interval source (`NodeConfig`) | Deps captured |
|---|---|---|---|
| `GcTask` | `Arc<GarbageCollector>` | `config.gc_interval_sec` | `metadata_store`, `lifecycle_registry` |
| `OrphanTask` | `Arc<OrphanReaper>` | `config.orphan_reaper_interval_sec` | — (reaper holds its deps) |
| `ScrubTask` | `Arc<ScrubCoordinator>` | `config.scrub_interval_sec` | `lifecycle_registry`, `storage.data_store` |
| `AeTask` | `Arc<AntiEntropy>` | `config.ae_interval_sec` | — (AE holds registry/store/tree) |

Tier-0 budget clients (single gate): `HealWorker` (1 permit per heal op),
`ReRepWorker` (1 permit per pull/write), healing gRPC hint handler (1 permit
per hint batch). Their former private gates
(`HealConfig.max_concurrent_heals`, `ReRepConfig.max_concurrent_repairs`, the
per-RPC `Semaphore(16)`) were deleted in f2.

## Data Flow

```
NodeConfig { durability: { repair_max_active: 16, housekeeping_max_active: 2,
             task_timeout_sec: 3600 },
             gc_interval_sec, scrub_interval_sec, orphan_reaper_interval_sec,
             ae_interval_sec, ... }
   │
   ▼
DurabilityModule::build(cfg, &storage)
   ├─ budget = Arc::new(DurabilityBudget::new(repair, housekeeping))
   ├─ GcTask / OrphanTask / ScrubTask / AeTask::new(...)
   ├─ scheduler = DurabilityScheduler::new(budget.clone(), timeout)
   ├─ scheduler.register(Arc<dyn DurabilityTask> × 4)
   ├─ heal / rep_worker constructed with budget.clone()      // Tier-0
   ├─ budget Arc exposed for the server builder → HealingGrpcService
   └─ budget.register_metrics(&metrics); scheduler.register_metrics(&metrics)
   │
   ▼
BackgroundTasks.spawn_all (c5): durability.spawn_scheduler(scheduler_cancel)
   │  each Tier-1 cycle: budget.acquire_housekeeping() → task.run_cycle()
   ▼
Node::shutdown(): scheduler_cancel.cancel(); await scheduler_handle
   (replaces gc_cancel/ae_cancel/scrub_cancel/reaper_cancel cancels)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds.
- [x] **Config tests:** `DurabilityConfig` defaults (16 / 2 / 3600) and TOML
      parsing (`repair_max_active`, `housekeeping_max_active`,
      `task_timeout_sec`, and 0 = timeout disabled) are covered in
      `crates/oceanfs-core` config tests; `heal_parallel_segments`,
      `background_io_class_idle`, `background_cpu_sched_idle` are gone from
      `NodeConfig`.
<!-- REVIEW: verified 2026-09-06 (iter 2, verdict PASS). durability_config_defaults (crates/oceanfs-core/src/config/node.rs:832-836) asserts the 16/2/3600 defaults on NodeConfig.durability; durability_config_toml_parse (node.rs:839-858) parses an explicit `[durability]` TOML with repair_max_active=8/housekeeping_max_active=4/task_timeout_sec=0 (asserts 0 => disabled) and asserts default fallback on an unset section. Both pass. `heal_parallel_segments`, `background_io_class_idle`, `background_cpu_sched_idle` removed from NodeConfig (grep: no matches in crates/).
-->
- [x] **Tests:** `cargo test -p oceanfs-node --lib -- --test-threads=1`
      (PIPELINE.md §4.6) passes. `grep -n "tokio::time::interval"` in
      `crates/oceanfs-node/src/modules/durability.rs` matches no
      gc/ae/scrub/orphan-reaper block (hint-prune remains).
- [x] **Gates removed:** the per-worker/cross-RPC semaphore gates are
      deleted — no worker-level `Semaphore::new` remains in `heal/worker.rs`
      or `repair.rs` (each heal op / pull-write now acquires a Tier-0 permit
      from the shared budget), and the healing-service hint handler acquires
      one Tier-0 permit per batch (the old per-RPC `Semaphore(16)` that
      bounded nothing across calls is gone as a gate);
      `HealConfig.max_concurrent_heals` and `ReRepConfig.max_concurrent_repairs`
      are removed.
      An intra-batch fetch cap (`FETCH_CONCURRENCY = 16`, healing_service.rs:1069)
      and an intra-pull holder-fetch cap (`MAX_PARALLEL_FETCHES`, repair.rs:545)
      remain as within-operation parallelism inside the Tier-0 permit (same
      rule as heal shard fetch / scrub batch concurrency — ADR-0017
      amendment).
<!-- REVIEW: verified 2026-09-06 (iter 2, verdict PASS). heal/worker.rs and repair.rs no longer construct a worker semaphore; HealConfig.max_concurrent_heals and ReRepConfig.max_concurrent_repairs fields are removed (crates/oceanfs-core/src/types/config.rs). The remaining `Semaphore::new` sites in the hint handler and repair.rs are within-operation fetch caps retained per the f4 in-scope wording ("An intra-batch fetch cap remains inside the permit") — DoD wording updated to match the code and the amended in-scope text. This is RECORDED DEVIATION 3 (healing-service intra-batch fetch cap retained inside the Tier-0 permit as within-operation parallelism).
-->
- [x] **Tier-0 unbounded only when no budget is wired:** `HealWorker`
      (`heal/worker.rs:98`), `ReRepWorker` (`repair.rs:173`), and the healing
      service (`healing_service.rs:295`) hold the budget as
      `Option<Arc<DurabilityBudget>>`; the composition root **always** wires
      the shared budget via `.with_budget(...)`
      (`modules/durability.rs:176,401`) so Tier-0 acquisition is bounded in
      production. `None` (unbounded) exists only for unit tests.
<!-- REVIEW: verified 2026-09-06 (iter 2, verdict PASS). This is RECORDED DEVIATION 4 (Tier-0 clients are unbounded only when no budget is wired — Option None is test-only; the composition root always wires the shared budget). Asserted in heal/worker.rs tests: budget is None before with_budget and Some after (worker.rs:649-661).
-->
- [x] **Niceness removed:** no `apply_background_io_class` /
      `apply_background_cpu_sched` call or `sched.rs` helper remains.
- [x] **Integration (all `--test-threads=1`):**
      `cargo test -p oceanfs-node --test durability_wiring`,
      `--test scrub_cycle`, `--test orphan_reaper`, `--test gc_compaction`,
      `--test read_write_roundtrip`, `--test node_lifecycle` pass with the
      scheduler driving the four tasks and heal/re-rep acquiring Tier-0
      permits.
- [ ] **Boot/e2e — SOLE REMAINING EXTERNAL GATE (do not mark done until
      executed):** a node boots with the scheduler running; GC/orphan/scrub/
      AE each move `durability_cycle_total{task}` on their configured
      intervals; a repair (scrub-detected heal or re-rep) moves
      `durability_repair_active`; shutdown cancels the scheduler token and the
      node exits cleanly.
<!-- REVIEW: f4 Boot/e2e DoD not verifiable locally per PIPELINE.md §6 (cloud e2e harness only). Local proxies verified: node_lifecycle boots a node and shuts it down cleanly (scheduler spawn + cancellation), and the scheduler engine unit tests exercise cycle/duration/skip metrics. The metric-movement assertions (durability_cycle_total{task} per interval, durability_repair_active on a repair) require a cloud e2e write/read + repair scenario. Condition to pass: run the sanctioned e2e suite on the harness and observe the metrics move. This item is the SOLE REMAINING EXTERNAL GATE for the whole durability-scheduler epic (shared with the epic README DoD #10); it must stay `[ ]` until the harness e2e run executes.
-->
- [x] **Metrics:** cycle + budget metrics registered once; no duplicate
      registration warnings from the central registry.
- [x] **ADR:** `[durability]` config per the amendment (`repair_max_active`,
      `housekeeping_max_active`, `task_timeout_sec`); task intervals are not
      relocated; heal/reconcile/hint delivery remain outside the scheduler as
      interval tasks while heal/re-rep/hint-apply sit on Tier-0 (README
      membership table is the reference).
- [x] **Docs:** every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes in touched crates.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).
