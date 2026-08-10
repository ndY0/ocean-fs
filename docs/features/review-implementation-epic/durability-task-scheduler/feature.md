---
feature: "Durability Task Scheduler"
epic: "review-implementation-epic"
status: rejected
priority: critical
owner: ""
dependencies:
  - epic: gap-closure-addendum
    reason: Item 6 (trait-object conversions for durability components) must be
      complete so that DurabilityTask::run_cycle() can accept Arc<dyn MetadataStore>
      rather than concrete RocksDbMetadataStore; Item 2 (scrub/AE/heal configs)
      must be complete so tasks can read their intervals from config
adr:
  - 0017-durability-task-abstraction
  - 0005-trait-in-consuming-crate
  - 0009-storage-crate-split
created: 2026-08-09
updated: 2026-08-09
---

# Durability Task Scheduler

## Summary

Every durability background process — garbage collection, orphan reaper,
segment compactor, anti-entropy, scrub — follows the same "look up a column
family + act" pattern (review finding #21), yet each is implemented with
duplicated scheduling logic, hardcoded intervals, no backpressure, and no
unified metrics (review findings #20, #19). This feature introduces a
`DurabilityTask` trait and `DurabilityScheduler` in `oceanfs-durability`. The
trait defines the contract: `name()`, `run_cycle()`, `interval()`,
`keyspace_fraction()`, and `concurrent_cycles()`. The scheduler manages a
registry of tasks, runs them on configured intervals, enforces a global
concurrency semaphore, partitions work via `keyspace_fraction` (enabling the
GC optimization from finding #20), and emits unified metrics. Existing
durability tasks (GC, orphan reaper, compactor, AE, scrub) are refactored to
implement the trait.

## Scope

### In Scope
- `DurabilityTask` trait with 5 methods in `oceanfs-durability`
- `DurabilityScheduler` with registration, interval-based execution, global concurrency semaphore, and keyspace partitioning
- Keyspace sharding: when `keyspace_fraction < 1.0`, scheduler partitions key ranges and rotates each cycle
- Unified metrics: 5 Prometheus metrics emitted by scheduler, not per-task constructors
- Refactor existing tasks (GC, orphan reaper, compactor, AE, scrub) to implement `DurabilityTask`
- Configuration: `[durability]` section in `NodeConfig` with `max_concurrent_tasks` and `task_timeout_sec`

### Out of Scope (for this feature)
- Changing individual task logic (GC still GCs, scrub still scrubs) — only the scheduling wrapper changes
- Adaptive interval tuning (future)
- Adding new durability tasks beyond the existing 5
- Fetch ordering strategy (read-path, separate feature)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | New modules: `scheduler/mod.rs`, `scheduler/task.rs` (trait), `scheduler/scheduler.rs`, `scheduler/metrics.rs`, `scheduler/keyspace.rs` |
| `oceanfs-durability` | Modify `gc/garbage_collector.rs`, `gc/orphan_reaper.rs`, `gc/segment_compactor.rs`, `anti_entropy/engine.rs`, `scrub.rs`: add `impl DurabilityTask` wrapper for each |
| `oceanfs-core` | New config section `DurabilityConfig` with `max_concurrent_tasks`, `task_timeout_sec` |
| `oceanfs-node` | In `node.rs`, construct `DurabilityScheduler`, register all tasks, spawn scheduler instead of individual task loops |

## Interface (Public API)

- `pub trait DurabilityTask: Send + Sync` in `oceanfs-durability::scheduler`
  - `fn name(&self) -> &'static str`
  - `async fn run_cycle(&self, metadata: &dyn MetadataStore, segments: &dyn SegmentStore) -> Result<u64>`
  - `fn interval(&self) -> std::time::Duration`
  - `fn keyspace_fraction(&self) -> f64` — default `1.0`
  - `fn concurrent_cycles(&self) -> bool` — default `false`

- `pub struct DurabilityScheduler`
  - `pub fn new(config: DurabilityConfig, metadata: Arc<dyn MetadataStore>, segments: Arc<dyn SegmentStore>) -> Self`
  - `pub fn register(&mut self, task: Arc<dyn DurabilityTask>)`
  - `pub async fn run(self) -> Result<()>` — runs until cancelled; spawns a `tokio::task` per registered task
  - `pub fn metrics(&self) -> &DurabilityMetrics` — access the unified metrics registry

- `pub struct DurabilityMetrics`
  - `pub cycle_total: CounterVec` — labeled by `task`, `status`
  - `pub cycle_duration: HistogramVec` — labeled by `task`
  - `pub items_processed: CounterVec` — labeled by `task`
  - `pub cycle_skipped: CounterVec` — labeled by `task`, `reason`
  - `pub scheduler_backlog: Gauge`

- `pub struct DurabilityConfig`
  - `pub max_concurrent_tasks: usize` — semaphore permits; default 2
  - `pub task_timeout_sec: u64` — max duration for a single cycle; default 3600

- `pub struct KeyspacePartitioner` — internal helper for `keyspace_fraction`
  - `pub fn new(fraction: f64) -> Self`
  - `pub fn next_range(&mut self, total_key_range: &KeyRange) -> KeyRange` — returns the next partition slice

## Data Flow

```
oceanfs-node::start()
  ↓
DurabilityScheduler::new(config, metadata_store, segment_store)
  ↓
scheduler.register(Arc::new(GcTask::new(gc_config)))
scheduler.register(Arc::new(OrphanReaperTask::new(orphan_config)))
scheduler.register(Arc::new(CompactorTask::new(compactor_config)))
scheduler.register(Arc::new(AeTask::new(ae_config)))
scheduler.register(Arc::new(ScrubTask::new(scrub_config)))
  ↓
tokio::spawn(scheduler.run())

--- Scheduler run loop ---

scheduler.run():
  for each registered task:
    tokio::spawn(async move {
      let mut interval = tokio::time::interval(task.interval());
      let mut keyspace_partitioner = KeyspacePartitioner::new(task.keyspace_fraction());
      loop {
        interval.tick().await;
        // Skip if previous cycle still running and !concurrent_cycles
        if running && !task.concurrent_cycles() {
          metrics::cycle_skipped(task.name(), "concurrent").inc();
          continue;
        }
        // Acquire semaphore permit (global concurrency limit)
        let permit = semaphore.acquire().await;
        // Compute keyspace shard if fraction < 1.0
        let key_range = if task.keyspace_fraction() < 1.0 {
          Some(keyspace_partitioner.next_range(&total_key_range))
        } else { None };
        // Run cycle
        let start = Instant::now();
        let result = tokio::time::timeout(
          Duration::from_secs(config.task_timeout_sec),
          task.run_cycle(metadata, segments)
        ).await;
        metrics::cycle_duration(task.name(), start.elapsed()).observe();
        match result {
          Ok(Ok(items)) => {
            metrics::cycle_total(task.name(), "ok").inc();
            metrics::items_processed(task.name(), items).inc();
          }
          Ok(Err(e)) => {
            metrics::cycle_total(task.name(), "error").inc();
            tracing::error!(task = task.name(), error = %e, "Durability cycle failed");
          }
          Err(_elapsed) => {
            metrics::cycle_total(task.name(), "timeout").inc();
            tracing::warn!(task = task.name(), "Durability cycle timed out");
          }
        }
        drop(permit); // release semaphore
      }
    });
```

## Definition of Done

- [ ] **D4.1** In `crates/oceanfs-durability/src/scheduler/task.rs`, define:
  ```rust
  /// A background maintenance task that operates on the storage engine.
  pub trait DurabilityTask: Send + Sync {
      /// Human-readable name for logging and metrics labels.
      fn name(&self) -> &'static str;
      /// Run one cycle of this task. Returns number of items processed.
      async fn run_cycle(
          &self,
          metadata: &dyn oceanfs_storage_api::MetadataStore,
          segments: &dyn oceanfs_storage_api::SegmentStore,
      ) -> Result<u64>;
      /// Interval between consecutive cycles.
      fn interval(&self) -> std::time::Duration;
      /// Fraction of keyspace to process per cycle, in (0.0, 1.0].
      /// Default 1.0 means process everything.
      fn keyspace_fraction(&self) -> f64 { 1.0 }
      /// Whether this task can run concurrently with itself.
      fn concurrent_cycles(&self) -> bool { false }
  }
  ```

- [ ] **D4.2** In `crates/oceanfs-durability/src/scheduler/scheduler.rs`, implement `struct DurabilityScheduler`:
  ```rust
  use std::sync::Arc;
  use tokio::sync::Semaphore;

  pub struct DurabilityScheduler {
      tasks: Vec<(Arc<dyn DurabilityTask>, KeyspacePartitioner)>,
      concurrency_limit: Arc<Semaphore>,
      metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
      segments: Arc<dyn oceanfs_storage_api::SegmentStore>,
      metrics: DurabilityMetrics,
      config: DurabilityConfig,
  }

  impl DurabilityScheduler {
      pub fn new(
          config: DurabilityConfig,
          metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
          segments: Arc<dyn oceanfs_storage_api::SegmentStore>,
      ) -> Self {
          Self {
              tasks: Vec::new(),
              concurrency_limit: Arc::new(Semaphore::new(config.max_concurrent_tasks)),
              metadata,
              segments,
              metrics: DurabilityMetrics::new(),
              config,
          }
      }

      pub fn register(&mut self, task: Arc<dyn DurabilityTask>) {
          let partitioner = KeyspacePartitioner::new(task.keyspace_fraction());
          self.tasks.push((task, partitioner));
      }

      pub async fn run(self) -> Result<()> {
          let mut handles = Vec::new();
          for (task, mut partitioner) in self.tasks {
              let metadata = Arc::clone(&self.metadata);
              let segments = Arc::clone(&self.segments);
              let semaphore = Arc::clone(&self.concurrency_limit);
              let metrics = self.metrics.clone();
              let timeout = Duration::from_secs(self.config.task_timeout_sec);
              let handle = tokio::spawn(async move {
                  let mut interval = tokio::time::interval(task.interval());
                  // Skip first immediate tick
                  interval.tick().await;
                  loop {
                      interval.tick().await;
                      let _permit = match semaphore.acquire().await {
                          Ok(p) => p,
                          Err(_) => break, // semaphore closed
                      };
                      let start = std::time::Instant::now();
                      let result = tokio::time::timeout(timeout, task.run_cycle(
                          metadata.as_ref(),
                          segments.as_ref(),
                      )).await;
                      let duration = start.elapsed();
                      metrics.cycle_duration.with_label_values(&[task.name()]).observe(duration.as_secs_f64());
                      match result {
                          Ok(Ok(items)) => {
                              metrics.cycle_total.with_label_values(&[task.name(), "ok"]).inc();
                              metrics.items_processed.with_label_values(&[task.name()]).inc_by(items);
                          }
                          Ok(Err(e)) => {
                              metrics.cycle_total.with_label_values(&[task.name(), "error"]).inc();
                              tracing::error!(task = task.name(), error = %e, "Durability cycle failed");
                          }
                          Err(_) => {
                              metrics.cycle_total.with_label_values(&[task.name(), "timeout"]).inc();
                              tracing::warn!(task = task.name(), "Durability cycle timed out after {}s", timeout.as_secs());
                          }
                      }
                      // permit dropped here
                  }
              });
              handles.push(handle);
          }
          // Wait for all handles (they run forever until cancelled)
          for handle in handles {
              let _ = handle.await;
          }
          Ok(())
      }

      pub fn metrics(&self) -> &DurabilityMetrics { &self.metrics }
  }
  ```

- [ ] **D4.3** In `crates/oceanfs-durability/src/scheduler/keyspace.rs`, implement `struct KeyspacePartitioner`:
  ```rust
  pub struct KeyspacePartitioner {
      fraction: f64,
      current_shard: usize,
      total_shards: usize,
  }

  impl KeyspacePartitioner {
      pub fn new(fraction: f64) -> Self {
          let f = fraction.clamp(0.0, 1.0);
          let total_shards = if f <= 0.0 || f >= 1.0 { 1 } else { (1.0 / f).ceil() as usize };
          Self { fraction: f, current_shard: 0, total_shards }
      }

      /// Advance to the next shard and return its key range boundaries.
      /// Returns `None` if keyspace_fraction == 1.0 (process all).
      pub fn next_range(&mut self) -> Option<(f64, f64)> {
          if self.total_shards == 1 { return None; }
          let start = self.current_shard as f64 / self.total_shards as f64;
          let end = (self.current_shard + 1) as f64 / self.total_shards as f64;
          self.current_shard = (self.current_shard + 1) % self.total_shards;
          Some((start, end))
      }
  }
  ```
  The returned `(start, end)` values are hash-range fractions in `[0.0, 1.0]` used by GC/orphan-reaper to limit their RocksDB scan range.

- [ ] **D4.4** In `crates/oceanfs-durability/src/scheduler/metrics.rs`, define unified metrics:
  ```rust
  use prometheus::{CounterVec, HistogramVec, Gauge, Registry, Opts};

  pub struct DurabilityMetrics {
      pub cycle_total: CounterVec,
      pub cycle_duration: HistogramVec,
      pub items_processed: CounterVec,
      pub cycle_skipped: CounterVec,
      pub scheduler_backlog: Gauge,
      registry: Registry,
  }

  impl DurabilityMetrics {
      pub fn new() -> Self {
          let cycle_total = CounterVec::new(
              Opts::new("durability_cycle_total", "Total durability cycles completed"),
              &["task", "status"]
          ).unwrap();
          let cycle_duration = HistogramVec::new(
              histogram_opts!("durability_cycle_duration_seconds", "Cycle duration", vec![0.01, 0.1, 1.0, 10.0, 60.0, 600.0, 3600.0]),
              &["task"]
          ).unwrap();
          let items_processed = CounterVec::new(
              Opts::new("durability_items_processed_total", "Items processed by durability tasks"),
              &["task"]
          ).unwrap();
          let cycle_skipped = CounterVec::new(
              Opts::new("durability_cycle_skipped_total", "Cycles skipped"),
              &["task", "reason"]
          ).unwrap();
          let scheduler_backlog = Gauge::new(
              "durability_scheduler_backlog", "Number of tasks waiting for semaphore"
          ).unwrap();
          let registry = Registry::new();
          registry.register(Box::new(cycle_total.clone())).unwrap();
          registry.register(Box::new(cycle_duration.clone())).unwrap();
          registry.register(Box::new(items_processed.clone())).unwrap();
          registry.register(Box::new(cycle_skipped.clone())).unwrap();
          registry.register(Box::new(scheduler_backlog.clone())).unwrap();
          Self { cycle_total, cycle_duration, items_processed, cycle_skipped, scheduler_backlog, registry }
      }
  }
  ```

- [ ] **D4.5** In `crates/oceanfs-durability/src/gc/garbage_collector.rs`, add `impl DurabilityTask for GcTask`:
  ```rust
  pub struct GcTask {
      gc: Arc<GarbageCollector>,
      config: GcTaskConfig,
  }

  impl DurabilityTask for GcTask {
      fn name(&self) -> &'static str { "gc" }
      async fn run_cycle(&self, metadata: &dyn MetadataStore, _segments: &dyn SegmentStore) -> Result<u64> {
          self.gc.run_cycle(metadata).await.map(|stats| stats.tombstones_collected)
      }
      fn interval(&self) -> Duration { Duration::from_secs(self.config.interval_sec) }
      fn keyspace_fraction(&self) -> f64 { self.config.keyspace_fraction }
      fn concurrent_cycles(&self) -> bool { false }
  }
  ```
  `GcTaskConfig` includes `interval_sec` (from `NodeConfig.gc_interval_sec`) and `keyspace_fraction` (new field, default `0.1` for smoother GC per finding #20).

- [ ] **D4.6** Apply the same `impl DurabilityTask` wrapper pattern to the remaining 4 tasks:
  - `crates/oceanfs-durability/src/gc/orphan_reaper.rs` → `OrphanReaperTask` with `name() = "orphan_reaper"`, `keyspace_fraction = 0.1`, `interval = gc_interval_sec`
  - `crates/oceanfs-durability/src/gc/segment_compactor.rs` → `CompactorTask` with `name() = "compactor"`, `keyspace_fraction = 1.0`, `interval = gc_interval_sec`
  - `crates/oceanfs-durability/src/anti_entropy/engine.rs` → `AntiEntropyTask` with `name() = "anti_entropy"`, `keyspace_fraction = 1.0`, `interval = ae_interval_sec`
  - `crates/oceanfs-durability/src/scrub.rs` → `ScrubTask` with `name() = "scrub"`, `keyspace_fraction = 0.01` (scrub 1% per cycle, full pass every ~7 days at default interval), `interval = scrub_interval_sec / 100`

- [ ] **D4.7** Remove per-task metric initialization from individual task constructors. All metrics now come from `DurabilityScheduler`.
  - `GarbageCollector::new()`: remove `metrics` parameter (was review finding #19).
  - `OrphanReaper::new()`: remove `metrics` parameter.
  - `SegmentCompactor::new()`: remove `metrics` parameter.
  - `AntiEntropy::new()`: remove `metrics` parameter (AE may retain its own Merkle-tree-specific metrics).
  - `ScrubCoordinator::new()`: remove `metrics` parameter.

- [ ] **D4.8** In `crates/oceanfs-core/src/config/node.rs`, add:
  ```rust
  /// Durability scheduler configuration.
  #[serde(default)]
  pub durability: DurabilityConfig,
  ```
  Define:
  ```rust
  #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
  pub struct DurabilityConfig {
      /// Maximum concurrent durability tasks (semaphore permits).
      /// Default: 2.
      #[serde(default = "default_durability_max_concurrent_tasks")]
      pub max_concurrent_tasks: usize,
      /// Maximum duration for a single task cycle in seconds.
      /// Default: 3600 (1 hour).
      #[serde(default = "default_durability_task_timeout_sec")]
      pub task_timeout_sec: u64,
      /// Keyspace fraction for GC. Default: 0.1 (10% per cycle).
      #[serde(default = "default_gc_keyspace_fraction")]
      pub gc_keyspace_fraction: f64,
      /// Keyspace fraction for orphan reaper. Default: 0.1.
      #[serde(default = "default_gc_keyspace_fraction")]
      pub orphan_reaper_keyspace_fraction: f64,
  }
  ```
  With corresponding default functions.

- [ ] **D4.9** In `crates/oceanfs-node/src/node.rs`, replace individual task spawning:
  ```rust
  // OLD (individual task loops):
  // tokio::spawn(gc_task.run_loop());
  // tokio::spawn(orphan_reaper.run_loop());
  // tokio::spawn(compactor.run_loop());
  // tokio::spawn(ae_task.run_loop());
  // tokio::spawn(scrub_task.run_loop());

  // NEW (scheduler):
  let mut scheduler = DurabilityScheduler::new(
      config.durability,
      metadata_store.clone() as Arc<dyn MetadataStore>,
      segment_store.clone() as Arc<dyn SegmentStore>,
  );
  scheduler.register(Arc::new(GcTask::new(gc, GcTaskConfig {
      interval_sec: config.gc_interval_sec,
      keyspace_fraction: config.durability.gc_keyspace_fraction,
  })));
  scheduler.register(Arc::new(OrphanReaperTask::new(orphan_reaper, OrphanReaperConfig {
      interval_sec: config.gc_interval_sec,
      keyspace_fraction: config.durability.orphan_reaper_keyspace_fraction,
  })));
  scheduler.register(Arc::new(CompactorTask::new(compactor, CompactorTaskConfig {
      interval_sec: config.gc_interval_sec,
  })));
  scheduler.register(Arc::new(AntiEntropyTask::new(ae, AeTaskConfig {
      interval_sec: config.ae_interval_sec,
  })));
  scheduler.register(Arc::new(ScrubTask::new(scrub, ScrubTaskConfig {
      interval_sec: config.scrub_interval_sec,
  })));
  // Expose scheduler metrics on /admin/metrics
  metrics_registry.register("durability", scheduler.metrics().registry());
  tokio::spawn(scheduler.run());
  ```

- [ ] **D4.10** Add to `oceanfs.toml` example:
  ```toml
  [durability]
  max_concurrent_tasks = 2
  task_timeout_sec = 3600
  gc_keyspace_fraction = 0.1
  orphan_reaper_keyspace_fraction = 0.1
  ```

## Tests Required

- [ ] **T4.1** `test_durability_scheduler_registers_and_runs_tasks` — In `crates/oceanfs-durability/tests/scheduler_integration.rs`:
  - Create `DurabilityScheduler` with `max_concurrent_tasks = 2`.
  - Register a mock task with `interval = Duration::from_millis(50)`, `run_cycle` that increments an `AtomicU64` counter and returns `Ok(1)`.
  - Spawn scheduler in background.
  - Sleep 200ms (allowing ~4 cycles).
  - Assert counter >= 3 (some cycles completed; allow for timing jitter).
  - Assert `scheduler.metrics().cycle_total` labeled "mock"/"ok" >= 3.

- [ ] **T4.2** `test_durability_scheduler_enforces_concurrency_limit` — In same file:
  - Create scheduler with `max_concurrent_tasks = 1`.
  - Register 3 tasks, each with `interval = Duration::from_millis(10)` and `run_cycle` that takes 100ms.
  - Spawn scheduler.
  - Sleep 50ms.
  - Assert at most 1 task is actively running (use an internal gauge or check that metrics show at most ~1 cycle completed within 50ms given 100ms cycle time).

- [ ] **T4.3** `test_keyspace_partitioner_rotates_shards` — In `crates/oceanfs-durability/src/scheduler/keyspace.rs` test module:
  - Create `KeyspacePartitioner::new(0.25)`.
  - Call `next_range()` 4 times.
  - Assert returns: `Some((0.0, 0.25))`, `Some((0.25, 0.5))`, `Some((0.5, 0.75))`, `Some((0.75, 1.0))`.
  - Call 5th time: assert returns `Some((0.0, 0.25))` (wraps around).

- [ ] **T4.4** `test_keyspace_partitioner_fraction_1_returns_none` — In same module:
  - Create `KeyspacePartitioner::new(1.0)`.
  - Call `next_range()`.
  - Assert returns `None`.

- [ ] **T4.5** `test_gc_task_implements_durability_task` — In `crates/oceanfs-durability/tests/gc_task_wrapper.rs`:
  - Create `GcTask` wrapping a `GarbageCollector`.
  - Assert `name() == "gc"`.
  - Assert `keyspace_fraction() == 0.1` (from config).
  - Mock `MetadataStore`, inject, call `run_cycle()`, assert it calls `gc.run_cycle()` and returns count.

- [ ] **T4.6** `test_scrub_task_implements_durability_task` — Same pattern for scrub task.
  - Assert `name() == "scrub"`.
  - Assert `keyspace_fraction() == 0.01`.

- [ ] **T4.7** `test_scheduler_metrics_emitted` — In `crates/oceanfs-durability/tests/scheduler_metrics.rs`:
  - Run scheduler with 2 tasks for 3 cycles each.
  - Scrape the `DurabilityMetrics` struct (not HTTP endpoint — direct access).
  - Assert `cycle_total` counter for both tasks >= 3.
  - Assert `cycle_duration` histogram has observations.
  - Assert `items_processed` counter is incremented.

- [ ] **T4.8** `test_scheduler_task_timeout_kills_cycle` — In same file:
  - Register a task with `run_cycle` that sleeps 5 seconds, interval = 50ms, scheduler `task_timeout_sec = 1`.
  - Run scheduler for 3 seconds.
  - Assert `cycle_total` with status "timeout" >= 2.
  - Assert `cycle_total` with status "ok" == 0.

- [ ] **T4.9** `test_concurrent_cycles_false_skips_on_overlap` — In same file:
  - Register task with `concurrent_cycles() = false`, `interval = 10ms`, `run_cycle` takes 200ms.
  - Run scheduler for 500ms.
  - Assert `cycle_skipped` with reason "concurrent" > 0.

## ADR References

- [ADR-0017](../../adr/0017-durability-task-abstraction.md) — Full design: `DurabilityTask` trait, `DurabilityScheduler`, keyspace sharding, unified metrics
- [ADR-0005](../../adr/0005-trait-in-consuming-crate.md) — `DurabilityTask` trait lives in `oceanfs-durability` (the consuming crate); task implementations also in `oceanfs-durability`
- [ADR-0009](../../adr/0009-storage-crate-split.md) — `MetadataStore` and `SegmentStore` traits from `oceanfs-storage-api` are consumed by `DurabilityTask::run_cycle()`
