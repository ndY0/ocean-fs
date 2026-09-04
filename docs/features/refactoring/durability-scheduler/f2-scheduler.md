---
feature: "f2: DurabilityScheduler Engine"
epic: "refactoring/durability-scheduler"
status: proposed
priority: critical
owner: ""
dependencies:
  - feature: f1-durability-task-trait
    epic: refactoring/durability-scheduler
    reason: The scheduler registers and drives DurabilityTask instances from f1
  - feature: c2-split-durability-builder
    epic: refactoring/composition-root-decomposition
    reason: The scheduler object is constructed inside DurabilityModule (wiring lands in f4; the engine itself is crate-level)
adr:
  - 0017-durability-task-abstraction
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f2: DurabilityScheduler Engine

## Summary

Implement `DurabilityScheduler` in `crates/oceanfs-durability/src/scheduler/`:
the engine that owns per-task interval loops, a single global concurrency
semaphore across all registered tasks, skip/overrun accounting, per-cycle
timeouts, error tolerance, `CancellationToken` shutdown, and the unified
`durability_*` metrics from ADR-0017 §4. The engine is store-agnostic — it
only calls `DurabilityTask::run_cycle(window)` (f1). It replaces the four
per-task interval loops that today live inline in
`node.rs::spawn_background_tasks` (GC node.rs:3299-3321, AE 3337-3363, scrub
3373-3405, orphan reaper 3413-3434) — the deletion of those loops is f4.

## Scope

### In Scope
- `pub struct DurabilityScheduler` with:
  - `pub fn new(max_concurrent_tasks: usize, task_timeout: Option<Duration>,
    registrar: Option<&dyn MetricRegistrar>) -> Self`
  - `pub fn register(&mut self, task: Arc<dyn DurabilityTask>)`
  - `pub async fn spawn(self, shutdown: CancellationToken) -> JoinHandle<()>`
    (one `tokio::task` per task, each with its own interval)
- Global concurrency: one `Arc<Semaphore>` with `max_concurrent_tasks`
  permits; a cycle acquires a permit before running and holds it for the
  whole cycle (bounds total durability I/O across all tasks).
- Per-task loop behavior:
  - `tokio::time::interval(task.interval())` with
    `MissedTickBehavior::Skip` (a slow cycle does not cause a burst of
    back-to-back catch-up cycles);
  - a `running` flag per task implements skip-if-still-running for
    `concurrent_cycles() == false` — an overrun is counted as
    `durability_cycle_skipped_total{reason="overrun"}` and the next cycle
    waits for the next aligned tick (ADR-0017 §2);
  - per-cycle `tokio::time::timeout` when `task_timeout` is set — an
    elapsed cycle counts `status="error"`, logs, and the loop continues;
  - cycles returning `Err` are logged and counted (`status="error"`) and do
    not stop the scheduler (ADR-0017 error tolerance);
  - keyspace rotation: when `task.keyspace_fraction() < 1.0` the scheduler
    advances a `cycle_index` per task and passes
    `KeyspaceWindow::Shard { index: cycle_index % total, total }` where
    `total = (1.0 / keyspace_fraction).round()`; otherwise
    `KeyspaceWindow::Full` (mechanism only — see f3 for which tasks shard).
- Background I/O/CPU niceness: apply
  `oceanfs_storage::io::apply_background_io_class(task.name())` and
  `apply_background_cpu_sched(task.name())` at spawn time, honoring the
  caller-provided `background_io_class_idle` / `background_cpu_sched_idle`
  flags (preserves node.rs:3300-3304 behavior).
- Unified metrics (ADR-0017 §4), registered with the node's
  `MetricRegistrar` (an `oceanfs_server::admin::MetricsRegistry`):
  | Metric | Type | Labels |
  |---|---|---|
  | `durability_cycle_total` | Counter | `task`, `status` (`ok`/`error`) |
  | `durability_cycle_duration_seconds` | Histogram | `task` |
  | `durability_items_processed_total` | Counter | `task` |
  | `durability_cycle_skipped_total` | Counter | `task`, `reason` (`overrun`) |
  | `durability_scheduler_backlog` | Gauge | — (tasks currently waiting for a global permit) |

### Out of Scope (for this feature)
- The `DurabilityTask` trait and adaptors (f1).
- Which tasks shard their keyspace and the GC/orphan scan constraint (f3).
- Node wiring, config plumbing, deleting the `node.rs` loops (f4).
- Per-task domain counters (`gc_cycles_total`, …) — those stay; this feature
  only adds the scheduler-generic metrics.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | New `src/scheduler/scheduler.rs` (engine), `src/scheduler/metrics.rs` (scheduler metric bundle); re-export `DurabilityScheduler` from `lib.rs` |
| `oceanfs-node` | None in this feature |

## Interface (Public API)

```rust
pub struct DurabilityScheduler {
    tasks: Vec<Arc<dyn DurabilityTask>>,
    permits: Arc<Semaphore>,
    task_timeout: Option<Duration>,
    // + metric handles + task-state (interval, cycle_index, running flag)
}

impl DurabilityScheduler {
    /// `max_concurrent_tasks` must be >= 1. `task_timeout = None` disables
    /// the per-cycle timeout. When `registrar` is given the scheduler
    /// registers its `durability_*` metrics immediately.
    pub fn new(
        max_concurrent_tasks: usize,
        task_timeout: Option<Duration>,
        registrar: Option<&dyn MetricRegistrar>,
    ) -> Self;

    /// Registers a task. Tasks are spawned in registration order.
    pub fn register(&mut self, task: Arc<dyn DurabilityTask>);

    /// Registers the `durability_*` metrics with `registrar`.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar);

    /// Spawns one tokio task per registered task. Each loop runs until
    /// `shutdown` is cancelled. Returns a join handle that resolves when
    /// every loop has exited.
    pub async fn spawn(self, shutdown: CancellationToken)
        -> tokio::task::JoinHandle<()>;
}
```

Design notes for the implementer:

- `CancellationToken` is `tokio_util::sync::CancellationToken` (the same type
  `node.rs:43` and `heal/worker.rs:178` use) so f4 can chain the scheduler
  handle into the existing `BackgroundTasks` shutdown path.
- Keyspace rotation is a per-task `cycle_index` counter; with `fraction ==
  1.0` (all four tasks in this epic) the window is always `Full` — the cursor
  logic is exercised by a mock task in tests.
- The backlog gauge increments when a task's loop starts waiting on the
  global permit and decrements when it acquires (or is cancelled).

## Data Flow

```
spawn(shutdown_token)
  │
  ├─ loop per task T (tokio::task, name = T.name())
  │    ├─ apply io/cpu idle niceness (config flags)
  │    ├─ interval = tokio::time::interval(T.interval()); Skip on miss
  │    └─ loop { select! { shutdown => break,
  │                        tick => {
  │            if running && !T.concurrent_cycles() { skipped{overrun}.inc(); continue }
  │            running = true; backlog.inc();
  │            permit = permits.acquire().await;      // global budget
  │            backlog.dec();
  │            started = Instant::now();
  │            result = timeout(task_timeout, T.run_cycle(window)).await;
  │            running = false;
  │            duration = started.elapsed();
  │            cycle_total{status}.inc(); duration_hist.record(duration);
  │            if let Ok(n) = result { items_processed{task}.add(n) }
  │            if Err => log warn
  │        }}}
  └─ when every loop breaks → JoinHandle resolves
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-durability`.
- [ ] **Tests:** `cargo test -p oceanfs-durability --lib` passes. Scheduler
      engine tests use mock `DurabilityTask`s (no RocksDB) with intervals in
      the 10-50 ms range and assert:
      - **global semaphore:** with `max_concurrent_tasks = 1` and two tasks
        whose cycles rendezvous, observed max concurrency across tasks is 1;
      - **per-task serialization:** a task with `concurrent_cycles() ==
        false` never overlaps itself (high-water mark == 1);
      - **skip/overrun:** a task whose cycle is slower than its interval is
        counted once per overrun in `durability_cycle_skipped_total` and does
        not start a catch-up cycle immediately after finishing;
      - **error tolerance:** a task returning `Err` keeps running; the next
        cycle fires; `durability_cycle_total{status="error"}` increments;
      - **timeout:** a task whose cycle never returns is cut at
        `task_timeout` and the loop continues;
      - **shutdown:** cancelling the token makes `spawn`'s join handle
        resolve; no task runs after cancellation;
      - **rotation:** a mock task with `keyspace_fraction() == 0.25` receives
        `Shard { index: 0..=3, total: 4 }` once each across 4 cycles, then
        wraps to 0.
- [ ] **Docs:** every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes in `oceanfs-durability`.
- [ ] **ADR:** scheduler behavior matches ADR-0017 §2 (own interval per task,
      global semaphore, skip-if-running, error tolerance, keyspace
      rotation); intervals and per-task params stay where they are today.
- [ ] **Integration:** `cargo test -p oceanfs-durability --lib --
      --test-threads=1` plus `cargo test -p oceanfs-node --test
      durability_wiring -- --test-threads=1` (RocksDB caveat, PIPELINE.md
      §4.6). Full node integration lands in f4.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).
