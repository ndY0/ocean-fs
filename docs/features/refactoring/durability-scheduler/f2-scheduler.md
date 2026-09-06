---
feature: "f2: DurabilityBudget + DurabilityScheduler Engine"
epic: "refactoring/durability-scheduler"
status: done
priority: critical
owner: ""
dependencies:
  - feature: f1-durability-task-trait
    epic: refactoring/durability-scheduler
    reason: The scheduler registers and drives DurabilityTask instances from f1
  - feature: c2-split-durability-builder
    epic: refactoring/composition-root-decomposition
    reason: The budget + scheduler objects are constructed inside DurabilityModule (wiring lands in f4; the engine itself is crate-level)
adr:
  - 0017-durability-task-abstraction
perf: []
created: 2026-09-04
updated: 2026-09-06
---

# f2: DurabilityBudget + DurabilityScheduler Engine

> **FINAL STATE (2026-09-06):** `done`. Independent review verdict **PASS**
> (iteration 2). Code green: fmt, `cargo build --all-targets`, clippy `-D
> warnings`, rustdoc `-D warnings`, lib suite 276 tests incl. 19 scheduler
> tests (`--test-threads=1`). Accepted deviations vs the original spec wording
> (recorded in the epic's deviation list): (1) the engine lives at
> `scheduler/engine.rs` (not `scheduler/scheduler.rs`) — the public path
> `oceanfs_durability::scheduler::DurabilityScheduler` is unchanged, so no
> consumer impact; (2) the duration/wait metrics are `*_millis` **u64
> histograms** (e.g. `durability_cycle_duration_millis`,
> `durability_repair_wait_duration_millis`,
> `durability_housekeeping_wait_duration_millis`), not second-resolution — the
> metric tables in this document already use the shipped names.

## Summary

Implement in `crates/oceanfs-durability/src/scheduler/`:

1. **`DurabilityBudget`** (`scheduler/budget.rs`) — the two-tier admission
   budget from the ADR-0017 2026-09-06 amendment. Tier-0 (`repair`) and
   Tier-1 (`housekeeping`) are **separate semaphores**: a Tier-0 acquisition
   is never gated behind Tier-1 activity, satisfying *"priority tasks are
   never blocked by low-tier ones"*. Within a tier, `tokio::Semaphore` is
   FIFO-fair — satisfying *fairness within a tier*. Tier separation is
   admission-level only (no device-level io-class arbitration).
2. **`DurabilityScheduler`** (`scheduler/engine.rs`) — the Tier-1 interval
   engine: owns per-task interval loops, skip/overrun accounting, per-cycle
   timeouts, error tolerance, `CancellationToken` shutdown, and the
   `durability_cycle_*` metrics. Each cycle acquires a Tier-1 permit from the
   shared `DurabilityBudget` before running `DurabilityTask::run_cycle`
   (f1). The engine is store-agnostic.

The engine replaces the four per-task interval loops that today live in
`DurabilityModule::spawn_loops` (`modules/durability.rs`: GC, AE, scrub,
orphan reaper) — deletion of those loops is f4. The Tier-0 side of the budget
is consumed by heal/re-rep/inbound-hint-apply *in f2's wake* (crate-level
worker changes: their private semaphores become Tier-0 acquisitions; see
"Tier-0 client work" below). Node wiring is f4.

## Scope

### In Scope
- `pub struct DurabilityBudget` (`scheduler/budget.rs`):
  - `pub fn new(repair_max_active: usize, housekeeping_max_active: usize) -> Self`
    (both must be >= 1);
  - `pub async fn acquire_repair(&self) -> DurabilityPermit` (Tier-0);
  - `pub async fn acquire_housekeeping(&self) -> DurabilityPermit` (Tier-1);
  - `pub fn register_metrics(&self, registrar: &dyn MetricRegistrar)`.
- `pub enum DurabilityTier { Repair, Housekeeping }` (labels, logging).
- `DurabilityPermit` — RAII guard holding an owned semaphore permit plus the
  active/waiters gauge accounting for its tier (dropping releases).
- Budget metrics:
  | Metric | Type | Meaning |
  |---|---|---|
  | `durability_repair_active` | Gauge | currently active Tier-0 operations |
  | `durability_housekeeping_active` | Gauge | currently active Tier-1 cycles |
  | `durability_repair_waiters` | Gauge | tasks waiting for a Tier-0 permit |
  | `durability_housekeeping_waiters` | Gauge | tasks waiting for a Tier-1 permit |
  | `durability_repair_wait_duration_millis` | Histogram | Tier-0 wait time before acquisition |
  | `durability_housekeeping_wait_duration_millis` | Histogram | Tier-1 wait time before acquisition |
- `pub struct DurabilityScheduler` (`scheduler/engine.rs`) with:
  - `pub fn new(budget: Arc<DurabilityBudget>, task_timeout: Option<Duration>) -> Self`
  - `pub fn register(&mut self, task: Arc<dyn DurabilityTask>)`
  - `pub fn register_metrics(&self, registrar: &dyn MetricRegistrar)`
  - `pub async fn spawn(self: Arc<Self>, shutdown: CancellationToken)
    -> JoinHandle<()>` (one `tokio::task` per registered task, each with
    its own interval; `Arc<Self>` so the composition root can spawn the
    scheduler it holds behind an `Arc`)
- Tier-1 (housekeeping) concurrency: every scheduled cycle acquires a Tier-1
  permit before running and holds it for the whole cycle (a slow cycle never
  blocks a Tier-0 operation, which has its own budget).
- Per-task loop behavior:
  - `tokio::time::interval(task.interval())` with
    `MissedTickBehavior::Skip`;
  - a `running` flag per task implements skip-if-still-running for
    `concurrent_cycles() == false` — an overrun is counted as
    `durability_cycle_skipped_total{reason="overrun"}` and the next cycle
    waits for the next aligned tick;
  - per-cycle `tokio::time::timeout` when `task_timeout` is set — an elapsed
    cycle counts `status="error"`, logs, and the loop continues;
  - cycles returning `Err` are logged and counted (`status="error"`) and do
    not stop the scheduler;
  - keyspace rotation: when `task.keyspace_fraction() < 1.0` the scheduler
    advances a `cycle_index` per task and passes
    `KeyspaceWindow::Shard { index: cycle_index % total, total }` where
    `total = (1.0 / keyspace_fraction).round()`; otherwise
    `KeyspaceWindow::Full` (mechanism only — see f3).
- Scheduler cycle metrics (ADR-0017 §4, `task` label):
  | Metric | Type | Labels |
  |---|---|---|
  | `durability_cycle_total` | Counter | `task`, `status` (`ok`/`error`) |
  | `durability_cycle_duration_millis` | Histogram | `task` |
  | `durability_items_processed_total` | Counter | `task` |
  | `durability_cycle_skipped_total` | Counter | `task`, `reason` (`overrun`) |
  (`durability_scheduler_backlog` is superseded by
  `durability_housekeeping_waiters` in the budget.)

### Tier-0 client work (crate-level, lands with f2)
- `HealWorker` drops its `max_concurrent_heals` semaphore and acquires a
  Tier-0 permit per heal op (`repair`).
- `ReRepWorker` drops its `max_concurrent_repairs` semaphore and acquires a
  Tier-0 permit per pull/write (`repair`).
- The healing-service gRPC hint handler drops its per-RPC `Semaphore(16)`
  and acquires a Tier-0 permit per hint batch (`repair`) — the review anchor
  `healing_service.rs:1032` is closed by the *shared* cross-RPC budget.
- Worker constructor signatures change to accept the shared budget (or a
  builder setter); their call sites are updated in f4. HealConfig
  `max_concurrent_heals` and ReRepConfig `max_concurrent_repairs` are removed.

### Out of Scope (for this feature)
- The `DurabilityTask` trait and adaptors (f1).
- Which tasks shard their keyspace and the GC/orphan scan constraint (f3).
- Node wiring, config plumbing, deleting the `spawn_loops` interval loops (f4).
- Per-task domain counters (`gc_cycles_total`, …) — those stay; this feature
  only adds the scheduler-generic + budget metrics.
- io/cpu niceness helpers — removed outright by the ADR amendment; the
  scheduler does NOT re-apply them.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | New `src/scheduler/budget.rs` (`DurabilityBudget`, `DurabilityTier`, `DurabilityPermit`), `src/scheduler/engine.rs` (engine), `src/scheduler/adaptors.rs`, `src/scheduler/task.rs`; re-export `DurabilityBudget`, `DurabilityTier`, `DurabilityScheduler`, `DurabilityTask`, `KeyspaceWindow` + the four adaptors from `lib.rs` |
| `oceanfs-durability` (Tier-0 client work) | `heal/worker.rs`, `repair.rs` (ReRepWorker), `healing_service.rs` drop private semaphores for Tier-0 acquisitions; `HealConfig`/`ReRepConfig` fields removed |
| `oceanfs-node` | None in this feature (wiring is f4) |

## Interface (Public API)

```rust
// scheduler/budget.rs

/// The two admission tiers of the durability I/O budget (ADR-0017
/// amendment). Tier-0 ("repair") operations are never gated behind
/// Tier-1 ("housekeeping") activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityTier {
    /// Data-layer / repair operations (heal, re-rep, inbound hint apply).
    Repair,
    /// Housekeeping cycles (GC, orphan reaper, scrub, AE).
    Housekeeping,
}

impl DurabilityTier {
    /// Prometheus label value (`"repair"` / `"housekeeping"`).
    pub fn label(&self) -> &'static str;
}

/// The two-tier admission budget shared by every durability I/O producer.
pub struct DurabilityBudget {
    // tier0: Arc<Semaphore>, tier1: Arc<Semaphore>  (separate — Tier-0 is
    // never blocked by Tier-1), plus per-tier active/waiters gauge + wait
    // histogram handles.
}

impl DurabilityBudget {
    /// Both budgets must be >= 1.
    pub fn new(repair_max_active: usize, housekeeping_max_active: usize) -> Self;

    /// Acquires a Tier-0 (repair) permit. Waits only on Tier-0 activity.
    pub async fn acquire_repair(&self) -> DurabilityPermit;

    /// Acquires a Tier-1 (housekeeping) permit.
    pub async fn acquire_housekeeping(&self) -> DurabilityPermit;

    /// Registers the budget metrics with `registrar`.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar);
}

/// RAII guard: releases the permit (and active-gauge slot) on drop.
pub struct DurabilityPermit { /* tier + owned semaphore permit */ }

// scheduler/engine.rs

pub struct DurabilityScheduler {
    tasks: Vec<Arc<dyn DurabilityTask>>,
    budget: Arc<DurabilityBudget>, // Tier-1 admission for every cycle
    task_timeout: Option<Duration>,
    // + per-task cycle metric handles + per-task state (cycle_index, running)
}

impl DurabilityScheduler {
    /// `task_timeout = None` disables the per-cycle timeout.
    pub fn new(budget: Arc<DurabilityBudget>, task_timeout: Option<Duration>) -> Self;

    /// Registers a task. Tasks are spawned in registration order.
    pub fn register(&mut self, task: Arc<dyn DurabilityTask>);

    /// Registers the `durability_cycle_*` metrics with `registrar`.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar);

    /// Spawns one tokio task per registered task. Each loop runs until
    /// `shutdown` is cancelled. Returns a join handle that resolves when
    /// every loop has exited. Takes `self: Arc<Self>` (the composition
    /// root holds the scheduler behind an `Arc`).
    pub async fn spawn(self: Arc<Self>, shutdown: CancellationToken)
        -> tokio::task::JoinHandle<()>;
}
```

Design notes for the implementer:

- `CancellationToken` is `tokio_util::sync::CancellationToken` (the same type
  `modules/durability.rs` and `heal/worker.rs` use) so f4 can chain the
  scheduler handle into the existing `BackgroundTasks` shutdown path.
- Keyspace rotation is a per-task `cycle_index` counter; with `fraction ==
  1.0` (all four tasks in this epic) the window is always `Full` — the cursor
  logic is exercised by a mock task in tests.
- The budget's waiters gauges increment when a caller starts waiting and
  decrement when it acquires (or the wait is abandoned).
- No io/cpu niceness calls anywhere in the scheduler (helpers removed).

## Data Flow

```
DurabilityModule::build (c2, f4)
  → DurabilityBudget::new(repair_max_active, housekeeping_max_active)   // Arc
  → scheduler = DurabilityScheduler::new(budget.clone(), task_timeout)
  → scheduler.register(GcTask|OrphanTask|ScrubTask|AeTask)              // f1 adaptors
  → heal_worker / rep_worker / healing_service acquire_repair()         // Tier-0
  → budget.register_metrics(&metrics); scheduler.register_metrics(&metrics)
        │
        ▼  each housekeeping cycle tick (scheduler.spawn):
   budget.acquire_housekeeping().await          // Tier-1 — FIFO-fair
   task.run_cycle(window)
        ▼
   worker.run_cycle(...) → stats → items_processed (u64)
        ▼
   durability_cycle_total{task,status}.inc(); duration_hist.record(...)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-durability`.
- [x] **Tests:** `cargo test -p oceanfs-durability --lib` passes.
      - **Budget — two-tier invariant:** with `repair_max_active = 1` and
        `housekeeping_max_active = 1`, two Tier-1 cycles rendezvous at
        concurrency 1, and a Tier-0 op that rendezvous with a *held* Tier-1
        permit starts immediately (observed concurrency reaches 2 across
        tiers) — Tier-0 is never blocked by Tier-1.
      - **Budget — fairness within a tier:** with `housekeeping_max_active =
        1` and two Tier-1 claimants, acquisition order is FIFO (second
        claimant acquires only after the first releases); no claimant starves.
      - **Scheduler — housekeeping cap:** with `housekeeping_max_active = 1`
        and two tasks whose cycles rendezvous, observed max Tier-1 concurrency
        across tasks is 1.
      - **Scheduler — per-task serialization:** a task with
        `concurrent_cycles() == false` never overlaps itself (high-water
        mark == 1).
      - **Scheduler — skip/overrun:** a task whose cycle is slower than its
        interval is counted once per overrun in
        `durability_cycle_skipped_total` and does not start a catch-up cycle
        immediately after finishing.
      - **Scheduler — error tolerance:** a task returning `Err` keeps
        running; the next cycle fires; `durability_cycle_total{status=
        "error"}` increments.
      - **Scheduler — timeout:** a task whose cycle never returns is cut at
        `task_timeout` and the loop continues.
      - **Scheduler — shutdown:** cancelling the token makes `spawn`'s join
        handle resolve; no task runs after cancellation.
      - **Scheduler — rotation:** a mock task with `keyspace_fraction() ==
        0.25` receives `Shard { index: 0..=3, total: 4 }` once each across 4
        cycles, then wraps to 0.
<!-- REVIEW: verified 2026-09-06 (iter 2, verdict PASS). Implementation at scheduler/engine.rs (DurabilityScheduler, engine.rs:125) + scheduler/budget.rs (DurabilityBudget, DurabilityTier, DurabilityPermit). Sub-bullet coverage in the scheduler module's tests:
- Budget two-tier invariant: tier0_is_never_blocked_by_tier1 (budget.rs:249); plus tier0_respects_its_own_budget (budget.rs:266).
- Budget fairness: housekeeping_admission_is_fair (budget.rs:281).
- Active/waiters gauges: active_gauges_track_held_permits (budget.rs:309).
- Housekeeping cap + per-task serialization + overrun skip: housekeeping_cap_bounds_cross_task_concurrency (engine.rs:436), per_task_serialization_and_overrun_skip (engine.rs:467).
- Error tolerance: error_tolerance_keeps_loop_alive (engine.rs:494). Timeout: cycle_timeout_cuts_stuck_cycle_and_loop_continues (engine.rs:519). Shutdown: shutdown_stops_loops (engine.rs:543). Rotation: rotation_delivers_shard_windows (engine.rs:568).
- Metrics shipped with *_millis u64 histogram names (deviations 1-2 recorded in the final-state note above): budget.rs registers durability_repair_active / durability_housekeeping_active / durability_repair_waiters / durability_housekeeping_waiters + durability_{repair,housekeeping}_wait_duration_millis (budget.rs:105-131); engine registers durability_cycle_total / durability_cycle_duration_millis / durability_items_processed_total / durability_cycle_skipped_total (engine.rs:75,165).
-->
- [x] **Docs:** every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes in `oceanfs-durability`.
- [x] **ADR:** budget + scheduler behavior match ADR-0017 §2 as amended
      (own interval per task, two-tier admission, skip-if-running, error
      tolerance, keyspace rotation); intervals and per-task params stay where
      they are today; no io/cpu niceness calls remain.
- [x] **Integration:** `cargo test -p oceanfs-durability --lib --
      --test-threads=1` plus `cargo test -p oceanfs-node --test
      durability_wiring -- --test-threads=1` (RocksDB caveat, PIPELINE.md
      §4.6). Full node integration lands in f4.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).
