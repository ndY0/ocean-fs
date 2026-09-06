//! `DurabilityScheduler` — the Tier-1 interval-cycle engine (ADR-0017, f2).
//!
//! Owns one interval loop per registered [`DurabilityTask`], with skip/
//! overrun accounting, per-cycle timeout, error tolerance,
//! [`CancellationToken`] shutdown, and the `durability_cycle_*` metrics.
//! Every cycle first acquires a Tier-1 permit from the shared
//! [`DurabilityBudget`], so concurrent scheduled cycles are bounded by
//! `housekeeping_max_active` — and a Tier-1 cycle can never block a Tier-0
//! (repair) operation, which draws from its own budget.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use oceanfs_core::{Counter, Histogram, HistogramConfig, LabelSet, MetricRegistrar};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::scheduler::{
    budget::DurabilityBudget,
    task::{DurabilityTask, KeyspaceWindow},
};

/// Buckets shared by the cycle-duration histograms (milliseconds).
fn duration_buckets() -> HistogramConfig {
    HistogramConfig {
        buckets: vec![1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 60_000],
    }
}

/// Per-task scheduler state + metric handles.
struct TaskState {
    task: Arc<dyn DurabilityTask>,
    budget: Arc<DurabilityBudget>,
    task_timeout: Option<Duration>,
    /// Round-robin rotation cursor for `keyspace_fraction() < 1.0`.
    cycle_index: AtomicU64,
    cycle_ok: Counter,
    cycle_err: Counter,
    duration_millis: Arc<Histogram>,
    items_processed: Counter,
    skipped_overrun: Counter,
}

impl TaskState {
    fn new(
        task: Arc<dyn DurabilityTask>,
        budget: Arc<DurabilityBudget>,
        task_timeout: Option<Duration>,
    ) -> Self {
        let name = task.name();
        let cfg = duration_buckets();
        let ok_labels = |value: &str| LabelSet::new(&[("task", name), ("status", value)]);
        let reason_labels = |value: &str| LabelSet::new(&[("task", name), ("reason", value)]);
        Self {
            cycle_index: AtomicU64::new(0),
            task,
            budget,
            task_timeout,
            cycle_ok: Counter::new(
                "durability_cycle_total".into(),
                "Durability cycles completed successfully".into(),
                ok_labels("ok"),
            ),
            cycle_err: Counter::new(
                "durability_cycle_total".into(),
                "Durability cycles that errored or timed out".into(),
                ok_labels("error"),
            ),
            duration_millis: Arc::new(Histogram::new(
                "durability_cycle_duration_millis".into(),
                "Durability cycle duration in milliseconds".into(),
                &cfg,
                LabelSet::new(&[("task", name)]),
            )),
            items_processed: Counter::new(
                "durability_items_processed_total".into(),
                "Items processed by durability cycles".into(),
                LabelSet::new(&[("task", name)]),
            ),
            skipped_overrun: Counter::new(
                "durability_cycle_skipped_total".into(),
                "Durability cycles skipped because the previous one was still running".into(),
                reason_labels("overrun"),
            ),
        }
    }

    fn next_window(&self) -> KeyspaceWindow {
        let index = self.cycle_index.fetch_add(1, Ordering::Relaxed);
        let fraction = self.task.keyspace_fraction();
        if fraction >= 1.0 {
            KeyspaceWindow::Full
        } else {
            let total = (1.0 / fraction).round().max(1.0) as u64;
            KeyspaceWindow::Shard { index: index % total, total }
        }
    }
}

/// The Tier-1 interval-cycle engine.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use std::time::Duration;
/// use oceanfs_durability::scheduler::{DurabilityBudget, DurabilityScheduler};
/// use tokio_util::sync::CancellationToken;
///
/// # async fn example() {
/// let budget = Arc::new(DurabilityBudget::new(16, 2));
/// let scheduler = Arc::new(DurabilityScheduler::new(budget, Some(Duration::from_secs(3600))));
/// // scheduler.register(Arc::new(my_task));  // DurabilityTask impls (f1)
/// let shutdown = CancellationToken::new();
/// let handle = scheduler.spawn(shutdown.clone()).await;
/// shutdown.cancel();
/// let _ = handle.await;
/// # }
/// ```
pub struct DurabilityScheduler {
    budget: Arc<DurabilityBudget>,
    task_timeout: Option<Duration>,
    states: Vec<Arc<TaskState>>,
}

impl DurabilityScheduler {
    /// Creates the scheduler.
    ///
    /// `budget` is the shared two-tier admission budget; every scheduled
    /// cycle acquires a Tier-1 permit from it. `task_timeout = None`
    /// disables the per-cycle timeout.
    pub fn new(budget: Arc<DurabilityBudget>, task_timeout: Option<Duration>) -> Self {
        Self { budget, task_timeout, states: Vec::new() }
    }

    /// Registers a task. Tasks are spawned in registration order.
    ///
    /// A task whose `name()` is already registered is ignored with a warning
    /// (metric labels are keyed by task name, so duplicates would collide).
    pub fn register(&mut self, task: Arc<dyn DurabilityTask>) {
        let name = task.name();
        if self.states.iter().any(|s| s.task.name() == name) {
            tracing::warn!(task = name, "DurabilityTask already registered; ignoring duplicate");
            return;
        }
        self.states.push(Arc::new(TaskState::new(
            task,
            Arc::clone(&self.budget),
            self.task_timeout,
        )));
    }

    /// Registers the `durability_cycle_*` metrics with `registrar`.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        for state in &self.states {
            registrar.register_counter(state.cycle_ok.clone());
            registrar.register_counter(state.cycle_err.clone());
            registrar.register_counter(state.items_processed.clone());
            registrar.register_counter(state.skipped_overrun.clone());
            registrar.register_histogram(Arc::clone(&state.duration_millis));
        }
    }

    /// Test-only accessor: clones of the per-task states, so tests can read
    /// metric counters after the scheduler has been spawned (consumed).
    #[cfg(test)]
    fn states_for_test(&self) -> Vec<Arc<TaskState>> {
        self.states.clone()
    }

    /// Spawns one tokio task per registered task. Each loop runs until
    /// `shutdown` is cancelled. Returns a join handle that resolves when
    /// every loop has exited.
    ///
    /// Takes `self: Arc<Self>` so callers holding the scheduler behind an
    /// `Arc` (the composition root) can spawn it directly.
    pub async fn spawn(self: Arc<Self>, shutdown: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut loops = tokio::task::JoinSet::new();
            for state in self.states.iter().cloned() {
                let token = shutdown.clone();
                loops.spawn(async move { run_loop(state, token).await });
            }
            while let Some(joined) = loops.join_next().await {
                if let Err(e) = joined {
                    tracing::warn!("durability scheduler loop exited with error: {e}");
                }
            }
        })
    }
}

/// One task's interval loop. Ticks spawn `run_one_cycle`; when a previous
/// cycle is still in flight and the task is serial (`concurrent_cycles ==
/// false`) the tick is counted as an overrun skip (no catch-up burst).
async fn run_loop(state: Arc<TaskState>, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(state.task.interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // `tokio::time::interval` fires its first tick immediately; that first
    // cycle matches the historical per-task loops (run at boot, then on
    // cadence).
    let mut in_flight = tokio::task::JoinSet::new();

    // NOTE: on shutdown the `in_flight` JoinSet is dropped when the loop
    // returns, which ABORTS any running cycle mid-await (no drain of
    // in-flight work). This is safe for the Tier-1 cycles because their
    // heavy phases are crash-recoverable state machines (GC compaction has
    // ADR-0025 recovery; orphan/scrub/AE re-run their cycle next boot).
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(task = state.task.name(), "durability scheduler loop cancelled");
                break;
            }
            joined = in_flight.join_next(), if !in_flight.is_empty() => {
                // A spawned cycle finished; its outcome was recorded inside
                // `run_one_cycle`. Consuming it keeps the set bounded.
                let _ = joined;
            }
            _ = interval.tick() => {
                if !state.task.concurrent_cycles() && !in_flight.is_empty() {
                    state.skipped_overrun.inc();
                    continue;
                }
                let st = Arc::clone(&state);
                in_flight.spawn(async move { run_one_cycle(st).await });
            }
        }
    }
}

/// Runs a single cycle under a Tier-1 permit and records its outcome.
async fn run_one_cycle(state: Arc<TaskState>) {
    let permit = state.budget.acquire_housekeeping().await;
    let window = state.next_window();
    let started = Instant::now();

    let result = match state.task_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, state.task.run_cycle(window)).await {
            Ok(result) => result,
            Err(_elapsed) => Err(crate::Error::Internal(format!(
                "{} cycle timed out after {}s",
                state.task.name(),
                timeout.as_secs(),
            ))),
        },
        None => state.task.run_cycle(window).await,
    };

    let elapsed = started.elapsed();
    let task = state.task.name();

    match result {
        Ok(items) => {
            state.cycle_ok.inc();
            state.items_processed.add(items);
            tracing::debug!(
                task,
                items,
                elapsed_ms = elapsed.as_millis() as u64,
                "durability cycle ok"
            );
        }
        Err(e) => {
            state.cycle_err.inc();
            tracing::warn!(
                task,
                error = %e,
                elapsed_ms = elapsed.as_millis() as u64,
                "durability cycle error"
            );
        }
    }
    state.duration_millis.observe(elapsed.as_millis() as u64);
    drop(permit);
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use async_trait::async_trait;

    use super::*;
    use crate::Result;

    /// Shared concurrency high-water tracker (shared across tasks when
    /// several mock tasks need one combined bound).
    #[derive(Debug, Default)]
    struct ConcurrencyTracker {
        current: AtomicU64,
        max: AtomicU64,
    }

    impl ConcurrencyTracker {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }
        fn enter(self: &Arc<Self>) -> TrackerGuard {
            let now = self.current.fetch_add(1, Ordering::Relaxed) + 1;
            let mut cur = self.max.load(Ordering::Relaxed);
            while now > cur {
                match self.max.compare_exchange_weak(cur, now, Ordering::Relaxed, Ordering::Relaxed)
                {
                    Ok(_) => break,
                    Err(actual) => cur = actual,
                }
            }
            TrackerGuard { tracker: Arc::clone(self) }
        }
        fn max(&self) -> u64 {
            self.max.load(Ordering::Relaxed)
        }
    }

    struct TrackerGuard {
        tracker: Arc<ConcurrencyTracker>,
    }
    impl Drop for TrackerGuard {
        fn drop(&mut self) {
            self.tracker.current.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// A mock task with configurable cadence/delay/error and window
    /// recording.
    struct MockTask {
        name: &'static str,
        interval: Duration,
        /// f64 `keyspace_fraction` stored as bits (interior mutability).
        fraction: AtomicU64,
        concurrent: bool,
        run_delay_millis: AtomicU64,
        run_error: AtomicBool,
        tracker: parking_lot::Mutex<Option<Arc<ConcurrencyTracker>>>,
        calls: AtomicU64,
        windows: parking_lot::Mutex<Vec<KeyspaceWindow>>,
    }

    impl MockTask {
        fn new(name: &'static str, interval: Duration) -> Arc<Self> {
            Arc::new(Self {
                name,
                interval,
                fraction: AtomicU64::new(1.0_f64.to_bits()),
                concurrent: false,
                run_delay_millis: AtomicU64::new(0),
                run_error: AtomicBool::new(false),
                tracker: parking_lot::Mutex::new(None),
                calls: AtomicU64::new(0),
                windows: parking_lot::Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> u64 {
            self.calls.load(Ordering::Relaxed)
        }

        fn set_fraction(&self, value: f64) {
            self.fraction.store(value.to_bits(), Ordering::Relaxed);
        }

        fn set_run_delay(&self, millis: u64) {
            self.run_delay_millis.store(millis, Ordering::Relaxed);
        }

        fn set_run_error(&self, error: bool) {
            self.run_error.store(error, Ordering::Relaxed);
        }

        fn set_tracker(&self, tracker: Arc<ConcurrencyTracker>) {
            *self.tracker.lock() = Some(tracker);
        }

        fn windows(&self) -> Vec<KeyspaceWindow> {
            self.windows.lock().clone()
        }
    }

    #[async_trait]
    impl DurabilityTask for MockTask {
        fn name(&self) -> &'static str {
            self.name
        }
        fn interval(&self) -> Duration {
            self.interval
        }
        fn keyspace_fraction(&self) -> f64 {
            f64::from_bits(self.fraction.load(Ordering::Relaxed))
        }
        fn concurrent_cycles(&self) -> bool {
            self.concurrent
        }
        async fn run_cycle(&self, window: KeyspaceWindow) -> Result<u64> {
            let _guard = self.tracker.lock().as_ref().map(|t| t.enter());
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.windows.lock().push(window);
            let delay = self.run_delay_millis.load(Ordering::Relaxed);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            if self.run_error.load(Ordering::Relaxed) {
                Err(crate::Error::Internal("mock failure".into()))
            } else {
                Ok(7)
            }
        }
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime")
    }

    fn spawn_tasks(scheduler: DurabilityScheduler, shutdown: CancellationToken) -> JoinHandle<()> {
        let scheduler = Arc::new(scheduler);
        tokio::spawn(async move {
            let handle = scheduler.spawn(shutdown.clone()).await;
            let _ = handle.await;
        })
    }

    /// Two scheduled tasks with `housekeeping_max_active = 1` never run
    /// concurrently across tasks (the Tier-1 cap).
    #[test]
    fn housekeeping_cap_bounds_cross_task_concurrency() {
        let rt = rt();
        rt.block_on(async {
            let tracker = ConcurrencyTracker::new();
            let budget = Arc::new(DurabilityBudget::new(1, 1));
            let mut scheduler = DurabilityScheduler::new(Arc::clone(&budget), None);
            let a = MockTask::new("a", Duration::from_millis(15));
            a.set_tracker(Arc::clone(&tracker));
            let b = MockTask::new("b", Duration::from_millis(15));
            b.set_tracker(Arc::clone(&tracker));
            scheduler.register(a.clone());
            scheduler.register(b.clone());

            let shutdown = CancellationToken::new();
            let handle = spawn_tasks(scheduler, shutdown.clone());
            tokio::time::sleep(Duration::from_millis(250)).await;
            shutdown.cancel();
            let _ = handle.await;

            assert!(a.calls() >= 3, "task a must have run several cycles");
            assert!(b.calls() >= 3, "task b must have run several cycles");
            assert_eq!(
                tracker.max(),
                1,
                "combined Tier-1 concurrency must not exceed housekeeping_max_active = 1"
            );
        });
    }

    /// A serial task never overlaps itself and overrun ticks are counted.
    #[test]
    fn per_task_serialization_and_overrun_skip() {
        let rt = rt();
        rt.block_on(async {
            let tracker = ConcurrencyTracker::new();
            let budget = Arc::new(DurabilityBudget::new(4, 4));
            let mut scheduler = DurabilityScheduler::new(Arc::clone(&budget), None);
            let a = MockTask::new("a", Duration::from_millis(10));
            a.set_tracker(Arc::clone(&tracker));
            a.set_run_delay(40); // much slower than the 10ms tick
            scheduler.register(a.clone());
            let states = scheduler.states_for_test();

            let shutdown = CancellationToken::new();
            let handle = spawn_tasks(scheduler, shutdown.clone());
            tokio::time::sleep(Duration::from_millis(300)).await;
            shutdown.cancel();
            let _ = handle.await;

            assert!(a.calls() >= 4, "serial task must still complete cycles");
            assert_eq!(tracker.max(), 1, "a serial task never overlaps itself");
            assert!(states[0].skipped_overrun.get() > 0, "overrun ticks must be counted");
        });
    }

    /// Error tolerance: an erroring task keeps running and the error counter
    /// increments.
    #[test]
    fn error_tolerance_keeps_loop_alive() {
        let rt = rt();
        rt.block_on(async {
            let budget = Arc::new(DurabilityBudget::new(4, 4));
            let mut scheduler = DurabilityScheduler::new(Arc::clone(&budget), None);
            let a = MockTask::new("a", Duration::from_millis(10));
            a.set_run_error(true);
            scheduler.register(a.clone());
            let states = scheduler.states_for_test();

            let shutdown = CancellationToken::new();
            let handle = spawn_tasks(scheduler, shutdown.clone());
            tokio::time::sleep(Duration::from_millis(150)).await;
            shutdown.cancel();
            let _ = handle.await;

            assert!(a.calls() >= 3, "erroring task keeps running cycles");
            assert_eq!(states[0].cycle_err.get(), a.calls());
            assert_eq!(states[0].cycle_ok.get(), 0);
        });
    }

    /// Timeout: a cycle that never returns is cut at `task_timeout` and the
    /// loop continues.
    #[test]
    fn cycle_timeout_cuts_stuck_cycle_and_loop_continues() {
        let rt = rt();
        rt.block_on(async {
            let budget = Arc::new(DurabilityBudget::new(4, 4));
            let mut scheduler =
                DurabilityScheduler::new(Arc::clone(&budget), Some(Duration::from_millis(30)));
            let a = MockTask::new("a", Duration::from_millis(10));
            a.set_run_delay(60_000); // effectively never returns on its own
            scheduler.register(a.clone());
            let states = scheduler.states_for_test();

            let shutdown = CancellationToken::new();
            let handle = spawn_tasks(scheduler, shutdown.clone());
            tokio::time::sleep(Duration::from_millis(200)).await;
            shutdown.cancel();
            let _ = handle.await;

            assert!(a.calls() >= 2, "timed-out cycles keep being attempted");
            assert!(states[0].cycle_err.get() >= 2, "timed-out cycles count as errors");
        });
    }

    /// Shutdown cancels every loop; no task runs after cancellation.
    #[test]
    fn shutdown_stops_loops() {
        let rt = rt();
        rt.block_on(async {
            let budget = Arc::new(DurabilityBudget::new(4, 4));
            let mut scheduler = DurabilityScheduler::new(budget, None);
            let a = MockTask::new("a", Duration::from_millis(5));
            scheduler.register(a.clone());

            let shutdown = CancellationToken::new();
            let handle = spawn_tasks(scheduler, shutdown.clone());
            tokio::time::sleep(Duration::from_millis(80)).await;
            shutdown.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("scheduler join handle must resolve");

            let after = a.calls();
            tokio::time::sleep(Duration::from_millis(40)).await;
            assert_eq!(a.calls(), after, "no cycle runs after cancellation");
        });
    }

    /// Rotation: a task with `keyspace_fraction() == 0.25` receives shard
    /// windows 0..=3 (total 4) in order, then wraps.
    #[test]
    fn rotation_delivers_shard_windows() {
        let rt = rt();
        rt.block_on(async {
            let budget = Arc::new(DurabilityBudget::new(4, 4));
            let mut scheduler = DurabilityScheduler::new(budget, None);
            let a = MockTask::new("a", Duration::from_millis(5));
            a.set_fraction(0.25);
            scheduler.register(a.clone());

            let shutdown = CancellationToken::new();
            let handle = spawn_tasks(scheduler, shutdown.clone());
            let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            while a.calls() < 8 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            shutdown.cancel();
            let _ = handle.await;

            let windows = a.windows();
            assert!(windows.len() >= 8, "expected >= 8 windows, got {}", windows.len());
            for (i, w) in windows.iter().enumerate().take(8) {
                assert_eq!(*w, KeyspaceWindow::Shard { index: (i as u64) % 4, total: 4 });
            }
        });
    }
}
