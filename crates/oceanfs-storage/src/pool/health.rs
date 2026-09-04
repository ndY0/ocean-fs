//! Health-signal processing + the failure state machine (ADR-0029 §D3).
//!
//! Two layers:
//!
//! **Trend detection (signal processing, pure):** [`evaluate_trend`]
//! turns the [`IoObserver`]'s per-window signals
//! into a verdict:
//!
//! - **Degrading** fires when an I/O signal series (error rate or
//!   worst-per-op p99 latency) shows a monotonic-worsening slope —
//!   `x[i] >= 2 * x[i-1]` for the **last two consecutive window pairs** —
//!   even while every value is below the absolute threshold. A disk
//!   failing exponentially *below* thresholds is caught this way.
//! - Erratic single-window spikes do **not** trip the slope (they
//!   accumulate into the next window's baseline and are handled by the
//!   g2 absolute-threshold fast path instead).
//! - Tech-aware SMART baselines: `hdd` additionally degrades on
//!   reallocated+pending sector growth, `ssd`/`nvme` on
//!   uncorrectable-ECC/wear growth, `cloud-ephemeral` on I/O signals
//!   only.
//!
//! **The state machine (g2):** [`HealthMonitor`] is the per-node
//! decision layer that consumes [`PoolSignal`]s each detection window,
//! runs [`evaluate_trend`] + [`decide_transition`], and drives
//! [`PoolRegistry`] status + `write_degraded`
//! (wal-Dead rule). Dead requires *confirmed loss* (see
//! [`ConfirmedLoss`]); latency/trend alone never confirms Dead.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use oceanfs_core::{PoolHealthConfig, PoolRole, PoolTech};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    io::disk_io::{IoErrorKind, IoObserver, IoOp, IO_ERROR_KIND_COUNT, IO_OP_COUNT},
    pool::{PoolRegistry, PoolStatus},
};

// ---------------------------------------------------------------------------
// Latency + SMART aggregates
// ---------------------------------------------------------------------------

/// Latency percentiles for one I/O op over one detection window.
///
/// `None` when the window had no samples of that op. Values are the
/// power-of-two histogram bucket upper bounds (approximate, lock-free).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Latency {
    /// Median (`p50`).
    pub p50: Option<Duration>,
    /// 99th percentile.
    pub p99: Option<Duration>,
    /// 99.9th percentile.
    pub p999: Option<Duration>,
}

/// SMART-derived device counters carried in a [`PoolSignal`].
///
/// Phase B v1: `Option` placeholders — real sysfs reads land later; the
/// observer can be fed synthetic values in tests (accepted deviation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SmartCounters {
    /// Reallocated sector count (`hdd` tell).
    pub reallocated_sectors: Option<u64>,
    /// Pending sector count (`hdd` tell).
    pub pending_sectors: Option<u64>,
    /// Uncorrectable ECC errors (`ssd`/`nvme` tell).
    pub uncorrectable_ecc: Option<u64>,
    /// Wear level indicator, 0-100 (`ssd`/`nvme` tell).
    pub wear_level: Option<u64>,
}

// ---------------------------------------------------------------------------
// PoolSignal
// ---------------------------------------------------------------------------

/// The per-window health aggregate — the trend detector's input and the
/// [`IoObserver::snapshot`](crate::io::IoObserver::snapshot) output.
///
/// `latency` is indexed by [`IoOp::as_usize`]; use
/// [`PoolSignal::latency_for`] to read a single op's percentiles.
/// `error_kinds` is indexed by [`IoErrorKind::as_usize`]
/// (crate::io::IoErrorKind) — g2's health monitor consumes it for
/// confirmed-loss detection (ADR-0029 §D3 Dead-confirming kinds).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct PoolSignal {
    /// Error rate (errors per op) over the window. `0.0` when the window
    /// had no ops.
    pub error_rate: f64,
    /// Total observed ops over the window (successes + failures).
    pub ops: u64,
    /// Observed errors over the window.
    pub errors: u64,
    /// Per-op latency percentiles (`IoOp`-indexed).
    pub latency: [Latency; IO_OP_COUNT],
    /// SMART counters for the window.
    pub smart: SmartCounters,
    /// Per-kind error counts over the window
    /// ([`IoErrorKind`]-indexed).
    pub error_kinds: [u64; IO_ERROR_KIND_COUNT],
}

impl PoolSignal {
    /// Returns the latency percentiles for one op.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::IoOp;
    /// use oceanfs_storage::pool::health::PoolSignal;
    ///
    /// let signal = PoolSignal::default();
    /// assert!(signal.latency_for(IoOp::Read).p50.is_none());
    /// ```
    pub fn latency_for(&self, op: IoOp) -> Latency {
        self.latency[op.as_usize()]
    }

    /// Returns the worst p99 latency across all ops (the "latency
    /// series" value for this window): any op's degradation trips the
    /// trend detector, so the detector sees the maximum.
    fn worst_p99_nanos(&self) -> u64 {
        self.latency.iter().filter_map(|l| l.p99.map(|d| d.as_nanos() as u64)).max().unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Trend verdict
// ---------------------------------------------------------------------------

/// The trend detector's verdict over a signal history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TrendVerdict {
    /// No monotonic-worsening slope in any observed signal.
    Stable,
    /// A signal series is worsening monotonically (doubling per window)
    /// or a tech-specific SMART counter is growing — suspicion, not
    /// confirmation (g2 maps this to the `Degraded` state).
    Degrading,
}

// ---------------------------------------------------------------------------
// evaluate_trend
// ---------------------------------------------------------------------------

/// Evaluates a signal history against the ADR-0029 §D3 trend rules.
///
/// Returns [`TrendVerdict::Degrading`] when any I/O signal series shows
/// a monotonic-worsening slope — `x[i] >= 2 * x[i-1]` for the last two
/// consecutive window pairs (needs at least 3 windows) — or when the
/// tech's SMART baseline shows counter growth across windows.
///
/// Windows with a zero baseline never trip the slope (an abrupt 0→N
/// spike is the g2 absolute-threshold fast path's job, not the trend's),
/// so erratic/intermittent errors do not flap state.
///
/// Tech baselines (ADR-0029 §D3):
/// - `hdd`: reallocated + pending sector growth → `Degrading`;
/// - `ssd`/`nvme`: uncorrectable-ECC + wear growth → `Degrading`;
/// - `cloud-ephemeral`: I/O signals only (no SMART);
/// - `Auto`: resolved to a concrete tech by the pool runtime before this
///   layer; treated as I/O-only here.
///
/// # Examples
///
/// ```
/// use oceanfs_core::PoolTech;
/// use oceanfs_storage::pool::health::{evaluate_trend, PoolSignal, TrendVerdict};
///
/// // Exponential error-rate growth (1 → 2 → 4 → 8), below any absolute
/// // threshold — the slope alone trips the trend detector.
/// let history: Vec<PoolSignal> = (0u32..4)
///     .map(|n| PoolSignal {
///         error_rate: 2u64.pow(n) as f64,
///         ops: 100,
///         errors: 0,
///         ..PoolSignal::default()
///     })
///     .collect();
/// assert_eq!(evaluate_trend(&history, PoolTech::Nvme), TrendVerdict::Degrading);
/// ```
pub fn evaluate_trend(history: &[PoolSignal], tech: PoolTech) -> TrendVerdict {
    // I/O-signal series: error rate + worst-per-op p99 latency.
    let error_rates: Vec<f64> = history.iter().map(|signal| signal.error_rate).collect();
    let p99_latencies: Vec<f64> =
        history.iter().map(|signal| signal.worst_p99_nanos() as f64).collect();

    if doubling(&error_rates) || doubling(&p99_latencies) {
        return TrendVerdict::Degrading;
    }

    // Tech-specific SMART baselines. Auto is resolved by the pool runtime
    // before this layer; cloud-ephemeral has no SMART (I/O only).
    let smart_degrading = match tech {
        PoolTech::Hdd => {
            let series: Vec<u64> = history
                .iter()
                .map(|s| {
                    s.smart
                        .reallocated_sectors
                        .unwrap_or(0)
                        .saturating_add(s.smart.pending_sectors.unwrap_or(0))
                })
                .collect();
            smart_growth(&series)
        }
        PoolTech::Ssd | PoolTech::Nvme => {
            let series: Vec<u64> = history
                .iter()
                .map(|s| {
                    s.smart
                        .uncorrectable_ecc
                        .unwrap_or(0)
                        .saturating_add(s.smart.wear_level.unwrap_or(0))
                })
                .collect();
            smart_growth(&series)
        }
        // Auto is resolved by the pool runtime before this layer;
        // cloud-ephemeral has no SMART (I/O only); unknown future techs
        // are I/O-only too.
        _ => false,
    };
    if smart_degrading {
        TrendVerdict::Degrading
    } else {
        TrendVerdict::Stable
    }
}

/// `true` when a series shows a monotonic-worsening slope: `x[i] >= 2 *
/// x[i-1]` for the LAST TWO consecutive window pairs (both the final and
/// the penultimate pair double). A zero baseline never counts as the
/// doubling base — an abrupt 0→N spike is the absolute-threshold fast
/// path's signal, not the trend's.
fn doubling(series: &[f64]) -> bool {
    let n = series.len();
    if n < 3 {
        return false;
    }
    let pair_doubles = |i: usize| -> bool {
        let previous = series[i - 1];
        previous > 0.0 && series[i] >= 2.0 * previous
    };
    pair_doubles(n - 1) && pair_doubles(n - 2)
}

/// `true` when a SMART counter series grows across any of the last two
/// window pairs ("counter growth across windows" per ADR-0029 §D3).
fn smart_growth(series: &[u64]) -> bool {
    let n = series.len();
    if n >= 2 && series[n - 1] > series[n - 2] {
        return true;
    }
    if n >= 3 && series[n - 2] > series[n - 3] {
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// ConfirmedLoss
// ---------------------------------------------------------------------------

/// The ONLY inputs that may transition a pool Degraded → Dead
/// (ADR-0029 §D3: "Dead requires *confirmed loss*"). Latency/trend alone
/// never confirms Dead.
///
/// Detected from the observer's error kinds (accepted deviation): the
/// `DiskIo` wrapper classifies ENOENT/EIO at the op boundary — the
/// monitor never interprets raw errno.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfirmedLoss {
    /// ENOENT on an owned segment file (`IoErrorKind::NotFound`).
    SegmentNotFound,
    /// EIO on fsync of a segment/WAL write (`IoErrorKind::Other` — raw
    /// EIO surfaces as `Other` in `std::io`).
    FsyncIo,
    /// Device unplug / write-verify mismatch
    /// (`IoErrorKind::InvalidData`).
    DeviceUnplug,
}

impl ConfirmedLoss {
    /// Derives a confirmed loss from a window's error-kind counts, if any
    /// Dead-confirming kind is present (priority:
    /// `SegmentNotFound` → `FsyncIo` → `DeviceUnplug`).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::{IoErrorKind, IO_ERROR_KIND_COUNT};
    /// use oceanfs_storage::pool::health::ConfirmedLoss;
    ///
    /// let mut kinds = [0u64; IO_ERROR_KIND_COUNT];
    /// kinds[IoErrorKind::NotFound.as_usize()] = 1;
    /// assert_eq!(ConfirmedLoss::from_kinds(&kinds), Some(ConfirmedLoss::SegmentNotFound));
    /// ```
    pub fn from_kinds(kinds: &[u64; IO_ERROR_KIND_COUNT]) -> Option<ConfirmedLoss> {
        if kinds[IoErrorKind::NotFound.as_usize()] > 0 {
            return Some(ConfirmedLoss::SegmentNotFound);
        }
        if kinds[IoErrorKind::Other.as_usize()] > 0 {
            return Some(ConfirmedLoss::FsyncIo);
        }
        if kinds[IoErrorKind::InvalidData.as_usize()] > 0 {
            return Some(ConfirmedLoss::DeviceUnplug);
        }
        None
    }

    /// Derives a confirmed loss from a window's [`PoolSignal`].
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::{IoErrorKind, IO_ERROR_KIND_COUNT};
    /// use oceanfs_storage::pool::health::{ConfirmedLoss, PoolSignal};
    ///
    /// let mut signal = PoolSignal::default();
    /// signal.error_kinds[IoErrorKind::Other.as_usize()] = 1;
    /// assert_eq!(ConfirmedLoss::from_signal(&signal), Some(ConfirmedLoss::FsyncIo));
    /// ```
    pub fn from_signal(signal: &PoolSignal) -> Option<ConfirmedLoss> {
        Self::from_kinds(&signal.error_kinds)
    }
}

// ---------------------------------------------------------------------------
// HealthEvent
// ---------------------------------------------------------------------------

/// Events the [`HealthMonitor`] emits on status transitions.
///
/// The node's consequence applier consumes these to map role → D3
/// consequences (wal → write_degraded is driven by the monitor itself;
/// metadata Dead → the node serves nothing — derived lazily from the
/// registry by the g6 read/write gates; data → affected segments,
/// hints → hint rejection are node-layer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HealthEvent {
    /// A pool's status changed. The registry already reflects the new
    /// status; the event is the node's notification to act on it.
    StatusChanged {
        /// The pool whose status changed.
        pool_id: u32,
        /// The new status.
        status: PoolStatus,
    },
}

// ---------------------------------------------------------------------------
// HealthMonitorConfig
// ---------------------------------------------------------------------------

/// Tuning for the [`HealthMonitor`].
///
/// # Examples
///
/// ```
/// use oceanfs_storage::pool::health::HealthMonitorConfig;
///
/// let config = HealthMonitorConfig::default();
/// assert!(config.tick_interval.is_none()); // per-pool cadence
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthMonitorConfig {
    /// Global tick cadence. `None` (default): each pool ticks every
    /// `detection_window_secs` from its own f1 health config. `Some`:
    /// overrides every pool's cadence (tests use a fast tick).
    pub tick_interval: Option<Duration>,
    /// Capacity of the bounded status-event channel (perf 2.6 — no
    /// unbounded channels). Default: 64.
    pub event_capacity: usize,
}

impl Default for HealthMonitorConfig {
    fn default() -> Self {
        Self { tick_interval: None, event_capacity: 64 }
    }
}

// ---------------------------------------------------------------------------
// HealthMonitor
// ---------------------------------------------------------------------------

/// The per-node decision layer (ADR-0029 §D3): consumes [`PoolSignal`]s
/// from the g1 [`IoObserver`] and drives each pool through
/// Healthy → Degraded → Dead with the confirmed-loss rules, applying the
/// `write_degraded` role consequence for the wal pool on the registry.
///
/// - Healthy → Degraded: [`evaluate_trend`] says `Degrading`, OR an
///   absolute-threshold spike (errors ≥ `min_errors` AND error rate >
///   `error_rate_threshold`), OR a latency spike (worst p99 >
///   `latency_factor` × the window's worst p50).
/// - Degraded → Dead: [`ConfirmedLoss`] only (observer error kinds).
///   Latency/trend alone never confirms Dead.
/// - Degraded → Healthy: `recovery_window_secs` of zero-error windows
///   (hysteresis).
/// - Dead is absorbing until replacement (g7/g8).
///
/// The monitor is shared (`Arc`): [`HealthMonitor::run`] owns the tick
/// loop; [`HealthMonitor::report_confirmed_loss`] lets the node feed
/// explicit confirmed-loss events (device unplug from a runtime probe)
/// from other tasks.
///
/// # Locking
///
/// LOCK ORDER: `HealthMonitor.state` → `PoolSignals.rotate_lock` (the
/// tick snapshots the observer while holding its per-pool state entry;
/// `report_confirmed_loss` takes only the state lock). No path acquires
/// them in reverse.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_storage::io::IoObserver;
/// use oceanfs_storage::pool::health::{HealthMonitor, HealthMonitorConfig};
/// use oceanfs_storage::PoolRegistry;
///
/// # let tmp = tempfile::tempdir().expect("tempdir");
/// # let data_dir = tmp.path().join("data");
/// # let storage = oceanfs_core::StorageConfig {
/// #     pools: vec![
/// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: None, tech: Default::default(), health: Default::default() },
/// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
/// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
/// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
/// #     ],
/// #     missing_root_policy: Default::default(),
/// # };
/// let registry = Arc::new(
///     PoolRegistry::from_config(&storage, &data_dir).expect("registry"),
/// );
/// let observer = Arc::new(IoObserver::new());
/// registry.observe_into(&observer);
/// let (monitor, _events) =
///     HealthMonitor::new(registry.clone(), observer.clone(), HealthMonitorConfig::default());
/// assert!(monitor.pool_count() >= 1);
/// ```
pub struct HealthMonitor {
    registry: Arc<PoolRegistry>,
    observer: Arc<IoObserver>,
    config: HealthMonitorConfig,
    /// Bounded status-event channel (perf 2.6). `try_send` — a dropped
    /// receiver or a full channel drops the event; the registry holds
    /// the authoritative state.
    events: mpsc::Sender<HealthEvent>,
    /// Per-pool monitor state. Touched only by the monitor task and
    /// `report_confirmed_loss` — a `parking_lot::Mutex` is fine (rare,
    /// never on the I/O path).
    state: Mutex<HashMap<u32, PoolState>>,
}

/// Per-pool monitor state.
#[derive(Debug)]
struct PoolState {
    /// The last status the monitor applied (mirrors the registry).
    status: PoolStatus,
    /// Consecutive zero-error windows (hysteresis accumulator).
    clean_windows: u64,
    /// Window history for `evaluate_trend` (bounded — perf 1.3).
    history: VecDeque<PoolSignal>,
    /// When the next tick for this pool is due.
    next_tick: Instant,
}

impl PoolState {
    fn new(status: PoolStatus, now: Instant) -> Self {
        Self {
            status,
            clean_windows: 0,
            history: VecDeque::new(),
            // `now` is the caller's tick timestamp, so the first tick
            // (with that same `now`) is immediately due.
            next_tick: now,
        }
    }
}

// [review][structure][low]
// usually wrapping an object in a smart pointer is done by the caller, not the constructor
// [end]
impl HealthMonitor {
    /// Creates a monitor plus its bounded status-event channel.
    ///
    /// The returned receiver is the node's consequence-applier input.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_storage::io::IoObserver;
    /// use oceanfs_storage::pool::health::{HealthMonitor, HealthMonitorConfig};
    /// use oceanfs_storage::PoolRegistry;
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = Arc::new(
    ///     PoolRegistry::from_config(&storage, &data_dir).expect("registry"),
    /// );
    /// let observer = Arc::new(IoObserver::new());
    /// registry.observe_into(&observer);
    /// let (monitor, _events) =
    ///     HealthMonitor::new(registry, observer, HealthMonitorConfig::default());
    /// assert_eq!(monitor.pool_count(), 4);
    /// ```
    pub fn new(
        registry: Arc<PoolRegistry>,
        observer: Arc<IoObserver>,
        config: HealthMonitorConfig,
    ) -> (Arc<HealthMonitor>, mpsc::Receiver<HealthEvent>) {
        let (events, receiver) = mpsc::channel(config.event_capacity.max(1));
        (
            Arc::new(Self {
                registry,
                observer,
                config,
                events,
                state: Mutex::new(HashMap::new()),
            }),
            receiver,
        )
    }

    /// The number of registered pools (observability/tests).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_storage::io::IoObserver;
    /// use oceanfs_storage::pool::health::{HealthMonitor, HealthMonitorConfig};
    /// use oceanfs_storage::PoolRegistry;
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = Arc::new(
    ///     PoolRegistry::from_config(&storage, &data_dir).expect("registry"),
    /// );
    /// let observer = Arc::new(IoObserver::new());
    /// registry.observe_into(&observer);
    /// let (monitor, _events) =
    ///     HealthMonitor::new(registry, observer, HealthMonitorConfig::default());
    /// assert_eq!(monitor.pool_count(), 4);
    /// ```
    pub fn pool_count(&self) -> usize {
        self.registry.pool_count()
    }

    /// Reports an explicit confirmed-loss event (device unplug from a
    /// runtime pool probe, etc.) — forces the pool to `Dead`.
    ///
    /// The monitor's tick derives confirmed loss from observer error
    /// kinds automatically; this is the escape hatch for events the
    /// observer cannot classify.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_storage::io::IoObserver;
    /// use oceanfs_storage::pool::health::{ConfirmedLoss, HealthMonitor, HealthMonitorConfig};
    /// use oceanfs_storage::{PoolRegistry, PoolStatus};
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = Arc::new(
    ///     PoolRegistry::from_config(&storage, &data_dir).expect("registry"),
    /// );
    /// let observer = Arc::new(IoObserver::new());
    /// registry.observe_into(&observer);
    /// let (monitor, _events) =
    ///     HealthMonitor::new(registry.clone(), observer.clone(), HealthMonitorConfig::default());
    /// monitor.report_confirmed_loss(0, ConfirmedLoss::DeviceUnplug);
    /// assert_eq!(registry.pool_by_id(0).expect("pool").status(), PoolStatus::Dead);
    /// ```
    pub fn report_confirmed_loss(&self, pool_id: u32, loss: ConfirmedLoss) {
        tracing::warn!(pool_id, ?loss, "confirmed pool loss reported");
        let changed = {
            let mut state = self.state.lock();
            let entry = state.entry(pool_id).or_insert_with(|| {
                PoolState::new(
                    self.registry
                        .pool_by_id(pool_id)
                        .map(|pool| pool.status())
                        .unwrap_or(PoolStatus::Healthy),
                    Instant::now(),
                )
            });
            let changed = entry.status != PoolStatus::Dead;
            entry.status = PoolStatus::Dead;
            changed
        };
        if changed {
            self.registry.set_status(pool_id, PoolStatus::Dead);
            self.apply_role_consequences(pool_id, PoolStatus::Dead);
            let _ = self
                .events
                .try_send(HealthEvent::StatusChanged { pool_id, status: PoolStatus::Dead });
        }
    }

    /// Resets the monitor's per-pool state to match an **external**
    /// status change — g7's WAL/metadata replacement resets a Dead pool
    /// to Healthy after a fresh store + catch-up.
    ///
    /// Without this, the monitor's internal mirror would stay `Dead`
    /// and the pool could never be re-confirmed Dead (or re-degraded)
    /// after recovery — `decide_transition(Dead, …)` is absorbing.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_storage::io::IoObserver;
    /// use oceanfs_storage::pool::health::{HealthMonitor, HealthMonitorConfig};
    /// use oceanfs_storage::{PoolRegistry, PoolStatus};
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = Arc::new(
    ///     PoolRegistry::from_config(&storage, &data_dir).expect("registry"),
    /// );
    /// let observer = Arc::new(IoObserver::new());
    /// registry.observe_into(&observer);
    /// let (monitor, _events) =
    ///     HealthMonitor::new(registry.clone(), observer.clone(), HealthMonitorConfig::default());
    /// monitor.reset_pool(0, PoolStatus::Healthy);
    /// assert_eq!(registry.pool_by_id(0).expect("pool").status(), PoolStatus::Healthy);
    /// ```
    pub fn reset_pool(&self, pool_id: u32, status: PoolStatus) {
        let mut state = self.state.lock();
        let entry = state.entry(pool_id).or_insert_with(|| PoolState::new(status, Instant::now()));
        entry.status = status;
        entry.clean_windows = 0;
        entry.history.clear();
        entry.next_tick = Instant::now();
    }

    // [review][configuration][high]
    // the tick rate should be configurable by the end user.
    // also, one second is  a very fast tick rate, disk health doesnt merely move that fast,
    // it should be mush slower.
    // [end]
    /// Runs the per-node monitor loop until `shutdown` is cancelled.
    ///
    /// A coarse 1s base ticker drives the per-pool cadence (each pool
    /// ticks every `detection_window_secs` from its f1 health config, or
    /// the config override).
    pub async fn run(&self, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = ticker.tick() => self.tick_due_pools(Instant::now()),
            }
        }
    }

    /// Ticks every pool whose cadence is due.
    fn tick_due_pools(&self, now: Instant) {
        for pool in self.registry.pools() {
            self.tick_pool(&pool, now);
        }
    }

    // [review][algorithmic][critical]
    // as of now, the pools health status are memory resident only.
    // meaning after every node restart, we might accept a dead pool,
    // and try writing to it.
    // [end]
    /// Ticks one pool: snapshot → trend → decide → apply.
    fn tick_pool(&self, pool: &Arc<crate::pool::StoragePool>, now: Instant) {
        let config = pool.health_config();
        let tick_secs = self
            .config
            .tick_interval
            .map(|interval| interval.as_secs())
            .unwrap_or(config.detection_window_secs)
            .max(1);
        let pool_id = pool.id();
        // Perf 7.1: the decision (snapshot + trend) runs under the pool
        // state lock; the registry writes + event send happen after it is
        // released.
        let new_status = {
            let mut state = self.state.lock();
            let entry = state.entry(pool_id).or_insert_with(|| PoolState::new(pool.status(), now));
            if now < entry.next_tick {
                return;
            }
            entry.next_tick = now + Duration::from_secs(tick_secs);
            let signal = self.observer.snapshot(pool_id).unwrap_or_default();
            // Bounded history (perf 1.3): trend_window / tick, clamped so
            // a fast test tick does not grow it unboundedly.
            let history_len = (config.trend_window_secs / tick_secs).clamp(4, 64) as usize;
            entry.history.push_back(signal);
            while entry.history.len() > history_len {
                entry.history.pop_front();
            }
            let history: Vec<PoolSignal> = entry.history.iter().copied().collect();
            let verdict = evaluate_trend(&history, pool.tech());
            let (new_status, clean_windows) = decide_transition(
                entry.status,
                &signal,
                verdict,
                &config,
                entry.clean_windows,
                tick_secs,
            );
            let changed = new_status != entry.status;
            entry.status = new_status;
            entry.clean_windows = clean_windows;
            if changed {
                Some(new_status)
            } else {
                None
            }
        };
        if let Some(status) = new_status {
            self.registry.set_status(pool_id, status);
            self.apply_role_consequences(pool_id, status);
            let _ = self.events.try_send(HealthEvent::StatusChanged { pool_id, status });
        }
    }

    /// Applies the D3 role consequence the monitor owns: `write_degraded`
    /// is set ONLY when the **wal** pool is **Dead** (Degraded never sets
    /// it), and cleared when the pool returns to Healthy (replacement,
    /// g7). Other role consequences (metadata → node_unavailable, data →
    /// affected segments, hints → rejection) are the node layer's (they
    /// subscribe via the event channel).
    fn apply_role_consequences(&self, pool_id: u32, status: PoolStatus) {
        if let Some(pool) = self.registry.pool_by_id(pool_id) {
            if pool.role() == PoolRole::Wal {
                self.registry.set_write_degraded(pool_id, status == PoolStatus::Dead);
            }
        }
    }
}

/// The pure per-tick transition decision (unit-testable).
///
/// Returns the pool's next status plus the updated consecutive
/// zero-error window count (the Degraded → Healthy hysteresis
/// accumulator).
///
/// # Examples
///
/// ```
/// use oceanfs_core::PoolHealthConfig;
/// use oceanfs_storage::pool::health::{
///     decide_transition, PoolSignal, TrendVerdict,
/// };
/// use oceanfs_storage::PoolStatus;
///
/// let config = PoolHealthConfig::default();
/// let mut signal = PoolSignal::default();
/// signal.errors = 0;
/// let (status, _) = decide_transition(
///     PoolStatus::Healthy,
///     &signal,
///     TrendVerdict::Stable,
///     &config,
///     0,
///     30,
/// );
/// assert_eq!(status, PoolStatus::Healthy);
/// ```
pub fn decide_transition(
    current: PoolStatus,
    signal: &PoolSignal,
    verdict: TrendVerdict,
    config: &PoolHealthConfig,
    clean_windows: u64,
    tick_secs: u64,
) -> (PoolStatus, u64) {
    let degrading = verdict == TrendVerdict::Degrading
        || (signal.errors >= config.min_errors && signal.error_rate > config.error_rate_threshold)
        || latency_spike(signal, config);

    match current {
        PoolStatus::Healthy => {
            if degrading {
                (PoolStatus::Degraded, 0)
            } else {
                (PoolStatus::Healthy, clean_windows)
            }
        }
        PoolStatus::Degraded => {
            if ConfirmedLoss::from_signal(signal).is_some() {
                // Confirmed loss only — latency/trend alone never
                // confirms Dead (ADR-0029 §D3).
                (PoolStatus::Dead, 0)
            } else if signal.errors == 0 {
                let cleaned = clean_windows.saturating_add(1);
                if cleaned.saturating_mul(tick_secs) >= config.recovery_window_secs {
                    // Hysteresis: a clean window long enough recovers.
                    (PoolStatus::Healthy, 0)
                } else {
                    (PoolStatus::Degraded, cleaned)
                }
            } else {
                // Errors persist but are not confirmed loss — stay
                // Degraded (suspicion, route around).
                (PoolStatus::Degraded, 0)
            }
        }
        // Dead is absorbing until replacement (g7/g8) — the node
        // explicitly resets it after a fresh WAL/store + catch-up.
        PoolStatus::Dead => (PoolStatus::Dead, clean_windows),
    }
}

/// `true` when the window shows a latency spike: the worst p99 across ops
/// exceeds `latency_factor` × the worst p50 across ops (both present).
fn latency_spike(signal: &PoolSignal, config: &PoolHealthConfig) -> bool {
    let p50 = signal
        .latency
        .iter()
        .filter_map(|l| l.p50.map(|d| d.as_nanos() as f64))
        .max_by(f64::total_cmp)
        .unwrap_or(0.0);
    let p99 = signal
        .latency
        .iter()
        .filter_map(|l| l.p99.map(|d| d.as_nanos() as f64))
        .max_by(f64::total_cmp)
        .unwrap_or(0.0);
    p50 > 0.0 && p99 > config.latency_factor * p50
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// A history whose error-rate series doubles every window: 1, 2, 4.
    fn doubling_error_history() -> Vec<PoolSignal> {
        [1.0_f64, 2.0, 4.0]
            .into_iter()
            .map(|rate| PoolSignal {
                error_rate: rate,
                ops: 100,
                errors: (rate * 100.0) as u64,
                ..PoolSignal::default()
            })
            .collect()
    }

    fn signal_with_p99(op: IoOp, nanos: u64) -> PoolSignal {
        let mut signal = PoolSignal::default();
        signal.latency[op.as_usize()].p99 = Some(Duration::from_nanos(nanos));
        signal
    }

    // -- Slope detection --

    #[test]
    fn exponential_error_growth_below_threshold_is_degrading() {
        let history = doubling_error_history();
        assert_eq!(evaluate_trend(&history, PoolTech::Nvme), TrendVerdict::Degrading);
    }

    #[test]
    fn flat_low_errors_are_stable() {
        let history: Vec<PoolSignal> = (0..6)
            .map(|_| PoolSignal {
                error_rate: 0.001,
                ops: 1000,
                errors: 1,
                ..PoolSignal::default()
            })
            .collect();
        assert_eq!(evaluate_trend(&history, PoolTech::Nvme), TrendVerdict::Stable);
    }

    #[test]
    fn erratic_intermittent_errors_do_not_flip_alone() {
        // A spike in one window then back to baseline: no monotonic slope.
        let history = vec![
            PoolSignal { error_rate: 0.001, ops: 1000, errors: 1, ..PoolSignal::default() },
            PoolSignal { error_rate: 0.050, ops: 1000, errors: 50, ..PoolSignal::default() },
            PoolSignal { error_rate: 0.001, ops: 1000, errors: 1, ..PoolSignal::default() },
        ];
        assert_eq!(evaluate_trend(&history, PoolTech::Nvme), TrendVerdict::Stable);
    }

    #[test]
    fn single_window_spike_does_not_trip_slope() {
        // 0 → 5 is a spike, not a doubling-from-a-baseline trend.
        let history = vec![
            PoolSignal { error_rate: 0.0, ops: 100, errors: 0, ..PoolSignal::default() },
            PoolSignal { error_rate: 0.0, ops: 100, errors: 0, ..PoolSignal::default() },
            PoolSignal { error_rate: 0.05, ops: 100, errors: 5, ..PoolSignal::default() },
        ];
        assert_eq!(evaluate_trend(&history, PoolTech::Nvme), TrendVerdict::Stable);
    }

    #[test]
    fn all_zero_windows_are_stable() {
        let history: Vec<PoolSignal> = (0..5)
            .map(|_| PoolSignal { error_rate: 0.0, ops: 100, errors: 0, ..PoolSignal::default() })
            .collect();
        assert_eq!(evaluate_trend(&history, PoolTech::Nvme), TrendVerdict::Stable);
    }

    #[test]
    fn plateau_after_doubling_is_stable() {
        // 1 → 2 → 4 → 4: the last pair does not double.
        let history = vec![
            PoolSignal { error_rate: 1.0, ops: 100, errors: 100, ..PoolSignal::default() },
            PoolSignal { error_rate: 2.0, ops: 100, errors: 200, ..PoolSignal::default() },
            PoolSignal { error_rate: 4.0, ops: 100, errors: 400, ..PoolSignal::default() },
            PoolSignal { error_rate: 4.0, ops: 100, errors: 400, ..PoolSignal::default() },
        ];
        assert_eq!(evaluate_trend(&history, PoolTech::Nvme), TrendVerdict::Stable);
    }

    #[test]
    fn short_history_is_stable() {
        assert_eq!(evaluate_trend(&[], PoolTech::Nvme), TrendVerdict::Stable);
        let one = vec![PoolSignal::default()];
        assert_eq!(evaluate_trend(&one, PoolTech::Nvme), TrendVerdict::Stable);
        let two = vec![PoolSignal::default(), PoolSignal::default()];
        assert_eq!(evaluate_trend(&two, PoolTech::Nvme), TrendVerdict::Stable);
    }

    #[test]
    fn latency_p99_doubling_is_degrading() {
        // Write p99 doubles across the last two pairs (100 → 200 → 400).
        let history = vec![
            signal_with_p99(IoOp::Write, 100),
            signal_with_p99(IoOp::Write, 200),
            signal_with_p99(IoOp::Write, 400),
        ];
        assert_eq!(evaluate_trend(&history, PoolTech::CloudEphemeral), TrendVerdict::Degrading);
    }

    #[test]
    fn worst_op_p99_drives_the_latency_series() {
        // Reads stay flat at 100 ns; writes double (100 → 200 → 400).
        // The worst op's p99 drives the latency series.
        let windows: Vec<[Latency; IO_OP_COUNT]> = (0..3)
            .map(|i| {
                let mut window = [Latency::default(); IO_OP_COUNT];
                window[IoOp::Read.as_usize()].p99 = Some(Duration::from_nanos(100));
                window[IoOp::Write.as_usize()].p99 =
                    Some(Duration::from_nanos(100 * 2_u64.pow(i as u32)));
                window
            })
            .collect();
        let history: Vec<PoolSignal> = windows
            .into_iter()
            .map(|latency| PoolSignal { latency, ..PoolSignal::default() })
            .collect();
        assert_eq!(evaluate_trend(&history, PoolTech::CloudEphemeral), TrendVerdict::Degrading);
    }

    // -- Tech-specific SMART baselines --

    #[test]
    fn hdd_reallocated_sector_growth_is_degrading() {
        let history = vec![
            PoolSignal {
                smart: SmartCounters { reallocated_sectors: Some(0), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
            PoolSignal {
                smart: SmartCounters { reallocated_sectors: Some(2), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
            PoolSignal {
                smart: SmartCounters { reallocated_sectors: Some(5), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
        ];
        assert_eq!(evaluate_trend(&history, PoolTech::Hdd), TrendVerdict::Degrading);
    }

    #[test]
    fn hdd_pending_sector_growth_is_degrading() {
        let history = vec![
            PoolSignal {
                smart: SmartCounters { pending_sectors: Some(1), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
            PoolSignal {
                smart: SmartCounters { pending_sectors: Some(4), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
        ];
        assert_eq!(evaluate_trend(&history, PoolTech::Hdd), TrendVerdict::Degrading);
    }

    #[test]
    fn hdd_flat_smart_is_stable() {
        let history: Vec<PoolSignal> = (0..4)
            .map(|_| PoolSignal {
                smart: SmartCounters {
                    reallocated_sectors: Some(3),
                    pending_sectors: Some(1),
                    ..SmartCounters::default()
                },
                ..PoolSignal::default()
            })
            .collect();
        assert_eq!(evaluate_trend(&history, PoolTech::Hdd), TrendVerdict::Stable);
    }

    #[test]
    fn nvme_ecc_growth_is_degrading() {
        let history = vec![
            PoolSignal {
                smart: SmartCounters { uncorrectable_ecc: Some(0), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
            PoolSignal {
                smart: SmartCounters { uncorrectable_ecc: Some(3), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
        ];
        assert_eq!(evaluate_trend(&history, PoolTech::Nvme), TrendVerdict::Degrading);
    }

    #[test]
    fn nvme_wear_growth_is_degrading() {
        let history = vec![
            PoolSignal {
                smart: SmartCounters { wear_level: Some(40), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
            PoolSignal {
                smart: SmartCounters { wear_level: Some(55), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
        ];
        assert_eq!(evaluate_trend(&history, PoolTech::Ssd), TrendVerdict::Degrading);
    }

    #[test]
    fn cloud_ephemeral_ignores_smart_growth() {
        // Same SMART growth on a cloud-ephemeral pool: I/O signals only.
        let history = vec![
            PoolSignal {
                smart: SmartCounters { reallocated_sectors: Some(0), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
            PoolSignal {
                smart: SmartCounters { reallocated_sectors: Some(9), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
        ];
        assert_eq!(evaluate_trend(&history, PoolTech::CloudEphemeral), TrendVerdict::Stable);
    }

    #[test]
    fn auto_is_io_signals_only() {
        let history = vec![
            PoolSignal {
                smart: SmartCounters { uncorrectable_ecc: Some(0), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
            PoolSignal {
                smart: SmartCounters { uncorrectable_ecc: Some(7), ..SmartCounters::default() },
                ..PoolSignal::default()
            },
        ];
        assert_eq!(evaluate_trend(&history, PoolTech::Auto), TrendVerdict::Stable);
    }

    // -- ConfirmedLoss --

    #[test]
    fn confirmed_loss_maps_observer_kinds() {
        let mut kinds = [0u64; IO_ERROR_KIND_COUNT];
        kinds[IoErrorKind::NotFound.as_usize()] = 1;
        assert_eq!(ConfirmedLoss::from_kinds(&kinds), Some(ConfirmedLoss::SegmentNotFound));

        let mut kinds = [0u64; IO_ERROR_KIND_COUNT];
        kinds[IoErrorKind::Other.as_usize()] = 1;
        assert_eq!(ConfirmedLoss::from_kinds(&kinds), Some(ConfirmedLoss::FsyncIo));

        let mut kinds = [0u64; IO_ERROR_KIND_COUNT];
        kinds[IoErrorKind::InvalidData.as_usize()] = 1;
        assert_eq!(ConfirmedLoss::from_kinds(&kinds), Some(ConfirmedLoss::DeviceUnplug));

        let kinds = [0u64; IO_ERROR_KIND_COUNT];
        assert_eq!(ConfirmedLoss::from_kinds(&kinds), None);
    }

    #[test]
    fn confirmed_loss_ignores_non_confirming_kinds() {
        let mut kinds = [0u64; IO_ERROR_KIND_COUNT];
        kinds[IoErrorKind::TimedOut.as_usize()] = 5;
        kinds[IoErrorKind::StorageFull.as_usize()] = 2;
        assert_eq!(ConfirmedLoss::from_kinds(&kinds), None);
    }

    // -- decide_transition --

    fn fast_config() -> PoolHealthConfig {
        PoolHealthConfig {
            min_errors: 1,
            detection_window_secs: 1,
            recovery_window_secs: 1,
            ..PoolHealthConfig::default()
        }
    }

    fn signal_with_errors(errors: u64) -> PoolSignal {
        // ops includes the errors so the rate is 1.0 when errors > 0.
        PoolSignal {
            errors,
            ops: errors,
            error_rate: if errors > 0 { 1.0 } else { 0.0 },
            ..PoolSignal::default()
        }
    }

    #[test]
    fn healthy_clean_stays_healthy() {
        let signal = signal_with_errors(0);
        let (status, _) = decide_transition(
            PoolStatus::Healthy,
            &signal,
            TrendVerdict::Stable,
            &fast_config(),
            0,
            1,
        );
        assert_eq!(status, PoolStatus::Healthy);
    }

    #[test]
    fn healthy_trend_degrading_degrades() {
        let signal = signal_with_errors(0);
        let (status, _) = decide_transition(
            PoolStatus::Healthy,
            &signal,
            TrendVerdict::Degrading,
            &fast_config(),
            0,
            1,
        );
        assert_eq!(status, PoolStatus::Degraded);
    }

    #[test]
    fn healthy_absolute_threshold_spike_degrades() {
        let signal = signal_with_errors(5);
        let (status, _) = decide_transition(
            PoolStatus::Healthy,
            &signal,
            TrendVerdict::Stable,
            &fast_config(),
            0,
            1,
        );
        assert_eq!(status, PoolStatus::Degraded);
    }

    #[test]
    fn healthy_errors_below_min_errors_stay_healthy() {
        let config = PoolHealthConfig { min_errors: 3, ..fast_config() };
        // 1 error < min_errors: the rate threshold is not enough alone.
        let signal = signal_with_errors(1);
        let (status, _) =
            decide_transition(PoolStatus::Healthy, &signal, TrendVerdict::Stable, &config, 0, 1);
        assert_eq!(status, PoolStatus::Healthy);
    }

    #[test]
    fn healthy_latency_spike_degrades() {
        let mut signal = signal_with_errors(0);
        let mut window = [Latency::default(); IO_OP_COUNT];
        window[IoOp::Read.as_usize()].p50 = Some(Duration::from_micros(100));
        window[IoOp::Read.as_usize()].p99 = Some(Duration::from_millis(1)); // 10x factor
        signal.latency = window;
        let (status, _) = decide_transition(
            PoolStatus::Healthy,
            &signal,
            TrendVerdict::Stable,
            &fast_config(),
            0,
            1,
        );
        assert_eq!(status, PoolStatus::Degraded);
    }

    #[test]
    fn degraded_not_found_kind_confirms_dead() {
        let mut signal = signal_with_errors(1);
        signal.error_kinds[IoErrorKind::NotFound.as_usize()] = 1;
        let (status, _) = decide_transition(
            PoolStatus::Degraded,
            &signal,
            TrendVerdict::Stable,
            &fast_config(),
            0,
            1,
        );
        assert_eq!(status, PoolStatus::Dead);
    }

    #[test]
    fn degraded_eio_kind_confirms_dead() {
        let mut signal = signal_with_errors(1);
        signal.error_kinds[IoErrorKind::Other.as_usize()] = 1;
        let (status, _) = decide_transition(
            PoolStatus::Degraded,
            &signal,
            TrendVerdict::Stable,
            &fast_config(),
            0,
            1,
        );
        assert_eq!(status, PoolStatus::Dead);
    }

    #[test]
    fn degraded_latency_alone_never_confirms_dead() {
        // High latency + trend-degrading, but NO confirming kinds: the
        // pool stays Degraded (suspicion) — Dead needs confirmed loss.
        let mut signal = signal_with_errors(1);
        let mut window = [Latency::default(); IO_OP_COUNT];
        window[IoOp::Write.as_usize()].p50 = Some(Duration::from_micros(100));
        window[IoOp::Write.as_usize()].p99 = Some(Duration::from_millis(5)); // 50x
        signal.latency = window;
        let (status, _) = decide_transition(
            PoolStatus::Degraded,
            &signal,
            TrendVerdict::Degrading,
            &fast_config(),
            0,
            1,
        );
        assert_eq!(status, PoolStatus::Degraded);
    }

    #[test]
    fn degraded_clean_windows_below_recovery_stay_degraded() {
        let config = PoolHealthConfig { recovery_window_secs: 3, ..fast_config() };
        let signal = signal_with_errors(0);
        let (status, clean) =
            decide_transition(PoolStatus::Degraded, &signal, TrendVerdict::Stable, &config, 1, 1);
        assert_eq!(status, PoolStatus::Degraded);
        assert_eq!(clean, 2);
    }

    #[test]
    fn degraded_clean_windows_reach_recovery_heals() {
        let config = PoolHealthConfig { recovery_window_secs: 3, ..fast_config() };
        let signal = signal_with_errors(0);
        let (status, _) =
            decide_transition(PoolStatus::Degraded, &signal, TrendVerdict::Stable, &config, 2, 1);
        assert_eq!(status, PoolStatus::Healthy);
    }

    #[test]
    fn dead_is_absorbing() {
        let mut signal = signal_with_errors(1);
        signal.error_kinds[IoErrorKind::NotFound.as_usize()] = 1;
        let (status, _) = decide_transition(
            PoolStatus::Dead,
            &signal,
            TrendVerdict::Stable,
            &fast_config(),
            0,
            1,
        );
        assert_eq!(status, PoolStatus::Dead);
    }

    // -- HealthMonitor (tick_pool drives the registry) --

    use oceanfs_core::{MissingRootPolicy, PoolRole, StorageConfig, StoragePoolConfig};

    fn pool_config(name: &str, role: PoolRole, root: &std::path::Path) -> StoragePoolConfig {
        StoragePoolConfig {
            name: name.to_string(),
            role,
            root: root.to_path_buf(),
            weight: None,
            tech: PoolTech::Auto,
            health: fast_config(),
        }
    }

    /// A 2-pool registry (data + wal) with a fast detection window,
    /// plus a monitor whose tick is due immediately.
    fn monitor_setup() -> (
        Arc<PoolRegistry>,
        Arc<IoObserver>,
        Arc<HealthMonitor>,
        mpsc::Receiver<HealthEvent>,
        tempfile::TempDir,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let storage = StorageConfig {
            pools: vec![
                pool_config("data-a", PoolRole::Data, &tmp.path().join("nvme0")),
                pool_config("journal", PoolRole::Wal, &tmp.path().join("optane0")),
                pool_config("meta", PoolRole::Metadata, &tmp.path().join("optane1")),
                pool_config("hints", PoolRole::Hints, &tmp.path().join("hints0")),
            ],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let registry = Arc::new(PoolRegistry::from_config(&storage, &data_dir).unwrap());
        let observer = Arc::new(IoObserver::new());
        registry.observe_into(&observer);
        let (monitor, events) = HealthMonitor::new(
            registry.clone(),
            observer.clone(),
            HealthMonitorConfig {
                tick_interval: Some(Duration::from_secs(1)),
                ..Default::default()
            },
        );
        (registry, observer, monitor, events, tmp)
    }

    /// Advances the simulated clock by one tick and ticks every pool.
    fn tick_all(monitor: &HealthMonitor, now: &mut Instant) {
        *now += Duration::from_secs(1);
        monitor.tick_due_pools(*now);
    }

    #[test]
    fn monitor_degrades_pool_on_error_spike() {
        let (registry, observer, monitor, _events, _tmp) = monitor_setup();
        let pool_id = 0;

        // Feed one error + one op (errors >= min_errors, rate 1.0).
        observer.record_error(pool_id, IoErrorKind::TimedOut);
        observer.record_latency(pool_id, IoOp::Read, Duration::from_micros(1));
        monitor.tick_due_pools(Instant::now());

        assert_eq!(
            registry.pool_by_id(pool_id).unwrap().status(),
            PoolStatus::Degraded,
            "an error spike must degrade the pool"
        );
    }

    #[test]
    fn monitor_confirms_dead_from_observer_kinds() {
        let (registry, observer, monitor, _events, _tmp) = monitor_setup();
        let pool_id = 0;

        let mut now = Instant::now();
        // Phase 1: degrade.
        observer.record_error(pool_id, IoErrorKind::TimedOut);
        observer.record_latency(pool_id, IoOp::Read, Duration::from_micros(1));
        tick_all(&monitor, &mut now);
        assert_eq!(registry.pool_by_id(pool_id).unwrap().status(), PoolStatus::Degraded);

        // Phase 2: confirmed loss (ENOENT kind) → Dead.
        observer.record_error(pool_id, IoErrorKind::NotFound);
        observer.record_latency(pool_id, IoOp::Read, Duration::from_micros(1));
        tick_all(&monitor, &mut now);
        assert_eq!(
            registry.pool_by_id(pool_id).unwrap().status(),
            PoolStatus::Dead,
            "a NotFound kind on a Degraded pool confirms Dead"
        );
    }

    #[test]
    fn monitor_recovers_degraded_pool_after_clean_windows() {
        let (registry, observer, monitor, _events, _tmp) = monitor_setup();
        let pool_id = 0;

        // Degrade.
        observer.record_error(pool_id, IoErrorKind::TimedOut);
        observer.record_latency(pool_id, IoOp::Read, Duration::from_micros(1));
        monitor.tick_due_pools(Instant::now());
        assert_eq!(registry.pool_by_id(pool_id).unwrap().status(), PoolStatus::Degraded);

        // Clean windows accumulate; recovery_window_secs = 1 → one clean
        // tick heals.
        let mut now = Instant::now();
        for _ in 0..3 {
            tick_all(&monitor, &mut now);
        }
        assert_eq!(
            registry.pool_by_id(pool_id).unwrap().status(),
            PoolStatus::Healthy,
            "clean windows must recover a Degraded pool (hysteresis)"
        );
    }

    #[test]
    fn monitor_wal_dead_sets_write_degraded() {
        let (registry, observer, monitor, _events, _tmp) = monitor_setup();
        let wal_pool = registry.pool_by_role(PoolRole::Wal).unwrap();
        let wal_id = wal_pool.id();

        let mut now = Instant::now();
        // Degrade the WAL pool, then confirm Dead with an EIO kind.
        observer.record_error(wal_id, IoErrorKind::Other);
        observer.record_latency(wal_id, IoOp::Fsync, Duration::from_micros(1));
        tick_all(&monitor, &mut now);
        assert!(
            !registry.pool_by_id(wal_id).unwrap().write_degraded(),
            "Degraded never sets write_degraded"
        );

        observer.record_error(wal_id, IoErrorKind::Other);
        observer.record_latency(wal_id, IoOp::Fsync, Duration::from_micros(1));
        tick_all(&monitor, &mut now);
        assert_eq!(registry.pool_by_id(wal_id).unwrap().status(), PoolStatus::Dead);
        assert!(
            registry.pool_by_id(wal_id).unwrap().write_degraded(),
            "wal Dead must set write_degraded (D3 matrix)"
        );
    }

    #[test]
    fn monitor_wal_recovery_clears_write_degraded() {
        let (registry, observer, monitor, _events, _tmp) = monitor_setup();
        let wal_id = registry.pool_by_role(PoolRole::Wal).unwrap().id();

        let mut now = Instant::now();
        // wal Dead → write_degraded.
        observer.record_error(wal_id, IoErrorKind::Other);
        observer.record_latency(wal_id, IoOp::Fsync, Duration::from_micros(1));
        tick_all(&monitor, &mut now);
        observer.record_error(wal_id, IoErrorKind::Other);
        observer.record_latency(wal_id, IoOp::Fsync, Duration::from_micros(1));
        tick_all(&monitor, &mut now);
        assert!(registry.pool_by_id(wal_id).unwrap().write_degraded());

        // The node replaces the WAL (g7) and resets the pool to Healthy
        // explicitly — the monitor's next tick keeps write_degraded off.
        registry.set_status(wal_id, PoolStatus::Healthy);
        registry.set_write_degraded(wal_id, false);
        let mut now = Instant::now();
        for _ in 0..3 {
            tick_all(&monitor, &mut now);
        }
        assert!(!registry.pool_by_id(wal_id).unwrap().write_degraded());
    }

    #[test]
    fn monitor_report_confirmed_loss_forces_dead() {
        let (registry, _observer, monitor, _events, _tmp) = monitor_setup();
        let pool_id = 0;

        monitor.report_confirmed_loss(pool_id, ConfirmedLoss::DeviceUnplug);
        assert_eq!(
            registry.pool_by_id(pool_id).unwrap().status(),
            PoolStatus::Dead,
            "an explicit confirmed-loss report must force Dead"
        );
    }

    #[test]
    fn monitor_reconfirms_dead_after_registry_reset() {
        // g7 handoff: after a WAL/store replacement resets a Dead pool
        // to Healthy, the monitor must be able to re-degrade and
        // re-confirm it — its internal mirror must not stay Dead.
        let (registry, observer, monitor, _events, _tmp) = monitor_setup();
        let pool_id = 0;

        // Dead first.
        let mut now = Instant::now();
        observer.record_error(pool_id, IoErrorKind::TimedOut);
        observer.record_latency(pool_id, IoOp::Read, Duration::from_micros(1));
        tick_all(&monitor, &mut now);
        observer.record_error(pool_id, IoErrorKind::NotFound);
        observer.record_latency(pool_id, IoOp::Read, Duration::from_micros(1));
        tick_all(&monitor, &mut now);
        assert_eq!(registry.pool_by_id(pool_id).unwrap().status(), PoolStatus::Dead);

        // g7 replaces the device: registry + monitor reset to Healthy.
        registry.set_status(pool_id, PoolStatus::Healthy);
        monitor.reset_pool(pool_id, PoolStatus::Healthy);

        // Re-degrade + re-confirm works.
        observer.record_error(pool_id, IoErrorKind::TimedOut);
        observer.record_latency(pool_id, IoOp::Read, Duration::from_micros(1));
        tick_all(&monitor, &mut now);
        assert_eq!(registry.pool_by_id(pool_id).unwrap().status(), PoolStatus::Degraded);
        observer.record_error(pool_id, IoErrorKind::NotFound);
        observer.record_latency(pool_id, IoOp::Read, Duration::from_micros(1));
        tick_all(&monitor, &mut now);
        assert_eq!(
            registry.pool_by_id(pool_id).unwrap().status(),
            PoolStatus::Dead,
            "a reset pool must be re-confirmable Dead (g7 handoff)"
        );
    }

    #[test]
    fn monitor_emits_status_events() {
        let (_registry, observer, monitor, mut events, _tmp) = monitor_setup();
        let pool_id = 0;

        observer.record_error(pool_id, IoErrorKind::TimedOut);
        observer.record_latency(pool_id, IoOp::Read, Duration::from_micros(1));
        monitor.tick_due_pools(Instant::now());

        // The bounded channel carries the transition event.
        let event = events.try_recv().expect("status event");
        assert!(matches!(
            event,
            HealthEvent::StatusChanged { pool_id: 0, status: PoolStatus::Degraded }
        ));
    }
}
