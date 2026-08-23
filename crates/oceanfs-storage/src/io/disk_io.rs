//! Observed disk I/O surface — the health monitor's signal source (g1).
//!
//! ADR-0029 §D3 needs *signals* before it can *decide*: the segment I/O
//! path must record per-pool error counts, per-op latency percentiles,
//! and (where available) SMART counters. This module is that signal
//! source:
//!
//! - [`DiskIo`] — the **single observed file-op surface** (read /
//!   write / fsync / open). Every method returns `io::Result` and, when
//!   called on a pool-aware implementation, records latency + errors on
//!   the shared [`IoObserver`]. Implementations:
//!   - [`ObservedIo`] — the pool-aware wrapper (pool id + backend +
//!     observer) the seal pipeline performs its writes/fsyncs through;
//!   - [`IoBackend`](crate::io::IoBackend) — the concrete dispatcher in
//!     its default state (pool 0, [`NoopIoObserver`] — no recording);
//!   - [`FaultyIo`] — the unit-level fault injector (test framework
//!     Level-1) wrapping any `DiskIo`.
//! - [`IoObserver`] — per-pool signal accumulation: the record path is
//!   **lock-free** (atomic increments only — perf 3.2/7.1: no lock, no
//!   allocation); only the periodic [`IoObserver::snapshot`] path takes
//!   the bounded rotation lock (perf 7.1).
//! - [`NoopIoObserver`] — the const, no-op observer (the default), so a
//!   `DiskIo` that is not pool-wired costs nothing.
//!
//! # Locking
//!
//! LOCK ORDER: `PoolSignals.rotate_lock` is the only lock in this
//! module. The record path never takes it (it reads the atomic window
//! index and increments window atomics); the snapshot path takes it to
//! rotate the window ring exclusively. No other lock is ever held
//! alongside it.
//!
//! # Window semantics
//!
//! `PoolSignals` keeps a fixed ring of `WindowSignals` buckets. The
//! health monitor (g2) calls [`IoObserver::snapshot`] once per
//! `detection_window_secs`; each call rotates the ring (the bounded
//! periodic-path lock) and returns the just-rotated-out bucket as the
//! "last window" aggregate — the trend detector's
//! [`crate::pool::health::evaluate_trend`] input.

use std::{
    future::Future,
    io,
    path::Path,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant},
};

use oceanfs_core::Counter;
use parking_lot::Mutex;

use crate::pool::health::{Latency, PoolSignal, SmartCounters};

/// The maximum number of concurrently observed pools.
///
/// Pool ids are dense config-order integers (f2), so the observer keeps
/// a fixed pre-sized slot array indexed by id — the record path is one
/// bounds-checked `OnceLock` lookup, no map, no lock (perf 1.3/3.2).
/// Real topologies are 5–20 pools; ids beyond this cap are silently not
/// observed (defensive — a >256-pool config is operator error).
pub const MAX_OBSERVED_POOLS: usize = 256;

/// The number of window buckets in each pool's time-bucketed ring.
///
/// The health monitor consumes one bucket per detection window; a small
/// ring tolerates a missed tick without losing the *current* window.
const WINDOW_BUCKETS: usize = 4;

/// Number of latency-histogram buckets — a power-of-two nanosecond
/// histogram (bucket `i` covers `(2^(i-1), 2^i]` ns).
const LATENCY_BUCKETS: usize = 64;

/// The I/O operations the health monitor observes.
///
/// The trait's recorded surface: every `DiskIo` method maps to exactly
/// one [`IoOp`] so latency and error signals are attributed per
/// operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum IoOp {
    /// Read data from a file at an offset.
    Read = 0,
    /// Append/write data to a file.
    Write,
    /// Sync a file's data to durable storage.
    Fsync,
    /// Open/create a file.
    Open,
}

/// Number of [`IoOp`] variants (the width of per-op signal arrays, e.g.
/// `PoolSignal::latency`).
pub const IO_OP_COUNT: usize = (IoOp::Open as u8 as usize) + 1;

impl IoOp {
    /// Returns the numeric discriminant (`0..IO_OP_COUNT`) — the index
    /// into per-op signal arrays.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::IoOp;
    ///
    /// assert_eq!(IoOp::Read.as_usize(), 0);
    /// assert_eq!(IoOp::Fsync.as_usize(), 2);
    /// ```
    pub fn as_usize(self) -> usize {
        self as usize
    }

    /// Returns the lowercase wire name of the operation.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::IoOp;
    ///
    /// assert_eq!(IoOp::Read.as_str(), "read");
    /// assert_eq!(IoOp::Fsync.as_str(), "fsync");
    /// ```
    pub fn as_str(self) -> &'static str {
        match self {
            IoOp::Read => "read",
            IoOp::Write => "write",
            IoOp::Fsync => "fsync",
            IoOp::Open => "open",
        }
    }
}

/// Classified I/O error kinds — what `record_error` accumulates per
/// window (the time-bucketed "ring buffer" of error kinds).
///
/// A small, `#[non_exhaustive]`, `#[repr(u8)]` classification so the
/// per-window counters are a fixed pre-sized array (perf 1.3) and the
/// g2 health monitor can distinguish "transient reset" from "ENOSPC"
/// from "EIO-on-fsync" (the ADR-0029 §D3 Dead-confirming kind) without
/// stringly errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum IoErrorKind {
    /// `ErrorKind::NotFound`
    NotFound = 0,
    /// `ErrorKind::PermissionDenied`
    PermissionDenied,
    /// `ErrorKind::ConnectionRefused`
    ConnectionRefused,
    /// `ErrorKind::ConnectionReset`
    ConnectionReset,
    /// `ErrorKind::BrokenPipe`
    BrokenPipe,
    /// `ErrorKind::TimedOut`
    TimedOut,
    /// `ErrorKind::WriteZero`
    WriteZero,
    /// `ErrorKind::UnexpectedEof`
    UnexpectedEof,
    /// `ErrorKind::AlreadyExists`
    AlreadyExists,
    /// `ErrorKind::WouldBlock`
    WouldBlock,
    /// `ErrorKind::InvalidData`
    InvalidData,
    /// `ErrorKind::OutOfMemory`
    OutOfMemory,
    /// `ErrorKind::Unsupported`
    Unsupported,
    /// `ErrorKind::StorageFull`
    StorageFull,
    /// `ErrorKind::NotADirectory`
    NotADirectory,
    /// `ErrorKind::IsADirectory`
    IsADirectory,
    /// Anything else (includes `ErrorKind::Other` — e.g. raw `EIO`
    /// surfaces here).
    Other,
}

/// Number of [`IoErrorKind`] variants (the width of the per-window
/// error-kind counter array and `PoolSignal::error_kinds`). Bump when a
/// variant is added.
pub const IO_ERROR_KIND_COUNT: usize = (IoErrorKind::Other as u8 as usize) + 1;

impl IoErrorKind {
    /// Classifies a `std::io::Error` into the observed kind set.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io;
    /// use oceanfs_storage::io::IoErrorKind;
    ///
    /// let err = io::Error::from(io::ErrorKind::NotFound);
    /// assert_eq!(IoErrorKind::from_io_error(&err), IoErrorKind::NotFound);
    /// ```
    pub fn from_io_error(err: &io::Error) -> IoErrorKind {
        match err.kind() {
            io::ErrorKind::NotFound => IoErrorKind::NotFound,
            io::ErrorKind::PermissionDenied => IoErrorKind::PermissionDenied,
            io::ErrorKind::ConnectionRefused => IoErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionReset => IoErrorKind::ConnectionReset,
            io::ErrorKind::BrokenPipe => IoErrorKind::BrokenPipe,
            io::ErrorKind::TimedOut => IoErrorKind::TimedOut,
            io::ErrorKind::WriteZero => IoErrorKind::WriteZero,
            io::ErrorKind::UnexpectedEof => IoErrorKind::UnexpectedEof,
            io::ErrorKind::AlreadyExists => IoErrorKind::AlreadyExists,
            io::ErrorKind::WouldBlock => IoErrorKind::WouldBlock,
            io::ErrorKind::InvalidData => IoErrorKind::InvalidData,
            io::ErrorKind::OutOfMemory => IoErrorKind::OutOfMemory,
            io::ErrorKind::Unsupported => IoErrorKind::Unsupported,
            io::ErrorKind::StorageFull => IoErrorKind::StorageFull,
            io::ErrorKind::NotADirectory => IoErrorKind::NotADirectory,
            io::ErrorKind::IsADirectory => IoErrorKind::IsADirectory,
            // `Other` and any future `std` kinds.
            _ => IoErrorKind::Other,
        }
    }

    /// Returns the numeric discriminant — the index into the per-window
    /// error-kind counter array.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::IoErrorKind;
    ///
    /// assert_eq!(IoErrorKind::NotFound.as_usize(), 0);
    /// assert_eq!(IoErrorKind::Other.as_usize() + 1, 17);
    /// ```
    pub fn as_usize(self) -> usize {
        self as usize
    }

    /// Returns the lowercase wire name of the error kind.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::IoErrorKind;
    ///
    /// assert_eq!(IoErrorKind::WriteZero.as_str(), "write_zero");
    /// assert_eq!(IoErrorKind::Other.as_str(), "other");
    /// ```
    pub fn as_str(self) -> &'static str {
        match self {
            IoErrorKind::NotFound => "not_found",
            IoErrorKind::PermissionDenied => "permission_denied",
            IoErrorKind::ConnectionRefused => "connection_refused",
            IoErrorKind::ConnectionReset => "connection_reset",
            IoErrorKind::BrokenPipe => "broken_pipe",
            IoErrorKind::TimedOut => "timed_out",
            IoErrorKind::WriteZero => "write_zero",
            IoErrorKind::UnexpectedEof => "unexpected_eof",
            IoErrorKind::AlreadyExists => "already_exists",
            IoErrorKind::WouldBlock => "would_block",
            IoErrorKind::InvalidData => "invalid_data",
            IoErrorKind::OutOfMemory => "out_of_memory",
            IoErrorKind::Unsupported => "unsupported",
            IoErrorKind::StorageFull => "storage_full",
            IoErrorKind::NotADirectory => "not_a_directory",
            IoErrorKind::IsADirectory => "is_a_directory",
            IoErrorKind::Other => "other",
        }
    }
}

// ---------------------------------------------------------------------------
// Observed-signal surface
// ---------------------------------------------------------------------------

/// The signal-recording surface a [`DiskIo`] records on.
///
/// Two implementations exist: [`IoObserver`] (real per-pool signal
/// accumulation) and [`NoopIoObserver`] (const, no-op — the default).
/// The `DiskIo` methods always call through this surface, so a
/// non-pool-wired implementation costs nothing.
pub trait IoObserving: Send + Sync + std::fmt::Debug {
    /// Records a classified error for a pool.
    ///
    /// Unknown/out-of-range pool ids are ignored (no-op).
    fn record_error(&self, pool_id: u32, kind: IoErrorKind);

    /// Records a completed op's latency for a pool.
    ///
    /// Unknown/out-of-range pool ids are ignored (no-op).
    fn record_latency(&self, pool_id: u32, op: IoOp, duration: Duration);

    /// Returns the "last window" aggregate for a pool, rotating the
    /// per-pool window ring (the bounded periodic-path lock).
    ///
    /// Returns `None` for unregistered/out-of-range pool ids.
    fn snapshot(&self, pool_id: u32) -> Option<PoolSignal>;
}

// ---------------------------------------------------------------------------
// IoObserver — the real per-pool signal accumulator
// ---------------------------------------------------------------------------

/// Per-pool, per-window signal accumulator (ADR-0029 §D3 signal source).
///
/// The record path is **lock-free**: [`IoObserver::record_error`] and
/// [`IoObserver::record_latency`] do a bounds-checked slot lookup (the
/// pool's signal state is installed once via
/// [`IoObserver::register_pool`]) followed by atomic increments — no
/// lock, no allocation (perf 3.2/7.1). Only the periodic
/// [`IoObserver::snapshot`] path takes the bounded rotation lock, and it
/// never contends with the record path (perf 7.1).
///
/// Windows: each pool keeps a fixed ring of four `WindowSignals`
/// buckets (perf 1.3 — pre-sized). `snapshot` rotates the ring and
/// returns the just-rotated-out bucket as the last detection window.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use oceanfs_storage::io::{IoErrorKind, IoOp, IoObserver};
///
/// let observer = IoObserver::new();
/// observer.register_pool(0, None);
/// observer.record_latency(0, IoOp::Read, Duration::from_micros(120));
/// observer.record_error(0, IoErrorKind::TimedOut);
///
/// let signal = observer.snapshot(0).expect("pool 0 registered");
/// assert_eq!(signal.errors, 1);
/// // `record_latency` is the op counter; `record_error` only counts errors.
/// assert_eq!(signal.ops, 1);
/// assert_eq!(observer.io_error_count(0), 1);
/// ```
#[derive(Debug)]
pub struct IoObserver {
    /// Per-pool signal state, indexed by dense pool id (f2 config-order
    /// ids; runtime attach appends the next id). Installed once per pool
    /// by `register_pool` — the record path's slot lookup is a wait-free
    /// `OnceLock::get`.
    pools: Box<[OnceLock<Arc<PoolSignals>>; MAX_OBSERVED_POOLS]>,
}

impl IoObserver {
    /// Creates an empty observer (no pools registered — every record is
    /// a no-op until [`IoObserver::register_pool`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::IoObserver;
    ///
    /// let observer = IoObserver::new();
    /// assert!(observer.snapshot(0).is_none());
    /// ```
    pub fn new() -> Self {
        Self { pools: Box::new(std::array::from_fn(|_| OnceLock::new())) }
    }

    /// Registers a pool's signal state, optionally binding the pool's
    /// `oceanfs_pool_io_errors_total{pool_id}` counter (so recorded
    /// errors also appear on the metrics surface).
    ///
    /// Idempotent: re-registering an existing pool keeps the first
    /// installation (the record path relies on stable slots). Out-of-range
    /// pool ids (>= [`MAX_OBSERVED_POOLS`]) are ignored.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::IoObserver;
    ///
    /// let observer = IoObserver::new();
    /// observer.register_pool(0, None);
    /// observer.register_pool(1, None);
    /// assert!(observer.snapshot(0).is_some());
    /// assert!(observer.snapshot(1).is_some());
    /// assert!(observer.snapshot(2).is_none());
    /// ```
    pub fn register_pool(&self, pool_id: u32, io_errors: Option<Counter>) {
        let Some(slot) = self.slot(pool_id) else { return };
        slot.get_or_init(|| Arc::new(PoolSignals::new(io_errors)));
    }

    /// Records a classified error for a pool: increments the window's
    /// error + kind counters, the pool's cumulative error count, and the
    /// bound `oceanfs_pool_io_errors_total` series.
    ///
    /// Lock-free (atomic increments only — perf 3.2/7.1). Unknown pools
    /// are ignored.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::{IoErrorKind, IoObserver};
    ///
    /// let observer = IoObserver::new();
    /// observer.register_pool(0, None);
    /// observer.record_error(0, IoErrorKind::StorageFull);
    /// assert_eq!(observer.io_error_count(0), 1);
    /// ```
    pub fn record_error(&self, pool_id: u32, kind: IoErrorKind) {
        let Some(signals) = self.signals(pool_id) else { return };
        let window = &signals.buckets[signals.current.load(Ordering::Relaxed) % WINDOW_BUCKETS];
        window.errors.fetch_add(1, Ordering::Relaxed);
        window.error_kinds[kind.as_usize()].fetch_add(1, Ordering::Relaxed);
        signals.total_errors.fetch_add(1, Ordering::Relaxed);
        if let Some(counter) = &signals.io_errors {
            counter.inc();
        }
    }

    /// Records a completed op's latency for a pool: one atomic per
    /// power-of-two histogram bucket + the window's op counter.
    ///
    /// Lock-free (atomic increments only — perf 3.2/7.1). Unknown pools
    /// are ignored.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use oceanfs_storage::io::{IoOp, IoObserver};
    ///
    /// let observer = IoObserver::new();
    /// observer.register_pool(0, None);
    /// observer.record_latency(0, IoOp::Read, Duration::from_micros(150));
    /// let signal = observer.snapshot(0).expect("pool");
    /// assert_eq!(signal.ops, 1);
    /// ```
    pub fn record_latency(&self, pool_id: u32, op: IoOp, duration: Duration) {
        let Some(signals) = self.signals(pool_id) else { return };
        let window = &signals.buckets[signals.current.load(Ordering::Relaxed) % WINDOW_BUCKETS];
        window.ops.fetch_add(1, Ordering::Relaxed);
        window.latency[op.as_usize()].observe(duration.as_nanos() as u64);
    }

    /// Feeds a SMART counter snapshot into the pool's current window.
    ///
    /// Phase B v1: the values are stored (not accumulated — SMART
    /// counters are absolute) and may be synthetic in tests; real sysfs
    /// reads land later (accepted deviation). Atomic stores, no lock.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::IoObserver;
    /// use oceanfs_storage::pool::health::SmartCounters;
    ///
    /// let observer = IoObserver::new();
    /// observer.register_pool(0, None);
    /// observer.record_smart(0, SmartCounters {
    ///     reallocated_sectors: Some(7),
    ///     ..SmartCounters::default()
    /// });
    /// let signal = observer.snapshot(0).expect("pool");
    /// assert_eq!(signal.smart.reallocated_sectors, Some(7));
    /// ```
    pub fn record_smart(&self, pool_id: u32, smart: SmartCounters) {
        let Some(signals) = self.signals(pool_id) else { return };
        let window = &signals.buckets[signals.current.load(Ordering::Relaxed) % WINDOW_BUCKETS];
        if let Some(value) = smart.reallocated_sectors {
            window.smart_reallocated.store(value, Ordering::Relaxed);
        }
        if let Some(value) = smart.pending_sectors {
            window.smart_pending.store(value, Ordering::Relaxed);
        }
        if let Some(value) = smart.uncorrectable_ecc {
            window.smart_uncorrectable_ecc.store(value, Ordering::Relaxed);
        }
        if let Some(value) = smart.wear_level {
            window.smart_wear.store(value, Ordering::Relaxed);
        }
    }

    /// Returns the "last detection window" aggregate for a pool.
    ///
    /// Rotates the pool's window ring under the bounded periodic-path
    /// lock (perf 7.1 — the record path never takes this lock) and
    /// returns the just-rotated-out bucket, reset for the next window.
    /// The returned [`PoolSignal`] is the trend detector's input
    /// (the g2 monitor accumulates snapshots into the history slice
    /// [`crate::pool::health::evaluate_trend`] consumes).
    ///
    /// Returns `None` for unregistered/out-of-range pool ids.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use oceanfs_storage::io::{IoErrorKind, IoOp, IoObserver};
    ///
    /// let observer = IoObserver::new();
    /// observer.register_pool(0, None);
    /// observer.record_latency(0, IoOp::Write, Duration::from_micros(400));
    /// observer.record_error(0, IoErrorKind::WriteZero);
    ///
    /// let signal = observer.snapshot(0).expect("pool 0");
    /// assert_eq!(signal.errors, 1);
    /// assert_eq!(signal.ops, 1);
    /// assert!(signal.error_rate > 0.0);
    /// // The window was consumed — the next snapshot sees only new ops.
    /// let next = observer.snapshot(0).expect("pool 0");
    /// assert_eq!(next.ops, 0);
    /// ```
    pub fn snapshot(&self, pool_id: u32) -> Option<PoolSignal> {
        let signals = self.signals(pool_id)?;
        // The snapshot path's bounded lock (perf 7.1): rotation is
        // exclusive so two concurrent monitors cannot skip a window.
        // The record path never takes it — no deadlock by construction.
        let _guard = signals.rotate_lock.lock();
        let old = signals.current.load(Ordering::Relaxed);
        signals.current.store((old + 1) % WINDOW_BUCKETS, Ordering::Relaxed);
        let window = &signals.buckets[old];
        let ops = window.ops.swap(0, Ordering::Relaxed);
        let errors = window.errors.swap(0, Ordering::Relaxed);
        let latency = std::array::from_fn(|i| window.latency[i].snapshot());
        // The per-kind counters ride the same rotation as the aggregate:
        // g2's HealthMonitor consumes them for confirmed-loss detection
        // (ENOENT → SegmentNotFound, EIO → FsyncIo, write-verify →
        // DeviceUnplug — ADR-0029 §D3 Dead-confirming kinds).
        let error_kinds = std::array::from_fn(|i| window.error_kinds[i].swap(0, Ordering::Relaxed));
        let smart = SmartCounters {
            reallocated_sectors: Some(window.smart_reallocated.swap(0, Ordering::Relaxed)),
            pending_sectors: Some(window.smart_pending.swap(0, Ordering::Relaxed)),
            uncorrectable_ecc: Some(window.smart_uncorrectable_ecc.swap(0, Ordering::Relaxed)),
            wear_level: Some(window.smart_wear.swap(0, Ordering::Relaxed)),
        };
        Some(PoolSignal {
            error_rate: if ops > 0 { errors as f64 / ops as f64 } else { 0.0 },
            ops,
            errors,
            latency,
            smart,
            error_kinds,
        })
    }

    /// Returns the pool's cumulative recorded error count (across all
    /// windows) — the observability/test counter.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::{IoErrorKind, IoObserver};
    ///
    /// let observer = IoObserver::new();
    /// observer.register_pool(0, None);
    /// observer.record_error(0, IoErrorKind::NotFound);
    /// observer.record_error(0, IoErrorKind::NotFound);
    /// assert_eq!(observer.io_error_count(0), 2);
    /// assert_eq!(observer.io_error_count(1), 0);
    /// ```
    pub fn io_error_count(&self, pool_id: u32) -> u64 {
        match self.signals(pool_id) {
            Some(signals) => signals.total_errors.load(Ordering::Relaxed),
            None => 0,
        }
    }

    /// Returns the pool signal slot, bounds-checked.
    fn slot(&self, pool_id: u32) -> Option<&OnceLock<Arc<PoolSignals>>> {
        self.pools.get(pool_id as usize)
    }

    /// Returns the pool's installed signal state, if registered.
    fn signals(&self, pool_id: u32) -> Option<&PoolSignals> {
        self.slot(pool_id)?.get().map(Arc::as_ref)
    }
}

impl Default for IoObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl IoObserving for IoObserver {
    fn record_error(&self, pool_id: u32, kind: IoErrorKind) {
        IoObserver::record_error(self, pool_id, kind);
    }

    fn record_latency(&self, pool_id: u32, op: IoOp, duration: Duration) {
        IoObserver::record_latency(self, pool_id, op, duration);
    }

    fn snapshot(&self, pool_id: u32) -> Option<PoolSignal> {
        IoObserver::snapshot(self, pool_id)
    }
}

/// The const, no-op observer — the default for non-pool-wired `DiskIo`s
/// (e.g. [`IoBackend`](crate::io::IoBackend)) and the sealer's
/// [`SealConfig`](crate::segment::sealer::SealConfig)
/// default, so the seal pipeline costs nothing until an observer is
/// wired.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use oceanfs_storage::io::{IoErrorKind, IoObserving, IoOp, NoopIoObserver};
///
/// let noop = NoopIoObserver;
/// noop.record_error(0, IoErrorKind::TimedOut);   // no-op
/// noop.record_latency(0, IoOp::Read, Duration::from_millis(1)); // no-op
/// assert!(noop.snapshot(0).is_none());
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopIoObserver;

impl IoObserving for NoopIoObserver {
    fn record_error(&self, _pool_id: u32, _kind: IoErrorKind) {}

    fn record_latency(&self, _pool_id: u32, _op: IoOp, _duration: Duration) {}

    fn snapshot(&self, _pool_id: u32) -> Option<PoolSignal> {
        None
    }
}

// ---------------------------------------------------------------------------
// Window + histogram internals
// ---------------------------------------------------------------------------

/// One detection-window bucket of per-pool signal counters.
///
/// All fields are atomics so the record path (concurrent writers) never
/// locks; the snapshot path reads them with `swap(0)` to reset.
#[derive(Debug)]
struct WindowSignals {
    /// Total observed ops this window (successes AND failures — the
    /// error-rate denominator).
    ops: AtomicU64,
    /// Observed errors this window.
    errors: AtomicU64,
    /// Per-kind error counts this window (the time-bucketed error-kind
    /// ring, pre-sized — perf 1.3).
    error_kinds: [AtomicU64; IO_ERROR_KIND_COUNT],
    /// Per-op power-of-two latency histograms.
    latency: [LatencyHist; IO_OP_COUNT],
    /// SMART absolute counters (Phase B v1: synthetic-fed).
    smart_reallocated: AtomicU64,
    smart_pending: AtomicU64,
    smart_uncorrectable_ecc: AtomicU64,
    smart_wear: AtomicU64,
}

impl WindowSignals {
    fn new() -> Self {
        Self {
            ops: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            error_kinds: std::array::from_fn(|_| AtomicU64::new(0)),
            latency: std::array::from_fn(|_| LatencyHist::new()),
            smart_reallocated: AtomicU64::new(0),
            smart_pending: AtomicU64::new(0),
            smart_uncorrectable_ecc: AtomicU64::new(0),
            smart_wear: AtomicU64::new(0),
        }
    }
}

/// Per-pool signal state: the time-bucketed window ring plus the bound
/// `oceanfs_pool_io_errors_total` series.
#[derive(Debug)]
struct PoolSignals {
    /// Fixed pre-sized window ring (perf 1.3). The record path writes to
    /// the `current` bucket; `snapshot` rotates under `rotate_lock`.
    buckets: Box<[WindowSignals; WINDOW_BUCKETS]>,
    /// Index of the bucket recorders are writing to.
    current: AtomicUsize,
    /// Exclusive rotation guard — the periodic snapshot path only
    /// (perf 7.1: the record path never takes a lock).
    rotate_lock: Mutex<()>,
    /// The pool's registered `oceanfs_pool_io_errors_total{pool_id}`
    /// series (None when no metrics are bound).
    io_errors: Option<Counter>,
    /// Cumulative error count across all windows (monotonic).
    total_errors: AtomicU64,
}

impl PoolSignals {
    fn new(io_errors: Option<Counter>) -> Self {
        Self {
            buckets: Box::new(std::array::from_fn(|_| WindowSignals::new())),
            current: AtomicUsize::new(0),
            rotate_lock: Mutex::new(()),
            io_errors,
            total_errors: AtomicU64::new(0),
        }
    }
}

/// Lock-free power-of-two nanosecond latency histogram.
///
/// Bucket `i` covers `(2^(i-1), 2^i]` ns (bucket 0 covers 0). `observe`
/// is one atomic increment; percentiles are computed on the snapshot
/// path by a cumulative scan (perf 3.2/7.1 — no lock on the record
/// path).
#[derive(Debug)]
struct LatencyHist {
    buckets: [AtomicU64; LATENCY_BUCKETS],
}

impl LatencyHist {
    fn new() -> Self {
        Self { buckets: std::array::from_fn(|_| AtomicU64::new(0)) }
    }

    /// Records one latency sample (atomic increment — perf 3.2).
    fn observe(&self, nanos: u64) {
        self.buckets[bucket_index(nanos)].fetch_add(1, Ordering::Relaxed);
    }

    /// Consumes the histogram (resetting it) and returns p50/p99/p999.
    fn snapshot(&self) -> Latency {
        let counts: [u64; LATENCY_BUCKETS] =
            std::array::from_fn(|i| self.buckets[i].swap(0, Ordering::Relaxed));
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return Latency::default();
        }
        let percentile = |fraction: f64| -> Duration {
            let target = (total as f64 * fraction).ceil().max(1.0) as u64;
            let mut cumulative = 0u64;
            for (index, count) in counts.iter().enumerate() {
                cumulative += *count;
                if cumulative >= target {
                    // Bucket `index`'s upper bound: 2^index ns.
                    return Duration::from_nanos(1u64 << index);
                }
            }
            // Unreachable: cumulative reaches `total >= target`.
            Duration::from_nanos(0)
        };
        Latency {
            p50: Some(percentile(0.50)),
            p99: Some(percentile(0.99)),
            p999: Some(percentile(0.999)),
        }
    }
}

/// Maps a nanosecond latency to its power-of-two histogram bucket
/// (`0..=63`; 0 for 0 ns).
fn bucket_index(nanos: u64) -> usize {
    if nanos == 0 {
        return 0;
    }
    let leading = nanos.leading_zeros();
    (63 - leading).min(LATENCY_BUCKETS as u32 - 1) as usize
}

// ---------------------------------------------------------------------------
// DiskIo — the single observed file-op surface
// ---------------------------------------------------------------------------

/// The single observed file-op surface of the segment I/O path
/// (ADR-0029 §D3 signal source).
///
/// Wraps read / write / fsync / open. Every *observed* method (the
/// `read`/`read_direct`/`open`/`write`/`fsync`/`write_handle`/
/// `fsync_handle` group) returns `io::Result` and records latency +
/// errors on [`DiskIo::observer`] for [`DiskIo::pool_id`]; the *raw*
/// methods (`*_raw`) are the actual file ops implementations override.
///
/// Implementations:
/// - [`IoBackend`](crate::io::IoBackend) — the concrete dispatcher in
///   its default state (pool 0, [`NoopIoObserver`]);
/// - [`ObservedIo`] — the pool-aware wrapper (pool id + backend +
///   observer) the seal pipeline performs writes/fsyncs through;
/// - [`FaultyIo`] — the test fault injector.
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// use oceanfs_storage::io::{DiskIo, IoBackend};
///
/// async fn read_first_bytes(io: &dyn DiskIo, path: &Path) -> std::io::Result<Vec<u8>> {
///     let mut buf = vec![0u8; 8];
///     let n = io.read(path, &mut buf, 0).await?;
///     buf.truncate(n);
///     Ok(buf)
/// }
///
/// # let _ = read_first_bytes;
/// ```
#[async_trait::async_trait]
pub trait DiskIo: Send + Sync {
    /// The pool this disk instance serves (0 = legacy / unattributed).
    fn pool_id(&self) -> u32;

    /// The observer latency/errors are recorded on.
    fn observer(&self) -> &dyn IoObserving;

    // -- raw ops (implemented by the backend) --

    /// Reads bytes from `path` at `offset` (buffered).
    async fn read_raw(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    /// Reads bytes from `path` at `offset` with `O_DIRECT` semantics.
    async fn read_direct_raw(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    /// Opens `path` for reading.
    async fn open_raw(&self, path: &Path) -> io::Result<tokio::fs::File>;

    /// Writes `buf` to `path` at `offset`.
    async fn write_raw(&self, path: &Path, buf: &[u8], offset: u64) -> io::Result<()>;

    /// Syncs `path`'s data to durable storage.
    async fn fsync_raw(&self, path: &Path) -> io::Result<()>;

    /// Writes `buf` to an already-open file handle — the durability
    /// (seal/flush) path, which owns an open temp file. Default: a plain
    /// `write_all`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the write fails (short write, disk
    /// error, ...).
    fn write_handle_raw(&self, file: &std::fs::File, buf: &[u8]) -> io::Result<()> {
        use std::io::Write;
        let mut reference = file;
        reference.write_all(buf)
    }

    /// Syncs an already-open file handle's data to durable storage.
    /// Default: `sync_data` (fdatasync semantics).
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the sync fails (e.g. EIO — the
    /// ADR-0029 §D3 Dead-confirming signal).
    fn fsync_handle_raw(&self, file: &std::fs::File) -> io::Result<()> {
        file.sync_data()
    }

    // -- observed wrappers (record latency + errors on the observer) --

    /// Observed buffered read.
    ///
    /// # Errors
    ///
    /// Returns the underlying read error (recorded on the observer).
    async fn read(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        observed_async(
            self.observer(),
            self.pool_id(),
            IoOp::Read,
            self.read_raw(path, buf, offset),
        )
        .await
    }

    /// Observed `O_DIRECT` read.
    ///
    /// # Errors
    ///
    /// Returns the underlying read error (recorded on the observer).
    async fn read_direct(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        observed_async(
            self.observer(),
            self.pool_id(),
            IoOp::Read,
            self.read_direct_raw(path, buf, offset),
        )
        .await
    }

    /// Observed open.
    ///
    /// # Errors
    ///
    /// Returns the underlying open error (recorded on the observer).
    async fn open(&self, path: &Path) -> io::Result<tokio::fs::File> {
        observed_async(self.observer(), self.pool_id(), IoOp::Open, self.open_raw(path)).await
    }

    /// Observed path write.
    ///
    /// # Errors
    ///
    /// Returns the underlying write error (recorded on the observer).
    async fn write(&self, path: &Path, buf: &[u8], offset: u64) -> io::Result<()> {
        observed_async(
            self.observer(),
            self.pool_id(),
            IoOp::Write,
            self.write_raw(path, buf, offset),
        )
        .await
    }

    /// Observed path fsync.
    ///
    /// # Errors
    ///
    /// Returns the underlying fsync error (recorded on the observer).
    async fn fsync(&self, path: &Path) -> io::Result<()> {
        observed_async(self.observer(), self.pool_id(), IoOp::Fsync, self.fsync_raw(path)).await
    }

    /// Observed handle write — the seal pipeline's temp-file write.
    ///
    /// # Errors
    ///
    /// Returns the underlying write error (recorded on the observer).
    fn write_handle(&self, file: &std::fs::File, buf: &[u8]) -> io::Result<()> {
        observed_sync(self.observer(), self.pool_id(), IoOp::Write, || {
            self.write_handle_raw(file, buf)
        })
    }

    /// Observed handle fsync — the flush coordinator's per-file barrier.
    ///
    /// # Errors
    ///
    /// Returns the underlying fsync error (recorded on the observer).
    fn fsync_handle(&self, file: &std::fs::File) -> io::Result<()> {
        observed_sync(self.observer(), self.pool_id(), IoOp::Fsync, || self.fsync_handle_raw(file))
    }
}

/// Records an awaited async op's latency (+ error kind) on the observer.
async fn observed_async<F, T>(
    observer: &dyn IoObserving,
    pool_id: u32,
    op: IoOp,
    future: F,
) -> io::Result<T>
where
    F: Future<Output = io::Result<T>>,
{
    let start = Instant::now();
    let result = future.await;
    let elapsed = start.elapsed();
    match &result {
        Ok(_) => observer.record_latency(pool_id, op, elapsed),
        Err(err) => {
            observer.record_latency(pool_id, op, elapsed);
            observer.record_error(pool_id, IoErrorKind::from_io_error(err));
        }
    }
    result
}

/// Records a synchronous op's latency (+ error kind) on the observer.
fn observed_sync<T>(
    observer: &dyn IoObserving,
    pool_id: u32,
    op: IoOp,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let start = Instant::now();
    let result = operation();
    let elapsed = start.elapsed();
    match &result {
        Ok(_) => observer.record_latency(pool_id, op, elapsed),
        Err(err) => {
            observer.record_latency(pool_id, op, elapsed);
            observer.record_error(pool_id, IoErrorKind::from_io_error(err));
        }
    }
    result
}

// ---------------------------------------------------------------------------
// ObservedIo — the pool-aware DiskIo
// ---------------------------------------------------------------------------

/// A pool-aware [`DiskIo`]: a concrete backend + a stable pool id + a
/// shared observer.
///
/// This is the single implementation production code performs observed
/// file ops through. The seal pipeline constructs one per seal (the
/// selected pool is known at that point); the g2 read path wires the
/// same shape.
///
/// The handle ops (`write_handle`/`fsync_handle`) use the trait's plain
/// file-op defaults; the path ops delegate to the wrapped backend.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_storage::io::{DiskIo, IoBackend, IoObserver, ObservedIo};
///
/// let observer = Arc::new(IoObserver::new());
/// observer.register_pool(0, None);
/// let io = ObservedIo {
///     pool_id: 0,
///     backend: Arc::new(IoBackend::default()),
///     observer,
/// };
/// assert_eq!(io.pool_id(), 0);
/// ```
#[derive(Debug)]
pub struct ObservedIo {
    /// The pool whose signals this instance records on.
    pub pool_id: u32,
    /// The concrete backend the path ops delegate to.
    pub backend: Arc<crate::io::IoBackend>,
    /// The shared observer signals are recorded on.
    pub observer: Arc<dyn IoObserving>,
}

#[async_trait::async_trait]
impl DiskIo for ObservedIo {
    fn pool_id(&self) -> u32 {
        self.pool_id
    }

    fn observer(&self) -> &dyn IoObserving {
        self.observer.as_ref()
    }

    async fn read_raw(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.backend.read_raw(path, buf, offset).await
    }

    async fn read_direct_raw(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.backend.read_direct_raw(path, buf, offset).await
    }

    async fn open_raw(&self, path: &Path) -> io::Result<tokio::fs::File> {
        self.backend.open_raw(path).await
    }

    async fn write_raw(&self, path: &Path, buf: &[u8], offset: u64) -> io::Result<()> {
        self.backend.write_raw(path, buf, offset).await
    }

    async fn fsync_raw(&self, path: &Path) -> io::Result<()> {
        self.backend.fsync_raw(path).await
    }
}

// ---------------------------------------------------------------------------
// FaultyIo — the test fault injector
// ---------------------------------------------------------------------------

/// Unit-level fault injector (test framework Level-1): wraps any
/// [`DiskIo`] and injects failures/latency into the ops the health
/// monitor observes.
///
/// Injection modes (each setter replaces the count-based fault mode and
/// clears the asymmetric kill switches):
///
/// - [`FaultyIo::fail_next`] — the next `calls` ops fail;
/// - [`FaultyIo::fail_after`] — the next `calls` ops succeed, then every
///   op fails;
/// - [`FaultyIo::die_on_read`] / [`FaultyIo::die_on_write`] — asymmetric
///   kill switches (only one op family fails);
/// - [`FaultyIo::delay`] — inject latency into every op (async ops await
///   `tokio::time::sleep`; sync handle ops block on the calling thread,
///   matching the seal pipeline's `spawn_blocking` context);
/// - [`FaultyIo::clear`] — resets all injection state.
///
/// Injected errors record on the wrapped implementation's observer, so
/// a `FaultyIo<ObservedIo>` fed into the seal pipeline makes the health
/// signal source verifiable under failure.
///
/// # Examples
///
/// ```
/// use std::io;
/// use oceanfs_storage::io::{DiskIo, FaultyIo, IoBackend};
///
/// let backend = IoBackend::TokioFs;
/// let faulty = FaultyIo::new(backend);
/// faulty.fail_next(2, io::ErrorKind::Other);
/// // The first two ops fail; the third passes through.
/// assert_eq!(faulty.inner().pool_id(), 0);
/// ```
#[derive(Debug)]
pub struct FaultyIo<D: DiskIo> {
    inner: D,
    state: Mutex<FaultState>,
}

/// The injector's mutable fault state (test-only; a lock is fine — this
/// type never runs on the production record path).
#[derive(Debug, Clone)]
struct FaultState {
    /// Remaining count-based failures (0 = disabled).
    fail_next: u64,
    /// `Some(remaining)` ops to let through before failing forever.
    fail_after: Option<u64>,
    /// Asymmetric kill switches.
    die_read: bool,
    die_write: bool,
    /// Injected latency for every op.
    delay: Option<Duration>,
    /// The error kind count-based faults inject.
    kind: io::ErrorKind,
}

impl Default for FaultState {
    fn default() -> Self {
        Self {
            fail_next: 0,
            fail_after: None,
            die_read: false,
            die_write: false,
            delay: None,
            kind: io::ErrorKind::Other,
        }
    }
}

impl<D: DiskIo> FaultyIo<D> {
    /// Wraps a `DiskIo`, initially fault-free.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::{FaultyIo, IoBackend};
    ///
    /// let faulty = FaultyIo::new(IoBackend::TokioFs);
    /// faulty.clear();
    /// ```
    pub fn new(inner: D) -> Self {
        Self { inner, state: Mutex::new(FaultState::default()) }
    }

    /// Makes the next `calls` ops fail with `kind`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io;
    /// use oceanfs_storage::io::{FaultyIo, IoBackend};
    ///
    /// let faulty = FaultyIo::new(IoBackend::TokioFs);
    /// faulty.fail_next(3, io::ErrorKind::TimedOut);
    /// ```
    pub fn fail_next(&self, calls: u64, kind: io::ErrorKind) {
        let mut state = self.state.lock();
        state.fail_next = calls;
        state.fail_after = None;
        state.die_read = false;
        state.die_write = false;
        state.kind = kind;
    }

    /// Lets the next `calls` ops succeed, then fails every subsequent op
    /// with `kind` (until cleared or re-configured).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io;
    /// use oceanfs_storage::io::{FaultyIo, IoBackend};
    ///
    /// let faulty = FaultyIo::new(IoBackend::TokioFs);
    /// faulty.fail_after(5, io::ErrorKind::StorageFull);
    /// ```
    pub fn fail_after(&self, calls: u64, kind: io::ErrorKind) {
        let mut state = self.state.lock();
        state.fail_after = Some(calls);
        state.fail_next = 0;
        state.die_read = false;
        state.die_write = false;
        state.kind = kind;
    }

    /// Kills every read op with `io::ErrorKind::Other` (writes and
    /// fsyncs pass through).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::{FaultyIo, IoBackend};
    ///
    /// let faulty = FaultyIo::new(IoBackend::TokioFs);
    /// faulty.die_on_read();
    /// ```
    pub fn die_on_read(&self) {
        let mut state = self.state.lock();
        state.die_read = true;
        state.fail_next = 0;
        state.fail_after = None;
    }

    /// Kills every write op with `io::ErrorKind::Other` (reads and
    /// fsyncs pass through).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::{FaultyIo, IoBackend};
    ///
    /// let faulty = FaultyIo::new(IoBackend::TokioFs);
    /// faulty.die_on_write();
    /// ```
    pub fn die_on_write(&self) {
        let mut state = self.state.lock();
        state.die_write = true;
        state.fail_next = 0;
        state.fail_after = None;
    }

    /// Injects `duration` of latency into every op (composes with any
    /// count-based fault mode).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use oceanfs_storage::io::{FaultyIo, IoBackend};
    ///
    /// let faulty = FaultyIo::new(IoBackend::TokioFs);
    /// faulty.delay(Duration::from_millis(5));
    /// ```
    pub fn delay(&self, duration: Duration) {
        self.state.lock().delay = Some(duration);
    }

    /// Resets every injection mode (fail counters, kill switches, delay).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io;
    /// use oceanfs_storage::io::{FaultyIo, IoBackend};
    ///
    /// let faulty = FaultyIo::new(IoBackend::TokioFs);
    /// faulty.fail_next(10, io::ErrorKind::Other);
    /// faulty.clear();
    /// ```
    pub fn clear(&self) {
        *self.state.lock() = FaultState::default();
    }

    /// Returns a reference to the wrapped `DiskIo`.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::{FaultyIo, IoBackend};
    ///
    /// let faulty = FaultyIo::new(IoBackend::TokioFs);
    /// assert!(matches!(faulty.inner(), IoBackend::TokioFs));
    /// ```
    pub fn inner(&self) -> &D {
        &self.inner
    }

    /// Consumes the wrapper, returning the wrapped `DiskIo`.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::{FaultyIo, IoBackend};
    ///
    /// let faulty = FaultyIo::new(IoBackend::TokioFs);
    /// let backend = faulty.into_inner();
    /// assert!(matches!(backend, IoBackend::TokioFs));
    /// ```
    pub fn into_inner(self) -> D {
        self.inner
    }

    /// Checks whether the current op should fail, consuming count-based
    /// faults.
    fn should_fail(&self, op: IoOp) -> Option<io::ErrorKind> {
        let mut state = self.state.lock();
        if op == IoOp::Read && state.die_read {
            return Some(io::ErrorKind::Other);
        }
        if op == IoOp::Write && state.die_write {
            return Some(io::ErrorKind::Other);
        }
        if state.fail_next > 0 {
            state.fail_next -= 1;
            return Some(state.kind);
        }
        if let Some(remaining) = &mut state.fail_after {
            if *remaining == 0 {
                return Some(state.kind);
            }
            *remaining -= 1;
        }
        None
    }

    /// Sleeps the injected delay on an async op (never blocks the
    /// runtime thread).
    async fn maybe_delay_async(&self) {
        let delay = self.state.lock().delay;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
    }

    /// Sleeps the injected delay on a sync handle op (the seal pipeline
    /// runs these on the blocking pool, so a blocking sleep matches the
    /// context).
    fn maybe_delay_sync(&self) {
        let delay = self.state.lock().delay;
        if let Some(delay) = delay {
            std::thread::sleep(delay);
        }
    }
}

#[async_trait::async_trait]
impl<D: DiskIo> DiskIo for FaultyIo<D> {
    fn pool_id(&self) -> u32 {
        self.inner.pool_id()
    }

    fn observer(&self) -> &dyn IoObserving {
        self.inner.observer()
    }

    async fn read_raw(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.maybe_delay_async().await;
        if let Some(kind) = self.should_fail(IoOp::Read) {
            return Err(inject(kind));
        }
        self.inner.read_raw(path, buf, offset).await
    }

    async fn read_direct_raw(&self, path: &Path, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.maybe_delay_async().await;
        if let Some(kind) = self.should_fail(IoOp::Read) {
            return Err(inject(kind));
        }
        self.inner.read_direct_raw(path, buf, offset).await
    }

    async fn open_raw(&self, path: &Path) -> io::Result<tokio::fs::File> {
        self.maybe_delay_async().await;
        if let Some(kind) = self.should_fail(IoOp::Open) {
            return Err(inject(kind));
        }
        self.inner.open_raw(path).await
    }

    async fn write_raw(&self, path: &Path, buf: &[u8], offset: u64) -> io::Result<()> {
        self.maybe_delay_async().await;
        if let Some(kind) = self.should_fail(IoOp::Write) {
            return Err(inject(kind));
        }
        self.inner.write_raw(path, buf, offset).await
    }

    async fn fsync_raw(&self, path: &Path) -> io::Result<()> {
        self.maybe_delay_async().await;
        if let Some(kind) = self.should_fail(IoOp::Fsync) {
            return Err(inject(kind));
        }
        self.inner.fsync_raw(path).await
    }

    fn write_handle_raw(&self, file: &std::fs::File, buf: &[u8]) -> io::Result<()> {
        self.maybe_delay_sync();
        if let Some(kind) = self.should_fail(IoOp::Write) {
            return Err(inject(kind));
        }
        self.inner.write_handle_raw(file, buf)
    }

    fn fsync_handle_raw(&self, file: &std::fs::File) -> io::Result<()> {
        self.maybe_delay_sync();
        if let Some(kind) = self.should_fail(IoOp::Fsync) {
            return Err(inject(kind));
        }
        self.inner.fsync_handle_raw(file)
    }
}

/// Builds the injected error for a fault.
fn inject(kind: io::ErrorKind) -> io::Error {
    io::Error::new(kind, "faulty-io: injected failure")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::io::IoBackend;

    fn registered_observer(pool_id: u32) -> IoObserver {
        let observer = IoObserver::new();
        observer.register_pool(pool_id, None);
        observer
    }

    // -- IoOp / IoErrorKind --

    #[test]
    fn io_op_discriminants_are_contiguous() {
        assert_eq!(IoOp::Read.as_usize(), 0);
        assert_eq!(IoOp::Write.as_usize(), 1);
        assert_eq!(IoOp::Fsync.as_usize(), 2);
        assert_eq!(IoOp::Open.as_usize(), 3);
        assert_eq!(IO_OP_COUNT, 4);
    }

    #[test]
    fn io_error_kind_counts_match_variants() {
        // Every variant maps to its own slot (the pre-sized ring width).
        assert_eq!(IoErrorKind::NotFound.as_usize(), 0);
        assert_eq!(IoErrorKind::Other.as_usize(), IO_ERROR_KIND_COUNT - 1);
        assert_eq!(IO_ERROR_KIND_COUNT, 17);
    }

    #[test]
    fn io_error_kind_classifies_std_kinds() {
        assert_eq!(
            IoErrorKind::from_io_error(&io::Error::from(io::ErrorKind::NotFound)),
            IoErrorKind::NotFound
        );
        assert_eq!(
            IoErrorKind::from_io_error(&io::Error::from(io::ErrorKind::StorageFull)),
            IoErrorKind::StorageFull
        );
        assert_eq!(
            IoErrorKind::from_io_error(&io::Error::from(io::ErrorKind::Interrupted)),
            IoErrorKind::Other
        );
    }

    // -- IoObserver --

    #[test]
    fn observer_records_error_and_latency_per_pool() {
        let observer = registered_observer(0);
        observer.record_latency(0, IoOp::Read, Duration::from_micros(120));
        observer.record_error(0, IoErrorKind::TimedOut);

        assert_eq!(observer.io_error_count(0), 1);
        let signal = observer.snapshot(0).unwrap();
        assert_eq!(signal.errors, 1);
        // `record_latency` is the op counter; `record_error` only counts
        // errors (the DiskIo wrapper records BOTH for a failed op).
        assert_eq!(signal.ops, 1);
        assert!(signal.error_rate > 0.0);
    }

    #[test]
    fn observer_records_are_per_pool() {
        let observer = IoObserver::new();
        observer.register_pool(0, None);
        observer.register_pool(1, None);
        observer.record_error(0, IoErrorKind::Other);

        assert_eq!(observer.io_error_count(0), 1);
        assert_eq!(observer.io_error_count(1), 0);
        let signal = observer.snapshot(1).unwrap();
        assert_eq!(signal.errors, 0);
    }

    #[test]
    fn observer_unknown_pool_is_noop() {
        let observer = IoObserver::new();
        observer.record_error(7, IoErrorKind::Other);
        observer.record_latency(7, IoOp::Read, Duration::from_micros(1));
        assert_eq!(observer.io_error_count(7), 0);
        assert!(observer.snapshot(7).is_none());
    }

    #[test]
    fn observer_register_pool_is_idempotent() {
        let observer = registered_observer(0);
        observer.register_pool(0, None);
        observer.record_error(0, IoErrorKind::Other);
        // The first registration wins; the count still lands on pool 0.
        assert_eq!(observer.io_error_count(0), 1);
    }

    #[test]
    fn observer_snapshot_rotates_windows() {
        let observer = registered_observer(0);
        observer.record_latency(0, IoOp::Read, Duration::from_micros(1));
        observer.record_error(0, IoErrorKind::Other);

        let first = observer.snapshot(0).unwrap();
        assert_eq!(first.ops, 1);
        assert_eq!(first.errors, 1);

        // The window was consumed and the ring rotated.
        let second = observer.snapshot(0).unwrap();
        assert_eq!(second.ops, 0);
        assert_eq!(second.errors, 0);

        // New records land in the fresh window.
        observer.record_error(0, IoErrorKind::Other);
        let third = observer.snapshot(0).unwrap();
        assert_eq!(third.errors, 1);
    }

    #[test]
    fn observer_window_ring_tolerates_more_rotations_than_buckets() {
        let observer = registered_observer(0);
        for _ in 0..(WINDOW_BUCKETS * 3) {
            observer.record_error(0, IoErrorKind::Other);
            let _ = observer.snapshot(0).unwrap();
        }
        // Ring wraps without losing the current window.
        observer.record_error(0, IoErrorKind::Other);
        let signal = observer.snapshot(0).unwrap();
        assert_eq!(signal.errors, 1);
    }

    #[test]
    fn observer_latency_percentiles_are_reported() {
        let observer = registered_observer(0);
        for _ in 0..1000 {
            observer.record_latency(0, IoOp::Write, Duration::from_micros(100));
        }
        let signal = observer.snapshot(0).unwrap();
        let write = signal.latency_for(IoOp::Write);
        // 100 µs = 100_000 ns → histogram bucket 16 (covers 65536..131072
        // ns); the percentile reports the bucket's upper bound.
        let expected = Duration::from_nanos(1u64 << 16);
        assert_eq!(write.p50, Some(expected));
        assert_eq!(write.p99, Some(expected));
        assert_eq!(write.p999, Some(expected));
    }

    #[test]
    fn observer_empty_window_has_zero_error_rate() {
        let observer = registered_observer(0);
        let signal = observer.snapshot(0).unwrap();
        assert_eq!(signal.ops, 0);
        assert_eq!(signal.errors, 0);
        assert_eq!(signal.error_rate, 0.0);
        assert!(signal.latency_for(IoOp::Read).p50.is_none());
    }

    #[test]
    fn observer_smart_counters_roundtrip_through_snapshot() {
        let observer = registered_observer(0);
        observer.record_smart(
            0,
            SmartCounters {
                reallocated_sectors: Some(12),
                pending_sectors: Some(3),
                uncorrectable_ecc: Some(1),
                wear_level: Some(77),
            },
        );
        let signal = observer.snapshot(0).unwrap();
        assert_eq!(signal.smart.reallocated_sectors, Some(12));
        assert_eq!(signal.smart.pending_sectors, Some(3));
        assert_eq!(signal.smart.uncorrectable_ecc, Some(1));
        assert_eq!(signal.smart.wear_level, Some(77));
    }

    #[test]
    fn noop_observer_does_nothing() {
        let noop = NoopIoObserver;
        noop.record_error(0, IoErrorKind::Other);
        noop.record_latency(0, IoOp::Read, Duration::from_millis(1));
        assert!(noop.snapshot(0).is_none());
    }

    // -- ObservedIo --

    #[test]
    fn observed_io_records_write_and_fsync_errors_on_observer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.dat");
        let observer = Arc::new(registered_observer(0));
        let io = ObservedIo {
            pool_id: 0,
            backend: Arc::new(IoBackend::default()),
            observer: observer.clone() as Arc<dyn IoObserving>,
        };

        let file = std::fs::File::create(&path).unwrap();
        io.write_handle(&file, b"data").unwrap();
        io.fsync_handle(&file).unwrap();
        drop(file);

        let signal = observer.snapshot(0).unwrap();
        assert_eq!(signal.errors, 0);
        assert_eq!(signal.ops, 2);
        assert!(signal.latency_for(IoOp::Write).p50.is_some());
        assert!(signal.latency_for(IoOp::Fsync).p50.is_some());
    }

    #[test]
    fn observed_io_records_error_for_missing_open() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.dat");
        let observer = Arc::new(registered_observer(0));
        let io = ObservedIo {
            pool_id: 0,
            backend: Arc::new(IoBackend::default()),
            observer: observer.clone() as Arc<dyn IoObserving>,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(io.open(&missing));
        assert!(result.is_err());

        let signal = observer.snapshot(0).unwrap();
        assert_eq!(signal.errors, 1);
        assert_eq!(signal.ops, 1);
        assert_eq!(IoErrorKind::from_io_error(&result.err().unwrap()), IoErrorKind::NotFound);
    }

    #[test]
    fn observed_io_delegates_reads_to_backend() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("read.dat");
        std::fs::write(&path, b"hello").unwrap();
        let observer = Arc::new(registered_observer(0));
        let io = ObservedIo {
            pool_id: 0,
            backend: Arc::new(IoBackend::default()),
            observer: observer.clone() as Arc<dyn IoObserving>,
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut buf = vec![0u8; 5];
        let n = rt.block_on(io.read(&path, &mut buf, 0)).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
        let signal = observer.snapshot(0).unwrap();
        assert_eq!(signal.ops, 1);
        assert!(signal.latency_for(IoOp::Read).p50.is_some());
    }

    // -- FaultyIo --

    #[test]
    fn faulty_io_fail_next_fails_exactly_n_ops() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("faulty.dat");
        let observer = Arc::new(registered_observer(0));
        let inner = ObservedIo {
            pool_id: 0,
            backend: Arc::new(IoBackend::default()),
            observer: observer.clone() as Arc<dyn IoObserving>,
        };
        let faulty = FaultyIo::new(inner);
        faulty.fail_next(2, io::ErrorKind::Other);

        // First write fails (injected), second fails, third succeeds.
        let file = std::fs::File::create(&path).unwrap();
        assert!(faulty.write_handle(&file, b"a").is_err());
        assert!(faulty.write_handle(&file, b"b").is_err());
        faulty.write_handle(&file, b"c").unwrap();
        assert!(faulty.fsync_handle(&file).is_ok());
        drop(file);

        let signal = observer.snapshot(0).unwrap();
        assert_eq!(signal.errors, 2);
        assert_eq!(signal.ops, 4);
        assert_eq!(observer.io_error_count(0), 2);
    }

    #[test]
    fn faulty_io_fail_after_lets_calls_through_then_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("faulty2.dat");
        let observer = Arc::new(registered_observer(0));
        let inner = ObservedIo {
            pool_id: 0,
            backend: Arc::new(IoBackend::default()),
            observer: observer.clone() as Arc<dyn IoObserving>,
        };
        let faulty = FaultyIo::new(inner);
        faulty.fail_after(2, io::ErrorKind::StorageFull);

        let file = std::fs::File::create(&path).unwrap();
        faulty.write_handle(&file, b"a").unwrap();
        faulty.write_handle(&file, b"b").unwrap();
        assert!(faulty.write_handle(&file, b"c").is_err());
        assert!(faulty.write_handle(&file, b"d").is_err());
        drop(file);

        let signal = observer.snapshot(0).unwrap();
        assert_eq!(signal.errors, 2);
    }

    #[test]
    fn faulty_io_die_on_read_is_asymmetric() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("asym.dat");
        std::fs::write(&path, b"payload").unwrap();
        let observer = Arc::new(registered_observer(0));
        let inner = ObservedIo {
            pool_id: 0,
            backend: Arc::new(IoBackend::default()),
            observer: observer.clone() as Arc<dyn IoObserving>,
        };
        let faulty = FaultyIo::new(inner);
        faulty.die_on_read();

        // Open read-write so both write+fsync and read succeed on the fd.
        let file = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
        // Write + fsync pass through.
        faulty.write_handle(&file, b"x").unwrap();
        faulty.fsync_handle(&file).unwrap();
        drop(file);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut buf = vec![0u8; 8];
        // Read fails.
        assert!(rt.block_on(faulty.read(&path, &mut buf, 0)).is_err());

        let signal = observer.snapshot(0).unwrap();
        assert_eq!(signal.errors, 1);
        assert_eq!(signal.ops, 3);
    }

    #[test]
    fn faulty_io_die_on_write_is_asymmetric() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("asym-write.dat");
        let observer = Arc::new(registered_observer(0));
        let inner = ObservedIo {
            pool_id: 0,
            backend: Arc::new(IoBackend::default()),
            observer: observer.clone() as Arc<dyn IoObserving>,
        };
        let faulty = FaultyIo::new(inner);
        faulty.die_on_write();

        // Create the file first, then open read-write so both write+fsync
        // and read succeed on the fd.
        std::fs::write(&path, b"payload").unwrap();
        let file = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
        // Write fails (injected); fsync passes through.
        assert!(faulty.write_handle(&file, b"x").is_err());
        faulty.fsync_handle(&file).unwrap();
        drop(file);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut buf = vec![0u8; 8];
        // Read passes through.
        rt.block_on(faulty.read(&path, &mut buf, 0)).unwrap();

        let signal = observer.snapshot(0).unwrap();
        assert_eq!(signal.errors, 1);
        assert_eq!(signal.ops, 3);
    }

    #[test]
    fn faulty_io_delay_injects_latency() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("slow.dat");
        let inner = ObservedIo {
            pool_id: 0,
            backend: Arc::new(IoBackend::default()),
            observer: Arc::new(registered_observer(0)) as Arc<dyn IoObserving>,
        };
        let faulty = FaultyIo::new(inner);
        faulty.delay(Duration::from_millis(20));

        let start = Instant::now();
        let file = std::fs::File::create(&path).unwrap();
        faulty.write_handle(&file, b"slow").unwrap();
        drop(file);
        assert!(start.elapsed() >= Duration::from_millis(15), "delay must sleep");
    }

    #[test]
    fn faulty_io_clear_resets_injection() {
        let faulty = FaultyIo::new(IoBackend::TokioFs);
        faulty.fail_next(5, io::ErrorKind::Other);
        faulty.clear();
        assert_eq!(faulty.state.lock().fail_next, 0);
    }

    #[test]
    fn faulty_io_pool_id_and_observer_delegate_to_inner() {
        let observer = Arc::new(registered_observer(0));
        let inner = ObservedIo {
            pool_id: 3,
            backend: Arc::new(IoBackend::default()),
            observer: observer.clone() as Arc<dyn IoObserving>,
        };
        let faulty = FaultyIo::new(inner);
        assert_eq!(faulty.pool_id(), 3);
    }

    // -- histogram internals --

    #[test]
    fn bucket_index_maps_nanos_to_power_of_two() {
        assert_eq!(bucket_index(0), 0);
        assert_eq!(bucket_index(1), 0);
        assert_eq!(bucket_index(2), 1);
        assert_eq!(bucket_index(4), 2);
        assert_eq!(bucket_index(1_000_000), 19); // ~1 ms
        assert_eq!(bucket_index(u64::MAX), 63);
    }
}
