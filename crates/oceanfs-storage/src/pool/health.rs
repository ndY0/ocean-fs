//! Health-signal trend detection (ADR-0029 §D3 signal processing).
//!
//! This module turns the [`IoObserver`](crate::io::IoObserver)'s
//! per-window signals into a trend verdict. The health monitor (g2)
//! accumulates one [`PoolSignal`] per detection window into a history
//! slice and calls [`evaluate_trend`]:
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
//! Pure logic — no I/O, no clocks, no locks. Unit-testable directly.

use std::time::Duration;

use oceanfs_core::PoolTech;

use crate::io::disk_io::{IoOp, IO_OP_COUNT};

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
}
