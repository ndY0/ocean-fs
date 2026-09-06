//! `DurabilityTask` trait + keyspace window (ADR-0017, f1).
//!
//! The trait is implemented by the four Tier-1 (housekeeping) interval tasks
//! (GC, orphan reaper, scrub, AE) and driven by the
//! [`DurabilityScheduler`](crate::scheduler::DurabilityScheduler). Heal,
//! re-replication, reconciliation, and hint delivery do NOT implement this
//! trait — they are queue/event-driven and participate in the two-tier
//! budget as Tier-0 clients (see the epic README scope table).

use std::time::Duration;

use async_trait::async_trait;

use crate::Result;

/// The window of a task's keyspace a single cycle should process.
///
/// # Examples
///
/// ```
/// use oceanfs_durability::scheduler::KeyspaceWindow;
///
/// let window = KeyspaceWindow::Full;
/// match window {
///     KeyspaceWindow::Full => {}
///     KeyspaceWindow::Shard { index, total } => {
///         let _ = (index, total);
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyspaceWindow {
    /// Process everything (used when `keyspace_fraction() == 1.0`).
    Full,
    /// Process shard `index` of `total` (round-robin rotation; only
    /// shard-aware tasks receive this — see f3).
    Shard {
        /// Zero-based shard index for this cycle.
        index: u64,
        /// Total number of shards (`(1.0 / keyspace_fraction).round()`).
        total: u64,
    },
}

/// A Tier-1 (housekeeping) background maintenance task scheduled by the
/// [`DurabilityScheduler`](crate::scheduler::DurabilityScheduler).
///
/// The scheduler owns the interval loop and, before each cycle, acquires a
/// Tier-1 permit from the shared
/// [`DurabilityBudget`](crate::scheduler::DurabilityBudget). Implementations
/// only describe their cadence and run one cycle.
#[async_trait]
pub trait DurabilityTask: Send + Sync {
    /// Human-readable name for logging and metrics labels (`"gc"`,
    /// `"orphan_reaper"`, `"scrub"`, `"anti_entropy"`).
    fn name(&self) -> &'static str;

    /// Interval between consecutive cycles. Read from the same `NodeConfig`
    /// fields the node's spawn loops use today (`gc_interval_sec`, etc.) and
    /// captured at adaptor construction — intervals do NOT move.
    fn interval(&self) -> Duration;

    /// Fraction of the keyspace to process per cycle (0.0, 1.0].
    /// Default 1.0 = full pass. Tasks that cannot shard return 1.0.
    fn keyspace_fraction(&self) -> f64 {
        1.0
    }

    /// Whether a new cycle may start while a previous one is still running.
    /// Default `false` (serial per task).
    fn concurrent_cycles(&self) -> bool {
        false
    }

    /// Runs one cycle over `window`. Returns the number of items processed
    /// (segments scanned / compared) or an error. Errors are logged and
    /// counted by the scheduler but do not stop it.
    async fn run_cycle(&self, window: KeyspaceWindow) -> Result<u64>;
}
