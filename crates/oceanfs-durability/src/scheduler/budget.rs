//! Two-tier durability I/O admission budget (ADR-0017 amendment 2026-09-06).
//!
//! Every heavy local durability I/O producer on a node belongs to exactly one
//! of two tiers:
//!
//! - **Tier-0 (`repair`)** — data-layer/repair operations (heal ops,
//!   re-replication pulls/writes, inbound hint apply). They are functionally
//!   the write path / placement restoration: the durability contract is in
//!   arrears until they finish.
//! - **Tier-1 (`housekeeping`)** — clock-driven scheduled cycles (GC, orphan
//!   reaper, scrub, AE).
//!
//! The invariant: **a Tier-0 acquisition is never gated behind Tier-1
//! activity** — the two tiers are separate semaphores. Within a tier,
//! [`tokio::sync::Semaphore`] admission is FIFO-fair, so no member can starve
//! another or hog the budget. Tier separation is admission-level only: there
//! is no device-level io-class arbitration (the old `apply_background_*`
//! helpers were removed — see ADR-0017 amendment).

use std::{sync::Arc, time::Instant};

use oceanfs_core::{Gauge, Histogram, HistogramConfig, LabelSet, MetricRegistrar};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The two admission tiers of the durability I/O budget.
///
/// Tier-0 ("repair") operations are never gated behind Tier-1
/// ("housekeeping") activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityTier {
    /// Data-layer / repair operations (heal, re-replication, inbound hint
    /// apply).
    Repair,
    /// Housekeeping cycles (GC, orphan reaper, scrub, AE).
    Housekeeping,
}

impl DurabilityTier {
    /// Prometheus label value (`"repair"` / `"housekeeping"`).
    pub fn label(self) -> &'static str {
        match self {
            DurabilityTier::Repair => "repair",
            DurabilityTier::Housekeeping => "housekeeping",
        }
    }
}

/// The two-tier admission budget shared by every durability I/O producer.
///
/// Constructed once in the durability builder (f4) and shared between the
/// [`DurabilityScheduler`](crate::scheduler::DurabilityScheduler) (which
/// acquires a Tier-1 permit per scheduled cycle) and the Tier-0 workers
/// (heal, re-replication, inbound hint apply — one Tier-0 permit per
/// operation).
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_durability::scheduler::DurabilityBudget;
///
/// let budget = Arc::new(DurabilityBudget::new(16, 2));
/// let rt = tokio::runtime::Builder::new_current_thread()
///     .enable_all()
///     .build()
///     .expect("runtime");
/// rt.block_on(async {
///     let _repair = budget.acquire_repair().await;
///     let _housekeeping = budget.acquire_housekeeping().await;
/// });
/// ```
#[derive(Debug)]
pub struct DurabilityBudget {
    repair: Arc<Semaphore>,
    housekeeping: Arc<Semaphore>,
    repair_active: Gauge,
    housekeeping_active: Gauge,
    repair_waiters: Gauge,
    housekeeping_waiters: Gauge,
    repair_wait_millis: Arc<Histogram>,
    housekeeping_wait_millis: Arc<Histogram>,
}

impl DurabilityBudget {
    /// Creates the two-tier budget.
    ///
    /// Both budgets must be at least 1. `repair_max_active` bounds
    /// concurrent Tier-0 operations node-wide (the single gate replacing
    /// heal/re-rep/per-RPC hint semaphores); `housekeeping_max_active`
    /// bounds concurrent Tier-1 scheduled cycles.
    ///
    /// # Panics
    ///
    /// Panics if either budget is zero.
    pub fn new(repair_max_active: usize, housekeeping_max_active: usize) -> Self {
        assert!(repair_max_active >= 1, "repair_max_active must be >= 1");
        assert!(housekeeping_max_active >= 1, "housekeeping_max_active must be >= 1");
        let wait_buckets = HistogramConfig {
            buckets: vec![1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000],
        };
        Self {
            repair: Arc::new(Semaphore::new(repair_max_active)),
            housekeeping: Arc::new(Semaphore::new(housekeeping_max_active)),
            repair_active: Gauge::new(
                "durability_repair_active".into(),
                "Currently active Tier-0 (repair) operations".into(),
                LabelSet::empty(),
            ),
            housekeeping_active: Gauge::new(
                "durability_housekeeping_active".into(),
                "Currently active Tier-1 (housekeeping) cycles".into(),
                LabelSet::empty(),
            ),
            repair_waiters: Gauge::new(
                "durability_repair_waiters".into(),
                "Tasks waiting for a Tier-0 (repair) permit".into(),
                LabelSet::empty(),
            ),
            housekeeping_waiters: Gauge::new(
                "durability_housekeeping_waiters".into(),
                "Tasks waiting for a Tier-1 (housekeeping) permit".into(),
                LabelSet::empty(),
            ),
            repair_wait_millis: Arc::new(Histogram::new(
                "durability_repair_wait_duration_millis".into(),
                "Tier-0 (repair) permit wait duration in milliseconds".into(),
                &wait_buckets,
                LabelSet::empty(),
            )),
            housekeeping_wait_millis: Arc::new(Histogram::new(
                "durability_housekeeping_wait_duration_millis".into(),
                "Tier-1 (housekeeping) permit wait duration in milliseconds".into(),
                &wait_buckets,
                LabelSet::empty(),
            )),
        }
    }

    /// Acquires a Tier-0 (repair) permit. Waits only on Tier-0 activity —
    /// never on Tier-1 (housekeeping) activity.
    pub async fn acquire_repair(&self) -> DurabilityPermit {
        self.acquire(DurabilityTier::Repair).await
    }

    /// Acquires a Tier-1 (housekeeping) permit.
    pub async fn acquire_housekeeping(&self) -> DurabilityPermit {
        self.acquire(DurabilityTier::Housekeeping).await
    }

    async fn acquire(&self, tier: DurabilityTier) -> DurabilityPermit {
        let (semaphore, waiters, active, wait_millis) = match tier {
            DurabilityTier::Repair => (
                Arc::clone(&self.repair),
                &self.repair_waiters,
                &self.repair_active,
                &self.repair_wait_millis,
            ),
            DurabilityTier::Housekeeping => (
                Arc::clone(&self.housekeeping),
                &self.housekeeping_waiters,
                &self.housekeeping_active,
                &self.housekeeping_wait_millis,
            ),
        };

        // The waiter gauge is decremented when `waiters_guard` drops — on
        // successful acquisition AND on cancellation mid-wait (future
        // dropped).
        let waiters_guard = WaitersGuard::inc(waiters.clone());
        let started = Instant::now();
        let permit = semaphore.acquire_owned().await.ok();
        wait_millis.observe(started.elapsed().as_millis() as u64);
        drop(waiters_guard);
        active.inc();

        DurabilityPermit { tier, _permit: permit, active: active.clone() }
    }

    /// Registers the budget metrics with `registrar`.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_gauge(self.repair_active.clone());
        registrar.register_gauge(self.housekeeping_active.clone());
        registrar.register_gauge(self.repair_waiters.clone());
        registrar.register_gauge(self.housekeeping_waiters.clone());
        registrar.register_histogram(Arc::clone(&self.repair_wait_millis));
        registrar.register_histogram(Arc::clone(&self.housekeeping_wait_millis));
    }
}

/// Cancellation-safe waiter accounting: increments the waiters gauge at
/// construction and decrements it on drop (successful acquisition or
/// cancellation mid-wait).
struct WaitersGuard {
    gauge: Gauge,
}

impl WaitersGuard {
    /// Increments the gauge immediately and returns a guard that decrements
    /// it when dropped.
    fn inc(gauge: Gauge) -> Self {
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for WaitersGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

/// RAII guard for a budget permit.
///
/// Releases the semaphore permit and decrements the active gauge for its
/// tier when dropped.
#[derive(Debug)]
pub struct DurabilityPermit {
    tier: DurabilityTier,
    _permit: Option<OwnedSemaphorePermit>,
    active: Gauge,
}

impl DurabilityPermit {
    /// The tier this permit was acquired from.
    pub fn tier(&self) -> DurabilityTier {
        self.tier
    }
}

impl Drop for DurabilityPermit {
    fn drop(&mut self) {
        // `_permit` is dropped first (releasing the semaphore slot), then
        // the active gauge is decremented.
        self._permit.take();
        self.active.dec();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::scheduler::budget::DurabilityTier;

    /// The two-tier invariant: a Tier-0 acquisition starts even while a
    /// Tier-1 permit is held (separate budgets — Tier-0 is never gated
    /// behind Tier-1 activity).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tier0_is_never_blocked_by_tier1() {
        let budget = Arc::new(DurabilityBudget::new(1, 1));

        let held_housekeeping = budget.acquire_housekeeping().await;

        // Tier-1 is exhausted; a Tier-0 acquisition must still succeed.
        let repair =
            tokio::time::timeout(std::time::Duration::from_millis(500), budget.acquire_repair())
                .await;
        assert!(repair.is_ok(), "Tier-0 acquisition blocked behind Tier-1");
        drop(held_housekeeping);
        drop(repair.unwrap());
    }

    /// A Tier-0 op waits only on other Tier-0 ops (budget respected), but
    /// never behind Tier-1.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tier0_respects_its_own_budget() {
        let budget = Arc::new(DurabilityBudget::new(1, 2));

        let _first_repair = budget.acquire_repair().await;
        // Second Tier-0 acquisition must wait (budget = 1) even though
        // Tier-1 has free capacity.
        let second = budget.acquire_repair();
        let second = tokio::time::timeout(std::time::Duration::from_millis(100), second).await;
        assert!(second.is_err(), "Tier-0 budget of 1 not enforced");
    }

    /// Fairness within a tier: a second claimant is admitted only when the
    /// first releases (admission blocks while capacity is held), and once
    /// capacity frees the waiting claimant acquires — no starvation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn housekeeping_admission_is_fair() {
        let budget = Arc::new(DurabilityBudget::new(1, 1));

        let first_permit = budget.acquire_housekeeping().await;

        // The only Tier-1 permit is held: a second acquisition must wait.
        let second = budget.acquire_housekeeping();
        let second_blocked =
            tokio::time::timeout(std::time::Duration::from_millis(80), second).await;
        assert!(
            second_blocked.is_err(),
            "second claimant acquired while the first held the permit"
        );

        drop(first_permit);
        // The waiting claimant is admitted as soon as capacity frees.
        let second_permit = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            budget.acquire_housekeeping(),
        )
        .await
        .expect("second claimant must acquire after the first releases");
        assert_eq!(second_permit.tier(), DurabilityTier::Housekeeping);
        assert_eq!(budget.housekeeping_active.get(), 1);
    }

    /// Active gauges track held permits and return to zero on release.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn active_gauges_track_held_permits() {
        let budget = Arc::new(DurabilityBudget::new(4, 4));
        let p1 = budget.acquire_repair().await;
        let p2 = budget.acquire_housekeeping().await;
        assert_eq!(budget.repair_active.get(), 1);
        assert_eq!(budget.housekeeping_active.get(), 1);
        drop(p1);
        drop(p2);
        assert_eq!(budget.repair_active.get(), 0);
        assert_eq!(budget.housekeeping_active.get(), 0);
    }
}
