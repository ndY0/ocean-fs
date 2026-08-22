//! Storage pool placement policy (ADR-0029 §D1/D8).
//!
//! Picks the pool a new segment is written to: role-aware (only `data`
//! pools are eligible), weight-aware and capacity-aware in one monotone
//! score — `free_bytes / weight` (weight as resolved by f2, min 1). The
//! pool with the **maximum** score wins, ties break by smaller pool id,
//! and pools below the free-space headroom are excluded.
//!
//! Pure logic over a [`PoolRegistry`] snapshot: no I/O, no wiring to the
//! sealer (that is f5).
//!
//! ## Selection rule note (f3 deviation)
//!
//! The feature doc's *weighted* example ("A w1/free 10 GiB vs B w2/free
//! 10 GiB → B wins (5 vs 10)") implies minimum `free/weight`, but the
//! Scope rule text and the Interface both specify **maximum**
//! `free/weight`, and the *capacity* example (A w1/free 10 GiB vs B
//! w1/free 20 GiB → B wins) requires it too. Maximum `free/weight` is the
//! standard weighted water-filling rule: with capacities proportional to
//! weights it keeps every pool at the same fill fraction, so a
//! `weight = 2` pool attracts ~2× the data of a `weight = 1` pool. The
//! weighted example's winner is treated as a doc error (see
//! `weighted_selection_prefers_pool_with_more_free_per_weight`).

use std::sync::Arc;

use oceanfs_core::PoolRole;

use super::{PoolRegistry, PoolStatus, StoragePool};

/// Pools with less free space than this are excluded from placement:
/// writing into a nearly-full pool risks immediate ENOSPC on the first
/// segment. 64 MiB.
const MIN_FREE_HEADROOM_BYTES: u64 = 64 * 1024 * 1024;

/// Stateless placement policy: pick the data pool a new segment lands on.
///
/// Phase A has no operator-facing knobs (the brainstorm's `weight_bias`
/// blend parameter is dropped — `max free/weight` needs no tuning; see the
/// f3 feature doc's accepted deviations).
///
/// # Examples
///
/// ```
/// use oceanfs_storage::{PlacementPolicy, PoolRegistry};
///
/// # let tmp = tempfile::tempdir().expect("tempdir");
/// # let data_dir = tmp.path().join("data");
/// let registry = PoolRegistry::from_config(
///     &oceanfs_core::StorageConfig::default(),
///     &data_dir,
/// )
/// .expect("legacy registry");
///
/// let policy = PlacementPolicy::new();
/// let pool = policy.select_data_pool(&registry).expect("implicit data pool");
/// assert_eq!(pool.role(), oceanfs_core::PoolRole::Data);
/// ```
pub struct PlacementPolicy;

impl PlacementPolicy {
    /// Creates a stateless placement policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::PlacementPolicy;
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// let policy = PlacementPolicy::new();
    /// let registry = oceanfs_storage::PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    /// assert!(policy.select_pinned_pool(&registry, PoolRole::Data).is_some());
    /// ```
    pub fn new() -> Self {
        PlacementPolicy
    }

    /// Selects the data pool a new segment is written to.
    ///
    /// Eligible pools: role `data`, status `Healthy`, `write_degraded ==
    /// false`, `free_bytes > MIN_FREE_HEADROOM_BYTES`. Among them, the pool
    /// with the maximum `free_bytes / weight` wins (weight min 1); equal
    /// scores break by smaller pool id. Returns `None` when no pool is
    /// eligible (f5 decides the fallback).
    ///
    /// Perf notes: one registry snapshot read (cloned `Arc`s — no lock held
    /// across scoring), a pre-sized candidate vec, and pure integer score
    /// math — no string work (guidelines 1.3, 7.1, 9.3).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::{PlacementPolicy, PoolRegistry};
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    ///
    /// let policy = PlacementPolicy::new();
    /// assert!(policy.select_data_pool(&registry).is_some());
    /// ```
    pub fn select_data_pool(&self, registry: &PoolRegistry) -> Option<Arc<StoragePool>> {
        // Single snapshot read of the registry (perf 7.1): `data_pools`
        // clones the Arcs under one short read lock; scoring runs outside
        // any lock.
        self.select_from_pools(&registry.data_pools())
    }

    /// Selects the target pool from an explicit pool slice (f5: the
    /// sealer holds a snapshot of the node's data pools and selects once
    /// per new segment without touching the registry).
    ///
    /// Same eligibility and scoring as [`PlacementPolicy::select_data_pool`],
    /// over a caller-provided pool list: role/status/headroom filtering is
    /// skipped for pools the caller already filtered (the sealer's
    /// `data_pools` are all `Data`-role); the weighted-least-free score
    /// (`max free / weight`, ties → lower id) and the 64 MiB headroom
    /// exclusion apply.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::{PlacementPolicy, PoolRegistry};
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    ///
    /// let policy = PlacementPolicy::new();
    /// let pools = registry.data_pools();
    /// assert!(policy.select_from_pools(&pools).is_some());
    /// ```
    pub fn select_from_pools(&self, pools: &[Arc<StoragePool>]) -> Option<Arc<StoragePool>> {
        // Pre-size to the pool count (perf 1.3).
        let mut eligible: Vec<Arc<StoragePool>> = Vec::with_capacity(pools.len());
        for pool in pools {
            if pool.status() == PoolStatus::Healthy
                && !pool.write_degraded()
                && pool.free_bytes() > MIN_FREE_HEADROOM_BYTES
            {
                eligible.push(Arc::clone(pool));
            }
        }

        // Weighted least-free: max `free / weight`, ties by smaller pool id
        // (perf 9.3: integer math only).
        let mut best: Option<(u64, Arc<StoragePool>)> = None;
        for pool in eligible {
            let score = pool.free_bytes() / u64::from(pool.weight().max(1));
            let replace = match &best {
                None => true,
                Some((best_score, best_pool)) => {
                    score > *best_score || (score == *best_score && pool.id() < best_pool.id())
                }
            };
            if replace {
                best = Some((score, pool));
            }
        }
        best.map(|(_, pool)| pool)
    }

    /// Returns the cardinality-1 pool of a pinned role (`wal`, `metadata`,
    /// `hints`) when it is `Healthy`, else `None`.
    ///
    /// f4 uses this to resolve each pinned path (metadata store root, WAL
    /// root, hint WAL root). For the `data` role it returns the first
    /// healthy data pool (prefer [`PlacementPolicy::select_data_pool`] for
    /// segment placement).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::{PlacementPolicy, PoolRegistry};
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    ///
    /// let policy = PlacementPolicy::new();
    /// // Legacy registry has no wal pool configured.
    /// assert!(policy.select_pinned_pool(&registry, PoolRole::Wal).is_none());
    /// ```
    pub fn select_pinned_pool(
        &self,
        registry: &PoolRegistry,
        role: PoolRole,
    ) -> Option<Arc<StoragePool>> {
        registry.pool_by_role(role).filter(|pool| pool.status() == PoolStatus::Healthy)
    }
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        PlacementPolicy::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::pool::{PoolCapacity, PoolStatus};

    /// One GiB as a u64 literal for readable test expectations.
    const GIB: u64 = 1024 * 1024 * 1024;

    /// Builds a registry from a pool config whose roots are siblings under
    /// a tempdir (the f1 disjointness rule holds), then overrides the
    /// real-statvfs capacities with the requested ones.
    ///
    /// `capacities[i]` is the (total, free) snapshot for pool `i`.
    fn registry_with_capacities(
        pools: &[(&str, PoolRole, u32)],
        capacities: &[(u64, u64)],
    ) -> (tempfile::TempDir, PoolRegistry) {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let storage = oceanfs_core::StorageConfig {
            pools: pools
                .iter()
                .enumerate()
                .map(|(index, (name, role, weight))| oceanfs_core::StoragePoolConfig {
                    name: name.to_string(),
                    role: *role,
                    root: tmp.path().join(format!("pool-{index}")),
                    weight: Some(*weight),
                    tech: oceanfs_core::PoolTech::Auto,
                    health: Default::default(),
                })
                .collect(),
            missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        for (index, &(total, free)) in capacities.iter().enumerate() {
            registry
                .pool_by_id(index as u32)
                .expect("pool")
                .set_capacity(PoolCapacity { total_bytes: total, free_bytes: free });
        }
        (tmp, registry)
    }

    #[test]
    fn selects_the_pool_with_more_free_space_at_equal_weight() {
        let (_tmp, registry) = registry_with_capacities(
            &[("pool-a", PoolRole::Data, 1), ("pool-b", PoolRole::Data, 1)],
            &[(100 * GIB, 10 * GIB), (100 * GIB, 20 * GIB)],
        );
        let policy = PlacementPolicy::new();
        let pool = policy.select_data_pool(&registry).unwrap();
        assert_eq!(pool.id(), 1, "pool-b has the most free space");
    }

    #[test]
    fn weighted_selection_prefers_pool_with_more_free_per_weight() {
        // Both pools have 10 GiB free; pool-b has weight 2. Score_a =
        // 10 GiB/1 = 10 GiB, score_b = 10 GiB/2 = 5 GiB → max rule picks
        // pool-a. (The feature doc's example says "B wins" — treated as a
        // doc error, see the module docs.)
        let (_tmp, registry) = registry_with_capacities(
            &[("pool-a", PoolRole::Data, 1), ("pool-b", PoolRole::Data, 2)],
            &[(100 * GIB, 10 * GIB), (100 * GIB, 10 * GIB)],
        );
        let policy = PlacementPolicy::new();
        let pool = policy.select_data_pool(&registry).unwrap();
        assert_eq!(pool.id(), 0, "max free/weight: 10 GiB > 5 GiB");
    }

    #[test]
    fn weight_two_pool_wins_when_free_space_doubles() {
        // pool-b (weight 2) with 20 GiB free scores 10 GiB/weight — tied
        // with pool-a's 10 GiB; the tie breaks to the lower id. Give
        // pool-b 21 GiB so it strictly wins: capacity × weight balance.
        let (_tmp, registry) = registry_with_capacities(
            &[("pool-a", PoolRole::Data, 1), ("pool-b", PoolRole::Data, 2)],
            &[(100 * GIB, 10 * GIB), (200 * GIB, 21 * GIB)],
        );
        let policy = PlacementPolicy::new();
        let pool = policy.select_data_pool(&registry).unwrap();
        assert_eq!(pool.id(), 1, "weight-2 pool with proportional free wins");
    }

    #[test]
    fn after_filling_winner_the_other_pool_wins() {
        // A w1/f10GiB vs B w1/f20GiB → B wins; simulate sealing 15 GiB into
        // B (free 5 GiB) → A wins.
        let (_tmp, registry) = registry_with_capacities(
            &[("pool-a", PoolRole::Data, 1), ("pool-b", PoolRole::Data, 1)],
            &[(100 * GIB, 10 * GIB), (100 * GIB, 20 * GIB)],
        );
        let policy = PlacementPolicy::new();
        assert_eq!(policy.select_data_pool(&registry).unwrap().id(), 1);

        registry
            .pool_by_id(1)
            .unwrap()
            .set_capacity(PoolCapacity { total_bytes: 100 * GIB, free_bytes: 5 * GIB });
        assert_eq!(policy.select_data_pool(&registry).unwrap().id(), 0);
    }

    #[test]
    fn only_data_pools_are_eligible() {
        // Non-data pools with enormous free space must never be selected.
        let (_tmp, registry) = registry_with_capacities(
            &[
                ("data-a", PoolRole::Data, 1),
                ("data-b", PoolRole::Data, 1),
                ("journal", PoolRole::Wal, 1),
                ("meta", PoolRole::Metadata, 1),
                ("hints", PoolRole::Hints, 1),
            ],
            &[
                (100 * GIB, 1 * GIB),
                (100 * GIB, 1 * GIB),
                (1_000 * GIB, 900 * GIB), // wal: huge free, never eligible
                (1_000 * GIB, 900 * GIB), // metadata: huge free, never eligible
                (1_000 * GIB, 900 * GIB), // hints: huge free, never eligible
            ],
        );
        let policy = PlacementPolicy::new();
        for _ in 0..10 {
            let pool = policy.select_data_pool(&registry).unwrap();
            assert_eq!(pool.role(), PoolRole::Data, "non-data pool selected");
        }
    }

    #[test]
    fn degraded_and_write_degraded_pools_are_excluded() {
        let (_tmp, registry) = registry_with_capacities(
            &[("pool-a", PoolRole::Data, 1), ("pool-b", PoolRole::Data, 1)],
            &[(100 * GIB, 20 * GIB), (100 * GIB, 20 * GIB)],
        );
        let policy = PlacementPolicy::new();

        // Exclude pool-b by status; pool-a must win.
        registry.set_status(1, PoolStatus::Degraded);
        assert_eq!(policy.select_data_pool(&registry).unwrap().id(), 0);

        // Exclude pool-a by write_degraded; now nothing is eligible.
        registry.set_write_degraded(0, true);
        assert!(policy.select_data_pool(&registry).is_none());
    }

    #[test]
    fn pools_below_min_free_headroom_are_excluded() {
        // The only pool has 32 MiB free — below the 64 MiB headroom — so
        // even with the best (only) score it must not be selected.
        let (_tmp, registry) = registry_with_capacities(
            &[("pool-a", PoolRole::Data, 1)],
            &[(100 * GIB, 32 * 1024 * 1024)],
        );
        let policy = PlacementPolicy::new();
        assert!(policy.select_data_pool(&registry).is_none());
    }

    #[test]
    fn empty_or_all_excluded_registry_returns_none() {
        // All pools below headroom.
        let (_tmp, registry) = registry_with_capacities(
            &[("pool-a", PoolRole::Data, 1), ("pool-b", PoolRole::Data, 1)],
            &[(100 * GIB, 1024), (100 * GIB, 1024)],
        );
        let policy = PlacementPolicy::new();
        assert!(policy.select_data_pool(&registry).is_none());
    }

    #[test]
    fn selection_is_deterministic_and_tie_breaks_by_lower_id() {
        // Identical scores: tie must consistently break to the lower id.
        let (_tmp, registry) = registry_with_capacities(
            &[("pool-a", PoolRole::Data, 2), ("pool-b", PoolRole::Data, 2)],
            &[(100 * GIB, 10 * GIB), (100 * GIB, 10 * GIB)],
        );
        let policy = PlacementPolicy::new();
        for _ in 0..10 {
            assert_eq!(policy.select_data_pool(&registry).unwrap().id(), 0);
        }
    }

    #[test]
    fn pinned_pool_returns_healthy_cardinality_pool() {
        let (_tmp, registry) = registry_with_capacities(
            &[
                ("data-a", PoolRole::Data, 1),
                ("journal", PoolRole::Wal, 1),
                ("meta", PoolRole::Metadata, 1),
            ],
            &[(100 * GIB, 10 * GIB), (100 * GIB, 10 * GIB), (100 * GIB, 10 * GIB)],
        );
        let policy = PlacementPolicy::new();

        assert_eq!(policy.select_pinned_pool(&registry, PoolRole::Wal).unwrap().id(), 1);
        assert_eq!(policy.select_pinned_pool(&registry, PoolRole::Metadata).unwrap().id(), 2);
        // Hints not configured → None.
        assert!(policy.select_pinned_pool(&registry, PoolRole::Hints).is_none());

        // Degraded pinned pool → None.
        registry.set_status(1, PoolStatus::Degraded);
        assert!(policy.select_pinned_pool(&registry, PoolRole::Wal).is_none());
    }

    #[test]
    fn pinned_pool_for_data_role_returns_first_healthy_data_pool() {
        let (_tmp, registry) = registry_with_capacities(
            &[("data-a", PoolRole::Data, 1), ("data-b", PoolRole::Data, 1)],
            &[(100 * GIB, 10 * GIB), (100 * GIB, 10 * GIB)],
        );
        let policy = PlacementPolicy::new();
        assert_eq!(policy.select_pinned_pool(&registry, PoolRole::Data).unwrap().id(), 0);
    }
}
