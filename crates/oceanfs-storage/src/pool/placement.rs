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
/// # let storage = oceanfs_core::StorageConfig {
/// #     pools: vec![
/// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: Some(1), tech: Default::default(), health: Default::default() },
/// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
/// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
/// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
/// #     ],
/// #     health: Default::default(),
/// #     missing_root_policy: Default::default(),
/// # };
/// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
///
/// let policy = PlacementPolicy::new();
/// let pool = policy.select_data_pool(&registry).expect("a data pool");
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
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let policy = PlacementPolicy::new();
    /// let registry =
    ///     oceanfs_storage::PoolRegistry::from_config(&storage, &data_dir).expect("registry");
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
    /// A `Draining` data pool (ADR-0036 D6) is **excluded with no extra
    /// rule**: `Draining != Healthy`, so the moment `begin_drain` flips a
    /// pool's status it stops receiving new segment targets (seal-time
    /// reservation goes through this same policy). A pool that turns
    /// `Draining` mid-run stops receiving selections immediately — the
    /// policy reads the live registry per selection.
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
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
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
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
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

        best_weighted_least_free(&eligible)
    }

    /// Selects the data pool a sealed segment is relocated onto during an
    /// intra-node drain (d3, ADR-0036 D5) — the sibling-pool mover's target
    /// pick.
    ///
    /// Same weighted-least-free rule and tie-break as
    /// [`PlacementPolicy::select_from_pools`], over the registry's *data*
    /// pools, with two differences:
    /// - pools whose id appears in `exclude` are skipped (the caller passes
    ///   the source pool and any other pool it must not write to); a
    ///   `Draining` pool is additionally excluded by the `Healthy`-only
    ///   filter, so the exclude list is belt-and-braces on top of the
    ///   status rule (ADR-0036 D6);
    /// - the free-space bound is `free_bytes >= required_free +
    ///   MIN_FREE_HEADROOM_BYTES`, i.e. the segment being moved must fit and
    ///   leave the usual headroom behind. `required_free` is the registry
    ///   entry's `total_bytes` (the relocation copy is whole-file).
    ///
    /// Returns `None` when no data pool has room — the drain worker parks
    /// the source pool with a blocked reason (no-destructive-failure rule);
    /// nothing is deleted and a later capacity change makes the pool
    /// eligible again.
    ///
    /// Perf notes: one registry snapshot read (cloned `Arc`s), a
    /// pre-sized candidate vec, and pure integer score math (guidelines
    /// 1.3, 7.1, 9.3).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::{PlacementPolicy, PoolRegistry};
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data-0"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "data-1".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data-1"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
    /// let policy = PlacementPolicy::new();
    ///
    /// // Exclude data pool 0 (the draining source); data pool 1 is picked.
    /// let target = policy
    ///     .select_data_pool_with_headroom(&registry, &[0], 4 * 1024 * 1024)
    ///     .expect("a sibling data pool");
    /// assert_eq!(target.role(), oceanfs_core::PoolRole::Data);
    /// ```
    pub fn select_data_pool_with_headroom(
        &self,
        registry: &PoolRegistry,
        exclude: &[u32],
        required_free: u64,
    ) -> Option<Arc<StoragePool>> {
        // One snapshot read of the registry (perf 7.1): `data_pools`
        // clones the Arcs under one short read lock; scoring runs outside
        // any lock.
        let pools = registry.data_pools();
        let required = required_free.saturating_add(MIN_FREE_HEADROOM_BYTES);
        let mut eligible: Vec<Arc<StoragePool>> = Vec::with_capacity(pools.len());
        for pool in pools {
            if exclude.contains(&pool.id()) {
                continue;
            }
            if pool.status() == PoolStatus::Healthy
                && !pool.write_degraded()
                && pool.free_bytes() >= required
            {
                eligible.push(pool);
            }
        }
        best_weighted_least_free(&eligible)
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
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
    ///
    /// let policy = PlacementPolicy::new();
    /// // The registry pins a wal pool (ADR-0031 role pinning).
    /// assert!(policy.select_pinned_pool(&registry, PoolRole::Wal).is_some());
    /// ```
    pub fn select_pinned_pool(
        &self,
        registry: &PoolRegistry,
        role: PoolRole,
    ) -> Option<Arc<StoragePool>> {
        registry.pool_by_role(role).filter(|pool| pool.status() == PoolStatus::Healthy)
    }
}

/// Weighted least-free pick shared by every selection method: maximum
/// `free / weight` wins (weight min 1), ties break by smaller pool id.
/// Operates on a caller-filtered candidate list (perf 9.3: integer math
/// only, no strings).
fn best_weighted_least_free(pools: &[Arc<StoragePool>]) -> Option<Arc<StoragePool>> {
    let mut best: Option<(u64, Arc<StoragePool>)> = None;
    for pool in pools {
        let score = pool.free_bytes() / u64::from(pool.weight().max(1));
        let replace = match &best {
            None => true,
            Some((best_score, best_pool)) => {
                score > *best_score || (score == *best_score && pool.id() < best_pool.id())
            }
        };
        if replace {
            best = Some((score, Arc::clone(pool)));
        }
    }
    best.map(|(_, pool)| pool)
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
        // ADR-0031: pools are mandatory — append any pinned role the
        // caller's list lacks (placement only ever selects `data` pools,
        // so the appended roles do not alter the scenarios under test).
        let mut pools = pools.to_vec();
        for (name, role) in
            [("journal", PoolRole::Wal), ("meta", PoolRole::Metadata), ("hints", PoolRole::Hints)]
        {
            if !pools.iter().any(|(_, r, _)| *r == role) {
                pools.push((name, role, 1));
            }
        }
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
            health: Default::default(),
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
                (100 * GIB, GIB),
                (100 * GIB, GIB),
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

    /// d1 (ADR-0036 D6): a `Draining` data pool is excluded immediately —
    /// no policy change was needed (`Draining != Healthy`); this pins it.
    #[test]
    fn draining_pools_are_excluded() {
        let (_tmp, registry) = registry_with_capacities(
            &[("pool-a", PoolRole::Data, 1), ("pool-b", PoolRole::Data, 1)],
            &[(100 * GIB, 20 * GIB), (100 * GIB, 20 * GIB)],
        );
        let policy = PlacementPolicy::new();

        // begin_drain flips the pool's status to Draining: the healthy
        // sibling must win from then on.
        registry.begin_drain(1).unwrap();
        assert_eq!(
            policy.select_data_pool(&registry).unwrap().id(),
            0,
            "a draining data pool must not receive new segment targets"
        );

        // A pool that turns Draining mid-run stops receiving selections:
        // drain the last data pool and nothing is eligible any more.
        registry.begin_drain(0).unwrap();
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

    // ---- d3 headroom-aware sibling selection (ADR-0036 C1a/D5) ----

    #[test]
    fn headroom_selection_skips_excluded_source_and_picks_a_sibling() {
        let (_tmp, registry) = registry_with_capacities(
            &[("data-a", PoolRole::Data, 1), ("data-b", PoolRole::Data, 1)],
            &[(100 * GIB, 20 * GIB), (100 * GIB, 20 * GIB)],
        );
        let policy = PlacementPolicy::new();

        // The drain worker excludes the source pool (data-a); the sibling
        // must be picked regardless of the tie-break.
        let target = policy
            .select_data_pool_with_headroom(&registry, &[0], 1 * GIB)
            .expect("a sibling data pool");
        assert_eq!(target.id(), 1);

        // Excluding the other sibling leaves only the source — and it is
        // excluded, so nothing is eligible.
        assert!(policy.select_data_pool_with_headroom(&registry, &[0, 1], 1 * GIB).is_none());
    }

    #[test]
    fn headroom_selection_requires_required_free_plus_min_headroom() {
        let (_tmp, registry) = registry_with_capacities(
            &[("data-a", PoolRole::Data, 1)],
            &[(100 * GIB, 64 * 1024 * 1024 + 1 * 1024 * 1024)],
        );
        let policy = PlacementPolicy::new();

        // Free = 65 MiB. A 1 MiB segment needs 1 + 64 = 65 MiB → eligible.
        let target = policy
            .select_data_pool_with_headroom(&registry, &[], 1 * 1024 * 1024)
            .expect("1 MiB segment fits with headroom");
        assert_eq!(target.id(), 0);

        // A 2 MiB segment needs 66 MiB → not eligible any more.
        assert!(policy.select_data_pool_with_headroom(&registry, &[], 2 * 1024 * 1024).is_none());
    }

    #[test]
    fn headroom_selection_excludes_a_draining_pool_even_when_not_listed() {
        // Three data pools; data-b drains. Excluding only the source
        // (data-a) must still route around the draining sibling.
        let (_tmp, registry) = registry_with_capacities(
            &[
                ("data-a", PoolRole::Data, 1),
                ("data-b", PoolRole::Data, 1),
                ("data-c", PoolRole::Data, 1),
            ],
            &[(100 * GIB, 20 * GIB), (100 * GIB, 20 * GIB), (100 * GIB, 20 * GIB)],
        );
        let policy = PlacementPolicy::new();
        registry.begin_drain(1).unwrap();

        let target = policy
            .select_data_pool_with_headroom(&registry, &[0], 1 * GIB)
            .expect("a non-draining sibling");
        assert_eq!(target.id(), 2, "the draining sibling is excluded by status");
    }

    #[test]
    fn headroom_selection_returns_none_without_enough_capacity_anywhere() {
        // One sibling that cannot fit the segment (below the combined
        // required + headroom bound) → None, so the worker parks blocked.
        let (_tmp, registry) = registry_with_capacities(
            &[("data-a", PoolRole::Data, 1), ("data-b", PoolRole::Data, 1)],
            &[(100 * GIB, 20 * GIB), (100 * GIB, 8 * GIB)],
        );
        let policy = PlacementPolicy::new();

        // data-a is excluded (source); data-b has only 8 GiB free while the
        // segment needs 9 GiB + headroom → no eligible target.
        assert!(policy.select_data_pool_with_headroom(&registry, &[0], 9 * GIB).is_none());
    }

    #[test]
    fn headroom_selection_uses_the_same_weighted_free_rule() {
        // Mirrors weighted_selection_prefers_pool_with_more_free_per_weight
        // under the headroom bound: max free/weight wins.
        let (_tmp, registry) = registry_with_capacities(
            &[
                ("data-a", PoolRole::Data, 1),
                ("data-b", PoolRole::Data, 2),
                ("data-c", PoolRole::Data, 1),
            ],
            &[
                (100 * GIB, 10 * GIB),
                (100 * GIB, 10 * GIB),
                (100 * GIB, 1 * GIB), // data-c: below the bound → excluded
            ],
        );
        let policy = PlacementPolicy::new();

        let target = policy
            .select_data_pool_with_headroom(&registry, &[0], 1 * GIB)
            .expect("data-b is eligible");
        assert_eq!(target.id(), 1, "score_b = 10GiB/2 = 5GiB > score_c's floor");

        // Exclude the only eligible winner → None (data-c is too full).
        assert!(policy.select_data_pool_with_headroom(&registry, &[0, 1], 1 * GIB).is_none());
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
        // The hints role is auto-appended by the helper (id 3).
        assert_eq!(policy.select_pinned_pool(&registry, PoolRole::Hints).unwrap().id(), 3);

        // Degraded pinned pool → None.
        registry.set_status(1, PoolStatus::Degraded);
        assert!(policy.select_pinned_pool(&registry, PoolRole::Wal).is_none());
        registry.set_status(3, PoolStatus::Degraded);
        assert!(policy.select_pinned_pool(&registry, PoolRole::Hints).is_none());
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
