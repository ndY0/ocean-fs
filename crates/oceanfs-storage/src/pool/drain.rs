//! Operator-visible drain lifecycle (ADR-0036 D6) for data pools.
//!
//! A pool an operator wants to remove is *drained*: its status becomes
//! [`PoolStatus::Draining`] (a distinct status, not a parallel flag — every
//! existing status consumer reasons about it in one place), placement
//! excludes it from new segments, reads keep serving from it, and the health
//! monitor does not fight the operator's drain. This module carries the
//! **drain record** the status atomic cannot express: the blocked reason and
//! the empty (`Detachable`) terminal state that d5's detach consumes.
//!
//! The machine (ADR-0036 D6):
//!
//! ```text
//! Idle ──(begin_drain)──▶ Draining ──(worker emptied the pool)──▶ Detachable
//!                          │
//!                          └──(no eligible target)──▶ Draining { blocked_reason }
//! ```
//!
//! No-destructive-failure rule: a drain that cannot complete (no eligible
//! sibling *and* no eligible cluster target) keeps the pool `Draining` with
//! the blocked reason surfaced (this module's record + the
//! `oceanfs_pool_drain_blocked_reason` gauge) and deletes nothing. `Detach`
//! (d5) is only accepted on a `Detachable` pool.
//!
//! ## Genuine loss beats the drain flag
//!
//! A confirmed-loss transition (`HealthMonitor` → `Dead`) never clears the
//! drain record: the pool keeps its `Draining` entry (harmless — d1 has no
//! worker), the drain worker of d3/d4 observes the `Dead` status and parks,
//! and the loss-announcement/heal path takes over. Because the monitor is
//! absorbing for `Draining` except on confirmed loss, a `begin_drain` that
//! races a genuine `ConfirmedLoss` is self-correcting: the monitor
//! re-confirms the loss on its next tick and moves the pool to `Dead`
//! regardless of the operator flag.
//!
//! The mutation methods live on [`PoolRegistry`] (this file) so the drain
//! record, the pool status atomic, and the drain gauges stay in one place.
//! The registry is pure state: manifest re-gossip after a drain transition
//! is the node composition root's job (the f8-attach hook pattern), not this
//! module's. The actual mover workers are d3 (intra-node) and d4 (cluster),
//! which consume this state.
//!
//! ## LOCK ORDER
//!
//! Mirrors `PoolRegistry::drain`'s note in `mod.rs`: the drain map is never
//! held while acquiring the `pools` lock. Every mutation reads pool status /
//! role first (short `pools` read lock inside `pool_by_id`, released on
//! return) and only then takes the drain write lock. `attach` (mod.rs)
//! pushes under the `pools` write lock first and releases it before
//! inserting the new pool's `Idle` record, so no path holds both locks at
//! once — no lock cycle.

use oceanfs_core::PoolRole;

use super::{PoolMetrics, PoolRegistry, PoolStatus};

/// The operator-visible drain record of one data pool (ADR-0036 D6).
///
/// Distinct from [`PoolStatus`]: the status atomic encodes *placement and
/// health truth* (`Draining` for both this machine's active and terminal
/// states); this record carries the operator lifecycle — the blocked reason
/// and the empty (`Detachable`) state d5 detaches on. A pool whose status
/// is `Healthy`/`Degraded`/`Dead` has no entry here and reports [`Idle`]
/// (self).
///
/// [`Idle`]: DrainState::Idle
///
/// # Examples
///
/// ```
/// use oceanfs_storage::DrainState;
///
/// assert_eq!(DrainState::Idle.as_u8(), 0);
/// assert_eq!(
///     DrainState::Draining { blocked_reason: None, paused: false }.as_u8(),
///     1
/// );
/// assert_eq!(DrainState::Detachable.as_u8(), 2);
/// assert_eq!(
///     DrainState::Draining { blocked_reason: Some("no headroom".into()), paused: false }
///         .blocked_reason(),
///     Some("no headroom")
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainState {
    /// No drain in progress (or requested) on this pool.
    Idle,
    /// The pool is being drained. `blocked_reason` is `Some` when no
    /// eligible target exists — the drain is parked, nothing is deleted,
    /// and the reason is surfaced (no-destructive-failure rule). `paused`
    /// is set by the operator pause/resume controls (d3): a paused drain
    /// makes the worker's next cycles a no-op for this pool without
    /// aborting a relocation that already holds the per-segment lock.
    Draining {
        /// Why the drain cannot proceed (`None` = actively draining).
        blocked_reason: Option<String>,
        /// Operator pause flag — `true` stops new relocations between
        /// worker ticks (the pool stays `Draining`).
        paused: bool,
    },
    /// The drain worker emptied the pool (no registered segment carries its
    /// `pool_id`); d5's detach is the only valid next step. The pool's
    /// *status* stays `Draining` so placement never refills it.
    Detachable,
}

impl DrainState {
    /// Numeric encoding used by the `oceanfs_pool_drain_state` gauge
    /// (0 = Idle, 1 = Draining, 2 = Detachable).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::DrainState;
    ///
    ///     assert_eq!(DrainState::Draining { blocked_reason: None, paused: false }.as_u8(), 1);
    /// ```
    pub fn as_u8(&self) -> u8 {
        match self {
            DrainState::Idle => 0,
            DrainState::Draining { .. } => 1,
            DrainState::Detachable => 2,
        }
    }

    /// Stable status string for the admin surface and dashboards
    /// (`"idle" | "draining" | "detachable"`).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::DrainState;
    ///
    /// assert_eq!(DrainState::Detachable.as_str(), "detachable");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            DrainState::Idle => "idle",
            DrainState::Draining { .. } => "draining",
            DrainState::Detachable => "detachable",
        }
    }

    /// The current blocked reason, when the pool's drain is parked on a
    /// missing eligible target.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::DrainState;
    ///
    ///     assert_eq!(DrainState::Draining { blocked_reason: Some("no headroom".into()), paused: false }
    ///         .blocked_reason(),
    ///     Some("no headroom")
    /// );
    /// assert_eq!(DrainState::Idle.blocked_reason(), None);
    /// ```
    pub fn blocked_reason(&self) -> Option<&str> {
        match self {
            DrainState::Draining { blocked_reason: Some(reason), .. } => Some(reason),
            _ => None,
        }
    }

    /// Whether the drain is parked on a blocked state (no eligible target).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::DrainState;
    ///
    /// assert!(DrainState::Draining { blocked_reason: Some("none".into()), paused: false }.is_blocked());
    /// assert!(!DrainState::Draining { blocked_reason: None, paused: false }.is_blocked());
    /// ```
    pub fn is_blocked(&self) -> bool {
        self.blocked_reason().is_some()
    }

    /// Whether the drain is paused by the operator (d3 pause/resume
    /// controls). A paused drain stays `Draining`; the worker skips it
    /// until [`PoolRegistry::set_drain_paused`] clears the flag.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::DrainState;
    ///
    /// assert!(DrainState::Draining { blocked_reason: None, paused: true }.is_paused());
    /// assert!(!DrainState::Draining { blocked_reason: None, paused: false }.is_paused());
    /// assert!(!DrainState::Idle.is_paused());
    /// assert!(!DrainState::Detachable.is_paused());
    /// ```
    pub fn is_paused(&self) -> bool {
        matches!(self, DrainState::Draining { paused: true, .. })
    }
}

/// Which drain mover owns a `Draining` pool (d3/d4 ownership
/// discriminator).
///
/// d3's `IntraNodeDrain` and d4's cluster controller both consume the
/// registry drain state, so a pool's mode routes it to exactly one mover:
/// `IntraNode` pools are emptied to sibling data pools (d3), `Cluster`
/// pools are emptied off-node through the ADR-0030 target-pull + source-
/// release path (d4). Pools begun through the d1 `begin_drain` seam (and
/// the d3 intra-node route) default to [`DrainMode::IntraNode`].
///
/// # Examples
///
/// ```
/// use oceanfs_storage::DrainMode;
///
/// assert_eq!(DrainMode::IntraNode.as_str(), "intra-node");
/// assert_eq!(DrainMode::Cluster.as_str(), "cluster");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DrainMode {
    /// d3's sibling-pool mover owns the pool.
    IntraNode,
    /// d4's cluster (off-node) controller owns the pool.
    Cluster,
}

impl DrainMode {
    /// Stable mode string for admin surfaces (`"intra-node" |
    /// "cluster"`).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::DrainMode;
    ///
    /// assert_eq!(DrainMode::Cluster.as_str(), "cluster");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            DrainMode::IntraNode => "intra-node",
            DrainMode::Cluster => "cluster",
        }
    }
}

/// Errors from the drain-lifecycle transitions on [`PoolRegistry`].
///
/// # Examples
///
/// ```
/// use oceanfs_storage::DrainStateError;
///
/// // The error type is `Display`-able and `std::error::Error`-compatible.
/// let err = DrainStateError::NotDataPool(0);
/// assert_eq!(err.to_string(), "pool 0 is not a data pool; only data pools can drain");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DrainStateError {
    /// No registered pool carries this id.
    #[error("pool {0} is not registered")]
    UnknownPool(u32),

    /// Only `data`-role pools drain (ADR-0036 D6; the wal/metadata/hints
    /// roles are cardinality-1 and their replacement is the g7/g8 path).
    #[error("pool {0} is not a data pool; only data pools can drain")]
    NotDataPool(u32),

    /// The pool has confirmed data loss; genuine loss beats the operator's
    /// drain flag (the drain worker observes `Dead` and parks).
    #[error("pool {0} is Dead (confirmed loss); a dead pool cannot be drained")]
    DeadPool(u32),

    /// The pool's status is already `Draining`.
    #[error("pool {0} is already draining")]
    AlreadyDraining(u32),

    /// A drain-transition was requested on a pool that is not draining.
    #[error("pool {0} is not draining")]
    NotDraining(u32),
}

/// Why [`PoolRegistry::detach`] rejected removing a pool (d5,
/// ADR-0036 D6/D8).
///
/// Detach is the inverse of f8 `attach` and is accepted **only** on an
/// empty (`Detachable`) `data` pool — the no-destructive-failure rule.
/// The registry does not own the segment-count knowledge that proves a
/// pool is empty; the node verifies emptiness (the d4 definition: no
/// `Reserved` entry and no `Sealed` entry this node still holds on the
/// pool) immediately before calling [`PoolRegistry::detach`] and reports a
/// non-empty pool as a `409`-class rejection without ever touching the
/// registry.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DetachError {
    /// No registered pool carries this id.
    #[error("pool {0} is not registered")]
    UnknownPool(u32),

    /// Only `data`-role pools detach (ADR-0036 D6; the wal/metadata/hints
    /// roles are cardinality-1 and their replacement is the g7/g8 path).
    #[error("pool {0} is not a data pool; only data pools can detach")]
    WrongRole(u32, oceanfs_core::PoolRole),

    /// The pool is not empty-and-`Detachable`; drain it to emptiness
    /// first (ADR-0036 D6 — detach only on empty; nothing half-removed).
    #[error("pool {0} is not Detachable; drain it to empty first (ADR-0036 D6)")]
    NotDetachable(u32),
}

/// Refreshes a pool's two drain gauges from its drain record.
fn update_drain_gauges(metric: &PoolMetrics, state: &DrainState) {
    metric.drain_state.set(u64::from(state.as_u8()));
    metric.drain_blocked.set(u64::from(state.is_blocked()));
}

impl PoolRegistry {
    /// Marks a `data`-role pool as `Draining` and records the drain
    /// lifecycle (ADR-0036 D6) with the **intra-node** mover owning it
    /// (d3). Equivalent to
    /// [`begin_drain_with_mode`](Self::begin_drain_with_mode) with
    /// [`DrainMode::IntraNode`] — see that method for the full contract.
    ///
    /// # Errors
    ///
    /// [`DrainStateError::UnknownPool`], [`DrainStateError::NotDataPool`],
    /// [`DrainStateError::DeadPool`], [`DrainStateError::AlreadyDraining`].
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::{DrainState, DrainMode, PoolRegistry, PoolStatus};
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "data-1".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data-1"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
    /// let data = registry.pool_by_role(PoolRole::Data).expect("data pool");
    ///
    /// registry.begin_drain(data.id()).expect("begin drain");
    /// assert_eq!(registry.pool_by_id(data.id()).expect("pool").status(), PoolStatus::Draining);
    /// assert_eq!(
    ///     registry.drain_state(data.id()),
    ///     DrainState::Draining { blocked_reason: None, paused: false }
    /// );
    /// assert_eq!(registry.drain_mode(data.id()), DrainMode::IntraNode);
    /// assert!(registry.is_draining(data.id()));
    /// ```
    pub fn begin_drain(&self, pool_id: u32) -> Result<(), DrainStateError> {
        self.begin_drain_with_mode(pool_id, DrainMode::IntraNode)
    }

    /// Marks a `data`-role pool as `Draining` and records the drain
    /// lifecycle (ADR-0036 D6), declaring which mover owns the pool.
    ///
    /// Placement excludes the pool from new segment targets immediately
    /// (only `Healthy` pools are eligible), reads keep serving, and the
    /// health monitor stops transitioning it on degrading signals. The
    /// mover workers (d3 `IntraNodeDrain` for [`DrainMode::IntraNode`],
    /// d4 cluster controller for [`DrainMode::Cluster`]) pick the pool up
    /// from this state; [`PoolRegistry::drain_mode`] routes a draining
    /// pool to exactly one of them.
    ///
    /// # Errors
    ///
    /// - [`DrainStateError::UnknownPool`] — no pool with this id.
    /// - [`DrainStateError::NotDataPool`] — only `data`-role pools drain.
    /// - [`DrainStateError::DeadPool`] — confirmed loss beats the operator
    ///   flag; recover/heal first.
    /// - [`DrainStateError::AlreadyDraining`] — the pool is already draining
    ///   (double drain is a no-op request error).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::{DrainMode, PoolRegistry};
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
    /// let data = registry.pool_by_role(PoolRole::Data).expect("data pool");
    /// registry.begin_drain_with_mode(data.id(), DrainMode::Cluster).expect("begin");
    /// assert_eq!(registry.drain_mode(data.id()), DrainMode::Cluster);
    /// ```
    pub fn begin_drain_with_mode(
        &self,
        pool_id: u32,
        mode: DrainMode,
    ) -> Result<(), DrainStateError> {
        // Validate against the live pool (short `pools` read lock, released
        // on return — never held while the drain lock is taken).
        let pool = self.pool_by_id(pool_id).ok_or(DrainStateError::UnknownPool(pool_id))?;
        if pool.role() != PoolRole::Data {
            return Err(DrainStateError::NotDataPool(pool_id));
        }
        match pool.status() {
            // Genuine confirmed loss beats the operator flag: a dead pool
            // cannot be drained; recover/heal it first.
            PoolStatus::Dead => return Err(DrainStateError::DeadPool(pool_id)),
            PoolStatus::Draining => return Err(DrainStateError::AlreadyDraining(pool_id)),
            PoolStatus::Healthy | PoolStatus::Degraded => {}
        }

        {
            let mut drain = self.drain.write();
            let state = DrainState::Draining { blocked_reason: None, paused: false };
            drain.insert(pool_id, state.clone());
            if let Some(metric) = self.metrics_for(pool_id) {
                update_drain_gauges(&metric, &state);
            }
        }
        // The mode routes the pool to one mover; a separate short lock from
        // the drain record (never both held).
        self.drain_mode.write().insert(pool_id, mode);
        // Placement/health/peers read the status atomic — flip it after the
        // record so the two writes never disagree for a concurrent reader
        // beyond one transition.
        self.set_status(pool_id, PoolStatus::Draining);
        Ok(())
    }

    /// The drain mover mode of a pool (d3 vs d4 ownership), defaulting to
    /// [`DrainMode::IntraNode`] for pools that never drained (or were
    /// begun through the plain d1 seam).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::{DrainMode, PoolRegistry};
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
    /// assert_eq!(registry.drain_mode(0), DrainMode::IntraNode);
    /// ```
    pub fn drain_mode(&self, pool_id: u32) -> DrainMode {
        self.drain_mode.read().get(&pool_id).copied().unwrap_or(DrainMode::IntraNode)
    }

    /// Records or clears the blocked reason on a `Draining` pool
    /// (no-destructive-failure surfacing, ADR-0036 D6).
    ///
    /// The d3/d4 drain workers call this when no eligible target exists:
    /// the pool stays `Draining`, the reason is surfaced (this record +
    /// `oceanfs_pool_drain_blocked_reason`), and **nothing is deleted**.
    /// `None` clears the blocked state (an eligible target appeared).
    ///
    /// # Errors
    ///
    /// [`DrainStateError::UnknownPool`] when no pool carries the id;
    /// [`DrainStateError::NotDraining`] when the pool is not `Draining`.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::{DrainState, PoolRegistry};
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "data-1".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data-1"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
    /// let data = registry.pool_by_role(PoolRole::Data).expect("data pool");
    /// registry.begin_drain(data.id()).expect("begin drain");
    ///
    /// registry.set_drain_blocked(data.id(), Some("no sibling headroom")).expect("block");
    /// let state = registry.drain_state(data.id());
    /// assert!(state.is_blocked());
    /// assert_eq!(state.blocked_reason(), Some("no sibling headroom"));
    ///
    /// registry.set_drain_blocked(data.id(), None).expect("clear");
    /// assert_eq!(
    ///     registry.drain_state(data.id()),
    ///     DrainState::Draining { blocked_reason: None, paused: false }
    /// );
    /// ```
    pub fn set_drain_blocked(
        &self,
        pool_id: u32,
        reason: Option<&str>,
    ) -> Result<(), DrainStateError> {
        if self.pool_by_id(pool_id).is_none() {
            return Err(DrainStateError::UnknownPool(pool_id));
        }
        let mut drain = self.drain.write();
        let paused = match drain.get(&pool_id) {
            Some(DrainState::Draining { paused, .. }) => *paused,
            _ => return Err(DrainStateError::NotDraining(pool_id)),
        };
        let state = DrainState::Draining { blocked_reason: reason.map(str::to_string), paused };
        drain.insert(pool_id, state.clone());
        if let Some(metric) = self.metrics_for(pool_id) {
            update_drain_gauges(&metric, &state);
        }
        Ok(())
    }

    /// Sets or clears the operator pause flag on a `Draining` pool
    /// (d3 pause/resume controls).
    ///
    /// A paused drain stays `Draining` and keeps its blocked reason (if
    /// any); the drain worker skips the pool until the flag is cleared.
    /// Pausing never aborts a relocation that already holds the per-segment
    /// lock — it only stops *new* relocations between worker ticks.
    ///
    /// # Errors
    ///
    /// [`DrainStateError::UnknownPool`] when no pool carries the id;
    /// [`DrainStateError::NotDraining`] when the pool is not actively
    /// `Draining` (an idle or already-`Detachable` pool cannot be paused).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::{DrainState, PoolRegistry};
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
    /// let data = registry.pool_by_role(PoolRole::Data).expect("data pool");
    /// registry.begin_drain(data.id()).expect("begin drain");
    ///
    /// registry.set_drain_paused(data.id(), true).expect("pause");
    /// let state = registry.drain_state(data.id());
    /// assert!(state.is_paused());
    /// assert_eq!(state.as_str(), "draining", "paused drains stay Draining");
    ///
    /// registry.set_drain_paused(data.id(), false).expect("resume");
    /// assert!(!registry.drain_state(data.id()).is_paused());
    /// ```
    pub fn set_drain_paused(&self, pool_id: u32, paused: bool) -> Result<(), DrainStateError> {
        if self.pool_by_id(pool_id).is_none() {
            return Err(DrainStateError::UnknownPool(pool_id));
        }
        let mut drain = self.drain.write();
        let blocked_reason = match drain.get(&pool_id) {
            Some(DrainState::Draining { blocked_reason, .. }) => blocked_reason.clone(),
            _ => return Err(DrainStateError::NotDraining(pool_id)),
        };
        let state = DrainState::Draining { blocked_reason, paused };
        drain.insert(pool_id, state.clone());
        if let Some(metric) = self.metrics_for(pool_id) {
            update_drain_gauges(&metric, &state);
        }
        Ok(())
    }

    /// Marks a `Draining` pool empty: the drain worker found no registered
    /// segment carrying this `pool_id`, so the pool becomes `Detachable`.
    ///
    /// The pool's **status stays `Draining`** — returning to `Healthy` would
    /// let placement silently refill a pool the operator is removing. d5's
    /// detach removes the pool from the registry entirely.
    ///
    /// # Errors
    ///
    /// [`DrainStateError::UnknownPool`] when no pool carries the id;
    /// [`DrainStateError::NotDraining`] when the pool is not actively
    /// `Draining` (including a `Dead` pool — confirmed loss parks the drain
    /// worker; an emptied-pool transition must not fire on a dead pool).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::{DrainState, PoolRegistry};
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "data-1".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data-1"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
    /// let data = registry.pool_by_role(PoolRole::Data).expect("data pool");
    /// registry.begin_drain(data.id()).expect("begin drain");
    ///
    /// registry.set_pool_empty(data.id()).expect("pool drained empty");
    /// assert_eq!(registry.drain_state(data.id()), DrainState::Detachable);
    /// // The status stays Draining so placement never refills the pool.
    /// assert!(registry.is_draining(data.id()));
    /// ```
    pub fn set_pool_empty(&self, pool_id: u32) -> Result<(), DrainStateError> {
        let pool = self.pool_by_id(pool_id).ok_or(DrainStateError::UnknownPool(pool_id))?;
        if pool.status() != PoolStatus::Draining {
            return match pool.status() {
                PoolStatus::Dead => Err(DrainStateError::DeadPool(pool_id)),
                _ => Err(DrainStateError::NotDraining(pool_id)),
            };
        }
        let mut drain = self.drain.write();
        if !matches!(drain.get(&pool_id), Some(DrainState::Draining { .. })) {
            return Err(DrainStateError::NotDraining(pool_id));
        }
        drain.insert(pool_id, DrainState::Detachable);
        if let Some(metric) = self.metrics_for(pool_id) {
            update_drain_gauges(&metric, &DrainState::Detachable);
        }
        Ok(())
    }

    /// Records that the drain mover dispatched one sealed segment's copy
    /// OFF this pool (d4's `oceanfs_drain_dispatched_total` close — the
    /// intra-node mover reports a relocation; the cluster mover reports an
    /// off-node re-replication).
    ///
    /// Fire-and-forget over the pool's metric series; unknown pools (or
    /// pools whose series were detached in d5) are ignored.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::PoolRegistry;
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
    /// let data = registry.pool_by_role(PoolRole::Data).expect("data pool");
    /// registry.note_drain_dispatched(data.id());
    /// ```
    pub fn note_drain_dispatched(&self, pool_id: u32) {
        if let Some(metric) = self.metrics_for(pool_id) {
            metric.drain_dispatched.inc();
        }
    }

    /// Records that the drain mover released one sealed segment's source
    /// copy from this pool (relocation unlink / cluster source-release /
    /// RF-satisfied direct release).
    ///
    /// Fire-and-forget over the pool's metric series; unknown pools are
    /// ignored.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::PoolRegistry;
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
    /// let data = registry.pool_by_role(PoolRole::Data).expect("data pool");
    /// registry.note_drain_released(data.id());
    /// ```
    pub fn note_drain_released(&self, pool_id: u32) {
        if let Some(metric) = self.metrics_for(pool_id) {
            metric.drain_released.inc();
        }
    }

    /// Publishes the number of sealed segments the node still holds on a
    /// pool (`oceanfs_drain_remaining{pool_id}`; the movers report it each
    /// cycle and 0 once the pool is `Detachable`).
    ///
    /// Fire-and-forget over the pool's metric series; unknown pools are
    /// ignored.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::PoolRegistry;
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
    /// let data = registry.pool_by_role(PoolRole::Data).expect("data pool");
    /// registry.set_drain_remaining(data.id(), 3);
    /// registry.set_drain_remaining(data.id(), 0);
    /// ```
    pub fn set_drain_remaining(&self, pool_id: u32, remaining: u64) {
        if let Some(metric) = self.metrics_for(pool_id) {
            metric.drain_remaining.set(remaining);
        }
    }

    /// Returns the drain record of a pool (`Idle` when no drain was
    /// requested or the pool is unknown).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::{DrainState, PoolRegistry};
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
    /// let data = registry.pool_by_role(PoolRole::Data).expect("data pool");
    ///
    /// assert_eq!(registry.drain_state(data.id()), DrainState::Idle);
    /// registry.begin_drain(data.id()).expect("begin drain");
    /// assert_eq!(
    ///     registry.drain_state(data.id()),
    ///     DrainState::Draining { blocked_reason: None, paused: false }
    /// );
    /// ```
    pub fn drain_state(&self, pool_id: u32) -> DrainState {
        self.drain.read().get(&pool_id).cloned().unwrap_or(DrainState::Idle)
    }

    /// Cheap "is draining" check for placement/doc sites.
    ///
    /// Reads the status atomic only (no lock): a pool whose status is
    /// `Draining` is not a placement target. This deliberately matches the
    /// *status* (not the drain record) — a `Detachable` pool keeps status
    /// `Draining` and stays excluded until d5 detaches it, and a pool that
    /// suffered genuine confirmed loss mid-drain reports `Dead` (loss beats
    /// the operator flag).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::PoolRegistry;
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
    /// let data = registry.pool_by_role(PoolRole::Data).expect("data pool");
    ///
    /// assert!(!registry.is_draining(data.id()));
    /// registry.begin_drain(data.id()).expect("begin drain");
    /// assert!(registry.is_draining(data.id()));
    /// ```
    pub fn is_draining(&self, pool_id: u32) -> bool {
        self.pool_by_id(pool_id).is_some_and(|pool| pool.status() == PoolStatus::Draining)
    }

    /// Removes an empty (`Detachable`) `data` pool from the live registry
    /// (d5, ADR-0036 D1/D6/D8) — the inverse of f8 [`PoolRegistry::attach`].
    ///
    /// The pool's `StoragePool` leaves the registry (`data_pools`,
    /// `pool_by_role`, placement snapshots — a detached pool is immediately
    /// invisible to new placement and root resolution), its per-pool metric
    /// series and drain/drain-mode records are dropped, and its
    /// id/name/root are released so a later `attach` with the same root
    /// (hot-swap) is no longer a duplicate.
    ///
    /// The **root directory is not deleted** — the pool is empty, but
    /// removing the directory/device is the operator's concern; detach
    /// only removes the pool from the live topology. Persisting the removal
    /// across restart (the `config − removed` overlay, ADR-0036 D8) and
    /// re-gossiping the manifest are the node composition root's job, not
    /// this method's.
    ///
    /// Preconditions (the node verifies the first two before calling):
    /// the pool's drain record is [`DrainState::Detachable`] — set only by
    /// a drain worker (d3/d4) that found zero registered segments on the
    /// pool — and the pool is `data`-role. Because `Detachable` is a
    /// terminal drain state (nothing transitions it back; only this method
    /// consumes it), the state re-checked here under the registry write
    /// lock cannot race a transition — a concurrent duplicate detach simply
    /// fails with [`DetachError::UnknownPool`].
    ///
    /// # Errors
    ///
    /// [`DetachError::UnknownPool`], [`DetachError::WrongRole`],
    /// [`DetachError::NotDetachable`].
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    /// use oceanfs_storage::{DrainState, PoolRegistry};
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "data-1".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data-1"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
    /// let data = registry.pool_by_role(PoolRole::Data).expect("data pool");
    ///
    /// // A healthy pool is not Detachable → detach is refused.
    /// assert!(registry.detach(data.id()).is_err());
    ///
    /// registry.begin_drain(data.id()).expect("begin drain");
    /// registry.set_pool_empty(data.id()).expect("worker emptied the pool");
    /// assert_eq!(registry.drain_state(data.id()), DrainState::Detachable);
    ///
    /// registry.detach(data.id()).expect("detach the empty pool");
    /// assert!(registry.pool_by_id(data.id()).is_none());
    /// ```
    pub fn detach(&self, pool_id: u32) -> Result<(), DetachError> {
        // ---- 1. Fast pre-checks under a short read lock ----
        // The pool must exist, be `data`-role, and be `Detachable`. These
        // re-run under the write lock below (attach's TOCTOU pattern); the
        // drain-state read happens BEFORE the write lock so no path holds
        // the `pools` and `drain` locks at once (LOCK ORDER note above).
        let pool = self.pool_by_id(pool_id).ok_or(DetachError::UnknownPool(pool_id))?;
        if pool.role() != PoolRole::Data {
            return Err(DetachError::WrongRole(pool_id, pool.role()));
        }
        if self.drain_state(pool_id) != DrainState::Detachable {
            return Err(DetachError::NotDetachable(pool_id));
        }

        // ---- 2. Remove under the write lock (short critical section) ----
        // A concurrent duplicate detach between step 1 and here sees
        // `UnknownPool` (the pool is already gone); no other transition can
        // consume `Detachable`, so the state re-check is stable.
        {
            let mut pools = self.pools.write();
            let pool = pools
                .iter()
                .find(|pool| pool.id() == pool_id)
                .ok_or(DetachError::UnknownPool(pool_id))?;
            if pool.role() != PoolRole::Data {
                return Err(DetachError::WrongRole(pool_id, pool.role()));
            }
            pools.retain(|pool| pool.id() != pool_id);
        }
        // Metric series + drain records are separate maps; drop the pool's
        // entries so refresh/status paths never touch a removed pool. These
        // locks are independent of `pools` and are taken after it is
        // released (LOCK ORDER note).
        self.metrics.write().retain(|metric| metric.pool_id != pool_id);
        self.drain.write().remove(&pool_id);
        self.drain_mode.write().remove(&pool_id);

        tracing::info!(pool_id, "storage pool detached at runtime");
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::Path;

    use super::*;

    /// Role-complete topology (ADR-0031): data×2 (ids 0,1), wal, metadata,
    /// hints — sibling roots under a fresh tempdir.
    fn registry_with_two_data_pools() -> (PoolRegistry, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let pool = |name: &str, role: PoolRole, root: &Path| oceanfs_core::StoragePoolConfig {
            name: name.to_string(),
            role,
            root: root.to_path_buf(),
            weight: Some(1),
            tech: Default::default(),
            health: Default::default(),
        };
        let storage = oceanfs_core::StorageConfig {
            pools: vec![
                pool("data-0", PoolRole::Data, &tmp.path().join("nvme0")),
                pool("data-1", PoolRole::Data, &tmp.path().join("nvme1")),
                pool("journal", PoolRole::Wal, &tmp.path().join("optane0")),
                pool("meta", PoolRole::Metadata, &tmp.path().join("optane1")),
                pool("hints", PoolRole::Hints, &tmp.path().join("hints0")),
            ],
            health: Default::default(),
            missing_root_policy: Default::default(),
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();
        (registry, tmp)
    }

    // -- begin_drain validation --

    #[test]
    fn begin_drain_on_unknown_pool_errors() {
        let (registry, _tmp) = registry_with_two_data_pools();
        assert_eq!(registry.begin_drain(99), Err(DrainStateError::UnknownPool(99)));
    }

    #[test]
    fn begin_drain_on_wal_pool_errors() {
        let (registry, _tmp) = registry_with_two_data_pools();
        let wal_id = registry.pool_by_role(PoolRole::Wal).unwrap().id();
        assert_eq!(registry.begin_drain(wal_id), Err(DrainStateError::NotDataPool(wal_id)));
    }

    #[test]
    fn begin_drain_on_dead_pool_errors() {
        let (registry, _tmp) = registry_with_two_data_pools();
        registry.set_status(0, PoolStatus::Dead);
        assert_eq!(registry.begin_drain(0), Err(DrainStateError::DeadPool(0)));
    }

    #[test]
    fn begin_drain_on_degraded_pool_succeeds() {
        let (registry, _tmp) = registry_with_two_data_pools();
        registry.set_status(0, PoolStatus::Degraded);
        registry.begin_drain(0).unwrap();
        assert_eq!(registry.pool_by_id(0).unwrap().status(), PoolStatus::Draining);
    }

    #[test]
    fn begin_drain_flips_status_and_records_state() {
        let (registry, _tmp) = registry_with_two_data_pools();
        registry.begin_drain(0).unwrap();
        assert_eq!(registry.pool_by_id(0).unwrap().status(), PoolStatus::Draining);
        assert_eq!(
            registry.drain_state(0),
            DrainState::Draining { blocked_reason: None, paused: false }
        );
        assert!(registry.is_draining(0));
        // The sibling data pool is untouched.
        assert_eq!(registry.drain_state(1), DrainState::Idle);
        assert!(!registry.is_draining(1));
    }

    #[test]
    fn double_begin_drain_errors() {
        let (registry, _tmp) = registry_with_two_data_pools();
        registry.begin_drain(0).unwrap();
        assert_eq!(registry.begin_drain(0), Err(DrainStateError::AlreadyDraining(0)));
    }

    // -- set_drain_blocked --

    #[test]
    fn set_drain_blocked_on_non_draining_pool_errors() {
        let (registry, _tmp) = registry_with_two_data_pools();
        assert_eq!(
            registry.set_drain_blocked(0, Some("no target")),
            Err(DrainStateError::NotDraining(0))
        );
    }

    #[test]
    fn set_drain_blocked_records_and_clears_reason() {
        let (registry, _tmp) = registry_with_two_data_pools();
        registry.begin_drain(0).unwrap();
        registry.set_drain_blocked(0, Some("no sibling headroom")).unwrap();
        let state = registry.drain_state(0);
        assert!(state.is_blocked());
        assert_eq!(state.blocked_reason(), Some("no sibling headroom"));
        // The pool stays Draining — nothing is deleted or half-removed.
        assert_eq!(registry.pool_by_id(0).unwrap().status(), PoolStatus::Draining);

        registry.set_drain_blocked(0, None).unwrap();
        assert_eq!(
            registry.drain_state(0),
            DrainState::Draining { blocked_reason: None, paused: false }
        );
    }

    // -- set_pool_empty --

    #[test]
    fn set_pool_empty_on_idle_pool_errors() {
        let (registry, _tmp) = registry_with_two_data_pools();
        assert_eq!(registry.set_pool_empty(0), Err(DrainStateError::NotDraining(0)));
    }

    #[test]
    fn set_pool_empty_on_dead_pool_errors() {
        let (registry, _tmp) = registry_with_two_data_pools();
        registry.begin_drain(0).unwrap();
        registry.set_status(0, PoolStatus::Dead);
        assert_eq!(registry.set_pool_empty(0), Err(DrainStateError::DeadPool(0)));
    }

    #[test]
    fn set_pool_empty_transitions_to_detachable_and_keeps_status_draining() {
        let (registry, _tmp) = registry_with_two_data_pools();
        registry.begin_drain(0).unwrap();
        registry.set_drain_blocked(0, Some("stale block")).unwrap();
        registry.set_pool_empty(0).unwrap();
        assert_eq!(registry.drain_state(0), DrainState::Detachable);
        // Placement must never refill the pool: status stays Draining.
        assert_eq!(registry.pool_by_id(0).unwrap().status(), PoolStatus::Draining);
        assert!(registry.is_draining(0), "a Detachable pool is still not a placement target");
    }

    // -- drain_state / is_draining --

    #[test]
    fn drain_state_of_unknown_or_never_drained_pool_is_idle() {
        let (registry, _tmp) = registry_with_two_data_pools();
        assert_eq!(registry.drain_state(0), DrainState::Idle);
        assert_eq!(registry.drain_state(99), DrainState::Idle);
        assert!(!registry.is_draining(99));
    }

    #[test]
    fn drain_state_round_trips_as_str_and_as_u8() {
        assert_eq!(DrainState::Idle.as_u8(), 0);
        assert_eq!(DrainState::Draining { blocked_reason: None, paused: false }.as_u8(), 1);
        assert_eq!(DrainState::Detachable.as_u8(), 2);
        assert_eq!(DrainState::Idle.as_str(), "idle");
        assert_eq!(
            DrainState::Draining { blocked_reason: None, paused: false }.as_str(),
            "draining"
        );
        assert_eq!(DrainState::Detachable.as_str(), "detachable");
    }

    // -- drain mode (d3 vs d4 mover ownership) --

    #[test]
    fn drain_mode_defaults_to_intra_node_and_round_trips() {
        let (registry, _tmp) = registry_with_two_data_pools();
        // A never-drained pool reads IntraNode (d3 owns it by default).
        assert_eq!(registry.drain_mode(0), DrainMode::IntraNode);
        assert_eq!(registry.drain_mode(1), DrainMode::IntraNode);
        assert_eq!(registry.drain_mode(99), DrainMode::IntraNode);

        registry.begin_drain(0).unwrap();
        assert_eq!(registry.drain_mode(0), DrainMode::IntraNode);
        assert_eq!(registry.drain_mode(0).as_str(), "intra-node");

        registry.begin_drain_with_mode(1, DrainMode::Cluster).unwrap();
        assert_eq!(registry.drain_mode(1), DrainMode::Cluster);
        assert_eq!(registry.drain_mode(1).as_str(), "cluster");
        // The lifecycle record is unchanged by the mode discriminator.
        assert!(registry.is_draining(1));
        assert_eq!(
            registry.drain_state(1),
            DrainState::Draining { blocked_reason: None, paused: false }
        );
    }

    // -- set_drain_paused (d3 pause/resume controls) --

    #[test]
    fn set_drain_paused_on_non_draining_pool_errors() {
        let (registry, _tmp) = registry_with_two_data_pools();
        // Idle pool (never began draining).
        assert_eq!(registry.set_drain_paused(0, true), Err(DrainStateError::NotDraining(0)));
        registry.begin_drain(0).unwrap();
        registry.set_pool_empty(0).unwrap();
        // Detachable pool is no longer draining.
        assert_eq!(registry.set_drain_paused(0, true), Err(DrainStateError::NotDraining(0)));
    }

    #[test]
    fn set_drain_paused_on_unknown_pool_errors() {
        let (registry, _tmp) = registry_with_two_data_pools();
        assert_eq!(registry.set_drain_paused(99, true), Err(DrainStateError::UnknownPool(99)));
    }

    #[test]
    fn pause_round_trips_and_preserves_blocked_reason() {
        let (registry, _tmp) = registry_with_two_data_pools();
        registry.begin_drain(0).unwrap();
        registry.set_drain_blocked(0, Some("no sibling headroom")).unwrap();

        registry.set_drain_paused(0, true).unwrap();
        let state = registry.drain_state(0);
        assert!(state.is_paused());
        // Pausing preserves the blocked reason and the Draining status.
        assert_eq!(state.blocked_reason(), Some("no sibling headroom"));
        assert_eq!(registry.pool_by_id(0).unwrap().status(), PoolStatus::Draining);

        registry.set_drain_paused(0, false).unwrap();
        let state = registry.drain_state(0);
        assert!(!state.is_paused());
        assert_eq!(state.blocked_reason(), Some("no sibling headroom"));
    }

    #[test]
    fn paused_state_still_counts_as_draining_for_gauges_and_status() {
        let (registry, _tmp) = registry_with_two_data_pools();
        registry.begin_drain(0).unwrap();
        registry.set_drain_paused(0, true).unwrap();
        // The drain-state gauge keeps its Draining value (1) — pause is a
        // sub-state, not a transition.
        assert_eq!(registry.drain_state(0).as_u8(), 1);
        assert_eq!(registry.drain_state(0).as_str(), "draining");
        assert!(registry.is_draining(0), "paused pools stay excluded from placement");
    }

    // -- detach (d5, ADR-0036 D6/D8) --

    #[test]
    fn detach_on_unknown_pool_errors() {
        let (registry, _tmp) = registry_with_two_data_pools();
        assert_eq!(registry.detach(99), Err(DetachError::UnknownPool(99)));
    }

    #[test]
    fn detach_refused_on_healthy_and_draining_pool() {
        let (registry, _tmp) = registry_with_two_data_pools();
        // Healthy (Idle): refused.
        assert_eq!(registry.detach(0), Err(DetachError::NotDetachable(0)));
        // Draining (not yet empty): still refused — Detachable is the only
        // detachable state (no-destructive-failure rule).
        registry.begin_drain(0).unwrap();
        assert_eq!(registry.detach(0), Err(DetachError::NotDetachable(0)));
    }

    #[test]
    fn detach_refused_on_non_data_pool() {
        let (registry, _tmp) = registry_with_two_data_pools();
        // The wal pool (id 2) can never detach — replacement is the g7/g8 path.
        assert_eq!(registry.detach(2), Err(DetachError::WrongRole(2, PoolRole::Wal)));
    }

    #[test]
    fn detach_succeeds_on_detachable_pool_and_releases_identity() {
        let (registry, tmp) = registry_with_two_data_pools();
        registry.begin_drain(0).unwrap();
        registry.set_pool_empty(0).unwrap();
        assert_eq!(registry.drain_state(0), DrainState::Detachable);

        registry.detach(0).unwrap();
        // The pool is gone from every lookup and its drain records dropped.
        assert!(registry.pool_by_id(0).is_none());
        assert_eq!(registry.pool_count(), 4);
        assert_eq!(registry.data_pools().len(), 1);
        assert_eq!(registry.data_pools()[0].id(), 1);
        assert_eq!(registry.drain_state(0), DrainState::Idle);
        assert_eq!(registry.drain_mode(0), DrainMode::IntraNode);

        // The released name/root is attachable again (hot-swap round-trip);
        // the freed slot id 0 is reused (lowest-free), matching the config
        // order the next boot assigns once the removed record is cleared.
        let id = registry
            .attach(oceanfs_core::StoragePoolConfig {
                name: "data-0".into(),
                role: PoolRole::Data,
                root: tmp.path().join("nvme0"),
                weight: Some(1),
                tech: Default::default(),
                health: Default::default(),
            })
            .expect("released identity is attachable");
        assert_eq!(id, 0);
        assert!(registry.pool_by_id(0).is_some());
        assert_eq!(registry.pool_count(), 5);
    }

    #[test]
    fn detach_of_middle_pool_leaves_survivor_id_unchanged() {
        // Detach id 1 (not id 0): the survivor keeps its durable id 0 — a
        // dense renumber would silently re-point id 0's sealed segments.
        let (registry, _tmp) = registry_with_two_data_pools();
        registry.begin_drain(1).unwrap();
        registry.set_pool_empty(1).unwrap();
        registry.detach(1).unwrap();
        assert!(registry.pool_by_id(1).is_none());
        let survivor = registry.pool_by_id(0).expect("pool 0 survives");
        assert_eq!(survivor.id(), 0);
        assert_eq!(registry.data_pools().len(), 1);
    }
}
