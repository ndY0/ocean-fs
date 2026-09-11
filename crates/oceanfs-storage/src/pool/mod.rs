//! Storage pool runtime (ADR-0029): `StoragePool` + `PoolRegistry`.
//!
//! A node's disks are modeled as *storage pools* — the unit of placement,
//! routing, and failure semantics (§D1). This module is the runtime heart:
//! the pool set is built from the topology config (f1) with startup probing
//! of every root (write+read, `Fatal`/`Degraded` policy), capacity is
//! snapshotted via `statvfs`, and per-pool metrics are registered once at
//! construction.
//!
//! Placement over the registry lives in the [`placement`] submodule (f3).
//!
//! ## Phase A scope (epic `disk-resilience`)
//!
//! - All pools start `Healthy` (or `Degraded` when the startup probe failed
//!   under the `Degraded` policy); `write_degraded` is always `false`. Phase
//!   B's health monitor drives real transitions through
//!   [`PoolRegistry::set_status`] / [`PoolRegistry::set_write_degraded`].
//! - `PoolTech::Auto` resolves to the `Nvme` placeholder; real
//!   auto-detection lands in Phase B with the health monitor.
//! - The registry is constructed once; runtime pool attach (f8) extends it.
//!
//! ## Concurrency (perf guidelines 2.3, 2.4, 7.2)
//!
//! The pool list sits behind a `parking_lot::RwLock`; lookups clone
//! `Arc<StoragePool>` handles under a short read lock. Mutable per-pool
//! state (status, `write_degraded`, capacity) is stored in atomics, so
//! readers never take a lock. The only I/O (`statvfs`) runs in
//! [`PoolRegistry::refresh_capacity`] **outside** the registry lock: it
//! snapshots the list first, then stats each pool.
//!
//! # LOCK ORDER
//!
//! Two independent `RwLock`s: `PoolRegistry.pools` (placement/routing
//! reads dominate — perf 7.2) and `PoolRegistry.metrics` (cold: boot
//! registration + the maintenance-tick capacity refresh). Each is held
//! briefly and never across I/O or across the other lock; the probe
//! (`statvfs`) in `attach`/`refresh_capacity` runs OUTSIDE both. Pool
//! registration on runtime attach (f8) takes the write lock only to
//! push the pre-constructed pool + metric series. `PoolMetrics` series
//! are immutable after construction (gauges/counters are
//! interior-mutable atomics); a new series is pushed on attach.

pub mod drain;
pub mod health;
pub mod placement;
pub mod probe;
pub mod removed;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc,
    },
};

use oceanfs_core::{
    Counter, Gauge, LabelSet, MetricRegistrar, MissingRootPolicy, PoolHealthConfig, PoolRole,
    PoolTech, StorageConfig,
};
use parking_lot::RwLock;
pub use placement::PlacementPolicy;
pub use probe::PoolRootProbe;
pub use removed::PoolRemovedRecord;

/// One GiB — the unit auto-derived placement weights are scaled to
/// (ADR-0029 §D8 "weights with capacity auto-detect default").
const GIB: u64 = 1024 * 1024 * 1024;

/// Marker bytes written and read back by the startup root probe.
const POOL_PROBE_MARKER: &[u8] = b"oceanfs pool probe";

/// Resolves a segment's durable pool id (the node backs it with the
/// lifecycle registry's `SegmentMetadata.pool_id`).
pub type PoolIdResolver = Arc<dyn Fn(&oceanfs_core::SegmentId) -> Option<u32> + Send + Sync>;

/// Resolves a segment's pool root from its durable `pool_id`.
///
/// Pools are mandatory (ADR-0031) and every segment carries a real pool
/// id (config-order scheme: 0 = the first configured pool), so `pool_id`
/// names that pool's root. `None` when no registered pool carries the
/// id — callers surface that as an explicit data-integrity error (the
/// legacy `data_dir` fallback was deleted by ADR-0031 D2). Pure lookup
/// over the pool snapshot — no locks, no I/O (f5 perf: 2.3/7.2).
///
/// # Examples
///
/// ```
/// use oceanfs_storage::resolve_pool_root;
/// # use std::sync::Arc;
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
/// # let registry = oceanfs_storage::PoolRegistry::from_config(&storage, &data_dir).expect("registry");
/// # let pools = registry.data_pools();
/// let root = resolve_pool_root(&pools, 0).expect("pool 0 is registered");
/// assert_eq!(root, tmp.path().join("pool-data"));
///
/// // An id no registered pool carries resolves to `None`.
/// assert_eq!(resolve_pool_root(&pools, 99), None);
/// ```
pub fn resolve_pool_root(pools: &[Arc<StoragePool>], pool_id: u32) -> Option<PathBuf> {
    pools.iter().find(|pool| pool.id() == pool_id).map(|pool| pool.root().to_path_buf())
}

// ---------------------------------------------------------------------------
// PoolStatus
// ---------------------------------------------------------------------------

/// Health state of a storage pool (ADR-0029 §D3 state machine, extended by
/// ADR-0036 D6 for the Phase C drain lifecycle).
///
/// Phase A: all pools are `Healthy`; a pool whose startup probe failed under
/// the `Degraded` policy registers as `Degraded`. `Dead` requires *confirmed
/// loss* (ENOENT on an owned segment, EIO on fsync, device unplug) and is
/// only reachable once Phase B's health monitor is wired.
///
/// `Draining` is the operator-driven Phase C state (ADR-0036 D1/D6): the pool
/// is being emptied and must not receive new segments. It is a *distinct*
/// status, not a parallel flag, so every existing status consumer reasons
/// about it in one place — placement excludes it (only `Healthy` pools are
/// targets), reads keep serving from it (the read path is status-agnostic),
/// and the health monitor never transitions it on degrading signals (genuine
/// confirmed loss still beats the operator flag and moves it to `Dead`).
/// Only `data`-role pools can be `Draining` (drain.rs `begin_drain`); the
/// `wal`/`metadata`/`hints` roles are cardinality-1 and their replacement is
/// the g7/g8 boot path, not drain.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::PoolStatus;
///
/// assert_eq!(PoolStatus::Healthy.as_u8(), 0);
/// assert_eq!(PoolStatus::Degraded.as_u8(), 1);
/// assert_eq!(PoolStatus::Dead.as_u8(), 2);
/// assert_eq!(PoolStatus::Draining.as_u8(), 3);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PoolStatus {
    /// The pool serves reads and writes normally.
    Healthy,
    /// Suspicion of failure (trend/spike); Phase A: startup-probe failure
    /// under the `Degraded` policy only.
    Degraded,
    /// Confirmed loss of the pool's data. Phase B only.
    Dead,
    /// Operator-initiated drain in progress (ADR-0036 D6). The pool is
    /// excluded from new-segment placement, still serves reads, and stays
    /// `Draining` until the drain worker empties it (`Detachable`, d3/d4)
    /// or genuine confirmed loss moves it to `Dead`. Data-role only.
    Draining,
}

impl PoolStatus {
    /// Numeric encoding used by the `oceanfs_pool_status` gauge
    /// (0 = Healthy, 1 = Degraded, 2 = Dead, 3 = Draining).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::PoolStatus;
    ///
    /// assert_eq!(PoolStatus::Degraded.as_u8(), 1);
    /// assert_eq!(PoolStatus::Draining.as_u8(), 3);
    /// ```
    pub fn as_u8(self) -> u8 {
        match self {
            PoolStatus::Healthy => 0,
            PoolStatus::Degraded => 1,
            PoolStatus::Dead => 2,
            PoolStatus::Draining => 3,
        }
    }
}

/// Decodes the atomic status byte back into a [`PoolStatus`].
fn pool_status_from_u8(value: u8) -> PoolStatus {
    match value {
        1 => PoolStatus::Degraded,
        2 => PoolStatus::Dead,
        3 => PoolStatus::Draining,
        _ => PoolStatus::Healthy,
    }
}

// ---------------------------------------------------------------------------
// PoolCapacity
// ---------------------------------------------------------------------------

/// Filesystem capacity snapshot of a pool root (`statvfs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct PoolCapacity {
    /// Total bytes on the filesystem holding the root.
    total_bytes: u64,
    /// Bytes available to unprivileged processes (`f_bavail`) — what a new
    /// segment could actually use; the signal capacity-aware placement reads.
    free_bytes: u64,
}

// ---------------------------------------------------------------------------
// StoragePool
// ---------------------------------------------------------------------------

/// One storage pool on this node: a root directory with a role.
///
/// Identity (id/name/role/root/weight/tech) is immutable; health and
/// capacity state are atomics so concurrent readers never block
/// (perf guidelines 2.4/7.2). Instances are shared via `Arc` and treated as
/// read-only by consumers; mutation goes through
/// [`PoolRegistry::refresh_capacity`], [`PoolRegistry::set_status`], and
/// [`PoolRegistry::set_write_degraded`].
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
///
/// let pool = registry.pool_by_role(PoolRole::Data).expect("the data pool");
/// assert_eq!(pool.role(), PoolRole::Data);
/// assert!(pool.weight() >= 1);
/// ```
pub struct StoragePool {
    /// Stable pool id — the config-order index (0..n).
    id: u32,
    /// Human-readable pool name from the topology config.
    name: String,
    /// Pool purpose (`data | wal | metadata | hints`).
    role: PoolRole,
    /// Mountpoint directory that is this pool's entire failure domain.
    root: PathBuf,
    /// Resolved placement weight: explicit config value, or auto-derived
    /// from capacity (`max(1, total / 1 GiB)`).
    weight: u32,
    /// Resolved device technology class (`Auto` → `Nvme` placeholder).
    tech: PoolTech,
    /// Health status byte (see [`PoolStatus::as_u8`]).
    status: AtomicU8,
    /// Role-consequence flag (ADR-0029 §D3); g2's health monitor drives
    /// it (wal pool Dead).
    write_degraded: AtomicBool,
    /// Per-pool health-monitor knobs (f1) — consumed by g2's
    /// `HealthMonitor` (thresholds, windows; the monitor's tick cadence
    /// comes from `detection_window_secs`).
    health: PoolHealthConfig,
    /// Total filesystem bytes at last capacity refresh.
    total_bytes: AtomicU64,
    /// Free filesystem bytes at last capacity refresh.
    free_bytes: AtomicU64,
}

impl StoragePool {
    /// Creates a pool with fully resolved values. Internal: construction
    /// happens in [`PoolRegistry::from_config`].
    // Clippy: 9 positional args; a config-bundle struct would hide the
    // resolved values this constructor pins (weight/tech/status/capacity),
    // and there is exactly one caller.
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: u32,
        name: String,
        role: PoolRole,
        root: PathBuf,
        weight: u32,
        tech: PoolTech,
        status: PoolStatus,
        capacity: PoolCapacity,
        health: PoolHealthConfig,
    ) -> Self {
        Self {
            id,
            name,
            role,
            root,
            weight,
            tech,
            status: AtomicU8::new(status.as_u8()),
            write_degraded: AtomicBool::new(false),
            health,
            total_bytes: AtomicU64::new(capacity.total_bytes),
            free_bytes: AtomicU64::new(capacity.free_bytes),
        }
    }

    /// Returns the stable pool id (config-order index).
    ///
    /// # Examples
    ///
    /// ```
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
    /// assert_eq!(registry.pools()[0].id(), 0);
    /// ```
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Returns the pool's configured name.
    ///
    /// # Examples
    ///
    /// ```
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
    /// assert_eq!(registry.pools()[0].name(), "data-0");
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the pool's role.
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
    /// assert_eq!(registry.pools()[0].role(), PoolRole::Data);
    /// ```
    pub fn role(&self) -> PoolRole {
        self.role
    }

    /// Returns the pool root directory.
    ///
    /// # Examples
    ///
    /// ```
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
    /// assert_eq!(registry.pools()[0].root(), tmp.path().join("pool-data"));
    /// ```
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the resolved placement weight.
    ///
    /// # Examples
    ///
    /// ```
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
    /// // The data pool explicit config weight (Some(1)) is honored.
    /// assert_eq!(registry.pools()[0].weight(), 1);
    /// ```
    pub fn weight(&self) -> u32 {
        self.weight
    }

    /// Returns the resolved device technology class.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolTech;
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
    /// // `Auto` resolves to the Phase A `Nvme` placeholder.
    /// assert_eq!(registry.pools()[0].tech(), PoolTech::Nvme);
    /// ```
    pub fn tech(&self) -> PoolTech {
        self.tech
    }

    /// Returns the pool's per-pool health-monitor knobs (f1) — consumed
    /// by g2's [`HealthMonitor`](crate::pool::health::HealthMonitor).
    ///
    /// # Examples
    ///
    /// ```
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
    /// assert_eq!(registry.pools()[0].health_config().detection_window_secs, 30);
    /// ```
    pub fn health_config(&self) -> PoolHealthConfig {
        self.health.clone()
    }

    /// Returns the pool's health status.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::{PoolRegistry, PoolStatus};
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
    /// assert_eq!(registry.pools()[0].status(), PoolStatus::Healthy);
    /// ```
    pub fn status(&self) -> PoolStatus {
        pool_status_from_u8(self.status.load(Ordering::Relaxed))
    }

    /// Returns whether the pool rejects new writes (role consequence,
    /// ADR-0029 §D3). Phase A: always `false`.
    ///
    /// # Examples
    ///
    /// ```
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
    /// assert!(!registry.pools()[0].write_degraded());
    /// ```
    pub fn write_degraded(&self) -> bool {
        self.write_degraded.load(Ordering::Relaxed)
    }

    /// Returns the filesystem's total bytes at the last capacity refresh.
    ///
    /// # Examples
    ///
    /// ```
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
    /// assert!(registry.pools()[0].total_bytes() > 0);
    /// ```
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Returns the filesystem's free bytes at the last capacity refresh.
    ///
    /// # Examples
    ///
    /// ```
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
    /// assert!(registry.pools()[0].free_bytes() > 0);
    /// ```
    pub fn free_bytes(&self) -> u64 {
        self.free_bytes.load(Ordering::Relaxed)
    }

    /// Updates the health status (Phase B drives this; Phase A only the
    /// startup-probe path sets `Degraded`).
    fn set_status(&self, status: PoolStatus) {
        self.status.store(status.as_u8(), Ordering::Relaxed);
    }

    /// Updates the `write_degraded` role-consequence flag.
    fn set_write_degraded(&self, write_degraded: bool) {
        self.write_degraded.store(write_degraded, Ordering::Relaxed);
    }

    /// Updates the capacity snapshot.
    fn set_capacity(&self, capacity: PoolCapacity) {
        self.total_bytes.store(capacity.total_bytes, Ordering::Relaxed);
        self.free_bytes.store(capacity.free_bytes, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for StoragePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Manual impl: the atomic fields (status, capacity) do not
        // implement Debug; render their current values instead.
        f.debug_struct("StoragePool")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("role", &self.role)
            .field("root", &self.root)
            .field("weight", &self.weight)
            .field("tech", &self.tech)
            .field("status", &self.status())
            .field("write_degraded", &self.write_degraded())
            .field("total_bytes", &self.total_bytes())
            .field("free_bytes", &self.free_bytes())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Startup probing + capacity
// ---------------------------------------------------------------------------

/// Probes a pool root: `create_dir_all` → write a `.probe-<uuid>` file →
/// fsync → read back → remove.
///
/// Any failure (uncreatable root, unwritable/read-only root, read-back
/// mismatch) surfaces as an `io::Error`; the caller resolves it against the
/// pool's `MissingRootPolicy`.
fn probe_root(root: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(root)?;
    let probe_path = root.join(format!(".probe-{}", uuid::Uuid::now_v7()));
    let result = (|| -> std::io::Result<()> {
        std::fs::write(&probe_path, POOL_PROBE_MARKER)?;
        // fsync the written probe so the test verifies durable writes, not
        // page-cache-only success.
        std::fs::File::open(&probe_path)?.sync_all()?;
        let read_back = std::fs::read(&probe_path)?;
        if read_back != POOL_PROBE_MARKER {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "probe read-back mismatch",
            ));
        }
        Ok(())
    })();
    // Best-effort cleanup — a failed probe must not leave litter behind.
    let _ = std::fs::remove_file(&probe_path);
    result
}

/// Returns the filesystem capacity of a pool root via `statvfs(2)`.
fn statvfs_capacity(root: &Path) -> std::io::Result<PoolCapacity> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;

        let c_root = std::ffi::CString::new(root.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "pool root contains a NUL byte")
        })?;
        // SAFETY: `vfs` is a valid out-parameter for `statvfs(2)`: writable,
        // correctly sized, zero-initialized memory. `c_root.as_ptr()` is a
        // NUL-terminated C string valid for the duration of the call.
        #[allow(unsafe_code)]
        let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: same invariants as above — `&mut vfs` is a valid out
        // pointer and `c_root.as_ptr()` a valid path argument.
        #[allow(unsafe_code)]
        let ret = unsafe { libc::statvfs(c_root.as_ptr(), &mut vfs) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(PoolCapacity {
            total_bytes: (vfs.f_frsize as u64).saturating_mul(vfs.f_blocks as u64),
            // `f_bavail` (blocks available to unprivileged processes), not
            // `f_bfree`: placement must see what a new segment could use.
            free_bytes: (vfs.f_frsize as u64).saturating_mul(vfs.f_bavail as u64),
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = root;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "statvfs capacity is only supported on Linux",
        ))
    }
}

/// Auto-derived placement weight from capacity: total bytes scaled to GiB
/// units, minimum 1 (ADR-0029 §D8; f2 spec: `max(1, total / 1 GiB)`).
fn auto_weight(total_bytes: u64) -> u32 {
    u32::try_from(total_bytes / GIB).unwrap_or(u32::MAX).max(1)
}

/// Resolves `PoolTech::Auto` to the Phase A placeholder (`Nvme`). Real
/// auto-detection lands in Phase B with the health monitor, where tech
/// first matters (accepted f2 deviation).
fn resolve_tech(tech: PoolTech) -> PoolTech {
    match tech {
        PoolTech::Auto => PoolTech::Nvme,
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Per-pool metrics
// ---------------------------------------------------------------------------

/// Per-pool Prometheus series, registered once at construction (the same
/// pattern the durability counters use, e.g. `hinted_handoff_*`).
///
/// `pub(crate)`: the g1 [`IoObserver`](crate::io::IoObserver) binds the
/// `io_errors` counter handle so recorded disk errors increment the same
/// series the node's metric registry renders.
#[derive(Clone)]
pub(crate) struct PoolMetrics {
    /// Pool id the series belong to (used to find the right series).
    pool_id: u32,
    /// `oceanfs_pool_status{pool_id, role}` — 0=Healthy 1=Degraded 2=Dead
    /// 3=Draining.
    status: Gauge,
    /// `oceanfs_pool_bytes_free{pool_id}`.
    bytes_free: Gauge,
    /// `oceanfs_pool_bytes_total{pool_id}`.
    bytes_total: Gauge,
    /// `oceanfs_pool_write_degraded{pool_id}` — 1 when the pool rejects
    /// new writes (wal pool Dead, ADR-0029 §D3; g2 drives it).
    write_degraded: Gauge,
    /// `oceanfs_pool_drain_state{pool_id}` — 0=Idle 1=Draining
    /// 2=Detachable (ADR-0036 D6 operator dashboard).
    drain_state: Gauge,
    /// `oceanfs_pool_drain_blocked_reason{pool_id}` — 1 while the pool's
    /// drain is blocked (no eligible target); 0 otherwise. The human
    /// reason text rides the admin pool-status JSON (a string label would
    /// churn Prometheus series per distinct reason).
    drain_blocked: Gauge,
    /// `oceanfs_pool_io_errors_total{pool_id}` — g1's `DiskIo` observer
    /// increments it via the bound handle (see `observe_into`).
    io_errors: Counter,
    /// `oceanfs_drain_dispatched_total{pool_id}` — sealed segments whose
    /// copy the drain mover dispatched OFF this pool this run
    /// (intra-node relocation to a sibling / cluster re-replication to
    /// another node; the d3/d4 mover workers report it, closing d4's
    /// recorded `oceanfs_drain_*` gap).
    drain_dispatched: Counter,
    /// `oceanfs_drain_released_total{pool_id}` — sealed segments whose
    /// source copy was released/unlinked from this pool (relocation
    /// unlink, cluster source-release, or a direct release that keeps RF
    /// satisfied without a new copy).
    drain_released: Counter,
    /// `oceanfs_drain_remaining{pool_id}` — sealed segments the node
    /// still holds on this pool (drain progress; 0 when `Detachable`).
    drain_remaining: Gauge,
}

impl PoolMetrics {
    fn new(pool: &StoragePool) -> Self {
        let pool_id = pool.id().to_string();
        let role = pool.role().as_str().to_string();
        let id_label = LabelSet::new(&[("pool_id", &pool_id)]);
        Self {
            pool_id: pool.id(),
            status: Gauge::new(
                "oceanfs_pool_status".into(),
                "Pool health status (0=Healthy 1=Degraded 2=Dead 3=Draining)".into(),
                LabelSet::new(&[("pool_id", &pool_id), ("role", &role)]),
            ),
            bytes_free: Gauge::new(
                "oceanfs_pool_bytes_free".into(),
                "Free bytes on the pool's filesystem".into(),
                id_label.clone(),
            ),
            bytes_total: Gauge::new(
                "oceanfs_pool_bytes_total".into(),
                "Total bytes on the pool's filesystem".into(),
                id_label.clone(),
            ),
            write_degraded: Gauge::new(
                "oceanfs_pool_write_degraded".into(),
                "Pool rejects new writes (wal pool Dead, ADR-0029 D3)".into(),
                id_label.clone(),
            ),
            drain_state: Gauge::new(
                "oceanfs_pool_drain_state".into(),
                "Pool drain lifecycle (0=Idle 1=Draining 2=Detachable)".into(),
                id_label.clone(),
            ),
            drain_blocked: Gauge::new(
                "oceanfs_pool_drain_blocked_reason".into(),
                "1 while the pool's drain is blocked (no eligible target); 0 otherwise".into(),
                id_label.clone(),
            ),
            io_errors: Counter::new(
                "oceanfs_pool_io_errors_total".into(),
                "I/O errors observed on the pool (g1 DiskIo observer increments it)".into(),
                LabelSet::new(&[("pool_id", &pool_id)]),
            ),
            drain_dispatched: Counter::new(
                "oceanfs_drain_dispatched_total".into(),
                "Segments whose copy the drain mover dispatched off this pool \
                 (intra-node relocation / cluster re-replication)".into(),
                id_label.clone(),
            ),
            drain_released: Counter::new(
                "oceanfs_drain_released_total".into(),
                "Segments whose source copy was released/unlinked from this pool".into(),
                id_label.clone(),
            ),
            drain_remaining: Gauge::new(
                "oceanfs_drain_remaining".into(),
                "Sealed segments the node still holds on this pool (drain progress; 0 = Detachable)"
                    .into(),
                id_label,
            ),
        }
    }

    /// The `oceanfs_pool_io_errors_total{pool_id}` counter handle.
    pub(crate) fn io_errors(&self) -> &Counter {
        &self.io_errors
    }
}

// ---------------------------------------------------------------------------
// PoolRegistry
// ---------------------------------------------------------------------------

/// The node's pool set: the lookup API placement and routing consume.
///
/// Built by [`PoolRegistry::from_config`] from the topology config.
/// Storage pools are mandatory (ADR-0031): the legacy implicit single
/// `data`-pool-at-`data_dir` mode was removed.
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
///
/// assert_eq!(registry.pools().len(), 4);
/// assert!(registry.pool_by_role(PoolRole::Data).is_some());
/// ```
pub struct PoolRegistry {
    /// The pool set. Reads dominate (placement + routing lookup per
    /// request), so an `RwLock` (perf guideline 7.2); the list only changes
    /// on runtime attach (f8).
    pools: RwLock<Vec<Arc<StoragePool>>>,
    /// Per-pool metric series. Reads are cold (registration at boot,
    /// capacity refresh on the maintenance tick); a new series is pushed
    /// under the write lock on runtime attach (f8) — a rare admin op
    /// (perf 7.1: the lock is held only for registration, never during
    /// placement reads).
    metrics: RwLock<Vec<PoolMetrics>>,
    /// The node's `MissingRootPolicy`: how a runtime attach's probe
    /// failure resolves (`Fatal` → `Err`, `Degraded` → pool registered as
    /// Degraded). Captured at construction from the topology config.
    missing_root_policy: MissingRootPolicy,
    /// The node's `data_dir`: a pool root must stay disjoint from it
    /// (checked at attach time too, not just at boot). Kept for the
    /// disjointness check only — the registry no longer creates an
    /// implicit pool there.
    data_dir: PathBuf,
    /// Operator-visible drain record per pool (ADR-0036 D6). Present only
    /// for pools whose drain lifecycle has been touched (`begin_drain` and
    /// onward); absence reads as [`DrainState::Idle`]. The pool's *status*
    /// stays the single source of truth for placement/health (`Draining`
    /// is encoded in the status atomic); this map carries the drain
    /// lifecycle (blocked reason, empty→`Detachable`) that status alone
    /// cannot express.
    ///
    /// # LOCK ORDER
    ///
    /// Never acquire `self.pools` while holding this lock: attach pushes a
    /// pool under the `pools` write lock and then releases it before
    /// inserting here; every drain mutation reads pool status first
    /// (without this lock held) and only then takes this write lock. The
    /// reverse order would deadlock against a concurrent attach.
    drain: RwLock<HashMap<u32, drain::DrainState>>,
    /// The node's resolved global `[storage.health]` defaults (f5 D4):
    /// runtime-attached pools resolve their own inline overrides over
    /// this, matching boot-time resolution.
    global_health: oceanfs_core::PoolHealthConfig,
    /// The drain mover mode per pool (d3 `IntraNode` vs d4 `Cluster`) —
    /// routes a `Draining` pool to exactly one mover. Short lock, never
    /// held together with `drain` or `pools`. Pools that never drained
    /// (or began through the plain d1 seam) read as `IntraNode`.
    drain_mode: RwLock<HashMap<u32, drain::DrainMode>>,
}

impl PoolRegistry {
    /// Builds the registry from the topology config, probing every root.
    ///
    /// - Re-validates the config via [`StorageConfig::validate`] (mandatory
    ///   roles, one-root-per-pool, weights, health knobs) — an empty or
    ///   role-incomplete pool list is refused here (ADR-0031).
    /// - One `StoragePool` per `PoolConfig`, ids in config order (0..n).
    ///   Probe failure resolves against `MissingRootPolicy`: `Fatal` →
    ///   `Err`, `Degraded` → pool registered with status `Degraded`
    ///   (Phase A: treated as Healthy by consumers until Phase B).
    /// - Weight resolution: explicit config weight wins; `None` →
    ///   `max(1, total / 1 GiB)` from the probe-time capacity snapshot.
    ///
    /// d5 (ADR-0036 D8): a node that has detached pools boots them through
    /// [`PoolRegistry::from_config_skipping`], which applies the persistent
    /// removed-pool overlay (`config − removed`). This plain entry point
    /// registers every configured pool (no overlay).
    ///
    /// # Errors
    ///
    /// Returns a human-readable message when the config fails validation or
    /// a root probe fails under the `Fatal` policy.
    ///
    /// # Examples
    ///
    /// ```
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
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir);
    /// assert!(registry.is_ok());
    /// ```
    pub fn from_config(storage: &StorageConfig, data_dir: &Path) -> Result<PoolRegistry, String> {
        // d5 (ADR-0036 D8): the persistent removed-pool overlay (`config −
        // removed`) is applied by callers that pass a removed set to
        // `from_config_skipping`; this plain entry point registers every
        // configured pool.
        Self::from_config_skipping(storage, data_dir, &[])
    }

    /// Builds the registry from the topology config minus the **removed
    /// pools** (d5, ADR-0036 D8) — the persistent detach overlay.
    ///
    /// Same contract as [`PoolRegistry::from_config`] with two deltas:
    ///
    /// - A config-declared pool is **not registered** when a removed record
    ///   matches its name **and** root (`config − removed`). Ids stay tied
    ///   to the *original* config order — a removed pool's slot simply goes
    ///   vacant — so the pools that remain keep the ids their sealed
    ///   segments are durably stamped with (a dense renumber would silently
    ///   re-point every surviving segment's `pool_id`).
    /// - The overlay is applied **before** role/cardinality validation of
    ///   the remaining set: a config whose every `data` pool is removed
    ///   refuses boot here (ADR-0031 on the post-overlay set). The
    ///   documented retirement sequence is detach → `leave(None)`; a node
    ///   that drained + detached its last data pool must never be restarted
    ///   expecting zero data pools.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message when the config fails validation,
    /// a root probe fails under the `Fatal` policy, or **no `data` pool
    /// remains after the overlay is applied**.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::{PoolRegistry, PoolRemovedRecord};
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let root = |name: &str| tmp.path().join(name);
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: root("pool-data"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "data-1".into(), role: oceanfs_core::PoolRole::Data, root: root("pool-data-1"), weight: Some(1), tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: root("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: root("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: root("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let removed = vec![PoolRemovedRecord::new(
    ///     "data-0".into(),
    ///     tmp.path().join("pool-data"),
    /// )];
    /// let registry =
    ///     PoolRegistry::from_config_skipping(&storage, &data_dir, &removed).expect("registry");
    /// // "data-0" is suppressed; "data-1" keeps its original id (1).
    /// assert_eq!(registry.data_pools().len(), 1);
    /// assert_eq!(registry.data_pools()[0].name(), "data-1");
    /// assert_eq!(registry.data_pools()[0].id(), 1);
    /// ```
    pub fn from_config_skipping(
        storage: &StorageConfig,
        data_dir: &Path,
        removed: &[removed::PoolRemovedRecord],
    ) -> Result<PoolRegistry, String> {
        // Validate the FULL config first (mandatory roles, one root per
        // pool, weights, health knobs) — the overlay only ever suppresses
        // `data` pools (wal/metadata/hints detach is refused by
        // `PoolRegistry::detach`), so the pinned-role checks run unchanged.
        storage.validate(data_dir).map_err(|e| format!("invalid storage config: {e}"))?;

        let mut pools: Vec<Arc<StoragePool>> = Vec::with_capacity(storage.pools.len());
        let mut metrics: Vec<PoolMetrics> = Vec::with_capacity(storage.pools.len());

        // ADR-0031 D1: pools are mandatory — `validate` already refused an
        // empty or role-incomplete list, so every pool here is a configured
        // one (no implicit legacy pool, no `data_dir` probing).
        for (index, config) in storage.pools.iter().enumerate() {
            // d5 (ADR-0036 D8): a removed record matching name AND root
            // suppresses the pool. The id stays the ORIGINAL config index
            // (the removed slot goes vacant), so surviving pools keep the
            // ids their sealed segments are durably stamped with.
            if removed.iter().any(|record| record.matches(&config.name, &config.root)) {
                tracing::info!(
                    pool = %config.name,
                    root = %config.root.display(),
                    "storage pool suppressed by the removed-pool overlay (ADR-0036 D8)"
                );
                continue;
            }
            let id = index as u32;
            // Role-aware missing-root policy: the hints role is
            // degraded-by-design. Hints are delivery intent (debt), not
            // data (ADR-0029 §D3) — a missing hints root must not refuse
            // node startup, because the honest-admission gate (f0) is the
            // enforcement point, not the boot. Every other role follows
            // the configured global policy.
            //
            // Note: the boot probe still `create_dir_all`s a creatable
            // missing root (unchanged for all roles), so Degraded applies
            // when creation/probe FAILS — uncreatable parent, read-only or
            // detached/unreadable mount, device errors — not when the
            // directory is merely absent but creatable.
            let effective_policy = if config.role == PoolRole::Hints {
                MissingRootPolicy::Degraded
            } else {
                storage.missing_root_policy
            };
            let (status, capacity) = match probe_root(&config.root) {
                Ok(()) => (PoolStatus::Healthy, statvfs_capacity(&config.root).unwrap_or_default()),
                Err(e) if effective_policy == MissingRootPolicy::Degraded => {
                    tracing::warn!(
                        pool = %config.name,
                        role = config.role.as_str(),
                        error = %e,
                        "pool root probe failed; registering pool as Degraded \
                         (missing-root policy)"
                    );
                    (PoolStatus::Degraded, PoolCapacity::default())
                }
                Err(e) => {
                    return Err(format!(
                        "pool '{}' root '{}' probe failed: {e}",
                        config.name,
                        config.root.display()
                    ));
                }
            };
            let weight = match config.weight {
                Some(weight) => weight,
                None => auto_weight(capacity.total_bytes),
            };
            let pool = Arc::new(StoragePool::new(
                id,
                config.name.clone(),
                config.role,
                config.root.clone(),
                weight,
                resolve_tech(config.tech),
                status,
                capacity,
                storage.resolved_pool_health(config),
            ));
            metrics.push(PoolMetrics::new(&pool));
            pools.push(pool);
        }

        // ADR-0036 D8 (post-overlay set): a config whose every `data` pool
        // is removed refuses boot. This is the documented retirement
        // sequence guard — detach → leave; never restart a fully-detached
        // node expecting zero data pools (ADR-0031).
        if !pools.iter().any(|pool| pool.role() == PoolRole::Data) {
            return Err(
                "no 'data' pool remains after applying the removed-pool overlay (ADR-0036 D8); \
                 a node that drained and detached its last data pool must not restart — the \
                 retirement sequence is detach then leave"
                    .to_string(),
            );
        }

        // Publish the initial capacity/status to the metric series.
        for pool in &pools {
            if let Some(metric) = metrics.iter().find(|metric| metric.pool_id == pool.id()) {
                metric.status.set(pool.status().as_u8() as u64);
                metric.bytes_free.set(pool.free_bytes());
                metric.bytes_total.set(pool.total_bytes());
            }
        }

        Ok(PoolRegistry {
            pools: RwLock::new(pools),
            metrics: RwLock::new(metrics),
            missing_root_policy: storage.missing_root_policy,
            // f5 D4: resolve the global defaults once; runtime attach
            // resolves a pool's inline overrides over this.
            global_health: storage.health.resolve(&oceanfs_core::PoolHealthConfig::default()),
            data_dir: data_dir.to_path_buf(),
            // Every registered pool starts its drain lifecycle Idle.
            drain: RwLock::new(HashMap::new()),
            drain_mode: RwLock::new(HashMap::new()),
        })
    }

    /// Returns a snapshot copy of all pools (config order).
    ///
    /// The returned `Arc` handles share the pools' live state, so capacity
    /// and status read current values without any further locking.
    ///
    /// # Examples
    ///
    /// ```
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
    /// assert_eq!(registry.pools().len(), 4);
    /// ```
    pub fn pools(&self) -> Vec<Arc<StoragePool>> {
        self.pools.read().clone()
    }

    /// Returns the pool with the given id, if registered.
    ///
    /// # Examples
    ///
    /// ```
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
    /// assert!(registry.pool_by_id(0).is_some());
    /// assert!(registry.pool_by_id(99).is_none());
    /// ```
    pub fn pool_by_id(&self, id: u32) -> Option<Arc<StoragePool>> {
        self.pools.read().iter().find(|pool| pool.id() == id).cloned()
    }

    /// Returns the first pool with the given role, if any.
    ///
    /// `wal`/`metadata`/`hints` are cardinality-1 (validated at
    /// construction), so the first match is the only one; `data` pools
    /// should be enumerated via [`PoolRegistry::data_pools`].
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
    /// assert!(registry.pool_by_role(PoolRole::Data).is_some());
    /// assert!(registry.pool_by_role(PoolRole::Hints).is_some());
    /// ```
    pub fn pool_by_role(&self, role: PoolRole) -> Option<Arc<StoragePool>> {
        self.pools.read().iter().find(|pool| pool.role() == role).cloned()
    }

    /// Returns all `data`-role pools in stable config order.
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
    /// let data_pools = registry.data_pools();
    /// assert_eq!(data_pools.len(), 1);
    /// assert_eq!(data_pools[0].role(), PoolRole::Data);
    /// ```
    pub fn data_pools(&self) -> Vec<Arc<StoragePool>> {
        let pools = self.pools.read();
        pools.iter().filter(|pool| pool.role() == PoolRole::Data).cloned().collect()
    }

    /// Re-stats each pool's filesystem capacity (called by the node's
    /// periodic maintenance task, not per request).
    ///
    /// Snapshot-first design (perf guideline 7.2): the pool list is cloned
    /// under a short read lock, then `statvfs` runs **outside** the lock —
    /// lookups never block on disk I/O. Gauges and pool atomics are updated
    /// together after each stat.
    ///
    /// # Examples
    ///
    /// ```
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
    /// registry.refresh_capacity();
    /// assert!(registry.pools()[0].total_bytes() > 0);
    /// ```
    pub fn refresh_capacity(&self) {
        for pool in self.pools() {
            if let Ok(capacity) = statvfs_capacity(pool.root()) {
                pool.set_capacity(capacity);
                if let Some(metric) = self.metrics_for(pool.id()) {
                    metric.bytes_free.set(capacity.free_bytes);
                    metric.bytes_total.set(capacity.total_bytes);
                }
            }
        }
    }

    /// Sets a pool's health status and its `oceanfs_pool_status` gauge.
    ///
    /// Phase B's health monitor drives this; Phase A never calls it (all
    /// pools are `Healthy`, or `Degraded` from the startup probe).
    /// Unknown ids are ignored (no-op).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::{PoolRegistry, PoolStatus};
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
    /// registry.set_status(0, PoolStatus::Degraded);
    /// assert_eq!(registry.pool_by_id(0).expect("pool").status(), PoolStatus::Degraded);
    /// ```
    pub fn set_status(&self, id: u32, status: PoolStatus) {
        if let Some(pool) = self.pool_by_id(id) {
            pool.set_status(status);
            if let Some(metric) = self.metrics_for(id) {
                metric.status.set(status.as_u8() as u64);
            }
        }
    }

    /// Sets a pool's `write_degraded` role-consequence flag.
    ///
    /// Phase B drives this; Phase A never calls it (always `false`).
    /// Unknown ids are ignored (no-op).
    ///
    /// # Examples
    ///
    /// ```
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
    /// registry.set_write_degraded(0, true);
    /// assert!(registry.pool_by_id(0).expect("pool").write_degraded());
    /// ```
    pub fn set_write_degraded(&self, id: u32, write_degraded: bool) {
        if let Some(pool) = self.pool_by_id(id) {
            pool.set_write_degraded(write_degraded);
            if let Some(metric) = self.metrics_for(id) {
                metric.write_degraded.set(u64::from(write_degraded));
            }
        }
    }

    /// Whether this node can serve requests at all (g6, ADR-0029 §D3):
    /// the metadata pool is NOT Dead. A node whose metadata pool is Dead
    /// has lost its object index — it can serve neither reads nor writes
    /// (g2's `node_unavailable` consequence). The registry always has a
    /// metadata pool (ADR-0031); the `unwrap_or(true)` arm guards the
    /// attach-less edge only.
    ///
    /// This is the SINGLE availability derivation the read and write
    /// paths consult (a node-level gate on top of the peer-side manifest
    /// routing in `oceanfs-node::routing_cache`). It reads the registry
    /// — the one source of truth — rather than a separately-maintained
    /// flag.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::{PoolRegistry, PoolStatus};
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
    /// assert!(registry.node_serves_requests(), "healthy metadata pool → available");
    /// ```
    pub fn node_serves_requests(&self) -> bool {
        self.pool_by_role(PoolRole::Metadata)
            .map(|pool| pool.status() != PoolStatus::Dead)
            .unwrap_or(true)
    }

    /// Whether this node can accept NEW writes (g6, ADR-0029 §D3): the
    /// node serves requests AND no wal pool is `write_degraded` (a Dead
    /// wal pool cannot journal — new writes must be rejected, not
    /// silently lost). The registry always has a wal pool (ADR-0031); the
    /// `unwrap_or(true)` arm guards the attach-less edge only.
    ///
    /// # Examples
    ///
    /// ```
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
    /// assert!(registry.accepts_writes(), "healthy wal pool → accepts writes");
    /// ```
    pub fn accepts_writes(&self) -> bool {
        if !self.node_serves_requests() {
            return false;
        }
        self.pool_by_role(PoolRole::Wal).map(|pool| !pool.write_degraded()).unwrap_or(true)
    }

    /// Overrides a pool's capacity snapshot and its metrics.
    ///
    /// The node's maintenance task normally drives capacity via
    /// [`PoolRegistry::refresh_capacity`] (`statvfs`). This setter exists
    /// for the paths where statvfs is not the source of truth: drain /
    /// rebalance accounting and runtime pool attach (Phase C / f8), and
    /// integration tests that simulate capacity evolution. Unknown ids are
    /// ignored (no-op).
    ///
    /// # Examples
    ///
    /// ```
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
    /// registry.set_pool_capacity(0, 100 * 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024);
    /// let pool = registry.pool_by_id(0).expect("pool");
    /// assert_eq!(pool.total_bytes(), 100 * 1024 * 1024 * 1024);
    /// assert_eq!(pool.free_bytes(), 10 * 1024 * 1024 * 1024);
    /// ```
    pub fn set_pool_capacity(&self, id: u32, total_bytes: u64, free_bytes: u64) {
        if let Some(pool) = self.pool_by_id(id) {
            pool.set_capacity(PoolCapacity { total_bytes, free_bytes });
            if let Some(metric) = self.metrics_for(id) {
                metric.bytes_free.set(free_bytes);
                metric.bytes_total.set(total_bytes);
            }
        }
    }

    /// Attaches a new pool at runtime (ADR-0029 §D8, f8) — no restart.
    ///
    /// The admin path calls this for `POST /admin/pools`: validate the
    /// single pool against the LIVE registry (rules: non-empty unique
    /// name, absolute unique root, role cardinality, weight/health knobs,
    /// root disjoint from `data_dir`), probe the root (the
    /// node's `MissingRootPolicy` decides `Fatal` → `Err` vs `Degraded`),
    /// resolve weight/tech, and register under the registry's write lock
    /// (perf 7.1: a short critical section held only for registration —
    /// never during placement reads, which take the read lock). The new
    /// pool gets the lowest unregistered id (d5: detach frees a pool's id,
    /// so ids are no longer dense; on a registry with no holes this is the
    /// old sequential next-id) and is visible to placement
    /// immediately (placement reads the registry snapshot per selection).
    ///
    /// # Errors
    ///
    /// Returns a human-readable message for validation failures
    /// ("duplicate pool name"/"duplicate pool root"/role cardinality),
    /// probe failures under the `Fatal` policy, or registration errors.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::{PoolRole, StoragePoolConfig};
    /// use oceanfs_storage::PoolRegistry;
    ///
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let attach_root = tmp.path().join("nvme-attach");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     health: Default::default(),
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
    /// let id = registry
    ///     .attach(StoragePoolConfig {
    ///         name: "fast-nvme-0".into(),
    ///         role: PoolRole::Data,
    ///         root: attach_root,
    ///         weight: None,
    ///         tech: oceanfs_core::PoolTech::Auto,
    ///         health: Default::default(),
    ///     })
    ///     .expect("attach must succeed");
    /// assert_eq!(registry.pool_count(), 5);
    /// assert!(registry.pool_by_id(id).is_some());
    /// ```
    pub fn attach(&self, pool: oceanfs_core::StoragePoolConfig) -> Result<u32, String> {
        // ---- 1. Validate the single pool against the live registry. ----
        // Fast-fail under a read lock BEFORE the probe I/O; the same
        // checks re-run under the write lock at registration (step 3) —
        // a concurrent attach between the two locks would otherwise pass
        // both validations and both register (the id stays race-free,
        // but duplicates would slip through). The admin path is
        // single-operator; the re-check is cheap (no I/O).
        {
            let existing = self.pools.read();
            Self::validate_attach(self, &pool, &existing)?;
        }

        // ---- 2. Probe the root (outside any lock — filesystem I/O). ----
        let (status, capacity) = match probe_root(&pool.root) {
            Ok(()) => (PoolStatus::Healthy, statvfs_capacity(&pool.root).unwrap_or_default()),
            Err(e) if self.missing_root_policy == MissingRootPolicy::Degraded => {
                tracing::warn!(
                    pool = %pool.name,
                    error = %e,
                    "attached pool root probe failed; registering pool as Degraded \
                     (Phase A: treated as Healthy by consumers until Phase B)"
                );
                (PoolStatus::Degraded, PoolCapacity::default())
            }
            Err(e) => {
                return Err(format!(
                    "pool '{}' root '{}' probe failed: {e}",
                    pool.name,
                    pool.root.display()
                ));
            }
        };
        let weight = match pool.weight {
            Some(weight) => weight,
            None => auto_weight(capacity.total_bytes),
        };

        // ---- 3. Register under the write lock (short critical section). ----
        // The pool + metric series are constructed OUTSIDE the lock (the
        // only work inside is the pure push — perf 7.1); the id is the
        // LOWEST UNUSED id computed under the lock, so concurrent attaches
        // cannot collide and a freed slot (d5 detach) is reused.
        let tech = resolve_tech(pool.tech);
        let name = pool.name.clone();
        let role = pool.role;
        let root = pool.root.clone();
        // f5 D4: runtime attach resolves the pool's inline overrides over
        // the node's global `[storage.health]` defaults.
        let health = pool.health.resolve(&self.global_health);
        let id = {
            let mut pools = self.pools.write();
            // Re-validate under the write lock (TOCTOU: a concurrent
            // attach may have registered a duplicate since step 1).
            Self::validate_attach(self, &pool, &pools)?;
            // The lowest id not already registered (d5): detach removes a
            // pool, so ids are no longer guaranteed dense — `pools.len()`
            // could collide with a surviving pool's id. On a registry with
            // no holes this is exactly `pools.len()` (the old sequential
            // scheme), so a re-attach after a detach round-trips back to
            // the freed slot (which matches the next boot's config order
            // once the removed record is cleared).
            let mut id = 0u32;
            while pools.iter().any(|pool| pool.id() == id) {
                id += 1;
            }
            let registered = Arc::new(StoragePool::new(
                id, name, role, root, weight, tech, status, capacity, health,
            ));
            // Publish the initial capacity/status to the metric series.
            let metric = PoolMetrics::new(&registered);
            metric.status.set(status.as_u8() as u64);
            metric.bytes_free.set(capacity.free_bytes);
            metric.bytes_total.set(capacity.total_bytes);
            self.metrics.write().push(metric);
            pools.push(registered);
            id
        };
        // The attached pool starts its drain lifecycle Idle. Taken after
        // the `pools` write lock is released (see the LOCK ORDER note on
        // `PoolRegistry::drain`) — a new data pool is never draining.
        self.drain.write().insert(id, drain::DrainState::Idle);

        tracing::info!(
            pool = %pool.name,
            pool_id = id,
            role = %pool.role.as_str(),
            root = %pool.root.display(),
            "storage pool attached at runtime"
        );
        Ok(id)
    }

    /// Validates a single pool definition against a registry snapshot:
    /// non-empty unique name, absolute unique root, role cardinality,
    /// weight/health knobs, root disjoint from `data_dir`. Shared by the
    /// attach read-lock fast-fail and the write-lock TOCTOU re-check.
    fn validate_attach(
        &self,
        pool: &oceanfs_core::StoragePoolConfig,
        existing: &[Arc<crate::pool::StoragePool>],
    ) -> Result<(), String> {
        if pool.name.trim().is_empty() {
            return Err("pool name must be non-empty".to_string());
        }
        if !pool.root.is_absolute() {
            return Err(format!(
                "pool '{}' root must be an absolute path, got '{}'",
                pool.name,
                pool.root.display()
            ));
        }
        for current in existing.iter() {
            if current.name() == pool.name {
                return Err(format!("duplicate pool name: '{}'", pool.name));
            }
            if current.root() == pool.root {
                return Err(format!("duplicate pool root: '{}'", pool.root.display()));
            }
        }
        // Role cardinality: wal/metadata/hints are at most one each.
        if matches!(pool.role, PoolRole::Wal | PoolRole::Metadata | PoolRole::Hints)
            && existing.iter().any(|current| current.role() == pool.role)
        {
            return Err(format!("at most one '{}' pool is allowed per node", pool.role.as_str()));
        }
        if let Some(weight) = pool.weight {
            if weight == 0 {
                return Err(format!("pool '{}' weight must be > 0, got 0", pool.name));
            }
        }
        if pool.health.monitor_tick_interval_secs.is_some()
            || pool.health.event_capacity.is_some()
            || pool.health.hints_probe_divisor.is_some()
        {
            return Err(format!(
                "pool '{}' health must not set monitor-level fields \
                 (monitor_tick_interval_secs / event_capacity / hints_probe_divisor); \
                 set them under [storage.health]",
                pool.name
            ));
        }
        let health = pool.health.resolve(&self.global_health);
        if !(health.error_rate_threshold > 0.0 && health.error_rate_threshold < 1.0) {
            return Err(format!(
                "pool '{}' health.error_rate_threshold must be in (0, 1), got {}",
                pool.name, health.error_rate_threshold
            ));
        }
        if health.trend_window_secs == 0
            || health.detection_window_secs == 0
            || health.recovery_window_secs == 0
        {
            return Err(format!(
                "pool '{}' health windows (trend/detection/recovery) must all be > 0",
                pool.name
            ));
        }
        if !health.trend_doubling_factor.is_finite() || health.trend_doubling_factor <= 1.0 {
            return Err(format!(
                "pool '{}' health.trend_doubling_factor must be > 1.0, got {}",
                pool.name, health.trend_doubling_factor
            ));
        }
        if health.trend_min_windows < 3 {
            return Err(format!(
                "pool '{}' health.trend_min_windows must be >= 3, got {}",
                pool.name, health.trend_min_windows
            ));
        }
        if health.history_max_windows < 4 {
            return Err(format!(
                "pool '{}' health.history_max_windows must be >= 4, got {}",
                pool.name, health.history_max_windows
            ));
        }
        // The pool root must stay disjoint from the node's data_dir.
        let overlaps = pool.root == self.data_dir
            || pool.root.starts_with(&self.data_dir)
            || self.data_dir.starts_with(&pool.root);
        if overlaps {
            return Err(format!(
                "pool '{}' root '{}' overlaps the legacy data_dir '{}'; \
                 pool roots must be disjoint from data_dir",
                pool.name,
                pool.root.display(),
                self.data_dir.display()
            ));
        }
        Ok(())
    }

    /// The number of registered pools (admin/observability).
    ///
    /// # Examples
    ///
    /// ```
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
    /// assert_eq!(registry.pool_count(), 4);
    /// ```
    pub fn pool_count(&self) -> usize {
        self.pools.read().len()
    }

    /// Registers every per-pool metric series with the node's registry.
    ///
    /// Called once at startup (the node's composition root), after
    /// `from_config` — the same registration pattern the durability
    /// counters use.
    ///
    /// # Examples
    ///
    /// ```
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
    /// // No-op registrar: registration must not panic on any registry.
    /// struct Noop;
    /// impl oceanfs_core::MetricRegistrar for Noop {
    ///     fn register_counter(&self, _: oceanfs_core::Counter) {}
    ///     fn register_gauge(&self, _: oceanfs_core::Gauge) {}
    ///     fn register_histogram(&self, _: std::sync::Arc<oceanfs_core::Histogram>) {}
    /// }
    /// registry.register_metrics(&Noop);
    /// ```
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        for metric in self.metrics.read().iter() {
            registrar.register_gauge(metric.status.clone());
            registrar.register_gauge(metric.bytes_free.clone());
            registrar.register_gauge(metric.bytes_total.clone());
            registrar.register_gauge(metric.write_degraded.clone());
            registrar.register_gauge(metric.drain_state.clone());
            registrar.register_gauge(metric.drain_blocked.clone());
            registrar.register_counter(metric.io_errors.clone());
            registrar.register_counter(metric.drain_dispatched.clone());
            registrar.register_counter(metric.drain_released.clone());
            registrar.register_gauge(metric.drain_remaining.clone());
        }
    }

    /// Returns the metric series for a pool id (linear scan; pool counts
    /// are 5–20, and this is a cold path).
    fn metrics_for(&self, pool_id: u32) -> Option<PoolMetrics> {
        self.metrics.read().iter().find(|metric| metric.pool_id == pool_id).cloned()
    }

    /// Returns the pool's `oceanfs_pool_io_errors_total{pool_id}` counter
    /// handle, if the pool has a metric series (g1: the
    /// [`IoObserver`](crate::io::IoObserver)
    /// binds it so recorded disk errors increment the same series the
    /// node's registry renders).
    ///
    /// # Examples
    ///
    /// ```
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
    /// let counter = registry.io_error_counter(0).expect("pool 0 series");
    /// counter.inc();
    /// assert_eq!(counter.get(), 1);
    /// ```
    pub fn io_error_counter(&self, pool_id: u32) -> Option<Counter> {
        self.metrics_for(pool_id).map(|metric| metric.io_errors().clone())
    }

    /// Registers every pool's signal state with the g1
    /// [`IoObserver`](crate::io::IoObserver),
    /// binding each pool's `oceanfs_pool_io_errors_total` counter.
    ///
    /// The node composition root calls this once after constructing the
    /// observer, so the segment I/O path can record per-pool signals
    /// immediately (ADR-0029 §D3). Runtime-attached pools (f8) are
    /// registered by the attach hook.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::IoObserver;
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
    /// let observer = IoObserver::new();
    /// registry.observe_into(&observer);
    /// assert!(observer.snapshot(0).is_some());
    /// ```
    pub fn observe_into(&self, observer: &crate::io::IoObserver) {
        for pool in self.pools() {
            let counter = self.io_error_counter(pool.id());
            observer.register_pool(pool.id(), counter);
        }
    }
}

impl std::fmt::Debug for PoolRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Manual impl: the metrics vec holds Gauge/Counter (Debug), but
        // rendering the pool list only is the useful surface.
        f.debug_struct("PoolRegistry")
            .field("pools", &self.pools())
            .field("metric_series", &self.metrics.read().len())
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    // NOTE: the storage-pool definition type is exported from the core
    // facade as `StoragePoolConfig` (f1 naming deviation — `PoolConfig`
    // already names the active-segment-pool config).
    use oceanfs_core::StoragePoolConfig;

    use super::*;

    /// A tempdir whose `data/` subdir is the node data_dir and whose other
    /// subdirs are pool roots (siblings, so the disjointness rule holds).
    fn layout() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        (tmp, data_dir)
    }

    fn pool(name: &str, role: PoolRole, root: &Path, weight: Option<u32>) -> StoragePoolConfig {
        StoragePoolConfig {
            name: name.to_string(),
            role,
            root: root.to_path_buf(),
            weight,
            tech: PoolTech::Auto,
            health: Default::default(),
        }
    }

    /// The full role-complete topology (ADR-0031): data×2, wal, metadata,
    /// hints — with sibling roots.
    fn full_pool_config(tmp: &Path) -> (StorageConfig, [PathBuf; 5]) {
        let roots = [
            tmp.join("nvme0"),
            tmp.join("nvme1"),
            tmp.join("optane0"),
            tmp.join("optane1"),
            tmp.join("hints0"),
        ];
        let storage = StorageConfig {
            pools: vec![
                pool("fast-nvme-0", PoolRole::Data, &roots[0], None),
                pool("fast-nvme-1", PoolRole::Data, &roots[1], Some(3)),
                pool("journal", PoolRole::Wal, &roots[2], None),
                pool("meta", PoolRole::Metadata, &roots[3], None),
                pool("hints", PoolRole::Hints, &roots[4], None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        (storage, roots)
    }

    /// A root that can never be created: its parent path is a file.
    fn uncreatable_root(tmp: &Path) -> PathBuf {
        let blocker = tmp.join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        blocker.join("pool-root")
    }

    // -- Construction --

    /// ADR-0031 D1: an empty pool list is refused at construction with the
    /// role-listing error — no implicit legacy pool is synthesized.
    #[test]
    fn empty_pools_refused_at_from_config() {
        let (tmp, data_dir) = layout();
        let err = PoolRegistry::from_config(&StorageConfig::default(), &data_dir).unwrap_err();
        assert!(err.contains("'data'"), "message: {err}");
        assert!(err.contains("'wal'"), "message: {err}");
        assert!(err.contains("'metadata'"), "message: {err}");
        assert!(err.contains("'hints'"), "message: {err}");
        drop(tmp);
    }

    #[test]
    fn explicit_mode_assigns_ids_in_config_order() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        let pools = registry.pools();
        assert_eq!(pools.len(), 5);
        let ids: Vec<u32> = pools.iter().map(|p| p.id()).collect();
        assert_eq!(ids, vec![0, 1, 2, 3, 4]);
        assert_eq!(pools[0].name(), "fast-nvme-0");
        assert_eq!(pools[2].name(), "journal");
        assert_eq!(pools[2].role(), PoolRole::Wal);
        assert_eq!(pools[3].role(), PoolRole::Metadata);
        assert_eq!(pools[4].role(), PoolRole::Hints);
        drop(tmp);
    }

    #[test]
    fn from_config_revalidates_via_storage_config_validate() {
        let (tmp, data_dir) = layout();
        // Two wal pools — rejected by StorageConfig::validate at
        // construction (f1 cardinality rule re-enforced here).
        let storage = StorageConfig {
            pools: vec![
                pool("journal-a", PoolRole::Wal, &tmp.path().join("wal-a"), None),
                pool("journal-b", PoolRole::Wal, &tmp.path().join("wal-b"), None),
                pool("data-a", PoolRole::Data, &tmp.path().join("data-a"), None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = PoolRegistry::from_config(&storage, &data_dir).unwrap_err();
        assert!(err.contains("wal"), "message: {err}");
        drop(tmp);
    }

    #[test]
    fn probe_creates_roots_and_leaves_no_probe_files() {
        let (tmp, data_dir) = layout();
        let (storage, roots) = full_pool_config(tmp.path());
        PoolRegistry::from_config(&storage, &data_dir).unwrap();

        for root in &roots {
            assert!(root.exists(), "root {root:?} must be created by the probe");
            let leftovers: Vec<_> = std::fs::read_dir(root)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.file_name().to_string_lossy().starts_with(".probe-"))
                .collect();
            assert!(leftovers.is_empty(), "probe files must be cleaned up in {root:?}");
        }
        drop(tmp);
    }

    #[test]
    fn missing_root_with_fatal_policy_fails_startup() {
        let (tmp, data_dir) = layout();
        let root = uncreatable_root(tmp.path());
        let storage = StorageConfig {
            pools: vec![
                pool("doomed", PoolRole::Data, &root, None),
                pool("journal", PoolRole::Wal, &tmp.path().join("optane0"), None),
                pool("meta", PoolRole::Metadata, &tmp.path().join("optane1"), None),
                pool("hints", PoolRole::Hints, &tmp.path().join("hints0"), None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = PoolRegistry::from_config(&storage, &data_dir).unwrap_err();
        assert!(err.contains("doomed"), "message: {err}");
        drop(tmp);
    }

    #[test]
    fn missing_root_with_degraded_policy_registers_degraded_pool() {
        let (tmp, data_dir) = layout();
        let root = uncreatable_root(tmp.path());
        let storage = StorageConfig {
            pools: vec![
                pool("degraded-pool", PoolRole::Data, &root, None),
                pool("journal", PoolRole::Wal, &tmp.path().join("optane0"), None),
                pool("meta", PoolRole::Metadata, &tmp.path().join("optane1"), None),
                pool("hints", PoolRole::Hints, &tmp.path().join("hints0"), None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Degraded,
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        let pool = registry.pool_by_id(0).expect("pool");
        assert_eq!(pool.status(), PoolStatus::Degraded);
        // Failed statvfs → zero capacity → auto weight floors at 1.
        assert_eq!(pool.weight(), 1);
        assert_eq!(pool.total_bytes(), 0);
        assert_eq!(pool.free_bytes(), 0);
        drop(tmp);
    }

    #[test]
    fn missing_hints_root_boots_degraded_even_under_fatal_policy() {
        // f0: hints are debt, not data — the gate enforces honesty, not
        // the boot. A missing hints root must degrade, never refuse.
        let (tmp, data_dir) = layout();
        let hints_root = uncreatable_root(tmp.path());
        let storage = StorageConfig {
            pools: vec![
                pool("data-0", PoolRole::Data, &tmp.path().join("nvme0"), None),
                pool("journal", PoolRole::Wal, &tmp.path().join("optane0"), None),
                pool("meta", PoolRole::Metadata, &tmp.path().join("optane1"), None),
                pool("hints-0", PoolRole::Hints, &hints_root, None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir)
            .expect("the hints role degrades instead of refusing boot");

        let hints = registry.pool_by_role(PoolRole::Hints).expect("hints pool");
        assert_eq!(hints.status(), PoolStatus::Degraded);
        // The fatal policy still applies to every other role.
        let data = registry.data_pools()[0].clone();
        assert_eq!(data.status(), PoolStatus::Healthy);
        drop(tmp);
    }

    // -- Weight + tech resolution --

    #[test]
    fn explicit_weight_wins_over_auto() {
        let (tmp, data_dir) = layout();
        let (storage, roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        // pool 0: weight None → auto; pool 1: explicit Some(3).
        assert_eq!(registry.pool_by_id(1).unwrap().weight(), 3);
        let auto = registry.pool_by_id(0).unwrap();
        let capacity = statvfs_capacity(&roots[0]).unwrap();
        assert_eq!(auto.weight(), auto_weight(capacity.total_bytes));
        drop(tmp);
    }

    #[test]
    fn auto_weight_is_at_least_one() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();
        assert!(registry.pool_by_id(0).unwrap().weight() >= 1);
        drop(tmp);
    }

    #[test]
    fn auto_tech_resolves_to_nvme_placeholder() {
        let (tmp, data_dir) = layout();
        let storage = StorageConfig {
            pools: vec![
                pool("auto-pool", PoolRole::Data, &tmp.path().join("auto-root"), None),
                StoragePoolConfig {
                    name: "ssd-pool".into(),
                    role: PoolRole::Data,
                    root: tmp.path().join("ssd-root"),
                    weight: None,
                    tech: PoolTech::Ssd,
                    health: Default::default(),
                },
                pool("journal", PoolRole::Wal, &tmp.path().join("optane0"), None),
                pool("meta", PoolRole::Metadata, &tmp.path().join("optane1"), None),
                pool("hints", PoolRole::Hints, &tmp.path().join("hints0"), None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();
        assert_eq!(registry.pool_by_id(0).unwrap().tech(), PoolTech::Nvme);
        assert_eq!(registry.pool_by_id(1).unwrap().tech(), PoolTech::Ssd);
        drop(tmp);
    }

    // -- Capacity --

    #[test]
    fn capacity_refresh_reflects_written_file() {
        let (tmp, data_dir) = layout();
        let (storage, roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        let pool = registry.pool_by_id(0).unwrap();
        let before_free = pool.free_bytes();
        let before_total = pool.total_bytes();

        // Write 1 MiB into the pool root; the filesystem free bytes drop.
        std::fs::write(roots[0].join("filler.bin"), vec![0xAB; 1024 * 1024]).unwrap();
        registry.refresh_capacity();

        assert!(pool.free_bytes() < before_free, "free must shrink after a write");
        assert_eq!(pool.total_bytes(), before_total, "total must not change");
        drop(tmp);
    }

    // -- Lookups --

    #[test]
    fn lookups_by_id_role_and_data_pools() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        assert_eq!(registry.pool_by_id(2).unwrap().name(), "journal");
        assert!(registry.pool_by_id(99).is_none());

        assert_eq!(registry.pool_by_role(PoolRole::Wal).unwrap().id(), 2);
        assert_eq!(registry.pool_by_role(PoolRole::Metadata).unwrap().id(), 3);
        assert_eq!(registry.pool_by_role(PoolRole::Hints).unwrap().id(), 4);

        let data_pools = registry.data_pools();
        assert_eq!(data_pools.len(), 2);
        let ids: Vec<u32> = data_pools.iter().map(|p| p.id()).collect();
        assert_eq!(ids, vec![0, 1], "data_pools order must be stable (config order)");
        drop(tmp);
    }

    // -- Phase B hooks --

    #[test]
    fn set_status_and_write_degraded_update_pool_state() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        registry.set_status(0, PoolStatus::Degraded);
        assert_eq!(registry.pool_by_id(0).unwrap().status(), PoolStatus::Degraded);
        registry.set_status(0, PoolStatus::Dead);
        assert_eq!(registry.pool_by_id(0).unwrap().status(), PoolStatus::Dead);

        registry.set_write_degraded(0, true);
        assert!(registry.pool_by_id(0).unwrap().write_degraded());

        // Unknown ids are a no-op.
        registry.set_status(99, PoolStatus::Dead);
        registry.set_write_degraded(99, true);
        drop(tmp);
    }

    /// d1: the status byte round-trips through `as_u8`/`pool_status_from_u8`
    /// for all four variants — the private decode must map 3 → `Draining`
    /// (never the silent `_ => Healthy` fallback), because the same byte is
    /// both the pool's atomic status and the `oceanfs_pool_status` gauge
    /// value.
    #[test]
    fn pool_status_byte_round_trips_all_variants() {
        for expected in
            [PoolStatus::Healthy, PoolStatus::Degraded, PoolStatus::Dead, PoolStatus::Draining]
        {
            assert_eq!(
                pool_status_from_u8(expected.as_u8()),
                expected,
                "byte {} must decode to {expected:?}",
                expected.as_u8()
            );
        }
    }

    /// g6: the shared availability derivation — a Dead metadata pool
    /// flips `node_serves_requests` to false (the node serves nothing);
    /// wal Dead sets `write_degraded`, which flips `accepts_writes` but
    /// NOT `node_serves_requests` (reads still served). This is the ONE
    /// source both the read and write coordinators' gates consult.
    #[test]
    fn availability_derives_from_metadata_and_wal_pools() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();
        let wal_id = registry.pool_by_role(PoolRole::Wal).unwrap().id();
        let meta_id = registry.pool_by_role(PoolRole::Metadata).unwrap().id();

        // Healthy start.
        assert!(registry.node_serves_requests(), "healthy metadata pool serves");
        assert!(registry.accepts_writes(), "healthy wal pool accepts writes");

        // wal Dead → write_degraded (the monitor sets the flag on Dead):
        // writes rejected, reads still served.
        registry.set_status(wal_id, PoolStatus::Dead);
        registry.set_write_degraded(wal_id, true);
        assert!(registry.node_serves_requests(), "metadata alive → serves reads");
        assert!(!registry.accepts_writes(), "wal write_degraded → no new writes");

        // metadata Dead → the node serves nothing (reads AND writes).
        registry.set_status(meta_id, PoolStatus::Dead);
        assert!(!registry.node_serves_requests(), "metadata Dead → serves nothing");
        assert!(!registry.accepts_writes(), "metadata Dead → no writes either");
        drop(tmp);
    }

    /// g6 availability with a Degraded-start topology: a data pool that
    /// failed its probe registers Degraded, and the role-complete registry
    /// still serves (the healthy metadata/wal pools govern availability).
    #[test]
    fn availability_open_with_degraded_data_pool() {
        let (tmp, data_dir) = layout();
        let root = uncreatable_root(tmp.path());
        let storage = StorageConfig {
            pools: vec![
                pool("degraded-data", PoolRole::Data, &root, None),
                pool("journal", PoolRole::Wal, &tmp.path().join("optane0"), None),
                pool("meta", PoolRole::Metadata, &tmp.path().join("optane1"), None),
                pool("hints", PoolRole::Hints, &tmp.path().join("hints0"), None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Degraded,
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();
        assert_eq!(registry.pool_by_id(0).unwrap().status(), PoolStatus::Degraded);
        assert!(registry.node_serves_requests(), "healthy metadata pool → available");
        assert!(registry.accepts_writes(), "healthy wal pool → accepts writes");
        drop(tmp);
    }

    #[test]
    fn set_pool_capacity_overrides_snapshot_and_metrics() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        registry.set_pool_capacity(0, 100 * GIB, 10 * GIB);
        let pool = registry.pool_by_id(0).unwrap();
        assert_eq!(pool.total_bytes(), 100 * GIB);
        assert_eq!(pool.free_bytes(), 10 * GIB);

        // Unknown id is a no-op.
        registry.set_pool_capacity(99, 1, 1);
        assert!(registry.pool_by_id(99).is_none());
        drop(tmp);
    }

    // -- Metrics --

    /// Test registrar capturing the registered gauges/counters.
    #[derive(Default)]
    struct TestRegistrar {
        counters: parking_lot::Mutex<Vec<Counter>>,
        gauges: parking_lot::Mutex<Vec<Gauge>>,
    }

    impl MetricRegistrar for TestRegistrar {
        fn register_counter(&self, counter: Counter) {
            self.counters.lock().push(counter);
        }
        fn register_gauge(&self, gauge: Gauge) {
            self.gauges.lock().push(gauge);
        }
        fn register_histogram(&self, _: Arc<oceanfs_core::Histogram>) {}
    }

    #[test]
    fn register_metrics_registers_per_pool_series() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        let registrar = TestRegistrar::default();
        registry.register_metrics(&registrar);

        let gauges = registrar.gauges.lock();
        let counters = registrar.counters.lock();
        // 5 pools × (status + bytes_free + bytes_total + write_degraded +
        // drain_state + drain_blocked + drain_remaining) gauges (d1 added
        // the two drain series; the d4 `oceanfs_drain_*` close added
        // drain_remaining).
        assert_eq!(gauges.len(), 35);
        // 5 pools × (io_errors + drain_dispatched + drain_released).
        assert_eq!(counters.len(), 15);

        let status_names: Vec<&str> =
            gauges.iter().filter(|g| g.name() == "oceanfs_pool_status").map(|g| g.name()).collect();
        assert_eq!(status_names.len(), 5, "one status gauge per pool");

        // Status gauges carry pool_id + role labels; pool 2 is the wal pool.
        let wal_status = gauges
            .iter()
            .find(|g| {
                g.name() == "oceanfs_pool_status" && g.labels().render().contains("pool_id=\"2\"")
            })
            .expect("wal status gauge");
        assert!(wal_status.labels().render().contains("role=\"wal\""));

        // Initial values published: status 0 (Healthy), bytes gauges > 0.
        let healthy = gauges
            .iter()
            .find(|g| {
                g.name() == "oceanfs_pool_status" && g.labels().render().contains("pool_id=\"0\"")
            })
            .expect("pool 0 status gauge");
        assert_eq!(healthy.get(), 0);
        let bytes_total = gauges
            .iter()
            .find(|g| {
                g.name() == "oceanfs_pool_bytes_total"
                    && g.labels().render().contains("pool_id=\"0\"")
            })
            .expect("pool 0 bytes gauge");
        assert!(bytes_total.get() > 0);

        // set_status propagates to the gauge.
        registry.set_status(0, PoolStatus::Degraded);
        assert_eq!(healthy.get(), 1);
        registry.set_status(0, PoolStatus::Healthy);
        assert_eq!(healthy.get(), 0);

        // The two drain series are registered per pool and start Idle (0).
        let drain_state = gauges
            .iter()
            .find(|g| {
                g.name() == "oceanfs_pool_drain_state"
                    && g.labels().render().contains("pool_id=\"0\"")
            })
            .expect("pool 0 drain_state gauge");
        let drain_blocked = gauges
            .iter()
            .find(|g| {
                g.name() == "oceanfs_pool_drain_blocked_reason"
                    && g.labels().render().contains("pool_id=\"0\"")
            })
            .expect("pool 0 drain_blocked gauge");
        assert_eq!(drain_state.get(), 0, "Idle before begin_drain");
        assert_eq!(drain_blocked.get(), 0, "not blocked before begin_drain");

        // begin_drain (d1): the status gauge renders Draining as 3 (single
        // source of truth — as_u8 = the atomic byte, accepted decision) and
        // drain_state flips to 1; the blocked gauge stays 0 until
        // set_drain_blocked.
        registry.begin_drain(0).expect("pool 0 is a data pool");
        assert_eq!(healthy.get(), 3, "status gauge renders Draining as 3");
        assert_eq!(drain_state.get(), 1, "Draining after begin_drain");
        assert_eq!(drain_blocked.get(), 0, "not blocked yet");

        // set/clear of oceanfs_pool_drain_blocked_reason (DoD item 6):
        // a blocked drain sets the gauge; Detachable clears it and moves
        // drain_state to 2.
        registry.set_drain_blocked(0, Some("no sibling headroom")).expect("block");
        assert_eq!(drain_blocked.get(), 1, "blocked metric set with the reason");
        registry.set_pool_empty(0).expect("pool drained empty");
        assert_eq!(drain_state.get(), 2, "Detachable gauge value");
        assert_eq!(drain_blocked.get(), 0, "blocked metric cleared on Detachable");

        // The d4 `oceanfs_drain_*` throughput series (gap close): the
        // registry-level reporters update the per-pool series the movers
        // drive.
        let dispatched = counters
            .iter()
            .find(|c| {
                c.name() == "oceanfs_drain_dispatched_total"
                    && c.labels().render().contains("pool_id=\"0\"")
            })
            .expect("pool 0 drain_dispatched counter");
        let released = counters
            .iter()
            .find(|c| {
                c.name() == "oceanfs_drain_released_total"
                    && c.labels().render().contains("pool_id=\"0\"")
            })
            .expect("pool 0 drain_released counter");
        let remaining = gauges
            .iter()
            .find(|g| {
                g.name() == "oceanfs_drain_remaining"
                    && g.labels().render().contains("pool_id=\"0\"")
            })
            .expect("pool 0 drain_remaining gauge");
        assert_eq!(dispatched.get(), 0);
        assert_eq!(released.get(), 0);
        assert_eq!(remaining.get(), 0);
        registry.note_drain_dispatched(0);
        registry.note_drain_dispatched(0);
        registry.note_drain_released(0);
        registry.set_drain_remaining(0, 7);
        assert_eq!(dispatched.get(), 2);
        assert_eq!(released.get(), 1);
        assert_eq!(remaining.get(), 7);
        drop(tmp);
    }

    /// f8: attach assigns sequential ids — the first attach gets
    /// `pools.len()`, the next one bumps.
    #[test]
    fn attach_assigns_sequential_ids() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();
        assert_eq!(registry.pool_count(), 5);

        let first = registry
            .attach(pool("attach-0", PoolRole::Data, &tmp.path().join("root-a"), None))
            .unwrap();
        assert_eq!(first, 5, "first attach id = current pool count");
        assert_eq!(registry.pool_count(), 6);

        let second = registry
            .attach(pool("attach-1", PoolRole::Data, &tmp.path().join("root-b"), None))
            .unwrap();
        assert_eq!(second, 6);
        assert_eq!(registry.pool_count(), 7);

        // The attached pools are visible to lookups and carry their root.
        assert_eq!(registry.pool_by_id(first).unwrap().root(), tmp.path().join("root-a"));
        assert_eq!(registry.pool_by_id(second).unwrap().name(), "attach-1");
    }

    /// f8: duplicate name and duplicate root are rejected against the
    /// LIVE registry (including the boot-configured pools).
    #[test]
    fn attach_rejects_duplicate_name_and_root() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();
        let root = tmp.path().join("root-a");

        let id = registry.attach(pool("dup", PoolRole::Data, &root, None)).unwrap();
        assert!(registry.pool_by_id(id).is_some());

        // Duplicate name (different root).
        let dup_name =
            registry.attach(pool("dup", PoolRole::Data, &tmp.path().join("root-b"), None));
        assert!(dup_name.is_err(), "duplicate name must be rejected");
        assert!(dup_name.unwrap_err().contains("duplicate pool name"));

        // Duplicate root (different name).
        let dup_root = registry.attach(pool("other", PoolRole::Data, &root, None));
        assert!(dup_root.is_err(), "duplicate root must be rejected");
        assert!(dup_root.unwrap_err().contains("duplicate pool root"));

        // Nothing was registered by the rejected attaches.
        assert_eq!(registry.pool_count(), 6);
    }

    /// f8: role cardinality is enforced against live pools — a second
    /// wal/metadata/hints pool is rejected, a second data pool is fine.
    #[test]
    fn attach_enforces_role_cardinality() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        // The boot topology already has a wal pool (id 2): a second wal
        // pool is rejected.
        let second_wal =
            registry.attach(pool("journal-2", PoolRole::Wal, &tmp.path().join("optane-b"), None));
        assert!(second_wal.is_err(), "at most one wal pool");
        assert!(second_wal.unwrap_err().contains("wal"));

        // A second data pool is allowed (placement spread).
        let data2 = registry
            .attach(pool("data-2", PoolRole::Data, &tmp.path().join("nvme2"), None))
            .unwrap();
        assert_eq!(data2, 5);
        assert_eq!(registry.pool_count(), 6);
    }

    /// f8: a probe failure under the Fatal policy rejects the attach; the
    /// missing root is not registered.
    #[test]
    fn attach_probe_failure_under_fatal_policy_rejects() {
        let (tmp, data_dir) = layout();
        // Fatal policy: a root that cannot be probed fails the attach.
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        // A root path that cannot be created (a regular file in the way).
        let blocked = tmp.path().join("blocked");
        std::fs::write(&blocked, b"file").unwrap();
        let attach = registry.attach(pool("bad", PoolRole::Data, &blocked, None));
        assert!(attach.is_err(), "Fatal policy must reject an unprobeable root");
        assert!(attach.unwrap_err().contains("probe failed"));
        assert_eq!(registry.pool_count(), 5, "the failed attach must not register");
    }

    /// f8: the Degraded policy registers an unprobeable root as Degraded
    /// (Phase A: treated as Healthy by consumers until Phase B).
    #[test]
    fn attach_probe_failure_under_degraded_policy_registers_degraded() {
        let (tmp, data_dir) = layout();
        let mut storage_config = StorageConfig {
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Degraded,
            ..StorageConfig::default()
        };
        let (full, _roots) = full_pool_config(tmp.path());
        storage_config.pools = full.pools;
        let registry = PoolRegistry::from_config(&storage_config, &data_dir).unwrap();

        let blocked = tmp.path().join("blocked");
        std::fs::write(&blocked, b"file").unwrap();
        let id = registry.attach(pool("soft", PoolRole::Data, &blocked, None)).unwrap();
        assert_eq!(registry.pool_by_id(id).unwrap().status(), PoolStatus::Degraded);
    }

    /// f8: the attached pool's root must stay disjoint from the node's
    /// data_dir.
    #[test]
    fn attach_rejects_root_overlapping_data_dir() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        // data_dir itself and a nested path trip the overlap rule.
        let direct = registry.attach(pool("bad-0", PoolRole::Data, &data_dir, None));
        assert!(direct.is_err(), "a root equal to data_dir must be rejected");

        let nested =
            registry.attach(pool("bad-1", PoolRole::Data, &data_dir.join("segments"), None));
        assert!(nested.is_err(), "a root nested in data_dir must be rejected");
        assert!(nested.unwrap_err().contains("overlaps the legacy data_dir"));

        assert_eq!(registry.pool_count(), 5, "no rejected attach may register");
    }

    /// f8: after attach, placement selection sees the new pool
    /// immediately (it reads the registry snapshot per selection — no
    /// cached pool list in the policy).
    #[test]
    fn placement_selects_attached_pool() {
        use crate::pool::placement::PlacementPolicy;

        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();
        let policy = PlacementPolicy::new();

        // The boot data pools only.
        assert_eq!(registry.data_pools().len(), 2);
        let before = policy.select_data_pool(&registry);
        assert!(before.unwrap().id() <= 1);

        // Attach a third data pool; selection may now return it.
        registry
            .attach(pool("attached", PoolRole::Data, &tmp.path().join("nvme-a"), None))
            .unwrap();
        assert_eq!(registry.data_pools().len(), 3);
        let after = policy.select_data_pool(&registry).expect("a data pool");
        assert!(after.id() <= 2, "selection must see the attached pool, got {}", after.id());
    }

    // -- d5 removed-pool overlay (ADR-0036 D8) --

    #[test]
    fn overlay_suppresses_matching_data_pool_and_preserves_survivor_ids() {
        let (tmp, data_dir) = layout();
        let (storage, roots) = full_pool_config(tmp.path());
        // "fast-nvme-0" (id 0) was drained + detached; the config still
        // declares it (config is untouched), so the overlay suppresses it.
        let removed = vec![PoolRemovedRecord::new("fast-nvme-0".into(), roots[0].clone())];
        let registry = PoolRegistry::from_config_skipping(&storage, &data_dir, &removed).unwrap();
        let data = registry.data_pools();
        assert_eq!(data.len(), 1, "the removed data pool must not register");
        assert_eq!(data[0].name(), "fast-nvme-1");
        assert_eq!(data[0].id(), 1, "survivors keep their original config-order id");
        assert!(registry.pool_by_id(0).is_none());
        assert_eq!(registry.pool_count(), 4, "wal/meta/hints still register");
    }

    #[test]
    fn overlay_partial_match_does_not_suppress() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        // A record matches on name only (different root) → not suppressed
        // (record must match name AND root, ADR-0036 D8).
        let removed =
            vec![PoolRemovedRecord::new("fast-nvme-0".into(), tmp.path().join("other-root"))];
        let registry = PoolRegistry::from_config_skipping(&storage, &data_dir, &removed).unwrap();
        assert_eq!(registry.data_pools().len(), 2);
        assert_eq!(registry.pool_count(), 5);
    }

    #[test]
    fn overlay_stale_record_is_inert() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = full_pool_config(tmp.path());
        // A record matching no config pool (operator edited the config to
        // drop the pool) suppresses nothing.
        let removed = vec![PoolRemovedRecord::new("ghost".into(), tmp.path().join("nope"))];
        let registry = PoolRegistry::from_config_skipping(&storage, &data_dir, &removed).unwrap();
        assert_eq!(registry.data_pools().len(), 2);
        assert_eq!(registry.pool_count(), 5);
    }

    #[test]
    fn overlay_removing_every_data_pool_refuses_boot() {
        let (tmp, data_dir) = layout();
        let (storage, roots) = full_pool_config(tmp.path());
        let removed = vec![
            PoolRemovedRecord::new("fast-nvme-0".into(), roots[0].clone()),
            PoolRemovedRecord::new("fast-nvme-1".into(), roots[1].clone()),
        ];
        let err = PoolRegistry::from_config_skipping(&storage, &data_dir, &removed).unwrap_err();
        assert!(
            err.contains("no 'data' pool remains after applying the removed-pool overlay"),
            "{err}"
        );
    }
}
