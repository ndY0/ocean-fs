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
//! Single lock only (`PoolRegistry.pools`), held briefly and never across
//! I/O or across another lock. `PoolMetrics` is immutable after construction
//! (its gauges/counters are interior-mutable atomics).

pub mod placement;

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc,
    },
};

use oceanfs_core::{
    Counter, Gauge, LabelSet, MetricRegistrar, MissingRootPolicy, PoolRole, PoolTech, StorageConfig,
};
use parking_lot::RwLock;
pub use placement::PlacementPolicy;

/// One GiB — the unit auto-derived placement weights are scaled to
/// (ADR-0029 §D8 "weights with capacity auto-detect default").
const GIB: u64 = 1024 * 1024 * 1024;

/// Marker bytes written and read back by the startup root probe.
const POOL_PROBE_MARKER: &[u8] = b"oceanfs pool probe";

// ---------------------------------------------------------------------------
// PoolStatus
// ---------------------------------------------------------------------------

/// Health state of a storage pool (ADR-0029 §D3 state machine).
///
/// Phase A: all pools are `Healthy`; a pool whose startup probe failed under
/// the `Degraded` policy registers as `Degraded`. `Dead` requires *confirmed
/// loss* (ENOENT on an owned segment, EIO on fsync, device unplug) and is
/// only reachable once Phase B's health monitor is wired.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::PoolStatus;
///
/// assert_eq!(PoolStatus::Healthy.as_u8(), 0);
/// assert_eq!(PoolStatus::Degraded.as_u8(), 1);
/// assert_eq!(PoolStatus::Dead.as_u8(), 2);
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
}

impl PoolStatus {
    /// Numeric encoding used by the `oceanfs_pool_status` gauge
    /// (0 = Healthy, 1 = Degraded, 2 = Dead).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::PoolStatus;
    ///
    /// assert_eq!(PoolStatus::Degraded.as_u8(), 1);
    /// ```
    pub fn as_u8(self) -> u8 {
        match self {
            PoolStatus::Healthy => 0,
            PoolStatus::Degraded => 1,
            PoolStatus::Dead => 2,
        }
    }
}

/// Decodes the atomic status byte back into a [`PoolStatus`].
fn pool_status_from_u8(value: u8) -> PoolStatus {
    match value {
        1 => PoolStatus::Degraded,
        2 => PoolStatus::Dead,
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
/// let registry = PoolRegistry::from_config(
///     &oceanfs_core::StorageConfig::default(),
///     &data_dir,
/// )
/// .expect("legacy single-pool registry");
///
/// let pool = registry.pool_by_role(PoolRole::Data).expect("implicit data pool");
/// assert_eq!(pool.role(), PoolRole::Data);
/// assert!(pool.weight() >= 1);
/// ```
pub struct StoragePool {
    /// Stable pool id — the config-order index (0..n). Legacy mode: 0.
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
    /// Role-consequence flag (ADR-0029 §D3); Phase A: always `false`.
    write_degraded: AtomicBool,
    /// Total filesystem bytes at last capacity refresh.
    total_bytes: AtomicU64,
    /// Free filesystem bytes at last capacity refresh.
    free_bytes: AtomicU64,
}

impl StoragePool {
    /// Creates a pool with fully resolved values. Internal: construction
    /// happens in [`PoolRegistry::from_config`].
    // Clippy: 8 positional args; a config-bundle struct would hide the
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    /// assert_eq!(registry.pools()[0].name(), "legacy");
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    /// assert_eq!(registry.pools()[0].root(), data_dir);
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    /// // Legacy mode: the implicit pool carries weight 1.
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    /// // `Auto` resolves to the Phase A `Nvme` placeholder.
    /// assert_eq!(registry.pools()[0].tech(), PoolTech::Nvme);
    /// ```
    pub fn tech(&self) -> PoolTech {
        self.tech
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
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
struct PoolMetrics {
    /// Pool id the series belong to (used to find the right series).
    pool_id: u32,
    /// `oceanfs_pool_status{pool_id, role}` — 0=Healthy 1=Degraded 2=Dead.
    status: Gauge,
    /// `oceanfs_pool_bytes_free{pool_id}`.
    bytes_free: Gauge,
    /// `oceanfs_pool_bytes_total{pool_id}`.
    bytes_total: Gauge,
    /// `oceanfs_pool_io_errors_total{pool_id}` — Phase A: exists, always 0
    /// (Phase B's `DiskIo` fault path increments it).
    io_errors: Counter,
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
                "Pool health status (0=Healthy 1=Degraded 2=Dead)".into(),
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
                id_label,
            ),
            io_errors: Counter::new(
                "oceanfs_pool_io_errors_total".into(),
                "I/O errors observed on the pool (Phase A: always 0)".into(),
                LabelSet::new(&[("pool_id", &pool_id)]),
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// PoolRegistry
// ---------------------------------------------------------------------------

/// The node's pool set: the lookup API placement and routing consume.
///
/// Built by [`PoolRegistry::from_config`] from the topology config (f1).
/// Legacy mode (no pools configured) yields a single implicit `data` pool
/// at `data_dir`, so nothing downstream changes behavior.
///
/// # Examples
///
/// ```
/// use oceanfs_core::PoolRole;
/// use oceanfs_storage::PoolRegistry;
///
/// # let tmp = tempfile::tempdir().expect("tempdir");
/// # let data_dir = tmp.path().join("data");
/// let registry = PoolRegistry::from_config(
///     &oceanfs_core::StorageConfig::default(),
///     &data_dir,
/// )
/// .expect("legacy registry");
///
/// assert_eq!(registry.pools().len(), 1);
/// assert!(registry.pool_by_role(PoolRole::Data).is_some());
/// ```
pub struct PoolRegistry {
    /// The pool set. Reads dominate (placement + routing lookup per
    /// request), so an `RwLock` (perf guideline 7.2); the list only changes
    /// on runtime attach (f8).
    pools: RwLock<Vec<Arc<StoragePool>>>,
    /// Per-pool metric series; immutable after construction (gauges and
    /// counters are interior-mutable).
    metrics: Vec<PoolMetrics>,
}

impl PoolRegistry {
    /// Builds the registry from the topology config, probing every root.
    ///
    /// - Re-validates the config via [`StorageConfig::validate`] (role
    ///   cardinality, one-root-per-pool, weights, health knobs).
    /// - Legacy mode (empty pool list): one implicit `data` pool at
    ///   `data_dir`, weight 1, tech `Nvme` (Auto placeholder). The root is
    ///   probed with `Fatal` semantics — today's node refuses to start when
    ///   `data_dir` cannot be created, and the `MissingRootPolicy` applies
    ///   to configured pools only.
    /// - Explicit mode: one `StoragePool` per `PoolConfig`, ids in config
    ///   order (0..n). Probe failure resolves against
    ///   `MissingRootPolicy`: `Fatal` → `Err`, `Degraded` → pool registered
    ///   with status `Degraded` (Phase A: treated as Healthy by consumers
    ///   until Phase B).
    /// - Weight resolution: explicit config weight wins; `None` →
    ///   `max(1, total / 1 GiB)` from the probe-time capacity snapshot.
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// );
    /// assert!(registry.is_ok());
    /// ```
    pub fn from_config(storage: &StorageConfig, data_dir: &Path) -> Result<PoolRegistry, String> {
        storage.validate(data_dir).map_err(|e| format!("invalid storage config: {e}"))?;

        let mut pools: Vec<Arc<StoragePool>> = Vec::with_capacity(storage.pools.len().max(1));
        let mut metrics: Vec<PoolMetrics> = Vec::with_capacity(storage.pools.len().max(1));

        if storage.pools.is_empty() {
            // Legacy zero-config fallback: single implicit data pool at
            // data_dir, probed with Fatal semantics (today's behavior).
            probe_root(data_dir).map_err(|e| {
                format!("legacy data_dir '{}' probe failed: {e}", data_dir.display())
            })?;
            let capacity = statvfs_capacity(data_dir).unwrap_or_default();
            let pool = Arc::new(StoragePool::new(
                0,
                "legacy".into(),
                PoolRole::Data,
                data_dir.to_path_buf(),
                1,
                resolve_tech(PoolTech::Auto),
                PoolStatus::Healthy,
                capacity,
            ));
            metrics.push(PoolMetrics::new(&pool));
            pools.push(pool);
        } else {
            for (index, config) in storage.pools.iter().enumerate() {
                let id = index as u32;
                let (status, capacity) = match probe_root(&config.root) {
                    Ok(()) => {
                        (PoolStatus::Healthy, statvfs_capacity(&config.root).unwrap_or_default())
                    }
                    Err(e) if storage.missing_root_policy == MissingRootPolicy::Degraded => {
                        tracing::warn!(
                            pool = %config.name,
                            error = %e,
                            "pool root probe failed; registering pool as Degraded \
                             (Phase A: treated as Healthy by consumers until Phase B)"
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
                ));
                metrics.push(PoolMetrics::new(&pool));
                pools.push(pool);
            }
        }

        // Publish the initial capacity/status to the metric series.
        for pool in &pools {
            if let Some(metric) = metrics.iter().find(|metric| metric.pool_id == pool.id()) {
                metric.status.set(pool.status().as_u8() as u64);
                metric.bytes_free.set(pool.free_bytes());
                metric.bytes_total.set(pool.total_bytes());
            }
        }

        Ok(PoolRegistry { pools: RwLock::new(pools), metrics })
    }

    /// Returns a snapshot copy of all pools (config order; legacy mode:
    /// exactly one).
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    /// assert_eq!(registry.pools().len(), 1);
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    /// assert!(registry.pool_by_id(0).is_some());
    /// assert!(registry.pool_by_id(1).is_none());
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    /// assert!(registry.pool_by_role(PoolRole::Data).is_some());
    /// assert!(registry.pool_by_role(PoolRole::Hints).is_none());
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
    /// registry.set_write_degraded(0, true);
    /// assert!(registry.pool_by_id(0).expect("pool").write_degraded());
    /// ```
    pub fn set_write_degraded(&self, id: u32, write_degraded: bool) {
        if let Some(pool) = self.pool_by_id(id) {
            pool.set_write_degraded(write_degraded);
        }
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
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
    /// let registry = PoolRegistry::from_config(
    ///     &oceanfs_core::StorageConfig::default(),
    ///     &data_dir,
    /// )
    /// .expect("registry");
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
        for metric in &self.metrics {
            registrar.register_gauge(metric.status.clone());
            registrar.register_gauge(metric.bytes_free.clone());
            registrar.register_gauge(metric.bytes_total.clone());
            registrar.register_counter(metric.io_errors.clone());
        }
    }

    /// Returns the metric series for a pool id (linear scan; pool counts
    /// are 5–20, and this is a cold path).
    fn metrics_for(&self, pool_id: u32) -> Option<&PoolMetrics> {
        self.metrics.iter().find(|metric| metric.pool_id == pool_id)
    }
}

impl std::fmt::Debug for PoolRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Manual impl: the metrics vec holds Gauge/Counter (Debug), but
        // rendering the pool list only is the useful surface.
        f.debug_struct("PoolRegistry")
            .field("pools", &self.pools())
            .field("metric_series", &self.metrics.len())
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

    /// A tempdir whose `data/` subdir is the legacy data_dir and whose
    /// other subdirs are pool roots (siblings, so the f1 disjointness rule
    /// holds).
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

    /// A 4-pool topology (data×2, wal, metadata) with sibling roots.
    fn four_pool_config(tmp: &Path) -> (StorageConfig, [PathBuf; 4]) {
        let roots =
            [tmp.join("nvme0"), tmp.join("nvme1"), tmp.join("optane0"), tmp.join("optane1")];
        let storage = StorageConfig {
            pools: vec![
                pool("fast-nvme-0", PoolRole::Data, &roots[0], None),
                pool("fast-nvme-1", PoolRole::Data, &roots[1], Some(3)),
                pool("journal", PoolRole::Wal, &roots[2], None),
                pool("meta", PoolRole::Metadata, &roots[3], None),
            ],
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

    #[test]
    fn legacy_mode_creates_single_implicit_data_pool() {
        let (tmp, data_dir) = layout();
        let registry = PoolRegistry::from_config(&StorageConfig::default(), &data_dir).unwrap();

        let pools = registry.pools();
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].id(), 0);
        assert_eq!(pools[0].name(), "legacy");
        assert_eq!(pools[0].role(), PoolRole::Data);
        assert_eq!(pools[0].root(), data_dir);
        assert_eq!(pools[0].weight(), 1);
        assert_eq!(pools[0].status(), PoolStatus::Healthy);
        assert!(!pools[0].write_degraded());
        // Auto tech resolves to the Phase A Nvme placeholder.
        assert_eq!(pools[0].tech(), PoolTech::Nvme);
        assert!(data_dir.exists(), "legacy probe must create data_dir");
        drop(tmp);
    }

    #[test]
    fn explicit_mode_assigns_ids_in_config_order() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = four_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        let pools = registry.pools();
        assert_eq!(pools.len(), 4);
        let ids: Vec<u32> = pools.iter().map(|p| p.id()).collect();
        assert_eq!(ids, vec![0, 1, 2, 3]);
        assert_eq!(pools[0].name(), "fast-nvme-0");
        assert_eq!(pools[2].name(), "journal");
        assert_eq!(pools[2].role(), PoolRole::Wal);
        assert_eq!(pools[3].role(), PoolRole::Metadata);
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
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = PoolRegistry::from_config(&storage, &data_dir).unwrap_err();
        assert!(err.contains("wal"), "message: {err}");
        drop(tmp);
    }

    #[test]
    fn probe_creates_roots_and_leaves_no_probe_files() {
        let (tmp, data_dir) = layout();
        let (storage, roots) = four_pool_config(tmp.path());
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
            pools: vec![pool("doomed", PoolRole::Data, &root, None)],
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
            pools: vec![pool("degraded-pool", PoolRole::Data, &root, None)],
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

    // -- Weight + tech resolution --

    #[test]
    fn explicit_weight_wins_over_auto() {
        let (tmp, data_dir) = layout();
        let (storage, roots) = four_pool_config(tmp.path());
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
        let (storage, _roots) = four_pool_config(tmp.path());
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
            ],
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
        let (storage, roots) = four_pool_config(tmp.path());
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
        let (storage, _roots) = four_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        assert_eq!(registry.pool_by_id(2).unwrap().name(), "journal");
        assert!(registry.pool_by_id(99).is_none());

        assert_eq!(registry.pool_by_role(PoolRole::Wal).unwrap().id(), 2);
        assert_eq!(registry.pool_by_role(PoolRole::Metadata).unwrap().id(), 3);
        assert!(registry.pool_by_role(PoolRole::Hints).is_none());

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
        let (storage, _roots) = four_pool_config(tmp.path());
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

    #[test]
    fn set_pool_capacity_overrides_snapshot_and_metrics() {
        let (tmp, data_dir) = layout();
        let (storage, _roots) = four_pool_config(tmp.path());
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
        let (storage, _roots) = four_pool_config(tmp.path());
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();

        let registrar = TestRegistrar::default();
        registry.register_metrics(&registrar);

        let gauges = registrar.gauges.lock();
        let counters = registrar.counters.lock();
        // 4 pools × (status + bytes_free + bytes_total) gauges.
        assert_eq!(gauges.len(), 12);
        // 4 pools × io_errors counter.
        assert_eq!(counters.len(), 4);

        let status_names: Vec<&str> =
            gauges.iter().filter(|g| g.name() == "oceanfs_pool_status").map(|g| g.name()).collect();
        assert_eq!(status_names.len(), 4, "one status gauge per pool");

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
        drop(tmp);
    }
}
