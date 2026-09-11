//! Storage pool configuration (ADR-0029 §D8).
//!
//! Introduces the disk-topology config surface: a node declares its disks
//! as *storage pools*, each with a role (`data | wal | metadata | hints`),
//! exactly one root (one pool = one root = one failure domain), an optional
//! placement weight, a device-tech hint, and per-pool health tuning knobs
//! (carried now, consumed by Phase B's health monitor).
//!
//! Storage pools are mandatory (ADR-0031): an empty `[storage.pools]` list
//! is rejected at validation with an error naming the required roles. The
//! legacy single-`data_dir` zero-config fallback was removed — migration to
//! pools is explicit, never automatic.
//!
//! ## Configuration shape
//!
//! ```toml
//! [storage]
//! missing_root_policy = "fatal"   # or "degraded"
//!
//! [[storage.pools]]
//! name = "fast-nvme-0"
//! role = "data"
//! root = "/mnt/nvme0"
//! weight = 2
//! tech = "nvme"
//! health = { error_rate_threshold = 0.001, min_errors = 3,
//!            latency_factor = 5.0, trend_window_secs = 300,
//!            detection_window_secs = 30, recovery_window_secs = 300 }
//! ```
//!
//! `health` is an inline table on each pool — per-pool, never a global
//! `[storage.pools.health]` block.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

// ---------------------------------------------------------------------------
// PoolRole
// ---------------------------------------------------------------------------

/// The purpose a storage pool serves on a node.
///
/// Role pinning is ADR-0029's headline feature: WAL/metadata traffic is
/// isolated from segment I/O so a segment-heavy disk does not stall the
/// durability-critical paths, and a failed role pool triggers role-specific
/// cluster consequences (see ADR-0029 §D3).
///
/// Exactly one `wal`, `metadata`, and `hints` pool must be configured
/// (role pinning, ADR-0031 — a node without them is refused at boot); any
/// number of `data` pools may be configured (each is a distinct failure
/// domain).
///
/// # Examples
///
/// ```
/// use oceanfs_core::PoolRole;
///
/// assert_eq!(PoolRole::Data.as_str(), "data");
/// assert_eq!(PoolRole::Wal.as_str(), "wal");
/// assert_eq!(PoolRole::Metadata.as_str(), "metadata");
/// assert_eq!(PoolRole::Hints.as_str(), "hints");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum PoolRole {
    /// Segment data pool — placement spreads sealed segments across these.
    Data,
    /// Write-ahead log pool — pinned for the data WAL and event WAL.
    Wal,
    /// Metadata store (RocksDB) pool.
    Metadata,
    /// Hinted-handoff WAL pool.
    Hints,
}

impl PoolRole {
    /// Returns the serialized (lowercase) wire name of this role.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::PoolRole;
    ///
    /// assert_eq!(PoolRole::Data.as_str(), "data");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            PoolRole::Data => "data",
            PoolRole::Wal => "wal",
            PoolRole::Metadata => "metadata",
            PoolRole::Hints => "hints",
        }
    }
}

// ---------------------------------------------------------------------------
// PoolTech
// ---------------------------------------------------------------------------

/// Device technology class of a storage pool root.
///
/// Phase A (epic `disk-resilience`, f1) carries the knob but does not act on
/// it: `Auto` resolves to an `Nvme` placeholder in the pool runtime (f2) and
/// real auto-detection lands in Phase B with the health monitor, where
/// technology defines the error profile (SMART reallocated sectors for HDD,
/// wear/ECC for SSD/NVMe, I/O signals only for cloud-ephemeral; ADR-0029 §D3).
///
/// # Examples
///
/// ```
/// use oceanfs_core::PoolTech;
///
/// #[derive(serde::Deserialize)]
/// struct TechWrapper {
///     tech: PoolTech,
/// }
///
/// // "auto" is the default when `tech` is omitted.
/// let wrapper: TechWrapper = toml::from_str("tech = \"auto\"").unwrap();
/// assert_eq!(wrapper.tech, PoolTech::Auto);
///
/// let wrapper: TechWrapper = toml::from_str("tech = \"nvme\"").unwrap();
/// assert_eq!(wrapper.tech, PoolTech::Nvme);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum PoolTech {
    /// Auto-detect from the device (Phase A: resolves to `Nvme` placeholder).
    #[default]
    Auto,
    /// Rotating magnetic disk.
    Hdd,
    /// SATA/SAS solid-state drive.
    Ssd,
    /// NVMe solid-state drive.
    Nvme,
    /// Cloud-attached ephemeral storage (I/O signals only).
    #[serde(rename = "cloud-ephemeral")]
    CloudEphemeral,
}

// ---------------------------------------------------------------------------
// MissingRootPolicy
// ---------------------------------------------------------------------------

/// Startup behavior when a configured pool root is missing or unprobeable.
///
/// ADR-0029 §D8: probe each root (write+read) at startup; a missing root is
/// either fatal (node refuses to start, mirroring today's `create_dir_all`
/// failure) or degraded (pool registered with status `Degraded`, node
/// continues). Default: `Fatal`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::MissingRootPolicy;
///
/// #[derive(serde::Deserialize)]
/// struct PolicyWrapper {
///     policy: MissingRootPolicy,
/// }
///
/// let wrapper: PolicyWrapper = toml::from_str("policy = \"degraded\"").unwrap();
/// assert_eq!(wrapper.policy, MissingRootPolicy::Degraded);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum MissingRootPolicy {
    /// A missing root aborts node startup.
    #[default]
    Fatal,
    /// A missing root registers the pool as `Degraded` and startup continues.
    Degraded,
}

// ---------------------------------------------------------------------------
// Health detector configuration
// ---------------------------------------------------------------------------

/// Latency percentile that feeds the health monitor's trend detector.
///
/// The observer records p50, p99, and p999 per op; the detector compares
/// the configured percentile across windows. Default: `P99` (the
/// pre-f5 hard-coded behavior).
///
/// # Examples
///
/// ```
/// use oceanfs_core::TrendPercentile;
///
/// assert_eq!(TrendPercentile::default(), TrendPercentile::P99);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TrendPercentile {
    /// Median latency.
    P50,
    /// 99th percentile (default).
    #[default]
    P99,
    /// 99.9th percentile.
    P999,
}

/// A SMART counter selectable for the trend detector.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{SmartCounter};
///
/// #[derive(serde::Deserialize)]
/// struct Wrapper {
///     counter: SmartCounter,
/// }
///
/// let wrapper: Wrapper = toml::from_str("counter = \"wear_level\"").unwrap();
/// assert_eq!(wrapper.counter, SmartCounter::WearLevel);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SmartCounter {
    /// Reallocated sector count (HDD tell).
    ReallocatedSectors,
    /// Pending sector count (HDD tell).
    PendingSectors,
    /// Uncorrectable ECC errors (SSD/NVMe tell).
    UncorrectableEcc,
    /// Wear-level indicator, 0-100 (SSD/NVMe tell).
    WearLevel,
}

/// Per-tech SMART counter selection for the trend detector.
///
/// The selected counters are summed per window; any growth across the
/// last two window pairs trips `Degrading`. An empty list disables the
/// SMART signal for that tech (e.g. cloud volumes). Defaults preserve the
/// pre-f5 hard-coded mapping.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{SmartCounter, SmartGrowthConfig};
///
/// let config = SmartGrowthConfig::default();
/// assert_eq!(
///     config.hdd,
///     vec![SmartCounter::ReallocatedSectors, SmartCounter::PendingSectors]
/// );
/// assert!(config.cloud_ephemeral.is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SmartGrowthConfig {
    /// Counters for `tech = "hdd"`.
    pub hdd: Vec<SmartCounter>,
    /// Counters for `tech = "ssd"`.
    pub ssd: Vec<SmartCounter>,
    /// Counters for `tech = "nvme"`.
    pub nvme: Vec<SmartCounter>,
    /// Counters for `tech = "cloud-ephemeral"` (default: none — I/O only).
    pub cloud_ephemeral: Vec<SmartCounter>,
}

impl Default for SmartGrowthConfig {
    fn default() -> Self {
        Self {
            hdd: vec![SmartCounter::ReallocatedSectors, SmartCounter::PendingSectors],
            ssd: vec![SmartCounter::UncorrectableEcc, SmartCounter::WearLevel],
            nvme: vec![SmartCounter::UncorrectableEcc, SmartCounter::WearLevel],
            cloud_ephemeral: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// PoolHealthConfig
// ---------------------------------------------------------------------------

/// Resolved per-pool health-monitor tuning knobs (ADR-0029 §D3).
///
/// This is the fully-resolved form (hard-coded defaults ← global
/// `[storage.health]` ← per-pool `health = { ... }`) consumed by the
/// health monitor. Configuration files use [`PoolHealthOverride`] for the
/// partial tables; every field here has a built-in default, so a minimal
/// pool entry only needs `name`, `role`, and `root`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::PoolHealthConfig;
///
/// let health = PoolHealthConfig::default();
/// assert_eq!(health.error_rate_threshold, 0.001);
/// assert_eq!(health.trend_window_secs, 300);
/// assert_eq!(health.trend_doubling_factor, 2.0);
/// assert_eq!(health.trend_min_windows, 3);
/// assert_eq!(health.history_max_windows, 64);
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolHealthConfig {
    /// I/O error rate (errors per operation) above which the trend fast-path
    /// flags the pool. Must be in `(0, 1)`. Default: `0.001`.
    pub error_rate_threshold: f64,
    /// Minimum number of errors in a window before a suspicion is raised.
    /// Default: `3`.
    pub min_errors: u64,
    /// Latency growth factor per window that counts as a worsening trend.
    /// Default: `5.0`.
    pub latency_factor: f64,
    /// Length of the trend window in seconds. Must be `> 0`. Default: `300`.
    pub trend_window_secs: u64,
    /// Length of the fast detection window in seconds. Must be `> 0`.
    /// Default: `30`.
    pub detection_window_secs: u64,
    /// Clean window (seconds) that moves a pool back to `Healthy`.
    /// Must be `> 0`. Default: `300`.
    pub recovery_window_secs: u64,
    /// Trend doubling factor: `x[i] >= factor * x[i-1]` for the last two
    /// window pairs counts as worsening. Must be `> 1.0`. Default: `2.0`.
    pub trend_doubling_factor: f64,
    /// Minimum number of windows in a series before the trend detector
    /// evaluates a slope. Must be `>= 2`. Default: `3` (last two pairs).
    pub trend_min_windows: usize,
    /// Which latency percentile feeds the latency trend. Default: `P99`.
    pub trend_latency_percentile: TrendPercentile,
    /// Upper bound of the per-pool signal history (windows). Must be
    /// `>= 4` (the hard lower clamp). Default: `64`.
    pub history_max_windows: usize,
    /// Per-tech SMART counters whose growth trips the trend. Defaults
    /// mirror the pre-f5 hard-coded mapping.
    pub smart_growth: SmartGrowthConfig,
}

impl Default for PoolHealthConfig {
    fn default() -> Self {
        Self {
            error_rate_threshold: 0.001,
            min_errors: 3,
            latency_factor: 5.0,
            trend_window_secs: 300,
            detection_window_secs: 30,
            recovery_window_secs: 300,
            trend_doubling_factor: 2.0,
            trend_min_windows: 3,
            trend_latency_percentile: TrendPercentile::P99,
            history_max_windows: 64,
            smart_growth: SmartGrowthConfig::default(),
        }
    }
}

/// Partial health overrides — the config-file surface (f5 D4).
///
/// Used for both `[storage.health]` (node-global defaults) and a pool's
/// inline `health = { ... }` table. Resolution is field-by-field:
/// hard-coded defaults ← global table ← per-pool table (per-pool wins).
///
/// The three monitor-level fields (`monitor_tick_interval_secs`,
/// `event_capacity`, `hints_probe_divisor`) are **global-only**; setting
/// them on a pool's inline table is rejected by
/// [`StorageConfig::validate`].
///
/// # Examples
///
/// ```
/// use oceanfs_core::{PoolHealthConfig, PoolHealthOverride};
///
/// let global: PoolHealthOverride = toml::from_str(
///     "latency_factor = 2.0\ntrend_doubling_factor = 1.5",
/// )
/// .unwrap();
/// let pool: PoolHealthOverride = toml::from_str("latency_factor = 1.25").unwrap();
///
/// let resolved = pool.resolve(&global.resolve(&PoolHealthConfig::default()));
/// assert_eq!(resolved.latency_factor, 1.25); // per-pool wins
/// assert_eq!(resolved.trend_doubling_factor, 1.5); // global applies
/// assert_eq!(resolved.min_errors, 3); // hard-coded default
/// ```
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolHealthOverride {
    /// See [`PoolHealthConfig::error_rate_threshold`].
    pub error_rate_threshold: Option<f64>,
    /// See [`PoolHealthConfig::min_errors`].
    pub min_errors: Option<u64>,
    /// See [`PoolHealthConfig::latency_factor`].
    pub latency_factor: Option<f64>,
    /// See [`PoolHealthConfig::trend_window_secs`].
    pub trend_window_secs: Option<u64>,
    /// See [`PoolHealthConfig::detection_window_secs`].
    pub detection_window_secs: Option<u64>,
    /// See [`PoolHealthConfig::recovery_window_secs`].
    pub recovery_window_secs: Option<u64>,
    /// See [`PoolHealthConfig::trend_doubling_factor`].
    pub trend_doubling_factor: Option<f64>,
    /// See [`PoolHealthConfig::trend_min_windows`].
    pub trend_min_windows: Option<usize>,
    /// See [`PoolHealthConfig::trend_latency_percentile`].
    pub trend_latency_percentile: Option<TrendPercentile>,
    /// See [`PoolHealthConfig::history_max_windows`].
    pub history_max_windows: Option<usize>,
    /// See [`PoolHealthConfig::smart_growth`].
    pub smart_growth: Option<SmartGrowthConfig>,
    /// Global monitor ticker override in seconds (`[storage.health]` only;
    /// `None` = per-pool `detection_window_secs` cadence).
    pub monitor_tick_interval_secs: Option<u64>,
    /// Global status-event channel capacity (`[storage.health]` only;
    /// default `64`).
    pub event_capacity: Option<usize>,
    /// Hints-root probe cadence divisor (`[storage.health]` only): the
    /// probe runs every `detection_window_secs / divisor` (min 1s).
    /// Default `6`.
    pub hints_probe_divisor: Option<u64>,
}

impl PoolHealthOverride {
    /// Resolves this partial override over `base` (per-pool fields only;
    /// monitor-level fields are ignored here).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::{PoolHealthConfig, PoolHealthOverride};
    ///
    /// let base = PoolHealthConfig::default();
    /// let override_config: PoolHealthOverride = toml::from_str("min_errors = 7").unwrap();
    /// assert_eq!(override_config.resolve(&base).min_errors, 7);
    /// assert_eq!(override_config.resolve(&base).detection_window_secs, 30);
    /// ```
    pub fn resolve(&self, base: &PoolHealthConfig) -> PoolHealthConfig {
        PoolHealthConfig {
            error_rate_threshold: self.error_rate_threshold.unwrap_or(base.error_rate_threshold),
            min_errors: self.min_errors.unwrap_or(base.min_errors),
            latency_factor: self.latency_factor.unwrap_or(base.latency_factor),
            trend_window_secs: self.trend_window_secs.unwrap_or(base.trend_window_secs),
            detection_window_secs: self.detection_window_secs.unwrap_or(base.detection_window_secs),
            recovery_window_secs: self.recovery_window_secs.unwrap_or(base.recovery_window_secs),
            trend_doubling_factor: self.trend_doubling_factor.unwrap_or(base.trend_doubling_factor),
            trend_min_windows: self.trend_min_windows.unwrap_or(base.trend_min_windows),
            trend_latency_percentile: self
                .trend_latency_percentile
                .unwrap_or(base.trend_latency_percentile),
            history_max_windows: self.history_max_windows.unwrap_or(base.history_max_windows),
            smart_growth: self.smart_growth.clone().unwrap_or_else(|| base.smart_growth.clone()),
        }
    }
}

// ---------------------------------------------------------------------------
// PoolConfig
// ---------------------------------------------------------------------------

/// One storage pool definition: a single root directory with a role.
///
/// The schema has exactly one `root` field — one pool = one root = one
/// failure domain (ADR-0029 §D8). A multi-root expression (e.g. `root` as an
/// array) is rejected at deserialization with a clear error; same-role
/// devices are expressed as multiple pool entries.
///
/// NOTE: the crate facade re-exports this type as
/// `oceanfs_core::StoragePoolConfig` (in `config::mod`, `PoolConfig as
/// StoragePoolConfig`) because `oceanfs_core::PoolConfig` already names the
/// active-segment-pool config (`types::config::PoolConfig`).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{PoolRole, PoolTech, StoragePoolConfig};
/// use std::path::PathBuf;
///
/// let pool = StoragePoolConfig {
///     name: "fast-nvme-0".into(),
///     role: PoolRole::Data,
///     root: PathBuf::from("/mnt/nvme0"),
///     weight: Some(2),
///     tech: PoolTech::Nvme,
///     health: Default::default(),
/// };
/// assert_eq!(pool.role, PoolRole::Data);
/// assert_eq!(pool.weight, Some(2));
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    /// Stable, human-readable pool name. Must be unique and non-empty.
    pub name: String,
    /// Pool purpose; role cardinality is enforced by
    /// [`StorageConfig::validate`].
    pub role: PoolRole,
    /// Mountpoint directory that is this pool's entire failure domain.
    /// Must be absolute and unique across pools.
    pub root: PathBuf,
    /// Placement weight; `None` = auto-derive from capacity at runtime (f2).
    /// When set, must be `> 0`.
    pub weight: Option<u32>,
    /// Device technology class. Default: `Auto` (resolved in f2).
    #[serde(default)]
    pub tech: PoolTech,
    /// Per-pool health overrides, merged field-by-field over the global
    /// `[storage.health]` table and the hard-coded defaults (f5 D4).
    /// Default: no overrides.
    #[serde(default)]
    pub health: PoolHealthOverride,
}

// ---------------------------------------------------------------------------
// StorageConfig
// ---------------------------------------------------------------------------

/// The `[storage]` section of `NodeConfig`: the node's storage-pool topology.
///
/// Storage pools are **mandatory** (ADR-0031): an empty `pools` list fails
/// [`StorageConfig::validate`] with an error naming the required roles —
/// the legacy single-`data_dir` fallback was removed.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{MissingRootPolicy, PoolRole, PoolTech, StorageConfig, StoragePoolConfig};
/// use std::path::{Path, PathBuf};
///
/// let config = StorageConfig {
///     pools: vec![
///         StoragePoolConfig {
///             name: "fast-nvme-0".into(),
///             role: PoolRole::Data,
///             root: PathBuf::from("/mnt/nvme0"),
///             weight: Some(2),
///             tech: PoolTech::Nvme,
///             health: Default::default(),
///         },
///         StoragePoolConfig {
///             name: "journal".into(),
///             role: PoolRole::Wal,
///             root: PathBuf::from("/mnt/optane0"),
///             weight: None,
///             tech: PoolTech::Auto,
///             health: Default::default(),
///         },
///         StoragePoolConfig {
///             name: "meta".into(),
///             role: PoolRole::Metadata,
///             root: PathBuf::from("/mnt/optane1"),
///             weight: None,
///             tech: PoolTech::Auto,
///             health: Default::default(),
///         },
///         StoragePoolConfig {
///             name: "hints".into(),
///             role: PoolRole::Hints,
///             root: PathBuf::from("/mnt/hints0"),
///             weight: None,
///             tech: PoolTech::Auto,
///             health: Default::default(),
///         },
///     ],
///     missing_root_policy: MissingRootPolicy::Fatal,
///     health: Default::default(),
/// };
/// assert!(config.validate(Path::new("/var/lib/oceanfs")).is_ok());
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Configured storage pools. Must declare at least one `data` pool and
    /// exactly one `wal`, `metadata`, and `hints` pool each (ADR-0031).
    pub pools: Vec<PoolConfig>,
    /// Node-global health-detector defaults (f5 D4). Per-pool
    /// `health = { ... }` tables override these field-by-field. The
    /// monitor-level keys (`monitor_tick_interval_secs`,
    /// `event_capacity`, `hints_probe_divisor`) live **only** here.
    pub health: PoolHealthOverride,
    /// Startup policy for a pool whose root is missing or unprobeable.
    /// Default: `Fatal`.
    pub missing_root_policy: MissingRootPolicy,
}

impl StorageConfig {
    /// Resolves a pool's effective health config: hard-coded defaults ←
    /// `[storage.health]` ← the pool's inline `health` table.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::{PoolHealthConfig, StorageConfig};
    ///
    /// let config = StorageConfig::default();
    /// assert_eq!(config.health.error_rate_threshold, None);
    /// // No pools to resolve against; the helper is exercised in config tests.
    /// let base: PoolHealthConfig = PoolHealthConfig::default();
    /// assert_eq!(base.min_errors, 3);
    /// ```
    pub fn resolved_pool_health(&self, pool: &PoolConfig) -> PoolHealthConfig {
        let global = self.health.resolve(&PoolHealthConfig::default());
        pool.health.resolve(&global)
    }

    /// Global monitor tick override in seconds (`None` = per-pool
    /// `detection_window_secs` cadence).
    pub fn monitor_tick_interval_secs(&self) -> Option<u64> {
        self.health.monitor_tick_interval_secs
    }

    /// Global status-event channel capacity (default `64`, perf 2.6).
    pub fn event_capacity(&self) -> usize {
        self.health.event_capacity.unwrap_or(64)
    }

    /// Hints-root probe cadence divisor (default `6`, minimum `1`): the
    /// probe runs every `detection_window_secs / divisor` seconds.
    pub fn hints_probe_divisor(&self) -> u64 {
        self.health.hints_probe_divisor.unwrap_or(6).max(1)
    }
}

impl StorageConfig {
    /// Validates the storage topology against the ADR-0029 §D8 + ADR-0031
    /// rules.
    ///
    /// Every node must declare a pool topology (the legacy empty-list
    /// fallback is gone). The list must satisfy:
    ///
    /// - at least one `data` pool;
    /// - exactly one `wal`, exactly one `metadata`, and exactly one
    ///   `hints` pool (role pinning, ADR-0029 §D8);
    /// - pool names non-empty and unique;
    /// - pool roots absolute and unique (one root per pool — the schema
    ///   itself only carries one `root` field);
    /// - `weight > 0` when set;
    /// - health knobs sane: `error_rate_threshold` in `(0, 1)`, all windows
    ///   `> 0`;
    /// - no pool root overlaps `data_dir` (pool roots must be disjoint from
    ///   the node's data directory).
    ///
    /// # Errors
    ///
    /// Returns a human-readable message describing the first rule violated;
    /// an empty pool list names the required roles.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::{MissingRootPolicy, PoolRole, StorageConfig, StoragePoolConfig};
    /// use std::path::{Path, PathBuf};
    ///
    /// fn pool(name: &str, role: PoolRole, root: &str) -> StoragePoolConfig {
    ///     StoragePoolConfig {
    ///         name: name.into(),
    ///         role,
    ///         root: PathBuf::from(root),
    ///         weight: None,
    ///         tech: Default::default(),
    ///         health: Default::default(),
    ///     }
    /// }
    ///
    /// let config = StorageConfig {
    ///     pools: vec![
    ///         pool("fast-nvme-0", PoolRole::Data, "/mnt/nvme0"),
    ///         pool("journal", PoolRole::Wal, "/mnt/optane0"),
    ///         pool("meta", PoolRole::Metadata, "/mnt/optane1"),
    ///         pool("hints", PoolRole::Hints, "/mnt/hints0"),
    ///     ],
    ///     missing_root_policy: MissingRootPolicy::Fatal,
    ///     health: Default::default(),
    /// };
    /// assert!(config.validate(Path::new("/var/lib/oceanfs")).is_ok());
    ///
    /// // An empty list is refused with a role-listing error (ADR-0031).
    /// assert!(StorageConfig::default().validate(Path::new("/var/lib/oceanfs")).is_err());
    /// ```
    pub fn validate(&self, data_dir: &Path) -> Result<(), String> {
        // Storage pools are mandatory (ADR-0031 D1): the legacy
        // single-`data_dir` fallback is removed. Every node must declare
        // at least one `data` pool and exactly one of each pinned role.
        if self.pools.is_empty() {
            return Err("at least one 'data', 'wal', 'metadata', and 'hints' pool is required; \
                 storage pools are mandatory (ADR-0031)"
                .to_string());
        }

        // Global monitor knobs (f5 D4) are `[storage.health]`-only and
        // must be usable when set.
        if let Some(tick) = self.health.monitor_tick_interval_secs {
            if tick == 0 {
                return Err("[storage.health].monitor_tick_interval_secs must be > 0".to_string());
            }
        }
        if self.health.event_capacity == Some(0) {
            return Err("[storage.health].event_capacity must be > 0".to_string());
        }
        if self.health.hints_probe_divisor == Some(0) {
            return Err("[storage.health].hints_probe_divisor must be > 0".to_string());
        }

        // At least one data pool must exist so placement has somewhere to
        // spread segments.
        if !self.pools.iter().any(|pool| pool.role == PoolRole::Data) {
            return Err("at least one 'data' pool is required when storage pools are configured"
                .to_string());
        }

        // Pre-size to the configured pool count: bounded and known up front
        // (perf guideline 1.3).
        let mut names: HashSet<&str> = HashSet::with_capacity(self.pools.len());
        let mut roots: HashSet<&Path> = HashSet::with_capacity(self.pools.len());
        let mut wal_pools = 0usize;
        let mut metadata_pools = 0usize;
        let mut hints_pools = 0usize;

        for pool in &self.pools {
            // Pool names: non-empty and unique.
            let name = pool.name.trim();
            if name.is_empty() {
                return Err("pool name must be non-empty".to_string());
            }
            if !names.insert(name) {
                return Err(format!("duplicate pool name: '{}'", pool.name));
            }

            // Pool roots: absolute and unique (one root per pool).
            if !pool.root.is_absolute() {
                return Err(format!(
                    "pool '{}' root must be an absolute path, got '{}'",
                    pool.name,
                    pool.root.display()
                ));
            }
            if !roots.insert(pool.root.as_path()) {
                return Err(format!("duplicate pool root: '{}'", pool.root.display()));
            }

            // Count pinned roles for the post-loop exactly-one check.
            match pool.role {
                PoolRole::Data => {}
                PoolRole::Wal => wal_pools += 1,
                PoolRole::Metadata => metadata_pools += 1,
                PoolRole::Hints => hints_pools += 1,
            }

            // Weight: positive when set (None = auto from capacity).
            if let Some(weight) = pool.weight {
                if weight == 0 {
                    return Err(format!("pool '{}' weight must be > 0, got 0", pool.name));
                }
            }

            // Health knobs (resolved: defaults ← [storage.health] ← pool):
            // error-rate threshold in (0, 1), windows > 0, detector knobs
            // sane. Monitor-level fields are rejected on the pool table.
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
            let health = self.resolved_pool_health(pool);
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
                    "pool '{}' health.trend_min_windows must be >= 3 (two window pairs), got {}",
                    pool.name, health.trend_min_windows
                ));
            }
            if health.history_max_windows < 4 {
                return Err(format!(
                    "pool '{}' health.history_max_windows must be >= 4, got {}",
                    pool.name, health.history_max_windows
                ));
            }

            // Pool roots must be disjoint from the node's data_dir: an
            // overlapping root would silently mix pool-managed paths with
            // node-managed ones.
            if paths_overlap(&pool.root, data_dir) {
                return Err(format!(
                    "pool '{}' root '{}' overlaps the legacy data_dir '{}'; \
                     pool roots must be disjoint from data_dir",
                    pool.name,
                    pool.root.display(),
                    data_dir.display()
                ));
            }
        }

        // Role pinning (ADR-0029 §D8 + ADR-0031): exactly one of each
        // pinned role. The ">1" checks run first so a config with two
        // `hints` pools reports the cardinality violation, not a missing
        // `wal`.
        if wal_pools > 1 {
            return Err("at most one 'wal' pool is allowed per node".to_string());
        }
        if metadata_pools > 1 {
            return Err("at most one 'metadata' pool is allowed per node".to_string());
        }
        if hints_pools > 1 {
            return Err("at most one 'hints' pool is allowed per node".to_string());
        }
        if wal_pools == 0 {
            return Err("exactly one 'wal' pool is required per node (role pinning, ADR-0029 §D8)"
                .to_string());
        }
        if metadata_pools == 0 {
            return Err(
                "exactly one 'metadata' pool is required per node (role pinning, ADR-0029 §D8)"
                    .to_string(),
            );
        }
        if hints_pools == 0 {
            return Err(
                "exactly one 'hints' pool is required per node (role pinning, ADR-0029 §D8)"
                    .to_string(),
            );
        }

        Ok(())
    }
}

/// Returns `true` when `a` and `b` refer to the same path or one is nested
/// inside the other (component-wise, so `/mnt/data` overlaps `/mnt/data` and
/// `/mnt/data/segments`).
fn paths_overlap(a: &Path, b: &Path) -> bool {
    a == b || a.starts_with(b) || b.starts_with(a)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::NodeConfig;

    /// A valid single-data-pool config rooted at a tempdir-free absolute
    /// path, disjoint from any realistic `data_dir`.
    fn data_pool(name: &str, root: &str, weight: Option<u32>) -> PoolConfig {
        PoolConfig {
            name: name.to_string(),
            role: PoolRole::Data,
            root: PathBuf::from(root),
            weight,
            tech: PoolTech::Auto,
            health: Default::default(),
        }
    }

    fn wal_pool(name: &str, root: &str) -> PoolConfig {
        PoolConfig {
            name: name.to_string(),
            role: PoolRole::Wal,
            root: PathBuf::from(root),
            weight: None,
            tech: PoolTech::Auto,
            health: Default::default(),
        }
    }

    fn metadata_pool(name: &str, root: &str) -> PoolConfig {
        PoolConfig {
            name: name.to_string(),
            role: PoolRole::Metadata,
            root: PathBuf::from(root),
            weight: None,
            tech: PoolTech::Auto,
            health: Default::default(),
        }
    }

    fn hints_pool(name: &str, root: &str) -> PoolConfig {
        PoolConfig {
            name: name.to_string(),
            role: PoolRole::Hints,
            root: PathBuf::from(root),
            weight: None,
            tech: PoolTech::Auto,
            health: Default::default(),
        }
    }

    /// The full 4-pool topology from the ADR-0029 §D8 example, at
    /// `StorageConfig` level (`[[pools]]`; the `[storage]`-wrapped variant is
    /// covered by `adr_d8_example_parses_inside_node_config`).
    ///
    /// NOTE: the ADR presents the `health` inline table wrapped across two
    /// lines for readability; TOML inline tables must be single-line, so the
    /// values below are the same data on one line.
    fn adr_d8_example() -> StorageConfig {
        let toml_str = r#"
            missing_root_policy = "fatal"

            [[pools]]
            name = "fast-nvme-0"
            role = "data"
            root = "/mnt/nvme0"
            weight = 2
            tech = "nvme"
            health = { error_rate_threshold = 0.001, min_errors = 3, latency_factor = 5.0, trend_window_secs = 300, detection_window_secs = 30, recovery_window_secs = 300 }

            [[pools]]
            name = "journal"
            role = "wal"
            root = "/mnt/optane0"

            [[pools]]
            name = "meta"
            role = "metadata"
            root = "/mnt/optane1"

            [[pools]]
            name = "hot-nvme"
            role = "data"
            root = "/mnt/nvme2"
            tech = "nvme"
            health = { error_rate_threshold = 0.001, min_errors = 3, latency_factor = 5.0, trend_window_secs = 300, detection_window_secs = 30, recovery_window_secs = 300 }

            [[pools]]
            name = "hints"
            role = "hints"
            root = "/mnt/nvme3"
        "#;
        toml::from_str(toml_str).expect("ADR §D8 example must deserialize")
    }

    // -- Defaults / mandatory pools (ADR-0031 D1) --

    #[test]
    fn default_storage_config_has_no_pools() {
        let config = StorageConfig::default();
        assert!(config.pools.is_empty());
        assert_eq!(config.missing_root_policy, MissingRootPolicy::Fatal);
    }

    #[test]
    fn empty_pools_rejected_with_role_listing_error() {
        // ADR-0031 D1: no silent legacy fallback — the empty list is
        // refused with an explicit role-listing error for any data dir.
        let config = StorageConfig::default();
        for dir in [Path::new("/var/lib/oceanfs"), Path::new("/")] {
            let err = config.validate(dir).unwrap_err();
            assert!(err.contains("'data'"), "message: {err}");
            assert!(err.contains("'wal'"), "message: {err}");
            assert!(err.contains("'metadata'"), "message: {err}");
            assert!(err.contains("'hints'"), "message: {err}");
            assert!(err.contains("mandatory"), "message: {err}");
        }
    }

    #[test]
    fn minimal_pool_entry_gets_defaults() {
        let toml_str = r#"
            [[pools]]
            name = "pool-a"
            role = "data"
            root = "/mnt/a"
        "#;
        let config: StorageConfig = toml::from_str(toml_str).unwrap();
        let pool = &config.pools[0];
        assert_eq!(pool.tech, PoolTech::Auto);
        assert_eq!(pool.weight, None);
        assert_eq!(pool.health, PoolHealthOverride::default());
        assert_eq!(config.missing_root_policy, MissingRootPolicy::Fatal);
    }

    // -- Serde round-trips --

    #[test]
    fn storage_config_serde_roundtrip_inline() {
        let config = StorageConfig {
            pools: vec![
                PoolConfig {
                    name: "pool-a".into(),
                    role: PoolRole::Data,
                    root: PathBuf::from("/mnt/a"),
                    weight: Some(2),
                    tech: PoolTech::Nvme,
                    health: PoolHealthOverride {
                        error_rate_threshold: Some(0.01),
                        ..Default::default()
                    },
                },
                data_pool("pool-b", "/mnt/b", None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Degraded,
        };

        let toml_str = toml::to_string(&config).unwrap();
        let roundtripped: StorageConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(roundtripped, config);
        assert_eq!(roundtripped.pools.len(), 2);
        assert_eq!(roundtripped.pools[0].weight, Some(2));
        assert_eq!(roundtripped.pools[0].tech, PoolTech::Nvme);
        assert_eq!(roundtripped.missing_root_policy, MissingRootPolicy::Degraded);
    }

    #[test]
    fn storage_config_serde_roundtrip_from_file() {
        let config = adr_d8_example();
        let toml_str = toml::to_string(&config).unwrap();

        let dir = std::env::temp_dir()
            .join(format!("oceanfs-storage-config-test-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("storage.toml");
        std::fs::write(&path, &toml_str).unwrap();

        let from_file = std::fs::read_to_string(&path).unwrap();
        let roundtripped: StorageConfig = toml::from_str(&from_file).unwrap();
        assert_eq!(roundtripped, config);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn role_tech_policy_serde_roundtrip_all_variants() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Wrapper {
            role: PoolRole,
            tech: PoolTech,
            policy: MissingRootPolicy,
        }

        for role in [PoolRole::Data, PoolRole::Wal, PoolRole::Metadata, PoolRole::Hints] {
            for tech in [
                PoolTech::Auto,
                PoolTech::Hdd,
                PoolTech::Ssd,
                PoolTech::Nvme,
                PoolTech::CloudEphemeral,
            ] {
                for policy in [MissingRootPolicy::Fatal, MissingRootPolicy::Degraded] {
                    let wrapper = Wrapper { role, tech, policy };
                    let toml_str = toml::to_string(&wrapper).unwrap();
                    let roundtripped: Wrapper = toml::from_str(&toml_str).unwrap();
                    assert_eq!(roundtripped, wrapper);
                }
            }
        }
    }

    #[test]
    fn pool_role_serializes_lowercase() {
        #[derive(serde::Serialize)]
        struct RoleWrapper {
            role: PoolRole,
        }

        let cases = [
            (PoolRole::Data, "role = \"data\""),
            (PoolRole::Wal, "role = \"wal\""),
            (PoolRole::Metadata, "role = \"metadata\""),
            (PoolRole::Hints, "role = \"hints\""),
        ];
        for (role, expected) in cases {
            let toml_str = toml::to_string(&RoleWrapper { role }).unwrap();
            assert!(toml_str.contains(expected), "got: {toml_str}");
        }
    }

    #[test]
    fn pool_tech_serializes_per_adr_notation() {
        #[derive(serde::Serialize)]
        struct TechWrapper {
            tech: PoolTech,
        }

        // ADR-0029 §D8 comment: `tech = "nvme" # hdd | ssd | nvme | cloud-ephemeral`
        let cases = [
            (PoolTech::Auto, "tech = \"auto\""),
            (PoolTech::Hdd, "tech = \"hdd\""),
            (PoolTech::Ssd, "tech = \"ssd\""),
            (PoolTech::Nvme, "tech = \"nvme\""),
            (PoolTech::CloudEphemeral, "tech = \"cloud-ephemeral\""),
        ];
        for (tech, expected) in cases {
            let toml_str = toml::to_string(&TechWrapper { tech }).unwrap();
            assert!(toml_str.contains(expected), "got: {toml_str}");
        }
    }

    // -- ADR §D8 example --

    /// The full example `[storage]` block from ADR-0029 §D8 deserializes and
    /// validates (epic DoD: integration acceptance for f1).
    #[test]
    fn adr_d8_example_deserializes_and_validates() {
        let config = adr_d8_example();
        assert_eq!(config.pools.len(), 5);
        assert_eq!(config.pools[0].name, "fast-nvme-0");
        assert_eq!(config.pools[1].role, PoolRole::Wal);
        assert_eq!(config.pools[2].role, PoolRole::Metadata);
        assert_eq!(config.pools[3].name, "hot-nvme");
        assert_eq!(config.pools[4].role, PoolRole::Hints);

        assert!(config.validate(Path::new("/var/lib/oceanfs")).is_ok());
    }

    /// The example also parses when mounted as `NodeConfig.storage`
    /// (`[storage]` section of `oceanfs.toml`).
    #[test]
    fn adr_d8_example_parses_inside_node_config() {
        let toml_str = r#"
            node_id = "node-1"

            [storage]
            missing_root_policy = "fatal"

            [[storage.pools]]
            name = "fast-nvme-0"
            role = "data"
            root = "/mnt/nvme0"
            weight = 2
            tech = "nvme"

            [[storage.pools]]
            name = "journal"
            role = "wal"
            root = "/mnt/optane0"

            [[storage.pools]]
            name = "meta"
            role = "metadata"
            root = "/mnt/optane1"

            [[storage.pools]]
            name = "hints"
            role = "hints"
            root = "/mnt/nvme3"
        "#;
        let config: NodeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.storage.pools.len(), 4);
        assert_eq!(config.storage.pools[0].name, "fast-nvme-0");
        assert_eq!(config.storage.pools[3].role, PoolRole::Hints);
        assert!(config.storage.validate(&config.data_dir).is_ok());
    }

    // -- Validation rules --

    #[test]
    fn validate_duplicate_pool_name_rejected() {
        let config = StorageConfig {
            pools: vec![
                data_pool("same-name", "/mnt/a", None),
                data_pool("same-name", "/mnt/b", None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("duplicate pool name"), "message: {err}");
    }

    #[test]
    fn validate_empty_pool_name_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("  ", "/mnt/a", None)],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("non-empty"), "message: {err}");
    }

    #[test]
    fn validate_duplicate_pool_root_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "/mnt/a", None), data_pool("pool-b", "/mnt/a", None)],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("duplicate pool root"), "message: {err}");
    }

    #[test]
    fn validate_non_absolute_root_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "mnt/a", None)],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("absolute"), "message: {err}");
    }

    #[test]
    fn validate_missing_data_pool_rejected() {
        let config = StorageConfig {
            pools: vec![wal_pool("journal", "/mnt/wal"), metadata_pool("meta", "/mnt/meta")],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("data' pool"), "message: {err}");
    }

    #[test]
    fn data_only_topology_rejected_missing_pinned_roles() {
        // ADR-0031: wal/metadata/hints are mandatory (role pinning).
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "/mnt/a", None)],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("'wal' pool is required"), "message: {err}");
    }

    #[test]
    fn topology_missing_hints_pool_rejected() {
        let config = StorageConfig {
            pools: vec![
                data_pool("pool-a", "/mnt/a", None),
                wal_pool("journal", "/mnt/wal"),
                metadata_pool("meta", "/mnt/meta"),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("'hints' pool is required"), "message: {err}");
    }

    #[test]
    fn validate_two_wal_pools_rejected() {
        let config = StorageConfig {
            pools: vec![
                wal_pool("journal-a", "/mnt/wal-a"),
                wal_pool("journal-b", "/mnt/wal-b"),
                data_pool("pool-a", "/mnt/a", None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("at most one 'wal' pool"), "message: {err}");
    }

    #[test]
    fn validate_two_metadata_pools_rejected() {
        let config = StorageConfig {
            pools: vec![
                metadata_pool("meta-a", "/mnt/meta-a"),
                metadata_pool("meta-b", "/mnt/meta-b"),
                data_pool("pool-a", "/mnt/a", None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("at most one 'metadata' pool"), "message: {err}");
    }

    #[test]
    fn validate_two_hints_pools_rejected() {
        let config = StorageConfig {
            pools: vec![
                hints_pool("hints-a", "/mnt/hints-a"),
                hints_pool("hints-b", "/mnt/hints-b"),
                data_pool("pool-a", "/mnt/a", None),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("at most one 'hints' pool"), "message: {err}");
    }

    #[test]
    fn validate_zero_weight_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "/mnt/a", Some(0))],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("weight must be > 0"), "message: {err}");
    }

    #[test]
    fn validate_health_threshold_out_of_range_rejected() {
        for bad_threshold in [0.0, 1.0, -0.5, 1.5] {
            let config = StorageConfig {
                pools: vec![PoolConfig {
                    name: "pool-a".into(),
                    role: PoolRole::Data,
                    root: PathBuf::from("/mnt/a"),
                    weight: None,
                    tech: PoolTech::Auto,
                    health: PoolHealthOverride {
                        error_rate_threshold: Some(bad_threshold),
                        ..Default::default()
                    },
                }],
                health: Default::default(),
                missing_root_policy: MissingRootPolicy::Fatal,
            };
            let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
            assert!(err.contains("error_rate_threshold"), "message: {err}");
        }
    }

    #[test]
    fn validate_zero_health_windows_rejected() {
        let health = PoolHealthOverride { trend_window_secs: Some(0), ..Default::default() };
        let config = StorageConfig {
            pools: vec![PoolConfig {
                name: "pool-a".into(),
                role: PoolRole::Data,
                root: PathBuf::from("/mnt/a"),
                weight: None,
                tech: PoolTech::Auto,
                health,
            }],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("health windows"), "message: {err}");

        let health = PoolHealthOverride { detection_window_secs: Some(0), ..Default::default() };
        let config = StorageConfig {
            pools: vec![PoolConfig {
                name: "pool-a".into(),
                role: PoolRole::Data,
                root: PathBuf::from("/mnt/a"),
                weight: None,
                tech: PoolTech::Auto,
                health,
            }],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        assert!(config.validate(Path::new("/var/lib/oceanfs")).is_err());

        let health = PoolHealthOverride { recovery_window_secs: Some(0), ..Default::default() };
        let config = StorageConfig {
            pools: vec![PoolConfig {
                name: "pool-a".into(),
                role: PoolRole::Data,
                root: PathBuf::from("/mnt/a"),
                weight: None,
                tech: PoolTech::Auto,
                health,
            }],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        assert!(config.validate(Path::new("/var/lib/oceanfs")).is_err());
    }

    #[test]
    fn validate_root_equal_to_data_dir_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "/var/lib/oceanfs", None)],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("overlaps the legacy data_dir"), "message: {err}");
    }

    #[test]
    fn validate_root_nested_inside_data_dir_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "/var/lib/oceanfs/segments", None)],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("overlaps the legacy data_dir"), "message: {err}");
    }

    #[test]
    fn validate_ok_with_multiple_data_pools() {
        let config = StorageConfig {
            pools: vec![
                data_pool("pool-a", "/mnt/a", Some(2)),
                data_pool("pool-b", "/mnt/b", None),
                wal_pool("journal", "/mnt/wal"),
                metadata_pool("meta", "/mnt/meta"),
                hints_pool("hints", "/mnt/hints"),
            ],
            health: Default::default(),
            missing_root_policy: MissingRootPolicy::Degraded,
        };
        assert!(config.validate(Path::new("/var/lib/oceanfs")).is_ok());
    }

    // -- Deserialization-level rejections (clear messages) --

    /// A multi-root attempt (`root` as an array) cannot be expressed in the
    /// schema and must fail deserialization with a clear error.
    #[test]
    fn multi_root_pool_rejected_at_deserialization() {
        let toml_str = r#"
            [[pools]]
            name = "pool-a"
            role = "data"
            root = ["/mnt/a", "/mnt/b"]
        "#;
        let err = toml::from_str::<StorageConfig>(toml_str).unwrap_err();
        assert!(err.to_string().contains("root"), "message: {err}");
    }

    /// An unknown tech value fails at deserialization (the enum only accepts
    /// the documented device classes).
    #[test]
    fn invalid_tech_rejected_at_deserialization() {
        let toml_str = r#"
            [[pools]]
            name = "pool-a"
            role = "data"
            root = "/mnt/a"
            tech = "sata"
        "#;
        let err = toml::from_str::<StorageConfig>(toml_str).unwrap_err();
        assert!(err.to_string().contains("tech"), "message: {err}");
    }

    /// The global-shape health attempt (`[storage.pools.health]` as an array
    /// of tables) is rejected: `health` is an inline table on each pool, not
    /// a global block.
    #[test]
    fn malformed_global_health_block_rejected() {
        let toml_str = r#"
            [storage]
            missing_root_policy = "fatal"

            [[storage.pools]]
            name = "pool-a"
            role = "data"
            root = "/mnt/a"

            [[storage.pools.health]]
            error_rate_threshold = 0.5
        "#;
        let err = toml::from_str::<NodeConfig>(toml_str).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("PoolHealthConfig") || message.contains("health"),
            "clear message expected, got: {message}"
        );
    }

    /// A stray top-level key under `[storage]` (e.g. a typo) is rejected
    /// rather than silently ignored.
    #[test]
    fn unknown_storage_key_rejected() {
        let toml_str = r#"
            node_id = "node-1"

            [storage]
            not_a_real_key = 1
        "#;
        let err = toml::from_str::<NodeConfig>(toml_str).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("not_a_real_key"), "message: {message}");
    }

    /// f5 D4: `[storage.health]` global defaults merge field-by-field with
    /// per-pool inline overrides (per-pool wins); monitor-level keys work
    /// at the global level only.
    #[test]
    fn storage_health_global_defaults_merge_with_pool_overrides() {
        let toml_str = r#"
            node_id = "node-1"

            [storage.health]
            latency_factor = 2.0
            trend_doubling_factor = 1.5
            monitor_tick_interval_secs = 5
            event_capacity = 32
            hints_probe_divisor = 3

            [[storage.pools]]
            name = "data-0"
            role = "data"
            root = "/mnt/a"
            health = { latency_factor = 1.25, min_errors = 9 }

            [[storage.pools]]
            name = "journal"
            role = "wal"
            root = "/mnt/journal"

            [[storage.pools]]
            name = "meta"
            role = "metadata"
            root = "/mnt/meta"

            [[storage.pools]]
            name = "hints"
            role = "hints"
            root = "/mnt/hints"
        "#;
        let node: NodeConfig = toml::from_str(toml_str).unwrap();
        let data = node.storage.pools.iter().find(|p| p.name == "data-0").unwrap();
        let resolved = node.storage.resolved_pool_health(data);
        assert_eq!(resolved.latency_factor, 1.25, "per-pool override wins");
        assert_eq!(resolved.min_errors, 9, "per-pool override wins");
        assert_eq!(resolved.trend_doubling_factor, 1.5, "global default applies");
        assert_eq!(resolved.detection_window_secs, 30, "hard-coded default stays");
        assert_eq!(resolved.trend_min_windows, 3, "hard-coded default stays");

        let journal = node.storage.pools.iter().find(|p| p.name == "journal").unwrap();
        assert_eq!(
            node.storage.resolved_pool_health(journal).latency_factor,
            2.0,
            "global default applies to pools without overrides"
        );

        assert_eq!(node.storage.monitor_tick_interval_secs(), Some(5));
        assert_eq!(node.storage.event_capacity(), 32);
        assert_eq!(node.storage.hints_probe_divisor(), 3);
        assert!(node.storage.validate(&node.data_dir).is_ok());
    }

    /// f5 D4: monitor-level keys are rejected on a pool's inline table.
    #[test]
    fn storage_health_monitor_keys_rejected_on_pool_table() {
        let toml_str = r#"
            node_id = "node-1"

            [[storage.pools]]
            name = "data-0"
            role = "data"
            root = "/mnt/a"
            health = { monitor_tick_interval_secs = 5 }

            [[storage.pools]]
            name = "journal"
            role = "wal"
            root = "/mnt/journal"

            [[storage.pools]]
            name = "meta"
            role = "metadata"
            root = "/mnt/meta"

            [[storage.pools]]
            name = "hints"
            role = "hints"
            root = "/mnt/hints"
        "#;
        let node: NodeConfig = toml::from_str(toml_str).unwrap();
        let err = node.storage.validate(&node.data_dir).unwrap_err();
        assert!(err.contains("monitor-level"), "message: {err}");
    }

    /// f5 D4: invalid detector knobs are rejected with clear messages.
    #[test]
    fn storage_health_invalid_detector_knobs_rejected() {
        let health_toml = |health: &str| {
            format!(
                r#"
                node_id = "node-1"

                [storage.health]
                {health}

                [[storage.pools]]
                name = "data-0"
                role = "data"
                root = "/mnt/a"

                [[storage.pools]]
                name = "journal"
                role = "wal"
                root = "/mnt/journal"

                [[storage.pools]]
                name = "meta"
                role = "metadata"
                root = "/mnt/meta"

                [[storage.pools]]
                name = "hints"
                role = "hints"
                root = "/mnt/hints"
            "#
            )
        };
        for (health, needle) in [
            ("trend_doubling_factor = 1.0", "trend_doubling_factor"),
            ("trend_min_windows = 2", "trend_min_windows"),
            ("history_max_windows = 3", "history_max_windows"),
            ("monitor_tick_interval_secs = 0", "monitor_tick_interval_secs"),
            ("event_capacity = 0", "event_capacity"),
            ("hints_probe_divisor = 0", "hints_probe_divisor"),
        ] {
            let node: NodeConfig = toml::from_str(&health_toml(health)).unwrap();
            let err = node.storage.validate(&node.data_dir).unwrap_err();
            assert!(err.contains(needle), "health {health:?}: expected {needle}, got {err}");
        }
    }
}
