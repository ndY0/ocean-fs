//! Storage pool configuration (ADR-0029 §D8).
//!
//! Introduces the disk-topology config surface: a node declares its disks
//! as *storage pools*, each with a role (`data | wal | metadata | hints`),
//! exactly one root (one pool = one root = one failure domain), an optional
//! placement weight, a device-tech hint, and per-pool health tuning knobs
//! (carried now, consumed by Phase B's health monitor).
//!
//! The zero-config fallback is preserved: an empty `[storage.pools]` list
//! means "single `data_dir`", byte-for-byte today's behavior. Migration to
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
/// At most one `wal`, `metadata`, and `hints` pool may be configured; any
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
// PoolHealthConfig
// ---------------------------------------------------------------------------

/// Per-pool health-monitor tuning knobs (ADR-0029 §D3).
///
/// Carried in config and validated now; consumed by Phase B's health
/// monitor, which detects trend-based, tech-aware disk failure. All fields
/// have built-in defaults, so a minimal pool entry only needs `name`, `role`,
/// and `root`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::PoolHealthConfig;
///
/// let health = PoolHealthConfig::default();
/// assert_eq!(health.error_rate_threshold, 0.001);
/// assert_eq!(health.trend_window_secs, 300);
/// assert_eq!(health.detection_window_secs, 30);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
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
    /// Per-pool health knobs. Default: [`PoolHealthConfig::default`].
    #[serde(default)]
    pub health: PoolHealthConfig,
}

// ---------------------------------------------------------------------------
// StorageConfig
// ---------------------------------------------------------------------------

/// The `[storage]` section of `NodeConfig`: the node's storage-pool topology.
///
/// An empty `pools` list is the zero-config fallback (ADR-0029 §D8): the
/// node behaves exactly as before, using the legacy single `data_dir` for
/// everything. Migration to pools is explicit and never automatic.
///
/// # Examples
///
/// ```
/// use oceanfs_core::StorageConfig;
///
/// // Legacy mode: no pools configured.
/// let config = StorageConfig::default();
/// assert!(config.pools.is_empty());
/// assert!(config.validate(std::path::Path::new("/var/lib/oceanfs")).is_ok());
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Configured storage pools. Empty = legacy single-`data_dir` mode.
    pub pools: Vec<PoolConfig>,
    /// Startup policy for a pool whose root is missing or unprobeable.
    /// Default: `Fatal`.
    pub missing_root_policy: MissingRootPolicy,
}

impl StorageConfig {
    /// Validates the storage topology against the ADR-0029 §D8 rules.
    ///
    /// The empty-pool list is the legacy fallback and validates nothing —
    /// every existing config stays valid. A non-empty list must satisfy:
    ///
    /// - at least one `data` pool;
    /// - pool names non-empty and unique;
    /// - pool roots absolute and unique (one root per pool — the schema
    ///   itself only carries one `root` field);
    /// - at most one `wal`, `metadata`, and `hints` pool each;
    /// - `weight > 0` when set;
    /// - health knobs sane: `error_rate_threshold` in `(0, 1)`, all windows
    ///   `> 0`;
    /// - no pool root overlaps the legacy `data_dir` (pool mode and legacy
    ///   mode are mutually exclusive layouts; overlap would silently mix
    ///   pool-managed paths with legacy-managed ones).
    ///
    /// # Errors
    ///
    /// Returns a human-readable message describing the first rule violated.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::{PoolRole, StorageConfig, StoragePoolConfig};
    /// use std::path::{Path, PathBuf};
    ///
    /// let config = StorageConfig {
    ///     pools: vec![StoragePoolConfig {
    ///         name: "fast-nvme-0".into(),
    ///         role: PoolRole::Data,
    ///         root: PathBuf::from("/mnt/nvme0"),
    ///         weight: None,
    ///         tech: Default::default(),
    ///         health: Default::default(),
    ///     }],
    ///     missing_root_policy: Default::default(),
    /// };
    /// assert!(config.validate(Path::new("/var/lib/oceanfs")).is_ok());
    /// ```
    pub fn validate(&self, data_dir: &Path) -> Result<(), String> {
        // Legacy zero-config fallback: no pools = single data_dir (today's
        // behavior, byte-for-byte). Nothing to validate.
        if self.pools.is_empty() {
            return Ok(());
        }

        // At least one data pool must exist when pools are configured, so
        // placement has somewhere to spread segments.
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

            // Role cardinality: wal/metadata/hints are at most one each.
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

            // Health knobs: error-rate threshold in (0, 1), windows > 0.
            let health = &pool.health;
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

            // Pool roots must be disjoint from the legacy data_dir: pool mode
            // and legacy mode are mutually exclusive layouts, and overlap
            // would silently mix pool-managed paths with legacy-managed ones.
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

        if wal_pools > 1 {
            return Err("at most one 'wal' pool is allowed per node".to_string());
        }
        if metadata_pools > 1 {
            return Err("at most one 'metadata' pool is allowed per node".to_string());
        }
        if hints_pools > 1 {
            return Err("at most one 'hints' pool is allowed per node".to_string());
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
            health: PoolHealthConfig::default(),
        }
    }

    fn wal_pool(name: &str, root: &str) -> PoolConfig {
        PoolConfig {
            name: name.to_string(),
            role: PoolRole::Wal,
            root: PathBuf::from(root),
            weight: None,
            tech: PoolTech::Auto,
            health: PoolHealthConfig::default(),
        }
    }

    fn metadata_pool(name: &str, root: &str) -> PoolConfig {
        PoolConfig {
            name: name.to_string(),
            role: PoolRole::Metadata,
            root: PathBuf::from(root),
            weight: None,
            tech: PoolTech::Auto,
            health: PoolHealthConfig::default(),
        }
    }

    fn hints_pool(name: &str, root: &str) -> PoolConfig {
        PoolConfig {
            name: name.to_string(),
            role: PoolRole::Hints,
            root: PathBuf::from(root),
            weight: None,
            tech: PoolTech::Auto,
            health: PoolHealthConfig::default(),
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
        "#;
        toml::from_str(toml_str).expect("ADR §D8 example must deserialize")
    }

    // -- Defaults / legacy fallback --

    #[test]
    fn default_storage_config_is_legacy_mode() {
        let config = StorageConfig::default();
        assert!(config.pools.is_empty());
        assert_eq!(config.missing_root_policy, MissingRootPolicy::Fatal);
    }

    #[test]
    fn legacy_fallback_validate_accepts_any_data_dir() {
        let config = StorageConfig::default();
        assert!(config.validate(Path::new("/var/lib/oceanfs")).is_ok());
        assert!(config.validate(Path::new("/")).is_ok());
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
        assert_eq!(pool.health, PoolHealthConfig::default());
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
                    health: PoolHealthConfig {
                        error_rate_threshold: 0.01,
                        ..PoolHealthConfig::default()
                    },
                },
                data_pool("pool-b", "/mnt/b", None),
            ],
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
        assert_eq!(config.pools.len(), 4);
        assert_eq!(config.pools[0].name, "fast-nvme-0");
        assert_eq!(config.pools[1].role, PoolRole::Wal);
        assert_eq!(config.pools[2].role, PoolRole::Metadata);
        assert_eq!(config.pools[3].name, "hot-nvme");

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
        "#;
        let config: NodeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.storage.pools.len(), 1);
        assert_eq!(config.storage.pools[0].name, "fast-nvme-0");
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
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("duplicate pool name"), "message: {err}");
    }

    #[test]
    fn validate_empty_pool_name_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("  ", "/mnt/a", None)],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("non-empty"), "message: {err}");
    }

    #[test]
    fn validate_duplicate_pool_root_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "/mnt/a", None), data_pool("pool-b", "/mnt/a", None)],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("duplicate pool root"), "message: {err}");
    }

    #[test]
    fn validate_non_absolute_root_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "mnt/a", None)],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("absolute"), "message: {err}");
    }

    #[test]
    fn validate_missing_data_pool_rejected() {
        let config = StorageConfig {
            pools: vec![wal_pool("journal", "/mnt/wal"), metadata_pool("meta", "/mnt/meta")],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("data' pool"), "message: {err}");
    }

    #[test]
    fn validate_missing_wal_pool_is_allowed() {
        // wal/metadata/hints are optional; only data is mandatory.
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "/mnt/a", None)],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        assert!(config.validate(Path::new("/var/lib/oceanfs")).is_ok());
    }

    #[test]
    fn validate_two_wal_pools_rejected() {
        let config = StorageConfig {
            pools: vec![
                wal_pool("journal-a", "/mnt/wal-a"),
                wal_pool("journal-b", "/mnt/wal-b"),
                data_pool("pool-a", "/mnt/a", None),
            ],
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
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("at most one 'hints' pool"), "message: {err}");
    }

    #[test]
    fn validate_zero_weight_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "/mnt/a", Some(0))],
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
                    health: PoolHealthConfig {
                        error_rate_threshold: bad_threshold,
                        ..PoolHealthConfig::default()
                    },
                }],
                missing_root_policy: MissingRootPolicy::Fatal,
            };
            let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
            assert!(err.contains("error_rate_threshold"), "message: {err}");
        }
    }

    #[test]
    fn validate_zero_health_windows_rejected() {
        let mut health = PoolHealthConfig::default();
        health.trend_window_secs = 0;
        let config = StorageConfig {
            pools: vec![PoolConfig {
                name: "pool-a".into(),
                role: PoolRole::Data,
                root: PathBuf::from("/mnt/a"),
                weight: None,
                tech: PoolTech::Auto,
                health,
            }],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("health windows"), "message: {err}");

        let mut health = PoolHealthConfig::default();
        health.detection_window_secs = 0;
        let config = StorageConfig {
            pools: vec![PoolConfig {
                name: "pool-a".into(),
                role: PoolRole::Data,
                root: PathBuf::from("/mnt/a"),
                weight: None,
                tech: PoolTech::Auto,
                health,
            }],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        assert!(config.validate(Path::new("/var/lib/oceanfs")).is_err());

        let mut health = PoolHealthConfig::default();
        health.recovery_window_secs = 0;
        let config = StorageConfig {
            pools: vec![PoolConfig {
                name: "pool-a".into(),
                role: PoolRole::Data,
                root: PathBuf::from("/mnt/a"),
                weight: None,
                tech: PoolTech::Auto,
                health,
            }],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        assert!(config.validate(Path::new("/var/lib/oceanfs")).is_err());
    }

    #[test]
    fn validate_root_equal_to_data_dir_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "/var/lib/oceanfs", None)],
            missing_root_policy: MissingRootPolicy::Fatal,
        };
        let err = config.validate(Path::new("/var/lib/oceanfs")).unwrap_err();
        assert!(err.contains("overlaps the legacy data_dir"), "message: {err}");
    }

    #[test]
    fn validate_root_nested_inside_data_dir_rejected() {
        let config = StorageConfig {
            pools: vec![data_pool("pool-a", "/var/lib/oceanfs/segments", None)],
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
            ],
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

    /// A stray top-level key under `[storage]` (e.g. a global `health` block)
    /// is rejected rather than silently ignored.
    #[test]
    fn unknown_storage_key_rejected() {
        let toml_str = r#"
            [storage]
            health = { error_rate_threshold = 0.5 }
        "#;
        let err = toml::from_str::<NodeConfig>(toml_str).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("health"), "message: {message}");
    }
}
