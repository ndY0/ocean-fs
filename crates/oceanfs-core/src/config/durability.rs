//! Scheduler/budget-level durability configuration (ADR-0017 amendment).

/// Scheduler/budget-level durability configuration (ADR-0017 amendment).
///
/// Controls the two-tier admission budget shared by the durability
/// subsystem (ADR-0017 amendment 2026-09-06):
///
/// - `repair_max_active` (Tier-0) bounds concurrent repair operations
///   node-wide — heal ops, re-replication pulls/writes, and inbound hint
///   batches draw from the same budget (the single gate replacing each
///   worker's private semaphore).
/// - `housekeeping_max_active` (Tier-1) bounds concurrent scheduled
///   housekeeping cycles (GC, orphan reaper, scrub, AE).
/// - `drain_max_bytes_per_tick` / `drain_interval_sec` pace the intra-node
///   drain task (ADR-0036 D5 — its own configurable byte budget, not a
///   shared framework).
///
/// Tier-0 work is never gated behind Tier-1 activity; within a tier
/// admission is FIFO-fair.
///
/// # Examples
///
/// ```
/// use oceanfs_core::DurabilityConfig;
///
/// let config = DurabilityConfig::default();
/// assert_eq!(config.repair_max_active, 16);
/// assert_eq!(config.housekeeping_max_active, 2);
/// assert_eq!(config.task_timeout_sec, 3600);
///     assert_eq!(config.drain_max_bytes_per_tick, 256 * 1024 * 1024);
///     assert_eq!(config.drain_cluster_max_bytes_per_tick, 64 * 1024 * 1024);
///     assert_eq!(config.drain_interval_sec, 1);
///     assert_eq!(config.repair_unrecoverable_sweeps, 3);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct DurabilityConfig {
    /// Tier-0 (repair) permits — bounds concurrent heal ops, re-rep
    /// pulls/writes, and inbound hint batches node-wide. Default 16.
    pub repair_max_active: usize,
    /// Tier-1 (housekeeping) permits — bounds concurrent scheduled
    /// cycles (GC/orphan/scrub/AE). Default 2.
    pub housekeeping_max_active: usize,
    /// Maximum duration of a single Tier-1 cycle in seconds (default
    /// 3600). 0 disables the timeout.
    pub task_timeout_sec: u64,
    /// Bytes of intra-node drain relocation per tick, across all draining
    /// pools on the node (default 256 MiB). A single segment larger than
    /// the budget overshoots one tick.
    pub drain_max_bytes_per_tick: u64,
    /// Bytes of **cluster** (off-node) drain re-replication dispatched per
    /// tick (default 64 MiB — full-file copies over the network, so a more
    /// conservative pace than the intra-node sibling copies).
    pub drain_cluster_max_bytes_per_tick: u64,
    /// Cadence of the `"drain_intra"` Tier-1 task in seconds (default 1);
    /// the byte budget above is the real pace-setter.
    pub drain_interval_sec: u64,
    /// Cadence of the periodic pool-capacity refresh in seconds (default
    /// 10); `0` disables the periodic refresh (boot/attach probes still
    /// run). One `statvfs` per registered pool per tick; the refreshed
    /// free/total values feed placement, the `oceanfs_pool_bytes_*`
    /// gauges and the re-declared manifest (pr1).
    pub capacity_refresh_interval_sec: u64,
    /// ae1: consecutive repair sweeps during which a parked segment has
    /// **no live recorded holder** before it is classified terminal
    /// (unrecoverable) — see `oceanfs_repair_unrecoverable_total`. Zero
    /// is rejected: a terminal classification must never be immediate
    /// (membership flaps must not trip it). Default 3.
    pub repair_unrecoverable_sweeps: u32,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            repair_max_active: default_repair_max_active(),
            housekeeping_max_active: default_housekeeping_max_active(),
            task_timeout_sec: default_task_timeout_sec(),
            drain_max_bytes_per_tick: default_drain_max_bytes_per_tick(),
            drain_cluster_max_bytes_per_tick: default_drain_cluster_max_bytes_per_tick(),
            drain_interval_sec: default_drain_interval_sec(),
            capacity_refresh_interval_sec: default_capacity_refresh_interval_sec(),
            repair_unrecoverable_sweeps: default_repair_unrecoverable_sweeps(),
        }
    }
}

impl DurabilityConfig {
    /// Validates the durability settings.
    ///
    /// # Errors
    ///
    /// Rejects a `capacity_refresh_interval_sec` outside `0` (disabled)
    /// or `1..=86400` (one day) seconds, naming the field and the accepted
    /// range, and a `repair_unrecoverable_sweeps` of `0`.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::DurabilityConfig;
    ///
    /// assert!(DurabilityConfig::default().validate().is_ok());
    /// let mut disabled = DurabilityConfig::default();
    /// disabled.capacity_refresh_interval_sec = 0;
    /// assert!(disabled.validate().is_ok());
    ///
    /// disabled.capacity_refresh_interval_sec = 86_401;
    /// assert!(disabled.validate().is_err());
    ///
    /// disabled.capacity_refresh_interval_sec = 10;
    /// disabled.repair_unrecoverable_sweeps = 0;
    /// assert!(disabled.validate().is_err());
    /// ```
    pub fn validate(&self) -> Result<(), String> {
        let secs = self.capacity_refresh_interval_sec;
        if secs != 0 && !(1..=CAPACITY_REFRESH_MAX_INTERVAL_SEC).contains(&secs) {
            return Err(format!(
                "durability.capacity_refresh_interval_sec: expected 0 (disabled) or \
                 1..={CAPACITY_REFRESH_MAX_INTERVAL_SEC} seconds, got {secs}"
            ));
        }
        if self.repair_unrecoverable_sweeps == 0 {
            return Err("durability.repair_unrecoverable_sweeps: expected at least 1 \
                 (a terminal classification must never be immediate), got 0"
                .into());
        }
        Ok(())
    }
}

/// Default Tier-0 (repair) permits: 16.
pub fn default_repair_max_active() -> usize {
    16
}

/// Default Tier-1 (housekeeping) permits: 2.
pub fn default_housekeeping_max_active() -> usize {
    2
}

/// Default per-cycle timeout in seconds: 3600.
pub fn default_task_timeout_sec() -> u64 {
    3600
}

/// Default intra-node drain byte budget per tick: 256 MiB.
pub fn default_drain_max_bytes_per_tick() -> u64 {
    256 * 1024 * 1024
}

/// Default cluster (off-node) drain byte budget per tick: 64 MiB.
pub fn default_drain_cluster_max_bytes_per_tick() -> u64 {
    64 * 1024 * 1024
}

/// Default intra-node drain task cadence: one cycle per second.
pub fn default_drain_interval_sec() -> u64 {
    1
}

/// Upper bound accepted for `capacity_refresh_interval_sec` (one day):
/// longer cadences are configuration mistakes, not disablement — `0` is
/// the documented disable value.
pub const CAPACITY_REFRESH_MAX_INTERVAL_SEC: u64 = 86_400;

/// Default pool-capacity refresh cadence: every 10 seconds.
pub fn default_capacity_refresh_interval_sec() -> u64 {
    10
}

/// Default consecutive no-live-holder sweeps before a parked repair is
/// classified unrecoverable (ae1): 3.
///
/// # Examples
///
/// ```ignore
/// // Internal default; applied through `DurabilityConfig::default()`:
/// assert_eq!(DurabilityConfig::default().repair_unrecoverable_sweeps, 3);
/// ```
pub fn default_repair_unrecoverable_sweeps() -> u32 {
    3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_capacity_refresh_interval_is_ten_seconds() {
        assert_eq!(DurabilityConfig::default().capacity_refresh_interval_sec, 10);
    }

    #[test]
    fn toml_defaults_and_explicit_values_parse() {
        #[derive(serde::Deserialize)]
        struct Wrapper {
            #[serde(default)]
            durability: DurabilityConfig,
        }

        let absent: Wrapper = toml::from_str("").expect("empty table parses");
        assert_eq!(absent.durability.capacity_refresh_interval_sec, 10, "serde default is 10");

        let explicit: Wrapper = toml::from_str("[durability]\ncapacity_refresh_interval_sec = 3\n")
            .expect("explicit value parses");
        assert_eq!(explicit.durability.capacity_refresh_interval_sec, 3);
    }

    #[test]
    fn validate_accepts_disabled_bounds_and_rejects_beyond_one_day() {
        assert!(DurabilityConfig::default().validate().is_ok());
        for secs in [0u64, 1, 86_400] {
            let mut config = DurabilityConfig::default();
            config.capacity_refresh_interval_sec = secs;
            assert!(config.validate().is_ok(), "{secs} must be accepted");
        }

        let mut config = DurabilityConfig::default();
        config.capacity_refresh_interval_sec = 86_401;
        let error = config.validate().expect_err("86_401 must be rejected");
        assert!(error.contains("capacity_refresh_interval_sec"), "{error}");
        assert!(error.contains("86400"), "{error}");
    }

    #[test]
    fn repair_unrecoverable_sweeps_defaults_to_three_and_rejects_zero() {
        assert_eq!(DurabilityConfig::default().repair_unrecoverable_sweeps, 3);

        let mut config = DurabilityConfig::default();
        config.repair_unrecoverable_sweeps = 0;
        let error = config.validate().expect_err("0 sweeps must be rejected");
        assert!(error.contains("repair_unrecoverable_sweeps"), "{error}");

        config.repair_unrecoverable_sweeps = 1;
        assert!(config.validate().is_ok(), "1 sweep is a valid (aggressive) bound");
    }

    #[test]
    fn repair_unrecoverable_sweeps_roundtrips_serde_default() {
        #[derive(serde::Deserialize)]
        struct Wrapper {
            #[serde(default)]
            durability: DurabilityConfig,
        }

        let absent: Wrapper = toml::from_str("").expect("empty table parses");
        assert_eq!(absent.durability.repair_unrecoverable_sweeps, 3, "serde default is 3");

        let explicit: Wrapper = toml::from_str("[durability]\nrepair_unrecoverable_sweeps = 5\n")
            .expect("explicit value parses");
        assert_eq!(explicit.durability.repair_unrecoverable_sweeps, 5);
    }
}
