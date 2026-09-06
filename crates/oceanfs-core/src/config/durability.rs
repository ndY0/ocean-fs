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
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            repair_max_active: default_repair_max_active(),
            housekeeping_max_active: default_housekeeping_max_active(),
            task_timeout_sec: default_task_timeout_sec(),
        }
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
