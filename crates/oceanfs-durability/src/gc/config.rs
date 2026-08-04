//! GC configuration — compaction thresholds and scheduling.

use oceanfs_core::SizeTier;

/// Returns the target segment size for a given storage tier.
pub(crate) fn tier_target_size(tier: SizeTier) -> u64 {
    match tier {
        SizeTier::Small => 65536,
        SizeTier::Standard => 4194304,
        SizeTier::Multi => 4194304,
        SizeTier::Inline => 0,
        _ => 4194304,
    }
}

// ---------------------------------------------------------------------------
// GcConfig
// ---------------------------------------------------------------------------

/// Configuration for garbage collection.
///
/// # Examples
///
/// ```
/// # use oceanfs_durability::GcConfig;
/// let config = GcConfig::default();
/// assert_eq!(config.interval_sec(), 3600);
/// ```
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Interval between GC cycles in seconds.
    pub(crate) interval_sec: u64,
    /// Tombstone TTL in seconds before reclaimable.
    pub(crate) tombstone_ttl_sec: u64,
    /// Liveness ratio threshold for compaction (0.0–1.0).
    pub(crate) compact_threshold: f64,
    /// Maximum concurrent compactions.
    pub(crate) max_concurrent_compactions: usize,
    /// Bounded channel capacity for compaction work queue.
    pub(crate) compaction_queue_capacity: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            interval_sec: 3600,
            tombstone_ttl_sec: 259200,
            compact_threshold: 0.5,
            max_concurrent_compactions: 4,
            compaction_queue_capacity: 64,
        }
    }
}

impl GcConfig {
    /// Creates a new `GcConfig` with the given values.
    ///
    /// # Examples
    ///
    /// ```
    /// # use oceanfs_durability::GcConfig;
    /// let config = GcConfig::new(3600, 259200, 0.5, 4, 64);
    /// assert_eq!(config.interval_sec(), 3600);
    /// ```
    pub fn new(
        interval_sec: u64,
        tombstone_ttl_sec: u64,
        compact_threshold: f64,
        max_concurrent_compactions: usize,
        compaction_queue_capacity: usize,
    ) -> Self {
        Self {
            interval_sec,
            tombstone_ttl_sec,
            compact_threshold,
            max_concurrent_compactions,
            compaction_queue_capacity,
        }
    }

    /// Returns the GC cycle interval in seconds.
    pub fn interval_sec(&self) -> u64 {
        self.interval_sec
    }

    /// Returns the tombstone TTL in seconds.
    pub fn tombstone_ttl_sec(&self) -> u64 {
        self.tombstone_ttl_sec
    }

    /// Returns the compaction threshold (liveness ratio).
    pub fn compact_threshold(&self) -> f64 {
        self.compact_threshold
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    // GcConfig
    // -----------------------------------------------------------------------

    #[test]
    fn default_gc_config_values() {
        let config = GcConfig::default();
        assert_eq!(config.interval_sec(), 3600);
        assert_eq!(config.tombstone_ttl_sec(), 259200);
        assert!((config.compact_threshold() - 0.5).abs() < f64::EPSILON);
    }

    // GcConfig custom
    // -----------------------------------------------------------------------

    #[test]
    fn gc_config_custom_values() {
        let config = GcConfig {
            interval_sec: 7200,
            tombstone_ttl_sec: 86400,
            compact_threshold: 0.3,
            max_concurrent_compactions: 8,
            compaction_queue_capacity: 128,
        };
        assert_eq!(config.interval_sec(), 7200);
        assert_eq!(config.tombstone_ttl_sec(), 86400);
        assert!((config.compact_threshold() - 0.3).abs() < f64::EPSILON);
    }

    // -----------------------------------------------------------------------
}
