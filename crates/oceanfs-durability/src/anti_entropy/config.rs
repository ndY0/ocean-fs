//! Configuration for the anti-entropy background task.
//!
//! Controls both continuous mode (root-only exchange on every segment write)
//! and sampling mode (periodic random subset of segments).

use oceanfs_core::AntiEntropyConfig as CoreAntiEntropyConfig;

// ---------------------------------------------------------------------------
// AntiEntropyConfig
// ---------------------------------------------------------------------------

/// Configuration for anti-entropy background task.
///
/// This extends [`oceanfs_core::AntiEntropyConfig`] with AE-specific
/// operational settings (cycle interval, peer count).
///
/// # Examples
///
/// ```
/// # use oceanfs_durability::AntiEntropyConfig;
/// let config = AntiEntropyConfig::default();
/// assert_eq!(config.interval_sec(), 300);
/// ```
#[derive(Debug, Clone)]
pub struct AntiEntropyConfig {
    /// Interval between anti-entropy cycles in seconds.
    pub(crate) interval_sec: u64,
    /// Number of random peers to compare with per cycle.
    pub(crate) peer_count: usize,
    /// Core anti-entropy settings (continuous, sampling, tree config).
    pub(crate) core: CoreAntiEntropyConfig,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self { interval_sec: 300, peer_count: 1, core: CoreAntiEntropyConfig::default() }
    }
}

impl AntiEntropyConfig {
    /// Creates a new configuration with the given settings.
    pub fn new(interval_sec: u64, peer_count: usize) -> Self {
        Self { interval_sec, peer_count, core: CoreAntiEntropyConfig::default() }
    }

    /// Returns the cycle interval in seconds.
    pub fn interval_sec(&self) -> u64 {
        self.interval_sec
    }

    /// Returns the number of peers per cycle.
    pub fn peer_count(&self) -> usize {
        self.peer_count
    }

    /// Returns a reference to the core anti-entropy settings.
    pub fn core(&self) -> &CoreAntiEntropyConfig {
        &self.core
    }

    /// Sets the core anti-entropy configuration.
    pub fn with_core(mut self, core: CoreAntiEntropyConfig) -> Self {
        self.core = core;
        self
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn default_anti_entropy_config() {
        let config = AntiEntropyConfig::default();
        assert_eq!(config.interval_sec(), 300);
        assert_eq!(config.peer_count(), 1);
    }

    #[test]
    fn test_ae_config_peer_count_respected() {
        let config = AntiEntropyConfig::new(300, 3);
        assert_eq!(config.interval_sec(), 300);
        assert_eq!(config.peer_count(), 3);

        let config = AntiEntropyConfig::new(600, 5);
        assert_eq!(config.interval_sec(), 600);
        assert_eq!(config.peer_count(), 5);
    }
}
