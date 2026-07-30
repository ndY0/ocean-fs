//! Distributed scrubbing — full cluster-wide segment scan for integrity.

/// Configuration for distributed scrubbing.
#[derive(Debug, Clone)]
pub struct ScrubConfig {
    /// Interval between scrub cycles in seconds.
    pub interval_sec: u64,
    /// Maximum number of nodes participating (0 = all).
    pub parallel_nodes: usize,
    /// Throughput limit in bytes per second (0 = unlimited).
    pub throttle_bytes_sec: u64,
}

impl Default for ScrubConfig {
    fn default() -> Self {
        Self {
            interval_sec: 604800, // 7 days
            parallel_nodes: 0,
            throttle_bytes_sec: 0,
        }
    }
}

/// Results from a full scrub cycle.
#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub struct ScrubReport {
    /// Total segments examined.
    pub segments_total: u64,
    /// Segments verified healthy.
    pub segments_healthy: u64,
    /// Segments found to be corrupt.
    pub segments_corrupt: u64,
    /// Segments successfully healed.
    pub segments_healed: u64,
}

/// Scrub coordinator — partitions segment space across nodes.
pub struct ScrubCoordinator {
    config: ScrubConfig,
}

impl ScrubCoordinator {
    /// Creates a new scrub coordinator.
    pub fn new(config: ScrubConfig) -> Self {
        Self { config }
    }

    /// Returns the configuration.
    pub fn config(&self) -> &ScrubConfig {
        &self.config
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn default_scrub_interval_is_7_days() {
        let config = ScrubConfig::default();
        assert_eq!(config.interval_sec, 604800);
    }
}
