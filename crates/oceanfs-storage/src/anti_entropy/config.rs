//! Configuration for the anti-entropy background task.

// ---------------------------------------------------------------------------
// AntiEntropyConfig
// ---------------------------------------------------------------------------

/// Configuration for anti-entropy background task.
///
/// # Examples
///
/// ```
/// # use oceanfs_storage::AntiEntropyConfig;
/// let config = AntiEntropyConfig::default();
/// assert_eq!(config.interval_sec(), 300);
/// ```
#[derive(Debug, Clone)]
pub struct AntiEntropyConfig {
    /// Interval between anti-entropy cycles in seconds.
    pub(crate) interval_sec: u64,
    /// Number of random peers to compare with per cycle.
    pub(crate) peer_count: usize,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self { interval_sec: 300, peer_count: 1 }
    }
}

impl AntiEntropyConfig {
    /// Creates a new configuration with the given settings.
    ///
    /// # Examples
    ///
    /// ```
    /// # use oceanfs_storage::AntiEntropyConfig;
    /// let config = AntiEntropyConfig::new(300, 1);
    /// assert_eq!(config.interval_sec(), 300);
    /// ```
    pub fn new(interval_sec: u64, peer_count: usize) -> Self {
        Self { interval_sec, peer_count }
    }

    /// Returns the cycle interval in seconds.
    pub fn interval_sec(&self) -> u64 {
        self.interval_sec
    }

    /// Returns the number of peers per cycle.
    pub fn peer_count(&self) -> usize {
        self.peer_count
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // AntiEntropyConfig
    // -----------------------------------------------------------------------

    #[test]
    fn default_anti_entropy_config() {
        let config = AntiEntropyConfig::default();
        assert_eq!(config.interval_sec(), 300);
        assert_eq!(config.peer_count(), 1);
    }
}
