//! Garbage collection — tombstone processing and segment compaction.

/// Configuration for garbage collection.
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Interval between GC cycles in seconds.
    pub interval_sec: u64,
    /// Tombstone TTL in seconds before reclaimable.
    pub tombstone_ttl_sec: u64,
    /// Liveness ratio threshold for compaction (0.0–1.0).
    pub compact_threshold: f64,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self { interval_sec: 3600, tombstone_ttl_sec: 259200, compact_threshold: 0.5 }
    }
}

/// Statistics from a GC cycle.
#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub struct GcStats {
    /// Number of segments scanned.
    pub segments_scanned: u64,
    /// Number of segments compacted.
    pub segments_compacted: u64,
    /// Bytes reclaimed.
    pub bytes_reclaimed: u64,
}

/// Garbage collector for tombstone-based deletion and segment compaction.
pub struct GarbageCollector {
    config: GcConfig,
}

impl GarbageCollector {
    /// Creates a new garbage collector.
    pub fn new(config: GcConfig) -> Self {
        Self { config }
    }

    /// Returns a reference to the configuration.
    pub fn config(&self) -> &GcConfig {
        &self.config
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = GcConfig::default();
        assert_eq!(config.interval_sec, 3600);
        assert_eq!(config.tombstone_ttl_sec, 259200);
    }
}
