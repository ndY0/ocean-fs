//! GC statistics — atomic counters for compaction and collection metrics.
//!
// ---------------------------------------------------------------------------
// GcStats
// ---------------------------------------------------------------------------

/// Statistics from a GC cycle.
#[derive(Debug, Default, Clone)]
pub struct GcStats {
    /// Number of segments scanned.
    pub segments_scanned: u64,
    /// Number of segments compacted.
    pub segments_compacted: u64,
    /// Bytes reclaimed.
    pub bytes_reclaimed: u64,
    /// Bytes that are live after compaction.
    pub live_bytes: u64,
    /// Bytes that are dead (reclaimable).
    pub dead_bytes: u64,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    // GcStats defaults
    // -----------------------------------------------------------------------

    #[test]
    fn gc_stats_defaults() {
        let stats = GcStats::default();
        assert_eq!(stats.segments_scanned, 0);
        assert_eq!(stats.segments_compacted, 0);
        assert_eq!(stats.bytes_reclaimed, 0);
        assert_eq!(stats.live_bytes, 0);
        assert_eq!(stats.dead_bytes, 0);
    }
}
