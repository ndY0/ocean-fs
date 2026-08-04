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
