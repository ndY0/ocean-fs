//! Liveness analysis — identifies dead segments by tombstone reference counting.

use std::collections::{HashMap, HashSet};

use oceanfs_core::{ChunkRef, SegmentId};

// ---------------------------------------------------------------------------
// LivenessTracker
// ---------------------------------------------------------------------------

/// Tracks per-segment live/dead byte counts during a GC cycle.
#[derive(Debug, Default)]
pub(crate) struct LivenessTracker {
    /// Per-segment live byte count (bytes still referenced).
    pub(crate) live_bytes: HashMap<SegmentId, u64>,
    /// Per-segment dead byte count (bytes from deleted objects).
    pub(crate) dead_bytes: HashMap<SegmentId, u64>,
    /// Set of segments known to exist.
    pub(crate) known_segments: HashSet<SegmentId>,
}

impl LivenessTracker {
    /// Creates a new empty tracker.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Registers a segment with its total size.
    pub(crate) fn register_segment(&mut self, segment_id: SegmentId, total_size: u64) {
        self.known_segments.insert(segment_id);
        // Initialize live bytes to total_size — deletions will move bytes to dead
        *self.live_bytes.entry(segment_id).or_insert(0) += total_size;
    }

    /// Adds live bytes to a segment (from object chunk metadata).
    pub(crate) fn add_live_bytes(&mut self, segment_id: SegmentId, bytes: u64) {
        self.known_segments.insert(segment_id);
        *self.live_bytes.entry(segment_id).or_insert(0) += bytes;
    }

    /// Marks a chunk as dead (from a tombstone).
    pub(crate) fn mark_dead(&mut self, chunk: &ChunkRef) {
        let dead = chunk.length as u64;
        *self.dead_bytes.entry(chunk.segment_id).or_insert(0) += dead;
        if let Some(live) = self.live_bytes.get_mut(&chunk.segment_id) {
            *live = live.saturating_sub(dead);
        }
    }

    /// Computes the liveness ratio (0.0–1.0) for a segment.
    /// Returns `None` if the segment is unknown.
    pub(crate) fn liveness_ratio(&self, segment_id: &SegmentId) -> Option<f64> {
        let live = self.live_bytes.get(segment_id)?;
        let dead = self.dead_bytes.get(segment_id).copied().unwrap_or(0);
        let total = *live + dead;
        if total == 0 {
            return Some(1.0);
        }
        Some(*live as f64 / total as f64)
    }

    /// Returns the set of segments that are candidates for compaction
    /// (liveness ratio below threshold).
    pub(crate) fn compaction_candidates(&self, threshold: f64) -> Vec<SegmentId> {
        self.known_segments
            .iter()
            .filter(|id| self.liveness_ratio(id).map(|r| r < threshold).unwrap_or(false))
            .copied()
            .collect()
    }

    /// Returns the dead byte count for a segment.
    pub(crate) fn dead_bytes_for(&self, segment_id: &SegmentId) -> u64 {
        self.dead_bytes.get(segment_id).copied().unwrap_or(0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{ChunkRef, SegmentId};

    use super::super::liveness_tracker::LivenessTracker;
    // LivenessTracker
    // -----------------------------------------------------------------------

    #[test]
    fn liveness_ratio_no_deletions_is_one() {
        let mut tracker = LivenessTracker::new();
        let id = SegmentId::new();
        tracker.register_segment(id, 1000);
        let ratio = tracker.liveness_ratio(&id).unwrap();
        assert!((ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn liveness_ratio_all_deleted_is_zero() {
        let mut tracker = LivenessTracker::new();
        let id = SegmentId::new();
        tracker.register_segment(id, 1000);
        let chunk = ChunkRef { segment_id: id, offset: 0, length: 1000 };
        tracker.mark_dead(&chunk);
        let ratio = tracker.liveness_ratio(&id).unwrap();
        assert!((ratio - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn liveness_ratio_half_deleted() {
        let mut tracker = LivenessTracker::new();
        let id = SegmentId::new();
        tracker.register_segment(id, 1000);
        let dead_chunk = ChunkRef { segment_id: id, offset: 0, length: 500 };
        tracker.mark_dead(&dead_chunk);
        let ratio = tracker.liveness_ratio(&id).unwrap();
        assert!((ratio - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn compaction_candidates_below_threshold() {
        let mut tracker = LivenessTracker::new();
        let id1 = SegmentId::new();
        let id2 = SegmentId::new();
        tracker.register_segment(id1, 1000);
        tracker.register_segment(id2, 1000);

        // Mark 800 bytes dead on id1 (20% liveness)
        let chunk = ChunkRef { segment_id: id1, offset: 0, length: 800 };
        tracker.mark_dead(&chunk);

        let candidates = tracker.compaction_candidates(0.5);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0], id1);
    }

    // -----------------------------------------------------------------------

    // LivenessTracker edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn liveness_ratio_unknown_segment_returns_none() {
        let tracker = LivenessTracker::new();
        let unknown_id = SegmentId::new();
        assert_eq!(tracker.liveness_ratio(&unknown_id), None);
    }

    #[test]
    fn dead_bytes_for_unknown_segment_returns_zero() {
        let tracker = LivenessTracker::new();
        assert_eq!(tracker.dead_bytes_for(&SegmentId::new()), 0);
    }

    #[test]
    fn compaction_candidates_all_healthy_returns_empty() {
        let mut tracker = LivenessTracker::new();
        let id = SegmentId::new();
        tracker.register_segment(id, 1000);
        let candidates = tracker.compaction_candidates(0.5);
        assert!(candidates.is_empty());
    }

    #[test]
    fn mark_dead_saturating_subtraction() {
        let mut tracker = LivenessTracker::new();
        let id = SegmentId::new();
        tracker.register_segment(id, 100);
        let chunk = ChunkRef { segment_id: id, offset: 0, length: 200 };
        tracker.mark_dead(&chunk);
        // Live bytes should not go below 0
        let ratio = tracker.liveness_ratio(&id).unwrap();
        assert!((ratio - 0.0).abs() < f64::EPSILON);
    }

    // liveness_ratio for multiple segments
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_segment_liveness_tracking() {
        let mut tracker = LivenessTracker::new();
        let id1 = SegmentId::new();
        let id2 = SegmentId::new();
        tracker.register_segment(id1, 1000);
        tracker.register_segment(id2, 2000);

        assert!((tracker.liveness_ratio(&id1).unwrap() - 1.0).abs() < f64::EPSILON);
        assert!((tracker.liveness_ratio(&id2).unwrap() - 1.0).abs() < f64::EPSILON);

        // Mark some dead on id1
        let chunk = ChunkRef { segment_id: id1, offset: 0, length: 500 };
        tracker.mark_dead(&chunk);
        // id1 should now be at 50% liveness, id2 still at 100%
        assert!((tracker.liveness_ratio(&id1).unwrap() - 0.5).abs() < f64::EPSILON);
        assert!((tracker.liveness_ratio(&id2).unwrap() - 1.0).abs() < f64::EPSILON);
    }

    // -----------------------------------------------------------------------
}
