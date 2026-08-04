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
