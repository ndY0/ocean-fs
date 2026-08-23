//! Compaction remap alias — the receiver-side `old → new` segment map
//! consulted when persisting object metadata (g3 `loss-announcement`,
//! Option A: owner-authoritative compaction propagation).
//!
//! ## Why this exists
//!
//! The compactor rewrites only the OWNER's RocksDB when it compacts
//! `S → S'` (segment_compactor.rs `ObjectsMoved`). Object metadata
//! replicas landing on a peer *after* that peer's GC compacted `S` away
//! reference a segment that exists nowhere (GAP-1 in the
//! sealed-segment-replication feature doc — the `45c8` read failure).
//! The remap announcement (healing.proto `AnnounceRemap`) tells the peer
//! `S → S'` plus the **chunk-remap table**; the peer:
//!
//! 1. records the alias here so the append/read-repair handlers
//!    translate late chunk refs at write time, AND
//! 2. batch-rewrites its already-persisted object rows.
//!
//! The repacked byte layout is NOT offset-preserving (the compactor
//! packs live chunks contiguously), so the alias stores the per-chunk
//! `(old_offset, length) → new_offset` table — a receiver cannot re-point
//! `old → new` keeping the same offset.
//!
//! The map is a memory-lookup fast path, not state: entries are advisory
//! and the periodic reconciliation (g4) is the mandatory failsafe that
//! re-points anything the push missed.

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use crate::SegmentId;

/// One repacked chunk's translation.
///
/// # Examples
///
/// ```
/// use oceanfs_core::RemappedChunk;
///
/// let c = RemappedChunk { old_offset: 100, length: 32, new_offset: 0 };
/// assert_eq!(c.old_offset, 100);
/// assert_eq!(c.length, 32);
/// assert_eq!(c.new_offset, 0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemappedChunk {
    /// The chunk's offset in the old (pre-compaction) segment.
    pub old_offset: u64,
    /// The chunk's length (unchanged by repacking).
    pub length: u32,
    /// The chunk's offset in the new (repacked) segment.
    pub new_offset: u64,
}

/// The per-old-segment translation table stored by [`SegmentRemapAlias`].
#[derive(Debug, Default)]
struct RemapEntry {
    /// The new (repacked) segment id.
    new_segment_id: SegmentId,
    /// `(old_offset, length) → new_offset` for every repacked chunk.
    chunks: HashMap<(u64, u32), u64>,
}

/// Receiver-side `old segment → new segment + chunk table` map.
///
/// Lookup is lock-free-friendly: readers take a read lock per object row
/// batch (metadata writes are rare relative to reads, and each batch
/// resolves many refs under one lock — perf 7.1: no lock held across
/// I/O or network).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{RemappedChunk, SegmentRemapAlias, SegmentId};
///
/// let alias = SegmentRemapAlias::new();
/// let old = SegmentId::new();
/// let new = SegmentId::new();
/// alias.insert(old, new, vec![RemappedChunk {
///     old_offset: 100,
///     length: 32,
///     new_offset: 0,
/// }]);
/// // The chunk (old, 100, 32) resolves to (new, 0).
/// let resolved = alias.resolve(old, 100, 32).expect("chunk resolves");
/// assert_eq!(resolved, (new, 0));
/// // An unknown chunk (wrong length) does not resolve.
/// assert_eq!(alias.resolve(old, 100, 64), None);
/// ```
#[derive(Debug, Default)]
pub struct SegmentRemapAlias {
    inner: RwLock<HashMap<SegmentId, RemapEntry>>,
}

impl SegmentRemapAlias {
    /// Creates an empty alias map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `old → new` with the chunk-remap table. An existing
    /// mapping for `old` is replaced (a later remap supersedes an
    /// earlier one — the owner is authoritative and only one repacked id
    /// per original is live).
    pub fn insert(&self, old: SegmentId, new: SegmentId, chunks: Vec<RemappedChunk>) {
        let mut map = HashMap::with_capacity(chunks.len());
        for c in chunks {
            map.insert((c.old_offset, c.length), c.new_offset);
        }
        self.inner.write().insert(old, RemapEntry { new_segment_id: new, chunks: map });
    }

    /// Resolves a chunk ref `(segment_id, offset, length)` through the
    /// alias. Returns `Some((new_segment_id, new_offset))` when the
    /// segment has been remapped AND the chunk is in the repacked table;
    /// `None` when the segment is current or the chunk is not part of
    /// the repack (e.g. a chunk of a tombstoned object the compactor
    /// filtered out).
    pub fn resolve(
        &self,
        segment_id: SegmentId,
        offset: u64,
        length: u32,
    ) -> Option<(SegmentId, u64)> {
        let map = self.inner.read();
        let entry = map.get(&segment_id)?;
        entry.chunks.get(&(offset, length)).map(|new_offset| (entry.new_segment_id, *new_offset))
    }

    /// Returns the new segment id for a remapped old segment, ignoring
    /// the chunk table (used for diagnostics / tests). `None` when the
    /// segment is not remapped.
    pub fn new_segment_for(&self, segment_id: SegmentId) -> Option<SegmentId> {
        self.inner.read().get(&segment_id).map(|e| e.new_segment_id)
    }

    /// The number of recorded aliases (observability / tests).
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Returns `true` when no aliases are recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes one mapping (g4 eviction / tests). Returns `true` when a
    /// mapping was present.
    pub fn remove(&self, old: SegmentId) -> bool {
        self.inner.write().remove(&old).is_some()
    }
}

/// Convenience: an `Arc`-wrapped alias map shared across handlers.
pub type SharedSegmentRemapAlias = Arc<SegmentRemapAlias>;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::SegmentId;

    #[test]
    fn resolve_translates_known_chunk_and_ignores_unknown() {
        let alias = SegmentRemapAlias::new();
        let old = SegmentId::new();
        let new = SegmentId::new();
        alias.insert(old, new, vec![RemappedChunk { old_offset: 100, length: 32, new_offset: 0 }]);
        assert_eq!(alias.resolve(old, 100, 32), Some((new, 0)));
        // Wrong length or offset does not resolve.
        assert_eq!(alias.resolve(old, 100, 64), None);
        assert_eq!(alias.resolve(old, 99, 32), None);
        // A current (non-remapped) segment does not resolve.
        assert_eq!(alias.resolve(new, 0, 32), None);
    }

    #[test]
    fn insert_replaces_and_remove_evicts() {
        let alias = SegmentRemapAlias::new();
        let old = SegmentId::new();
        let new = SegmentId::new();
        let newer = SegmentId::new();
        alias.insert(old, new, vec![RemappedChunk { old_offset: 0, length: 8, new_offset: 0 }]);
        assert_eq!(alias.len(), 1);
        assert_eq!(alias.new_segment_for(old), Some(new));

        // A later remap supersedes.
        alias.insert(old, newer, vec![RemappedChunk { old_offset: 0, length: 8, new_offset: 64 }]);
        assert_eq!(alias.new_segment_for(old), Some(newer));
        assert_eq!(alias.resolve(old, 0, 8), Some((newer, 64)));

        assert!(alias.remove(old));
        assert!(alias.is_empty());
    }

    #[test]
    fn empty_alias_resolves_nothing() {
        let alias = SegmentRemapAlias::new();
        let id = SegmentId::new();
        assert_eq!(alias.resolve(id, 0, 1), None);
        assert!(alias.is_empty());
        assert_eq!(alias.len(), 0);
    }
}
