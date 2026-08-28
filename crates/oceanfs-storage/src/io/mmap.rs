//! Memory-mapped segment file cache.
//!
//! When `read_cache_segments = true` (read-optimised profile),
//! frequently-accessed segment shard files are mapped with `memmap2::Mmap`
//! for zero-copy reads from the kernel page cache.
//!
//! The cache is a bounded LRU using a `Vec` with access timestamps plus
//! concurrent access via `Arc<Mmap>` cloning.
//! Per performance guideline §3.3 and §2.2.
//!
//! # Safety
//!
//! This module uses `unsafe` to call `memmap2::Mmap::map()`. Per
//! [ADR-0011], segment shard files are **immutable after sealing** —
//! no code path opens a writable handle after `SegmentSealer` writes
//! the file. This makes the mmap usage sound: the `&[u8]` reference
//! points to truly immutable bytes for the mapping's lifetime.

use std::{path::Path, sync::Arc};

use oceanfs_core::SegmentId;
use parking_lot::RwLock;

/// A bounded LRU cache of memory-mapped segment files.
///
/// Maps `SegmentId` → `Arc<Mmap>`. Each entry wraps a `memmap2::Mmap`
/// handle. Multiple readers can concurrently access the same mapped
/// segment via `Arc` cloning — no copying of segment data.
///
/// Eviction is triggered when the cache exceeds `max_entries`.
/// Eviction uses approximate LRU: the entry with the oldest access
/// timestamp is removed. Timestamps are updated on each `get` call.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_storage::io::SegmentFileCache;
/// use oceanfs_core::SegmentId;
/// use std::path::Path;
///
/// let cache = SegmentFileCache::new(64);
/// let segment_id = SegmentId::new();
///
/// // Cache miss → map the file.
/// let mmap = cache.get_or_map(segment_id, Path::new("/data/segments/abc.dat"))?;
/// let data: &[u8] = &mmap;
/// ```
pub struct SegmentFileCache {
    /// Maximum number of entries before eviction is triggered.
    max_entries: usize,
    /// The cached entries, keyed by segment ID.
    entries: RwLock<Vec<CacheEntry>>,
}

struct CacheEntry {
    segment_id: SegmentId,
    mmap: Arc<memmap2::Mmap>,
    /// Approximate access timestamp for LRU eviction.
    last_access: u64,
}
// [review][implementation][critical]
// i believe we never tested this optimization.
// in a broader sense, we should update phase 2 test harness to allow testing for different
// combinaisons of optimizations, io modes, even target cpu and so on
// [end]
impl SegmentFileCache {
    /// Creates a new cache with the given maximum capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_storage::io::SegmentFileCache;
    ///
    /// let cache = SegmentFileCache::new(128);
    /// ```
    pub fn new(max_entries: usize) -> Self {
        Self { max_entries, entries: RwLock::new(Vec::with_capacity(max_entries)) }
    }

    /// Looks up a segment in the cache, or maps it from disk on miss.
    ///
    /// On a cache hit, returns the existing `Arc<Mmap>` and updates
    /// the access timestamp.
    ///
    /// On a cache miss, calls `memmap2::Mmap::map(path)`, inserts
    /// the result into the cache, and returns it. If the cache is
    /// full, the least-recently-used entry is evicted first.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or mapped.
    ///
    /// # Safety
    ///
    /// The returned `Mmap` provides `Deref<Target = [u8]>` for
    /// zero-copy access to the file's bytes. The caller must not
    /// modify the underlying file while the mapping is live. This
    /// invariant holds because segment files are immutable after
    /// sealing (see [ADR-0011]).
    #[allow(unsafe_code)]
    pub fn get_or_map(
        &self,
        segment_id: SegmentId,
        path: &Path,
    ) -> std::io::Result<Arc<memmap2::Mmap>> {
        let mut entries = self.entries.write();

        // Check for existing entry (cache hit).
        if let Some(entry) = entries.iter_mut().find(|e| e.segment_id == segment_id) {
            entry.last_access = monotonic_timestamp();
            return Ok(Arc::clone(&entry.mmap));
        }

        // Cache miss — map the file.
        let file = std::fs::File::open(path)?;

        // SAFETY: Segment shard files are immutable after sealing
        // (ADR-0011). `SegmentSealer::seal_from_data()` is the sole
        // code path that writes segment data to disk. Once a segment
        // is sealed, no code path opens a writable handle to the
        // segment file. The OS cannot modify the file bytes
        // concurrently because no write handle exists anywhere in
        // the system. Therefore the `&[u8]` reference returned by
        // `Mmap::map()` points to truly immutable bytes for the
        // mapping's lifetime.
        let mmap = unsafe { memmap2::Mmap::map(&file)? };

        let arc = Arc::new(mmap);

        // Evict if full.
        if entries.len() >= self.max_entries {
            if let Some((idx, _)) = entries.iter().enumerate().min_by_key(|(_, e)| e.last_access) {
                entries.swap_remove(idx);
            }
        }

        entries.push(CacheEntry {
            segment_id,
            mmap: Arc::clone(&arc),
            last_access: monotonic_timestamp(),
        });

        Ok(arc)
    }

    /// Returns the number of currently cached entries.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Removes all entries from the cache.
    ///
    /// Existing `Arc<Mmap>` handles held by callers remain valid;
    /// this only removes the cache's references.
    pub fn clear(&self) {
        self.entries.write().clear();
    }

    /// Removes a specific segment from the cache.
    ///
    /// Used by GC compaction and healing to proactively evict stale
    /// entries after a segment is replaced or deleted. Existing
    /// `Arc<Mmap>` handles held by in-flight readers remain valid.
    ///
    /// This is a no-op if the segment is not cached.
    pub fn invalidate(&self, segment_id: SegmentId) {
        self.entries.write().retain(|e| e.segment_id != segment_id);
    }

    /// Returns the maximum number of entries.
    pub fn capacity(&self) -> usize {
        self.max_entries
    }
}

/// Returns a monotonically increasing timestamp for LRU bookkeeping.
fn monotonic_timestamp() -> u64 {
    use std::time::Instant;
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::SegmentId;

    use super::*;

    fn temp_segment_file(dir: &tempfile::TempDir, id: SegmentId) -> std::path::PathBuf {
        let path = dir.path().join(format!("{id}.dat"));
        std::fs::write(&path, vec![0xABu8; 4096]).unwrap();
        path
    }

    #[test]
    fn new_cache_is_empty() {
        let cache = SegmentFileCache::new(4);
        assert!(cache.is_empty());
        assert_eq!(cache.capacity(), 4);
    }

    #[test]
    fn cache_miss_maps_file() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        let path = temp_segment_file(&dir, id);

        let cache = SegmentFileCache::new(4);
        let mmap = cache.get_or_map(id, &path).unwrap();

        assert_eq!(mmap.len(), 4096);
        assert_eq!(mmap[0], 0xAB);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn cache_hit_returns_same_arc() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        let path = temp_segment_file(&dir, id);

        let cache = SegmentFileCache::new(4);
        let mmap1 = cache.get_or_map(id, &path).unwrap();
        let mmap2 = cache.get_or_map(id, &path).unwrap();

        assert!(Arc::ptr_eq(&mmap1, &mmap2));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn cache_evicts_lru_when_full() {
        let dir = tempfile::tempdir().unwrap();

        let cache = SegmentFileCache::new(2);

        let id1 = SegmentId::new();
        let id2 = SegmentId::new();
        let id3 = SegmentId::new();

        let _m1 = cache.get_or_map(id1, &temp_segment_file(&dir, id1)).unwrap();
        let _m2 = cache.get_or_map(id2, &temp_segment_file(&dir, id2)).unwrap();

        // Access id1 again so id2 becomes LRU.
        let _m1_again = cache.get_or_map(id1, &temp_segment_file(&dir, id1)).unwrap();

        // Insert id3 — should evict id2.
        let _m3 = cache.get_or_map(id3, &temp_segment_file(&dir, id3)).unwrap();

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn cache_clear_removes_all_entries() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        let path = temp_segment_file(&dir, id);

        let cache = SegmentFileCache::new(4);
        let _mmap = cache.get_or_map(id, &path).unwrap();
        assert_eq!(cache.len(), 1);

        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn cache_nonexistent_file_returns_error() {
        let cache = SegmentFileCache::new(2);
        let id = SegmentId::new();
        let result = cache.get_or_map(id, Path::new("/nonexistent/segment.dat"));
        assert!(result.is_err());
    }

    #[test]
    fn mmap_data_is_readable() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        let path = temp_segment_file(&dir, id);

        let cache = SegmentFileCache::new(4);
        let mmap = cache.get_or_map(id, &path).unwrap();

        // Verify all bytes match what we wrote.
        assert_eq!(mmap.len(), 4096);
        for byte in &mmap[..] {
            assert_eq!(*byte, 0xAB);
        }
    }

    #[test]
    fn cache_invalidate_removes_specific_entry() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = SegmentId::new();
        let id2 = SegmentId::new();
        let path1 = temp_segment_file(&dir, id1);
        let path2 = temp_segment_file(&dir, id2);

        let cache = SegmentFileCache::new(4);
        let _m1 = cache.get_or_map(id1, &path1).unwrap();
        let _m2 = cache.get_or_map(id2, &path2).unwrap();
        assert_eq!(cache.len(), 2);

        cache.invalidate(id1);
        assert_eq!(cache.len(), 1);

        cache.invalidate(id2);
        assert!(cache.is_empty());
    }

    #[test]
    fn cache_invalidate_nonexistent_is_noop() {
        let cache = SegmentFileCache::new(4);
        let id = SegmentId::new();
        cache.invalidate(id); // Should not panic.
        assert!(cache.is_empty());
    }
}
