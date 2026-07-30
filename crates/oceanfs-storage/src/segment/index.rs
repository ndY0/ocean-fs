//! Segment blob index — B-tree index for O(log n) blob lookup.
//!
//! Each sealed segment stores a sorted index at its head mapping blob
//! offsets to key hashes. Used by the read path to efficiently locate
//! blobs within segments.

use std::collections::BTreeMap;

/// An entry in the segment blob index.
pub use oceanfs_core::SegmentIndexEntry;

use crate::error::{Error, Result};

/// A sorted B-tree index of blobs within a segment.
///
/// Maps byte offsets to key hashes for O(log n) lookup. Serialized at
/// the segment head during sealing and loaded on first access.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::segment::index::{SegmentIndex, SegmentIndexEntry};
///
/// let entries = vec![
///     SegmentIndexEntry { offset: 0, length: 100, blob_key_hash: [1u8; 32] },
///     SegmentIndexEntry { offset: 100, length: 200, blob_key_hash: [2u8; 32] },
/// ];
/// let index = SegmentIndex::new(entries).unwrap();
/// assert_eq!(index.len(), 2);
/// assert!(index.lookup(0).is_some());
/// assert!(index.lookup(50).is_none());
/// ```
#[derive(Debug, Clone)]
pub struct SegmentIndex {
    /// Sorted map from offset → index entry.
    entries: BTreeMap<u64, SegmentIndexEntry>,
}

impl SegmentIndex {
    /// Creates a new segment index from a list of entries.
    ///
    /// # Errors
    ///
    /// Returns an error if entries have overlapping or invalid offsets.
    pub fn new(entries: Vec<SegmentIndexEntry>) -> Result<Self> {
        let mut map = BTreeMap::new();
        for entry in entries {
            if map.contains_key(&entry.offset) {
                return Err(Error::InvalidConfig(format!(
                    "duplicate offset {} in segment index",
                    entry.offset
                )));
            }
            map.insert(entry.offset, entry);
        }
        Ok(Self { entries: map })
    }

    /// Looks up an index entry by exact offset.
    ///
    /// Returns `None` if no blob starts at the given offset.
    pub fn lookup(&self, offset: u64) -> Option<&SegmentIndexEntry> {
        self.entries.get(&offset)
    }

    /// Returns the number of blobs indexed.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Serializes the index to bytes (JSON for simplicity; replace with
    /// a compact binary format in production).
    pub fn to_bytes(&self) -> Vec<u8> {
        let entries: Vec<&SegmentIndexEntry> = self.entries.values().collect();
        serde_json::to_vec(&entries).unwrap_or_default()
    }

    /// Deserializes an index from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the data is not valid JSON or contains
    /// entries with duplicate offsets.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let entries: Vec<SegmentIndexEntry> = serde_json::from_slice(data)
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        Self::new(entries)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn entry(offset: u64, length: u32) -> SegmentIndexEntry {
        SegmentIndexEntry { offset, length, blob_key_hash: [0u8; 32] }
    }

    #[test]
    fn lookup_returns_entry_at_offset() {
        let index = SegmentIndex::new(vec![entry(0, 100), entry(100, 200)]).unwrap();
        assert_eq!(index.lookup(0).unwrap().length, 100);
        assert_eq!(index.lookup(100).unwrap().length, 200);
    }

    #[test]
    fn lookup_returns_none_for_missing_offset() {
        let index = SegmentIndex::new(vec![entry(0, 100)]).unwrap();
        assert!(index.lookup(50).is_none());
    }

    #[test]
    fn duplicate_offset_rejected() {
        let result = SegmentIndex::new(vec![entry(0, 100), entry(0, 200)]);
        assert!(result.is_err());
    }

    #[test]
    fn empty_index_has_len_zero() {
        let index = SegmentIndex::new(vec![]).unwrap();
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn roundtrip_serialize_deserialize() {
        let index = SegmentIndex::new(vec![entry(0, 50), entry(50, 150), entry(200, 300)]).unwrap();
        let bytes = index.to_bytes();
        let restored = SegmentIndex::from_bytes(&bytes).unwrap();
        assert_eq!(restored.len(), 3);
        assert!(restored.lookup(50).is_some());
    }
}
