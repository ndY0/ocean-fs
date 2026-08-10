//! Object and segment metadata types.
//!
//! Core metadata structures for the object store: `ObjectMetadata` (per-object),
//! `SegmentMetadata` (per-segment), `ChunkRef` (blob-to-segment mapping),
//! `SegmentIndexEntry` (in-segment blob index), `Tombstone` (deletion marker),
//! `StorageLocation` (node + shard), and the `MetadataStore` trait.

use crate::types::hash_output::HashOutput;

use super::{
    config::SizeTier,
    id::{NodeId, ObjectKey, SegmentId},
};
use crate::Hlc;

// ---------------------------------------------------------------------------
// ChunkRef
// ---------------------------------------------------------------------------

/// A reference to a blob chunk within a segment.
///
/// Each blob's data is stored as one or more chunks within segments.
/// For inline blobs, `chunks` is empty and `inline_data` in
/// [`ObjectMetadata`] holds the payload.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{ChunkRef, SegmentId};
///
/// let chunk = ChunkRef {
///     segment_id: SegmentId::new(),
///     offset: 0,
///     length: 1024,
/// };
/// assert_eq!(chunk.length, 1024);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChunkRef {
    /// The segment containing this chunk.
    pub segment_id: SegmentId,
    /// Byte offset of the chunk within the segment.
    pub offset: u64,
    /// Length of the chunk in bytes.
    pub length: u32,
}

// ---------------------------------------------------------------------------
// ObjectMetadata
// ---------------------------------------------------------------------------

/// Metadata for a stored object.
///
/// Stored in the `objects` RocksDB column family. For inline blobs
/// (size ≤ `inline_threshold_bytes`), the payload is stored directly
/// in `inline_data` and `chunks` is empty.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata, Hlc};
///
/// let meta = ObjectMetadata {
///     object_key: ObjectKey::new("photo.jpg"),
///     size: 1024,
///     blake3_hash: None,
///     chunks: smallvec::SmallVec::new(),
///     inline_data: Some(bytes::Bytes::from_static(b"hello")),
///     created_at: 0,
///     hlc: Hlc::zero(),
/// };
/// assert!(meta.is_inline());
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ObjectMetadata {
    /// The object's key within its bucket.
    pub object_key: ObjectKey,
    /// Total size of the object in bytes.
    pub size: u64,
    /// BLAKE3 hash of the object content (None if not yet computed).
    pub blake3_hash: Option<HashOutput>,
    /// References to the segments holding this object's data.
    /// Empty for inline blobs.
    pub chunks: smallvec::SmallVec<[ChunkRef; 4]>,
    /// Inline payload for small objects (None for segment-stored blobs).
    pub inline_data: Option<bytes::Bytes>,
    /// Unix timestamp when the object was created (milliseconds since epoch).
    pub created_at: i64,
    /// HLC timestamp for conflict resolution.
    pub hlc: Hlc,
}

impl ObjectMetadata {
    /// Returns `true` if this object is stored inline (payload in metadata).
    pub fn is_inline(&self) -> bool {
        self.inline_data.is_some()
    }

    /// Returns `true` if this object is stored in one or more segments.
    pub fn is_segment_stored(&self) -> bool {
        !self.chunks.is_empty()
    }
}

// ---------------------------------------------------------------------------
// SegmentMetadata
// ---------------------------------------------------------------------------

/// Metadata for a sealed segment.
///
/// Stored in the `segments` RocksDB column family. Tracks EC parameters,
/// storage locations, and the Merkle root for integrity verification.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{SegmentMetadata, SegmentId, SizeTier};
///
/// let meta = SegmentMetadata {
///     segment_id: SegmentId::new(),
///     ec_k: 4,
///     ec_m: 2,
///     size_tier: SizeTier::Standard,
///     merkle_root: None,
///     storage_locations: smallvec::SmallVec::new(),
///     sealed_at: Some(1700000000000),
/// };
/// assert!(meta.is_sealed());
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SegmentMetadata {
    /// The segment's unique identifier.
    pub segment_id: SegmentId,
    /// Number of data shards (k) used for this segment.
    pub ec_k: u8,
    /// Number of parity shards (m) used for this segment.
    pub ec_m: u8,
    /// Storage tier of this segment.
    pub size_tier: SizeTier,
    /// Merkle tree root hash (None until computed post-seal).
    pub merkle_root: Option<HashOutput>,
    /// Node IDs holding this segment's shards.
    pub storage_locations: smallvec::SmallVec<[NodeId; 16]>,
    /// Timestamp when the segment was sealed (milliseconds since epoch).
    pub sealed_at: Option<i64>,
}

impl SegmentMetadata {
    /// Returns `true` if the segment has been sealed.
    pub fn is_sealed(&self) -> bool {
        self.sealed_at.is_some()
    }
}

// ---------------------------------------------------------------------------
// Tombstone
// ---------------------------------------------------------------------------

/// A deletion marker for a soft-deleted object.
///
/// Stored in the `deletions` RocksDB column family. Objects with a
/// tombstone are considered deleted even if their data still exists
/// in segments (until GC compaction reclaims the space).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Tombstone {
    /// When the deletion occurred (milliseconds since epoch).
    pub deletion_time: i64,
    /// HLC timestamp for conflict resolution.
    pub hlc: Hlc,
}

// ---------------------------------------------------------------------------
// SegmentIndexEntry
// ---------------------------------------------------------------------------

/// An entry in a segment's blob index.
///
/// Maps a blob's position within the segment to its key hash for O(log n) lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentIndexEntry {
    /// Byte offset of the blob within the segment.
    pub offset: u64,
    /// Length of the blob in bytes.
    pub length: u32,
    /// SHA-256 hash of the blob's object key for identity verification.
    pub blob_key_hash: [u8; 32],
}

// ---------------------------------------------------------------------------
// StorageLocation
// ---------------------------------------------------------------------------

/// A storage location — a node holding a segment shard.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StorageLocation {
    /// The node holding this shard.
    pub node_id: NodeId,
    /// Index of the shard (0..k for data, k..k+m-1 for parity).
    pub shard_index: u8,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::Hlc;

    // -- ChunkRef --

    #[test]
    fn chunk_ref_construction_and_access() {
        let seg = SegmentId::new();
        let chunk = ChunkRef { segment_id: seg, offset: 4096, length: 1024 };
        assert_eq!(chunk.offset, 4096);
        assert_eq!(chunk.length, 1024);
        assert_eq!(chunk.segment_id, seg);
    }

    #[test]
    fn chunk_ref_copy_and_eq() {
        let seg = SegmentId::new();
        let a = ChunkRef { segment_id: seg, offset: 0, length: 100 };
        let b = a; // Copy
        assert_eq!(a, b);
    }

    // -- ObjectMetadata: is_inline, is_segment_stored --

    #[test]
    fn object_metadata_is_inline_when_inline_data_present() {
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("x"),
            size: 4,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(bytes::Bytes::from_static(b"data")),
            created_at: 0,
            hlc: Hlc::zero(),
        };
        assert!(meta.is_inline());
        assert!(!meta.is_segment_stored());
    }

    #[test]
    fn object_metadata_is_segment_stored_when_chunks_present() {
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef { segment_id: SegmentId::new(), offset: 0, length: 200 });
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("y"),
            size: 200,
            blake3_hash: None,
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };
        assert!(meta.is_segment_stored());
        assert!(!meta.is_inline());
    }

    // -- Hlc in ObjectMetadata --

    #[test]
    fn object_metadata_hlc_integration() {
        let hlc = Hlc::new(1000, 5);
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("hlc-obj"),
            size: 42,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: None,
            created_at: 1000,
            hlc,
        };
        assert_eq!(meta.hlc.wall_time(), 1000);
        assert_eq!(meta.hlc.logical(), 5);
    }

    // -- Tombstone with HLC --

    #[test]
    fn tombstone_hlc_integration() {
        let ts = Tombstone { deletion_time: 1700000000000, hlc: Hlc::new(1700000000000, 0) };
        assert_eq!(ts.deletion_time, 1700000000000);
        assert_eq!(ts.hlc.wall_time(), 1700000000000);
    }

    // -- SegmentMetadata: is_sealed --

    #[test]
    fn segment_metadata_is_sealed_when_sealed_at_present() {
        let meta = SegmentMetadata {
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
        };
        assert!(meta.is_sealed());
    }

    #[test]
    fn segment_metadata_is_not_sealed_when_none() {
        let meta = SegmentMetadata {
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: None,
        };
        assert!(!meta.is_sealed());
    }

    // -- SegmentIndexEntry --

    #[test]
    fn segment_index_entry_construction() {
        let entry = SegmentIndexEntry { offset: 1024, length: 512, blob_key_hash: [0xABu8; 32] };
        assert_eq!(entry.offset, 1024);
        assert_eq!(entry.length, 512);
        assert_eq!(entry.blob_key_hash, [0xABu8; 32]);
    }

    // -- StorageLocation --

    #[test]
    fn storage_location_construction() {
        let loc = StorageLocation { node_id: NodeId::new("n1"), shard_index: 3 };
        assert_eq!(loc.node_id.as_str(), "n1");
        assert_eq!(loc.shard_index, 3);
    }
}
