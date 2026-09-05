//! Object and segment metadata types.
//!
//! Core metadata structures for the object store: `ObjectMetadata` (per-object),
//! `SegmentMetadata` (per-segment), `ChunkRef` (blob-to-segment mapping),
//! `SegmentIndexEntry` (in-segment blob index), `Tombstone` (deletion marker),
//! `StorageLocation` (node + shard), and the `MetadataStore` trait.

use super::{
    config::SizeTier,
    id::{BucketId, NodeId, ObjectKey, SegmentId},
};
use crate::{types::hash_output::HashOutput, Hlc};

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
///     length: 1024, compressed: false, logical_length: 1024,
/// };
/// assert_eq!(chunk.length, 1024);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChunkRef {
    /// The segment containing this chunk.
    pub segment_id: SegmentId,
    /// Byte offset of the chunk within the segment.
    pub offset: u64,
    /// Length of the chunk as stored in the segment file (compressed
    /// when `compressed` is true, logical otherwise).
    pub length: u32,
    /// Whether the stored bytes are compressed (zstd via the accel
    /// dispatcher). When `true`, `logical_length` holds the original
    /// uncompressed size and readers must decompress before use.
    #[serde(default)]
    pub compressed: bool,
    /// Original uncompressed length of the chunk. Meaningful only when
    /// `compressed` is true; ignored otherwise.
    #[serde(default)]
    pub logical_length: u32,
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
/// Tracks EC parameters, storage locations, the Merkle root for integrity
/// verification, and the storage-pool id (ADR-0029: the segment→pool
/// mapping, persisted through the event WAL / checkpoint — the only
/// durable segment-state path, ADR-0024/25). `pool_id = 0` is the legacy
/// single-root layout.
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
///     pool_id: 0,
///     total_bytes: 0,
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
    /// The storage pool holding this segment's `.dat` file (ADR-0029 §D1).
    ///
    /// `0` = the legacy `{data_dir}/segments` root. Defaults to `0` on
    /// deserialize so legacy event-WAL records, checkpoints, and metadata
    /// payloads keep working — no migration pass (f5 accepted deviation).
    #[serde(default)]
    pub pool_id: u32,
    /// Logical byte total of the segment's data section, recorded at seal
    /// (ADR-0034 D1).
    ///
    /// `total_bytes` is the data-section byte length of the segment's
    /// `.dat` at seal (= Σ blob lengths, and = the `size` field the sealer
    /// writes into the segment header). It is the `logical_total` half of
    /// the accounting invariant `live = logical_total − dead` that GC
    /// liveness and orphan detection consume (f2). Defaults to `0` on
    /// deserialize so pre-f3 JSON metadata stays readable; accounting
    /// consumers treat a Sealed entry whose `total_bytes` is `0` as
    /// "unknown" and never as fully-dead.
    #[serde(default)]
    pub total_bytes: u64,
}

impl SegmentMetadata {
    /// Returns `true` if the segment has been sealed.
    pub fn is_sealed(&self) -> bool {
        self.sealed_at.is_some()
    }
}

// ---------------------------------------------------------------------------
// ContainedObject
// ---------------------------------------------------------------------------

/// One object contained in a sealed segment (ADR-0034 D5).
///
/// The write coordinator knows the `(bucket, key)` of every chunk it
/// appends, so at seal time the segment records a compact
/// **contained-objects membership list** — deduplicated by `(bucket, key)`
/// and sorted so its serialization is deterministic (an object split across
/// chunks in one segment appears once). The list lives with the segment's
/// metadata on the event-WAL + checkpoint path, never inside the `.dat`
/// binary (ADR-0034 boundary; ADR-0029 D7's deferred self-description stays
/// deferred). GC compaction enumerates a segment's objects from this list
/// (plus point lookups) instead of scanning the whole objects column family.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{BucketId, ContainedObject, ObjectKey};
///
/// let a = ContainedObject { bucket: BucketId::new("b1"), key: ObjectKey::new("k1") };
/// let b = ContainedObject { bucket: BucketId::new("b1"), key: ObjectKey::new("k2") };
/// let mut objs = vec![b.clone(), a.clone(), a.clone()];
/// let dedup = ContainedObject::sorted_dedup(objs);
/// assert_eq!(dedup, vec![a, b]);
/// ```
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct ContainedObject {
    /// Bucket of the contained object.
    pub bucket: BucketId,
    /// Key of the contained object.
    pub key: ObjectKey,
}

impl ContainedObject {
    /// Sorts a contained-object list by `(bucket, key)` and removes
    /// duplicates, so its serialization is deterministic and an object
    /// split across chunks in one segment appears exactly once.
    pub fn sorted_dedup(mut objects: Vec<ContainedObject>) -> Vec<ContainedObject> {
        objects.sort();
        objects.dedup();
        objects
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
///
/// The tombstone carries the deleted object's chunk references: the
/// object row is removed from `CF_OBJECTS` at delete time (S3 GET/LIST
/// semantics), so the tombstone is the ONLY surviving record of which
/// segments hold the dead bytes. GC marks those chunks dead directly
/// from the tombstone — without this, GC could never detect dead bytes
/// for deleted objects (`gc_dead_bytes_total` stayed 0 forever).
///
/// `chunks` is empty for legacy tombstones written before this field
/// existed (defaulted on deserialize); their dead bytes are unreachable
/// and rely on the orphan reaper instead.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Tombstone {
    /// When the deletion occurred (milliseconds since epoch).
    pub deletion_time: i64,
    /// HLC timestamp for conflict resolution.
    pub hlc: Hlc,
    /// Chunk references of the deleted object, captured before the
    /// object row was removed. GC marks these chunks dead.
    #[serde(default)]
    pub chunks: smallvec::SmallVec<[ChunkRef; 4]>,
}

// ---------------------------------------------------------------------------
// Dead-chunk records
// ---------------------------------------------------------------------------

/// The kind of a captured dead-chunk record in the `deletions` column
/// family (ADR-0034 D2).
///
/// Every chunk reference that stops being referenced by a live object row
/// is captured into a dead-chunk record **atomically with the row change**
/// — either a delete (`Tombstone`) or an overwrite of a superseded version
/// (`Supersede`). GC and the orphan reaper read both kinds through the
/// [`DeadChunkRecord`] enumeration; the two kinds differ only in how they
/// were produced, never in how their bytes are accounted.
///
/// # Examples
///
/// ```
/// use oceanfs_core::DeadChunkKind;
///
/// assert_ne!(DeadChunkKind::Tombstone, DeadChunkKind::Supersede);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeadChunkKind {
    /// A chunk set captured by `delete_object` (a plain `deletions`-CF
    /// tombstone record at the exact `{bucket}\0{key}` key).
    Tombstone,
    /// A chunk set captured by an overwrite at `put_object_in_bucket`
    /// (a versioned supersede record keyed with the superseded version's
    /// HLC, so it coexists with the new live row, ages under the tombstone
    /// TTL discipline, and is never interpreted as a delete of the key).
    Supersede,
}

/// Typed read-side view of a captured dead-chunk record.
///
/// Returned by `MetadataStore::list_dead_chunk_records_all` (f2's
/// accounting feed). It is **not** a new on-disk format: the stored value
/// keeps the [`Tombstone`] shape (`deletion_time` = `captured_at`, `hlc` =
/// the superseded version's HLC for supersedes / the delete HLC for plain
/// tombstones), and the `kind` is derived from the deletions-CF key
/// classification.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{ChunkRef, DeadChunkKind, DeadChunkRecord, Hlc};
///
/// let record = DeadChunkRecord {
///     kind: DeadChunkKind::Supersede,
///     captured_at: 1_700_000_000_000,
///     hlc: Hlc::new(1_700_000_000_000, 3),
///     chunks: smallvec::SmallVec::new(),
/// };
/// assert!(record.chunks.is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadChunkRecord {
    /// Whether this record came from a delete or an overwrite supersede.
    pub kind: DeadChunkKind,
    /// When the dead bytes were captured (milliseconds since epoch);
    /// drives TTL aging (`now_ms - captured_at > tombstone_ttl`).
    pub captured_at: i64,
    /// The superseded version's HLC (supersedes) or the delete's HLC
    /// (plain tombstones). For supersedes this is also the version
    /// discriminator that reconstructs the stored key.
    pub hlc: Hlc,
    /// The chunk references whose bytes became dead. For a supersede this
    /// is the superseded version's chunk set, attributed to the segments it
    /// referenced; empty for inline objects and legacy records.
    pub chunks: smallvec::SmallVec<[ChunkRef; 4]>,
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

    #[test]
    fn contained_object_sorted_dedup_is_deterministic_and_dedupes() {
        // ADR-0034 D5: an object split across three chunks in one segment
        // appears exactly once, and the serialization order is sorted.
        let a = ContainedObject { bucket: BucketId::new("b2"), key: ObjectKey::new("z") };
        let b = ContainedObject { bucket: BucketId::new("b1"), key: ObjectKey::new("m") };
        let c = ContainedObject { bucket: BucketId::new("b1"), key: ObjectKey::new("a") };
        // Three duplicate appends of `b` (one per chunk) plus a and c.
        let mut list = vec![b.clone(), b.clone(), b.clone(), a.clone(), c.clone()];
        let dedup = ContainedObject::sorted_dedup(std::mem::take(&mut list));
        assert_eq!(dedup, vec![c, b, a], "sorted by (bucket, key) with duplicates removed");
    }

    // -- ChunkRef --

    #[test]
    fn chunk_ref_construction_and_access() {
        let seg = SegmentId::new();
        let chunk = ChunkRef {
            segment_id: seg,
            offset: 4096,
            length: 1024,
            compressed: false,
            logical_length: 1024,
        };
        assert_eq!(chunk.offset, 4096);
        assert_eq!(chunk.length, 1024);
        assert_eq!(chunk.segment_id, seg);
    }

    #[test]
    fn chunk_ref_copy_and_eq() {
        let seg = SegmentId::new();
        let a = ChunkRef {
            segment_id: seg,
            offset: 0,
            length: 100,
            compressed: false,
            logical_length: 100,
        };
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
        chunks.push(ChunkRef {
            segment_id: SegmentId::new(),
            offset: 0,
            length: 200,
            compressed: false,
            logical_length: 200,
        });
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
        let ts = Tombstone {
            deletion_time: 1700000000000,
            hlc: Hlc::new(1700000000000, 0),
            chunks: smallvec::SmallVec::new(),
        };
        assert_eq!(ts.deletion_time, 1700000000000);
        assert_eq!(ts.hlc.wall_time(), 1700000000000);
    }

    // -- SegmentMetadata: is_sealed --

    #[test]
    fn segment_metadata_is_sealed_when_sealed_at_present() {
        let meta = SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
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
            pool_id: 0,
            total_bytes: 0,
        };
        assert!(!meta.is_sealed());
    }

    // -- SegmentMetadata: pool_id (ADR-0029 f5) --

    /// The pool_id round-trips through serde (json + bincode — the
    /// checkpoint wire format).
    #[test]
    fn segment_metadata_pool_id_serde_roundtrip() {
        let meta = SegmentMetadata {
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1700000000000),
            pool_id: 3,
            total_bytes: 0,
        };

        let json = serde_json::to_string(&meta).unwrap();
        let from_json: SegmentMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(from_json.pool_id, 3);

        let bincode_bytes = bincode::serialize(&meta).unwrap();
        let from_bincode: SegmentMetadata = bincode::deserialize(&bincode_bytes).unwrap();
        assert_eq!(from_bincode.pool_id, 3);
    }

    /// Legacy records (no pool_id field) deserialize with pool_id = 0 —
    /// the legacy root. No migration needed (f5 accepted deviation).
    #[test]
    fn segment_metadata_legacy_record_defaults_pool_id_zero() {
        let legacy_json = r#"{
            "segment_id": "00000000-0000-0000-0000-000000000001",
            "ec_k": 4,
            "ec_m": 2,
            "size_tier": "Standard",
            "merkle_root": null,
            "storage_locations": [],
            "sealed_at": 1700000000000
        }"#;
        let meta: SegmentMetadata = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(meta.pool_id, 0, "legacy records default to the legacy root");
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
