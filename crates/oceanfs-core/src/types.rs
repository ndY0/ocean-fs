//! Shared types used across all OceanFS crates.
//!
//! These are the fundamental domain types — identifiers, hashes, and
//! keys — that every subsystem references.

use std::fmt;

use crate::Hlc;

/// A time-sortable segment identifier (UUIDv7).
///
/// Segment IDs are generated when a new active segment is created.
/// They are used as keys in the `segments` RocksDB column family and
/// as references in `ObjectMetadata.chunks`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::SegmentId;
///
/// let id = SegmentId::new();
/// let as_uuid = id.as_uuid();
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct SegmentId(uuid::Uuid);

impl SegmentId {
    /// Creates a new time-sortable segment ID (UUIDv7).
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    /// Returns the underlying [`uuid::Uuid`].
    pub fn as_uuid(&self) -> uuid::Uuid {
        self.0
    }

    /// Creates a `SegmentId` from a 16-byte UUID byte array.
    pub fn from_uuid_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }
}

impl Default for SegmentId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// ---------------------------------------------------------------------------
// NodeId
// ---------------------------------------------------------------------------

/// A unique identifier for a node in the OceanFS cluster.
///
/// # Examples
///
/// ```
/// use oceanfs_core::NodeId;
///
/// let node = NodeId::new("node-1");
/// assert_eq!(node.as_str(), "node-1");
/// ```
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct NodeId(String);

impl NodeId {
    /// Creates a new `NodeId` from a string identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the node ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

// ---------------------------------------------------------------------------
// BucketId
// ---------------------------------------------------------------------------

/// A bucket identifier.
///
/// Bucket names must follow S3 naming conventions: 3–63 characters,
/// lowercase letters, numbers, hyphens, and periods.
///
/// # Examples
///
/// ```
/// use oceanfs_core::BucketId;
///
/// let bucket = BucketId::new("my-photos");
/// assert_eq!(bucket.as_str(), "my-photos");
/// ```
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct BucketId(String);

impl BucketId {
    /// Creates a new `BucketId` from a string identifier.
    ///
    /// # Panics
    ///
    /// Only in debug builds: panics if the name is empty or contains
    /// uppercase characters to catch configuration errors early.
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        debug_assert!(!name.is_empty(), "bucket name must not be empty");
        Self(name)
    }

    /// Returns the bucket name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BucketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for BucketId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for BucketId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

// ---------------------------------------------------------------------------
// ObjectKey
// ---------------------------------------------------------------------------

/// An object key within a bucket.
///
/// Object keys are UTF-8 strings that may include `/` delimiters for
/// hierarchical namespacing (e.g., `photos/2026/vacation/img_001.jpg`).
///
/// # Examples
///
/// ```
/// use oceanfs_core::ObjectKey;
///
/// let key = ObjectKey::new("photos/cat.jpg");
/// assert_eq!(key.as_str(), "photos/cat.jpg");
/// ```
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Creates a new `ObjectKey`.
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// Returns the key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&str> for ObjectKey {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for ObjectKey {
    fn from(s: String) -> Self {
        Self(s)
    }
}

// ---------------------------------------------------------------------------
// HashOutput
// ---------------------------------------------------------------------------

/// A 256-bit BLAKE3 hash output (32 bytes).
///
/// Used as the object-content checksum, segment checksum, and Merkle tree
/// node hash throughout the system.
///
/// # Examples
///
/// ```
/// use oceanfs_core::HashOutput;
///
/// let hash = HashOutput::from_bytes([0u8; 32]);
/// let hex = hash.to_hex();
/// assert_eq!(hex.len(), 64);
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct HashOutput([u8; 32]);

impl HashOutput {
    /// Creates a `HashOutput` from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the hash as a byte slice.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the hash as a lowercase hexadecimal string.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Display for HashOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

// Minimal hex encoding — avoids an external dependency for a single function.
// ---------------------------------------------------------------------------
// SizeTier
// ---------------------------------------------------------------------------

/// The segment tier for a blob, based on its size.
///
/// Determines how the blob is stored: inline in metadata, packed into
/// a small segment, one blob per standard segment, or split across
/// multiple segments.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{SizeTier, SegmentSizeConfig};
///
/// let config = SegmentSizeConfig::default();
/// assert_eq!(config.classify(1024), SizeTier::Inline);
/// assert_eq!(config.classify(65536), SizeTier::Small);
/// assert_eq!(config.classify(1048576), SizeTier::Standard);
/// assert_eq!(config.classify(10485760), SizeTier::Multi);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum SizeTier {
    /// Blob stored inline in metadata (≤ `inline_threshold_bytes`).
    Inline,
    /// Blob packed into a small segment (≤ `small_threshold_bytes`).
    Small,
    /// One blob per standard segment (≤ `default_target_size`).
    Standard,
    /// Blob split across multiple segments (> `default_target_size`).
    Multi,
}

// ---------------------------------------------------------------------------
// SegmentSizeConfig
// ---------------------------------------------------------------------------

/// Configuration for tiered segment sizing.
///
/// Controls the four-tier storage strategy defined in ADR-0001:
/// inline, small segment, standard segment, and multi-segment.
///
/// # Examples
///
/// ```
/// use oceanfs_core::SegmentSizeConfig;
///
/// let config = SegmentSizeConfig::default();
/// assert_eq!(config.inline_threshold_bytes, 4096);
/// assert_eq!(config.small_threshold_bytes, 262144);
/// ```
#[derive(Debug, Clone)]
pub struct SegmentSizeConfig {
    /// Maximum blob size for inline storage (default 4 KB).
    pub inline_threshold_bytes: u64,
    /// Maximum blob size for small segment packing (default 256 KB).
    pub small_threshold_bytes: u64,
    /// Target total size for a small segment (default 64 KB).
    pub small_target_size: u64,
    /// Target total size for a standard segment (default 4 MB).
    pub default_target_size: u64,
}

impl Default for SegmentSizeConfig {
    fn default() -> Self {
        Self {
            inline_threshold_bytes: 4096,
            small_threshold_bytes: 262144,
            small_target_size: 65536,
            default_target_size: 4194304,
        }
    }
}

impl SegmentSizeConfig {
    /// Classifies a blob size into its appropriate storage tier.
    ///
    /// # Panics
    ///
    /// In debug builds: panics if `blob_size` is zero.
    pub fn classify(&self, blob_size: u64) -> SizeTier {
        debug_assert!(blob_size > 0, "blob size must be > 0");
        if blob_size <= self.inline_threshold_bytes {
            SizeTier::Inline
        } else if blob_size <= self.small_threshold_bytes {
            SizeTier::Small
        } else if blob_size <= self.default_target_size {
            SizeTier::Standard
        } else {
            SizeTier::Multi
        }
    }
}

// ---------------------------------------------------------------------------
// Hex encoding helper
// ---------------------------------------------------------------------------

/// Hex encoding for HashOutput.
mod hex {
    pub(super) const CHARS: &[u8; 16] = b"0123456789abcdef";

    pub(super) fn encode(bytes: [u8; 32]) -> String {
        let mut out = String::with_capacity(64);
        for byte in bytes {
            out.push(CHARS[(byte >> 4) as usize] as char);
            out.push(CHARS[(byte & 0x0f) as usize] as char);
        }
        out
    }
}

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
// SegmentIndexEntry and SegmentIndex
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
// VnodeRange
// ---------------------------------------------------------------------------

/// A key range affected by a ring topology change.
///
/// When a node is added or removed from the ring, the affected key range
/// identifies which keys need data migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VnodeRange {
    /// Start of the affected key range (inclusive).
    pub start: [u8; 32],
    /// End of the affected key range (exclusive).
    pub end: [u8; 32],
}

// ---------------------------------------------------------------------------
// NodeState
// ---------------------------------------------------------------------------

/// The state of a node in the cluster membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Node is healthy and participating.
    Alive,
    /// Node is suspected down (unreachable via direct or indirect ping).
    Suspect,
    /// Node is confirmed dead.
    Dead,
    /// Node is gracefully leaving the cluster.
    Leaving,
    /// Node has left the cluster.
    Left,
}

// ---------------------------------------------------------------------------
// GossipConfig
// ---------------------------------------------------------------------------

/// Configuration for the SWIM gossip membership protocol.
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// Interval between gossip rounds in milliseconds.
    pub interval_ms: u64,
    /// Time in SUSPECT state before declaring DEAD.
    pub suspicion_timeout_ms: u64,
    /// Total time before declaring DEAD.
    pub failure_timeout_ms: u64,
    /// Number of peers to route indirect pings through.
    pub indirect_ping_count: u8,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            interval_ms: 1000,
            suspicion_timeout_ms: 5000,
            failure_timeout_ms: 15000,
            indirect_ping_count: 3,
        }
    }
}

// ---------------------------------------------------------------------------
// WriteResult / WriteAck
// ---------------------------------------------------------------------------

/// Result of a successful write operation.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// The object key that was written.
    pub object_key: ObjectKey,
    /// The chunks referencing the object's data in segments.
    pub chunks: smallvec::SmallVec<[ChunkRef; 4]>,
    /// Total size of the object in bytes.
    pub size: u64,
    /// BLAKE3 hash of the object content.
    pub blake3_hash: Option<HashOutput>,
}

/// Acknowledgment from a replica node for a write.
#[derive(Debug, Clone)]
pub struct WriteAck {
    /// The node that acknowledged.
    pub node_id: NodeId,
    /// WAL position on that node.
    pub wal_position: u64,
    /// HLC timestamp of the write.
    pub hlc: Hlc,
}

// ---------------------------------------------------------------------------
// CodecType / CodecConfig
// ---------------------------------------------------------------------------

/// Supported erasure coding codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CodecType {
    /// Cauchy Reed-Solomon over GF(2^8).
    CauchyRs,
}

/// Configuration for an erasure coding codec.
#[derive(Debug, Clone)]
pub struct CodecConfig {
    /// The codec to use.
    pub codec_type: CodecType,
    /// Number of data shards (k).
    pub data_shards: u8,
    /// Number of parity shards (m).
    pub parity_shards: u8,
    /// Size of each shard in bytes.
    pub strip_size_bytes: usize,
}

impl Default for CodecConfig {
    fn default() -> Self {
        Self {
            codec_type: CodecType::CauchyRs,
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 65536,
        }
    }
}

// ---------------------------------------------------------------------------
// EncodingPlan
// ---------------------------------------------------------------------------

/// A pre-computed plan for encoding a segment.
#[derive(Debug, Clone)]
pub struct EncodingPlan {
    /// Number of stripes in the segment.
    pub stripe_count: usize,
    /// Total size after padding.
    pub padded_size: u64,
    /// Size of each individual shard.
    pub shard_size: usize,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- SegmentId --

    #[test]
    fn segment_id_new_generates_unique_ids() {
        let a = SegmentId::new();
        let b = SegmentId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn segment_id_display_is_uuid_string() {
        let id = SegmentId::new();
        let s = id.to_string();
        // UUIDv7 format: 36 chars with 4 hyphens
        assert_eq!(s.len(), 36);
        assert_eq!(s.chars().filter(|c| *c == '-').count(), 4);
    }

    // -- NodeId --

    #[test]
    fn node_id_from_str_and_display_roundtrip() {
        let id = NodeId::from("node-7");
        assert_eq!(id.as_str(), "node-7");
        assert_eq!(id.to_string(), "node-7");
    }

    // -- BucketId --

    #[test]
    fn bucket_id_as_str_returns_name() {
        let bucket = BucketId::new("archive");
        assert_eq!(bucket.as_str(), "archive");
    }

    // -- ObjectKey --

    #[test]
    fn object_key_preserves_slashes() {
        let key = ObjectKey::new("a/b/c");
        assert_eq!(key.as_str(), "a/b/c");
    }

    // -- HashOutput --

    #[test]
    fn hash_output_to_hex_is_64_chars() {
        let hash = HashOutput::from_bytes([0xabu8; 32]);
        let hex = hash.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_output_to_hex_is_lowercase() {
        let hash = HashOutput::from_bytes([0xFFu8; 32]);
        let hex = hash.to_hex();
        assert_eq!(hex, "ff".repeat(32));
    }

    // -- SizeTier / SegmentSizeConfig --

    #[test]
    fn classify_1kb_is_inline() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(1024), SizeTier::Inline);
    }

    #[test]
    fn classify_4096_is_inline_boundary() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(4096), SizeTier::Inline);
    }

    #[test]
    fn classify_4097_is_small() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(4097), SizeTier::Small);
    }

    #[test]
    fn classify_256kb_is_small_boundary() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(262144), SizeTier::Small);
    }

    #[test]
    fn classify_256kb_plus_one_is_standard() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(262145), SizeTier::Standard);
    }

    #[test]
    fn classify_4mb_is_standard_boundary() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(4194304), SizeTier::Standard);
    }

    #[test]
    fn classify_4mb_plus_one_is_multi() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(4194305), SizeTier::Multi);
    }

    #[test]
    fn classify_10mb_is_multi() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(10485760), SizeTier::Multi);
    }

    #[test]
    #[should_panic]
    fn classify_zero_panics_in_debug() {
        let config = SegmentSizeConfig::default();
        config.classify(0);
    }
}
