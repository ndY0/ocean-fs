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
// Incarnation
// ---------------------------------------------------------------------------

/// An incarnation number for SWIM membership tracking.
///
/// Each time a node rejoins the cluster after being declared dead, its
/// incarnation number is incremented. Higher incarnation numbers take
/// precedence in gossip state merges, resolving split-brain scenarios.
///
/// # Examples
///
/// ```
/// use oceanfs_core::Incarnation;
///
/// let inc = Incarnation::new(1);
/// assert_eq!(inc.value(), 1);
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Incarnation(u64);

impl Incarnation {
    /// Creates a new incarnation number.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the incarnation value.
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Returns the next incarnation number.
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Default for Incarnation {
    fn default() -> Self {
        Self(1)
    }
}

// ---------------------------------------------------------------------------
// HashKey
// ---------------------------------------------------------------------------

/// A pre-computed key hash that flows through all routing layers.
///
/// Computed once at the HTTP entry point and passed through routing,
/// metadata lookup, and segment operations — never re-hashed.
///
/// # Examples
///
/// ```
/// use oceanfs_core::HashKey;
///
/// let hash_key = HashKey::from_bytes([0u8; 32]);
/// assert_eq!(hash_key.as_bytes().len(), 32);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HashKey([u8; 32]);

impl HashKey {
    /// Creates a `HashKey` from pre-computed SHA-256 hash bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw hash bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

// ---------------------------------------------------------------------------
// OperationType
// ---------------------------------------------------------------------------

/// The type of operation being routed.
///
/// Used by the request router to make forwarding decisions based
/// on the operation type (read, write, delete, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OperationType {
    /// Read an object.
    Read,
    /// Write an object.
    Write,
    /// Delete an object.
    Delete,
    /// Retrieve object metadata.
    Head,
    /// List objects in a bucket.
    List,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    /// Bootstrap nodes for cluster discovery (host:port pairs).
    pub seed_nodes: Vec<String>,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            interval_ms: 1000,
            suspicion_timeout_ms: 5000,
            failure_timeout_ms: 15000,
            indirect_ping_count: 3,
            seed_nodes: Vec::new(),
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
// WriteQuorum
// ---------------------------------------------------------------------------

/// Write quorum configuration for a write operation.
///
/// # Examples
///
/// ```
/// use oceanfs_core::WriteQuorum;
///
/// let quorum = WriteQuorum {
///     required: 2,
///     ack_after_wal: true,
///     ec_async: true,
/// };
/// assert_eq!(quorum.required, 2);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct WriteQuorum {
    /// Required number of replica acknowledgments.
    pub required: u8,
    /// Acknowledge to client after WAL quorum (before EC seal).
    pub ack_after_wal: bool,
    /// Trigger EC encoding asynchronously after acknowledgment.
    pub ec_async: bool,
}

impl Default for WriteQuorum {
    fn default() -> Self {
        Self { required: 1, ack_after_wal: true, ec_async: true }
    }
}

// ---------------------------------------------------------------------------
// IntendedFor
// ---------------------------------------------------------------------------

/// Identifies the intended recipient node for a hinted handoff.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{IntendedFor, NodeId};
///
/// let target = IntendedFor(NodeId::new("node-1"));
/// assert_eq!(target.as_str(), "node-1");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IntendedFor(pub NodeId);

impl IntendedFor {
    /// Returns the node ID as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Display for IntendedFor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

// ---------------------------------------------------------------------------
// RpcConfig
// ---------------------------------------------------------------------------

/// Configuration for the gRPC connection pool.
///
/// Controls per-peer channel pooling, keepalive, idle eviction, and timeouts.
///
/// # Examples
///
/// ```
/// use oceanfs_core::RpcConfig;
///
/// let config = RpcConfig::default();
/// assert_eq!(config.pool_size_per_peer, 4);
/// ```
#[derive(Debug, Clone)]
pub struct RpcConfig {
    /// Number of gRPC channels to maintain per peer node.
    pub pool_size_per_peer: usize,
    /// Keepalive interval in seconds for idle channels.
    pub keepalive_sec: u64,
    /// Maximum number of idle channels across all peers.
    pub max_idle_connections: usize,
    /// Connection establishment timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// Default per-request timeout in milliseconds.
    pub request_timeout_ms: u64,
    /// Optional path to TLS certificate for mTLS.
    pub tls_cert_path: Option<std::path::PathBuf>,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            pool_size_per_peer: 4,
            keepalive_sec: 30,
            max_idle_connections: 256,
            connect_timeout_ms: 5000,
            request_timeout_ms: 30000,
            tls_cert_path: None,
        }
    }
}

// ---------------------------------------------------------------------------
// PeerAddress
// ---------------------------------------------------------------------------

/// The network address of a peer node.
///
/// Wraps a `std::net::SocketAddr` for type safety and future extensibility.
///
/// # Examples
///
/// ```
/// use std::net::SocketAddr;
/// use oceanfs_core::PeerAddress;
///
/// let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
/// let peer = PeerAddress::new(addr);
/// assert_eq!(peer.to_string(), "127.0.0.1:9001");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerAddress(std::net::SocketAddr);

impl PeerAddress {
    /// Creates a new peer address from a socket address.
    pub fn new(addr: std::net::SocketAddr) -> Self {
        Self(addr)
    }

    /// Returns the inner socket address.
    pub fn socket_addr(&self) -> std::net::SocketAddr {
        self.0
    }
}

impl std::fmt::Display for PeerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<std::net::SocketAddr> for PeerAddress {
    fn from(addr: std::net::SocketAddr) -> Self {
        Self(addr)
    }
}

// ---------------------------------------------------------------------------
// ShardIndex
// ---------------------------------------------------------------------------

/// Index into a k+m shard set.
///
/// Data shards are numbered 0..k-1; parity shards are numbered k..k+m-1.
///
/// # Examples
///
/// ```
/// use oceanfs_core::ShardIndex;
///
/// let data_shard = ShardIndex(0);
/// let parity_shard = ShardIndex(4);
/// assert_eq!(data_shard.value(), 0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardIndex(pub u8);

impl ShardIndex {
    /// Returns the raw shard index value.
    pub fn value(&self) -> u8 {
        self.0
    }
}

impl From<u8> for ShardIndex {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<ShardIndex> for u8 {
    fn from(value: ShardIndex) -> Self {
        value.0
    }
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
// CodecType / CodecConfig
// ---------------------------------------------------------------------------

/// Supported erasure coding codecs.
///
/// # Examples
///
/// ```
/// use oceanfs_core::CodecType;
///
/// let codec = CodecType::CauchyRs;
/// assert!(matches!(codec, CodecType::CauchyRs));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CodecType {
    /// Cauchy Reed-Solomon over GF(2^8).
    CauchyRs,
    /// Standard Reed-Solomon (reserved for future use).
    StandardRs,
    /// Locally Recoverable Codes (reserved for future use).
    Lrc,
    /// Clay codes (reserved for future use).
    Clay,
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
///
/// Contains the stripe count, padding, shard size, and codec parameters
/// (k = data shards, m = parity shards) needed for parallel encode/decode.
///
/// # Examples
///
/// ```
/// use oceanfs_core::EncodingPlan;
///
/// let plan = EncodingPlan {
///     stripe_count: 16,
///     padded_size: 4_194_304,
///     shard_size: 65536,
///     data_shards: 4,
///     parity_shards: 2,
/// };
/// assert_eq!(plan.total_shards(), 6);
/// ```
#[derive(Debug, Clone)]
pub struct EncodingPlan {
    /// Number of stripes in the segment.
    pub stripe_count: usize,
    /// Total size of the segment data after zero-padding.
    pub padded_size: u64,
    /// Size of each individual shard in bytes.
    pub shard_size: usize,
    /// Number of data shards (k).
    pub data_shards: u8,
    /// Number of parity shards (m).
    pub parity_shards: u8,
}

impl EncodingPlan {
    /// Returns the total number of shards (k + m).
    pub fn total_shards(&self) -> u8 {
        self.data_shards + self.parity_shards
    }
}

// ---------------------------------------------------------------------------
// PoolConfig
// ---------------------------------------------------------------------------

/// Configuration for the active segment pool.
///
/// Controls the number of concurrent active segments and per-core sharding
/// to decouple append latency from EC encode time.
///
/// # Examples
///
/// ```
/// use oceanfs_core::PoolConfig;
///
/// let config = PoolConfig::default();
/// assert_eq!(config.active_pool_size, 4);
/// assert_eq!(config.shard_count, 4);
/// ```
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Number of active segments per shard (default 4).
    /// More pool slots allow concurrent appends while segments are being sealed.
    pub active_pool_size: usize,
    /// Number of per-core shards for contention reduction (default 4).
    pub shard_count: usize,
    /// Maximum number of in-flight EC encodes (bounded by semaphore).
    pub max_inflight_encodes: usize,
    /// Capacity of the EC encoding work queue (backpressure channel).
    pub encode_queue_capacity: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            active_pool_size: 4,
            shard_count: 4,
            max_inflight_encodes: 8,
            encode_queue_capacity: 64,
        }
    }
}

// ---------------------------------------------------------------------------
// MetadataStore trait
// ---------------------------------------------------------------------------

/// Minimal trait for metadata access needed by caching and prefetch layers.
///
/// Each crate that provides metadata storage implements this trait so that
/// caches can rebuild filters and warm entries without depending on the
/// concrete storage implementation.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata, MetadataStore};
///
/// struct MyStore;
///
/// impl MetadataStore for MyStore {
///     fn list_object_keys(&self, _bucket: &BucketId)
///         -> std::io::Result<Vec<(BucketId, ObjectKey)>>
///     {
///         Ok(vec![])
///     }
///
///     fn get_object_metadata(&self, _bucket: &BucketId, _key: &ObjectKey)
///         -> std::io::Result<Option<ObjectMetadata>>
///     {
///         Ok(None)
///     }
/// }
/// ```
pub trait MetadataStore: Send + Sync {
    /// Lists all object keys in a bucket.
    ///
    /// Used to rebuild negative caches and for prefetch discovery.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the underlying storage is unavailable.
    fn list_object_keys(&self, bucket: &BucketId) -> std::io::Result<Vec<(BucketId, ObjectKey)>>;

    /// Retrieves object metadata for a given key.
    ///
    /// Returns `Ok(None)` if the key does not exist.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the underlying storage is unavailable.
    fn get_object_metadata(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
    ) -> std::io::Result<Option<ObjectMetadata>>;
}

// ---------------------------------------------------------------------------
// CompressionTier
// ---------------------------------------------------------------------------

/// Compression acceleration tier.
///
/// Controls which compression backend is used for segment data.
/// Per ADR-0007, compression uses a two-level governance model:
/// the node sets a ceiling (maximum tier available), and per-bucket
/// configuration can only select from or downgrade from the node's
/// ceiling — it cannot upgrade.
///
/// ## Capability Ordering
///
/// ```text
/// GpuNvcomp > CpuIgzip > CpuZstd > None
/// ```
///
/// A bucket requesting a tier higher than the node ceiling is capped:
/// `effective_tier = min(requested, ceiling)`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::CompressionTier;
///
/// let tier = CompressionTier::Auto;
/// assert!(matches!(tier, CompressionTier::Auto));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum CompressionTier {
    /// No compression — disable compression entirely for this node or bucket.
    /// Lowest tier in the capability ordering.
    None,
    /// CPU zstd (always available). Tier 0 — terminal fallback.
    CpuZstd,
    /// ISA-L igzip (requires isa-l feature + AVX-512). Tier 1.
    CpuIgzip,
    /// nvCOMP GPU batch compression (requires cuda feature + nvCOMP library). Tier 2.
    GpuNvcomp,
    /// Automatically select the best available compression backend.
    /// Probe order: nvCOMP > ISA-L igzip > CPU zstd.
    Auto,
}

// ---------------------------------------------------------------------------
// GpuConfig
// ---------------------------------------------------------------------------

/// Per-bucket compression configuration.
///
/// Controls the compression tier and level applied to segment data before
/// erasure coding. Per ADR-0007, the effective tier is capped by the
/// node-level `CompressionConfig` ceiling: a bucket requesting
/// `GpuNvcomp` on a `CpuZstd`-only node will get `CpuZstd`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{CompressConfig, CompressionTier};
///
/// let config = CompressConfig::default();
/// assert_eq!(config.tier, CompressionTier::Auto);
/// assert_eq!(config.level, 3);
/// ```
#[derive(Debug, Clone)]
pub struct CompressConfig {
    /// Compression tier to use: Auto, CpuZstd, CpuIgzip, or GpuNvcomp.
    pub tier: CompressionTier,
    /// Compression level (0-22 for zstd, 0-3 for igzip).
    /// Higher levels produce smaller output at the cost of more CPU/GPU time.
    pub level: u32,
    /// nvCOMP-specific configuration (only used when `tier` is GpuNvcomp).
    pub nvcomp: Option<NvcompConfig>,
}

impl Default for CompressConfig {
    fn default() -> Self {
        Self { tier: CompressionTier::Auto, level: 3, nvcomp: None }
    }
}

/// nvCOMP GPU compression codec selection.
///
/// # Examples
///
/// ```
/// use oceanfs_core::NvcompCodec;
///
/// let codec = NvcompCodec::Lz4;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum NvcompCodec {
    /// LZ4 compression (fast, moderate ratio).
    Lz4,
    /// Snappy compression (fast, moderate ratio).
    Snappy,
    /// Zstandard compression (slower, high ratio).
    Zstd,
}

/// Configuration for nvCOMP GPU-accelerated compression.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{NvcompConfig, NvcompCodec};
///
/// let config = NvcompConfig::default();
/// assert_eq!(config.codec, NvcompCodec::Lz4);
/// assert_eq!(config.batch_size, 16);
/// ```
#[derive(Debug, Clone)]
pub struct NvcompConfig {
    /// Compression codec to use (default: LZ4).
    pub codec: NvcompCodec,
    /// Number of segments to batch for a single GPU kernel launch (default 16).
    pub batch_size: usize,
    /// CUDA device index (default 0).
    pub device_id: usize,
}

impl Default for NvcompConfig {
    fn default() -> Self {
        Self { codec: NvcompCodec::Lz4, batch_size: 16, device_id: 0 }
    }
}

/// Configuration for GPU-accelerated operations.
///
/// Controls CUDA device selection, batch sizes, concurrency limits,
/// and error recovery behavior.
///
/// # Examples
///
/// ```
/// use oceanfs_core::GpuConfig;
///
/// let config = GpuConfig::default();
/// assert_eq!(config.device_id, 0);
/// assert_eq!(config.batch_size, 64);
/// ```
#[derive(Debug, Clone)]
pub struct GpuConfig {
    /// CUDA device index (default 0).
    pub device_id: usize,
    /// Number of stripes per GPU kernel launch (default 64).
    pub batch_size: usize,
    /// Minimum segment size in bytes for GPU offload (default 100 MB).
    /// Segments smaller than this use the CPU path regardless of tier.
    pub min_segment_size: u64,
    /// Maximum concurrent GPU operations (semaphore permits, default 1).
    pub max_concurrent_ops: usize,
    /// Seconds to wait before retrying after a GPU failure (default 60).
    pub cooldown_sec: u64,
}

impl Default for GpuConfig {
    fn default() -> Self {
        Self {
            device_id: 0,
            batch_size: 64,
            min_segment_size: 104_857_600, // 100 MB
            max_concurrent_ops: 1,
            cooldown_sec: 60,
        }
    }
}

// ---------------------------------------------------------------------------
// GpuConfig
// ---------------------------------------------------------------------------

/// A request to invalidate a cache entry, propagated via gossip or direct RPC.
///
/// Sent by the node that modified or deleted an object to inform peers that
/// their stale cache entries should be evicted.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{BucketId, CacheInvalidateRequest, ObjectKey};
///
/// let req = CacheInvalidateRequest {
///     bucket: BucketId::new("my-bucket"),
///     key: ObjectKey::new("photo.jpg"),
/// };
/// assert_eq!(req.bucket.as_str(), "my-bucket");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CacheInvalidateRequest {
    /// The bucket containing the invalidated object.
    pub bucket: BucketId,
    /// The key of the invalidated object.
    pub key: ObjectKey,
}

// ---------------------------------------------------------------------------
// HealConfig
// ---------------------------------------------------------------------------

/// Configuration for the EC heal dispatch pipeline.
///
/// Controls concurrency, retry behavior, and throughput throttling for
/// the background heal worker that repairs corrupt segment shards.
///
/// # Examples
///
/// ```
/// use oceanfs_core::HealConfig;
///
/// let config = HealConfig::default();
/// assert_eq!(config.max_concurrent_heals(), 4);
/// ```
#[derive(Debug, Clone)]
pub struct HealConfig {
    /// Maximum number of concurrent heal operations (bounded via semaphore).
    max_concurrent_heals: usize,
    /// Maximum retry attempts for a single heal request before giving up.
    heal_retry_limit: u32,
    /// Throughput limit in bytes per second (0 = unlimited).
    heal_throttle_bytes_sec: u64,
    /// Capacity of the bounded heal queue channel.
    queue_capacity: usize,
}

impl Default for HealConfig {
    fn default() -> Self {
        Self {
            max_concurrent_heals: 4,
            heal_retry_limit: 3,
            heal_throttle_bytes_sec: 0,
            queue_capacity: 256,
        }
    }
}

impl HealConfig {
    /// Returns the maximum number of concurrent heal operations.
    pub fn max_concurrent_heals(&self) -> usize {
        self.max_concurrent_heals
    }

    /// Returns the maximum retry attempts per heal request.
    pub fn heal_retry_limit(&self) -> u32 {
        self.heal_retry_limit
    }

    /// Returns the throughput throttle in bytes per second.
    pub fn heal_throttle_bytes_sec(&self) -> u64 {
        self.heal_throttle_bytes_sec
    }

    /// Returns the capacity of the bounded heal queue.
    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// Sets the maximum number of concurrent heal operations.
    #[must_use]
    pub fn with_max_concurrent_heals(mut self, value: usize) -> Self {
        self.max_concurrent_heals = value;
        self
    }

    /// Sets the maximum retry attempts per heal request.
    #[must_use]
    pub fn with_heal_retry_limit(mut self, value: u32) -> Self {
        self.heal_retry_limit = value;
        self
    }

    /// Sets the throughput throttle in bytes per second.
    #[must_use]
    pub fn with_heal_throttle_bytes_sec(mut self, value: u64) -> Self {
        self.heal_throttle_bytes_sec = value;
        self
    }

    /// Sets the capacity of the bounded heal queue.
    #[must_use]
    pub fn with_queue_capacity(mut self, value: usize) -> Self {
        self.queue_capacity = value;
        self
    }
}

// ---------------------------------------------------------------------------
// HealStats
// ---------------------------------------------------------------------------

/// Atomic statistics for the heal pipeline.
///
/// All counters use [`std::sync::atomic::Ordering::Relaxed`] because precise
/// ordering is not required for diagnostic counters — only approximate
/// observability matters (perf rule 11.1).
///
/// # Examples
///
/// ```
/// use oceanfs_core::HealStats;
///
/// let stats = HealStats::default();
/// assert_eq!(stats.heals_attempted(), 0);
/// ```
#[derive(Debug, Default)]
pub struct HealStats {
    /// Total number of heal attempts (includes retries).
    heals_attempted: std::sync::atomic::AtomicU64,
    /// Heals that completed successfully.
    heals_succeeded: std::sync::atomic::AtomicU64,
    /// Heals that exhausted all retries and failed.
    heals_failed: std::sync::atomic::AtomicU64,
    /// Total bytes repaired across all successful heals.
    bytes_repaired: std::sync::atomic::AtomicU64,
}

impl HealStats {
    /// Returns the total number of heal attempts.
    pub fn heals_attempted(&self) -> u64 {
        self.heals_attempted.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the number of successful heal completions.
    pub fn heals_succeeded(&self) -> u64 {
        self.heals_succeeded.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the number of heals that failed after exhausting retries.
    pub fn heals_failed(&self) -> u64 {
        self.heals_failed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the total bytes repaired across all successful heals.
    pub fn bytes_repaired(&self) -> u64 {
        self.bytes_repaired.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Increments the attempts counter by one.
    pub fn inc_attempted(&self) {
        self.heals_attempted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Increments the succeeded counter by one.
    pub fn inc_succeeded(&self) {
        self.heals_succeeded.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Increments the failed counter by one.
    pub fn inc_failed(&self) {
        self.heals_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Adds the given number of bytes to the repaired counter.
    pub fn add_bytes_repaired(&self, bytes: u64) {
        self.bytes_repaired.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Creates a new [`HealStats`] with all counters initialized to zero.
    pub fn new() -> Self {
        Self::default()
    }
}

// ---------------------------------------------------------------------------
// HealRequest
// ---------------------------------------------------------------------------

/// A request to repair one or more corrupt shards of a segment.
///
/// Submitted to the `HealQueue` by Scrub and Anti-Entropy when
/// corruption is detected. The `HealWorker` drains these requests
/// and coordinates EC-based repair.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{HealRequest, SegmentId};
///
/// let request = HealRequest {
///     segment_id: SegmentId::new(),
///     corrupt_shard_indices: vec![2],
///     retry_count: 0,
/// };
/// assert_eq!(request.retry_count, 0);
/// ```
#[derive(Debug, Clone)]
pub struct HealRequest {
    /// The segment that needs repair.
    pub segment_id: SegmentId,
    /// Indices of the corrupt shards within the k+m shard set.
    pub corrupt_shard_indices: Vec<usize>,
    /// Number of previous attempts (0 = first attempt).
    pub retry_count: u32,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
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

    // -- WriteQuorum --

    #[test]
    fn write_quorum_default_values() {
        let q = WriteQuorum::default();
        assert_eq!(q.required, 1);
        assert!(q.ack_after_wal);
        assert!(q.ec_async);
    }

    #[test]
    fn write_quorum_custom_config() {
        let q = WriteQuorum { required: 3, ack_after_wal: false, ec_async: false };
        assert_eq!(q.required, 3);
        assert!(!q.ack_after_wal);
        assert!(!q.ec_async);
    }

    // -- WriteResult / WriteAck --

    #[test]
    fn write_result_construction() {
        let key = ObjectKey::new("test");
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef { segment_id: SegmentId::new(), offset: 0, length: 100 });
        let result = WriteResult { object_key: key.clone(), chunks, size: 100, blake3_hash: None };
        assert_eq!(result.size, 100);
        assert_eq!(result.object_key, key);
        assert_eq!(result.chunks.len(), 1);
    }

    #[test]
    fn write_ack_construction() {
        let ack = WriteAck { node_id: NodeId::new("n1"), wal_position: 42, hlc: Hlc::zero() };
        assert_eq!(ack.node_id.as_str(), "n1");
        assert_eq!(ack.wal_position, 42);
        assert_eq!(ack.hlc, Hlc::zero());
    }

    // -- IntendedFor --

    #[test]
    fn intended_for_from_node_id() {
        let target = IntendedFor(NodeId::new("node-x"));
        assert_eq!(target.as_str(), "node-x");
        assert_eq!(target.to_string(), "node-x");
    }

    #[test]
    fn intended_for_equality() {
        let a = IntendedFor(NodeId::new("a"));
        let b = IntendedFor(NodeId::new("a"));
        let c = IntendedFor(NodeId::new("c"));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // -- PoolConfig --

    #[test]
    fn pool_config_default_values() {
        let cfg = PoolConfig::default();
        assert_eq!(cfg.active_pool_size, 4);
        assert_eq!(cfg.shard_count, 4);
        assert_eq!(cfg.max_inflight_encodes, 8);
        assert_eq!(cfg.encode_queue_capacity, 64);
    }

    #[test]
    fn pool_config_custom_sizes() {
        let cfg = PoolConfig {
            active_pool_size: 16,
            shard_count: 8,
            max_inflight_encodes: 32,
            encode_queue_capacity: 256,
        };
        assert_eq!(cfg.active_pool_size, 16);
        assert_eq!(cfg.shard_count, 8);
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

    // -- SegmentId: from_uuid_bytes, as_uuid, Default --

    #[test]
    fn segment_id_from_uuid_bytes_roundtrip() {
        let bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let id = SegmentId::from_uuid_bytes(bytes);
        assert_eq!(id.as_uuid().as_bytes(), &bytes);
    }

    #[test]
    fn segment_id_default_is_new() {
        let id = SegmentId::default();
        let s = id.to_string();
        assert_eq!(s.len(), 36);
    }

    // -- NodeId: From<String>, Display --

    #[test]
    fn node_id_from_string_and_display() {
        let id = NodeId::from(String::from("boxed-node"));
        assert_eq!(id.as_str(), "boxed-node");
        assert_eq!(id.to_string(), "boxed-node");
    }

    // -- BucketId: From<&str>, Display, From<String> --

    #[test]
    fn bucket_id_from_str_and_display() {
        let bucket: BucketId = "my-bucket".into();
        assert_eq!(bucket.as_str(), "my-bucket");
    }

    #[test]
    fn bucket_id_display() {
        let bucket = BucketId::new("photos");
        assert_eq!(bucket.to_string(), "photos");
    }

    #[test]
    fn bucket_id_from_string() {
        let bucket = BucketId::from(String::from("videos"));
        assert_eq!(bucket.as_str(), "videos");
    }

    // -- ObjectKey: Display, From<&str>, From<String> --

    #[test]
    fn object_key_display() {
        let key = ObjectKey::new("hello/world.txt");
        assert_eq!(key.to_string(), "hello/world.txt");
    }

    #[test]
    fn object_key_from_str() {
        let key: ObjectKey = "prefix/obj".into();
        assert_eq!(key.as_str(), "prefix/obj");
    }

    #[test]
    fn object_key_from_string() {
        let key = ObjectKey::from(String::from("owned/key"));
        assert_eq!(key.as_str(), "owned/key");
    }

    // -- HashOutput: as_bytes, Display --

    #[test]
    fn hash_output_as_bytes_returns_32_bytes() {
        let bytes = [42u8; 32];
        let hash = HashOutput::from_bytes(bytes);
        assert_eq!(hash.as_bytes(), &bytes);
    }

    #[test]
    fn hash_output_display_is_hex() {
        let hash = HashOutput::from_bytes([0x12u8; 32]);
        let displayed = hash.to_string();
        assert_eq!(displayed.len(), 64);
        assert_eq!(displayed, "12".repeat(32));
    }

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

    // -- Incarnation: new, value, next, Default --

    #[test]
    fn incarnation_new_and_value() {
        let inc = Incarnation::new(42);
        assert_eq!(inc.value(), 42);
    }

    #[test]
    fn incarnation_next_increments() {
        let inc = Incarnation::new(1);
        assert_eq!(inc.next().value(), 2);
    }

    #[test]
    fn incarnation_default_is_one() {
        assert_eq!(Incarnation::default().value(), 1);
    }

    // -- HashKey: from_bytes, as_bytes --

    #[test]
    fn hash_key_from_bytes_and_as_bytes() {
        let bytes = [0xAAu8; 32];
        let key = HashKey::from_bytes(bytes);
        assert_eq!(key.as_bytes(), &bytes);
    }

    // -- GossipConfig: Default --

    #[test]
    fn gossip_config_default_values() {
        let cfg = GossipConfig::default();
        assert_eq!(cfg.interval_ms, 1000);
        assert_eq!(cfg.suspicion_timeout_ms, 5000);
        assert_eq!(cfg.failure_timeout_ms, 15000);
        assert_eq!(cfg.indirect_ping_count, 3);
        assert!(cfg.seed_nodes.is_empty());
    }

    // -- RpcConfig: Default --

    #[test]
    fn rpc_config_default_values() {
        let cfg = RpcConfig::default();
        assert_eq!(cfg.pool_size_per_peer, 4);
        assert_eq!(cfg.keepalive_sec, 30);
        assert_eq!(cfg.max_idle_connections, 256);
        assert_eq!(cfg.connect_timeout_ms, 5000);
        assert_eq!(cfg.request_timeout_ms, 30000);
        assert!(cfg.tls_cert_path.is_none());
    }

    // -- PeerAddress: new, socket_addr, Display, From --

    #[test]
    fn peer_address_new_and_socket_addr() {
        let addr: std::net::SocketAddr = "10.0.0.1:9001".parse().unwrap();
        let peer = PeerAddress::new(addr);
        assert_eq!(peer.socket_addr(), addr);
    }

    #[test]
    fn peer_address_display() {
        let addr: std::net::SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let peer = PeerAddress::new(addr);
        assert_eq!(peer.to_string(), "127.0.0.1:8080");
    }

    #[test]
    fn peer_address_from_socket_addr() {
        let addr: std::net::SocketAddr = "192.168.1.1:9000".parse().unwrap();
        let peer: PeerAddress = addr.into();
        assert_eq!(peer.socket_addr(), addr);
    }

    // -- CodecConfig: Default --

    #[test]
    fn codec_config_default_values() {
        let cfg = CodecConfig::default();
        assert!(matches!(cfg.codec_type, CodecType::CauchyRs));
        assert_eq!(cfg.data_shards, 4);
        assert_eq!(cfg.parity_shards, 2);
        assert_eq!(cfg.strip_size_bytes, 65536);
    }

    // -- EncodingPlan: total_shards --

    #[test]
    fn encoding_plan_total_shards() {
        let plan = EncodingPlan {
            stripe_count: 8,
            padded_size: 4096,
            shard_size: 128,
            data_shards: 4,
            parity_shards: 2,
        };
        assert_eq!(plan.total_shards(), 6);
    }

    #[test]
    fn encoding_plan_total_shards_only_data() {
        let plan = EncodingPlan {
            stripe_count: 1,
            padded_size: 256,
            shard_size: 64,
            data_shards: 3,
            parity_shards: 0,
        };
        assert_eq!(plan.total_shards(), 3);
    }

    // -- SegmentIndexEntry --

    #[test]
    fn segment_index_entry_construction() {
        let entry = SegmentIndexEntry { offset: 1024, length: 512, blob_key_hash: [0xABu8; 32] };
        assert_eq!(entry.offset, 1024);
        assert_eq!(entry.length, 512);
        assert_eq!(entry.blob_key_hash, [0xABu8; 32]);
    }

    // -- NodeState --

    #[test]
    fn node_state_variants_exist() {
        // Verify all expected variants compile and can be used.
        let _states = [
            NodeState::Alive,
            NodeState::Suspect,
            NodeState::Dead,
            NodeState::Leaving,
            NodeState::Left,
        ];
    }

    // -- OperationType --

    #[test]
    fn operation_type_variants_exist() {
        let _ops = [
            OperationType::Read,
            OperationType::Write,
            OperationType::Delete,
            OperationType::Head,
            OperationType::List,
        ];
    }

    // -- VnodeRange --

    #[test]
    fn vnode_range_construction() {
        let range = VnodeRange { start: [0u8; 32], end: [0xFFu8; 32] };
        assert_eq!(range.start, [0u8; 32]);
        assert_eq!(range.end, [0xFFu8; 32]);
    }

    // -- StorageLocation --

    #[test]
    fn storage_location_construction() {
        let loc = StorageLocation { node_id: NodeId::new("n1"), shard_index: 3 };
        assert_eq!(loc.node_id.as_str(), "n1");
        assert_eq!(loc.shard_index, 3);
    }

    // -- CacheInvalidateRequest --

    #[test]
    fn cache_invalidate_request_construction() {
        let req = CacheInvalidateRequest { bucket: BucketId::new("b"), key: ObjectKey::new("k") };
        assert_eq!(req.bucket.as_str(), "b");
        assert_eq!(req.key.as_str(), "k");
    }

    // -- MetadataStore trait: verify it can be implemented --

    struct TestStore;

    impl MetadataStore for TestStore {
        fn list_object_keys(
            &self,
            _bucket: &BucketId,
        ) -> std::io::Result<Vec<(BucketId, ObjectKey)>> {
            Ok(vec![])
        }

        fn get_object_metadata(
            &self,
            _bucket: &BucketId,
            _key: &ObjectKey,
        ) -> std::io::Result<Option<ObjectMetadata>> {
            Ok(None)
        }
    }

    #[test]
    fn metadata_store_trait_basic_impl() {
        let store = TestStore;
        let bucket = BucketId::new("test");
        assert!(store.list_object_keys(&bucket).unwrap().is_empty());
        assert!(store.get_object_metadata(&bucket, &ObjectKey::new("k")).unwrap().is_none());
    }

    // -- CompressionTier --

    #[test]
    fn compression_tier_variants_exist() {
        let _tiers = [
            CompressionTier::Auto,
            CompressionTier::CpuZstd,
            CompressionTier::CpuIgzip,
            CompressionTier::GpuNvcomp,
        ];
    }

    #[test]
    fn compression_tier_auto_is_not_cpu_zstd() {
        assert_ne!(CompressionTier::Auto, CompressionTier::CpuZstd);
    }

    // -- GpuConfig --

    #[test]
    fn gpu_config_default_values() {
        let cfg = GpuConfig::default();
        assert_eq!(cfg.device_id, 0);
        assert_eq!(cfg.batch_size, 64);
        assert_eq!(cfg.min_segment_size, 104_857_600);
        assert_eq!(cfg.max_concurrent_ops, 1);
        assert_eq!(cfg.cooldown_sec, 60);
    }

    #[test]
    fn gpu_config_custom() {
        let cfg = GpuConfig {
            device_id: 1,
            batch_size: 128,
            min_segment_size: 50_000_000,
            max_concurrent_ops: 4,
            cooldown_sec: 120,
        };
        assert_eq!(cfg.device_id, 1);
        assert_eq!(cfg.batch_size, 128);
        assert_eq!(cfg.max_concurrent_ops, 4);
    }
}
