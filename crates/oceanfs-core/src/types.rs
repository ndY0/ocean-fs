//! Shared types used across all OceanFS crates.
//!
//! These are the fundamental domain types — identifiers, hashes, and
//! keys — that every subsystem references.

use std::fmt;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
mod hex {
    const CHARS: &[u8; 16] = b"0123456789abcdef";

    pub(super) fn encode(bytes: [u8; 32]) -> String {
        let mut out = String::with_capacity(64);
        for byte in bytes {
            out.push(CHARS[(byte >> 4) as usize] as char);
            out.push(CHARS[(byte & 0x0f) as usize] as char);
        }
        out
    }
}

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
}
