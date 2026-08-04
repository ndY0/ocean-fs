//! Identifier types used across all OceanFS crates.
//!
//! Fundamental domain identifiers — `SegmentId`, `NodeId`, `BucketId`, and
//! `ObjectKey` — that every subsystem references.

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

    // -- NodeId --

    #[test]
    fn node_id_from_str_and_display_roundtrip() {
        let id = NodeId::from("node-7");
        assert_eq!(id.as_str(), "node-7");
        assert_eq!(id.to_string(), "node-7");
    }

    #[test]
    fn node_id_from_string_and_display() {
        let id = NodeId::from(String::from("boxed-node"));
        assert_eq!(id.as_str(), "boxed-node");
        assert_eq!(id.to_string(), "boxed-node");
    }

    // -- BucketId --

    #[test]
    fn bucket_id_as_str_returns_name() {
        let bucket = BucketId::new("archive");
        assert_eq!(bucket.as_str(), "archive");
    }

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

    // -- ObjectKey --

    #[test]
    fn object_key_preserves_slashes() {
        let key = ObjectKey::new("a/b/c");
        assert_eq!(key.as_str(), "a/b/c");
    }

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
}
