//! Metadata operations trait consumed by the S3 HTTP handler.
//!
//! Defines the interface that `S3Handler` uses for object lookup,
//! deletion, and listing. Concrete implementations live in
//! `oceanfs-storage` and are wired in `oceanfs-node`.

use oceanfs_core::{BucketId, Hlc, ObjectKey, ObjectMetadata, SegmentId, SegmentMetadata};

/// Result type for metadata operations.
pub type Result<T, E = MetadataError> = std::result::Result<T, E>;

/// Errors returned by metadata operations.
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    /// The object was not found.
    #[error("object not found: {0}")]
    NotFound(String),

    /// A storage I/O error occurred.
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Internal metadata store error.
    #[error("metadata store error: {0}")]
    Internal(String),
}

/// Metadata operations required by the S3 handler.
///
/// This trait abstracts the metadata store so the S3 handler does
/// not depend on `oceanfs-storage` directly. The concrete RocksDB
/// implementation is wired at startup in `oceanfs-node`.
///
/// All implementations must be `Send + Sync + 'static`.
pub trait MetadataOps: Send + Sync + 'static {
    /// Retrieves object metadata by bucket and key.
    ///
    /// Returns `None` if the object does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store is unavailable or
    /// the underlying storage operation fails.
    fn get_object(&self, bucket: &BucketId, key: &ObjectKey) -> Result<Option<ObjectMetadata>>;

    /// Soft-deletes an object by writing a tombstone entry.
    ///
    /// `hlc` is the delete's timestamp, minted by the caller's clock:
    /// the tombstone must carry the version of the delete itself so
    /// delete-vs-write LWW is decidable across replicas
    /// (hlc-causality-closure G4).
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store is unavailable or
    /// the tombstone write fails.
    fn delete_object(&self, bucket: &BucketId, key: &ObjectKey, hlc: Hlc) -> Result<()>;

    /// Stores object metadata.
    ///
    /// This must be called after a successful write to persist the
    /// object → segment mapping so subsequent reads can locate the data.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store is unavailable or
    /// the underlying storage operation fails.
    fn put_object(&self, bucket: &BucketId, meta: ObjectMetadata) -> Result<()>;

    /// Stores segment metadata.
    ///
    /// Called after a new segment is created to register it in the
    /// metadata store so it appears in segment inventory reports.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store is unavailable.
    fn put_segment(&self, meta: SegmentMetadata) -> Result<()>;

    /// Retrieves segment metadata for a given segment ID.
    ///
    /// Returns `Ok(None)` if the segment does not exist. Used by the
    /// write path to avoid clobbering a sealed segment's metadata with
    /// a pre-seal registration entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store is unavailable.
    fn get_segment(&self, id: SegmentId) -> Result<Option<SegmentMetadata>>;

    /// Lists objects in a bucket matching the given prefix.
    ///
    /// Results are sorted by key. Returns objects whose key starts
    /// with `prefix`.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store is unavailable or
    /// the iteration over keys fails.
    fn list_objects(&self, bucket: &BucketId, prefix: &str) -> Result<Vec<ObjectMetadata>>;
}
