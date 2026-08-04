//! Metadata store trait — CRUD operations for object metadata.
//!
//! Each crate that provides metadata storage implements this trait so
//! that caches can rebuild filters and warm entries without depending
//! on the concrete storage implementation.

use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata};

/// Minimal trait for metadata access needed by caching and prefetch layers.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata};
/// use oceanfs_storage_api::MetadataStore;
///
/// struct MyStore;
///
/// impl MetadataStore for MyStore {
///     fn list_object_keys(
///         &self,
///         _bucket: &BucketId,
///     ) -> std::io::Result<Vec<(BucketId, ObjectKey)>> {
///         Ok(vec![])
///     }
///
///     fn get_object_metadata(
///         &self,
///         _bucket: &BucketId,
///         _key: &ObjectKey,
///     ) -> std::io::Result<Option<ObjectMetadata>> {
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
