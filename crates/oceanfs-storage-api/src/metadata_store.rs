//! Metadata store trait — CRUD operations for object and segment metadata.
//!
//! Each crate that provides metadata storage implements this trait so
//! that caches can rebuild filters and warm entries without depending
//! on the concrete storage implementation. Durability components (GC,
//! scrub, heal, anti-entropy) consume this trait to avoid coupling to
//! RocksDB.

use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata, SegmentId, SegmentMetadata, Tombstone};

/// A single batch operation for atomic metadata writes.
///
/// Batched operations are written atomically to the underlying store
/// where the backend supports it (e.g., RocksDB WriteBatch). Where
/// the backend does not support atomic batches, implementations
/// fall back to sequential writes.
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Put an object metadata entry.
    PutObject(ObjectKey, ObjectMetadata),
    /// Delete an object.
    DeleteObject(BucketId, ObjectKey),
    /// Put a tombstone.
    PutTombstone(BucketId, ObjectKey, Tombstone),
    /// Put a segment metadata entry.
    PutSegment(SegmentMetadata),
    /// Delete a segment metadata entry.
    DeleteSegment(SegmentId),
    /// Delete a tombstone entry for the given object key.
    DeleteTombstone(BucketId, ObjectKey),
}

/// Trait for metadata access needed by caching, prefetch, and durability layers.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{BucketId, ObjectKey, SegmentId, SegmentMetadata, ObjectMetadata, Tombstone};
/// use oceanfs_storage_api::{MetadataStore, BatchOp};
/// use std::io;
///
/// struct MyStore;
///
/// impl MetadataStore for MyStore {
///     fn list_object_keys(&self, _bucket: &BucketId) -> io::Result<Vec<(BucketId, ObjectKey)>> {
///         Ok(vec![])
///     }
///     fn get_object_metadata(&self, _bucket: &BucketId, _key: &ObjectKey) -> io::Result<Option<ObjectMetadata>> {
///         Ok(None)
///     }
///     fn list_objects(&self, _bucket: &BucketId, _prefix: &str) -> Vec<io::Result<ObjectMetadata>> {
///         vec![]
///     }
///     fn get_segment(&self, _id: SegmentId) -> io::Result<Option<SegmentMetadata>> {
///         Ok(None)
///     }
///     fn list_segments(&self) -> Vec<io::Result<SegmentMetadata>> {
///         vec![]
///     }
///     fn list_tombstones(&self, _bucket: &BucketId) -> Vec<io::Result<(ObjectKey, Tombstone)>> {
///         vec![]
///     }
///     fn delete_tombstone(&self, _bucket: &BucketId, _key: &ObjectKey) -> io::Result<()> {
///         Ok(())
///     }
///     fn put_segment(&self, _meta: SegmentMetadata) -> io::Result<()> {
///         Ok(())
///     }
///     fn delete_segment(&self, _id: SegmentId) -> io::Result<()> {
///         Ok(())
///     }
///     fn put_object(&self, _bucket: &BucketId, _meta: ObjectMetadata) -> io::Result<()> {
///         Ok(())
///     }
///     fn batch_write(&self, _ops: Vec<BatchOp>) -> std::io::Result<()> {
///         Ok(())
///     }
/// }
/// ```
pub trait MetadataStore: Send + Sync {
    /// Lists all object keys in a bucket.
    ///
    /// Used to rebuild negative caches and for prefetch discovery.
    fn list_object_keys(&self, bucket: &BucketId) -> std::io::Result<Vec<(BucketId, ObjectKey)>>;

    /// Retrieves object metadata for a given key.
    ///
    /// Returns `Ok(None)` if the key does not exist.
    fn get_object_metadata(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
    ) -> std::io::Result<Option<ObjectMetadata>>;

    /// Lists object metadata for all objects in a bucket matching the prefix.
    ///
    /// Each element is a Result — individual objects may fail to deserialize
    /// without failing the entire scan. Used by GC liveness tracking and orphan reaper.
    fn list_objects(&self, bucket: &BucketId, prefix: &str)
        -> Vec<std::io::Result<ObjectMetadata>>;

    /// Retrieves segment metadata for a given segment ID.
    ///
    /// Returns `Ok(None)` if the segment does not exist.
    fn get_segment(&self, id: SegmentId) -> std::io::Result<Option<SegmentMetadata>>;

    /// Lists all sealed segment metadata.
    ///
    /// Each element is a Result. Used by GC, scrub, anti-entropy, and orphan reaper.
    fn list_segments(&self) -> Vec<std::io::Result<SegmentMetadata>>;

    /// Lists tombstone entries for a bucket.
    ///
    /// Each element is a Result. Used by GC to find expired deletion markers.
    fn list_tombstones(&self, bucket: &BucketId) -> Vec<std::io::Result<(ObjectKey, Tombstone)>>;

    /// Deletes a tombstone entry for the given object key.
    ///
    /// Called by the garbage collector after successfully compacting a segment
    /// and reclaiming the dead chunks for objects whose tombstones have been
    /// processed.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the deletion fails.
    fn delete_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> std::io::Result<()>;

    /// Checks whether a deletion tombstone exists for the given key.
    ///
    /// Used by the gRPC segment service to reject read-repair pushes that
    /// would resurrect a deleted object: a tombstoned key is authoritative
    /// and may only be overwritten by a genuine new write (which clears the
    /// tombstone via `put_object`).
    ///
    /// Implementors with real tombstone storage MUST override this; the
    /// default exists so that in-memory test doubles can stay minimal.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the lookup fails.
    fn has_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> std::io::Result<bool> {
        // In-memory doubles without tombstone tracking default to "no
        // tombstone" — a test store that needs the gate must override.
        Ok(self.list_tombstones(bucket).into_iter().filter_map(|r| r.ok()).any(|(k, _)| &k == key))
    }

    /// Stores (or updates) segment metadata.
    ///
    /// Used by heal worker to update metadata after repairing a segment.
    fn put_segment(&self, meta: SegmentMetadata) -> std::io::Result<()>;

    /// Deletes segment metadata for a given segment ID.
    ///
    /// Used by orphan reaper and segment compactor.
    fn delete_segment(&self, id: SegmentId) -> std::io::Result<()>;

    /// Stores (or updates) object metadata.
    ///
    /// Used by segment compactor to update object chunk references after repacking.
    fn put_object(&self, bucket: &BucketId, meta: ObjectMetadata) -> std::io::Result<()>;

    /// Deletes object metadata for a given key.
    ///
    /// Used by the gRPC segment service to handle object deletion requests.
    fn delete_object(&self, bucket: &BucketId, key: &ObjectKey) -> std::io::Result<()>;

    /// Atomically writes a batch of metadata operations.
    ///
    /// Where the backend supports atomic batches (e.g., RocksDB WriteBatch),
    /// all operations are committed together. Backends without atomic batch
    /// support fall back to sequential writes.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if any operation in the batch fails.
    fn batch_write(&self, ops: Vec<BatchOp>) -> std::io::Result<()>;
}
