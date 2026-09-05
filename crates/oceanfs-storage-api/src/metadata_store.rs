//! Metadata store trait — CRUD operations for object metadata.
//!
//! Each crate that provides metadata storage implements this trait so
//! that caches can rebuild filters and warm entries without depending
//! on the concrete storage implementation. Durability components (GC,
//! scrub, heal, anti-entropy) consume this trait to avoid coupling to
//! RocksDB.
//!
//! Segment lifecycle state is NOT part of this trait (ADR-0025
//! Decision 3): the `segments` CF is removed, and consumers read the
//! machine (`SegmentLifecycleRegistry`) through the consuming crate's
//! own boundary.

use oceanfs_core::{BucketId, DeadChunkRecord, Hlc, ObjectKey, ObjectMetadata, Tombstone};

/// A single batch operation for atomic metadata writes.
///
/// Batched operations are written atomically to the underlying store
/// where the backend supports it (e.g., RocksDB WriteBatch). Where
/// the backend does not support atomic batches, implementations
/// fall back to sequential writes.
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Put an object metadata entry.
    ///
    /// The bucket is carried explicitly: object keys are only unique
    /// within a bucket, and the store encodes the bucket into the key.
    /// (Previously the bucket was implicit "default", which silently
    /// moved every non-default-bucket object to the default bucket on
    /// rewrite — e.g. during GC compaction repacking.)
    PutObject(BucketId, ObjectKey, ObjectMetadata),
    /// Delete an object.
    DeleteObject(BucketId, ObjectKey),
    /// Put a tombstone.
    PutTombstone(BucketId, ObjectKey, Tombstone),
    /// Delete a tombstone entry for the given object key.
    DeleteTombstone(BucketId, ObjectKey),
}

/// Trait for metadata access needed by caching, prefetch, and durability layers.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata, Tombstone};
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
///     fn list_tombstones(&self, _bucket: &BucketId) -> Vec<io::Result<(ObjectKey, Tombstone)>> {
///         vec![]
///     }
///     fn delete_tombstone(&self, _bucket: &BucketId, _key: &ObjectKey) -> io::Result<()> {
///         Ok(())
///     }
///     fn put_object(&self, _bucket: &BucketId, _meta: ObjectMetadata) -> io::Result<()> {
///         Ok(())
///     }
///     fn delete_object(&self, _bucket: &BucketId, _key: &ObjectKey, _hlc: oceanfs_core::Hlc) -> io::Result<()> {
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

    /// Lists object metadata for **every** object across all buckets.
    ///
    /// Each element is a Result — individual objects may fail to deserialize
    /// without failing the entire scan. Used by the orphan reaper to build
    /// the set of referenced segments: restricting the scan to a single
    /// bucket (as `list_objects` does) would classify every segment owned
    /// by other buckets as an orphan and delete live data.
    ///
    /// The default implementation returns an empty list (no objects
    /// referenced); stores that support cross-bucket scans override it.
    fn list_objects_all(&self) -> Vec<std::io::Result<ObjectMetadata>> {
        Vec::new()
    }

    /// Lists object metadata for **every** object across all buckets,
    /// carrying each object's owning bucket.
    ///
    /// The bucket is not part of [`ObjectMetadata`] — it lives in the
    /// store's key. GC liveness tracking needs it to match tombstones
    /// against objects per-bucket (the same object key may exist in
    /// multiple buckets), so this method decodes it from the key.
    ///
    /// The default implementation returns an empty list; stores that
    /// support cross-bucket scans override it.
    fn list_objects_all_with_bucket(&self) -> Vec<std::io::Result<(BucketId, ObjectMetadata)>> {
        Vec::new()
    }

    /// Lists tombstone entries for a bucket.
    ///
    /// Each element is a Result. Used by GC to find expired deletion markers.
    fn list_tombstones(&self, bucket: &BucketId) -> Vec<std::io::Result<(ObjectKey, Tombstone)>>;

    /// Lists tombstone entries for **every** bucket.
    ///
    /// Each element is a Result carrying the tombstone's owning bucket.
    /// Used by GC liveness tracking: restricting the scan to a single
    /// bucket (as `list_tombstones` does) would hide every deletion in
    /// other buckets and stall compaction for those buckets' segments
    /// (observed with the load-test bucket — GC only scanned "default",
    /// so no dead bytes were ever detected for load-test data).
    ///
    /// The default implementation returns an empty list; stores that
    /// support cross-bucket scans override it.
    fn list_tombstones_all(&self) -> Vec<std::io::Result<(BucketId, ObjectKey, Tombstone)>> {
        Vec::new()
    }

    /// Lists every captured dead-chunk record across all buckets.
    ///
    /// Returns plain tombstones (`kind: Tombstone`) AND versioned
    /// supersedes (`kind: Supersede`, ADR-0034 D2) as the typed accounting
    /// feed GC liveness and orphan detection consume (f2): live bytes are
    /// `logical_total − dead`, where `dead` sums the `chunks` of aged
    /// records over each referenced segment.
    ///
    /// The record's `kind` is derived from the store's deletions-CF key
    /// classification, so a supersede record is never surfaced as a plain
    /// tombstone of its (live) key. This is the only enumeration that
    /// exposes supersedes; [`Self::list_tombstones`] and
    /// [`Self::list_tombstones_all`] keep returning plain tombstones only.
    ///
    /// The default implementation returns an empty list so in-memory test
    /// doubles stay minimal; the RocksDB store overrides it.
    fn list_dead_chunk_records_all(
        &self,
    ) -> Vec<std::io::Result<(BucketId, ObjectKey, DeadChunkRecord)>> {
        Vec::new()
    }

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

    /// Retrieves the deletion tombstone for the given key, if one exists.
    ///
    /// Used by the gRPC segment service for order-aware delete-vs-write
    /// resolution at the repair-push boundary (hlc-causality-closure G6).
    ///
    /// Implementors with real tombstone storage MUST override this; the
    /// default exists so that in-memory test doubles can stay minimal.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the lookup fails.
    fn get_tombstone(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
    ) -> std::io::Result<Option<Tombstone>> {
        Ok(self
            .list_tombstones(bucket)
            .into_iter()
            .filter_map(|r| r.ok())
            .find(|(k, _)| k == key)
            .map(|(_, t)| t))
    }

    /// Stores (or updates) object metadata.
    ///
    /// Used by segment compactor to update object chunk references after repacking.
    fn put_object(&self, bucket: &BucketId, meta: ObjectMetadata) -> std::io::Result<()>;

    /// Deletes object metadata for a given key and stamps the deletion
    /// tombstone with the given HLC.
    ///
    /// The `hlc` is the delete's timestamp, minted by the originating
    /// node's clock (hlc-causality-closure G4): the tombstone must carry
    /// the version of the delete itself so delete-vs-write LWW is
    /// decidable across replicas.
    ///
    /// Used by the gRPC segment service to handle object deletion
    /// requests and by the S3 delete handler for the local tombstone.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the delete or the tombstone write fails.
    fn delete_object(&self, bucket: &BucketId, key: &ObjectKey, hlc: Hlc) -> std::io::Result<()>;

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
