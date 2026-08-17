//! Blob store trait — raw blob read/write operations.
//!
//! Abstracts disk-persisted blob data storage. Backend implementations
//! (RocksDB, FUSE, S3, in-memory) implement this trait.

use bytes::Bytes;
use oceanfs_core::SegmentId;

use crate::error::Error;

/// Raw blob store for reading and writing segment data.
///
/// # Examples
///
/// ```
/// use bytes::Bytes;
/// use oceanfs_core::SegmentId;
/// use oceanfs_storage_api::BlobStore;
/// use oceanfs_storage_api::error::Error;
///
/// struct MyBlobStore;
///
/// impl BlobStore for MyBlobStore {
///     fn write_blob(&self, _segment_id: &SegmentId, _data: &[u8]) -> Result<(), Error> {
///         Ok(())
///     }
///
///     fn read_blob(&self, _segment_id: &SegmentId) -> Result<Option<Bytes>, Error> {
///         Ok(None)
///     }
///
///     fn delete_blob(&self, _segment_id: &SegmentId) -> Result<(), Error> {
///         Ok(())
///     }
///
///     fn list_blobs(&self) -> Result<Vec<SegmentId>, Error> {
///         Ok(vec![])
///     }
/// }
/// ```
pub trait BlobStore: Send + Sync {
    /// Writes blob data for a segment to the store.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be written.
    fn write_blob(&self, segment_id: &SegmentId, data: &[u8]) -> Result<(), Error>;

    /// Reads blob data for a segment from the store.
    ///
    /// Returns `Ok(None)` if the blob does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the data exists but cannot be read.
    fn read_blob(&self, segment_id: &SegmentId) -> Result<Option<Bytes>, Error>;

    /// Deletes a blob for a segment.
    ///
    /// If the blob doesn't exist, this is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the blob exists but cannot be deleted.
    fn delete_blob(&self, segment_id: &SegmentId) -> Result<(), Error>;

    /// Lists all segment IDs with persisted blob data.
    ///
    /// Used on startup to discover which segments have persisted data.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be read.
    fn list_blobs(&self) -> Result<Vec<SegmentId>, Error>;
}
