//! Segment data store trait — whole-file `.dat` data access.
//!
//! ADR-0032 D1: this is the **only** segment data-access abstraction.
//! The historical split — a read/write-only `SegmentDataStore`
//! (`oceanfs-durability::anti_entropy`) plus a delete/list-only
//! `SegmentShardStore` (`oceanfs-durability::gc`) — folded into this
//! single trait. Implementations live in `oceanfs-storage`
//! (production, `DiskSegmentStore`) or in test crates (in-memory
//! doubles).

use std::path::{Path, PathBuf};

use bytes::Bytes;
use oceanfs_core::SegmentId;

use crate::error::Result;

/// A segment's parsed on-disk header + data section.
///
/// `read_segment_data` returns this value instead of raw bytes so that
/// callers stop hand-rolling the v1 (76-byte) / v2 (92-byte) header
/// slicing logic (review #35): the implementation parses the header
/// once and exposes the payload boundaries.
///
/// # Examples
///
/// ```
/// use bytes::Bytes;
/// use oceanfs_core::SegmentId;
/// use oceanfs_storage_api::SegmentFile;
///
/// let file = SegmentFile {
///     segment_id: SegmentId::new(),
///     version: 1,
///     header_len: 76,
///     data_end: 76 + 3,
///     data: Bytes::from_static(b"abc"),
/// };
/// assert_eq!(file.version, 1);
/// assert_eq!(&file.data[..], b"abc");
/// ```
#[derive(Debug, Clone)]
pub struct SegmentFile {
    /// The segment this file belongs to.
    pub segment_id: SegmentId,
    /// On-disk format version (v1 = 76-byte header, v2 = 92-byte
    /// header).
    pub version: u16,
    /// Byte length of the parsed header.
    pub header_len: usize,
    /// End offset (exclusive) of the data section within the file.
    pub data_end: u64,
    /// The data section payload (`file[header_len..data_end]`).
    pub data: Bytes,
}

/// Data access to a segment's `.dat` file(s).
///
/// # Examples
///
/// ```
/// use std::path::{Path, PathBuf};
///
/// use bytes::Bytes;
/// use oceanfs_core::SegmentId;
/// use oceanfs_storage_api::error::Error;
/// use oceanfs_storage_api::{SegmentDataStore, SegmentFile};
///
/// // A no-op store demonstrating the implementable surface.
/// struct NullStore;
///
/// #[async_trait::async_trait]
/// impl SegmentDataStore for NullStore {
///     async fn read_segment_data(
///         &self,
///         _id: &SegmentId,
///     ) -> Result<Option<SegmentFile>, Error> {
///         Ok(None)
///     }
///
///     async fn write_segment_data(
///         &self,
///         _id: &SegmentId,
///         _data: &[u8],
///     ) -> Result<(), Error> {
///         Ok(())
///     }
///
///     async fn delete_shards(&self, _id: &SegmentId) -> Result<u64, Error> {
///         Ok(0)
///     }
///
///     async fn delete_shards_with_pool(
///         &self,
///         _id: &SegmentId,
///         _pool_id: u32,
///     ) -> Result<u64, Error> {
///         Ok(0)
///     }
///
///     fn list_segment_files(&self, _root: &Path) -> Result<Vec<PathBuf>, Error> {
///         Ok(Vec::new())
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait SegmentDataStore: Send + Sync {
    /// Full-file read.
    ///
    /// Returns the parsed header + data section, or `None` when no
    /// `.dat` exists for the segment. NotFound is a value, not an
    /// error — scrub/heal historically sniffed `ErrorKind::NotFound`
    /// to distinguish "not yet sealed / already reclaimed" (not
    /// corruption) from genuine I/O failures.
    ///
    /// # Errors
    ///
    /// Returns an error only for genuine failures: I/O errors,
    /// unreadable/corrupt headers, or a segment whose pool cannot be
    /// resolved.
    async fn read_segment_data(&self, id: &SegmentId) -> Result<Option<SegmentFile>>;

    /// Full-file write of the data section.
    ///
    /// A valid header is synthesized by the implementation. This is
    /// authoritative persistence — see ADR-0032 D3: implementations
    /// serialize writers per `.dat` and route the write through the
    /// optimized I/O layer.
    ///
    /// # Errors
    ///
    /// Returns an error if the write or its durability sync fails.
    async fn write_segment_data(&self, id: &SegmentId, data: &[u8]) -> Result<()>;

    /// Deletes a segment's `.dat`, resolving the pool through the
    /// lifecycle registry.
    ///
    /// Returns the reclaimed byte count (0 when no file existed).
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails for a reason other than a
    /// missing file.
    async fn delete_shards(&self, id: &SegmentId) -> Result<u64>;

    /// Deletes a `.dat` under an explicit pool root.
    ///
    /// The GC-compaction / recovery fast path — the caller already
    /// holds the pool id and skips the registry lookup.
    ///
    /// Returns the reclaimed byte count (0 when no file existed).
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails for a reason other than a
    /// missing file.
    async fn delete_shards_with_pool(&self, id: &SegmentId, pool_id: u32) -> Result<u64>;

    /// Lists `.dat` files under one root.
    ///
    /// Multi-root orphan sweep: the caller invokes this once per
    /// candidate pool root. The returned paths name the `.dat` files
    /// found directly under `root` (file names are `{uuid}.dat`).
    ///
    /// # Errors
    ///
    /// Returns an error when the root cannot be read for a reason
    /// other than not existing (a missing root lists nothing).
    fn list_segment_files(&self, root: &Path) -> Result<Vec<PathBuf>>;
}
