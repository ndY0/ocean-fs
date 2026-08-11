//! Segment store trait — append, seal, and read operations on segments.
//!
//! Abstracts segment lifecycle management so that coordinators are
//! decoupled from the concrete segment storage backend.

use oceanfs_core::{BucketId, ObjectKey, SegmentId, SegmentMetadata};

use crate::error::Error;

/// Handle to a segment that has been written to.
#[derive(Debug, Clone)]
pub struct SegmentHandle {
    segment_id: SegmentId,
    offset: u64,
    length: u32,
}

impl SegmentHandle {
    /// Creates a new segment handle.
    pub fn new(segment_id: SegmentId, offset: u64, length: u32) -> Self {
        Self { segment_id, offset, length }
    }

    /// Returns the segment identifier.
    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    /// Returns the byte offset within the segment.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the length of the written data.
    pub fn length(&self) -> u32 {
        self.length
    }
}

/// Trait for segment storage operations.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{BucketId, ObjectKey, SegmentId, SegmentMetadata};
/// use oceanfs_storage_api::{SegmentHandle, SegmentStore};
/// use oceanfs_storage_api::error::Error;
///
/// struct MySegmentStore;
///
/// #[async_trait::async_trait]
/// impl SegmentStore for MySegmentStore {
///     async fn append(
///         &self,
///         _bucket: &BucketId,
///         _key: &ObjectKey,
///         _data: &[u8],
///     ) -> Result<SegmentHandle, Error> {
///         Ok(SegmentHandle::new(SegmentId::new(), 0, 0))
///     }
///
///     async fn seal(
///         &self,
///         _segment_id: &SegmentId,
///     ) -> Result<SegmentMetadata, Error> {
/// #       use oceanfs_core::{NodeId, SizeTier, HashOutput};
///         Ok(SegmentMetadata {
///             segment_id: SegmentId::new(),
///             ec_k: 0,
///             ec_m: 0,
///             size_tier: SizeTier::Standard,
///             merkle_root: None,
///             storage_locations: vec![NodeId::new("n1")].into(),
///             sealed_at: None,
///         })
///     }
///
///     async fn read(
///         &self,
///         _segment_id: &SegmentId,
///     ) -> Result<Vec<u8>, Error> {
///         Ok(vec![])
///     }
/// }
/// ```
#[async_trait::async_trait]
#[allow(clippy::double_must_use)]
pub trait SegmentStore: Send + Sync {
    /// Appends data to the active segment for the given bucket and key.
    ///
    /// Returns a handle identifying the location of the written data.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is full or the write fails.
    async fn append(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        data: &[u8],
    ) -> Result<SegmentHandle, Error>;

    /// Seals the active segment, making it immutable.
    ///
    /// After sealing, the segment is available for EC encoding and
    /// distribution.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment cannot be sealed.
    async fn seal(&self, segment_id: &SegmentId) -> Result<SegmentMetadata, Error>;

    /// Reads the raw data for a sealed segment.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is not found or cannot be read.
    async fn read(&self, segment_id: &SegmentId) -> Result<Vec<u8>, Error>;
}
