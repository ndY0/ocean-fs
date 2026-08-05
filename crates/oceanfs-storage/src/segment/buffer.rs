//! Active segment — in-memory append-only buffer.
//!
//! An `ActiveSegment` accumulates blob writes in a `BytesMut` buffer.
//! When the buffer reaches its target size, the segment is sealed and
//! a new active segment takes its place.

use bytes::BytesMut;
use oceanfs_core::{SegmentId, SegmentSizeConfig, SizeTier};

use crate::{
    buffer_pool::BufferPool,
    error::{Error, Result},
};

/// An in-memory segment buffer accepting append operations.
///
/// Backed by a `BytesMut` from the buffer pool. Tracks the current
/// write cursor and whether the segment has reached its target size.
///
/// # Examples
///
/// ```ignore
/// // ActiveSegment is pub(crate); examples are in unit tests.
/// use oceanfs_core::{SegmentSizeConfig, SizeTier};
/// use oceanfs_storage::segment::buffer::ActiveSegment;
/// use oceanfs_storage::BufferPool;
///
/// let pool = BufferPool::new(65536, 4);
/// let config = SegmentSizeConfig::default();
/// let mut seg = ActiveSegment::new(SizeTier::Small, &config, &pool).unwrap();
///
/// let (offset, length) = seg.append(b"hello").unwrap();
/// assert_eq!(offset, 0);
/// assert_eq!(length, 5);
/// ```
pub struct ActiveSegment {
    /// Unique identifier for this segment.
    id: SegmentId,
    /// The storage tier this segment belongs to.
    tier: SizeTier,
    /// Accumulated blob data.
    buffer: BytesMut,
    /// Current write position in the buffer.
    cursor: u64,
    /// Target size in bytes — segment is considered full when `cursor >= target`.
    target_size: u64,
}

impl ActiveSegment {
    /// Creates a new active segment for the given tier.
    ///
    /// The segment acquires its backing buffer from `pool` and is
    /// initialized with the appropriate target size for its tier.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] if the tier is `Inline`.
    /// Returns an error if the buffer pool has no free buffers.
    pub fn new(tier: SizeTier, config: &SegmentSizeConfig, pool: &BufferPool) -> Result<Self> {
        let target_size = match tier {
            SizeTier::Small => config.small_target_size,
            SizeTier::Standard => config.default_target_size,
            SizeTier::Multi => config.default_target_size,
            // Inline tier does not use active segments.
            SizeTier::Inline => {
                return Err(Error::InvalidConfig(
                    "Inline tier does not use active segments".into(),
                ));
            }
            // non_exhaustive: future tiers fall through to default.
            _ => {
                return Err(Error::InvalidConfig(format!(
                    "unsupported tier for active segment: {tier:?}"
                )));
            }
        };

        let buffer = pool.acquire()?;
        // Ensure the buffer has sufficient capacity. BytesMut will
        // grow automatically, but pre-reserving avoids reallocation.
        let mut buffer = buffer;
        buffer.reserve(target_size as usize);

        Ok(Self { id: SegmentId::new(), tier, buffer, cursor: 0, target_size })
    }

    /// Appends data to the segment buffer.
    ///
    /// Returns `(offset, length)` indicating where the data was placed
    /// within the segment.
    ///
    /// # Errors
    ///
    /// Returns [`Error::SegmentFull`] if the segment has reached its
    /// target size and cannot accept more data.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // ActiveSegment is pub(crate); examples are in unit tests.
    /// # use oceanfs_core::{SegmentSizeConfig, SizeTier};
    /// # use oceanfs_storage::segment::buffer::ActiveSegment;
    /// # use oceanfs_storage::BufferPool;
    /// # let pool = BufferPool::new(65536, 4);
    /// # let config = SegmentSizeConfig::default();
    /// let mut seg = ActiveSegment::new(SizeTier::Standard, &config, &pool).unwrap();
    ///
    /// let (off1, len1) = seg.append(b"abc").unwrap();
    /// let (off2, len2) = seg.append(b"def").unwrap();
    /// assert_eq!(off1, 0);
    /// assert_eq!(off2, 3); // starts after first blob
    /// ```
    pub fn append(&mut self, data: &[u8]) -> Result<(u64, usize)> {
        if self.is_full() {
            return Err(Error::SegmentFull { segment_id: self.id, current_size: self.cursor });
        }

        let offset = self.cursor;
        let length = data.len();

        self.buffer.extend_from_slice(data);
        self.cursor += length as u64;

        Ok((offset, length))
    }

    /// Returns `true` if the segment has reached or exceeded its target size.
    pub fn is_full(&self) -> bool {
        self.cursor >= self.target_size
    }

    /// Returns the segment's unique identifier.
    pub fn id(&self) -> SegmentId {
        self.id
    }

    /// Returns the storage tier of this segment.
    pub fn tier(&self) -> SizeTier {
        self.tier
    }

    /// Returns the current size of the segment in bytes.
    pub fn size(&self) -> u64 {
        self.cursor
    }

    /// Returns the target size of the segment in bytes.
    pub fn target_size(&self) -> u64 {
        self.target_size
    }

    /// Returns a reference to the accumulated data.
    pub fn data(&self) -> &[u8] {
        &self.buffer
    }

    /// Consumes the segment, returning the backing buffer for pool reuse.
    pub fn into_buffer(self) -> BytesMut {
        self.buffer
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn make_pool() -> BufferPool {
        BufferPool::new(65536, 4)
    }

    fn config() -> SegmentSizeConfig {
        SegmentSizeConfig::default()
    }

    #[test]
    fn new_segment_starts_empty() {
        let pool = make_pool();
        let seg = ActiveSegment::new(SizeTier::Standard, &config(), &pool).unwrap();
        assert_eq!(seg.size(), 0);
        assert!(!seg.is_full());
    }

    #[test]
    fn append_increments_cursor() {
        let pool = make_pool();
        let mut seg = ActiveSegment::new(SizeTier::Standard, &config(), &pool).unwrap();
        let (off, len) = seg.append(&[0u8; 100]).unwrap();
        assert_eq!(off, 0);
        assert_eq!(len, 100);
        assert_eq!(seg.size(), 100);
    }

    #[test]
    fn append_returns_sequential_offsets() {
        let pool = make_pool();
        let mut seg = ActiveSegment::new(SizeTier::Standard, &config(), &pool).unwrap();
        let (off1, _) = seg.append(b"aaa").unwrap();
        let (off2, _) = seg.append(b"bbb").unwrap();
        let (off3, _) = seg.append(b"ccc").unwrap();
        assert_eq!(off1, 0);
        assert_eq!(off2, 3);
        assert_eq!(off3, 6);
    }

    #[test]
    fn is_full_when_cursor_exceeds_target() {
        // Use a tiny config so we can easily fill it
        let config = SegmentSizeConfig { default_target_size: 10, ..config() };
        let pool = BufferPool::new(1024, 2);
        let mut seg = ActiveSegment::new(SizeTier::Standard, &config, &pool).unwrap();
        assert!(!seg.is_full());
        seg.append(&[0u8; 10]).unwrap();
        assert!(seg.is_full());
    }

    #[test]
    fn is_full_when_cursor_exceeds_target_by_more() {
        let config = SegmentSizeConfig { default_target_size: 10, ..config() };
        let pool = BufferPool::new(1024, 2);
        let mut seg = ActiveSegment::new(SizeTier::Standard, &config, &pool).unwrap();
        seg.append(&[0u8; 15]).unwrap();
        assert!(seg.is_full());
    }

    #[test]
    fn append_on_full_segment_returns_error() {
        let config = SegmentSizeConfig { default_target_size: 5, ..config() };
        let pool = BufferPool::new(1024, 2);
        let mut seg = ActiveSegment::new(SizeTier::Standard, &config, &pool).unwrap();
        seg.append(&[0u8; 5]).unwrap();
        let result = seg.append(b"x");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::SegmentFull { .. }));
    }

    #[test]
    fn small_tier_uses_small_target() {
        let config =
            SegmentSizeConfig { small_target_size: 100, default_target_size: 1000, ..config() };
        let pool = BufferPool::new(65536, 4);
        let seg = ActiveSegment::new(SizeTier::Small, &config, &pool).unwrap();
        assert_eq!(seg.target_size(), 100);
    }

    #[test]
    fn standard_tier_uses_default_target() {
        let config = SegmentSizeConfig { default_target_size: 2000, ..config() };
        let pool = BufferPool::new(65536, 4);
        let seg = ActiveSegment::new(SizeTier::Standard, &config, &pool).unwrap();
        assert_eq!(seg.target_size(), 2000);
    }

    #[test]
    fn inline_tier_rejected() {
        let pool = BufferPool::new(65536, 4);
        let result = ActiveSegment::new(SizeTier::Inline, &config(), &pool);
        assert!(result.is_err());
    }

    #[test]
    fn data_returns_appended_bytes() {
        let pool = BufferPool::new(65536, 4);
        let mut seg = ActiveSegment::new(SizeTier::Standard, &config(), &pool).unwrap();
        seg.append(b"hello").unwrap();
        seg.append(b" world").unwrap();
        assert_eq!(seg.data(), b"hello world");
    }

    #[test]
    fn into_buffer_returns_buffer() {
        let pool = BufferPool::new(65536, 4);
        let mut seg = ActiveSegment::new(SizeTier::Standard, &config(), &pool).unwrap();
        seg.append(b"data").unwrap();
        let buf = seg.into_buffer();
        assert_eq!(&buf[..], b"data");
    }

    #[test]
    fn id_is_unique_per_segment() {
        let pool = BufferPool::new(65536, 4);
        let seg1 = ActiveSegment::new(SizeTier::Standard, &config(), &pool).unwrap();
        let seg2 = ActiveSegment::new(SizeTier::Standard, &config(), &pool).unwrap();
        assert_ne!(seg1.id(), seg2.id());
    }
}
