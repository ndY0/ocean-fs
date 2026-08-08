//! On-disk segment header format.
//!
//! The segment header is written at the start of every sealed segment file.
//! It contains metadata needed to load the segment index and verify integrity.

use oceanfs_core::SegmentId;

/// Magic bytes identifying a sealed segment file.
pub(crate) const SEGMENT_MAGIC: [u8; 4] = *b"OFSG";

/// Current segment format version.
pub(crate) const SEGMENT_VERSION: u16 = 1;

/// On-disk size of a [`SegmentHeader`] in bytes.
pub const SEGMENT_HEADER_SIZE: usize = 76;

/// On-disk header for a sealed segment.
///
/// Written at offset 0 of every segment data file. Contains the
/// segment identity, blob count, index location, and integrity checksum.
///
/// # Binary Layout (32 bytes)
///
/// | Field       | Offset | Size |
/// |-------------|--------|------|
/// | magic       | 0      | 4    |
/// | version     | 4      | 2    |
/// | segment_id  | 6      | 16   |
/// | size        | 22     | 8    |
/// | blob_count  | 30     | 4    |
/// | index_offset| 34     | 8    |
/// | checksum    | 42     | 32   |
/// | _pad        | 74     | 2    |
///
/// Total: 76 bytes (padded to 8-byte alignment).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Magic bytes: `[O, F, S, G]`.
    pub magic: [u8; 4],
    /// Format version (currently 1).
    pub version: u16,
    /// The segment this header belongs to.
    pub segment_id: SegmentId,
    /// Total size of the segment data in bytes.
    pub size: u64,
    /// Number of blobs stored in this segment.
    pub blob_count: u32,
    /// Byte offset where the segment index begins (after the data).
    pub index_offset: u64,
    /// BLAKE3 checksum of the segment data (not including the header).
    pub checksum: [u8; 32],
}

impl SegmentHeader {
    /// Creates a new segment header.
    pub fn new(
        segment_id: SegmentId,
        size: u64,
        blob_count: u32,
        index_offset: u64,
        checksum: [u8; 32],
    ) -> Self {
        Self {
            magic: SEGMENT_MAGIC,
            version: SEGMENT_VERSION,
            segment_id,
            size,
            blob_count,
            index_offset,
            checksum,
        }
    }

    /// Serializes the header to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; 76];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..22].copy_from_slice(self.segment_id.as_uuid().as_bytes());
        buf[22..30].copy_from_slice(&self.size.to_le_bytes());
        buf[30..34].copy_from_slice(&self.blob_count.to_le_bytes());
        buf[34..42].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[42..74].copy_from_slice(&self.checksum);
        buf[74..76].fill(0); // padding
        buf
    }

    /// Deserializes a segment header from bytes.
    ///
    /// Returns `None` if the magic bytes don't match.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 76 {
            return None;
        }
        let magic: [u8; 4] = data[0..4].try_into().ok()?;
        if magic != SEGMENT_MAGIC {
            return None;
        }
        let version = u16::from_le_bytes(data[4..6].try_into().ok()?);
        let segment_id_bytes: [u8; 16] = data[6..22].try_into().ok()?;
        let segment_id = SegmentId::from_uuid_bytes(segment_id_bytes);
        let size = u64::from_le_bytes(data[22..30].try_into().ok()?);
        let blob_count = u32::from_le_bytes(data[30..34].try_into().ok()?);
        let index_offset = u64::from_le_bytes(data[34..42].try_into().ok()?);
        let checksum: [u8; 32] = data[42..74].try_into().ok()?;

        Some(Self { magic, version, segment_id, size, blob_count, index_offset, checksum })
    }

    /// Size of a serialized header in bytes.
    pub fn serialized_size() -> usize {
        76
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let hdr = SegmentHeader::new(SegmentId::new(), 4096, 10, 4096, [0xABu8; 32]);
        let bytes = hdr.to_bytes();
        assert_eq!(bytes.len(), 76);
        let restored = SegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(hdr.segment_id, restored.segment_id);
        assert_eq!(hdr.size, restored.size);
        assert_eq!(hdr.blob_count, restored.blob_count);
        assert_eq!(hdr.checksum, restored.checksum);
    }

    #[test]
    fn from_bytes_rejects_wrong_magic() {
        let bytes = vec![0u8; 76];
        assert!(SegmentHeader::from_bytes(&bytes).is_none());
    }

    #[test]
    fn from_bytes_rejects_short_data() {
        assert!(SegmentHeader::from_bytes(&[0u8; 4]).is_none());
    }
}
