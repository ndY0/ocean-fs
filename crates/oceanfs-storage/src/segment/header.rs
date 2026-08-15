//! On-disk segment header format.
//!
//! The segment header is written at the start of every sealed segment file.
//! It contains metadata needed to load the segment index and verify integrity.

use oceanfs_core::SegmentId;

/// Magic bytes identifying a sealed segment file.
pub(crate) const SEGMENT_MAGIC: [u8; 4] = *b"OFSG";

/// Current segment format version.
///
/// v2 adds the EC parity section: the layout becomes
/// header + data + parity + index, with `parity_offset`/`parity_size`
/// recorded in the header. v1 files (76-byte header, no parity) remain
/// readable — `SegmentHeader::from_bytes` reports `parity_offset: 0`.
pub(crate) const SEGMENT_VERSION: u16 = 2;

/// Version of the original segment format (no parity section).
#[cfg(test)]
pub(crate) const SEGMENT_VERSION_V1: u16 = 1;

/// On-disk size of a v2 [`SegmentHeader`] in bytes.
pub const SEGMENT_HEADER_SIZE: usize = 92;

/// On-disk size of a v1 [`SegmentHeader`] in bytes.
pub const SEGMENT_HEADER_SIZE_V1: usize = 76;

/// On-disk header for a sealed segment.
///
/// Written at offset 0 of every segment data file. Contains the
/// segment identity, blob count, index location, integrity checksum,
/// and (v2) the location of the EC parity section.
///
/// # Binary Layout (92 bytes, v2)
///
/// | Field         | Offset | Size |
/// |---------------|--------|------|
/// | magic         | 0      | 4    |
/// | version       | 4      | 2    |
/// | segment_id    | 6      | 16   |
/// | size          | 22     | 8    |
/// | blob_count    | 30     | 4    |
/// | index_offset  | 34     | 8    |
/// | checksum      | 42     | 32   |
/// | parity_offset | 74     | 8    |
/// | parity_size   | 82     | 8    |
/// | _pad          | 90     | 2    |
///
/// Total: 92 bytes (padded to 8-byte alignment).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Magic bytes: `[O, F, S, G]`.
    pub magic: [u8; 4],
    /// Format version (1 = no parity section, 2 = with parity).
    pub version: u16,
    /// The segment this header belongs to.
    pub segment_id: SegmentId,
    /// Total size of the segment data in bytes.
    pub size: u64,
    /// Number of blobs stored in this segment.
    pub blob_count: u32,
    /// Byte offset where the segment index begins (after the data and
    /// any parity section).
    pub index_offset: u64,
    /// BLAKE3 checksum of the segment data (not including the header).
    pub checksum: [u8; 32],
    /// Byte offset where the EC parity section begins, or 0
    /// for v1 files (no parity section).
    pub parity_offset: u64,
    /// Total size of the parity section in bytes, or 0 for v1 files.
    pub parity_size: u64,
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
            parity_offset: 0,
            parity_size: 0,
        }
    }

    /// Creates a v2 segment header with the EC parity section.
    pub fn with_parity(
        segment_id: SegmentId,
        size: u64,
        blob_count: u32,
        index_offset: u64,
        checksum: [u8; 32],
        parity_offset: u64,
        parity_size: u64,
    ) -> Self {
        Self {
            magic: SEGMENT_MAGIC,
            version: SEGMENT_VERSION,
            segment_id,
            size,
            blob_count,
            index_offset,
            checksum,
            parity_offset,
            parity_size,
        }
    }

    /// Serializes the header to bytes (v2 layout).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; SEGMENT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..22].copy_from_slice(self.segment_id.as_uuid().as_bytes());
        buf[22..30].copy_from_slice(&self.size.to_le_bytes());
        buf[30..34].copy_from_slice(&self.blob_count.to_le_bytes());
        buf[34..42].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[42..74].copy_from_slice(&self.checksum);
        buf[74..82].copy_from_slice(&self.parity_offset.to_le_bytes());
        buf[82..90].copy_from_slice(&self.parity_size.to_le_bytes());
        buf[90..92].fill(0); // padding
        buf
    }

    /// Deserializes a segment header from bytes.
    ///
    /// Returns `None` if the magic bytes don't match. Accepts both v1
    /// (76-byte) and v2 (92-byte) headers; v1 headers deserialize with
    /// `parity_offset`/`parity_size` = 0.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < SEGMENT_HEADER_SIZE_V1 {
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

        let (parity_offset, parity_size) =
            if version >= SEGMENT_VERSION && data.len() >= SEGMENT_HEADER_SIZE {
                (
                    u64::from_le_bytes(data[74..82].try_into().ok()?),
                    u64::from_le_bytes(data[82..90].try_into().ok()?),
                )
            } else {
                (0, 0)
            };

        Some(Self {
            magic,
            version,
            segment_id,
            size,
            blob_count,
            index_offset,
            checksum,
            parity_offset,
            parity_size,
        })
    }

    /// Returns the on-disk header size for the given format version.
    pub fn header_size(version: u16) -> usize {
        if version >= SEGMENT_VERSION {
            SEGMENT_HEADER_SIZE
        } else {
            SEGMENT_HEADER_SIZE_V1
        }
    }

    /// Returns the on-disk header size of this header.
    pub fn serialized_size(&self) -> usize {
        Self::header_size(self.version)
    }

    /// Returns the end offset of the data section: `parity_offset` when
    /// a parity section exists (v2), otherwise `index_offset`.
    pub fn data_end(&self) -> u64 {
        if self.parity_offset > 0 {
            self.parity_offset
        } else {
            self.index_offset
        }
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
        assert_eq!(bytes.len(), SEGMENT_HEADER_SIZE);
        let restored = SegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(hdr.segment_id, restored.segment_id);
        assert_eq!(hdr.size, restored.size);
        assert_eq!(hdr.blob_count, restored.blob_count);
        assert_eq!(hdr.checksum, restored.checksum);
        assert_eq!(restored.version, SEGMENT_VERSION);
        assert_eq!(restored.parity_offset, 0);
    }

    #[test]
    fn header_with_parity_roundtrip() {
        let hdr = SegmentHeader::with_parity(
            SegmentId::new(),
            4096,
            10,
            4096 + 2048,
            [0xCDu8; 32],
            4096,
            2048,
        );
        let bytes = hdr.to_bytes();
        let restored = SegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored.parity_offset, 4096);
        assert_eq!(restored.parity_size, 2048);
        assert_eq!(restored.index_offset, 4096 + 2048);
    }

    #[test]
    fn v1_header_deserializes_without_parity() {
        // A v1 header is 76 bytes: magic + version(1) + the original
        // fields, no parity offsets.
        let mut bytes = vec![0u8; SEGMENT_HEADER_SIZE_V1];
        bytes[0..4].copy_from_slice(&SEGMENT_MAGIC);
        bytes[4..6].copy_from_slice(&SEGMENT_VERSION_V1.to_le_bytes());
        bytes[6..22].copy_from_slice(SegmentId::new().as_uuid().as_bytes());
        bytes[34..42].copy_from_slice(&4096u64.to_le_bytes());
        let restored = SegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored.version, SEGMENT_VERSION_V1);
        assert_eq!(restored.parity_offset, 0);
        assert_eq!(restored.parity_size, 0);
        assert_eq!(SegmentHeader::header_size(SEGMENT_VERSION_V1), SEGMENT_HEADER_SIZE_V1);
        assert_eq!(restored.serialized_size(), SEGMENT_HEADER_SIZE_V1);
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
