//! WAL entry — binary-serializable record of a segment append with inline data.
//!
//! Each entry carries a fixed-size header followed by the blob data inline.
//! On crash recovery, the data can be replayed directly into active segments
//! without consulting external storage.

use bytes::Bytes;
use oceanfs_core::{HashOutput, SegmentId};

/// Magic bytes at the start of every WAL entry header (4 bytes: "WAL\0").
pub(crate) const WAL_ENTRY_MAGIC: [u8; 4] = [b'W', b'A', b'L', 0];

/// A single entry in the Write-Ahead Log.
///
/// Records one blob append to a segment: which segment, where in the
/// segment the data starts, how long it is, an HLC timestamp for clock
/// reconstruction, and a checksum of the data. The blob data itself is
/// stored inline after the header for crash recovery replay.
///
/// # Binary Layout
///
/// The on-disk format is a fixed 80-byte header followed by `length` bytes
/// of inline data:
///
/// | Field         | Offset | Size |
/// |--------------|--------|------|
/// | magic        | 0      | 4    |
/// | segment_id   | 4      | 16   |
/// | offset       | 20     | 8    |
/// | length       | 28     | 4    |
/// | hlc_wall_time| 32     | 8    |
/// | hlc_logical  | 40     | 4    |
/// | checksum     | 44     | 32   |
/// | crc          | 76     | 4    |
/// | data         | 80     | N    |
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    /// Magic bytes identifying this as a WAL entry.
    pub magic: [u8; 4],
    /// The segment this append belongs to.
    pub segment_id: [u8; 16],
    /// Byte offset within the segment where the blob starts.
    pub offset: u64,
    /// Length of the blob data in bytes.
    pub length: u32,
    /// HLC wall-clock component (milliseconds since epoch) for clock reconstruction.
    pub hlc_wall_time: u64,
    /// HLC logical counter for events at the same wall time.
    pub hlc_logical: u32,
    /// BLAKE3 checksum of the blob data (32 bytes).
    pub checksum: [u8; 32],
    /// CRC32 of the preceding header fields for integrity verification.
    pub crc: u32,
    /// Inline blob data for crash recovery reconstruction.
    /// Stored as `Bytes` for zero-copy sharing from the network layer.
    pub data: Bytes,
}

impl WalEntry {
    /// Creates a new WAL entry with inline data.
    ///
    /// All fields are set; `crc` is computed from the header fields.
    /// The `data` is stored inline so that crash recovery can replay
    /// the entry without external storage.
    pub fn new(
        segment_id: SegmentId,
        offset: u64,
        length: u32,
        hlc_wall_time: u64,
        hlc_logical: u32,
        checksum: HashOutput,
        data: Bytes,
    ) -> Self {
        debug_assert_eq!(data.len(), length as usize, "data length must match declared length");
        let mut entry = Self {
            magic: WAL_ENTRY_MAGIC,
            segment_id: *segment_id.as_uuid().as_bytes(),
            offset,
            length,
            hlc_wall_time,
            hlc_logical,
            checksum: *checksum.as_bytes(),
            crc: 0,
            data,
        };
        entry.crc = entry.compute_crc();
        entry
    }

    /// Returns the segment ID from this entry.
    pub fn segment_id(&self) -> SegmentId {
        SegmentId::from_uuid_bytes(self.segment_id)
    }

    /// Returns the checksum as a `HashOutput`.
    pub fn checksum_hash(&self) -> HashOutput {
        HashOutput::from_bytes(self.checksum)
    }

    /// Returns the HLC wall time for clock reconstruction on replay.
    pub fn hlc_wall_time(&self) -> u64 {
        self.hlc_wall_time
    }

    /// Returns the HLC logical counter for clock reconstruction on replay.
    pub fn hlc_logical(&self) -> u32 {
        self.hlc_logical
    }

    /// Verifies the entry's CRC. Returns `true` if valid.
    pub fn verify_crc(&self) -> bool {
        self.crc == self.compute_crc()
    }

    /// Size of the serialized WAL entry header in bytes.
    pub fn header_size() -> usize {
        // 4 + 16 + 8 + 4 + 8 + 4 + 32 + 4 = 80
        80
    }

    /// Serializes the header to bytes (80 bytes, excluding inline data).
    ///
    /// Used by the writer to produce the fixed-size prefix before the
    /// variable-length data segment.
    pub fn to_header_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::header_size());
        buf.extend_from_slice(&self.magic);
        buf.extend_from_slice(&self.segment_id);
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.length.to_le_bytes());
        buf.extend_from_slice(&self.hlc_wall_time.to_le_bytes());
        buf.extend_from_slice(&self.hlc_logical.to_le_bytes());
        buf.extend_from_slice(&self.checksum);
        buf.extend_from_slice(&self.crc.to_le_bytes());
        debug_assert_eq!(buf.len(), Self::header_size());
        buf
    }

    /// Serializes the full entry: header + data.
    ///
    /// Used for round-trip testing and debugging.
    pub fn to_bytes(&self) -> Vec<u8> {
        // Reserve the exact serialized size up front: the header Vec is
        // capacity-exact, so extending it with the payload would
        // otherwise reallocate and copy the full data twice.
        let mut buf = self.to_header_bytes();
        buf.reserve(self.data.len());
        buf.extend_from_slice(&self.data);
        buf
    }

    /// Total serialized size: header + data.
    pub fn serialized_size(&self) -> usize {
        Self::header_size() + self.data.len()
    }

    /// Deserializes a WAL entry header from bytes.
    ///
    /// Returns `None` if the slice is too short or the magic bytes don't match.
    /// The caller must separately read the `length` bytes of inline data.
    pub fn from_header_bytes(header: &[u8]) -> Option<Self> {
        if header.len() < Self::header_size() {
            return None;
        }
        let magic: [u8; 4] = header[0..4].try_into().ok()?;
        if magic != WAL_ENTRY_MAGIC {
            return None;
        }
        let segment_id: [u8; 16] = header[4..20].try_into().ok()?;
        let offset = u64::from_le_bytes(header[20..28].try_into().ok()?);
        let length = u32::from_le_bytes(header[28..32].try_into().ok()?);
        let hlc_wall_time = u64::from_le_bytes(header[32..40].try_into().ok()?);
        let hlc_logical = u32::from_le_bytes(header[40..44].try_into().ok()?);
        let checksum: [u8; 32] = header[44..76].try_into().ok()?;
        let crc = u32::from_le_bytes(header[76..80].try_into().ok()?);

        let entry = Self {
            magic,
            segment_id,
            offset,
            length,
            hlc_wall_time,
            hlc_logical,
            checksum,
            crc,
            data: Bytes::new(),
        };

        if !entry.verify_crc() {
            return None;
        }

        Some(entry)
    }

    /// Deserializes a full WAL entry (header + data) from bytes.
    ///
    /// Returns `None` if the header is invalid or the data is too short.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        let mut entry = Self::from_header_bytes(data)?;
        let header_sz = Self::header_size();
        let data_len = entry.length as usize;
        if data.len() < header_sz + data_len {
            return None;
        }
        entry.data = Bytes::copy_from_slice(&data[header_sz..header_sz + data_len]);
        Some(entry)
    }

    fn compute_crc(&self) -> u32 {
        // CRC32 over all header fields preceding crc (76 bytes):
        // magic(4) + segment_id(16) + offset(8) + length(4) +
        // hlc_wall_time(8) + hlc_logical(4) + checksum(32) = 76
        let header_bytes = {
            let mut buf = [0u8; 76];
            buf[0..4].copy_from_slice(&self.magic);
            buf[4..20].copy_from_slice(&self.segment_id);
            buf[20..28].copy_from_slice(&self.offset.to_le_bytes());
            buf[28..32].copy_from_slice(&self.length.to_le_bytes());
            buf[32..40].copy_from_slice(&self.hlc_wall_time.to_le_bytes());
            buf[40..44].copy_from_slice(&self.hlc_logical.to_le_bytes());
            buf[44..76].copy_from_slice(&self.checksum);
            buf
        };
        crc32fast::hash(&header_bytes)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn new_entry_has_correct_magic() {
        let entry = WalEntry::new(
            SegmentId::new(),
            0,
            3,
            1000,
            1,
            HashOutput::from_bytes([0u8; 32]),
            vec![1, 2, 3].into(),
        );
        assert_eq!(entry.magic, WAL_ENTRY_MAGIC);
    }

    #[test]
    fn new_entry_preserves_fields() {
        let id = SegmentId::new();
        let data = Bytes::from_static(b"hello world");
        let len = data.len() as u32;
        let checksum = HashOutput::from_bytes([0xABu8; 32]);
        let entry = WalEntry::new(id, 1024, len, 5000, 3, checksum, data.clone());
        assert_eq!(entry.segment_id(), id);
        assert_eq!(entry.offset, 1024);
        assert_eq!(entry.length, len);
        assert_eq!(entry.hlc_wall_time, 5000);
        assert_eq!(entry.hlc_logical, 3);
        assert_eq!(entry.checksum, *checksum.as_bytes());
        assert_eq!(entry.data, data);
    }

    #[test]
    fn entry_roundtrip_serialize_deserialize() {
        let data = Bytes::from(vec![0xABu8; 128]);
        let len = data.len() as u32;
        let entry = WalEntry::new(
            SegmentId::new(),
            2048,
            len,
            7000,
            2,
            HashOutput::from_bytes([0xCDu8; 32]),
            data,
        );
        let bytes = entry.to_bytes();
        let restored = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry.magic, restored.magic);
        assert_eq!(entry.segment_id, restored.segment_id);
        assert_eq!(entry.offset, restored.offset);
        assert_eq!(entry.length, restored.length);
        assert_eq!(entry.hlc_wall_time, restored.hlc_wall_time);
        assert_eq!(entry.hlc_logical, restored.hlc_logical);
        assert_eq!(entry.checksum, restored.checksum);
        assert_eq!(entry.crc, restored.crc);
        assert_eq!(entry.data, restored.data);
    }

    #[test]
    fn verify_crc_passes_for_valid_entry() {
        let entry = WalEntry::new(
            SegmentId::new(),
            0,
            3,
            100,
            0,
            HashOutput::from_bytes([1u8; 32]),
            vec![7, 8, 9].into(),
        );
        assert!(entry.verify_crc());
    }

    #[test]
    fn verify_crc_fails_for_corrupted_entry() {
        let mut entry = WalEntry::new(
            SegmentId::new(),
            0,
            3,
            100,
            0,
            HashOutput::from_bytes([1u8; 32]),
            vec![7, 8, 9].into(),
        );
        entry.length = 999; // corrupt
        assert!(!entry.verify_crc());
    }

    #[test]
    fn from_header_bytes_rejects_wrong_magic() {
        let entry = WalEntry::new(
            SegmentId::new(),
            0,
            3,
            0,
            0,
            HashOutput::from_bytes([0u8; 32]),
            vec![1, 2, 3].into(),
        );
        let mut header = entry.to_header_bytes();
        header[0] = b'X';
        assert!(WalEntry::from_header_bytes(&header).is_none());
    }

    #[test]
    fn from_header_bytes_rejects_short_header() {
        assert!(WalEntry::from_header_bytes(&[0u8; 4]).is_none());
    }

    #[test]
    fn from_bytes_rejects_data_too_short() {
        let entry = WalEntry::new(
            SegmentId::new(),
            0,
            100,
            0,
            0,
            HashOutput::from_bytes([0u8; 32]),
            vec![0u8; 100].into(),
        );
        let bytes = entry.to_bytes();
        // Truncate: keep header only, drop data.
        let truncated = &bytes[..WalEntry::header_size()];
        assert!(WalEntry::from_bytes(truncated).is_none());
    }

    #[test]
    fn header_size_is_constant() {
        let size = WalEntry::header_size();
        assert_eq!(size, 80);
    }

    #[test]
    fn empty_data_entry_roundtrip() {
        let entry = WalEntry::new(
            SegmentId::new(),
            0,
            0,
            0,
            0,
            HashOutput::from_bytes([0u8; 32]),
            Bytes::new(),
        );
        let bytes = entry.to_bytes();
        let restored = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(restored.length, 0);
        assert!(restored.data.is_empty());
    }
}
