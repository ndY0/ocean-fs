//! WAL entry — binary-serializable record of a segment append.

use oceanfs_core::{HashOutput, SegmentId};

/// Magic bytes at the start of every WAL entry (4 bytes: "WAL\0").
pub(crate) const WAL_ENTRY_MAGIC: [u8; 4] = [b'W', b'A', b'L', 0];

/// A single entry in the Write-Ahead Log.
///
/// Records one blob append to a segment: which segment, where in the
/// segment the data starts, how long it is, and a checksum of the data.
///
/// # Binary Layout
///
/// `#[repr(C)]` — 72 bytes on disk:
///
/// | Field      | Offset | Size |
/// |-----------|--------|------|
/// | magic     | 0      | 4    |
/// | segment_id| 4      | 16   |
/// | offset    | 20     | 8    |
/// | length    | 28     | 4    |
/// | checksum  | 32     | 32   |
/// | crc       | 64     | 4    |
/// | _pad      | 68     | 4    |
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
    /// BLAKE3 checksum of the blob data (32 bytes).
    pub checksum: [u8; 32],
    /// CRC32 of the preceding fields for integrity verification.
    pub crc: u32,
    /// Padding to align to 8 bytes (reserved, always 0).
    pub _pad: u32,
}

impl WalEntry {
    /// Creates a new WAL entry.
    ///
    /// All fields are set; `crc` is computed from the other fields.
    pub fn new(segment_id: SegmentId, offset: u64, length: u32, checksum: HashOutput) -> Self {
        let mut entry = Self {
            magic: WAL_ENTRY_MAGIC,
            segment_id: *segment_id.as_uuid().as_bytes(),
            offset,
            length,
            checksum: *checksum.as_bytes(),
            crc: 0,
            _pad: 0,
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

    /// Verifies the entry's CRC. Returns `true` if valid.
    pub fn verify_crc(&self) -> bool {
        self.crc == self.compute_crc()
    }

    /// Serializes this entry to a byte vector.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::serialized_size());
        buf.extend_from_slice(&self.magic);
        buf.extend_from_slice(&self.segment_id);
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.length.to_le_bytes());
        buf.extend_from_slice(&self.checksum);
        buf.extend_from_slice(&self.crc.to_le_bytes());
        buf.extend_from_slice(&self._pad.to_le_bytes());
        buf
    }

    /// Deserializes a WAL entry from bytes.
    ///
    /// # Errors
    ///
    /// Returns `None` if the slice is too short or the magic bytes don't match.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < Self::serialized_size() {
            return None;
        }
        let magic: [u8; 4] = data[0..4].try_into().ok()?;
        if magic != WAL_ENTRY_MAGIC {
            return None;
        }
        let segment_id: [u8; 16] = data[4..20].try_into().ok()?;
        let offset = u64::from_le_bytes(data[20..28].try_into().ok()?);
        let length = u32::from_le_bytes(data[28..32].try_into().ok()?);
        let checksum: [u8; 32] = data[32..64].try_into().ok()?;
        let crc = u32::from_le_bytes(data[64..68].try_into().ok()?);
        let _pad = u32::from_le_bytes(data[68..72].try_into().ok()?);

        let entry = Self { magic, segment_id, offset, length, checksum, crc, _pad };

        if !entry.verify_crc() {
            return None;
        }

        Some(entry)
    }

    /// Size of a serialized WAL entry in bytes.
    pub fn serialized_size() -> usize {
        // 4 + 16 + 8 + 4 + 32 + 4 + 4 = 72
        72
    }

    fn compute_crc(&self) -> u32 {
        // CRC32 over the fixed fields preceding crc (magic + segment_id + offset + length + checksum = 64 bytes).
        let header_bytes = {
            let mut buf = [0u8; 64];
            buf[0..4].copy_from_slice(&self.magic);
            buf[4..20].copy_from_slice(&self.segment_id);
            buf[20..28].copy_from_slice(&self.offset.to_le_bytes());
            buf[28..32].copy_from_slice(&self.length.to_le_bytes());
            buf[32..64].copy_from_slice(&self.checksum);
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
        let entry = WalEntry::new(SegmentId::new(), 0, 100, HashOutput::from_bytes([0u8; 32]));
        assert_eq!(entry.magic, WAL_ENTRY_MAGIC);
    }

    #[test]
    fn new_entry_preserves_fields() {
        let id = SegmentId::new();
        let checksum = HashOutput::from_bytes([0xABu8; 32]);
        let entry = WalEntry::new(id, 1024, 512, checksum);
        assert_eq!(entry.segment_id(), id);
        assert_eq!(entry.offset, 1024);
        assert_eq!(entry.length, 512);
        assert_eq!(entry.checksum, *checksum.as_bytes());
    }

    #[test]
    fn entry_roundtrip_serialize_deserialize() {
        let entry =
            WalEntry::new(SegmentId::new(), 2048, 256, HashOutput::from_bytes([0xCDu8; 32]));
        let bytes = entry.to_bytes();
        let restored = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry, restored);
    }

    #[test]
    fn verify_crc_passes_for_valid_entry() {
        let entry = WalEntry::new(SegmentId::new(), 0, 10, HashOutput::from_bytes([1u8; 32]));
        assert!(entry.verify_crc());
    }

    #[test]
    fn verify_crc_fails_for_corrupted_entry() {
        let mut entry = WalEntry::new(SegmentId::new(), 0, 10, HashOutput::from_bytes([1u8; 32]));
        entry.length = 999; // corrupt
        assert!(!entry.verify_crc());
    }

    #[test]
    fn from_bytes_rejects_wrong_magic() {
        let entry = WalEntry::new(SegmentId::new(), 0, 1, HashOutput::from_bytes([0u8; 32]));
        let mut bytes = entry.to_bytes();
        bytes[0] = b'X'; // corrupt magic
        assert!(WalEntry::from_bytes(&bytes).is_none());
    }

    #[test]
    fn from_bytes_rejects_short_data() {
        assert!(WalEntry::from_bytes(&[0u8; 4]).is_none());
    }

    #[test]
    fn serialized_size_is_constant() {
        let size = WalEntry::serialized_size();
        let entry = WalEntry::new(SegmentId::new(), 0, 0, HashOutput::from_bytes([0u8; 32]));
        assert_eq!(entry.to_bytes().len(), size);
    }
}
