//! Trait impl bridge: implements `SegmentDataStore` using segment files
//! from the authoritative `segments/` directory.
//!
//! Replaces the previous `BlobStore` bridge (`blob_store_impl.rs`) which
//! read from a redundant `blobs/` directory.

use bytes::Bytes;
use oceanfs_core::SegmentId;
use oceanfs_storage::Error;

use crate::anti_entropy::SegmentDataStore;

/// A `SegmentDataStore` backed by segment files in `{segment_dir}/`.
///
/// Reads the full raw segment data (skipping the 76-byte header).
/// Writes include a minimal 76-byte header for compatibility.
pub struct DiskSegmentStore {
    segment_dir: std::path::PathBuf,
}

impl DiskSegmentStore {
    /// Creates a new store rooted at `segment_dir`.
    ///
    /// `segment_dir` is the directory containing `{segment_id}.dat` files
    /// written by the `SegmentSealer`.
    pub fn new(segment_dir: std::path::PathBuf) -> Self {
        Self { segment_dir }
    }
}

impl SegmentDataStore for DiskSegmentStore {
    fn read_segment_data(&self, segment_id: &SegmentId) -> std::result::Result<Bytes, Error> {
        let path = self.segment_dir.join(format!("{segment_id}.dat"));
        // Preserve the original io::Error (including its ErrorKind): scrub
        // distinguishes NotFound (shard not yet sealed / already reclaimed —
        // NOT corruption) from genuine I/O failures by matching the kind.
        // Wrapping with `Error::other(format!(...))` would collapse every
        // error into ErrorKind::Other and hide the NotFound signal.
        let data = std::fs::read(&path).map_err(Error::Io)?;

        // The on-disk header size depends on the format version (v1 = 76
        // bytes, v2 = 92 bytes); the returned data is the DATA section
        // only (header..index_offset — excluding any parity section).
        let header = oceanfs_storage::SegmentHeader::from_bytes(&data).ok_or_else(|| {
            Error::InvalidConfig(format!("segment file {segment_id} has a bad header"))
        })?;
        let hdr_size = oceanfs_storage::SegmentHeader::header_size(header.version);
        let data_end = header.data_end() as usize;
        if data.len() < hdr_size || data_end > data.len() {
            return Err(Error::InvalidConfig(format!(
                "segment file {segment_id} too short: {} bytes",
                data.len()
            )));
        }
        Ok(Bytes::from(data[hdr_size..data_end].to_vec()))
    }

    fn write_segment_data(
        &self,
        segment_id: &SegmentId,
        data: &[u8],
    ) -> std::result::Result<(), Error> {
        let path = self.segment_dir.join(format!("{segment_id}.dat"));
        // Ensure the segment directory exists — the gRPC append_segment
        // handler must be able to write segments even if no local segment
        // sealing has ever created the directory.
        std::fs::create_dir_all(&self.segment_dir)
            .map_err(|e| Error::Io(std::io::Error::other(format!("{e}"))))?;
        // Write a valid v1 segment file: the read path verifies the
        // header (magic, version, checksum) on first touch, so a
        // zeroed header would be rejected as corrupt. Heal/anti-entropy
        // repaired segments therefore carry a real v1 header.
        let mut file_data = vec![0u8; 76];
        file_data[0..4].copy_from_slice(b"OFSG");
        file_data[4..6].copy_from_slice(&1u16.to_le_bytes());
        file_data[22..30].copy_from_slice(&(data.len() as u64).to_le_bytes());
        file_data[30..34].copy_from_slice(&0u32.to_le_bytes()); // blob_count
        file_data[34..42].copy_from_slice(&((76 + data.len()) as u64).to_le_bytes());
        let checksum = *blake3::hash(data).as_bytes();
        file_data[42..74].copy_from_slice(&checksum);
        file_data.extend_from_slice(data);
        std::fs::write(&path, &file_data)
            .map_err(|e| Error::Io(std::io::Error::other(format!("{e}"))))?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::SegmentId;

    use super::*;

    #[test]
    fn write_then_read_roundtrip_is_header_valid() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskSegmentStore::new(dir.path().to_path_buf());
        let id = SegmentId::new();
        let data: Vec<u8> = (0..2048u32).map(|i| (i % 251) as u8).collect();

        store.write_segment_data(&id, &data).unwrap();
        let read_back = store.read_segment_data(&id).unwrap();
        assert_eq!(&read_back[..], &data[..], "heal-written data must round-trip");

        // The file must be a valid v1 segment (the read path's strict
        // header verification accepts it).
        let file = std::fs::read(dir.path().join(format!("{id}.dat"))).unwrap();
        let header = oceanfs_storage::SegmentHeader::from_bytes(&file).expect("valid header");
        assert_eq!(header.version, 1);
        assert_eq!(header.data_end() as usize, 76 + data.len());
    }
}
