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
        let data =
            std::fs::read(&path).map_err(|e| Error::Io(std::io::Error::other(format!("{e}"))))?;

        const HEADER_SIZE: usize = 76;
        if data.len() < HEADER_SIZE {
            return Err(Error::InvalidConfig(format!(
                "segment file {segment_id} too short: {} bytes",
                data.len()
            )));
        }
        Ok(Bytes::from(data[HEADER_SIZE..].to_vec()))
    }

    fn write_segment_data(
        &self,
        segment_id: &SegmentId,
        data: &[u8],
    ) -> std::result::Result<(), Error> {
        let path = self.segment_dir.join(format!("{segment_id}.dat"));
        let header = vec![0u8; 76];
        let mut file_data = header;
        file_data.extend_from_slice(data);
        std::fs::write(&path, &file_data)
            .map_err(|e| Error::Io(std::io::Error::other(format!("{e}"))))?;
        Ok(())
    }
}
