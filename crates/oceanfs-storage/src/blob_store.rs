//! Disk-persisted blob data store.
//!
//! Stores segment blob data on disk so it survives process restarts.
//! Each blob is stored as a flat file named by its SegmentId UUID.
//!
//! Used by the S3 handler on PUT to durably persist blob data alongside
//! the in-memory segment reader. On node startup, blob data files are
//! scanned and loaded into the in-memory store for fast access.

use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
};

use oceanfs_core::SegmentId;

use crate::error::Result;

/// A disk-backed blob data store.
///
/// Stores blob data in a flat directory as `{segment_id}.blob` files.
/// The `SegmentDataStore` trait is implemented in `oceanfs-durability`
/// for this type.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_storage::BlobStore;
///
/// let store = BlobStore::open("/tmp/oceanfs/data/blobs").unwrap();
/// store.write_blob(&segment_id, &data).unwrap();
/// let data = store.read_blob(&segment_id).unwrap().unwrap();
/// ```
pub struct BlobStore {
    /// Directory where blob files are stored.
    dir: PathBuf,
}

impl BlobStore {
    /// Opens (or creates) the blob store directory.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be created.
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self { dir: dir.to_path_buf() })
    }

    /// Writes blob data for a segment to disk.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be written.
    pub fn write_blob(&self, segment_id: &SegmentId, data: &[u8]) -> Result<()> {
        let path = self.blob_path(segment_id);
        let mut file = std::fs::File::create(&path)?;
        file.write_all(data)?;
        file.sync_all()?;
        Ok(())
    }

    /// Reads blob data for a segment from disk.
    ///
    /// Returns `Ok(None)` if the blob file does not exist.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file exists but cannot be read.
    pub fn read_blob(&self, segment_id: &SegmentId) -> Result<Option<Vec<u8>>> {
        let path = self.blob_path(segment_id);
        if !path.exists() {
            return Ok(None);
        }
        let mut file = std::fs::File::open(&path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Ok(Some(data))
    }

    /// Deletes a blob file for a segment.
    ///
    /// If the file doesn't exist, this is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file exists but cannot be deleted.
    pub fn delete_blob(&self, segment_id: &SegmentId) -> Result<()> {
        let path = self.blob_path(segment_id);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Scans the blob directory and returns all SegmentIds found.
    ///
    /// Used on startup to discover which segments have persisted data.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be read.
    pub fn list_blobs(&self) -> Result<Vec<SegmentId>> {
        let mut ids = Vec::new();
        let entries = std::fs::read_dir(&self.dir)?;
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(uuid_str) = name.strip_suffix(".blob") {
                if let Ok(uuid) = uuid::Uuid::parse_str(uuid_str) {
                    ids.push(SegmentId::from_uuid_bytes(*uuid.as_bytes()));
                }
            }
        }
        Ok(ids)
    }

    fn blob_path(&self, segment_id: &SegmentId) -> PathBuf {
        self.dir.join(format!("{}.blob", segment_id.as_uuid()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::SegmentId;

    use super::*;

    #[test]
    fn write_and_read_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = BlobStore::open(tmp.path()).unwrap();
        let id = SegmentId::new();
        let data = b"hello blob store";
        store.write_blob(&id, data).unwrap();
        let read = store.read_blob(&id).unwrap().unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn read_nonexistent_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let store = BlobStore::open(tmp.path()).unwrap();
        let id = SegmentId::new();
        assert!(store.read_blob(&id).unwrap().is_none());
    }

    #[test]
    fn delete_removes_blob() {
        let tmp = tempfile::tempdir().unwrap();
        let store = BlobStore::open(tmp.path()).unwrap();
        let id = SegmentId::new();
        store.write_blob(&id, b"data").unwrap();
        store.delete_blob(&id).unwrap();
        assert!(store.read_blob(&id).unwrap().is_none());
    }

    #[test]
    fn list_blobs_finds_all() {
        let tmp = tempfile::tempdir().unwrap();
        let store = BlobStore::open(tmp.path()).unwrap();
        let ids: Vec<_> = (0..5).map(|_| SegmentId::new()).collect();
        for id in &ids {
            store.write_blob(id, b"data").unwrap();
        }
        let found = store.list_blobs().unwrap();
        assert_eq!(found.len(), 5);
    }
}
