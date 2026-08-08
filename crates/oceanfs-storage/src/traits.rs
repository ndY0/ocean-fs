//! Trait implementations: bridges concrete storage types to `oceanfs-storage-api` traits.
//!
//! This module implements `BlobStore`, `WalWriter` from `oceanfs_storage_api`
//! for the concrete RocksDB-backed types in this crate.

use bytes::Bytes;
use oceanfs_core::SegmentId;
use oceanfs_storage_api::{error::Error as ApiError, BlobStore as BlobStoreTrait};

use crate::{wal::WalEntry, BlobStore, WalWriter};

/// Converts a crate-local error into a storage-API error.
///
/// I/O errors are forwarded; other variants are wrapped as internal errors.
fn map_error(e: crate::Error) -> ApiError {
    match e {
        crate::Error::Io(io) => ApiError::Io(io),
        crate::Error::SegmentNotFound(id) => ApiError::SegmentNotFound(id),
        other => ApiError::Internal(other.to_string()),
    }
}

impl BlobStoreTrait for BlobStore {
    fn write_blob(&self, segment_id: &SegmentId, data: &[u8]) -> Result<(), ApiError> {
        self.write_blob(segment_id, data).map_err(map_error)
    }

    fn read_blob(&self, segment_id: &SegmentId) -> Result<Option<Bytes>, ApiError> {
        self.read_blob(segment_id).map_err(map_error)
    }

    fn delete_blob(&self, segment_id: &SegmentId) -> Result<(), ApiError> {
        self.delete_blob(segment_id).map_err(map_error)
    }

    fn list_blobs(&self) -> Result<Vec<SegmentId>, ApiError> {
        self.list_blobs().map_err(map_error)
    }
}

#[async_trait::async_trait]
impl oceanfs_storage_api::WalWriter for WalWriter {
    async fn append(&self, entry_data: &[u8]) -> Result<u64, ApiError> {
        let entry = WalEntry::from_bytes(entry_data)
            .ok_or_else(|| ApiError::InvalidArgument("invalid WAL entry bytes".into()))?;
        self.append(entry).await.map_err(map_error)
    }

    async fn truncate(&self, position: u64) -> Result<(), ApiError> {
        self.truncate(position).await.map_err(map_error)
    }

    async fn sync(&self) -> Result<(), ApiError> {
        self.sync().await.map_err(map_error)
    }

    async fn global_position(&self) -> u64 {
        self.global_position().await
    }
}
