//! Trait implementations: bridges concrete storage types to `oceanfs-storage-api` traits.
//!
//! This module implements `WalWriter` from `oceanfs_storage_api`
//! for the concrete RocksDB-backed types in this crate.

use oceanfs_storage_api::error::Error as ApiError;

use crate::{wal::WalEntry, WalWriter};

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

#[async_trait::async_trait]
impl oceanfs_storage_api::WalWriter for WalWriter {
    async fn append(&self, entry_data: &[u8]) -> Result<u64, ApiError> {
        let entry = WalEntry::from_bytes(entry_data)
            .ok_or_else(|| ApiError::InvalidArgument("invalid WAL entry bytes".into()))?;
        // The API contract's position is the in-file offset (truncate's
        // unit); the file sequence is carried only by the crate-level
        // DataWalPos (ADR-0024 Decision 2).
        self.append(entry).await.map_err(map_error).map(|pos| pos.offset)
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
