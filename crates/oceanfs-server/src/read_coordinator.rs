//! Read coordinator — parallel shard fetch and blob reconstruction.

use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata};

use crate::router::HashKey;

/// A request to read an object.
#[derive(Debug, Clone)]
pub struct ReadRequest {
    /// Source bucket.
    pub bucket: BucketId,
    /// Object key.
    pub key: ObjectKey,
    /// Pre-computed key hash.
    pub hash_key: HashKey,
}

/// Result of a read operation.
#[derive(Debug, Clone)]
pub struct ReadResult {
    /// The object's data.
    pub data: bytes::Bytes,
    /// Metadata for the object.
    pub metadata: ObjectMetadata,
}

/// Coordinates distributed blob reads with parallel shard fetch.
pub struct ReadCoordinator;

impl ReadCoordinator {
    /// Creates a new read coordinator.
    pub fn new() -> Self {
        Self
    }

    /// Executes a read. In Phase 4, this is a stub.
    ///
    /// # Errors
    ///
    /// Always returns `Routing("not implemented")` in Phase 4.
    pub async fn get(&self, _req: ReadRequest) -> crate::error::Result<ReadResult> {
        Err(crate::error::Error::Routing("not implemented".into()))
    }
}

impl Default for ReadCoordinator {
    fn default() -> Self {
        Self::new()
    }
}
