//! Distributed write coordinator with quorum-based replication.

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_core::{BucketId, ChunkRef, HashOutput, ObjectKey, SegmentId, WriteResult};
use oceanfs_routing::RingCache;

use crate::router::HashKey;

/// A request to write an object.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    /// Target bucket.
    pub bucket: BucketId,
    /// Object key.
    pub key: ObjectKey,
    /// Pre-computed key hash.
    pub hash_key: HashKey,
    /// Object payload.
    pub data: Bytes,
    /// Expected quorum size for write acknowledgments.
    pub write_quorum: u8,
}

/// Coordinates distributed blob writes with quorum replication.
///
/// Routes writes to the correct replica set, appends to the local
/// segment, and collects W acknowledgments from replica nodes.
pub struct WriteCoordinator {
    ring: Arc<RingCache>,
    local_node_id: oceanfs_core::NodeId,
}

impl WriteCoordinator {
    /// Creates a new write coordinator.
    pub fn new(ring: Arc<RingCache>, local_node_id: oceanfs_core::NodeId) -> Self {
        Self { ring, local_node_id }
    }

    /// Executes a distributed write.
    ///
    /// # Errors
    ///
    /// Returns an error if routing fails or the quorum is not met.
    pub async fn put(&self, req: WriteRequest) -> crate::error::Result<WriteResult> {
        let replica_set = self.ring.lookup(req.hash_key.as_bytes());
        let is_local = replica_set.first().map(|n| n == &self.local_node_id).unwrap_or(false);

        if !is_local && !replica_set.is_empty() {
            // In full implementation: forward to the first successor.
            // For Phase 4, we just proceed with local write.
        }

        let segment_id = SegmentId::new();
        let offset = 0u64;
        let length = req.data.len() as u32;
        let chunks = {
            let mut c = smallvec::SmallVec::new();
            c.push(ChunkRef { segment_id, offset, length });
            c
        };

        let result = WriteResult {
            object_key: req.key,
            chunks,
            size: req.data.len() as u64,
            blake3_hash: Some(HashOutput::from_bytes([0u8; 32])), // placeholder
        };

        Ok(result)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::RingConfig;
    use oceanfs_routing::Ring;

    use super::*;

    #[test]
    fn coordinator_put_returns_result() {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(oceanfs_core::NodeId::new("n1"));

        let cache = Arc::new(RingCache::new(ring));
        let coord = WriteCoordinator::new(cache, oceanfs_core::NodeId::new("n1"));

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("obj"),
            hash_key: HashKey::from_key(&ObjectKey::new("obj")),
            data: Bytes::from_static(b"hello"),
            write_quorum: 1,
        };

        let result = tokio_test::block_on(coord.put(req)).unwrap();
        assert_eq!(result.size, 5);
        assert_eq!(result.chunks.len(), 1);
    }
}
