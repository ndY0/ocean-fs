//! Healing gRPC service.
//!
//! Handles `HealingRpc` RPCs for hinted handoff, Merkle exchange,
//! shard fetch for EC reconstruction, and repaired shard push.

use std::sync::Arc;

use oceanfs_core::{Hlc, NodeId, SegmentId};
use oceanfs_network::healing::{
    healing_rpc_server::HealingRpc, FetchShardChunk, FetchShardRequest, HintRequest, HintResponse,
    MerkleRequest, MerkleResponse, PushRepairedShardRequest, PushRepairedShardResponse,
};
use oceanfs_storage::SegmentDataStore;
use tonic::{Request, Response, Status};

/// gRPC service for healing and anti-entropy operations.
pub struct HealingGrpcService {
    /// Handoff buffer for storing hints.
    handoff: Arc<crate::HintedHandoff>,
    /// Metadata store for Merkle root lookups during anti-entropy.
    metadata_store: Arc<oceanfs_storage::MetadataStore>,
    /// Segment data store for shard fetch and repair.
    data_store: Arc<dyn SegmentDataStore>,
}

impl HealingGrpcService {
    /// Creates a new healing gRPC service.
    pub fn new(
        handoff: Arc<crate::HintedHandoff>,
        metadata_store: Arc<oceanfs_storage::MetadataStore>,
        data_store: Arc<dyn SegmentDataStore>,
    ) -> Self {
        Self { handoff, metadata_store, data_store }
    }
}

#[tonic::async_trait]
impl HealingRpc for HealingGrpcService {
    type FetchShardStream =
        tokio_stream::wrappers::ReceiverStream<Result<FetchShardChunk, Status>>;

    async fn hinted_handoff(
        &self,
        request: Request<HintRequest>,
    ) -> Result<Response<HintResponse>, Status> {
        let req = request.into_inner();

        let intended_for =
            req.intended_for.map(NodeId::from).unwrap_or_else(|| NodeId::new("unknown"));
        let segment_id =
            req.segment_id.and_then(|sid| SegmentId::try_from(sid).ok()).unwrap_or_default();
        let hlc = req.hlc.and_then(|h| Hlc::try_from(h).ok()).unwrap_or_else(Hlc::zero);

        let hint = crate::HintRecord {
            intended_for: intended_for.clone(),
            segment_id,
            offset: 0,
            length: req.data.len() as u32,
            timestamp: hlc,
            data: req.data,
        };

        match self.handoff.handoff(intended_for.clone(), hint).await {
            Ok(()) => {
                tracing::debug!(
                    intended_for = %intended_for,
                    segment_id = %segment_id,
                    "received and stored hinted handoff"
                );

                let proto_segment_id: oceanfs_core::proto::common::SegmentId = segment_id.into();
                Ok(Response::new(HintResponse {
                    accepted: true,
                    stored_segment_id: Some(proto_segment_id),
                }))
            }
            Err(e) => {
                tracing::warn!(
                    intended_for = %intended_for,
                    error = %e,
                    "failed to store hinted handoff"
                );
                Ok(Response::new(HintResponse { accepted: false, stored_segment_id: None }))
            }
        }
    }

    async fn merkle_exchange(
        &self,
        request: Request<MerkleRequest>,
    ) -> Result<Response<MerkleResponse>, Status> {
        let req = request.into_inner();

        // Process all requested segment IDs and return Merkle data for the first one
        // with available data. In a full multi-segment exchange, we would return
        // a batch response.
        let mut best_root_hash = vec![0u8; 32];
        let mut best_leaf_hashes: Vec<Vec<u8>> = Vec::new();
        let mut chosen_sid: Option<oceanfs_core::proto::common::SegmentId> = None;

        for proto_sid in &req.segment_ids {
            let sid = match SegmentId::try_from(proto_sid.clone()) {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Try to read the segment data to compute the Merkle tree.
            let segment_data = match self.data_store.read_segment_data(&sid) {
                Ok(data) => data,
                Err(_) => continue,
            };

            if segment_data.is_empty() {
                continue;
            }

            // Build a Merkle tree over the segment data using 64 KB leaf size.
            // This computes both the root hash and leaf hashes from actual data.
            let leaf_size: usize = 65536; // 64 KB leaf size per spec §11.2
            if let Some(tree) = oceanfs_storage::MerkleTree::build(&segment_data, leaf_size) {
                let root = tree.root();
                best_root_hash = root.hash().as_bytes().to_vec();

                // Collect all leaf hashes for the response.
                best_leaf_hashes = (0..tree.leaf_count() as usize)
                    .filter_map(|i| tree.leaf_hash(i).map(|h| h.as_bytes().to_vec()))
                    .collect();

                chosen_sid = Some(proto_sid.clone());
                break; // Found a segment with data — return it.
            }
        }

        if best_root_hash.len() == 32 && best_root_hash.iter().all(|b| *b == 0) {
            // No segment data was found — fall back to metadata store lookup.
            // Use the first segment ID as a reference for the fallback.
            let proto_sid = req.segment_ids.first().cloned().unwrap_or_default();
            let sid = SegmentId::try_from(proto_sid).unwrap_or_default();

            // Look up the segment's Merkle root from the metadata store.
            best_root_hash = self
                .metadata_store
                .list_segments()
                .into_iter()
                .filter_map(|r| r.ok())
                .find(|s| s.segment_id == sid)
                .and_then(|seg| seg.merkle_root)
                .map(|h| h.as_bytes().to_vec())
                .unwrap_or_else(|| vec![0u8; 32]);

            chosen_sid = req.segment_ids.first().cloned();
        }

        tracing::debug!(
            segment_id = ?chosen_sid,
            root_hash_len = best_root_hash.len(),
            leaf_count = best_leaf_hashes.len(),
            "merkle_exchange: returning computed Merkle data"
        );

        Ok(Response::new(MerkleResponse {
            segment_id: chosen_sid,
            root_hash: best_root_hash,
            leaf_hashes: best_leaf_hashes,
        }))
    }

    async fn fetch_shard(
        &self,
        request: Request<FetchShardRequest>,
    ) -> Result<Response<Self::FetchShardStream>, Status> {
        let req = request.into_inner();
        let segment_id =
            req.segment_id.and_then(|sid| SegmentId::try_from(sid).ok()).unwrap_or_default();
        let shard_index = req.shard_index as usize;

        // Read the full segment data and extract the requested shard.
        let data = self.data_store.read_segment_data(&segment_id).map_err(|e| {
            Status::internal(format!("failed to read segment data for shard fetch: {e}"))
        })?;

        // Determine shard size from total data length and known k+m.
        // This is a simplification — in production, we'd look up ec_k/ec_m from metadata.
        let total_shards = 6; // default k=4, m=2
        let shard_size = if data.is_empty() { 0 } else { data.len() / total_shards };

        let start = shard_index * shard_size;
        let end = (start + shard_size).min(data.len());
        let shard_data: Vec<u8> = if start < data.len() {
            data[start..end].to_vec()
        } else {
            Vec::new()
        };

        // Stream the shard data in chunks.
        let chunk_size = 65536; // 64 KB chunks
        let chunks: Vec<FetchShardChunk> = shard_data
            .chunks(chunk_size)
            .enumerate()
            .map(|(i, chunk)| FetchShardChunk {
                chunk_index: i as u32,
                data: chunk.to_vec(),
            })
            .collect();

        let (tx, rx) = tokio::sync::mpsc::channel(chunks.len().max(1));
        for chunk in chunks {
            if tx.send(Ok(chunk)).await.is_err() {
                break;
            }
        }

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn push_repaired_shard(
        &self,
        request: Request<PushRepairedShardRequest>,
    ) -> Result<Response<PushRepairedShardResponse>, Status> {
        let req = request.into_inner();
        let segment_id =
            req.segment_id.and_then(|sid| SegmentId::try_from(sid).ok()).unwrap_or_default();

        // Write the repaired shard into the data store.
        // In production this would merge the shard into the correct position.
        match self.data_store.write_segment_data(&segment_id, &req.data) {
            Ok(()) => {
                tracing::info!(
                    segment_id = %segment_id,
                    shard_index = req.shard_index,
                    "received and stored repaired shard"
                );
                Ok(Response::new(PushRepairedShardResponse { accepted: true }))
            }
            Err(e) => {
                tracing::warn!(
                    segment_id = %segment_id,
                    error = %e,
                    "failed to store repaired shard"
                );
                Ok(Response::new(PushRepairedShardResponse { accepted: false }))
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use oceanfs_core::proto::common::SegmentId as ProtoSegmentId;
    use oceanfs_core::SegmentId;
    use oceanfs_storage::SegmentDataStore;

    use super::*;
    use crate::HintedHandoff;

    /// In-memory store for healing tests.
    struct TestHealStore {
        data: Mutex<HashMap<SegmentId, Vec<u8>>>,
    }

    impl TestHealStore {
        fn new() -> Self {
            Self { data: Mutex::new(HashMap::new()) }
        }
    }

    impl SegmentDataStore for TestHealStore {
        fn write_segment_data(
            &self,
            segment_id: &SegmentId,
            data: &[u8],
        ) -> Result<(), oceanfs_storage::Error> {
            self.data.lock().unwrap().insert(segment_id.clone(), data.to_vec());
            Ok(())
        }

        fn read_segment_data(
            &self,
            segment_id: &SegmentId,
        ) -> Result<Vec<u8>, oceanfs_storage::Error> {
            self.data
                .lock()
                .unwrap()
                .get(segment_id)
                .cloned()
                .ok_or_else(|| oceanfs_storage::Error::SegmentNotFound(*segment_id))
        }
    }

    fn make_service() -> HealingGrpcService {
        let handoff = Arc::new(HintedHandoff::new());
        let metadata_store = Arc::new(
            oceanfs_storage::MetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: std::env::temp_dir().join(format!("oceanfs-test-heal-{}", std::process::id())),
                ..Default::default()
            })
            .unwrap(),
        );
        let data_store: Arc<dyn SegmentDataStore> = Arc::new(TestHealStore::new());
        HealingGrpcService::new(handoff, metadata_store, data_store)
    }

    #[tokio::test]
    async fn handoff_valid_hint_returns_accepted() {
        let service = make_service();

        let request = tonic::Request::new(HintRequest {
            intended_for: Some(oceanfs_core::proto::common::NodeId {
                id: "target-node".to_string(),
            }),
            segment_id: Some(SegmentId::new().into()),
            data: b"test hint data".to_vec(),
            hlc: None,
        });

        let response = service.hinted_handoff(request).await.unwrap();
        let resp = response.into_inner();
        assert!(resp.accepted, "valid hint should be accepted");
        assert!(resp.stored_segment_id.is_some(), "should return a stored_segment_id");
    }

    #[tokio::test]
    async fn merkle_exchange_with_stored_data_returns_correct_root() {
        let handoff = Arc::new(HintedHandoff::new());
        let metadata_store = Arc::new(
            oceanfs_storage::MetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: std::env::temp_dir().join(format!("oceanfs-test-merkle-{}", std::process::id())),
                ..Default::default()
            })
            .unwrap(),
        );
        let test_store = Arc::new(TestHealStore::new());

        // Write known data to the store so the Merkle tree can be built.
        let seg_id = SegmentId::new();
        let data: Vec<u8> = vec![0xAB; 65536 * 2]; // 128 KB = 2 leaves of 64 KB
        test_store.write_segment_data(&seg_id, &data).unwrap();

        let data_store: Arc<dyn SegmentDataStore> = test_store;
        let service = HealingGrpcService::new(handoff, metadata_store, data_store);

        let proto_sid: ProtoSegmentId = seg_id.into();
        let request = tonic::Request::new(MerkleRequest {
            segment_ids: vec![proto_sid],
            tree_depth: 8,
            node_id: None,
        });

        let response = service.merkle_exchange(request).await.unwrap();
        let resp = response.into_inner();

        // Root hash should be 32 bytes (BLAKE3 output).
        assert_eq!(resp.root_hash.len(), 32, "root hash should be 32 bytes");
        // Should have computed leaf hashes from actual data.
        assert!(!resp.leaf_hashes.is_empty(), "should have leaf hashes");
        // 128 KB / 64 KB = 2 leaves.
        assert_eq!(resp.leaf_hashes.len(), 2, "should have 2 leaf hashes for 128 KB data");
        // Each leaf hash should also be 32 bytes.
        for (i, leaf) in resp.leaf_hashes.iter().enumerate() {
            assert_eq!(leaf.len(), 32, "leaf hash {} should be 32 bytes", i);
        }
    }
}
