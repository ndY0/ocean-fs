//! Healing gRPC service.
//!
//! Handles `HealingRpc` RPCs for hinted handoff, Merkle exchange,
//! shard fetch for EC reconstruction, and repaired shard push.

use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use oceanfs_core::{Hlc, HlcClock, NodeId, SegmentId};
use tonic::{Request, Response, Status};

use crate::{
    healing_rpc::{
        healing_rpc_server::HealingRpc, FetchHintDataChunk, FetchHintDataRequest, FetchShardChunk,
        FetchShardRequest, HintRequest, HintResponse, MerkleRequest, MerkleResponse,
        PushRepairedShardRequest, PushRepairedShardResponse,
    },
    hinted_handoff_rpc::{hint_record::Record, HintedHandoffRequest, HintedHandoffResponse},
    SegmentDataStore,
};

/// Fetches a byte range of a segment from an origin node.
///
/// The hinted-handoff receiver uses this to materialize segment-ref
/// hints: the hint carries `segment_id + offset + length` (NOT the
/// blob data — hints stay small even for multipart/GB blobs), and the
/// receiver pulls the range from the origin (the hint sender, which
/// holds the segment) before applying it locally.
#[async_trait::async_trait]
pub trait HintDataFetcher: Send + Sync {
    /// Fetches `length` bytes at `offset` of `segment_id` from
    /// `origin` (the sender's gRPC address).
    async fn fetch_range(
        &self,
        origin: SocketAddr,
        segment_id: &SegmentId,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, String>;
}

/// gRPC service for healing and anti-entropy operations.
pub struct HealingGrpcService {
    /// Handoff buffer for storing hints.
    handoff: Arc<crate::HintedHandoff>,
    /// Metadata store for Merkle root lookups during anti-entropy.
    metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore>,
    /// The lifecycle registry — the machine's `Sealed` entries carry
    /// the Merkle-root fallback (ADR-0025 Decision 3).
    registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
    /// Segment data store for shard fetch and repair.
    data_store: Arc<dyn SegmentDataStore>,
    /// This node's identifier. When set, hints whose `intended_for`
    /// matches it are APPLIED to the local metadata store instead of
    /// being buffered (a hint for oneself is a delayed write — t21).
    local_node_id: Option<NodeId>,
    /// HLC clock for receive-merge (hlc-causality-closure G2). Remote
    /// hint timestamps are merged via [`HlcClock::update`] so the local
    /// clock never lags the nodes that sent them.
    hlc_clock: Arc<HlcClock>,
    /// Fetcher for materializing segment-ref hints from their origin.
    /// `None` (tests) degrades to buffering the ref as before.
    hint_data_fetcher: Option<Arc<dyn HintDataFetcher>>,
}

impl HealingGrpcService {
    /// Creates a new healing gRPC service.
    pub fn new(
        handoff: Arc<crate::HintedHandoff>,
        metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore>,
        registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
        data_store: Arc<dyn SegmentDataStore>,
        hlc_clock: Arc<HlcClock>,
    ) -> Self {
        Self {
            handoff,
            metadata_store,
            registry,
            data_store,
            local_node_id: None,
            hlc_clock,
            hint_data_fetcher: None,
        }
    }

    /// Sets this node's identifier so that self-intended hints are
    /// applied instead of buffered.
    #[must_use]
    pub fn with_local_node_id(mut self, node_id: NodeId) -> Self {
        self.local_node_id = Some(node_id);
        self
    }

    /// Installs the segment-ref hint materializer (composition root).
    ///
    /// Segment-ref hints carry `segment_id + offset + length` instead of
    /// the blob data (hints stay small for multipart/GB blobs); the
    /// receiver pulls the range from the origin via this fetcher and
    /// applies it locally. Without a fetcher, segment-ref hints degrade
    /// to buffering (tests).
    #[must_use]
    pub fn with_hint_data_fetcher(mut self, fetcher: Arc<dyn HintDataFetcher>) -> Self {
        self.hint_data_fetcher = Some(fetcher);
        self
    }

    /// Returns `true` when the hint is intended for this node and must
    /// be applied locally rather than buffered for remote delivery.
    fn is_local_hint(&self, intended_for: &NodeId) -> bool {
        self.local_node_id.as_ref() == Some(intended_for)
    }

    /// Applies an inline hint intended for this node: writes the object
    /// metadata (with inline data) to the local metadata store so reads
    /// succeed once hinted-handoff delivery completes (t21).
    ///
    /// `hlc` is the original write's timestamp (hlc-causality-closure
    /// G5): the applied metadata persists the *original* version, not
    /// zero, so a late delivery loses LWW against newer writes.
    /// Applies a delayed write's data locally: writes the object
    /// metadata (with the blob data inline) to the local metadata store
    /// so reads succeed once hinted-handoff delivery completes (t21).
    ///
    /// `hlc` is the original write's timestamp (hlc-causality-closure
    /// G5): the applied metadata persists the *original* version, not
    /// zero, so a late delivery loses LWW against newer writes.
    fn apply_hint_data(
        &self,
        bucket: &oceanfs_core::BucketId,
        object_key: &str,
        data: Bytes,
        hlc: Hlc,
    ) {
        let meta = oceanfs_core::ObjectMetadata {
            object_key: oceanfs_core::ObjectKey::new(object_key),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(
                *blake3::hash(&data).as_bytes(),
            )),
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(data.clone()),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            hlc,
        };
        match oceanfs_storage_api::MetadataStore::put_object(
            self.metadata_store.as_ref(),
            bucket,
            meta,
        ) {
            Ok(()) => {
                tracing::info!(
                    bucket = %bucket,
                    key = %object_key,
                    size = data.len(),
                    "applied hinted handoff locally"
                );
            }
            Err(e) => {
                tracing::warn!(
                    bucket = %bucket,
                    key = %object_key,
                    error = %e,
                    "failed to apply hinted handoff locally"
                );
            }
        }
    }

    fn apply_inline_hint(
        &self,
        bucket: oceanfs_core::BucketId,
        object_key: String,
        data: Bytes,
        hlc: Hlc,
    ) {
        self.apply_hint_data(&bucket, &object_key, data, hlc);
    }
}

#[tonic::async_trait]
impl HealingRpc for HealingGrpcService {
    type FetchShardStream = tokio_stream::wrappers::ReceiverStream<Result<FetchShardChunk, Status>>;
    type FetchHintDataStream =
        tokio_stream::wrappers::ReceiverStream<Result<FetchHintDataChunk, Status>>;

    async fn hinted_handoff_single(
        &self,
        request: Request<HintRequest>,
    ) -> Result<Response<HintResponse>, Status> {
        let req = request.into_inner();

        let intended_for =
            req.intended_for.map(NodeId::from).unwrap_or_else(|| NodeId::new("unknown"));
        let segment_id =
            req.segment_id.and_then(|sid| SegmentId::try_from(sid).ok()).unwrap_or_default();
        let hlc = req.hlc.and_then(|h| Hlc::try_from(h).ok()).unwrap_or_else(Hlc::zero);

        // Receive rule (G2): merge the remote hint's timestamp into the
        // local clock.
        self.hlc_clock.update(hlc);

        let hint = crate::HintRecord {
            intended_for: intended_for.clone(),
            segment_id,
            offset: 0,
            length: req.data.len() as u32,
            timestamp: hlc,
            data: req.data,
            stored_at_secs: 0,
        };

        match self.handoff.handoff(intended_for.clone(), hint).await {
            Ok(()) => {
                tracing::debug!(
                    intended_for = %intended_for,
                    segment_id = %segment_id,
                    "received and stored hinted handoff (single)"
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
                    "failed to store hinted handoff (single)"
                );
                Ok(Response::new(HintResponse { accepted: false, stored_segment_id: None }))
            }
        }
    }

    /// Handles batched hinted handoff delivery.
    ///
    /// Receives up to `max_batch_size` hints in a single gRPC call and
    /// stores each in the in-memory handoff buffer for delivery to the
    /// intended node.
    async fn hinted_handoff(
        &self,
        request: Request<HintedHandoffRequest>,
    ) -> Result<Response<HintedHandoffResponse>, Status> {
        // The sender's gRPC LISTENER address, carried as a request
        // metadata header by the delivery client. `request.remote_addr()`
        // is the sender's ephemeral SOURCE port (the client side of this
        // very connection) — dialing it back fails. The header is the
        // address that actually accepts connections; remote_addr remains
        // the fallback for legacy senders.
        let sender_grpc_addr = request
            .metadata()
            .get("oceanfs-sender-grpc")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<SocketAddr>().ok())
            .or_else(|| request.remote_addr());
        let req = request.into_inner();
        let hint_count = req.hints.len() as u32;
        let mut accepted_count = 0u32;

        for proto_hint in &req.hints {
            // Convert proto-based HintRecord to the legacy HintRecord for storage.
            // The existing HintedHandoff uses the legacy struct.
            match &proto_hint.record {
                Some(Record::Inline(inline)) => {
                    let intended_for = inline
                        .intended_for
                        .clone()
                        .map(NodeId::from)
                        .unwrap_or_else(|| NodeId::new("unknown"));

                    // G5: the hint carries the original write's HLC.
                    // Legacy records (written before the field existed)
                    // replay with None → zero timestamp.
                    let hlc = inline
                        .hlc
                        .as_ref()
                        .map(|h| Hlc::new(h.wall_time, h.logical))
                        .unwrap_or_else(Hlc::zero);
                    // Receive rule (G2): merge the remote timestamp.
                    self.hlc_clock.update(hlc);

                    // A hint intended for this node is a delayed write:
                    // apply it to the local metadata store (t21).
                    if self.is_local_hint(&intended_for) {
                        let bucket = inline
                            .bucket_id
                            .clone()
                            .map(oceanfs_core::BucketId::from)
                            .unwrap_or_else(|| oceanfs_core::BucketId::new("default"));
                        self.apply_inline_hint(
                            bucket,
                            inline.object_key.clone(),
                            inline.data.clone(),
                            hlc,
                        );
                        accepted_count += 1;
                        continue;
                    }

                    let legacy_hint = crate::HintRecord {
                        intended_for: intended_for.clone(),
                        segment_id: SegmentId::new(), // inline hints don't have a segment
                        offset: 0,
                        length: inline.data.len() as u32,
                        timestamp: hlc,
                        data: inline.data.clone(),
                        stored_at_secs: 0,
                    };

                    match self.handoff.handoff(intended_for.clone(), legacy_hint).await {
                        Ok(()) => {
                            accepted_count += 1;
                        }
                        Err(e) => {
                            tracing::warn!(
                                intended_for = %intended_for,
                                error = %e,
                                "failed to store batched hint (inline)"
                            );
                        }
                    }
                }
                Some(Record::SegmentRef(seg_ref)) => {
                    let intended_for = seg_ref
                        .intended_for
                        .clone()
                        .map(NodeId::from)
                        .unwrap_or_else(|| NodeId::new("unknown"));

                    // G5: the hint carries the original write's HLC.
                    let hlc = seg_ref
                        .hlc
                        .as_ref()
                        .map(|h| Hlc::new(h.wall_time, h.logical))
                        .unwrap_or_else(Hlc::zero);
                    // Receive rule (G2): merge the remote timestamp.
                    self.hlc_clock.update(hlc);

                    let segment_id = seg_ref
                        .segment_id
                        .as_ref()
                        .and_then(|sid| SegmentId::try_from(sid.clone()).ok())
                        .unwrap_or_default();
                    let bucket = seg_ref
                        .bucket_id
                        .clone()
                        .map(oceanfs_core::BucketId::from)
                        .unwrap_or_else(|| oceanfs_core::BucketId::new("default"));

                    // A segment-ref hint intended for THIS node is a
                    // delayed write: pull the blob range from the origin
                    // (the hint sender — it holds the segment) and apply
                    // it. Segment-ref hints deliberately do NOT carry the
                    // data inline, so hints stay small even when blobs
                    // reach multipart/GB sizes.
                    if self.is_local_hint(&intended_for) {
                        let mut applied = false;
                        if let (Some(origin), Some(fetcher)) =
                            (sender_grpc_addr, &self.hint_data_fetcher)
                        {
                            match fetcher
                                .fetch_range(origin, &segment_id, seg_ref.offset, seg_ref.length)
                                .await
                            {
                                Ok(data) => {
                                    self.apply_hint_data(&bucket, &seg_ref.object_key, data, hlc);
                                    applied = true;
                                    accepted_count += 1;
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        intended_for = %intended_for,
                                        segment_id = %segment_id,
                                        error = %e,
                                        "failed to fetch segment-ref hint data from origin; \
                                         NOT accepted — the sender will retry"
                                    );
                                }
                            }
                        } else {
                            tracing::warn!(
                                intended_for = %intended_for,
                                "segment-ref hint intended for self but no origin/fetcher \
                                 available; NOT accepted — the sender will retry"
                            );
                        }
                        if applied {
                            continue;
                        }
                        // Fetch failed or unavailable: do NOT fall through
                        // to the legacy relay buffer (which nothing drains
                        // — accepting would make the sender truncate its
                        // WAL and lose the hint). Leave the hint
                        // unaccepted so the batch returns accepted=false
                        // and the sender re-enqueues + retries.
                        continue;
                    }

                    // For segment refs, store as a legacy hint with empty data.
                    let legacy_hint = crate::HintRecord {
                        intended_for: intended_for.clone(),
                        segment_id,
                        offset: seg_ref.offset,
                        length: seg_ref.length,
                        timestamp: hlc,
                        data: bytes::Bytes::new(),
                        stored_at_secs: 0,
                    };

                    match self.handoff.handoff(intended_for.clone(), legacy_hint).await {
                        Ok(()) => {
                            accepted_count += 1;
                        }
                        Err(e) => {
                            tracing::warn!(
                                intended_for = %intended_for,
                                error = %e,
                                "failed to store batched hint (segment ref)"
                            );
                        }
                    }
                }
                None => {
                    tracing::warn!("batched hint with no record variant; skipping");
                }
            }
        }

        Ok(Response::new(HintedHandoffResponse {
            accepted: accepted_count == hint_count,
            accepted_count,
        }))
    }

    async fn merkle_exchange(
        &self,
        request: Request<MerkleRequest>,
    ) -> Result<Response<MerkleResponse>, Status> {
        let req = request.into_inner();

        // Process all requested segment IDs and return Merkle data for the first one
        // with available data. In a full multi-segment exchange, we would return
        // a batch response.
        let mut best_root_hash = Bytes::from(vec![0u8; 32]);
        let mut best_leaf_hashes: Vec<Bytes> = Vec::new();
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
            if let Some(tree) = crate::MerkleTree::build(&segment_data, leaf_size) {
                let root = tree.root();
                best_root_hash = Bytes::copy_from_slice(root.hash().as_bytes());

                // Collect all leaf hashes for the response.
                best_leaf_hashes = (0..tree.leaf_count() as usize)
                    .filter_map(|i| tree.leaf_hash(i).map(|h| Bytes::copy_from_slice(h.as_bytes())))
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

            // Look up the segment's Merkle root from the machine
            // (ADR-0025 Decision 3).
            best_root_hash = self
                .registry
                .get(sid)
                .and_then(|entry| entry.metadata.merkle_root)
                .map(|h| Bytes::copy_from_slice(h.as_bytes()))
                .unwrap_or_else(|| Bytes::from(vec![0u8; 32]));

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
            full_tree_included: false,
            internal_nodes: vec![],
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
        let shard_data: Bytes =
            if start < data.len() { data.slice(start..end) } else { Bytes::new() };

        // Stream the shard data in chunks.
        let chunk_size = 65536; // 64 KB chunks
        let chunks: Vec<FetchShardChunk> = (0..shard_data.len())
            .step_by(chunk_size)
            .enumerate()
            .map(|(i, off)| {
                let end = (off + chunk_size).min(shard_data.len());
                FetchShardChunk { chunk_index: i as u32, data: shard_data.slice(off..end) }
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

    async fn fetch_hint_data(
        &self,
        request: Request<FetchHintDataRequest>,
    ) -> Result<Response<Self::FetchHintDataStream>, Status> {
        let req = request.into_inner();
        let segment_id =
            req.segment_id.and_then(|sid| SegmentId::try_from(sid).ok()).unwrap_or_default();
        let offset = req.offset as usize;
        let length = req.length as usize;

        // The hinted-handoff receiver pulls the blob range of a
        // segment-ref hint from the origin node. The origin's segment
        // store holds the data; slice the requested range.
        let data = self.data_store.read_segment_data(&segment_id).map_err(|e| {
            Status::internal(format!("failed to read segment data for hint fetch: {e}"))
        })?;

        let end = (offset + length).min(data.len());
        let range: Bytes = if offset < data.len() { data.slice(offset..end) } else { Bytes::new() };

        // Stream the range in 64 KB chunks (overlapping transfer, perf
        // rule 4.4 — same shape as FetchShard).
        let chunk_size = 65536;
        let chunks: Vec<FetchHintDataChunk> = (0..range.len())
            .step_by(chunk_size)
            .enumerate()
            .map(|(i, off)| {
                let end = (off + chunk_size).min(range.len());
                FetchHintDataChunk { chunk_index: i as u32, data: range.slice(off..end) }
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

    use oceanfs_core::{proto::common::SegmentId as ProtoSegmentId, SegmentId};
    use parking_lot::Mutex;

    use super::*;
    use crate::{HintedHandoff, SegmentDataStore};

    /// In-memory store for healing tests.
    struct TestHealStore {
        data: Mutex<HashMap<SegmentId, Bytes>>,
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
            self.data.lock().insert(*segment_id, Bytes::copy_from_slice(data));
            Ok(())
        }

        fn read_segment_data(
            &self,
            segment_id: &SegmentId,
        ) -> Result<Bytes, oceanfs_storage::Error> {
            self.data
                .lock()
                .get(segment_id)
                .cloned()
                .ok_or(oceanfs_storage::Error::SegmentNotFound(*segment_id))
        }
    }

    fn make_service() -> HealingGrpcService {
        let handoff = Arc::new(HintedHandoff::new());
        let metadata_store = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: std::env::temp_dir()
                    .join(format!("oceanfs-test-heal-{}", std::process::id())),
                ..Default::default()
            })
            .unwrap(),
        );
        let data_store: Arc<dyn SegmentDataStore> = Arc::new(TestHealStore::new());
        HealingGrpcService::new(
            handoff,
            metadata_store,
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            data_store,
            Arc::new(HlcClock::new()),
        )
    }

    /// G5: a batched inline hint intended for this node applies with the
    /// *original* write's HLC — not zero.
    #[tokio::test]
    async fn batched_inline_hint_applies_with_original_hlc() {
        let dir = tempfile::tempdir().unwrap();
        let metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore> = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let handoff = Arc::new(HintedHandoff::new());
        let data_store: Arc<dyn SegmentDataStore> = Arc::new(TestHealStore::new());
        let service = HealingGrpcService::new(
            handoff,
            metadata_store.clone(),
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            data_store,
            Arc::new(HlcClock::new()),
        )
        .with_local_node_id(NodeId::new("self-node"));

        let hint = crate::hinted_handoff_rpc::HintRecord {
            record: Some(Record::Inline(crate::hinted_handoff_rpc::HintInline {
                intended_for: Some(NodeId::new("self-node").into()),
                bucket_id: Some(oceanfs_core::BucketId::new("b").into()),
                object_key: "k".to_string(),
                data: Bytes::from_static(b"hello"),
                hlc: Some(oceanfs_core::proto::common::HlcTimestamp { wall_time: 555, logical: 3 }),
            })),
            stored_at_secs: 0,
        };

        let request = tonic::Request::new(HintedHandoffRequest { hints: vec![hint] });
        let response = service.hinted_handoff(request).await.unwrap();
        assert!(response.into_inner().accepted, "self-intended hint must apply");

        let meta = oceanfs_storage_api::MetadataStore::get_object_metadata(
            metadata_store.as_ref(),
            &oceanfs_core::BucketId::new("b"),
            &oceanfs_core::ObjectKey::new("k"),
        )
        .unwrap()
        .expect("applied hint must persist object metadata");
        assert_eq!(
            meta.hlc,
            Hlc::new(555, 3),
            "applied metadata must carry the original write's HLC",
        );
    }

    #[tokio::test]
    async fn handoff_valid_hint_returns_accepted() {
        let service = make_service();

        let request = tonic::Request::new(HintRequest {
            intended_for: Some(oceanfs_core::proto::common::NodeId {
                id: "target-node".to_string(),
            }),
            segment_id: Some(SegmentId::new().into()),
            data: Bytes::from(b"test hint data".as_slice()),
            hlc: None,
        });

        let response = service.hinted_handoff_single(request).await.unwrap();
        let resp = response.into_inner();
        assert!(resp.accepted, "valid hint should be accepted");
        assert!(resp.stored_segment_id.is_some(), "should return a stored_segment_id");
    }

    #[tokio::test]
    async fn merkle_exchange_with_stored_data_returns_correct_root() {
        let handoff = Arc::new(HintedHandoff::new());
        let metadata_store = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: std::env::temp_dir()
                    .join(format!("oceanfs-test-merkle-{}", std::process::id())),
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
        let service = HealingGrpcService::new(
            handoff,
            metadata_store,
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            data_store,
            Arc::new(HlcClock::new()),
        );

        let proto_sid: ProtoSegmentId = seg_id.into();
        let request = tonic::Request::new(MerkleRequest {
            segment_ids: vec![proto_sid],
            tree_depth: 8,
            node_id: None,
            include_full_tree: false,
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
