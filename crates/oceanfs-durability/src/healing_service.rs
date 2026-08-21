//! Healing gRPC service.
//!
//! Handles `HealingRpc` RPCs for hinted handoff, Merkle exchange,
//! shard fetch for EC reconstruction, and repaired shard push.

use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use oceanfs_core::{Hlc, HlcClock, NodeId, SegmentId};

/// Converts a core [`Hlc`] to the proto timestamp for the hint fetch
/// response header.
fn proto_hlc(hlc: Hlc) -> oceanfs_core::proto::common::HlcTimestamp {
    oceanfs_core::proto::common::HlcTimestamp { wall_time: hlc.wall_time(), logical: hlc.logical() }
}
use tonic::{Request, Response, Status};

use crate::{
    healing_rpc::{
        healing_rpc_server::HealingRpc, FetchHintObjectChunk, FetchHintObjectRequest,
        FetchShardChunk, FetchShardRequest, HintRequest, HintResponse, MerkleRequest,
        MerkleResponse, PushRepairedShardRequest, PushRepairedShardResponse,
    },
    hinted_handoff_rpc::{hint_record::Record, HintedHandoffRequest, HintedHandoffResponse},
    SegmentDataStore,
};

/// Fetches an object's CURRENT state from an origin node.
///
/// The hinted-handoff receiver uses this to materialize hints BY KEY:
/// it asks the origin "what is the current state of K?" and applies
/// the answer with HLC-LWW. The origin's metadata is the truth — if
/// the hinted version was deleted or superseded (its segment data
/// GC'd/reaped), the current state is exactly what the recipient must
/// converge to; replaying the stale version would resurrect a deleted
/// object or regress a newer write.
#[async_trait::async_trait]
pub trait HintObjectFetcher: Send + Sync {
    /// Fetches the object's current logical data + metadata from
    /// `origin` (the sender's gRPC listener address). `Ok(None)` when
    /// the object no longer exists (deleted/tombstoned).
    async fn fetch_object(
        &self,
        origin: SocketAddr,
        bucket: &oceanfs_core::BucketId,
        key: &str,
    ) -> Result<Option<(oceanfs_core::ObjectMetadata, Bytes)>, String>;
}

/// Reads an object's CURRENT state on the origin (server side of the
/// hint fetch). The logical data is resolved through the node's read
/// path (metadata → chunks → segment reads → decompression), so the
/// receiver gets exactly what a GET would return.
#[async_trait::async_trait]
pub trait HintObjectReader: Send + Sync {
    /// Returns the object's current metadata + logical data, or `None`
    /// when the object no longer exists (deleted/tombstoned).
    async fn read_object(
        &self,
        bucket: &oceanfs_core::BucketId,
        key: &oceanfs_core::ObjectKey,
    ) -> Result<Option<(oceanfs_core::ObjectMetadata, Bytes)>, String>;
}

/// Applies a hinted object's data to the LOCAL store through the
/// normal segment pipeline (tier selection + append + WAL + seal +
/// metadata row with REAL chunk refs and the hint's HLC).
///
/// The alternative — the healing service storing the fetched data
/// inline in the objects CF — ballooned the metadata with 16 MiB
/// blobs and collapsed the orphan reaper's full-metadata scan
/// (build_referenced_set runs once per cycle AND once per orphan for
/// the double-check) — the fleet disk-fill root cause. Segment-
/// applied objects get the normal lifecycle: the row references the
/// local segment, the reaper sees it referenced, the tombstone
/// captures the local chunks, the GC compacts it.
#[async_trait::async_trait]
pub trait HintObjectApplier: Send + Sync {
    /// Appends `data` to a local segment (or stores it inline for the
    /// inline tier) and persists the object row with `hlc`. Returns
    /// the applied metadata.
    ///
    /// # Errors
    ///
    /// Returns a string error when the append or metadata write fails
    /// — the sender keeps the hint and retries.
    async fn apply_object(
        &self,
        bucket: &oceanfs_core::BucketId,
        key: &oceanfs_core::ObjectKey,
        data: bytes::Bytes,
        hlc: Hlc,
        created_at: i64,
    ) -> Result<oceanfs_core::ObjectMetadata, String>;
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
    /// Fetcher for materializing hints from their origin (key-based).
    /// `None` (tests) degrades to not accepting the hint — the sender
    /// retries.
    hint_object_fetcher: Option<Arc<dyn HintObjectFetcher>>,
    /// Server-side object reader for the hint fetch RPC (wired by the
    /// composition root to the node's read path).
    hint_object_reader: Option<Arc<dyn HintObjectReader>>,
    /// Local applier for hinted object data (wired by the composition
    /// root to the node's segment pipeline). `None` (tests) degrades
    /// to the historical inline-in-metadata storage.
    hint_object_applier: Option<Arc<dyn HintObjectApplier>>,
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
            hint_object_fetcher: None,
            hint_object_reader: None,
            hint_object_applier: None,
        }
    }

    /// Sets this node's identifier so that self-intended hints are
    /// applied instead of buffered.
    #[must_use]
    pub fn with_local_node_id(mut self, node_id: NodeId) -> Self {
        self.local_node_id = Some(node_id);
        self
    }

    /// Installs the hint materializer (composition root).
    ///
    /// Hints are materialized BY KEY: the receiver asks the origin for
    /// the object's CURRENT state and applies it with HLC-LWW (the
    /// origin's metadata is the truth — a GC'd/reaped hinted version
    /// was deleted or superseded, so the current state is what the
    /// recipient must converge to). Without a fetcher, hints are not
    /// accepted — the sender retries (tests).
    #[must_use]
    pub fn with_hint_object_fetcher(mut self, fetcher: Arc<dyn HintObjectFetcher>) -> Self {
        self.hint_object_fetcher = Some(fetcher);
        self
    }

    /// Installs the server-side object reader for the hint fetch RPC
    /// (composition root — the node's read path).
    #[must_use]
    pub fn with_hint_object_reader(mut self, reader: Arc<dyn HintObjectReader>) -> Self {
        self.hint_object_reader = Some(reader);
        self
    }

    /// Installs the local applier for hinted object data (composition
    /// root — the node's segment pipeline).
    #[must_use]
    pub fn with_hint_object_applier(mut self, applier: Arc<dyn HintObjectApplier>) -> Self {
        self.hint_object_applier = Some(applier);
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
    /// Applies a delayed write with HLC-LWW: the hint's version wins
    /// only when it is newer than (or equal to) the LOCAL state for the
    /// key (metadata or tombstone). A stale hint is discarded — applying
    /// it would regress a newer write or resurrect a deleted object.
    ///
    /// The applied metadata stores the data inline (self-contained —
    /// the receiver needs no segment access of its own).
    async fn apply_hint_object(
        &self,
        bucket: &oceanfs_core::BucketId,
        object_key: &str,
        meta: oceanfs_core::ObjectMetadata,
        data: Bytes,
    ) {
        // LWW: local metadata newer → discard.
        if let Ok(Some(local)) = self.metadata_store.get_object_metadata(bucket, &meta.object_key) {
            if local.hlc > meta.hlc {
                tracing::debug!(
                    bucket = %bucket,
                    key = %object_key,
                    local_wall = local.hlc.wall_time(),
                    hint_wall = meta.hlc.wall_time(),
                    "hint discarded: local version newer (LWW)"
                );
                return;
            }
        }
        // LWW: local tombstone newer → discard.
        if let Ok(Some(tombstone)) = self.metadata_store.get_tombstone(bucket, &meta.object_key) {
            if tombstone.hlc > meta.hlc {
                tracing::debug!(
                    bucket = %bucket,
                    key = %object_key,
                    tombstone_wall = tombstone.hlc.wall_time(),
                    hint_wall = meta.hlc.wall_time(),
                    "hint discarded: local tombstone newer (LWW)"
                );
                return;
            }
        }

        let data_len = data.len();
        // The composition root wires the segment pipeline (when set):
        // the data is appended to a local segment through the normal
        // write path and the row carries the REAL local chunk refs —
        // the object gets the normal lifecycle (reaper sees it
        // referenced, the tombstone captures the local chunks, the GC
        // compacts). Without the applier (tests) the historical
        // inline-in-metadata storage is used.
        let result = match &self.hint_object_applier {
            Some(applier) => {
                applier
                    .apply_object(bucket, &meta.object_key, data.clone(), meta.hlc, meta.created_at)
                    .await
            }
            None => {
                let applied = oceanfs_core::ObjectMetadata {
                    object_key: meta.object_key.clone(),
                    size: meta.size,
                    blake3_hash: meta.blake3_hash,
                    chunks: smallvec::SmallVec::new(),
                    inline_data: Some(data.clone()),
                    created_at: meta.created_at,
                    hlc: meta.hlc,
                };
                match oceanfs_storage_api::MetadataStore::put_object(
                    self.metadata_store.as_ref(),
                    bucket,
                    applied,
                ) {
                    Ok(()) => Ok(oceanfs_core::ObjectMetadata {
                        object_key: meta.object_key,
                        size: meta.size,
                        blake3_hash: meta.blake3_hash,
                        chunks: smallvec::SmallVec::new(),
                        inline_data: Some(data),
                        created_at: meta.created_at,
                        hlc: meta.hlc,
                    }),
                    Err(e) => Err(e.to_string()),
                }
            }
        };
        match result {
            Ok(_) => {
                tracing::info!(
                    bucket = %bucket,
                    key = %object_key,
                    size = data_len,
                    hlc_wall = meta.hlc.wall_time(),
                    hlc_logical = meta.hlc.logical(),
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

    async fn apply_inline_hint(
        &self,
        bucket: oceanfs_core::BucketId,
        object_key: String,
        data: Bytes,
        hlc: Hlc,
    ) {
        let meta = oceanfs_core::ObjectMetadata {
            object_key: oceanfs_core::ObjectKey::new(&object_key),
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
        self.apply_hint_object(&bucket, &object_key, meta, data).await;
    }

    /// Applies a hinted DELETE (a tombstone) with HLC-LWW: the delete
    /// wins only when it is newer than the LOCAL state for the key
    /// (metadata or tombstone). A stale delete is discarded — applying
    /// it would erase a newer write.
    ///
    /// Deletes are hinted exactly like writes (the write coordinator
    /// hints replicas that missed a delete). Without it a node that
    /// missed a delete keeps its stale row forever, and the sender-side
    /// obsolete pre-check then drops later write hints for keys that
    /// are still live elsewhere — the churn divergence this fixes.
    fn apply_hint_delete(&self, bucket: &oceanfs_core::BucketId, object_key: &str, hlc: Hlc) {
        // LWW: local metadata newer → the object was rewritten after
        // the delete → discard (erasing it would regress the newer
        // write).
        let key = oceanfs_core::ObjectKey::new(object_key);
        if let Ok(Some(local)) = self.metadata_store.get_object_metadata(bucket, &key) {
            if local.hlc > hlc {
                tracing::debug!(
                    bucket = %bucket,
                    key = %object_key,
                    local_wall = local.hlc.wall_time(),
                    hint_wall = hlc.wall_time(),
                    "hint delete discarded: local version newer (LWW)"
                );
                return;
            }
        }
        // LWW: local tombstone at least as new → already deleted.
        if let Ok(Some(tombstone)) = self.metadata_store.get_tombstone(bucket, &key) {
            if tombstone.hlc >= hlc {
                tracing::debug!(
                    bucket = %bucket,
                    key = %object_key,
                    tombstone_wall = tombstone.hlc.wall_time(),
                    hint_wall = hlc.wall_time(),
                    "hint delete discarded: local tombstone newer (LWW)"
                );
                return;
            }
        }

        match oceanfs_storage_api::MetadataStore::delete_object(
            self.metadata_store.as_ref(),
            bucket,
            &key,
            hlc,
        ) {
            Ok(()) => {
                tracing::info!(
                    bucket = %bucket,
                    key = %object_key,
                    hlc_wall = hlc.wall_time(),
                    hlc_logical = hlc.logical(),
                    "applied hinted delete locally"
                );
            }
            Err(e) => {
                tracing::warn!(
                    bucket = %bucket,
                    key = %object_key,
                    error = %e,
                    "failed to apply hinted delete locally"
                );
            }
        }
    }
}

#[tonic::async_trait]
impl HealingRpc for HealingGrpcService {
    type FetchShardStream = tokio_stream::wrappers::ReceiverStream<Result<FetchShardChunk, Status>>;
    type FetchHintObjectStream =
        tokio_stream::wrappers::ReceiverStream<Result<FetchHintObjectChunk, Status>>;

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
        // Local segment-ref hints deferred to the parallel fetch pass
        // (see below): serial fetches of a full batch exceed the
        // sender's delivery timeout and make it re-enqueue — thrash.
        let mut pending_fetches: Vec<(oceanfs_core::BucketId, String)> = Vec::new();

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
                        )
                        .await;
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
                    let object_key = seg_ref.object_key.clone();

                    // A hint intended for THIS node is a delayed write:
                    // materialize it BY KEY — ask the origin for the
                    // object's CURRENT state and apply it with HLC-LWW.
                    // Segment-ref hints deliberately do NOT carry the
                    // data inline, so hints stay small even for
                    // multipart/GB blobs; and materializing the current
                    // state (not the hinted version) is the correct
                    // semantics when the origin's data has been
                    // GC'd/reaped — the hinted version was deleted or
                    // superseded, and replaying it would resurrect a
                    // deleted object or regress a newer write.
                    //
                    // The fetch is deferred to the PARALLEL pass below:
                    // each fetch is an independent network roundtrip,
                    // and processing a 256-hint batch serially exceeds
                    // the sender's delivery timeout (the sender then
                    // re-enqueues and redelivers — the batch thrashes).
                    if self.is_local_hint(&intended_for) {
                        if sender_grpc_addr.is_some() && self.hint_object_fetcher.is_some() {
                            pending_fetches.push((bucket, object_key));
                        } else {
                            tracing::warn!(
                                intended_for = %intended_for,
                                "hint intended for self but no origin/fetcher \
                                 available; NOT accepted — the sender will retry"
                            );
                        }
                        // Do NOT fall through to the legacy relay buffer
                        // (which nothing drains — accepting would make
                        // the sender truncate its WAL and lose the
                        // hint). Leave the hint unaccepted so the batch
                        // returns accepted=false and the sender
                        // re-enqueues + retries.
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
                Some(Record::Delete(delete)) => {
                    let intended_for = delete
                        .intended_for
                        .clone()
                        .map(NodeId::from)
                        .unwrap_or_else(|| NodeId::new("unknown"));

                    // G5: the hint carries the original delete's HLC.
                    let hlc = delete
                        .hlc
                        .as_ref()
                        .map(|h| Hlc::new(h.wall_time, h.logical))
                        .unwrap_or_else(Hlc::zero);
                    // Receive rule (G2): merge the remote timestamp.
                    self.hlc_clock.update(hlc);

                    let bucket = delete
                        .bucket_id
                        .clone()
                        .map(oceanfs_core::BucketId::from)
                        .unwrap_or_else(|| oceanfs_core::BucketId::new("default"));
                    let object_key = delete.object_key.clone();

                    // A hint intended for THIS node is a delayed delete:
                    // apply the tombstone locally with HLC-LWW (a newer
                    // local write or a newer local tombstone discards it).
                    if self.is_local_hint(&intended_for) {
                        self.apply_hint_delete(&bucket, &object_key, hlc);
                        accepted_count += 1;
                        continue;
                    }

                    // Misrouted delete hint: the legacy relay cannot
                    // carry a delete, so do NOT accept it — the sender
                    // re-enqueues and retries until the hint reaches
                    // its intended node (never ack what you can't
                    // materialize).
                    tracing::warn!(
                        intended_for = %intended_for,
                        bucket = %bucket,
                        key = %object_key,
                        "delete hint arrived at the wrong node; \
                         NOT accepted — the sender will retry"
                    );
                }
                None => {
                    tracing::warn!("batched hint with no record variant; skipping");
                }
            }
        }

        // ── Parallel fetch pass ─────────────────────────────────
        // Materialize the deferred local segment-ref hints. Each fetch
        // is an independent network roundtrip; a bounded concurrency
        // (16 in flight) keeps a full batch inside the sender's
        // delivery timeout without starving the runtime.
        if !pending_fetches.is_empty() {
            let (Some(origin), Some(fetcher)) =
                (sender_grpc_addr, self.hint_object_fetcher.clone())
            else {
                tracing::warn!(
                    count = pending_fetches.len(),
                    "deferred segment-ref hints have no origin/fetcher; \
                     NOT accepted — the sender will retry"
                );
                // Leave them unaccepted: the batch returns
                // accepted=false and the sender re-enqueues.
                return Ok(Response::new(HintedHandoffResponse {
                    accepted: accepted_count == hint_count,
                    accepted_count,
                }));
            };

            const FETCH_CONCURRENCY: usize = 16;
            let semaphore = Arc::new(tokio::sync::Semaphore::new(FETCH_CONCURRENCY));
            let mut set = tokio::task::JoinSet::new();
            for (bucket, key) in pending_fetches {
                let fetcher = Arc::clone(&fetcher);
                let semaphore = Arc::clone(&semaphore);
                set.spawn(async move {
                    let _permit = match semaphore.acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => return (bucket, key, Err("fetch semaphore closed".to_string())),
                    };
                    let result = fetcher.fetch_object(origin, &bucket, &key).await;
                    (bucket, key, result)
                });
            }
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok((bucket, key, Ok(Some((meta, data))))) => {
                        self.apply_hint_object(&bucket, &key, meta, data).await;
                        accepted_count += 1;
                    }
                    Ok((_bucket, _key, Ok(None))) => {
                        // The object no longer exists on the origin —
                        // the hint resolved (the delete/supersede won).
                        // Accept it: the recipient must not receive the
                        // stale version.
                        tracing::debug!(
                            bucket = %_bucket,
                            key = %_key,
                            "hint resolved: object absent on origin"
                        );
                        accepted_count += 1;
                    }
                    Ok((bucket, key, Err(e))) => {
                        tracing::warn!(
                            bucket = %bucket,
                            key = %key,
                            error = %e,
                            "failed to fetch hint object state from origin; \
                             NOT accepted — the sender will retry"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "hint fetch task failed; NOT accepted — the sender will retry"
                        );
                    }
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

    async fn fetch_hint_object(
        &self,
        request: Request<FetchHintObjectRequest>,
    ) -> Result<Response<Self::FetchHintObjectStream>, Status> {
        let req = request.into_inner();
        let bucket = req
            .bucket_id
            .clone()
            .map(oceanfs_core::BucketId::from)
            .unwrap_or_else(|| oceanfs_core::BucketId::new("default"));
        let key = oceanfs_core::ObjectKey::new(&req.object_key);

        let reader = self
            .hint_object_reader
            .as_ref()
            .ok_or_else(|| Status::unavailable("hint object reader not installed"))?;

        // The object's CURRENT state via the node's read path (the
        // metadata is the truth — a GC'd/reaped hinted version was
        // deleted or superseded; the current state is what the
        // recipient must converge to).
        let (meta, data) = match reader.read_object(&bucket, &key).await {
            Ok(Some((meta, data))) => (meta, data),
            Ok(None) => {
                // Absent: one chunk with present=false (hlc = tombstone
                // HLC when available).
                let tombstone_hlc = self
                    .metadata_store
                    .get_tombstone(&bucket, &key)
                    .ok()
                    .flatten()
                    .map(|t| t.hlc)
                    .unwrap_or_else(Hlc::zero);
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                let _ = tx
                    .send(Ok(FetchHintObjectChunk {
                        chunk_index: 0,
                        data: Bytes::new(),
                        present: false,
                        hlc: Some(proto_hlc(tombstone_hlc)),
                        size: 0,
                        blake3_hash: Bytes::new(),
                    }))
                    .await;
                return Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)));
            }
            Err(e) => {
                return Err(Status::internal(format!("failed to read hint object state: {e}")));
            }
        };

        // Stream the logical data in 64 KB chunks; the first chunk
        // carries the object's state (present + hlc + size).
        let chunk_size = 65536;
        let mut chunks: Vec<FetchHintObjectChunk> = (0..data.len())
            .step_by(chunk_size)
            .enumerate()
            .map(|(i, off)| {
                let end = (off + chunk_size).min(data.len());
                FetchHintObjectChunk {
                    chunk_index: i as u32,
                    data: data.slice(off..end),
                    present: true,
                    hlc: None,
                    size: 0,
                    blake3_hash: Bytes::new(),
                }
            })
            .collect();
        if let Some(first) = chunks.first_mut() {
            first.hlc = Some(proto_hlc(meta.hlc));
            first.size = meta.size;
            // Carry the stored hash so the receiver can verify the
            // reassembled stream (see the proto field doc).
            if let Some(hash) = &meta.blake3_hash {
                first.blake3_hash = Bytes::copy_from_slice(hash.as_bytes());
            }
        }

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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unnecessary_cast,
    clippy::useless_conversion
)]
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

    /// A batched DELETE hint intended for this node applies the
    /// tombstone with the original delete's HLC (G5) — and wins LWW
    /// against an older local write.
    #[tokio::test]
    async fn batched_delete_hint_applies_tombstone_with_original_hlc() {
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

        let bucket = oceanfs_core::BucketId::new("b");
        let key = oceanfs_core::ObjectKey::new("k");

        // Seed an OLD write (hlc 100) — the delete (hlc 200) must win.
        oceanfs_storage_api::MetadataStore::put_object(
            metadata_store.as_ref(),
            &bucket,
            oceanfs_core::ObjectMetadata {
                object_key: key.clone(),
                size: 4,
                blake3_hash: None,
                chunks: smallvec::SmallVec::new(),
                inline_data: Some(Bytes::from_static(b"data")),
                created_at: 0,
                hlc: Hlc::new(100, 0),
            },
        )
        .unwrap();

        let hint = crate::hinted_handoff_rpc::HintRecord {
            record: Some(Record::Delete(crate::hinted_handoff_rpc::HintDelete {
                intended_for: Some(NodeId::new("self-node").into()),
                bucket_id: Some(bucket.clone().into()),
                object_key: "k".to_string(),
                hlc: Some(oceanfs_core::proto::common::HlcTimestamp { wall_time: 200, logical: 0 }),
            })),
            stored_at_secs: 0,
        };

        let request = tonic::Request::new(HintedHandoffRequest { hints: vec![hint] });
        let response = service.hinted_handoff(request).await.unwrap();
        assert!(response.into_inner().accepted, "self-intended delete hint must apply");

        let meta = oceanfs_storage_api::MetadataStore::get_object_metadata(
            metadata_store.as_ref(),
            &bucket,
            &key,
        )
        .unwrap();
        assert!(meta.is_none(), "the delete must remove the object row");
        let tombstone = oceanfs_storage_api::MetadataStore::get_tombstone(
            metadata_store.as_ref(),
            &bucket,
            &key,
        )
        .unwrap();
        assert!(tombstone.is_some(), "the delete must persist a tombstone");
        assert_eq!(
            tombstone.unwrap().hlc,
            Hlc::new(200, 0),
            "the tombstone must carry the original delete's HLC",
        );
    }

    /// LWW: a DELETE hint OLDER than the local write is discarded —
    /// applying it would erase a newer object.
    #[tokio::test]
    async fn batched_delete_hint_stale_against_newer_write_discarded() {
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

        let bucket = oceanfs_core::BucketId::new("b");
        let key = oceanfs_core::ObjectKey::new("k");

        // Seed a NEWER write (hlc 300) — the delete (hlc 200) is stale.
        oceanfs_storage_api::MetadataStore::put_object(
            metadata_store.as_ref(),
            &bucket,
            oceanfs_core::ObjectMetadata {
                object_key: key.clone(),
                size: 4,
                blake3_hash: None,
                chunks: smallvec::SmallVec::new(),
                inline_data: Some(Bytes::from_static(b"data")),
                created_at: 0,
                hlc: Hlc::new(300, 0),
            },
        )
        .unwrap();

        let hint = crate::hinted_handoff_rpc::HintRecord {
            record: Some(Record::Delete(crate::hinted_handoff_rpc::HintDelete {
                intended_for: Some(NodeId::new("self-node").into()),
                bucket_id: Some(bucket.clone().into()),
                object_key: "k".to_string(),
                hlc: Some(oceanfs_core::proto::common::HlcTimestamp { wall_time: 200, logical: 0 }),
            })),
            stored_at_secs: 0,
        };

        let request = tonic::Request::new(HintedHandoffRequest { hints: vec![hint] });
        let response = service.hinted_handoff(request).await.unwrap();
        assert!(response.into_inner().accepted, "the hint is still accepted (resolved)");

        let meta = oceanfs_storage_api::MetadataStore::get_object_metadata(
            metadata_store.as_ref(),
            &bucket,
            &key,
        )
        .unwrap();
        assert!(meta.is_some(), "a stale delete hint must NOT erase the newer local write",);
    }

    /// The hint object fetch verifies the reassembled stream against the
    /// origin's advertised size — a truncated stream must NOT be applied
    /// as a full version (it would win LWW and spread unrecorded data to
    /// every node fetching from the same origin).
    #[tokio::test]
    async fn fetch_object_rejects_truncated_stream() {
        use oceanfs_core::RpcConfig;
        use oceanfs_network::ConnectionPool;
        use tokio_stream::wrappers::TcpListenerStream;

        use crate::healing_rpc::healing_rpc_server::HealingRpcServer;

        // A reader whose advertised size disagrees with the data it
        // returns — simulates a stream truncated mid-way on the wire.
        struct TruncatedReader;
        #[async_trait::async_trait]
        impl HintObjectReader for TruncatedReader {
            async fn read_object(
                &self,
                _bucket: &oceanfs_core::BucketId,
                _key: &oceanfs_core::ObjectKey,
            ) -> Result<Option<(oceanfs_core::ObjectMetadata, Bytes)>, String> {
                Ok(Some((
                    oceanfs_core::ObjectMetadata {
                        object_key: oceanfs_core::ObjectKey::new("k"),
                        size: 65536 * 4, // advertised: 256 KB
                        blake3_hash: None,
                        chunks: smallvec::SmallVec::new(),
                        inline_data: None,
                        created_at: 0,
                        hlc: oceanfs_core::Hlc::new(500, 0),
                    },
                    Bytes::from(vec![0xAB; 65536]), // actually only 64 KB
                )))
            }
        }

        let handoff = Arc::new(HintedHandoff::new());
        let metadata_store = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: std::env::temp_dir()
                    .join(format!("oceanfs-test-fetch-{}", std::process::id())),
                ..Default::default()
            })
            .unwrap(),
        );
        let data_store: Arc<dyn SegmentDataStore> = Arc::new(TestHealStore::new());
        let service = HealingGrpcService::new(
            handoff,
            metadata_store,
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            data_store,
            Arc::new(HlcClock::new()),
        )
        .with_hint_object_reader(Arc::new(TruncatedReader));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(HealingRpcServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let fetcher = crate::hinted_handoff::GrpcHintObjectFetcher::new(pool);
        let result = fetcher.fetch_object(addr, &oceanfs_core::BucketId::new("b"), "k").await;
        assert!(
            result.is_err(),
            "a size-mismatched (truncated) stream must be rejected, got: {result:?}",
        );
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
