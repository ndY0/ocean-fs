//! Healing gRPC service.
//!
//! Handles `HealingRpc` RPCs for hinted handoff, Merkle exchange,
//! shard fetch for EC reconstruction, and repaired shard push.

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use oceanfs_core::{Hlc, HlcClock, NodeId, RemappedChunk, SegmentId, SegmentRemapAlias};
use oceanfs_storage_api::SegmentDataStore;
use tonic::{Request, Response, Status};

use crate::scheduler::DurabilityBudget;

/// Converts a core [`Hlc`] to the proto timestamp for the hint fetch
/// response header.
fn proto_hlc(hlc: Hlc) -> oceanfs_core::proto::common::HlcTimestamp {
    oceanfs_core::proto::common::HlcTimestamp { wall_time: hlc.wall_time(), logical: hlc.logical() }
}

use crate::{
    healing_rpc::{
        healing_rpc_server::HealingRpc, FetchHintObjectChunk, FetchHintObjectRequest,
        FetchShardChunk, FetchShardRequest, HintRequest, HintResponse, LossAck, LossAnnouncement,
        MerkleRequest, MerkleResponse, PushRepairedShardRequest, PushRepairedShardResponse,
        RemapAck, RequestReReplicationRequest, RequestReReplicationResponse, SegmentRemap,
    },
    hinted_handoff_rpc::{hint_record::Record, HintedHandoffRequest, HintedHandoffResponse},
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

/// A single re-replication repair request enqueued by the loss
/// announcement handler (g3, ADR-0029 §D4 fast path) or the
/// reconciliation loop (g4, ADR-0029 §D4 pull safety net).
///
/// The request is routing intent only (ADR-0030 Decision 2): it names
/// the segment and the LIVE holders the acquiring node may fetch the
/// data from, plus which detector drove the repair (pacing/metrics).
/// The holder side dispatches it to the acquiring target via the
/// `RequestReReplication` RPC; the target's `ReRepWorker` (g5) pulls
/// the segment, writes it through its pool-aware store, and stamps
/// `storage_locations`. This is distinct from the heal queue (which
/// repairs EC shard corruption).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{NodeId, SizeTier};
/// use oceanfs_durability::healing_service::{ReRepRequest, RepairReason};
///
/// let req = ReRepRequest {
///     origin: NodeId::new("node-a"),
///     segment_id: oceanfs_core::SegmentId::new(),
///     holders: vec![NodeId::new("node-b")],
///     reason: RepairReason::Announcement,
///     retry_count: 0,
///     merkle_root: None,
///     tier: SizeTier::Standard,
///     ec_k: 1,
///     ec_m: 0,
/// };
/// assert_eq!(req.origin, NodeId::new("node-a"));
/// assert_eq!(req.reason, RepairReason::Announcement);
/// assert_eq!(req.tier, SizeTier::Standard);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReRepRequest {
    /// The node that announced the loss (whose pool died), or the
    /// reconciliation loop's self id.
    pub origin: NodeId,
    /// The segment needing an additional live copy.
    pub segment_id: SegmentId,
    /// LIVE holders the acquiring target may fetch the segment data
    /// from (the dispatcher filters unavailable nodes before sending).
    pub holders: Vec<NodeId>,
    /// Which detector drove this repair (metrics/pacing).
    pub reason: RepairReason,
    /// Retry attempt counter (the worker re-enqueues with an
    /// incremented count on failure; bounded by `ReRepConfig::retry_limit`).
    pub retry_count: u32,
    /// The segment's seal-time merkle root (the dispatcher reads it from
    /// its own registry entry). The worker verifies the fetched data
    /// against it — a truncated/corrupt transfer is rejected (ADR-0030).
    /// `None` (tests / legacy enqueuers) skips the verification.
    pub merkle_root: Option<oceanfs_core::HashOutput>,
    /// The segment's seal-time size tier (the enqueuer reads it from its
    /// own registry entry). The acquiring worker registers the pulled
    /// copy with THIS tier — the source's real shape, not a default.
    pub tier: oceanfs_core::SizeTier,
    /// The segment's seal-time EC data-shard count (same provenance).
    pub ec_k: u8,
    /// The segment's seal-time EC parity-shard count (same provenance).
    pub ec_m: u8,
}

/// Which detector drove a re-replication repair (ADR-0029 §D6 — the
/// worker reports it as `oceanfs_repair_queue_depth{priority}`).
///
/// # Examples
///
/// ```
/// use oceanfs_durability::healing_service::RepairReason;
///
/// let reason = RepairReason::Reconciliation;
/// assert_eq!(u32::from(reason), 2);
/// assert_eq!(RepairReason::from(2), RepairReason::Reconciliation);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepairReason {
    /// The g3 loss-announcement fast path.
    Announcement,
    /// The g4 periodic reconciliation safety net.
    Reconciliation,
}

impl From<RepairReason> for u32 {
    fn from(reason: RepairReason) -> Self {
        match reason {
            RepairReason::Announcement => 1,
            RepairReason::Reconciliation => 2,
        }
    }
}

impl From<u32> for RepairReason {
    fn from(value: u32) -> Self {
        match value {
            1 => RepairReason::Announcement,
            2 => RepairReason::Reconciliation,
            _ => RepairReason::Reconciliation,
        }
    }
}

/// Receives verified re-replication requests from the loss-announcement
/// handler (g3). The composition root wires this to the ReRepWorker's
/// bounded queue (g5); `None` (tests / minimal embeddings) means the
/// handler still verifies + acks but enqueues nothing.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_core::NodeId;
/// use oceanfs_durability::healing_service::{ReRepRequest, RepairSink};
///
/// struct Sink;
/// #[async_trait::async_trait]
/// impl RepairSink for Sink {
///     async fn enqueue(&self, _req: ReRepRequest) -> Result<(), String> {
///         Ok(())
///     }
/// }
/// let sink: Arc<dyn RepairSink> = Arc::new(Sink);
/// assert!(Arc::strong_count(&sink) >= 1);
/// ```
#[async_trait::async_trait]
pub trait RepairSink: Send + Sync {
    /// Accepts one repair request.
    ///
    /// # Errors
    ///
    /// Returns a string error when the queue is at capacity or closed —
    /// the announcement is best-effort (the g4 reconciliation loop is
    /// the mandatory failsafe).
    async fn enqueue(&self, request: ReRepRequest) -> Result<(), String>;
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
    /// Receiver-side compaction remap alias (`old → new`). The
    /// `AnnounceRemap` handler records it so the append/read-repair
    /// handlers translate late chunk refs at write time, and the
    /// `batch_write` re-point below rewrites already-persisted rows.
    /// `None` (tests) means remaps are verified + acknowledged but
    /// recorded nowhere (a no-op — the g4 reconciliation failsafe still
    /// covers the divergence).
    remap_alias: Option<Arc<SegmentRemapAlias>>,
    /// Lifecycle coordinator (wired by the composition root). The
    /// `AnnounceRemap` handler deletes the stale replica THROUGH the
    /// machine (`request_delete`) after re-pointing — the ADR-0025
    /// delete-before-unlink invariant, never a direct registry write.
    /// `None` (tests) means the stale replica is left for the receiver's
    /// own GC to reclaim (fully-dead path).
    lifecycle_coordinator:
        Option<Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>>,
    /// Re-replication repair sink (g3 → g5). `None` (tests) verifies +
    /// acks but enqueues nothing.
    repair_sink: Option<Arc<dyn RepairSink>>,
    /// Re-replication request sink (g5 → target). The `RequestReReplication`
    /// RPC handler enqueues into the LOCAL `ReRepWorker` queue here; the
    /// worker pulls + writes + stamps (ADR-0030). `None` (tests) acks but
    /// enqueues nothing.
    replication_request_sink: Option<Arc<dyn RepairSink>>,
    /// The two-tier budget (ADR-0017 amendment). Inbound hint batches
    /// acquire a Tier-0 (repair) permit per batch when a budget is wired
    /// (composition root). `None` (tests) leaves the handler unbounded.
    repair_budget: Option<Arc<DurabilityBudget>>,
    /// g3 announcement receive counters (ADR-0029 §D4 observability).
    /// Incremented by the `announce_loss` / `announce_remap` handlers.
    announce_rx_total: oceanfs_core::Counter,
    announce_accepted_total: oceanfs_core::Counter,
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
            remap_alias: None,
            lifecycle_coordinator: None,
            repair_sink: None,
            replication_request_sink: None,
            repair_budget: None,
            announce_rx_total: oceanfs_core::Counter::new(
                "oceanfs_announcements_rx_total".into(),
                "Loss/remap announcements received".into(),
                oceanfs_core::LabelSet::empty(),
            ),
            announce_accepted_total: oceanfs_core::Counter::new(
                "oceanfs_announcements_accepted".into(),
                "Announced segments accepted for repair/remap".into(),
                oceanfs_core::LabelSet::empty(),
            ),
        }
    }

    /// Registers the g3 announcement counters with a registrar.
    pub fn register_metrics(&self, registrar: &dyn oceanfs_core::MetricRegistrar) {
        registrar.register_counter(self.announce_rx_total.clone());
        registrar.register_counter(self.announce_accepted_total.clone());
    }

    /// Sets this node's identifier so that self-intended hints are
    /// applied instead of buffered.
    #[must_use]
    pub fn with_local_node_id(mut self, node_id: NodeId) -> Self {
        self.local_node_id = Some(node_id);
        self
    }

    /// Wires the shared two-tier budget (ADR-0017 amendment): the
    /// batched hinted-handoff handler acquires a Tier-0 (repair) permit
    /// per batch. The composition root always calls this.
    #[must_use]
    pub fn with_repair_budget(mut self, budget: Arc<DurabilityBudget>) -> Self {
        self.repair_budget = Some(budget);
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

    /// Installs the receiver-side compaction remap alias (composition
    /// root). `AnnounceRemap` records `old → new` here so the append /
    /// read-repair handlers translate late chunk refs at write time.
    #[must_use]
    pub fn with_remap_alias(mut self, alias: Arc<SegmentRemapAlias>) -> Self {
        self.remap_alias = Some(alias);
        self
    }

    /// Installs the lifecycle coordinator (composition root). The remap
    /// handler deletes the stale replica through the machine
    /// (`request_delete` — ADR-0025 Decision 4).
    #[must_use]
    pub fn with_lifecycle_coordinator(
        mut self,
        coordinator: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator>,
    ) -> Self {
        self.lifecycle_coordinator = Some(coordinator);
        self
    }

    /// Installs the re-replication repair sink (composition root; g3 →
    /// g5). `AnnounceLoss` enqueues verified repairs here.
    #[must_use]
    pub fn with_repair_sink(mut self, sink: Arc<dyn RepairSink>) -> Self {
        self.repair_sink = Some(sink);
        self
    }

    /// Installs the re-replication request sink (composition root; g5 →
    /// target). The `RequestReReplication` handler enqueues into the
    /// local `ReRepWorker` queue here.
    #[must_use]
    pub fn with_replication_request_sink(mut self, sink: Arc<dyn RepairSink>) -> Self {
        self.replication_request_sink = Some(sink);
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

    /// Rewrites the locally-persisted object rows for the announced
    /// object-key list that reference the old (compacted) segment,
    /// translating each chunk ref through the repacked chunk table.
    ///
    /// `object_keys` is the ADR-0034 D5/2b payload the owner attached to
    /// the remap: the `(bucket, key)` of every live object it repacked.
    /// Each announced key is re-pointed via a point lookup; a key this
    /// holder does not own is skipped without error. Rows for keys NOT in
    /// the announced list are left untouched even when they reference the
    /// old segment — the owner does not vouch for them.
    ///
    /// The table maps `(old_offset, length) → new_offset`. A chunk whose
    /// `(offset, length)` is absent from the table is left untouched —
    /// it was NOT part of the repack (e.g. a chunk of a tombstoned
    /// object the compactor filtered out), so re-pointing it would be
    /// wrong.
    ///
    /// Returns the number of rewritten rows.
    fn repoint_objects(
        &self,
        old_segment_id: SegmentId,
        new_segment_id: SegmentId,
        chunk_table: &[RemappedChunk],
        object_keys: &[oceanfs_core::ContainedObject],
    ) -> std::io::Result<usize> {
        // Build the lookup: (old_offset, length) → new_offset.
        let mut table: HashMap<(u64, u32), u64> = HashMap::with_capacity(chunk_table.len());
        for c in chunk_table {
            table.insert((c.old_offset, c.length), c.new_offset);
        }
        // An empty table, or an empty object-key list (a peer on an older
        // binary in a mixed-version window — the owner sent only the
        // chunk table), re-points nothing: the alias recorded by the
        // `AnnounceRemap` handler still translates late chunk refs, and
        // the g4 reconciliation is the mandatory failsafe. We NEVER fall
        // back to scanning the objects CF.
        if table.is_empty() || object_keys.is_empty() {
            return Ok(0);
        }

        let mut ops: Vec<oceanfs_storage_api::BatchOp> = Vec::with_capacity(object_keys.len());
        let mut rewritten = 0usize;
        for co in object_keys {
            // Per-announced-key point read (bounded by objects in the
            // repacked segment × the fan-out, ADR-0034 D5/2b) — never an
            // objects-CF scan.
            let Ok(Some(obj)) = self.metadata_store.get_object_metadata(&co.bucket, &co.key) else {
                // This holder does not own the announced key — no-op.
                continue;
            };
            // Only rows that actually reference the old segment change.
            if !obj.chunks.iter().any(|c| c.segment_id == old_segment_id) {
                continue;
            }
            let mut new_chunks = smallvec::SmallVec::<[oceanfs_core::ChunkRef; 4]>::new();
            let mut changed = false;
            for chunk in &obj.chunks {
                if chunk.segment_id == old_segment_id {
                    if let Some(new_offset) = table.get(&(chunk.offset, chunk.length)) {
                        new_chunks.push(oceanfs_core::ChunkRef {
                            segment_id: new_segment_id,
                            offset: *new_offset,
                            length: chunk.length,
                            compressed: chunk.compressed,
                            logical_length: chunk.logical_length,
                        });
                        changed = true;
                        continue;
                    }
                    // Chunk not in the repack (tombstoned object) — keep
                    // the old ref; the object is deleted anyway.
                }
                new_chunks.push(*chunk);
            }
            if changed {
                let updated_meta = oceanfs_core::ObjectMetadata { chunks: new_chunks, ..obj };
                ops.push(oceanfs_storage_api::BatchOp::PutObject(
                    co.bucket.clone(),
                    co.key.clone(),
                    updated_meta,
                ));
                rewritten += 1;
            }
        }
        if !ops.is_empty() {
            self.metadata_store.batch_write(ops)?;
        }
        Ok(rewritten)
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

        // ADR-0017 amendment: the batch is a Tier-0 (repair) operation —
        // one permit per inbound hint batch bounds cross-RPC concurrency at
        // the shared node-wide repair budget. The old per-RPC fetch
        // semaphore below bounded nothing across concurrent calls (the
        // review anchor at :1030-1036 is closed by this shared gate).
        let _repair = match &self.repair_budget {
            Some(budget) => Some(budget.acquire_repair().await),
            None => None,
        };

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

            // [review][architecture][critical][resolved]
            // RESOLVED by ADR-0017 amendment 2026-09-06: the per-RPC
            // semaphore here bounded only WITHIN one batch, never across
            // calls. The handler now acquires one Tier-0 (repair) permit
            // from the shared `DurabilityBudget` for the whole batch (above),
            // so concurrent inbound hint batches are bounded node-wide by
            // `[durability].repair_max_active`. This intra-batch cap is
            // kept as within-operation parallelism (one permit, bounded
            // fetches inside it).
            // [end]
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
            // A missing `.dat` (Ok(None)) or a read error skips the
            // candidate (the peer keeps looking for a holder).
            let segment_data = match self.data_store.read_segment_data(&sid).await {
                Ok(Some(file)) => file.data,
                Ok(None) => continue,
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
        // A missing `.dat` (Ok(None)) is surfaced exactly like the
        // pre-f1 NotFound read error (internal status, same as every
        // read failure here) — a fetch of a segment this node does not
        // hold must fail so the caller tries another holder.
        let data = match self.data_store.read_segment_data(&segment_id).await {
            Ok(Some(file)) => file.data,
            Ok(None) => {
                return Err(Status::internal(format!(
                    "failed to read segment data for shard fetch: segment {segment_id} not found"
                )));
            }
            Err(e) => {
                return Err(Status::internal(format!(
                    "failed to read segment data for shard fetch: {e}"
                )));
            }
        };

        // Full-segment mode (ADR-0030 target-pull; g5): when offset 0 +
        // length 0, stream the ENTIRE data section — the re-replication
        // fetch (the worker materializes a whole copy on the target).
        // Otherwise, return the named shard's requested byte range (EC
        // reconstruction).
        let shard_data: Bytes = if req.offset == 0 && req.length == 0 {
            data
        } else {
            // Determine shard size from total data length and known k+m.
            // This is a simplification — in production, we'd look up
            // ec_k/ec_m from metadata.
            let total_shards = 6; // default k=4, m=2
            let shard_size = if data.is_empty() { 0 } else { data.len() / total_shards };
            let start = (shard_index * shard_size).saturating_add(req.offset as usize);
            let len = req.length as usize;
            let end = (start + len).min(data.len());
            if start < data.len() {
                data.slice(start..end)
            } else {
                Bytes::new()
            }
        };

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
        // [review][architecture][critical][resolved]
        // again, we have a lot of concurrent tasks writing to disk through the store, potentially conflicting.
        // this is a huge architectural oversight in my opinion, and must be discussed with high priority
        // RESOLVED by store-unification f2 (ADR-0032 D3): one shared
        // store serializes writers per `.dat` (per-segment exclusive
        // locks + atomic whole-file writes) — concurrent writers to one
        // segment are unrepresentable.
        // [end]
        // Write the repaired shard into the data store.
        // In production this would merge the shard into the correct position.
        match self.data_store.write_segment_data(&segment_id, &req.data).await {
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

    /// Handles a loss announcement (ADR-0029 §D4 fast path; g3).
    ///
    /// The sender's data pool died; it announces the affected segment
    /// set. For each segment, the receiver verifies it actually HOLDS a
    /// replica (lifecycle registry contains the segment AND the segment's
    /// `storage_locations` includes the origin — the origin was a
    /// legitimate holder). Verified segments are enqueued as
    /// re-replication repair requests (the g5 ReRepWorker restores RF);
    /// the ack counts exactly what was enqueued. Un-held segments are
    /// NOT acked — the sender's bounded retries (or g4's reconciliation
    /// failsafe) cover them.
    async fn announce_loss(
        &self,
        request: Request<LossAnnouncement>,
    ) -> Result<Response<LossAck>, Status> {
        let req = request.into_inner();
        let origin = req.origin.map(NodeId::from).unwrap_or_else(|| NodeId::new("unknown"));
        let mut accepted = 0u32;
        self.announce_rx_total.inc();

        let Some(sink) = &self.repair_sink else {
            // No repair sink wired (tests / minimal embedding): verify
            // nothing, ack nothing. The announcement is best-effort; g4
            // is the failsafe.
            tracing::debug!(
                origin = %origin,
                pool_id = req.pool_id,
                announced = req.segments.len(),
                "loss announcement received but no repair sink wired; nothing accepted"
            );
            return Ok(Response::new(LossAck { accepted: 0 }));
        };

        for proto_sid in &req.segments {
            let segment_id = match SegmentId::try_from(proto_sid.clone()) {
                Ok(sid) => sid,
                Err(_) => continue,
            };
            // Verify the local hold-set: the receiver holds a replica of
            // the announced segment AND the origin was one of its
            // storage_locations holders (a legitimate announcer).
            let holds = self
                .registry
                .get(segment_id)
                .map(|entry| entry.metadata.storage_locations.iter().any(|loc| loc == &origin))
                .unwrap_or(false);
            if !holds {
                tracing::debug!(
                    origin = %origin,
                    segment_id = %segment_id,
                    "loss announcement: receiver does not hold a verified replica; not accepted"
                );
                continue;
            }
            // The request carries the FULL holder set from the local
            // registry entry; the node-side dispatcher filters it to the
            // LIVE holders before selecting a target and sending the
            // RequestReReplication RPC (ADR-0030).
            let entry = self.registry.get(segment_id);
            let holders: Vec<NodeId> = entry
                .as_ref()
                .map(|entry| entry.metadata.storage_locations.to_vec())
                .unwrap_or_default();
            let merkle_root = entry.as_ref().and_then(|e| e.metadata.merkle_root);
            // The seal-time shape rides the request (ADR-0030): the
            // acquiring worker registers the pulled copy with the
            // SOURCE's tier/EC geometry, not hardcoded defaults.
            let (tier, ec_k, ec_m) = entry
                .as_ref()
                .map(|e| (e.metadata.size_tier, e.metadata.ec_k, e.metadata.ec_m))
                .unwrap_or((oceanfs_core::SizeTier::Standard, 1, 0));
            match sink
                .enqueue(ReRepRequest {
                    origin: origin.clone(),
                    segment_id,
                    holders,
                    reason: RepairReason::Announcement,
                    retry_count: 0,
                    merkle_root,
                    tier,
                    ec_k,
                    ec_m,
                })
                .await
            {
                Ok(()) => {
                    accepted += 1;
                    self.announce_accepted_total.inc();
                }
                Err(e) => {
                    tracing::warn!(
                        origin = %origin,
                        segment_id = %segment_id,
                        error = %e,
                        "loss announcement: repair enqueue failed (queue full/closed); not accepted — g4 failsafe"
                    );
                }
            }
        }

        tracing::info!(
            origin = %origin,
            pool_id = req.pool_id,
            announced = req.segments.len(),
            accepted,
            "loss announcement processed"
        );
        Ok(Response::new(LossAck { accepted }))
    }

    /// Handles a compaction segment-remap (g3 Option A —
    /// owner-authoritative compaction propagation).
    ///
    /// The owner compacted `old_segment_id → new_segment_id` and rewrote
    /// only its own metadata. This receiver (a holder of the old segment)
    /// must:
    ///
    /// 1. verify it holds the old segment AND the origin was a
    ///    legitimate holder (`storage_locations` contains origin);
    /// 2. record the alias + chunk table so the append/read-repair
    ///    handlers translate late chunk refs at write time;
    /// 3. batch-rewrite its already-persisted object rows for the
    ///    ANNOUNCED object keys (ADR-0034 D5/2b) through the chunk
    ///    table — per-key point lookups, never an objects-CF scan;
    /// 4. delete the stale replica (durable `request_delete` then
    ///    unlink — ADR-0024 invariant 3).
    ///
    /// `RemapAck.applied` is true only when all four succeeded. A
    /// receiver that does not hold the old segment acks `applied=false`
    /// — the sender's bounded retries (or g4's reconciliation failsafe)
    /// cover it.
    async fn announce_remap(
        &self,
        request: Request<SegmentRemap>,
    ) -> Result<Response<RemapAck>, Status> {
        let req = request.into_inner();
        let origin = req.origin.map(NodeId::from).unwrap_or_else(|| NodeId::new("unknown"));
        self.announce_rx_total.inc();
        let Some(old_sid) =
            req.old_segment_id.as_ref().and_then(|s| SegmentId::try_from(s.clone()).ok())
        else {
            return Ok(Response::new(RemapAck { applied: false }));
        };
        let Some(new_sid) =
            req.new_segment_id.as_ref().and_then(|s| SegmentId::try_from(s.clone()).ok())
        else {
            return Ok(Response::new(RemapAck { applied: false }));
        };
        let chunk_table: Vec<RemappedChunk> = req
            .chunks
            .iter()
            .map(|c| RemappedChunk {
                old_offset: c.old_offset,
                length: c.length,
                new_offset: c.new_offset,
            })
            .collect();

        // The announced object-key list (ADR-0034 D5/2b): the `(bucket,
        // key)` of every live object the owner repacked. Malformed
        // entries (missing bucket/key) are skipped, mirroring the
        // sealed-segment-push receiver's membership decoding.
        let object_keys: Vec<oceanfs_core::ContainedObject> = req
            .objects
            .iter()
            .filter_map(|co| {
                let bucket = co.bucket.as_ref()?;
                let key = co.key.as_ref()?;
                Some(oceanfs_core::ContainedObject {
                    bucket: oceanfs_core::BucketId::new(&bucket.name),
                    key: oceanfs_core::ObjectKey::new(&key.key),
                })
            })
            .collect();

        // Step 1: verify the receiver holds the old segment AND the
        // origin was a legitimate holder. A non-holder has nothing to
        // re-point and must not be tricked by a spoofed remap.
        let verified = self
            .registry
            .get(old_sid)
            .map(|entry| entry.metadata.storage_locations.iter().any(|loc| loc == &origin))
            .unwrap_or(false);
        if !verified {
            tracing::warn!(
                origin = %origin,
                old_segment_id = %old_sid,
                "remap rejected: receiver does not hold the old segment with the origin as a holder"
            );
            return Ok(Response::new(RemapAck { applied: false }));
        }

        // Step 2: record the alias + chunk table so LATE metadata
        // referencing the old segment is translated at write time (the
        // GAP-1 mechanism — a row landing after this peer's GC compacted
        // the old segment away would otherwise reference a segment that
        // exists nowhere).
        if let Some(alias) = &self.remap_alias {
            alias.insert(old_sid, new_sid, chunk_table.clone());
        }

        // Step 3: batch-rewrite the already-persisted object rows for the
        // announced keys (point lookups — ADR-0034 D5/2b).
        if let Err(e) = self.repoint_objects(old_sid, new_sid, &chunk_table, &object_keys) {
            tracing::warn!(
                origin = %origin,
                old_segment_id = %old_sid,
                new_segment_id = %new_sid,
                error = %e,
                "remap: object metadata re-point failed; alias recorded — g4 failsafe"
            );
            return Ok(Response::new(RemapAck { applied: false }));
        }

        // Step 4: delete the stale replica through the machine
        // (ADR-0025 Decision 4 — the coordinator is the only writer of
        // lifecycle state), then unlink its `.dat` (ADR-0024 invariant
        // 3: delete before unlink). The receiver's own GC would
        // otherwise re-compact the stale replica into a divergent id.
        if let Some(coordinator) = &self.lifecycle_coordinator {
            match coordinator.request_delete(old_sid).await {
                Ok(())
                | Err(oceanfs_storage::segment::lifecycle::TransitionError::AlreadyDeleted)
                | Err(oceanfs_storage::segment::lifecycle::TransitionError::Missing) => {
                    // The stale replica was registered with pool_id 0
                    // (replica placement is pool-0/legacy) — unlink
                    // through the shared unified store (ADR-0032 D4).
                    if let Err(e) = self.data_store.delete_shards_with_pool(&old_sid, 0).await {
                        tracing::warn!(
                            old_segment_id = %old_sid,
                            error = %e,
                            "remap: stale replica .dat unlink failed; GC will reclaim"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        old_segment_id = %old_sid,
                        error = ?e,
                        "remap: stale replica delete failed; receiver GC will reclaim"
                    );
                }
            }
        }

        tracing::info!(
            origin = %origin,
            old_segment_id = %old_sid,
            new_segment_id = %new_sid,
            chunks = chunk_table.len(),
            objects = object_keys.len(),
            "compaction remap applied"
        );
        self.announce_accepted_total.inc();
        Ok(Response::new(RemapAck { applied: true }))
    }

    /// Handles a re-replication request (ADR-0030 target-pull; g5).
    ///
    /// The sender is a DISPATCHER — a live holder that detected
    /// under-replication — asking THIS node (the acquiring target) to
    /// pull the segment data from a live holder and materialize a new
    /// copy. The request is routing intent only: it carries the segment
    /// id and the live holder set to fetch from.
    ///
    /// The handler enqueues the request into the local `ReRepWorker`
    /// queue (the `replication_request_sink`); the worker performs the
    /// actual fetch + write + register + stamp. `accepted` is true when
    /// the request was enqueued. No local verification is required —
    /// the dispatcher verified the sender is a legitimate holder before
    /// sending, and the target's worker is idempotent (a duplicate
    /// request for an already-held segment is a no-op).
    async fn request_re_replication(
        &self,
        request: Request<RequestReReplicationRequest>,
    ) -> Result<Response<RequestReReplicationResponse>, Status> {
        let req = request.into_inner();
        let segment_id = match req.segment_id {
            Some(sid) => match SegmentId::try_from(sid) {
                Ok(sid) => sid,
                Err(_) => {
                    return Ok(Response::new(RequestReReplicationResponse { accepted: false }));
                }
            },
            None => {
                return Ok(Response::new(RequestReReplicationResponse { accepted: false }));
            }
        };
        let holders: Vec<NodeId> = req.holders.into_iter().map(NodeId::from).collect();
        // The segment's seal-time merkle root (the dispatcher read it
        // from its own registry entry). The worker verifies the fetched
        // data against it (ADR-0030). Empty (a legacy/tests sender)
        // skips the verification.
        let merkle_root = if req.merkle_root.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&req.merkle_root);
            Some(oceanfs_core::HashOutput::from_bytes(arr))
        } else {
            None
        };
        // The proto enum's integer value is our wire format (1 =
        // Announcement, 2 = Reconciliation); map through the generated
        // enum so an unknown value degrades to Reconciliation.
        let reason = match crate::healing_rpc::RepairReason::try_from(req.reason) {
            Ok(crate::healing_rpc::RepairReason::Announcement) => RepairReason::Announcement,
            Ok(_) => RepairReason::Reconciliation,
            Err(_) => RepairReason::Reconciliation,
        };
        // The seal-time shape (the dispatcher read it from its own
        // registry entry): tier encodes as the SizeTier wire u8 (0 =
        // Inline, 1 = Small, 2 = Standard, 3 = Multi), matching the
        // segment-push protocol. Unknown tiers degrade to Standard (the
        // same fallback the push receiver uses).
        let tier = match req.tier {
            0 => oceanfs_core::SizeTier::Inline,
            1 => oceanfs_core::SizeTier::Small,
            2 => oceanfs_core::SizeTier::Standard,
            3 => oceanfs_core::SizeTier::Multi,
            _ => oceanfs_core::SizeTier::Standard,
        };
        let ec_k = req.ec_k.min(u8::MAX as u32) as u8;
        let ec_m = req.ec_m.min(u8::MAX as u32) as u8;

        let Some(sink) = &self.replication_request_sink else {
            // No worker queue wired (tests / minimal embedding): ack
            // nothing. The dispatcher's bounded retries (or g4's
            // reconciliation failsafe) cover it.
            tracing::debug!(
                segment_id = %segment_id,
                holders = holders.len(),
                "re-replication request received but no local worker queue wired; not accepted"
            );
            return Ok(Response::new(RequestReReplicationResponse { accepted: false }));
        };

        let req = ReRepRequest {
            origin: NodeId::new("dispatcher"),
            segment_id,
            holders,
            reason,
            retry_count: 0,
            merkle_root,
            tier,
            ec_k,
            ec_m,
        };
        let holder_count = req.holders.len();
        match sink.enqueue(req).await {
            Ok(()) => {
                tracing::info!(
                    segment_id = %segment_id,
                    holders = holder_count,
                    reason = ?reason,
                    "re-replication request accepted; worker will pull"
                );
                Ok(Response::new(RequestReReplicationResponse { accepted: true }))
            }
            Err(e) => {
                tracing::warn!(
                    segment_id = %segment_id,
                    error = %e,
                    "re-replication request enqueue failed (queue full/closed); not accepted"
                );
                Ok(Response::new(RequestReReplicationResponse { accepted: false }))
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
    use crate::{healing_rpc::RemappedChunk as ProtoRemappedChunk, HintedHandoff};

    /// In-memory store for healing tests.
    struct TestHealStore {
        data: Mutex<HashMap<SegmentId, Bytes>>,
    }

    impl TestHealStore {
        fn new() -> Self {
            Self { data: Mutex::new(HashMap::new()) }
        }
    }

    #[async_trait::async_trait]
    impl SegmentDataStore for TestHealStore {
        async fn write_segment_data(
            &self,
            segment_id: &SegmentId,
            data: &[u8],
        ) -> oceanfs_storage_api::error::Result<()> {
            self.data.lock().insert(*segment_id, Bytes::copy_from_slice(data));
            Ok(())
        }

        async fn read_segment_data(
            &self,
            segment_id: &SegmentId,
        ) -> oceanfs_storage_api::error::Result<Option<oceanfs_storage_api::SegmentFile>> {
            Ok(self.data.lock().get(segment_id).cloned().map(|data| {
                oceanfs_storage_api::SegmentFile {
                    segment_id: *segment_id,
                    version: 1,
                    header_len: 76,
                    data_end: (76 + data.len()) as u64,
                    data,
                }
            }))
        }

        async fn delete_shards(
            &self,
            segment_id: &SegmentId,
        ) -> oceanfs_storage_api::error::Result<u64> {
            Ok(self.data.lock().remove(segment_id).map(|removed| removed.len() as u64).unwrap_or(0))
        }

        async fn delete_shards_with_pool(
            &self,
            segment_id: &SegmentId,
            _pool_id: u32,
        ) -> oceanfs_storage_api::error::Result<u64> {
            self.delete_shards(segment_id).await
        }

        fn list_segment_files(
            &self,
            _root: &std::path::Path,
        ) -> oceanfs_storage_api::error::Result<Vec<std::path::PathBuf>> {
            Ok(Vec::new())
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
        test_store.write_segment_data(&seg_id, &data).await.unwrap();

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

    // -----------------------------------------------------------------------
    // g3 `loss-announcement` / compaction-remap handlers
    // -----------------------------------------------------------------------

    /// A repair sink that records every enqueued request (shared with
    /// the handler via `Arc`).
    #[derive(Clone, Default)]
    struct RecordingRepairSink {
        requests: Arc<parking_lot::Mutex<Vec<ReRepRequest>>>,
        reject: Arc<parking_lot::Mutex<bool>>,
    }

    #[async_trait::async_trait]
    impl RepairSink for RecordingRepairSink {
        async fn enqueue(&self, request: ReRepRequest) -> Result<(), String> {
            if *self.reject.lock() {
                return Err("queue full".to_string());
            }
            self.requests.lock().push(request);
            Ok(())
        }
    }

    /// Seeds a Sealed segment entry whose `storage_locations` lists
    /// `[origin, self_id]` — the shape a push-receiver holds after the
    /// backbone stamped the holder set. The locations ride the SEAL
    /// metadata (the seal replaces the reserved metadata entirely —
    /// matching the push receiver's `request_reserve` + `request_seal`
    /// with the pushed metadata).
    fn seed_sealed_with_locations(
        registry: &oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry,
        segment_id: SegmentId,
        origin: &NodeId,
        self_id: &NodeId,
    ) {
        let mut locations = smallvec::SmallVec::new();
        locations.push(origin.clone());
        locations.push(self_id.clone());
        registry
            .reserve(
                segment_id,
                oceanfs_core::SegmentMetadata {
                    pool_id: 0,
                    total_bytes: 0,
                    segment_id,
                    ec_k: 4,
                    ec_m: 2,
                    size_tier: oceanfs_core::SizeTier::Standard,
                    merkle_root: None,
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: None,
                },
            )
            .unwrap();
        registry
            .seal(
                segment_id,
                oceanfs_core::SegmentMetadata {
                    pool_id: 0,
                    total_bytes: 0,
                    segment_id,
                    ec_k: 4,
                    ec_m: 2,
                    size_tier: oceanfs_core::SizeTier::Standard,
                    merkle_root: Some(oceanfs_core::HashOutput::from_bytes([0x11; 32])),
                    storage_locations: locations,
                    sealed_at: Some(1),
                },
            )
            .unwrap();
    }

    #[tokio::test]
    async fn announce_loss_acks_only_held_segments() {
        let dir = tempfile::tempdir().unwrap();
        let metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore> = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let origin = NodeId::new("node-a");
        let self_id = NodeId::new("node-b");
        let held = SegmentId::new();
        let unheld = SegmentId::new();
        seed_sealed_with_locations(&registry, held, &origin, &self_id);

        let sink = RecordingRepairSink::default();
        let service = HealingGrpcService::new(
            Arc::new(HintedHandoff::new()),
            metadata_store,
            registry,
            Arc::new(TestHealStore::new()),
            Arc::new(HlcClock::new()),
        )
        .with_repair_sink(Arc::new(sink.clone()));

        let proto_origin: oceanfs_core::proto::common::NodeId = origin.clone().into();
        let request = tonic::Request::new(LossAnnouncement {
            origin: Some(proto_origin),
            pool_id: 3,
            segments: vec![held.into(), unheld.into()],
        });

        let response = service.announce_loss(request).await.unwrap();
        assert_eq!(response.into_inner().accepted, 1, "only the held+verified segment is accepted");
        let recorded = sink.requests.lock();
        assert_eq!(recorded.len(), 1, "exactly one repair enqueued");
        assert_eq!(recorded[0].segment_id, held);
        assert_eq!(recorded[0].origin, origin);
        assert_eq!(
            recorded[0].holders,
            vec![origin.clone(), self_id.clone()],
            "the request carries the full holder set from the registry entry"
        );
        assert_eq!(
            recorded[0].reason,
            RepairReason::Announcement,
            "g3 requests are announcement-driven"
        );
    }

    #[tokio::test]
    async fn announce_loss_ignores_unknown_origin() {
        let dir = tempfile::tempdir().unwrap();
        let metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore> = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let self_id = NodeId::new("node-b");
        let held = SegmentId::new();
        seed_sealed_with_locations(&registry, held, &NodeId::new("node-a"), &self_id);

        let sink = RecordingRepairSink::default();
        let service = HealingGrpcService::new(
            Arc::new(HintedHandoff::new()),
            metadata_store,
            registry,
            Arc::new(TestHealStore::new()),
            Arc::new(HlcClock::new()),
        )
        .with_repair_sink(Arc::new(sink.clone()));

        // A DIFFERENT origin (not in storage_locations) announces — the
        // receiver must not enqueue: the origin was never a holder.
        let proto_origin: oceanfs_core::proto::common::NodeId = NodeId::new("node-attacker").into();
        let request = tonic::Request::new(LossAnnouncement {
            origin: Some(proto_origin),
            pool_id: 3,
            segments: vec![held.into()],
        });

        let response = service.announce_loss(request).await.unwrap();
        assert_eq!(response.into_inner().accepted, 0, "an unknown origin must not trigger repairs");
        assert!(sink.requests.lock().is_empty());
    }

    #[tokio::test]
    async fn announce_remap_repoints_objects_and_records_alias() {
        let dir = tempfile::tempdir().unwrap();
        let metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore> = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let origin = NodeId::new("node-a");
        let self_id = NodeId::new("node-b");
        let old = SegmentId::new();
        let new = SegmentId::new();
        seed_sealed_with_locations(&registry, old, &origin, &self_id);

        // Seed helper: writes one row under `bucket` referencing the OLD
        // segment with the given chunk geometry.
        let seed_row = |metadata_store: &Arc<dyn oceanfs_storage_api::MetadataStore>,
                        bucket: &oceanfs_core::BucketId,
                        key: &oceanfs_core::ObjectKey,
                        offset: u64,
                        length: u32| {
            let mut chunks = smallvec::SmallVec::new();
            chunks.push(oceanfs_core::ChunkRef {
                segment_id: old,
                offset,
                length,
                compressed: false,
                logical_length: length,
            });
            oceanfs_storage_api::MetadataStore::put_object(
                metadata_store.as_ref(),
                bucket,
                oceanfs_core::ObjectMetadata {
                    object_key: key.clone(),
                    size: length as u64,
                    blake3_hash: None,
                    chunks,
                    inline_data: None,
                    created_at: 0,
                    hlc: Hlc::zero(),
                },
            )
            .unwrap();
        };

        let bucket = oceanfs_core::BucketId::new("b");
        // ANNOUNCED + chunk present in the chunk table → re-pointed.
        let key = oceanfs_core::ObjectKey::new("k");
        seed_row(&metadata_store, &bucket, &key, 100, 32);
        // ANNOUNCED but chunk ABSENT from the chunk table (a tombstoned
        // object the compactor filtered out) → keeps its old ref.
        let stale = oceanfs_core::ObjectKey::new("stale");
        seed_row(&metadata_store, &bucket, &stale, 400, 32);
        // NOT ANNOUNCED but referencing the old segment with a chunk that
        // IS in the table → must stay untouched (the owner did not vouch
        // for it).
        let unannounced = oceanfs_core::ObjectKey::new("unannounced");
        seed_row(&metadata_store, &bucket, &unannounced, 100, 32);
        // ANNOUNCED but ABSENT locally → skipped without error.
        let absent = oceanfs_core::ObjectKey::new("absent");

        let alias = Arc::new(SegmentRemapAlias::new());
        let service = HealingGrpcService::new(
            Arc::new(HintedHandoff::new()),
            metadata_store.clone(),
            registry,
            Arc::new(TestHealStore::new()),
            Arc::new(HlcClock::new()),
        )
        .with_remap_alias(Arc::clone(&alias));

        let proto_origin: oceanfs_core::proto::common::NodeId = origin.into();
        let proto_co =
            |key: &oceanfs_core::ObjectKey| oceanfs_core::proto::segment::ContainedObject {
                bucket: Some(bucket.clone().into()),
                key: Some(key.clone().into()),
            };
        let request = tonic::Request::new(SegmentRemap {
            origin: Some(proto_origin),
            old_segment_id: Some(old.into()),
            new_segment_id: Some(new.into()),
            chunks: vec![ProtoRemappedChunk { old_offset: 100, length: 32, new_offset: 0 }],
            objects: vec![proto_co(&key), proto_co(&stale), proto_co(&absent)],
        });

        let response = service.announce_remap(request).await.unwrap();
        assert!(response.into_inner().applied, "verified remap must apply");

        // ANNOUNCED + in-table: the object row now references the NEW
        // segment at the new offset.
        let meta = oceanfs_storage_api::MetadataStore::get_object_metadata(
            metadata_store.as_ref(),
            &bucket,
            &key,
        )
        .unwrap()
        .expect("object survives remap");
        assert_eq!(meta.chunks.len(), 1);
        assert_eq!(meta.chunks[0].segment_id, new);
        assert_eq!(meta.chunks[0].offset, 0, "chunk offset translated through the table");
        assert_eq!(meta.chunks[0].length, 32);

        // ANNOUNCED but chunk absent from the table → old ref untouched.
        let meta = oceanfs_storage_api::MetadataStore::get_object_metadata(
            metadata_store.as_ref(),
            &bucket,
            &stale,
        )
        .unwrap()
        .expect("stale-key object survives remap");
        assert_eq!(meta.chunks.len(), 1);
        assert_eq!(meta.chunks[0].segment_id, old, "chunk absent from the table keeps its old ref");
        assert_eq!(meta.chunks[0].offset, 400);

        // NOT ANNOUNCED → untouched even though it references the old
        // segment with a chunk the table carries.
        let meta = oceanfs_storage_api::MetadataStore::get_object_metadata(
            metadata_store.as_ref(),
            &bucket,
            &unannounced,
        )
        .unwrap()
        .expect("unannounced object survives remap");
        assert_eq!(meta.chunks.len(), 1);
        assert_eq!(
            meta.chunks[0].segment_id, old,
            "an unannounced key referencing the old segment is left untouched"
        );
        assert_eq!(meta.chunks[0].offset, 100);

        // ANNOUNCED but absent locally → no error, no row created.
        assert!(
            oceanfs_storage_api::MetadataStore::get_object_metadata(
                metadata_store.as_ref(),
                &bucket,
                &absent,
            )
            .unwrap()
            .is_none(),
            "an announced key this holder does not own is skipped without error"
        );

        // The alias is recorded for late metadata writes.
        assert_eq!(alias.resolve(old, 100, 32), Some((new, 0)));
    }

    #[tokio::test]
    async fn announce_remap_rejects_unheld_or_spoofed() {
        let dir = tempfile::tempdir().unwrap();
        let metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore> = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let self_id = NodeId::new("node-b");
        let old = SegmentId::new();
        let new = SegmentId::new();
        // The receiver holds `old` but the origin is NOT a holder.
        seed_sealed_with_locations(&registry, old, &NodeId::new("node-c"), &self_id);

        let alias = Arc::new(SegmentRemapAlias::new());
        let service = HealingGrpcService::new(
            Arc::new(HintedHandoff::new()),
            metadata_store,
            registry,
            Arc::new(TestHealStore::new()),
            Arc::new(HlcClock::new()),
        )
        .with_remap_alias(Arc::clone(&alias));

        let proto_origin: oceanfs_core::proto::common::NodeId = NodeId::new("node-attacker").into();
        let request = tonic::Request::new(SegmentRemap {
            origin: Some(proto_origin),
            old_segment_id: Some(old.into()),
            new_segment_id: Some(new.into()),
            chunks: vec![],
            objects: vec![],
        });

        let response = service.announce_remap(request).await.unwrap();
        assert!(
            !response.into_inner().applied,
            "a spoofed remap (origin not a holder) must be rejected"
        );
        assert!(alias.is_empty(), "no alias recorded for a rejected remap");
    }

    #[tokio::test]
    async fn announce_remap_empty_object_keys_degrades_to_alias_only() {
        // A peer on an OLDER binary (or an empty repack edge) sends the
        // chunk table WITHOUT the object-key list (ADR-0034 D5/2b guard):
        // the receiver records the alias but re-points NOTHING and never
        // falls back to a full objects-CF scan — the g4 reconciliation
        // covers the divergence.
        let dir = tempfile::tempdir().unwrap();
        let metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore> = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let origin = NodeId::new("node-a");
        let self_id = NodeId::new("node-b");
        let old = SegmentId::new();
        let new = SegmentId::new();
        seed_sealed_with_locations(&registry, old, &origin, &self_id);

        // Seed a row referencing the old segment with a chunk the table
        // would translate — it must remain untouched with no key list.
        let bucket = oceanfs_core::BucketId::new("b");
        let key = oceanfs_core::ObjectKey::new("k");
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(oceanfs_core::ChunkRef {
            segment_id: old,
            offset: 100,
            length: 32,
            compressed: false,
            logical_length: 32,
        });
        oceanfs_storage_api::MetadataStore::put_object(
            metadata_store.as_ref(),
            &bucket,
            oceanfs_core::ObjectMetadata {
                object_key: key.clone(),
                size: 32,
                blake3_hash: None,
                chunks,
                inline_data: None,
                created_at: 0,
                hlc: Hlc::zero(),
            },
        )
        .unwrap();

        let alias = Arc::new(SegmentRemapAlias::new());
        let service = HealingGrpcService::new(
            Arc::new(HintedHandoff::new()),
            metadata_store.clone(),
            registry,
            Arc::new(TestHealStore::new()),
            Arc::new(HlcClock::new()),
        )
        .with_remap_alias(Arc::clone(&alias));

        let proto_origin: oceanfs_core::proto::common::NodeId = origin.into();
        let request = tonic::Request::new(SegmentRemap {
            origin: Some(proto_origin),
            old_segment_id: Some(old.into()),
            new_segment_id: Some(new.into()),
            chunks: vec![ProtoRemappedChunk { old_offset: 100, length: 32, new_offset: 0 }],
            objects: vec![],
        });

        let response = service.announce_remap(request).await.unwrap();
        assert!(response.into_inner().applied, "a holder-verified remap still applies");

        // The alias is recorded (late chunk refs still translate)…
        assert_eq!(alias.resolve(old, 100, 32), Some((new, 0)));
        // …but the persisted row is NOT re-pointed.
        let meta = oceanfs_storage_api::MetadataStore::get_object_metadata(
            metadata_store.as_ref(),
            &bucket,
            &key,
        )
        .unwrap()
        .expect("object survives remap");
        assert_eq!(meta.chunks.len(), 1);
        assert_eq!(
            meta.chunks[0].segment_id, old,
            "an empty object-key list re-points nothing (alias + g4 failsafe)"
        );
        assert_eq!(meta.chunks[0].offset, 100);
    }

    #[tokio::test]
    async fn announce_loss_no_sink_acks_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore> = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let origin = NodeId::new("node-a");
        let self_id = NodeId::new("node-b");
        let held = SegmentId::new();
        seed_sealed_with_locations(&registry, held, &origin, &self_id);

        // No repair sink wired — the handler verifies nothing and acks 0
        // (the announcement is best-effort; g4 is the failsafe).
        let service = HealingGrpcService::new(
            Arc::new(HintedHandoff::new()),
            metadata_store,
            registry,
            Arc::new(TestHealStore::new()),
            Arc::new(HlcClock::new()),
        );

        let proto_origin: oceanfs_core::proto::common::NodeId = origin.into();
        let request = tonic::Request::new(LossAnnouncement {
            origin: Some(proto_origin),
            pool_id: 3,
            segments: vec![held.into()],
        });

        let response = service.announce_loss(request).await.unwrap();
        assert_eq!(response.into_inner().accepted, 0);
    }

    // -----------------------------------------------------------------------
    // g5 `request_re_replication` (ADR-0030 target-pull)
    // -----------------------------------------------------------------------

    /// The `RequestReReplication` handler enqueues the request into the
    /// local worker queue and acks `accepted = true`.
    #[tokio::test]
    async fn request_re_replication_enqueues_into_local_worker_queue() {
        let dir = tempfile::tempdir().unwrap();
        let metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore> = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let service = HealingGrpcService::new(
            Arc::new(HintedHandoff::new()),
            metadata_store,
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            Arc::new(TestHealStore::new()),
            Arc::new(HlcClock::new()),
        );

        let sink = RecordingRepairSink::default();
        let service = service.with_replication_request_sink(Arc::new(sink.clone()));

        let segment_id = SegmentId::new();
        let holder_a = NodeId::new("node-a");
        let holder_b = NodeId::new("node-b");
        let proto_sid: oceanfs_core::proto::common::SegmentId = segment_id.into();
        let root = oceanfs_core::HashOutput::from_bytes([0xAB; 32]);
        let request = tonic::Request::new(RequestReReplicationRequest {
            segment_id: Some(proto_sid),
            holders: vec![holder_a.clone().into(), holder_b.clone().into()],
            reason: crate::healing_rpc::RepairReason::Announcement as i32,
            merkle_root: bytes::Bytes::copy_from_slice(root.as_bytes()),
            tier: 1, // Small (SizeTier wire u8)
            ec_k: 4,
            ec_m: 2,
        });

        let response = service.request_re_replication(request).await.unwrap();
        assert!(response.into_inner().accepted, "the target must accept the request");
        let recorded = sink.requests.lock();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].segment_id, segment_id);
        assert_eq!(recorded[0].holders, vec![holder_a, holder_b]);
        assert_eq!(recorded[0].reason, RepairReason::Announcement);
        assert_eq!(recorded[0].merkle_root, Some(root), "the seal-time root rides the request");
        assert_eq!(
            recorded[0].tier,
            oceanfs_core::SizeTier::Small,
            "the seal-time tier rides the request"
        );
        assert_eq!(recorded[0].ec_k, 4, "the seal-time ec_k rides the request");
        assert_eq!(recorded[0].ec_m, 2, "the seal-time ec_m rides the request");
    }

    /// Without a local worker queue wired, the handler acks nothing
    /// (the dispatcher's retries / g4 failsafe cover it).
    #[tokio::test]
    async fn request_re_replication_no_queue_acks_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let metadata_store: Arc<dyn oceanfs_storage_api::MetadataStore> = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let service = HealingGrpcService::new(
            Arc::new(HintedHandoff::new()),
            metadata_store,
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            Arc::new(TestHealStore::new()),
            Arc::new(HlcClock::new()),
        );

        let segment_id = SegmentId::new();
        let proto_sid: oceanfs_core::proto::common::SegmentId = segment_id.into();
        let request = tonic::Request::new(RequestReReplicationRequest {
            segment_id: Some(proto_sid),
            holders: vec![NodeId::new("node-a").into()],
            reason: crate::healing_rpc::RepairReason::Reconciliation as i32,
            merkle_root: bytes::Bytes::new(),
            tier: 2, // Standard (default when absent)
            ec_k: 0,
            ec_m: 0,
        });

        let response = service.request_re_replication(request).await.unwrap();
        assert!(!response.into_inner().accepted, "no queue → not accepted");
    }

    /// The healing `fetch_shard` full-segment mode (offset 0 + length 0)
    /// streams the ENTIRE data section — the g5 re-replication fetch.
    #[tokio::test]
    async fn fetch_shard_full_segment_mode_returns_whole_data() {
        let store = TestHealStore::new();
        let segment_id = SegmentId::new();
        let data: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
        store.write_segment_data(&segment_id, &data).await.unwrap();

        let service = HealingGrpcService::new(
            Arc::new(HintedHandoff::new()),
            Arc::new(
                oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                    data_dir: std::env::temp_dir()
                        .join(format!("oceanfs-test-fetch-full-{}", std::process::id())),
                    ..Default::default()
                })
                .unwrap(),
            ),
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            Arc::new(store),
            Arc::new(HlcClock::new()),
        );

        let proto_sid: oceanfs_core::proto::common::SegmentId = segment_id.into();
        let request = tonic::Request::new(FetchShardRequest {
            segment_id: Some(proto_sid),
            shard_index: 0,
            offset: 0,
            length: 0, // full-segment mode
        });

        let response = service.fetch_shard(request).await.unwrap();
        let mut stream = response.into_inner();
        let mut received = bytes::BytesMut::new();
        use tokio_stream::StreamExt;
        while let Some(chunk_result) = stream.next().await {
            if let Ok(chunk) = chunk_result {
                received.extend_from_slice(&chunk.data);
            }
        }
        assert_eq!(&received[..], &data[..], "full-segment fetch returns the whole data section");
    }

    /// The single-shard mode (length > 0) still returns the requested
    /// byte range of the named shard (EC reconstruction unchanged).
    #[tokio::test]
    async fn fetch_shard_single_shard_mode_unchanged() {
        let store = TestHealStore::new();
        let segment_id = SegmentId::new();
        let data: Vec<u8> = (0..60_000).map(|i| (i % 251) as u8).collect();
        store.write_segment_data(&segment_id, &data).await.unwrap();

        let service = HealingGrpcService::new(
            Arc::new(HintedHandoff::new()),
            Arc::new(
                oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                    data_dir: std::env::temp_dir()
                        .join(format!("oceanfs-test-fetch-shard-{}", std::process::id())),
                    ..Default::default()
                })
                .unwrap(),
            ),
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            Arc::new(store),
            Arc::new(HlcClock::new()),
        );

        let proto_sid: oceanfs_core::proto::common::SegmentId = segment_id.into();
        // shard_index 0, offset 100, length 500 → bytes [100, 600).
        let request = tonic::Request::new(FetchShardRequest {
            segment_id: Some(proto_sid),
            shard_index: 0,
            offset: 100,
            length: 500,
        });

        let response = service.fetch_shard(request).await.unwrap();
        let mut stream = response.into_inner();
        let mut received = bytes::BytesMut::new();
        use tokio_stream::StreamExt;
        while let Some(chunk_result) = stream.next().await {
            if let Ok(chunk) = chunk_result {
                received.extend_from_slice(&chunk.data);
            }
        }
        assert_eq!(&received[..], &data[100..600], "single-shard mode returns the requested range");
    }
}
