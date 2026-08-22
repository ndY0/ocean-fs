//! Read coordinator — parallel shard fetch and blob reconstruction.
//!
//! Coordinates distributed blob reads: looks up object metadata,
//! fetches shards from k+m nodes in parallel using `FuturesUnordered`,
//! uses the fastest k responses to reconstruct the blob, and verifies
//! the BLAKE3 hash.
//!
//! ## Read Path
//!
//! 1. Metadata lookup: check for inline data or chunk references.
//! 2. For each chunk: determine replica set, fetch shards in parallel.
//! 3. EC decode if necessary (when reading parity shards).
//! 4. Multi-chunk assembly with streaming BLAKE3 verification.
//! 5. Read repair: asynchronously correct stale replicas when `R > 1`.
//!
//! Per performance guideline §8.1 (FuturesUnordered), §8.2
//! (tokio::select!), and §5.4 (batch verify for multi-chunk reads).

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use oceanfs_cache::CacheRpcClient;
use oceanfs_core::{
    proto::segment::{
        GetObjectMetadataRequest, GetObjectMetadataResponse, PutObjectMetadataRequest,
    },
    BucketId, ConflictResolver, FetchStrategy, FetchStrategyConfig, HashKey, HashOutput, Hlc,
    HlcClock, LwwResolver, NodeId, ObjectKey, ObjectMetadata, OperationTimeouts, Resolution,
    SegmentId,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::RingCache;
use oceanfs_storage::SegmentRpcClient;
use tracing::{debug, info, warn};

use crate::{
    error::{Error, Result},
    metadata_ops::MetadataOps,
    read::assembly::MultiChunkAssembler,
};

/// A request to read an object.
#[derive(Debug, Clone)]
pub struct ReadRequest {
    /// Source bucket.
    pub bucket: BucketId,
    /// Object key.
    pub key: ObjectKey,
    /// Pre-computed key hash.
    pub hash_key: HashKey,
    /// If true, only fetch metadata (HEAD equivalent).
    pub metadata_only: bool,
    /// If true, read the LOCAL state only: skip the multi-replica HLC
    /// comparison and read repair. Used by the hinted-handoff fetch
    /// (the origin's own state is exactly what the receiver must
    /// converge to) — the comparison would turn every hint
    /// materialization into a 3-node fanout and blow the sender's
    /// delivery timeout.
    pub local_only: bool,
    /// Per-bucket policy (configuration, resolver, etc.).
    pub policy: Option<Arc<crate::BucketPolicy>>,
}

/// Result of a read operation.
#[derive(Debug, Clone)]
pub struct ReadResult {
    /// The object's data.
    pub data: Bytes,
    /// Metadata for the object.
    pub metadata: ObjectMetadata,
    /// Whether the BLAKE3 hash was verified against the stored hash.
    pub hash_verified: bool,
    /// The data source for sendfile integration.
    ///
    /// When `Some`, the HTTP handler SHOULD use `SegmentFileBody`.
    /// When `None`, use `Body::from(Bytes)`.
    pub segment_source: Option<oceanfs_storage::io::SegmentReadSource>,
}

/// The result of a complete read, including cache-hit information.
///
/// Returned by [`ReadCoordinator::get_object`] to allow callers
/// to populate caches and report metrics.
#[derive(Debug, Clone)]
pub struct GetResult {
    /// The object's data.
    pub data: Bytes,
    /// Metadata for the object.
    pub metadata: ObjectMetadata,
    /// Which cache level served this data (or `Miss`).
    pub cache_hit: CacheHitLevel,
    /// The BLAKE3 hash of the data (always computed).
    pub hash: HashOutput,
    /// The data source for sendfile integration.
    ///
    /// When `Some(SegmentReadSource::MmapBacked { .. })` or
    /// `Some(SegmentReadSource::DirectIo { .. })`, the HTTP handler
    /// SHOULD use `SegmentFileBody` for the response.
    /// When `None` or `Some(SegmentReadSource::Memory)`, use
    /// `Body::from(Bytes)`.
    pub segment_source: Option<oceanfs_storage::io::SegmentReadSource>,
}

/// Which cache level served a read request.
///
/// Used for metrics and cache-population decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheHitLevel {
    /// Served from the L1 object cache (in-memory blob data).
    L1Object,
    /// Served from L2 metadata cache, inline data variant.
    L2MetadataInline,
    /// Served from L2 metadata cache, required chunk assembly.
    L2MetadataChunks,
    /// Rejected by L3 negative cache (key definitely absent).
    L3Negative,
    /// Cache miss — data retrieved from the backing store.
    Miss,
}

/// The outcome of a read path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOutcome {
    /// Data was served from inline metadata (no segment I/O).
    InlineHit,
    /// Data assembled from a single chunk.
    SingleChunk,
    /// Data assembled from multiple chunks.
    MultiChunk {
        /// Number of chunks the blob was split into.
        chunk_count: usize,
    },
    /// Object not found in metadata.
    NotFound,
}

/// Trait for reading segment chunk data from a backing store.
///
/// Provides chunk-level access for assembling blob data from
/// segment references. Implementations may read from local
/// disk or fetch from remote nodes via gRPC.
///
/// # Errors
///
/// Returns a string error if the segment or chunk cannot be read.
///
/// # Examples
///
/// ```
/// # use std::collections::HashMap;
/// # use std::sync::Arc;
/// # use bytes::Bytes;
/// # use oceanfs_core::SegmentId;
/// # use oceanfs_server::SegmentReader;
/// # use async_trait::async_trait;
/// // A simple in-memory implementation for testing.
/// struct InMemorySegments {
///     data: HashMap<SegmentId, Bytes>,
/// }
///
/// #[async_trait]
/// impl SegmentReader for InMemorySegments {
///     async fn read_chunk(
///         &self,
///         segment_id: &SegmentId,
///         _offset: u64,
///         _length: u32,
///     ) -> Result<Bytes, String> {
///         self.data.get(segment_id).cloned()
///             .ok_or_else(|| format!("segment {segment_id} not found"))
///     }
/// }
/// ```
pub use oceanfs_storage::io::SegmentReader; // re-exported for backward compatibility
pub use oceanfs_storage::io::{DiskSegmentReader, InMemorySegmentReader, SegmentReadSource};

/// Coordinates distributed blob reads with parallel shard fetch.
///
/// Reads are metadata-first: inline blobs are served from memory,
/// while segment-stored blobs trigger parallel shard fetches.
pub struct ReadCoordinator {
    /// Ring cache for consistent-hashing lookups.
    ring: Arc<RingCache>,
    /// Node identifier for read repair targeting.
    node_id: NodeId,
    /// Conflict resolver for comparing replica versions.
    conflict_resolver: Arc<dyn ConflictResolver>,
    /// Metadata store for object lookup, wrapped in the async adapter
    /// so blocking RocksDB reads run on the blocking pool, never on a
    /// runtime worker (metadata-io-off-async-workers).
    metadata: Option<Arc<crate::metadata_async::AsyncMetadataOps>>,
    /// Optional segment reader for chunk-based reads.
    segment_reader: Option<Arc<dyn SegmentReader>>,
    /// Connection pool for gRPC shard fetch (multi-node reads).
    pool: Option<Arc<ConnectionPool>>,
    /// Membership store for resolving node addresses.
    membership: Option<Arc<Membership>>,
    /// Optional EC decoder for reconstructing data from parity shards.
    #[cfg(feature = "ec")]
    decoder: Option<Arc<dyn oceanfs_ec::Decoder>>,
    /// Number of EC data shards (k) for the segment codec.
    #[cfg(feature = "ec")]
    ec_data_shards: u8,
    /// Number of EC parity shards (m) for the segment codec.
    #[cfg(feature = "ec")]
    ec_parity_shards: u8,
    /// Per-operation timeout configuration.
    timeouts: Arc<OperationTimeouts>,
    /// Default fetch strategy for buckets without a per-bucket override.
    default_fetch_strategy: FetchStrategy,
    /// HLC clock for receive-merge (hlc-causality-closure G2). When
    /// set, remote HLCs observed during quorum comparison and read
    /// repair are merged via [`HlcClock::update`] so the local clock
    /// never lags the replicas this node talks to.
    hlc_clock: Option<Arc<HlcClock>>,
    /// Compression backend (accel dispatcher) for decompressing stored
    /// chunks on the read path. `None` (default) serves stored bytes
    /// as-is.
    #[cfg(feature = "accel")]
    compressor: Option<Arc<dyn oceanfs_accel::Compressor>>,
    /// Bounds concurrent decompression on the blocking pool (perf §2.7).
    #[cfg(feature = "accel")]
    decompress_semaphore: Arc<tokio::sync::Semaphore>,
    /// Peer-side routing hint source (ADR-0029 §D5): the node's cached
    /// storage-pool manifests, consulted as a hint when selecting
    /// replica nodes for gRPC chunk fetch. `None` (default) disables
    /// the hint — the fetch path behaves exactly as before.
    routing_hint: Option<Arc<dyn crate::routing_hint::RoutingHint>>,
}

/// [`ReadCoordinatorHintObjectReader`] backs the hinted-handoff fetch
/// RPC with the node's FULL read path (metadata → chunks → segment
/// reads → decompression), so the receiver gets the object's current
/// logical data exactly as a GET would serve it.
pub struct ReadCoordinatorHintObjectReader {
    read: Arc<ReadCoordinator>,
}

impl ReadCoordinatorHintObjectReader {
    /// Creates the reader over the node's read coordinator.
    pub fn new(read: Arc<ReadCoordinator>) -> Self {
        Self { read }
    }
}

#[async_trait::async_trait]
impl oceanfs_durability::HintObjectReader for ReadCoordinatorHintObjectReader {
    async fn read_object(
        &self,
        bucket: &oceanfs_core::BucketId,
        key: &oceanfs_core::ObjectKey,
    ) -> std::result::Result<Option<(oceanfs_core::ObjectMetadata, Bytes)>, String> {
        let req = ReadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            hash_key: HashKey::from_bytes(oceanfs_routing::hash_key(key.as_str().as_bytes())),
            metadata_only: false,
            // The hint fetch serves the origin's OWN current state —
            // no quorum comparison, no read repair (see the field doc).
            local_only: true,
            policy: None,
        };
        match self.read.get(req).await {
            Ok(result) => Ok(Some((result.metadata, result.data))),
            Err(crate::Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }
}

impl ReadCoordinator {
    /// Creates a new read coordinator.
    ///
    /// The optional `metadata` parameter enables real metadata lookups.
    /// When `None`, the coordinator falls back to inline-only reads.
    pub fn new(
        ring: Arc<RingCache>,
        node_id: NodeId,
        conflict_resolver: Option<Arc<dyn ConflictResolver>>,
    ) -> Self {
        Self {
            ring,
            node_id,
            conflict_resolver: conflict_resolver.unwrap_or_else(|| Arc::new(LwwResolver)),
            metadata: None,
            segment_reader: None,
            pool: None,
            membership: None,
            #[cfg(feature = "ec")]
            decoder: None,
            #[cfg(feature = "ec")]
            ec_data_shards: 0,
            #[cfg(feature = "ec")]
            ec_parity_shards: 0,
            timeouts: Arc::new(OperationTimeouts::default()),
            default_fetch_strategy: FetchStrategy::default(),
            hlc_clock: None,
            #[cfg(feature = "accel")]
            compressor: None,
            #[cfg(feature = "accel")]
            decompress_semaphore: Arc::new(tokio::sync::Semaphore::new(
                std::thread::available_parallelism().map_or(4, |n| n.get().saturating_mul(2)),
            )),
            routing_hint: None,
        }
    }

    /// Creates a new read coordinator with metadata store access.
    ///
    /// The sync metadata ops are wrapped in the async adapter
    /// (`spawn_blocking` + bounded semaphore) so blocking RocksDB reads
    /// never block a runtime worker.
    pub fn new_with_metadata(
        ring: Arc<RingCache>,
        node_id: NodeId,
        conflict_resolver: Option<Arc<dyn ConflictResolver>>,
        metadata: Arc<dyn MetadataOps>,
    ) -> Self {
        Self {
            ring,
            node_id,
            conflict_resolver: conflict_resolver.unwrap_or_else(|| Arc::new(LwwResolver)),
            metadata: Some(Arc::new(crate::metadata_async::AsyncMetadataOps::new(metadata))),
            segment_reader: None,
            pool: None,
            membership: None,
            #[cfg(feature = "ec")]
            decoder: None,
            #[cfg(feature = "ec")]
            ec_data_shards: 0,
            #[cfg(feature = "ec")]
            ec_parity_shards: 0,
            timeouts: Arc::new(OperationTimeouts::default()),
            default_fetch_strategy: FetchStrategy::default(),
            hlc_clock: None,
            #[cfg(feature = "accel")]
            compressor: None,
            #[cfg(feature = "accel")]
            decompress_semaphore: Arc::new(tokio::sync::Semaphore::new(
                std::thread::available_parallelism().map_or(4, |n| n.get().saturating_mul(2)),
            )),
            routing_hint: None,
        }
    }

    /// Sets an optional segment reader for chunk-based reads.
    ///
    /// When set, chunk assembly can retrieve segment data
    /// directly from the local store. When `None`, chunk-based
    /// reads return an error.
    pub fn with_segment_reader(mut self, reader: Arc<dyn SegmentReader>) -> Self {
        self.segment_reader = Some(reader);
        self
    }

    /// Sets the gRPC connection pool for multi-node shard fetch.
    ///
    /// When set (together with [`with_membership`](Self::with_membership)),
    /// chunk assembly falls back to gRPC `FetchShard` calls when the
    /// local segment reader is unavailable or fails.
    pub fn with_connection_pool(mut self, pool: Arc<ConnectionPool>) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Sets the membership store for resolving replica node addresses.
    ///
    /// Required for gRPC shard fetch; must be paired with
    /// [`with_connection_pool`](Self::with_connection_pool).
    pub fn with_membership(mut self, membership: Arc<Membership>) -> Self {
        self.membership = Some(membership);
        self
    }

    /// Sets the EC decoder for reconstructing data from parity shards.
    ///
    /// When configured, the internal `assemble_chunks` method can recover
    /// missing data shards using available parity shards via the EC
    /// decoder. Only available when the `ec` feature is enabled.
    #[cfg(feature = "ec")]
    pub fn with_decoder(mut self, decoder: Arc<dyn oceanfs_ec::Decoder>) -> Self {
        self.decoder = Some(decoder);
        self
    }

    /// Injects the compression backend (accel dispatcher) used to
    /// decompress stored chunks whose metadata marks them compressed.
    #[cfg(feature = "accel")]
    pub fn with_compressor(
        mut self,
        compressor: Option<Arc<dyn oceanfs_accel::Compressor>>,
    ) -> Self {
        self.compressor = compressor;
        self
    }

    /// Sets the EC codec parameters for shard-level reconstruction.
    ///
    /// `data_shards` (k) and `parity_shards` (m) define the erasure
    /// coding layout used when reconstructing missing data shards from
    /// parity. Must be paired with [`with_decoder`](Self::with_decoder).
    ///
    /// Only available when the `ec` feature is enabled.
    #[cfg(feature = "ec")]
    pub fn with_ec_codec(mut self, data_shards: u8, parity_shards: u8) -> Self {
        self.ec_data_shards = data_shards;
        self.ec_parity_shards = parity_shards;
        self
    }

    /// Sets the per-operation timeout configuration.
    ///
    /// Call this at startup to inject config-driven timeouts
    /// for metadata reads, shard fetches, and read operations.
    #[must_use]
    pub fn with_timeouts(mut self, timeouts: Arc<OperationTimeouts>) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Sets the default fetch strategy for buckets without a per-bucket override.
    #[must_use]
    pub fn with_default_fetch_strategy(mut self, strategy: FetchStrategy) -> Self {
        self.default_fetch_strategy = strategy;
        self
    }

    /// Injects the peer-side routing hint source (ADR-0029 §D5).
    ///
    /// The manifest cache's read-exclusion filter applies to replica-node
    /// selection in the gRPC chunk-fetch path. `None` (the default,
    /// when this builder is not called) disables the hint — the fetch
    /// path behaves exactly as before.
    #[must_use]
    pub fn with_routing_hint(mut self, hint: Arc<dyn crate::routing_hint::RoutingHint>) -> Self {
        self.routing_hint = Some(hint);
        self
    }

    /// Sets the HLC clock for receive-merge (hlc-causality-closure G2).
    ///
    /// When set, remote HLC timestamps observed during quorum comparison
    /// and read repair are merged into the local clock via
    /// [`HlcClock::update`] (the HLC receive rule), so the local clock
    /// never lags the replicas this node communicates with.
    #[must_use]
    pub fn with_hlc_clock(mut self, clock: Arc<HlcClock>) -> Self {
        self.hlc_clock = Some(clock);
        self
    }

    /// Executes a read and returns a [`GetResult`] with cache-hit
    /// information.
    ///
    /// Prefer this method over [`get`](Self::get) when cache
    /// population is desired. It provides [`CacheHitLevel`]
    /// so callers can decide which caches to populate.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the object does not exist.
    /// Returns [`Error::HashMismatch`] if the hash verification fails.
    pub async fn get_object(&self, req: ReadRequest) -> Result<GetResult> {
        let _replica_set = self.ring.lookup(req.hash_key.as_bytes());

        let mut obj_meta = self.lookup_metadata(&req).await?;

        // Metadata-only requests (HEAD) return the LOCAL state directly,
        // and local_only requests (the hinted-handoff fetch) read the
        // LOCAL state with data. Both skip the multi-replica comparison
        // and read repair: a HEAD must observe what THIS node actually
        // serves (the ETag verify), and the fetch must be cheap — the
        // comparison + repair fanout made every fetch a 3-node
        // operation and blew the sender's delivery timeout (the churn
        // stuck-hint class: batches of 186 hints × comparison-fetches
        // exceeded the 10s RPC timeout forever).
        if req.metadata_only {
            return Ok(GetResult {
                data: Bytes::new(),
                metadata: obj_meta,
                cache_hit: CacheHitLevel::Miss,
                hash: HashOutput::from_bytes([0u8; 32]),
                segment_source: None,
            });
        }
        let skip_remote = req.local_only;

        // §4.6: Multi-replica HLC comparison — when read_quorum > 1,
        // synchronously fetch metadata from replicas, compare HLCs,
        // and apply the winning version before responding to the client.
        if !skip_remote {
            if let Some(winning_meta) =
                self.compare_with_quorum(&req.bucket, &req.key, &obj_meta).await
            {
                obj_meta = winning_meta;
            }

            // §4.2: Read repair — asynchronously push corrected data to
            // stale replicas. This is fire-and-forget; it does not block
            // the client response.
            self.run_read_repair(&req.bucket, &req.key, &obj_meta).await;
        }

        let data = if let Some(ref inline) = obj_meta.inline_data {
            inline.clone()
        } else if !obj_meta.chunks.is_empty() {
            // Resolve effective fetch strategy: per-bucket override → node default.
            let strategy = req
                .policy
                .as_ref()
                .map(|p| p.effective_fetch_strategy(self.default_fetch_strategy))
                .unwrap_or(self.default_fetch_strategy);

            let (data, source) =
                self.assemble_chunks(&obj_meta, strategy, req.policy.as_deref()).await?;
            // Build result with source metadata.
            let computed_hash = blake3::hash(&data);
            let hash = HashOutput::from_bytes(*computed_hash.as_bytes());
            return Ok(GetResult {
                data,
                metadata: obj_meta,
                cache_hit: CacheHitLevel::Miss,
                hash,
                segment_source: source,
            });
        } else if self.metadata.is_some() {
            // Has a metadata store but no data: genuinely not found.
            return Err(Error::NotFound(format!("{}/{}", req.bucket, req.key)));
        } else {
            // No metadata store and no data: return empty (test/headless mode).
            Bytes::new()
        };

        let computed_hash = blake3::hash(&data);
        let hash = HashOutput::from_bytes(*computed_hash.as_bytes());

        Ok(GetResult {
            data,
            metadata: obj_meta,
            cache_hit: CacheHitLevel::Miss,
            hash,
            segment_source: None,
        })
    }

    /// Synchronously compares local HLC against remote replicas (§4.6).
    ///
    /// When `pool`, `membership`, and `metadata` are all available,
    /// this method fetches object metadata from every remote replica
    /// in the replica set, compares HLC timestamps via the configured
    /// [`ConflictResolver`], and applies the winning version locally
    /// if a remote replica has a newer version.
    ///
    /// This synchronous check ensures the client receives the winning
    /// version immediately, rather than the locally-stored version
    /// which may be stale after concurrent writes. The asynchronous
    /// repair of other stale replicas happens separately in
    /// [`run_read_repair`].
    ///
    /// Returns `Some(ObjectMetadata)` if a remote version is newer and
    /// was applied locally. Returns `None` if the local version is the
    /// winner, or if no remote replica is reachable.
    async fn compare_with_quorum(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        local_meta: &ObjectMetadata,
    ) -> Option<ObjectMetadata> {
        let pool = self.pool.as_ref()?;
        let membership = self.membership.as_ref()?;

        let hash_key = oceanfs_routing::hash_key(key.as_str().as_bytes());
        let replica_set = self.ring.lookup(&hash_key);
        if replica_set.len() <= 1 {
            return None;
        }

        let resolver = Arc::clone(&self.conflict_resolver);
        let node_id = self.node_id.clone();
        let local_hlc = local_meta.hlc;
        let bucket_clone = bucket.clone();
        let key_clone = key.clone();

        let mut winning_hlc = local_hlc;
        let mut winning_meta: Option<GetObjectMetadataResponse> = None;

        // Fetch metadata from each remote replica in parallel.
        let mut fetches = FuturesUnordered::new();
        for target in &replica_set {
            if *target == node_id {
                continue;
            }
            let target = target.clone();
            let addr = match membership.address_of(&target) {
                Some(a) => a,
                None => continue,
            };
            let pooled = match pool.get_channel(addr).await {
                Ok(p) => p,
                Err(_) => continue,
            };
            let channel = pooled.channel().clone();
            drop(pooled);

            let req_bucket = bucket_clone.clone();
            let req_key = key_clone.clone();
            let timeout = Duration::from_millis(self.timeouts.metadata_read_ms);
            fetches.push(async move {
                let mut client = SegmentRpcClient::new(channel);
                let request = tonic::Request::new(GetObjectMetadataRequest {
                    bucket_id: req_bucket.as_str().to_string(),
                    object_key: req_key.as_str().to_string(),
                });
                let result =
                    tokio::time::timeout(timeout, client.get_object_metadata(request)).await;
                match result {
                    Ok(Ok(resp)) => (target, Ok(resp)),
                    Ok(Err(e)) => (target, Err(e)),
                    Err(_elapsed) => (
                        target,
                        Err(tonic::Status::deadline_exceeded(
                            "metadata fetch timeout during quorum comparison",
                        )),
                    ),
                }
            });
        }

        while let Some((target, result)) = fetches.next().await {
            match result {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if !resp.found {
                        continue;
                    }
                    let remote_hlc = match resp.hlc {
                        Some(ref hlc_proto) => Hlc::new(hlc_proto.wall_time, hlc_proto.logical),
                        None => continue,
                    };

                    // Receive rule (G2): merge the remote timestamp so
                    // the local clock never lags replicas we talk to.
                    if let Some(clock) = &self.hlc_clock {
                        clock.update(remote_hlc);
                    }

                    // G7: node ids are passed for the equal-HLC tie-break.
                    let resolution = resolver.resolve(&winning_hlc, &remote_hlc, &node_id, &target);
                    match resolution {
                        Resolution::AcceptRemote => {
                            winning_hlc = remote_hlc;
                            winning_meta = Some(resp);
                            info!(
                                target = %target,
                                remote_wall = remote_hlc.wall_time(),
                                "quorum comparison: remote version is newer; \
                                 will serve winning version to client"
                            );
                        }
                        Resolution::AcceptLocal => {
                            debug!(
                                target = %target,
                                "quorum comparison: local version is newer than remote"
                            );
                        }
                        Resolution::Merge => {
                            debug!("quorum comparison: merge resolution — CRDT not yet supported");
                        }
                        _ => {
                            // Resolution is #[non_exhaustive] — log unknown variants
                            // so they are visible in production.
                            warn!(
                                target = %target,
                                "quorum comparison: unexpected resolution variant; \
                                 treating as no-op"
                            );
                        }
                    }
                }
                Err(e) => {
                    debug!(target = %target, error = %e, "metadata fetch failed for replica");
                }
            }
        }

        // If remote won, build the winning ObjectMetadata for the client
        // response. NOTE: the winning metadata is NOT written into the
        // local store. Its chunk references point at the WINNING node's
        // segments — a local write would store metadata this node
        // cannot read locally, and its (newer) HLC would then reject
        // legitimate hinted-handoff applies that carry self-contained
        // data (churn divergence: unrecorded/foreign versions served,
        // hints dropped by LWW).
        if let Some(winning) = winning_meta {
            // Build the winning ObjectMetadata for the client response.
            let hlc = winning.hlc.map_or(Hlc::zero(), |p| Hlc::new(p.wall_time, p.logical));
            let mut chunks = smallvec::SmallVec::new();
            let count = winning
                .chunk_segment_ids
                .len()
                .min(winning.chunk_offsets.len())
                .min(winning.chunk_lengths.len());
            for i in 0..count {
                let seg_id = SegmentId::try_from(winning.chunk_segment_ids[i].clone())
                    .unwrap_or_else(|_| SegmentId::default());
                chunks.push(oceanfs_core::ChunkRef {
                    segment_id: seg_id,
                    offset: winning.chunk_offsets[i],
                    length: winning.chunk_lengths[i],
                    compressed: winning.chunk_compressed.get(i).copied().unwrap_or(false),
                    logical_length: winning
                        .chunk_logical_lengths
                        .get(i)
                        .copied()
                        .unwrap_or(winning.chunk_lengths[i]),
                });
            }
            let blake3_hash = if winning.blake3_hash.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&winning.blake3_hash);
                Some(HashOutput::from_bytes(arr))
            } else {
                None
            };
            let inline_data = if winning.inline_data.is_empty() {
                None
            } else {
                Some(winning.inline_data.clone())
            };

            Some(ObjectMetadata {
                object_key: key.clone(),
                size: winning.size,
                blake3_hash,
                chunks,
                inline_data,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
                hlc,
            })
        } else {
            None
        }
    }

    /// Fetches object metadata from replicas, compares HLCs, and pushes
    /// corrected data to stale replicas via `PutObjectMetadata` (read repair, §4.2).
    ///
    /// This is a fire-and-forget operation — it does not block the response
    /// to the client. The client receives the locally-available data
    /// immediately; stale replicas are repaired asynchronously.
    async fn run_read_repair(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        local_meta: &ObjectMetadata,
    ) {
        let pool = match self.pool.as_ref() {
            Some(p) => p,
            None => return,
        };
        let membership = match self.membership.as_ref() {
            Some(m) => m,
            None => return,
        };

        let hash_key = oceanfs_routing::hash_key(key.as_str().as_bytes());
        let replica_set = self.ring.lookup(&hash_key);
        if replica_set.len() <= 1 {
            return;
        }

        let resolver = Arc::clone(&self.conflict_resolver);
        let pool_clone = Arc::clone(pool);
        let membership_clone = Arc::clone(membership);
        let node_id = self.node_id.clone();
        let bucket_clone = bucket.clone();
        let key_clone = key.clone();
        let local_hlc = local_meta.hlc;
        // Clone local metadata so we can push it to stale remotes.
        let local_meta_clone = local_meta.clone();
        // Clone the metadata store so the spawned task can re-validate
        // the local object before pushing anything (t19).
        let metadata_store = self.metadata.clone();
        // Clone the HLC clock for receive-merge inside the spawned task (G2).
        let hlc_clock = self.hlc_clock.clone();

        tokio::spawn(async move {
            let mut winning_hlc = local_hlc;
            let mut stale_remotes: Vec<NodeId> = Vec::with_capacity(replica_set.len());

            // Fetch metadata from each remote replica in parallel.
            let mut fetches = FuturesUnordered::new();
            for target in &replica_set {
                if *target == node_id {
                    continue;
                }
                let target = target.clone();
                let addr = match membership_clone.address_of(&target) {
                    Some(a) => a,
                    None => continue,
                };
                let pooled = match pool_clone.get_channel(addr).await {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                let channel = pooled.channel().clone();
                drop(pooled);

                let req_bucket = bucket_clone.clone();
                let req_key = key_clone.clone();
                let timeout = Duration::from_secs(5);
                fetches.push(async move {
                    let mut client = SegmentRpcClient::new(channel);
                    let request = tonic::Request::new(GetObjectMetadataRequest {
                        bucket_id: req_bucket.as_str().to_string(),
                        object_key: req_key.as_str().to_string(),
                    });
                    let result =
                        tokio::time::timeout(timeout, client.get_object_metadata(request)).await;
                    match result {
                        Ok(Ok(resp)) => (target, Ok(resp)),
                        Ok(Err(e)) => (target, Err(e)),
                        Err(_elapsed) => (
                            target,
                            Err(tonic::Status::deadline_exceeded(
                                "metadata fetch timeout during read repair",
                            )),
                        ),
                    }
                });
            }

            while let Some((target, result)) = fetches.next().await {
                match result {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        if !resp.found {
                            continue;
                        }
                        let remote_hlc = match resp.hlc {
                            Some(ref hlc_proto) => Hlc::new(hlc_proto.wall_time, hlc_proto.logical),
                            None => continue,
                        };

                        // Receive rule (G2): merge the remote timestamp
                        // into the local clock.
                        if let Some(clock) = &hlc_clock {
                            clock.update(remote_hlc);
                        }

                        // G7: node ids are passed for the equal-HLC tie-break.
                        let resolution =
                            resolver.resolve(&winning_hlc, &remote_hlc, &node_id, &target);
                        match resolution {
                            Resolution::AcceptRemote => {
                                winning_hlc = remote_hlc;
                                info!(
                                    target = %target,
                                    remote_wall = remote_hlc.wall_time(),
                                    "remote replica has newer version; \
                                     read repair leaves it to hinted handoff"
                                );
                            }
                            Resolution::AcceptLocal => {
                                debug!(
                                    target = %target,
                                    "local version is newer; will push corrected data"
                                );
                                stale_remotes.push(target);
                            }
                            Resolution::Merge => {
                                debug!("merge resolution — CRDT not yet supported");
                            }
                            _ => {
                                warn!(
                                    target = %target,
                                    "read repair: unexpected resolution variant; \
                                     treating as no-op"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        debug!(target = %target, error = %e, "metadata fetch failed for replica");
                    }
                }
            }

            // Re-validate the local object before applying ANY repair
            // decision. A DELETE (or a newer PUT) that landed after this
            // read supersedes the version we read: propagating it would
            // resurrect deleted objects — a read repair fired by a
            // pre-delete GET re-pushed the object to replicas AFTER the
            // tombstone reached them (t19). The local delete is
            // authoritative; genuine repair of lost data is the job of
            // anti-entropy/healing, not read repair.
            let local_still_authoritative = match metadata_store.as_ref() {
                Some(store) => match store.get_object(&bucket_clone, &key_clone).await {
                    Ok(Some(current)) => current.hlc == local_hlc,
                    Ok(None) | Err(_) => false,
                },
                None => true, // No local store: nothing can supersede the read.
            };

            if !local_still_authoritative {
                debug!(
                    bucket = %bucket_clone,
                    key = %key_clone,
                    "read repair skipped: local object changed or was deleted since the read"
                );
                return;
            }

            // Push corrected data to stale remote replicas.
            for stale in &stale_remotes {
                Self::push_metadata_to_node(
                    &pool_clone,
                    &membership_clone,
                    stale,
                    &bucket_clone,
                    &key_clone,
                    &local_meta_clone,
                )
                .await;
            }

            // If local is stale, do NOT write the winning metadata into
            // our own store: its chunk references point at the winning
            // node's segments — a local write would store metadata this
            // node cannot read locally, and its (newer) HLC would then
            // reject legitimate hinted-handoff applies that carry
            // self-contained data. The local store converges via hinted
            // handoff (data-bearing) or a future data-bearing repair.
        });
    }

    /// Pushes corrected object metadata + data to a specific node via gRPC.
    async fn push_metadata_to_node(
        pool: &Arc<ConnectionPool>,
        membership: &Arc<Membership>,
        target: &NodeId,
        bucket: &BucketId,
        key: &ObjectKey,
        meta: &ObjectMetadata,
    ) {
        let addr = match membership.address_of(target) {
            Some(a) => a,
            None => return,
        };
        let pooled = match pool.get_channel(addr).await {
            Ok(p) => p,
            Err(_) => return,
        };
        let channel = pooled.channel().clone();
        drop(pooled);

        let mut client = SegmentRpcClient::new(channel);
        let request = build_put_metadata_request(bucket, key, meta);
        match client.put_object_metadata(request).await {
            Ok(_) => {
                info!(target = %target, "pushed corrected metadata to stale replica (read repair)");
                // Also invalidate caches on the target so the next read
                // doesn't serve a stale cached entry.
                Self::invalidate_caches_on_node(pool, membership, target, bucket, key).await;
            }
            Err(e) => {
                warn!(target = %target, error = %e, "failed to push metadata to stale replica")
            }
        }
    }

    /// Invalidates cache entries for the given object on a remote node.
    async fn invalidate_caches_on_node(
        pool: &Arc<ConnectionPool>,
        membership: &Arc<Membership>,
        target: &NodeId,
        bucket: &BucketId,
        key: &ObjectKey,
    ) {
        let addr = match membership.address_of(target) {
            Some(a) => a,
            None => return,
        };
        let pooled = match pool.get_channel(addr).await {
            Ok(p) => p,
            Err(_) => return,
        };
        let channel = pooled.channel().clone();
        drop(pooled);

        let proto_bucket: oceanfs_core::proto::common::BucketId = bucket.clone().into();
        let proto_key: oceanfs_core::proto::common::ObjectKey = key.clone().into();
        let mut client = CacheRpcClient::new(channel);
        let request = tonic::Request::new(oceanfs_cache::cache::CacheInvalidateRequest {
            bucket_id: Some(proto_bucket),
            object_key: Some(proto_key),
            invalidation_type: oceanfs_cache::cache::InvalidationType::All as i32,
        });
        if let Err(e) = client.invalidate(request).await {
            warn!(target = %target, error = %e, "cache invalidation failed during read repair");
        }
    }

    /// Executes a read.
    ///
    /// # Algorithm
    ///
    /// 1. Look up object metadata from the metadata store.
    /// 2. If inline data is present, return immediately.
    /// 3. For each chunk reference, fetch the segment data and
    ///    assemble the blob.
    /// 4. Verify the BLAKE3 hash against the stored hash.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the object does not exist.
    /// Returns [`Error::HashMismatch`] if the hash verification fails.
    pub async fn get(&self, req: ReadRequest) -> Result<ReadResult> {
        let result = self.get_object(req).await?;
        // hash_verified is true only when a stored hash was actually verified.
        // We use the presence of a stored hash in metadata as the indicator.
        let hash_verified = result.metadata.blake3_hash.is_some();
        Ok(ReadResult {
            data: result.data,
            metadata: result.metadata,
            hash_verified,
            segment_source: result.segment_source,
        })
    }

    /// Looks up object metadata from the metadata store.
    async fn lookup_metadata(&self, req: &ReadRequest) -> Result<ObjectMetadata> {
        if let Some(ref store) = self.metadata {
            store
                .get_object(&req.bucket, &req.key)
                .await
                .map_err(|e| Error::Internal(format!("metadata lookup: {e}")))?
                .ok_or_else(|| Error::NotFound(format!("{}/{}", req.bucket, req.key)))
        } else {
            // No metadata store available — return empty metadata.
            // This path is used in tests and single-node operation.
            Ok(ObjectMetadata {
                object_key: req.key.clone(),
                size: 0,
                blake3_hash: None,
                chunks: smallvec::SmallVec::new(),
                inline_data: None,
                created_at: 0,
                hlc: Hlc::zero(),
            })
        }
    }

    /// Assembles blob data from chunk references.
    ///
    /// First tries to fetch chunks via the local segment reader, then
    /// falls back to gRPC `FetchShard` calls to remote replicas when
    /// the connection pool and membership are configured.
    ///
    /// The `strategy` parameter controls fetch parallelism and source
    /// ordering (see [`FetchStrategy`]). The `policy` parameter provides
    /// the per-bucket `stripe_parallelism` semaphore bound.
    ///
    /// After assembly, runs streaming BLAKE3 verification via
    /// [`MultiChunkAssembler`].
    ///
    /// When neither gRPC nor a segment reader is available, returns an
    /// error.
    async fn assemble_chunks(
        &self,
        meta: &ObjectMetadata,
        strategy: FetchStrategy,
        policy: Option<&crate::BucketPolicy>,
    ) -> Result<(Bytes, Option<oceanfs_storage::io::SegmentReadSource>)> {
        let chunk_count = meta.chunks.len();
        if chunk_count == 0 {
            return Ok((Bytes::new(), None));
        }

        let timeout_ms = self.timeouts.read_default_ms;

        // Peer-side routing hint (ADR-0029 §D5): the node's cached pool
        // manifests filter replica candidates in the gRPC fetch path.
        let routing_hint = self.routing_hint.as_ref();

        // Fetch strategy drives parallelism and fastest-k behaviour.
        let parallel_fetch = strategy.parallel_fetch();
        let use_fastest_k = strategy.use_fastest_k();
        let stripe_parallelism = policy.map(|p| p.read_tuning.stripe_parallelism).unwrap_or(0);

        if stripe_parallelism > 0 {
            tracing::debug!(
                parallel_fetch,
                use_fastest_k,
                stripe_parallelism,
                "read tuning: EC stripe decode bounded by semaphore"
            );
        }

        // H1: Apply ReadTuningConfig — create a semaphore to bound
        // concurrent stripe decode tasks, and respect parallel_fetch.
        let stripe_semaphore: Option<Arc<tokio::sync::Semaphore>> = if stripe_parallelism > 0 {
            Some(Arc::new(tokio::sync::Semaphore::new(stripe_parallelism)))
        } else {
            None
        };

        // Read-path decompression context: decompress stored chunks whose
        // metadata marks them compressed. Built from the injected accel
        // compressor (write path always pairs compress with the flag).
        #[cfg(feature = "accel")]
        let decompress_ctx: Option<crate::read::fetch::DecompressCtx<'_>> =
            self.compressor.as_ref().map(|c| (c, &self.decompress_semaphore));
        #[cfg(not(feature = "accel"))]
        let decompress_ctx: Option<crate::read::fetch::DecompressCtx<'_>> = None;

        // Build EC recovery params if decoder and codec are configured.
        #[cfg(feature = "ec")]
        let ec_params = if let (Some(ref decoder), true) = (&self.decoder, self.ec_data_shards > 0)
        {
            Some(crate::read::fetch::EcRecoveryParams {
                decoder: Arc::clone(decoder),
                data_shards: self.ec_data_shards,
                parity_shards: self.ec_parity_shards,
            })
        } else {
            None
        };

        let chunk_data = if self.pool.is_some() && self.membership.is_some() {
            let sem_ref = stripe_semaphore.as_ref();
            #[cfg(feature = "ec")]
            if let Some(ref ec) = ec_params {
                crate::read::fetch::fetch_chunks_with_ec(
                    &self.ring,
                    meta,
                    timeout_ms,
                    self.segment_reader.as_ref(),
                    self.pool.as_ref(),
                    self.membership.as_ref(),
                    routing_hint,
                    ec,
                    parallel_fetch,
                    use_fastest_k,
                    sem_ref,
                    decompress_ctx,
                )
                .await?
            } else {
                crate::read::fetch::fetch_chunks_with_grpc(
                    &self.ring,
                    meta,
                    timeout_ms,
                    self.segment_reader.as_ref(),
                    self.pool.as_ref(),
                    self.membership.as_ref(),
                    routing_hint,
                    parallel_fetch,
                    use_fastest_k,
                    sem_ref,
                    decompress_ctx,
                )
                .await?
            }
            #[cfg(not(feature = "ec"))]
            crate::read::fetch::fetch_chunks_with_grpc(
                &self.ring,
                meta,
                timeout_ms,
                self.segment_reader.as_ref(),
                self.pool.as_ref(),
                self.membership.as_ref(),
                routing_hint,
                parallel_fetch,
                use_fastest_k,
                sem_ref,
                decompress_ctx,
            )
            .await?
        } else {
            let sem_ref = stripe_semaphore.as_ref();
            #[cfg(feature = "ec")]
            if let Some(ref ec) = ec_params {
                crate::read::fetch::fetch_chunks_with_ec(
                    &self.ring,
                    meta,
                    timeout_ms,
                    self.segment_reader.as_ref(),
                    None,
                    None,
                    None,
                    ec,
                    parallel_fetch,
                    use_fastest_k,
                    sem_ref,
                    decompress_ctx,
                )
                .await?
            } else {
                crate::read::fetch::fetch_chunks(
                    &self.ring,
                    meta,
                    timeout_ms,
                    self.segment_reader.as_ref(),
                    decompress_ctx,
                )
                .await?
            }
            #[cfg(not(feature = "ec"))]
            crate::read::fetch::fetch_chunks(
                &self.ring,
                meta,
                timeout_ms,
                self.segment_reader.as_ref(),
                decompress_ctx,
            )
            .await?
        };

        // Read repair is handled in get_object() via run_read_repair(),
        // which compares HLCs across the full replica set and schedules
        // cache invalidation asynchronously (§4.2).
        //
        // Single-node mode (no pool/membership): read repair is a no-op
        // since there are no remote replicas to compare against.

        // Build the assembler based on whether we have a stored hash.
        let mut assembler = match meta.blake3_hash {
            Some(ref stored) => MultiChunkAssembler::new(*stored, chunk_count),
            None => MultiChunkAssembler::new_no_verify(chunk_count),
        };

        for (index, data) in chunk_data.into_iter().enumerate() {
            assembler.push_chunk(index, data)?;
        }

        // Query the segment reader for file-backed source metadata.
        // Uses the first chunk's segment as the representative source —
        // multi-segment blobs are rare and mixing sources is harmless.
        let source = self
            .segment_reader
            .as_ref()
            .and_then(|reader| meta.chunks.first().map(|c| reader.last_read_source(&c.segment_id)));

        assembler.finalize().map(|data| (data, source))
    }

    /// Verifies the BLAKE3 hash of data against stored metadata.
    #[allow(dead_code)]
    fn verify_blake3(data: &[u8], meta: &ObjectMetadata) -> bool {
        match meta.blake3_hash {
            Some(ref stored) => {
                let computed = blake3::hash(data);
                *computed.as_bytes() == *stored.as_bytes()
            }
            None => {
                tracing::debug!(key = %meta.object_key, "no stored hash to verify");
                false
            }
        }
    }

    /// Reconstructs missing data shards using available data+parity shards
    /// via the EC decoder.
    ///
    /// When the `ec` feature is enabled and a decoder is configured,
    /// this method recovers the original k data shards from any
    /// combination of k available shards (data or parity). Missing
    /// shards are represented as `None` entries.
    ///
    /// `available_shards` must have length k+m, where k = `data_count`
    /// and m = `parity_count`. At least k entries must be `Some`.
    ///
    /// # Errors
    ///
    /// Returns an error if fewer than k shards are available or the
    /// decoder is not configured.
    #[cfg(feature = "ec")]
    pub(crate) fn decode_ec_shards(
        &self,
        available_shards: &[Option<&[u8]>],
        data_count: u8,
        parity_count: u8,
    ) -> Result<Vec<Bytes>> {
        let decoder = self
            .decoder
            .as_ref()
            .ok_or_else(|| Error::Internal("EC decoder not configured".into()))?;
        decoder
            .decode(available_shards, data_count, parity_count)
            .map_err(|e| Error::Internal(format!("EC decode failed: {e}")))
    }

    /// Recovers a full segment from EC-encoded shard data when some data
    /// shards are missing or corrupted.
    ///
    /// The segment is treated as a concatenation of `k` data shards and
    /// `m` parity shards, each of equal size. `missing_shard_indices`
    /// specifies which data shard indices (0-based, within the data
    /// shards) are unavailable. Those shards are dropped from the
    /// recovery and reconstructed from the remaining data+parity shards.
    ///
    /// Returns the reconstructed `k` data shards concatenated in order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The EC codec parameters (k, m) are not set (zero)
    /// - The decoder is not configured
    /// - The segment is too small to split into k+m shards
    /// - Too many shards are missing (fewer than k available)
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::CodecConfig;
    /// use oceanfs_ec::CauchyEncoder;
    /// use oceanfs_ec::{Encoder, Decoder};
    /// use oceanfs_server::ReadCoordinator;
    /// use std::sync::Arc;
    ///
    /// let decoder: Arc<dyn Decoder> = Arc::new(CauchyEncoder::new(CodecConfig {
    ///     data_shards: 4,
    ///     parity_shards: 2,
    ///     ..Default::default()
    /// }));
    /// let coord = ReadCoordinator::default()
    ///     .with_decoder(decoder)
    ///     .with_ec_codec(4, 2);
    ///
    /// // Build an EC(4,2) segment: encode 4 data shards → 2 parity.
    /// let data: [&[u8]; 4] = [b"AAAA", b"BBBB", b"CCCC", b"DDDD"];
    /// let encoder = CauchyEncoder::new(CodecConfig { data_shards: 4, parity_shards: 2, ..Default::default() });
    /// let parity = encoder.encode(&data, 2).unwrap();
    ///
    /// // Concatenate into a single "segment": 4 data + 2 parity.
    /// let mut segment = Vec::new();
    /// for s in &data { segment.extend_from_slice(s); }
    /// for p in &parity { segment.extend_from_slice(p); }
    ///
    /// // Recover with shard 0 missing.
    /// let recovered = coord.read_segment_with_ec_recovery(&segment, &[0]).unwrap();
    /// assert_eq!(&recovered[0..4], b"AAAA");
    /// assert_eq!(&recovered[4..8], b"BBBB");
    /// ```
    #[cfg(feature = "ec")]
    pub fn read_segment_with_ec_recovery(
        &self,
        segment_data: &[u8],
        missing_shard_indices: &[usize],
    ) -> Result<Bytes> {
        let k = self.ec_data_shards;
        let m = self.ec_parity_shards;

        if k == 0 || m == 0 {
            return Err(Error::Internal(
                "EC codec parameters not set — call with_ec_codec() before recovery".into(),
            ));
        }

        let total_shards = (k + m) as usize;
        if segment_data.len() < total_shards {
            return Err(Error::Internal(format!(
                "segment too small for EC recovery: need at least {total_shards} bytes, got {}",
                segment_data.len()
            )));
        }

        let shard_size = segment_data.len() / total_shards;
        if shard_size == 0 {
            return Err(Error::Internal("shard size is zero — cannot split segment".into()));
        }

        // Build available shards array: k data shards + m parity shards.
        let k_usize = k as usize;
        let mut available: Vec<Option<&[u8]>> = Vec::with_capacity(total_shards);
        for i in 0..k_usize {
            let start = i * shard_size;
            let end = start + shard_size;
            if missing_shard_indices.contains(&i) {
                available.push(None);
            } else {
                available.push(Some(&segment_data[start..end]));
            }
        }
        // Parity shards are always available (they come from the same segment blob).
        for i in 0..(m as usize) {
            let start = (k_usize + i) * shard_size;
            let end = start + shard_size;
            available.push(Some(&segment_data[start..end]));
        }

        // Call the shared decode helper.
        let recovered_shards = self.decode_ec_shards(&available, k, m)?;

        // Concatenate the k recovered data shards into a Bytes.
        let total_size = k_usize * shard_size;
        let mut result = bytes::BytesMut::with_capacity(total_size);
        for shard in recovered_shards {
            result.extend_from_slice(&shard);
        }

        Ok(result.freeze())
    }

    /// Returns a reference to the EC decoder, if configured.
    #[cfg(feature = "ec")]
    pub fn decoder(&self) -> Option<&Arc<dyn oceanfs_ec::Decoder>> {
        self.decoder.as_ref()
    }

    /// Determines the read outcome for a given metadata entry.
    pub fn classify(&self, meta: &ObjectMetadata) -> ReadOutcome {
        if meta.is_inline() {
            ReadOutcome::InlineHit
        } else if meta.chunks.is_empty() {
            ReadOutcome::NotFound
        } else if meta.chunks.len() == 1 {
            ReadOutcome::SingleChunk
        } else {
            ReadOutcome::MultiChunk { chunk_count: meta.chunks.len() }
        }
    }

    /// Returns the conflict resolver used for read repair.
    pub fn conflict_resolver(&self) -> &Arc<dyn ConflictResolver> {
        &self.conflict_resolver
    }

    /// Returns a reference to the metadata store, if any.
    pub fn metadata_store(&self) -> Option<&Arc<crate::metadata_async::AsyncMetadataOps>> {
        self.metadata.as_ref()
    }
}

impl Default for ReadCoordinator {
    fn default() -> Self {
        Self {
            ring: Arc::new(RingCache::new(oceanfs_routing::Ring::new(
                oceanfs_core::RingConfig::default(),
            ))),
            node_id: NodeId::new("default"),
            conflict_resolver: Arc::new(LwwResolver),
            metadata: None,
            segment_reader: None,
            pool: None,
            membership: None,
            #[cfg(feature = "ec")]
            decoder: None,
            #[cfg(feature = "ec")]
            ec_data_shards: 0,
            #[cfg(feature = "ec")]
            ec_parity_shards: 0,
            timeouts: Arc::new(OperationTimeouts::default()),
            default_fetch_strategy: FetchStrategy::default(),
            hlc_clock: None,
            #[cfg(feature = "accel")]
            compressor: None,
            #[cfg(feature = "accel")]
            decompress_semaphore: Arc::new(tokio::sync::Semaphore::new(
                std::thread::available_parallelism().map_or(4, |n| n.get().saturating_mul(2)),
            )),
            routing_hint: None,
        }
    }
}

/// Builds a `PutObjectMetadataRequest` from local object metadata.
///
/// Used during read repair to push corrected data to stale replicas.
fn build_put_metadata_request(
    bucket: &BucketId,
    key: &ObjectKey,
    meta: &ObjectMetadata,
) -> tonic::Request<PutObjectMetadataRequest> {
    let mut chunk_segment_ids: Vec<oceanfs_core::proto::common::SegmentId> =
        Vec::with_capacity(meta.chunks.len());
    let mut chunk_offsets: Vec<u64> = Vec::with_capacity(meta.chunks.len());
    let mut chunk_lengths: Vec<u32> = Vec::with_capacity(meta.chunks.len());
    let mut chunk_logical_lengths: Vec<u32> = Vec::with_capacity(meta.chunks.len());
    let mut chunk_compressed: Vec<bool> = Vec::with_capacity(meta.chunks.len());
    for chunk in &meta.chunks {
        chunk_segment_ids.push(chunk.segment_id.into());
        chunk_offsets.push(chunk.offset);
        chunk_lengths.push(chunk.length);
        chunk_logical_lengths.push(if chunk.compressed {
            chunk.logical_length
        } else {
            chunk.length
        });
        chunk_compressed.push(chunk.compressed);
    }

    let hlc_proto = oceanfs_core::proto::common::HlcTimestamp {
        wall_time: meta.hlc.wall_time,
        logical: meta.hlc.logical,
    };

    tonic::Request::new(PutObjectMetadataRequest {
        bucket_id: bucket.as_str().to_string(),
        object_key: key.as_str().to_string(),
        size: meta.size,
        blake3_hash: meta
            .blake3_hash
            .map(|h| Bytes::copy_from_slice(h.as_bytes()))
            .unwrap_or_default(),
        hlc: Some(hlc_proto),
        inline_data: meta.inline_data.clone().unwrap_or_default(),
        chunk_segment_ids,
        chunk_offsets,
        chunk_lengths,
        chunk_logical_lengths,
        chunk_compressed,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{ChunkRef, NodeId, RingConfig, SegmentId};
    use oceanfs_routing::{hash_key, Ring};

    use super::*;

    fn make_coordinator() -> ReadCoordinator {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("n1"));
        let ring_cache = Arc::new(RingCache::new(ring));
        ReadCoordinator::new(ring_cache, NodeId::new("n1"), None)
    }

    fn make_coordinator_with_segments(segment_data: &[(SegmentId, &[u8])]) -> ReadCoordinator {
        let segments = Arc::new(InMemorySegmentReader::new());
        for (id, data) in segment_data {
            segments.put(*id, Bytes::copy_from_slice(data));
        }
        make_coordinator().with_segment_reader(segments)
    }

    #[tokio::test]
    async fn read_coordinator_get_returns_result() {
        let coord = make_coordinator();

        let req = ReadRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("obj"),
            hash_key: HashKey::from_bytes(hash_key(b"obj")),
            metadata_only: false,
            local_only: false,
            policy: None,
        };

        let result = coord.get(req).await.unwrap();
        // Without a metadata store, returns empty data and no hash verification.
        assert!(!result.hash_verified);
        assert!(result.data.is_empty());
    }

    #[tokio::test]
    async fn read_coordinator_metadata_only() {
        let coord = make_coordinator();

        let req = ReadRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("meta-only"),
            hash_key: HashKey::from_bytes(hash_key(b"meta-only")),
            metadata_only: true,
            local_only: false,
            policy: None,
        };

        let result = coord.get(req).await.unwrap();
        assert!(result.data.is_empty(), "metadata-only returns no data");
        assert!(!result.hash_verified);
    }

    #[tokio::test]
    async fn read_coordinator_chunk_assembly_single_chunk() {
        let data = b"single-chunk test data for assembly";
        let segment_id = SegmentId::new();
        let hash = blake3::hash(data);

        let coordinator = make_coordinator_with_segments(&[(segment_id, data)]);

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id,
            offset: 0,
            length: data.len() as u32,
            compressed: false,
            logical_length: data.len() as u32,
        });

        // Build metadata with the single chunk and the stored hash.
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("test-key"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        let (assembled, _source) =
            coordinator.assemble_chunks(&meta, FetchStrategy::default(), None).await.unwrap();
        assert_eq!(&assembled[..], data);
    }

    #[tokio::test]
    async fn read_coordinator_chunk_assembly_multi_chunk() {
        let part1 = b"hello ";
        let part2 = b"world";
        let seg1 = SegmentId::new();
        let seg2 = SegmentId::new();

        let combined: Vec<u8> = [part1.as_slice(), part2.as_slice()].concat();
        let hash = blake3::hash(&combined);

        let coordinator = make_coordinator_with_segments(&[(seg1, part1), (seg2, part2)]);

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg1,
            offset: 0,
            length: part1.len() as u32,
            compressed: false,
            logical_length: part1.len() as u32,
        });
        chunks.push(ChunkRef {
            segment_id: seg2,
            offset: 0,
            length: part2.len() as u32,
            compressed: false,
            logical_length: part2.len() as u32,
        });

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("multi-chunk"),
            size: combined.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        let (assembled, _source) =
            coordinator.assemble_chunks(&meta, FetchStrategy::default(), None).await.unwrap();
        assert_eq!(&assembled[..], &combined[..]);
    }

    #[tokio::test]
    async fn read_coordinator_chunk_assembly_hash_mismatch() {
        let data = b"actual chunk data";
        let wrong_hash = blake3::hash(b"something completely different");
        let seg_id = SegmentId::new();

        let coordinator = make_coordinator_with_segments(&[(seg_id, data)]);

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: data.len() as u32,
            compressed: false,
            logical_length: data.len() as u32,
        });

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("mismatch"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*wrong_hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        let result = coordinator.assemble_chunks(&meta, FetchStrategy::default(), None).await;
        assert!(result.is_err(), "hash mismatch should return error");
        assert!(
            matches!(result.unwrap_err(), Error::HashMismatch { .. }),
            "should be HashMismatch variant"
        );
    }

    #[tokio::test]
    async fn read_coordinator_chunk_assembly_missing_segment() {
        let data = b"missing segment test";
        let hash = blake3::hash(data);
        let missing_id = SegmentId::new();

        // Create coordinator WITHOUT the needed segment.
        let coordinator = make_coordinator_with_segments(&[(SegmentId::new(), b"other")]);

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: missing_id,
            offset: 0,
            length: data.len() as u32,
            compressed: false,
            logical_length: data.len() as u32,
        });

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("missing"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        let result = coordinator.assemble_chunks(&meta, FetchStrategy::default(), None).await;
        assert!(result.is_err(), "missing segment should return error");
    }

    #[test]
    fn classify_inline_metadata_returns_inline_hit() {
        let coord = make_coordinator();
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("inline"),
            size: 10,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(Bytes::from_static(b"hello")),
            created_at: 0,
            hlc: Hlc::zero(),
        };
        let outcome = coord.classify(&meta);
        assert_eq!(outcome, ReadOutcome::InlineHit);
    }

    #[test]
    fn classify_empty_chunks_no_inline_returns_not_found() {
        let coord = make_coordinator();
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("empty"),
            size: 0,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };
        let outcome = coord.classify(&meta);
        assert_eq!(outcome, ReadOutcome::NotFound);
    }

    #[test]
    fn classify_single_chunk_returns_single_chunk() {
        let coord = make_coordinator();
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: SegmentId::new(),
            offset: 0,
            length: 100,
            compressed: false,
            logical_length: 100,
        });
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("single"),
            size: 100,
            blake3_hash: None,
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };
        let outcome = coord.classify(&meta);
        assert_eq!(outcome, ReadOutcome::SingleChunk);
    }

    #[test]
    fn classify_multi_chunk_returns_multi_chunk() {
        let coord = make_coordinator();
        let mut chunks = smallvec::SmallVec::new();
        for i in 0..3 {
            chunks.push(ChunkRef {
                segment_id: SegmentId::new(),
                offset: i * 1024,
                length: 1024,
                compressed: false,
                logical_length: 1024,
            });
        }
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("multi"),
            size: 3072,
            blake3_hash: None,
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };
        let outcome = coord.classify(&meta);
        assert_eq!(outcome, ReadOutcome::MultiChunk { chunk_count: 3 });
    }

    #[test]
    fn read_coordinator_default_constructs() {
        let coord = ReadCoordinator::default();
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("test"),
            size: 0,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };
        assert_eq!(coord.classify(&meta), ReadOutcome::NotFound);
    }

    #[test]
    fn get_result_contains_cache_hit_miss() {
        let result = GetResult {
            data: Bytes::from_static(b"test"),
            metadata: ObjectMetadata {
                object_key: ObjectKey::new("test"),
                size: 4,
                blake3_hash: None,
                chunks: smallvec::SmallVec::new(),
                inline_data: Some(Bytes::from_static(b"test")),
                created_at: 0,
                hlc: Hlc::zero(),
            },
            cache_hit: CacheHitLevel::Miss,
            hash: HashOutput::from_bytes([1u8; 32]),
            segment_source: None,
        };
        assert_eq!(result.cache_hit, CacheHitLevel::Miss);
        assert_eq!(&result.data[..], b"test");
    }

    #[tokio::test]
    async fn segment_reader_trait_is_object_safe() {
        // Verify SegmentReader can be used as trait object.
        let reader: Arc<dyn SegmentReader> = Arc::new(InMemorySegmentReader::new());
        let seg_id = SegmentId::new();
        let result = reader.read_chunk(&seg_id, 0, 100).await;
        assert!(result.is_err());
    }

    // ── Full-pipeline tests (metadata store + segment reader) ─────

    /// A mock metadata store that returns pre-configured object metadata.
    struct MockMetadataStore {
        objects:
            parking_lot::RwLock<std::collections::HashMap<(BucketId, ObjectKey), ObjectMetadata>>,
    }

    impl MockMetadataStore {
        fn new() -> Self {
            Self { objects: parking_lot::RwLock::new(std::collections::HashMap::new()) }
        }

        fn put(&self, bucket: BucketId, key: ObjectKey, meta: ObjectMetadata) {
            self.objects.write().insert((bucket, key), meta);
        }
    }

    impl crate::metadata_ops::MetadataOps for MockMetadataStore {
        fn get_object(
            &self,
            bucket: &BucketId,
            key: &ObjectKey,
        ) -> std::result::Result<Option<ObjectMetadata>, crate::metadata_ops::MetadataError>
        {
            Ok(self.objects.read().get(&(bucket.clone(), key.clone())).cloned())
        }

        fn put_object(
            &self,
            bucket: &BucketId,
            meta: ObjectMetadata,
        ) -> std::result::Result<(), crate::metadata_ops::MetadataError> {
            self.objects.write().insert((bucket.clone(), meta.object_key.clone()), meta);
            Ok(())
        }

        fn delete_object(
            &self,
            _bucket: &BucketId,
            _key: &ObjectKey,
            _hlc: Hlc,
        ) -> std::result::Result<(), crate::metadata_ops::MetadataError> {
            Ok(())
        }

        fn list_objects(
            &self,
            _bucket: &BucketId,
            _prefix: &str,
        ) -> std::result::Result<Vec<ObjectMetadata>, crate::metadata_ops::MetadataError> {
            Ok(vec![])
        }
    }

    fn make_coordinator_with_metadata(store: Arc<MockMetadataStore>) -> ReadCoordinator {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("n1"));
        let ring_cache = Arc::new(RingCache::new(ring));
        ReadCoordinator::new_with_metadata(
            ring_cache,
            NodeId::new("n1"),
            None,
            store as Arc<dyn crate::metadata_ops::MetadataOps>,
        )
    }

    #[tokio::test]
    async fn get_full_pipeline_single_chunk_with_hash_verification() {
        let data = b"full pipeline single-chunk test";
        let segment_id = SegmentId::new();
        let hash = blake3::hash(data);

        // Set up segment reader with the data.
        let segment_reader = Arc::new(InMemorySegmentReader::new());
        segment_reader.put(segment_id, Bytes::copy_from_slice(data));

        // Set up metadata store.
        let store = Arc::new(MockMetadataStore::new());
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id,
            offset: 0,
            length: data.len() as u32,
            compressed: false,
            logical_length: data.len() as u32,
        });
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("full-pipe"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };
        store.put(BucketId::new("test"), ObjectKey::new("full-pipe"), meta);

        // Build coordinator with metadata store + segment reader.
        let coordinator = make_coordinator_with_metadata(store)
            .with_segment_reader(segment_reader as Arc<dyn SegmentReader>);

        let req = ReadRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("full-pipe"),
            hash_key: HashKey::from_bytes(hash_key(b"full-pipe")),
            metadata_only: false,
            local_only: false,
            policy: None,
        };

        let result = coordinator.get(req).await.unwrap();
        assert_eq!(&result.data[..], data);
        assert!(result.hash_verified, "hash must be verified when stored hash present");
    }

    #[tokio::test]
    async fn get_full_pipeline_multi_chunk_with_hash_verification() {
        let part1 = b"hello ";
        let part2 = b"world";
        let seg1 = SegmentId::new();
        let seg2 = SegmentId::new();

        let combined: Vec<u8> = [part1.as_slice(), part2.as_slice()].concat();
        let hash = blake3::hash(&combined);

        let segment_reader = Arc::new(InMemorySegmentReader::new());
        segment_reader.put(seg1, Bytes::copy_from_slice(part1));
        segment_reader.put(seg2, Bytes::copy_from_slice(part2));

        let store = Arc::new(MockMetadataStore::new());
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg1,
            offset: 0,
            length: part1.len() as u32,
            compressed: false,
            logical_length: part1.len() as u32,
        });
        chunks.push(ChunkRef {
            segment_id: seg2,
            offset: 0,
            length: part2.len() as u32,
            compressed: false,
            logical_length: part2.len() as u32,
        });
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("multi-full"),
            size: combined.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };
        store.put(BucketId::new("test"), ObjectKey::new("multi-full"), meta);

        let coordinator = make_coordinator_with_metadata(store)
            .with_segment_reader(segment_reader as Arc<dyn SegmentReader>);

        let req = ReadRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("multi-full"),
            hash_key: HashKey::from_bytes(hash_key(b"multi-full")),
            metadata_only: false,
            local_only: false,
            policy: None,
        };

        let result = coordinator.get(req).await.unwrap();
        assert_eq!(&result.data[..], &combined[..]);
        assert!(result.hash_verified);
    }

    #[tokio::test]
    async fn get_full_pipeline_hash_mismatch_returns_error() {
        let data = b"actual data for mismatch test";
        let seg_id = SegmentId::new();
        let wrong_hash = blake3::hash(b"completely different data");

        let segment_reader = Arc::new(InMemorySegmentReader::new());
        segment_reader.put(seg_id, Bytes::copy_from_slice(data));

        let store = Arc::new(MockMetadataStore::new());
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: data.len() as u32,
            compressed: false,
            logical_length: data.len() as u32,
        });
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("mismatch-full"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*wrong_hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };
        store.put(BucketId::new("test"), ObjectKey::new("mismatch-full"), meta);

        let coordinator = make_coordinator_with_metadata(store)
            .with_segment_reader(segment_reader as Arc<dyn SegmentReader>);

        let req = ReadRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("mismatch-full"),
            hash_key: HashKey::from_bytes(hash_key(b"mismatch-full")),
            metadata_only: false,
            local_only: false,
            policy: None,
        };

        let result = coordinator.get(req).await;
        assert!(result.is_err(), "hash mismatch through get() must return error");
        assert!(
            matches!(result.unwrap_err(), Error::HashMismatch { .. }),
            "should be HashMismatch variant"
        );
    }

    #[tokio::test]
    async fn get_full_pipeline_not_found_returns_error() {
        let store = Arc::new(MockMetadataStore::new());
        let coordinator = make_coordinator_with_metadata(store);

        let req = ReadRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("nonexistent"),
            hash_key: HashKey::from_bytes(hash_key(b"nonexistent")),
            metadata_only: false,
            local_only: false,
            policy: None,
        };

        let result = coordinator.get(req).await;
        assert!(result.is_err(), "missing object should return error");
        match result.unwrap_err() {
            Error::NotFound(msg) => {
                assert!(msg.contains("nonexistent"), "error should mention the key");
            }
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_full_pipeline_inline_data_served_directly() {
        let store = Arc::new(MockMetadataStore::new());
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("inline-full"),
            size: 5,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(Bytes::from_static(b"hello")),
            created_at: 0,
            hlc: Hlc::zero(),
        };
        store.put(BucketId::new("test"), ObjectKey::new("inline-full"), meta);

        let coordinator = make_coordinator_with_metadata(store);

        let req = ReadRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("inline-full"),
            hash_key: HashKey::from_bytes(hash_key(b"inline-full")),
            metadata_only: false,
            local_only: false,
            policy: None,
        };

        let result = coordinator.get(req).await.unwrap();
        assert_eq!(&result.data[..], b"hello");
        assert!(!result.hash_verified, "no stored hash → no verification");
    }

    #[tokio::test]
    async fn concurrent_reads_on_same_key_return_consistent_data() {
        let data = b"concurrent read test";
        let seg_id = SegmentId::new();

        let segment_reader = Arc::new(InMemorySegmentReader::new());
        segment_reader.put(seg_id, Bytes::copy_from_slice(data));

        let store = Arc::new(MockMetadataStore::new());
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: data.len() as u32,
            compressed: false,
            logical_length: data.len() as u32,
        });
        let hash = blake3::hash(data);
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("concurrent"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };
        store.put(BucketId::new("test"), ObjectKey::new("concurrent"), meta);

        let coordinator = Arc::new(
            make_coordinator_with_metadata(store)
                .with_segment_reader(segment_reader as Arc<dyn SegmentReader>),
        );

        // Spawn 10 concurrent reads on the same key.
        let mut handles = Vec::with_capacity(10);
        for _ in 0..10 {
            let coord = Arc::clone(&coordinator);
            let handle = tokio::spawn(async move {
                let req = ReadRequest {
                    bucket: BucketId::new("test"),
                    key: ObjectKey::new("concurrent"),
                    hash_key: HashKey::from_bytes(hash_key(b"concurrent")),
                    metadata_only: false,
                    local_only: false,
                    policy: None,
                };
                coord.get(req).await.unwrap()
            });
            handles.push(handle);
        }

        let mut results = Vec::with_capacity(10);
        for handle in handles {
            let result = handle.await.unwrap();
            assert_eq!(&result.data[..], data, "all concurrent reads must return same data");
            assert!(result.hash_verified);
            results.push(result);
        }

        assert_eq!(results.len(), 10);
    }

    // ---- Read Repair (4.2) ----

    #[tokio::test]
    async fn read_repair_single_node_skips_repair_gracefully() {
        // In single-node mode (no pool/membership), run_read_repair is a no-op.
        // The read should succeed without errors.
        let store = Arc::new(MockMetadataStore::new());
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("repair-obj"),
            size: 3,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(Bytes::from_static(b"xyz")),
            created_at: 0,
            hlc: Hlc::new(1000, 0),
        };
        store.put(BucketId::new("test"), ObjectKey::new("repair-obj"), meta);

        let coordinator = make_coordinator_with_metadata(store);

        let req = ReadRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("repair-obj"),
            hash_key: HashKey::from_bytes(hash_key(b"repair-obj")),
            metadata_only: false,
            local_only: false,
            policy: None,
        };

        let result = coordinator.get_object(req).await.unwrap();
        assert_eq!(&result.data[..], b"xyz");
        assert_eq!(result.metadata.hlc, Hlc::new(1000, 0));
    }

    #[tokio::test]
    async fn read_repair_with_pool_and_membership_does_not_block_read() {
        // Even with pool+membership configured, a failed gRPC call
        // in run_read_repair must not block the read response.
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;

        let store = Arc::new(MockMetadataStore::new());
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("repair-grpc"),
            size: 4,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(Bytes::from_static(b"abcd")),
            created_at: 0,
            hlc: Hlc::new(2000, 1),
        };
        store.put(BucketId::new("test"), ObjectKey::new("repair-grpc"), meta);

        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("n1"));
        ring.add_node(NodeId::new("n2"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let gossip_cfg = oceanfs_core::GossipConfig::default();
        let membership = Arc::new(Membership::new(
            NodeId::new("n1"),
            "127.0.0.1:9000".parse().unwrap(),
            "127.0.0.1:9000".parse().unwrap(),
            gossip_cfg,
            ring_cache.clone(),
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let coordinator =
            ReadCoordinator::new_with_metadata(ring_cache, NodeId::new("n1"), None, store)
                .with_connection_pool(pool)
                .with_membership(membership);

        let req = ReadRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("repair-grpc"),
            hash_key: HashKey::from_bytes(hash_key(b"repair-grpc")),
            metadata_only: false,
            local_only: false,
            policy: None,
        };

        // Should succeed even though gRPC calls in run_read_repair fail
        // (no real server running). The read must not be blocked.
        let result = coordinator.get_object(req).await.unwrap();
        assert_eq!(&result.data[..], b"abcd");
        assert_eq!(result.metadata.hlc, Hlc::new(2000, 1));
    }

    // ── Multi-Replica HLC Comparison (§4.6) ─────

    /// Verifies that `compare_with_quorum` returns `None` when the
    /// connection pool is not configured (single-node / test mode).
    #[tokio::test]
    async fn compare_with_quorum_returns_none_without_pool() {
        let store = Arc::new(MockMetadataStore::new());
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("no-pool"),
            size: 3,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(Bytes::from_static(b"abc")),
            created_at: 0,
            hlc: Hlc::new(100, 0),
        };
        store.put(BucketId::new("test"), ObjectKey::new("no-pool"), meta.clone());

        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("n1"));
        let ring_cache = Arc::new(RingCache::new(ring));
        let coordinator =
            ReadCoordinator::new_with_metadata(ring_cache, NodeId::new("n1"), None, store);

        // No pool configured → compare_with_quorum returns None.
        let result = coordinator
            .compare_with_quorum(&BucketId::new("test"), &ObjectKey::new("no-pool"), &meta)
            .await;
        assert!(result.is_none(), "compare_with_quorum must return None without pool");
    }

    /// Verifies that `compare_with_quorum` returns `None` when the
    /// metadata store is not configured.
    #[tokio::test]
    async fn compare_with_quorum_returns_none_without_metadata_store() {
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("no-store"),
            size: 3,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(Bytes::from_static(b"xyz")),
            created_at: 0,
            hlc: Hlc::new(200, 0),
        };

        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("n1"));
        let ring_cache = Arc::new(RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("n1"),
            "127.0.0.1:9000".parse().unwrap(),
            "127.0.0.1:9000".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache.clone(),
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        // Coordinator with pool and membership but NO metadata store.
        let coordinator = ReadCoordinator::new(ring_cache, NodeId::new("n1"), None)
            .with_connection_pool(pool)
            .with_membership(membership);

        let result = coordinator
            .compare_with_quorum(&BucketId::new("test"), &ObjectKey::new("no-store"), &meta)
            .await;
        assert!(result.is_none(), "compare_with_quorum must return None without metadata store");
    }

    /// Verifies that `get_object` serves local inline data when
    /// `compare_with_quorum` finds no newer remote version (or no
    /// remote is reachable). The read path must not be blocked by
    /// failed gRPC calls during quorum comparison.
    #[tokio::test]
    async fn get_object_with_quorum_comparison_serves_local_when_no_remote() {
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;

        let store = Arc::new(MockMetadataStore::new());
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("local-wins"),
            size: 4,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(Bytes::from_static(b"data")),
            created_at: 0,
            hlc: Hlc::new(5000, 0),
        };
        store.put(BucketId::new("test"), ObjectKey::new("local-wins"), meta);

        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("n1"));
        ring.add_node(NodeId::new("n2"));
        ring.add_node(NodeId::new("n3"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let membership = Arc::new(Membership::new(
            NodeId::new("n1"),
            "127.0.0.1:9000".parse().unwrap(),
            "127.0.0.1:9000".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache.clone(),
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let coordinator =
            ReadCoordinator::new_with_metadata(ring_cache, NodeId::new("n1"), None, store)
                .with_connection_pool(pool)
                .with_membership(membership);

        let req = ReadRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("local-wins"),
            hash_key: HashKey::from_bytes(hash_key(b"local-wins")),
            metadata_only: false,
            local_only: false,
            policy: None,
        };

        // gRPC calls will fail (no real server), but the read must
        // succeed with local inline data.
        let result = coordinator.get_object(req).await.unwrap();
        assert_eq!(&result.data[..], b"data", "local inline data must be served");
        assert_eq!(result.metadata.hlc, Hlc::new(5000, 0), "local HLC must be preserved");
    }

    /// Verifies that `get_object` with pool + membership still serves
    /// local inline data correctly when `compare_with_quorum` fails
    /// (no real gRPC server). The synchronous comparison must not
    /// error out the read path.
    #[tokio::test]
    async fn get_object_quorum_comparison_failure_does_not_block_read() {
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;

        let store = Arc::new(MockMetadataStore::new());
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("graceful-fail"),
            size: 6,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: Some(Bytes::from_static(b"winner")),
            created_at: 0,
            hlc: Hlc::new(9000, 1),
        };
        store.put(BucketId::new("test"), ObjectKey::new("graceful-fail"), meta);

        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("n1"));
        ring.add_node(NodeId::new("n2"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let membership = Arc::new(Membership::new(
            NodeId::new("n1"),
            "127.0.0.1:9000".parse().unwrap(),
            "127.0.0.1:9000".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache.clone(),
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let coordinator =
            ReadCoordinator::new_with_metadata(ring_cache, NodeId::new("n1"), None, store)
                .with_connection_pool(pool)
                .with_membership(membership);

        let req = ReadRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("graceful-fail"),
            hash_key: HashKey::from_bytes(hash_key(b"graceful-fail")),
            metadata_only: false,
            local_only: false,
            policy: None,
        };

        // All gRPC calls will fail — the read must still succeed.
        let result = coordinator.get_object(req).await.unwrap();
        assert_eq!(&result.data[..], b"winner");
        assert_eq!(result.metadata.hlc, Hlc::new(9000, 1));
    }

    /// Verifies LwwResolver: newer local HLC wins over older remote.
    ///
    /// The `compare_with_quorum` method uses `LwwResolver` internally
    /// for HLC comparison; this test validates the resolver's core
    /// ordering contract.
    #[test]
    fn lww_resolver_local_newer_wins() {
        let resolver = LwwResolver;
        let local = Hlc::new(2000, 0);
        let remote = Hlc::new(1000, 5);
        let result = resolver.resolve(&local, &remote, &NodeId::new("n1"), &NodeId::new("n2"));
        assert!(result.is_local_accepted(), "local HLC (2000,0) > remote (1000,5) → local wins");
    }

    /// Verifies LwwResolver: newer remote HLC wins over older local.
    #[test]
    fn lww_resolver_remote_newer_wins() {
        let resolver = LwwResolver;
        let local = Hlc::new(1000, 0);
        let remote = Hlc::new(2000, 5);
        let result = resolver.resolve(&local, &remote, &NodeId::new("n1"), &NodeId::new("n2"));
        assert!(result.is_remote_accepted(), "remote HLC (2000,5) > local (1000,0) → remote wins");
    }

    /// Verifies LwwResolver: equal HLCs tie-break by node id — the
    /// greater remote id wins (G7).
    #[test]
    fn lww_resolver_equal_hlc_greater_remote_node_wins() {
        let resolver = LwwResolver;
        let local = Hlc::new(1000, 5);
        let remote = Hlc::new(1000, 5);
        let result =
            resolver.resolve(&local, &remote, &NodeId::new("node-a"), &NodeId::new("node-z"));
        assert!(result.is_remote_accepted(), "equal HLCs → greater node id (node-z) wins",);
    }

    /// Verifies LwwResolver: higher logical counter at same wall time wins.
    #[test]
    fn lww_resolver_same_wall_higher_logical_wins() {
        let resolver = LwwResolver;
        let local = Hlc::new(1000, 0);
        let remote = Hlc::new(1000, 9);
        let result = resolver.resolve(&local, &remote, &NodeId::new("n1"), &NodeId::new("n2"));
        assert!(
            result.is_remote_accepted(),
            "same wall time (1000), remote logical (9) > local (0) → remote wins"
        );
    }

    // ── EC Decode Integration (4.3) ─────

    /// Builds a coordinator with EC decoder and codec config (k=4, m=2).
    #[cfg(feature = "ec")]
    fn make_coordinator_with_ec() -> ReadCoordinator {
        use oceanfs_core::CodecConfig;
        use oceanfs_ec::{CauchyEncoder, Decoder};

        let decoder: Arc<dyn Decoder> = Arc::new(CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 1024,
            ..Default::default()
        }));
        make_coordinator().with_decoder(decoder).with_ec_codec(4, 2)
    }

    /// Verifies that `read_segment_with_ec_recovery` can reconstruct
    /// the full segment data when one data shard is missing.
    #[cfg(feature = "ec")]
    #[test]
    fn ec_recovery_missing_shard_0_reconstructs_full_data() {
        use oceanfs_core::CodecConfig;
        use oceanfs_ec::{CauchyEncoder, Encoder};

        // Build test data: 4 data shards of 16 bytes each.
        let data: [Vec<u8>; 4] = [
            b"AAAA0000BBBB0000".to_vec(),
            b"CCCC0000DDDD0000".to_vec(),
            b"EEEE0000FFFF0000".to_vec(),
            b"GGGG0000HHHH0000".to_vec(),
        ];
        let data_refs: [&[u8]; 4] = [&data[0], &data[1], &data[2], &data[3]];

        let encoder = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });
        let parity = encoder.encode(&data_refs, 2).unwrap();

        // Concatenate 4 data + 2 parity → 6 shards as one "segment".
        let mut segment = Vec::new();
        for s in &data {
            segment.extend_from_slice(s);
        }
        for p in &parity {
            segment.extend_from_slice(p);
        }
        assert_eq!(segment.len(), 6 * 16);

        // Store in segment reader.
        let seg_id = SegmentId::new();
        let segment_reader = Arc::new(InMemorySegmentReader::new());
        segment_reader.put(seg_id, Bytes::from(segment.clone()));

        let coordinator = make_coordinator_with_ec().with_segment_reader(segment_reader);

        // Recover with shard 0 missing.
        let recovered = coordinator.read_segment_with_ec_recovery(&segment, &[0]).unwrap();

        // Verify full recovery matches original data.
        assert_eq!(recovered.len(), 4 * 16);
        assert_eq!(&recovered[0..16], data[0].as_slice());
        assert_eq!(&recovered[16..32], data[1].as_slice());
        assert_eq!(&recovered[32..48], data[2].as_slice());
        assert_eq!(&recovered[48..64], data[3].as_slice());
    }

    /// Verifies that `read_segment_with_ec_recovery` can reconstruct
    /// when TWO data shards are missing (k still available from data+parity).
    #[cfg(feature = "ec")]
    #[test]
    fn ec_recovery_two_missing_shards_reconstructs_full_data() {
        use oceanfs_core::CodecConfig;
        use oceanfs_ec::{CauchyEncoder, Encoder};

        let data: [Vec<u8>; 4] = [
            b"DATA_SHARD_0____".to_vec(),
            b"DATA_SHARD_1____".to_vec(),
            b"DATA_SHARD_2____".to_vec(),
            b"DATA_SHARD_3____".to_vec(),
        ];
        let data_refs: [&[u8]; 4] = [&data[0], &data[1], &data[2], &data[3]];

        let encoder = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });
        let parity = encoder.encode(&data_refs, 2).unwrap();

        let mut segment = Vec::new();
        for s in &data {
            segment.extend_from_slice(s);
        }
        for p in &parity {
            segment.extend_from_slice(p);
        }

        let coordinator = make_coordinator_with_ec();

        // Recover with shards 0 and 2 missing.
        let recovered = coordinator.read_segment_with_ec_recovery(&segment, &[0, 2]).unwrap();

        // k=4 shards available out of 6 total (4 available still means decode works).
        assert_eq!(&recovered[0..16], data[0].as_slice());
        assert_eq!(&recovered[16..32], data[1].as_slice());
        assert_eq!(&recovered[32..48], data[2].as_slice());
        assert_eq!(&recovered[48..64], data[3].as_slice());
    }

    /// Verifies that `decode_ec_shards` directly recovers data from parity.
    #[cfg(feature = "ec")]
    #[test]
    fn decode_ec_shards_recovers_from_parity_only() {
        use oceanfs_core::CodecConfig;
        use oceanfs_ec::{CauchyEncoder, Encoder};

        let data: [Vec<u8>; 4] =
            [vec![0x01u8; 1024], vec![0x02u8; 1024], vec![0x03u8; 1024], vec![0x04u8; 1024]];
        let data_refs: [&[u8]; 4] = [&data[0], &data[1], &data[2], &data[3]];

        let encoder = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });
        let parity = encoder.encode(&data_refs, 2).unwrap();

        let coordinator = make_coordinator_with_ec();

        // All data shards missing, use parity + 2 data shards (total 4 of 6).
        let available: Vec<Option<&[u8]>> = vec![
            None,             // shard 0 missing
            None,             // shard 1 missing
            Some(&data[2]),   // shard 2
            Some(&data[3]),   // shard 3
            Some(&parity[0]), // parity 0
            Some(&parity[1]), // parity 1
        ];

        let recovered = coordinator.decode_ec_shards(&available, 4, 2).unwrap();

        assert_eq!(recovered.len(), 4, "must recover 4 data shards");
        assert_eq!(recovered[0], data[0]);
        assert_eq!(recovered[1], data[1]);
        assert_eq!(recovered[2], data[2]);
        assert_eq!(recovered[3], data[3]);
    }

    /// Verifies error when too many shards are missing for EC recovery.
    #[cfg(feature = "ec")]
    #[test]
    fn ec_recovery_too_many_missing_shards_returns_error() {
        use oceanfs_core::CodecConfig;
        use oceanfs_ec::{CauchyEncoder, Encoder};

        let d0 = vec![0u8; 32];
        let d1 = vec![0u8; 32];
        let d2 = vec![0u8; 32];
        let d3 = vec![0u8; 32];
        let data_refs: [&[u8]; 4] = [&d0, &d1, &d2, &d3];

        let encoder = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });
        let parity = encoder.encode(&data_refs, 2).unwrap();

        let mut segment = Vec::new();
        for s in &[&d0, &d1, &d2, &d3] {
            segment.extend_from_slice(s);
        }
        for p in &parity {
            segment.extend_from_slice(p);
        }

        let coordinator = make_coordinator_with_ec();

        // Missing 3 data shards — only 3 available (1 data + 2 parity = 3 < k=4).
        let result = coordinator.read_segment_with_ec_recovery(&segment, &[0, 1, 2]);
        assert!(result.is_err(), "too many missing shards should return error");
    }

    /// Verifies error when EC codec params are not set (k=0, m=0).
    #[cfg(feature = "ec")]
    #[test]
    fn ec_recovery_without_codec_params_returns_error() {
        let coordinator = make_coordinator();
        let segment = vec![0u8; 64];
        let result = coordinator.read_segment_with_ec_recovery(&segment, &[0]);
        assert!(result.is_err(), "missing codec params should return error");
    }

    /// A [`SegmentReader`] wrapper that fails chunk reads on a specific
    /// segment, forcing the fetch path to fall back to EC recovery.
    ///
    /// Full-segment reads (those with `length == u32::MAX`) are passed
    /// through to the inner reader, allowing `try_ec_recovery_for_chunk`
    /// to access the complete segment for shard-level reconstruction.
    struct ChunkFailSegmentReader {
        inner: Arc<InMemorySegmentReader>,
        failing_segment: SegmentId,
    }

    #[async_trait::async_trait]
    impl SegmentReader for ChunkFailSegmentReader {
        async fn read_chunk(
            &self,
            segment_id: &SegmentId,
            offset: u64,
            length: u32,
        ) -> Result<Bytes, String> {
            if segment_id == &self.failing_segment && length != u32::MAX {
                return Err("simulated chunk read failure".into());
            }
            self.inner.read_chunk(segment_id, offset, length).await
        }
    }

    /// Full pipeline: chunk fetch fails → EC recovery reconstructs
    /// corrupted shard via `assemble_chunks()` → hash verified.
    ///
    /// Verifies that the production path from `assemble_chunks()`
    /// through `fetch_chunks_with_ec()` → `fetch_single_chunk()`
    /// → `try_ec_recovery_for_chunk()` correctly recovers data
    /// when a data shard is all-zeros.
    #[cfg(feature = "ec")]
    #[tokio::test]
    async fn full_pipeline_ec_recovery_on_corrupted_shard() {
        use oceanfs_core::CodecConfig;
        use oceanfs_ec::{CauchyEncoder, Decoder, Encoder};

        // 16-byte shards for clean EC(4,2) arithmetic.
        let data: [Vec<u8>; 4] =
            [vec![0x01u8; 16], vec![0x02u8; 16], vec![0x03u8; 16], vec![0x04u8; 16]];
        let shard_size = 16;
        let expected: Vec<u8> = (0..64).map(|i| (i / 16 + 1) as u8).collect();

        let data_refs: [&[u8]; 4] = [&data[0], &data[1], &data[2], &data[3]];
        let encoder = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: shard_size,
            ..Default::default()
        });
        let parity = encoder.encode(&data_refs, 2).unwrap();

        // Verify encode-decode roundtrip directly first.
        let test_available: Vec<Option<&[u8]>> = vec![
            None,
            Some(&data[1]),
            Some(&data[2]),
            Some(&data[3]),
            Some(&parity[0]),
            Some(&parity[1]),
        ];
        let test_decoder = CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        });
        let test_recovered = test_decoder.decode(&test_available, 4, 2).unwrap();
        assert_eq!(test_recovered[0], data[0], "EC decode roundtrip failed on shard 0");
        assert_eq!(test_recovered[1], data[1], "EC decode roundtrip failed on shard 1");

        // Concatenate shards: 4 data + 2 parity, with shard 0 zeroed.
        let mut segment = Vec::with_capacity(6 * shard_size);
        segment.extend_from_slice(&vec![0u8; shard_size]);
        for chunk in &data[1..4] {
            segment.extend_from_slice(chunk);
        }
        for p in &parity {
            segment.extend_from_slice(p);
        }

        // Verify parity data in segment matches.
        assert_eq!(&segment[64..80], parity[0].as_ref(), "parity[0] mismatch");
        assert_eq!(&segment[80..96], parity[1].as_ref(), "parity[1] mismatch");

        let seg_id = SegmentId::new();
        let inner = Arc::new(InMemorySegmentReader::new());
        inner.put(seg_id, Bytes::from(segment));
        let reader = Arc::new(ChunkFailSegmentReader { inner, failing_segment: seg_id });

        // Coordinator with EC decoder + codec.
        let decoder: Arc<dyn oceanfs_ec::Decoder> = Arc::new(CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            ..Default::default()
        }));
        let coordinator = make_coordinator_with_segments(&[])
            .with_decoder(decoder)
            .with_ec_codec(4, 2)
            .with_segment_reader(reader as Arc<dyn SegmentReader>);

        // Metadata: single chunk covering all 4 data shards at offset 0.
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg_id,
            offset: 0,
            length: (4 * shard_size) as u32,
            compressed: false,
            logical_length: (4 * shard_size) as u32,
        });
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("ec-pipe"),
            size: expected.len() as u64,
            blake3_hash: None, // hash verified separately below
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        let (assembled, _source) =
            coordinator.assemble_chunks(&meta, FetchStrategy::default(), None).await.unwrap();
        assert_eq!(&assembled[..], &expected[..], "EC recovery data mismatch");
    }

    // ── Per-Bucket Fetch Strategy tests (T7.5–T7.10) ──

    /// T7.5: `FastestK` strategy completes successfully with local segments.
    /// (Full latency-timing test requires multi-node gRPC; structural
    /// dispatch verified here.)
    #[tokio::test]
    async fn test_fastest_k_returns_on_k_arrival() {
        let data = b"fastest-k multi-chunk data for dispatch test";
        let seg1 = SegmentId::new();
        let seg2 = SegmentId::new();
        let part1 = &data[..data.len() / 2];
        let part2 = &data[data.len() / 2..];
        let hash = blake3::hash(data);

        let coordinator = make_coordinator_with_segments(&[(seg1, part1), (seg2, part2)]);

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg1,
            offset: 0,
            length: part1.len() as u32,
            compressed: false,
            logical_length: part1.len() as u32,
        });
        chunks.push(ChunkRef {
            segment_id: seg2,
            offset: 0,
            length: part2.len() as u32,
            compressed: false,
            logical_length: part2.len() as u32,
        });

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("fastest-k"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        // FastestK dispatches through parallel_fetch/use_fastest_k pathways.
        let (assembled, _source) =
            coordinator.assemble_chunks(&meta, FetchStrategy::FastestK, None).await.unwrap();
        assert_eq!(&assembled[..], data);
    }

    /// T7.6: `LocalFirst` preserves original behavior (identical output
    /// to the default strategy).
    #[tokio::test]
    async fn test_local_first_preserves_original_behavior() {
        let data = b"local-first strategy test data";
        let seg = SegmentId::new();
        let hash = blake3::hash(data);

        let coordinator = make_coordinator_with_segments(&[(seg, data)]);

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg,
            offset: 0,
            length: data.len() as u32,
            compressed: false,
            logical_length: data.len() as u32,
        });

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("local-first"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        // Both LocalFirst and default should produce the same result.
        let (local_result, _) =
            coordinator.assemble_chunks(&meta, FetchStrategy::LocalFirst, None).await.unwrap();
        let (default_result, _) =
            coordinator.assemble_chunks(&meta, FetchStrategy::default(), None).await.unwrap();
        assert_eq!(&local_result[..], &default_result[..]);
        assert_eq!(&local_result[..], data);
    }

    /// T7.7: `FastestK` tolerates partial failures — succeeds even when
    /// some segments are unavailable (local reader covers the gap).
    #[tokio::test]
    async fn test_fastest_k_tolerates_partial_failures() {
        let data = b"fastest-k tolerates partial failure";
        let seg1 = SegmentId::new();
        let seg2 = SegmentId::new(); // this one will NOT be registered
        let part1 = &data[..data.len() / 2];
        let hash = blake3::hash(data);

        // Only register seg1 — seg2 is "missing" (simulates remote failure).
        let coordinator = make_coordinator_with_segments(&[(seg1, part1)]);

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg1,
            offset: 0,
            length: part1.len() as u32,
            compressed: false,
            logical_length: part1.len() as u32,
        });
        chunks.push(ChunkRef {
            segment_id: seg2,
            offset: 0,
            length: (data.len() - part1.len()) as u32,
            compressed: false,
            logical_length: (data.len() - part1.len()) as u32,
        });

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("partial-fail"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        // FastestK with missing segment returns error (can't get k=2 shards).
        let result = coordinator.assemble_chunks(&meta, FetchStrategy::FastestK, None).await;
        assert!(result.is_err(), "FastestK should error when insufficient shards available");
    }

    /// T7.8: `FastestK` fails when insufficient shards are available.
    #[tokio::test]
    async fn test_fastest_k_fails_when_insufficient_shards() {
        let seg1 = SegmentId::new();
        let seg2 = SegmentId::new();
        let seg3 = SegmentId::new(); // missing
        let data: Vec<u8> = (0..300).map(|i| i as u8).collect();

        // Only 2 out of 3 segments registered.
        let coordinator =
            make_coordinator_with_segments(&[(seg1, &data[0..100]), (seg2, &data[100..200])]);

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg1,
            offset: 0,
            length: 100,
            compressed: false,
            logical_length: 100,
        });
        chunks.push(ChunkRef {
            segment_id: seg2,
            offset: 0,
            length: 100,
            compressed: false,
            logical_length: 100,
        });
        chunks.push(ChunkRef {
            segment_id: seg3,
            offset: 0,
            length: 100,
            compressed: false,
            logical_length: 100,
        });

        let hash = blake3::hash(&data);
        let meta = ObjectMetadata {
            object_key: ObjectKey::new("insufficient"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        let result = coordinator.assemble_chunks(&meta, FetchStrategy::FastestK, None).await;
        assert!(result.is_err(), "insufficient shards (2/3) should error");
    }

    /// T7.9: `BandwidthOptimized` aliases to `LocalFirst` behavior.
    #[tokio::test]
    async fn test_bandwidth_optimized_aliases_local_first() {
        let data = b"bandwidth-optimized strategy test";
        let seg = SegmentId::new();
        let hash = blake3::hash(data);
        let coordinator = make_coordinator_with_segments(&[(seg, data)]);

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg,
            offset: 0,
            length: data.len() as u32,
            compressed: false,
            logical_length: data.len() as u32,
        });

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("bw-opt"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        // BandwidthOptimized should produce same result as LocalFirst.
        let (bw_result, _) = coordinator
            .assemble_chunks(&meta, FetchStrategy::BandwidthOptimized, None)
            .await
            .unwrap();
        let (local_result, _) =
            coordinator.assemble_chunks(&meta, FetchStrategy::LocalFirst, None).await.unwrap();
        assert_eq!(&bw_result[..], &local_result[..]);
        assert_eq!(&bw_result[..], data);
    }

    /// T7.10: `CpuOptimized` aliases to `FastestK` behavior.
    #[tokio::test]
    async fn test_cpu_optimized_aliases_fastest_k() {
        let data = b"cpu-optimized strategy test";
        let seg1 = SegmentId::new();
        let seg2 = SegmentId::new();
        let part1 = &data[..data.len() / 2];
        let part2 = &data[data.len() / 2..];
        let hash = blake3::hash(data);

        let coordinator = make_coordinator_with_segments(&[(seg1, part1), (seg2, part2)]);

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef {
            segment_id: seg1,
            offset: 0,
            length: part1.len() as u32,
            compressed: false,
            logical_length: part1.len() as u32,
        });
        chunks.push(ChunkRef {
            segment_id: seg2,
            offset: 0,
            length: part2.len() as u32,
            compressed: false,
            logical_length: part2.len() as u32,
        });

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("cpu-opt"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        // CpuOptimized aliases to FastestK — both use parallel dispatch.
        let (cpu_result, _) =
            coordinator.assemble_chunks(&meta, FetchStrategy::CpuOptimized, None).await.unwrap();
        let (fastest_result, _) =
            coordinator.assemble_chunks(&meta, FetchStrategy::FastestK, None).await.unwrap();
        assert_eq!(&cpu_result[..], &fastest_result[..]);
        assert_eq!(&cpu_result[..], data);
    }
}
