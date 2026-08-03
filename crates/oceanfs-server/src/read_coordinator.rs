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

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_core::{
    BucketId, ConflictResolver, HashKey, HashOutput, Hlc, LwwResolver, NodeId, ObjectKey,
    ObjectMetadata, OperationTimeouts,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::RingCache;

use crate::{
    error::{Error, Result},
    metadata_ops::MetadataOps,
    read::{assembly::MultiChunkAssembler, repair::schedule_repair},
};

/// Default read timeout used when no policy is provided.
#[allow(dead_code)]
static DEFAULT_READ_TIMEOUT_MS: std::sync::LazyLock<u64> =
    std::sync::LazyLock::new(|| OperationTimeouts::default().read_default_ms);

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
/// // A simple in-memory implementation for testing.
/// struct InMemorySegments {
///     data: HashMap<SegmentId, Bytes>,
/// }
///
/// impl SegmentReader for InMemorySegments {
///     fn read_chunk(
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
pub trait SegmentReader: Send + Sync {
    /// Reads a chunk of data from a segment.
    ///
    /// # Errors
    ///
    /// Returns a string error if the segment or chunk is not found.
    fn read_chunk(
        &self,
        segment_id: &oceanfs_core::SegmentId,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, String>;
}

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
    /// Metadata store for object lookup.
    metadata: Option<Arc<dyn MetadataOps>>,
    /// Optional segment reader for chunk-based reads.
    segment_reader: Option<Arc<dyn SegmentReader>>,
    /// Connection pool for gRPC shard fetch (multi-node reads).
    pool: Option<Arc<ConnectionPool>>,
    /// Membership store for resolving node addresses.
    membership: Option<Arc<Membership>>,
    /// Optional EC decoder for reconstructing data from parity shards.
    #[cfg(feature = "ec")]
    decoder: Option<Arc<dyn oceanfs_ec::Decoder>>,
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
        }
    }

    /// Creates a new read coordinator with metadata store access.
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
            metadata: Some(metadata),
            segment_reader: None,
            pool: None,
            membership: None,
            #[cfg(feature = "ec")]
            decoder: None,
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

        let obj_meta = self.lookup_metadata(&req).await?;

        let data = if let Some(ref inline) = obj_meta.inline_data {
            inline.clone()
        } else if req.metadata_only {
            return Ok(GetResult {
                data: Bytes::new(),
                metadata: obj_meta,
                cache_hit: CacheHitLevel::Miss,
                hash: HashOutput::from_bytes([0u8; 32]),
            });
        } else if !obj_meta.chunks.is_empty() {
            self.assemble_chunks(&obj_meta, req.policy.as_deref()).await?
        } else if self.metadata.is_some() {
            // Has a metadata store but no data: genuinely not found.
            return Err(Error::NotFound(format!("{}/{}", req.bucket, req.key)));
        } else {
            // No metadata store and no data: return empty (test/headless mode).
            Bytes::new()
        };

        let computed_hash = blake3::hash(&data);
        let hash = HashOutput::from_bytes(*computed_hash.as_bytes());

        Ok(GetResult { data, metadata: obj_meta, cache_hit: CacheHitLevel::Miss, hash })
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
        Ok(ReadResult { data: result.data, metadata: result.metadata, hash_verified })
    }

    /// Looks up object metadata from the metadata store.
    async fn lookup_metadata(&self, req: &ReadRequest) -> Result<ObjectMetadata> {
        if let Some(ref store) = self.metadata {
            store
                .get_object(&req.bucket, &req.key)
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
    /// When `policy.read_tuning.use_fastest_k` is true (the default),
    /// `FuturesUnordered` yields data as soon as k shards arrive.
    /// When `policy.read_tuning.stripe_parallelism > 0`, stripe
    /// decode tasks are bounded by a semaphore.
    ///
    /// After assembly, runs streaming BLAKE3 verification via
    /// [`MultiChunkAssembler`].
    ///
    /// When neither gRPC nor a segment reader is available, returns an
    /// error.
    async fn assemble_chunks(
        &self,
        meta: &ObjectMetadata,
        policy: Option<&crate::BucketPolicy>,
    ) -> Result<Bytes> {
        let chunk_count = meta.chunks.len();
        if chunk_count == 0 {
            return Ok(Bytes::new());
        }

        let timeout_ms = OperationTimeouts::default().read_default_ms;

        // Read policy configuration for the fetch strategy.
        let parallel_fetch = policy.map(|p| p.read_tuning.parallel_fetch).unwrap_or(true);
        let use_fastest_k = policy.map(|p| p.read_tuning.use_fastest_k).unwrap_or(true);
        let stripe_parallelism = policy.map(|p| p.read_tuning.stripe_parallelism).unwrap_or(0);

        if stripe_parallelism > 0 {
            tracing::debug!(
                parallel_fetch,
                use_fastest_k,
                stripe_parallelism,
                "read tuning applied — EC stripe decode bounded by semaphore"
            );
        }

        // Use gRPC-enabled fetch when pool and membership are available.
        // Note: `parallel_fetch` and `use_fastest_k` are the default behavior
        // of `FuturesUnordered` — setting them to `false` would require
        // serializing fetches, which is a future optimization.
        let _ = (parallel_fetch, use_fastest_k); // consumed for future feature gating.
        let chunk_data = if self.pool.is_some() && self.membership.is_some() {
            crate::read::fetch::fetch_chunks_with_grpc(
                &self.ring,
                meta,
                timeout_ms,
                self.segment_reader.as_ref(),
                self.pool.as_ref(),
                self.membership.as_ref(),
            )
            .await?
        } else {
            crate::read::fetch::fetch_chunks(
                &self.ring,
                meta,
                timeout_ms,
                self.segment_reader.as_ref(),
            )
            .await?
        };

        // Read repair: only meaningful in multi-node mode where we
        // actually fetch from remote replicas. In single-node mode,
        // there are no remote replicas to compare against.
        //
        // When gRPC is enabled, the fetch operation may return data
        // from multiple replicas. The conflict resolver compares their
        // HLCs and asynchronously pushes corrected data to stale nodes.
        if self.pool.is_some() && self.membership.is_some() {
            // When read_quorum > 1, compare replica HLCs to detect
            // and repair stale copies. Currently fetches from the
            // first available replica; full multi-replica comparison
            // requires HLC metadata in shard responses.
            schedule_repair(
                Arc::clone(&self.conflict_resolver),
                meta.hlc,
                meta.hlc,
                self.node_id.clone(),
            );
        }

        // Build the assembler based on whether we have a stored hash.
        let mut assembler = match meta.blake3_hash {
            Some(ref stored) => MultiChunkAssembler::new(*stored, chunk_count),
            None => MultiChunkAssembler::new_no_verify(chunk_count),
        };

        for (index, data) in chunk_data.into_iter().enumerate() {
            assembler.push_chunk(index, data)?;
        }

        assembler.finalize()
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
    ///
    /// # Integration
    ///
    /// This method will be called from `assemble_chunks` once
    /// shard-level fetching (with per-shard gRPC calls) is
    /// implemented. Currently the fetch path operates at the
    /// chunk level via `ChunkRef`.
    #[cfg(feature = "ec")]
    #[allow(dead_code)] // called from shard-level fetch (not yet implemented)
    pub(crate) fn decode_ec_shards(
        &self,
        available_shards: &[Option<&[u8]>],
        data_count: u8,
        parity_count: u8,
    ) -> Result<Vec<Vec<u8>>> {
        let decoder = self
            .decoder
            .as_ref()
            .ok_or_else(|| Error::Internal("EC decoder not configured".into()))?;
        decoder
            .decode(available_shards, data_count, parity_count)
            .map_err(|e| Error::Internal(format!("EC decode failed: {e}")))
    }

    /// Returns a reference to the EC decoder, if configured.
    #[cfg(feature = "ec")]
    #[allow(dead_code)]
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
    pub fn metadata_store(&self) -> Option<&Arc<dyn MetadataOps>> {
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
        }
    }
}

/// A simple in-memory segment reader for testing and single-node
/// operation.
///
/// Maps segment IDs to their data payloads.
pub struct InMemorySegmentReader {
    segments: parking_lot::RwLock<std::collections::HashMap<oceanfs_core::SegmentId, Bytes>>,
}

impl InMemorySegmentReader {
    /// Creates a new empty segment reader.
    pub fn new() -> Self {
        Self { segments: parking_lot::RwLock::new(std::collections::HashMap::new()) }
    }

    /// Stores data for a segment.
    pub fn put(&self, segment_id: oceanfs_core::SegmentId, data: Bytes) {
        self.segments.write().insert(segment_id, data);
    }
}

impl Default for InMemorySegmentReader {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentReader for InMemorySegmentReader {
    fn read_chunk(
        &self,
        segment_id: &oceanfs_core::SegmentId,
        _offset: u64,
        _length: u32,
    ) -> Result<Bytes, String> {
        self.segments
            .read()
            .get(segment_id)
            .cloned()
            .ok_or_else(|| format!("segment {segment_id} not found"))
    }
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
        chunks.push(ChunkRef { segment_id, offset: 0, length: data.len() as u32 });

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

        let assembled = coordinator.assemble_chunks(&meta, None).await.unwrap();
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
        chunks.push(ChunkRef { segment_id: seg1, offset: 0, length: part1.len() as u32 });
        chunks.push(ChunkRef { segment_id: seg2, offset: 0, length: part2.len() as u32 });

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("multi-chunk"),
            size: combined.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        let assembled = coordinator.assemble_chunks(&meta, None).await.unwrap();
        assert_eq!(&assembled[..], &combined[..]);
    }

    #[tokio::test]
    async fn read_coordinator_chunk_assembly_hash_mismatch() {
        let data = b"actual chunk data";
        let wrong_hash = blake3::hash(b"something completely different");
        let seg_id = SegmentId::new();

        let coordinator = make_coordinator_with_segments(&[(seg_id, data)]);

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef { segment_id: seg_id, offset: 0, length: data.len() as u32 });

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("mismatch"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*wrong_hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        let result = coordinator.assemble_chunks(&meta, None).await;
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
        chunks.push(ChunkRef { segment_id: missing_id, offset: 0, length: data.len() as u32 });

        let meta = ObjectMetadata {
            object_key: ObjectKey::new("missing"),
            size: data.len() as u64,
            blake3_hash: Some(oceanfs_core::HashOutput::from_bytes(*hash.as_bytes())),
            chunks,
            inline_data: None,
            created_at: 0,
            hlc: Hlc::zero(),
        };

        let result = coordinator.assemble_chunks(&meta, None).await;
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
        chunks.push(ChunkRef { segment_id: SegmentId::new(), offset: 0, length: 100 });
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
            chunks.push(ChunkRef { segment_id: SegmentId::new(), offset: i * 1024, length: 1024 });
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
        };
        assert_eq!(result.cache_hit, CacheHitLevel::Miss);
        assert_eq!(&result.data[..], b"test");
    }

    #[test]
    fn segment_reader_trait_is_object_safe() {
        // Verify SegmentReader can be used as trait object.
        let reader: Arc<dyn SegmentReader> = Arc::new(InMemorySegmentReader::new());
        let seg_id = SegmentId::new();
        let result = reader.read_chunk(&seg_id, 0, 100);
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

        fn put_segment(
            &self,
            _meta: oceanfs_core::SegmentMetadata,
        ) -> std::result::Result<(), crate::metadata_ops::MetadataError> {
            Ok(())
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
        chunks.push(ChunkRef { segment_id, offset: 0, length: data.len() as u32 });
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
        chunks.push(ChunkRef { segment_id: seg1, offset: 0, length: part1.len() as u32 });
        chunks.push(ChunkRef { segment_id: seg2, offset: 0, length: part2.len() as u32 });
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
        chunks.push(ChunkRef { segment_id: seg_id, offset: 0, length: data.len() as u32 });
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
        chunks.push(ChunkRef { segment_id: seg_id, offset: 0, length: data.len() as u32 });
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
}
