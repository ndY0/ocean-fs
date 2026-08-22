//! Composition root: wires all subsystem crates into a running OceanFS node.
//!
//! This is the **only** crate allowed to import concrete types from multiple
//! subsystem crates per architecture.md §4.1. It constructs every component,
//! injects dependencies via `Arc`, spawns background tasks, and binds the
//! HTTP + gRPC servers.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use oceanfs_core::{
    shard, AccelConfig, BucketId, Hlc, HlcClock, Incarnation, MetadataConfig, MetricRegistrar,
    NodeConfig, NodeId, ObjectKey, ObjectMetadata, PoolConfig, RingConfig, RpcConfig,
    SegmentSizeConfig, SizeTier, Tombstone, WalConfig,
};
use oceanfs_durability::{
    recover_incomplete_compactions, CompactionRecoveryAction, GrpcHintDeliveryClient,
    HintedHandoff, HintedHandoffConfig, HintedHandoffManager, StoreObjectLookup,
};
use oceanfs_network::{apply_opts_to_fd, create_reuseport_listener};
use oceanfs_server::{
    auth::AuthMiddleware, metadata_ops::MetadataOps, AdminHandler, BucketConfigStore,
    ReadCoordinator, Router, S3Handler, WriteCoordinator,
};
use oceanfs_storage::{SegmentPool, SegmentShard};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    membership_state::{default_state_path, MembershipStateStore},
    metadata_adapter::MetadataStoreAdapter,
};

// ---------------------------------------------------------------------------
// PrefetchStoreAdapter — bridges concrete store to oceanfs_storage_api::MetadataStore
// ---------------------------------------------------------------------------

/// Minimal adapter wrapping `oceanfs_storage::RocksDbMetadataStore` to implement
/// the `oceanfs_storage_api::MetadataStore` trait needed by `PrefetchEngine`.
struct PrefetchStoreAdapter {
    store: Arc<oceanfs_storage::RocksDbMetadataStore>,
}

impl oceanfs_storage_api::MetadataStore for PrefetchStoreAdapter {
    fn list_object_keys(&self, bucket: &BucketId) -> std::io::Result<Vec<(BucketId, ObjectKey)>> {
        let results = self.store.list_objects(bucket, "");
        results
            .into_iter()
            .map(|r| {
                r.map(|meta| (bucket.clone(), meta.object_key))
                    .map_err(|e| std::io::Error::other(e.to_string()))
            })
            .collect()
    }

    fn get_object_metadata(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
    ) -> std::io::Result<Option<ObjectMetadata>> {
        self.store.get_object(bucket, key).map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn list_objects(
        &self,
        bucket: &BucketId,
        prefix: &str,
    ) -> Vec<std::io::Result<ObjectMetadata>> {
        self.store
            .list_objects(bucket, prefix)
            .into_iter()
            .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            .collect()
    }

    fn list_tombstones(&self, bucket: &BucketId) -> Vec<std::io::Result<(ObjectKey, Tombstone)>> {
        self.store
            .list_tombstones(bucket)
            .into_iter()
            .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            .collect()
    }

    fn delete_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> std::io::Result<()> {
        self.store.delete_tombstone(bucket, key).map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn has_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> std::io::Result<bool> {
        self.store.has_tombstone(bucket, key).map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn put_object(&self, bucket: &BucketId, meta: ObjectMetadata) -> std::io::Result<()> {
        self.store
            .put_object_in_bucket(bucket, meta)
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn batch_write(&self, ops: Vec<oceanfs_storage_api::BatchOp>) -> std::io::Result<()> {
        // Fall back to sequential writes for the adapter.
        for op in ops {
            match op {
                oceanfs_storage_api::BatchOp::PutObject(bucket, key, meta) => {
                    // The adapter's put_object writes with the caller's
                    // bucket; replicate that here for the rewritten object.
                    self.store
                        .put_object_in_bucket(&bucket, meta)
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    let _ = key;
                }
                oceanfs_storage_api::BatchOp::DeleteObject(_, _) => {}
                oceanfs_storage_api::BatchOp::PutTombstone(_, _, _) => {}
                oceanfs_storage_api::BatchOp::DeleteTombstone(bucket, key) => {
                    self.delete_tombstone(&bucket, &key)?;
                }
            }
        }
        Ok(())
    }

    fn delete_object(&self, bucket: &BucketId, key: &ObjectKey, hlc: Hlc) -> std::io::Result<()> {
        self.store.delete_object(bucket, key, hlc).map_err(|e| std::io::Error::other(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Node leave handler — implements GracefulLeaveHandler for WAL + shard handoff
// ---------------------------------------------------------------------------

/// Handles WAL sealing and segment shard streaming during graceful leave.
struct NodeLeaveHandler {
    /// WAL writer for flushing pending entries.
    wal_writer: Arc<oceanfs_storage::WalWriter>,
    /// Directory containing authoritative segment files.
    segment_dir: std::path::PathBuf,
    /// Connection pool for gRPC data transfer.
    pool: Arc<oceanfs_network::ConnectionPool>,
    /// Membership for resolving successor node addresses.
    membership: Arc<oceanfs_membership::Membership>,
}

impl NodeLeaveHandler {
    /// Lists segment IDs from the segments directory.
    fn list_segments(&self) -> std::io::Result<Vec<oceanfs_core::SegmentId>> {
        let mut ids = Vec::new();
        for entry in std::fs::read_dir(&self.segment_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map(|e| e == "dat").unwrap_or(false) {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(parsed) = uuid::Uuid::parse_str(stem) {
                        ids.push(oceanfs_core::SegmentId::from_uuid_bytes(*parsed.as_bytes()));
                    }
                }
            }
        }
        Ok(ids)
    }

    /// Reads segment data, skipping the 76-byte header.
    fn read_segment_data(
        &self,
        seg_id: &oceanfs_core::SegmentId,
    ) -> std::io::Result<Option<bytes::Bytes>> {
        let path = self.segment_dir.join(format!("{seg_id}.dat"));
        match std::fs::read(&path) {
            Ok(data) if data.len() >= 76 => Ok(Some(bytes::Bytes::from(data[76..].to_vec()))),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[async_trait::async_trait]
impl oceanfs_membership::GracefulLeaveHandler for NodeLeaveHandler {
    async fn handoff_wal_to(&self, successor: &oceanfs_core::NodeId) -> oceanfs_core::Result<()> {
        // Seal: flush pending WAL entries to disk.
        self.wal_writer
            .sync()
            .await
            .map_err(|e| oceanfs_core::Error::Leave(format!("WAL sync failed: {e}")))?;

        info!(
            successor = %successor,
            "WAL flushed to disk for graceful leave handoff"
        );

        // Push: transfer WAL-protected segment data to the successor.
        // Segments in the blob store represent data that was previously
        // WAL-protected and has been sealed. Transferring them completes
        // the WAL handoff.
        let segments = self
            .list_segments()
            .map_err(|e| oceanfs_core::Error::Leave(format!("segment list failed: {e}")))?;

        let mut transferred = 0usize;
        for seg_id in &segments {
            if let Some(data) = self
                .read_segment_data(seg_id)
                .map_err(|e| oceanfs_core::Error::Leave(format!("segment read failed: {e}")))?
            {
                if self.push_data_to_node(successor, seg_id, &data).await.is_ok() {
                    transferred += 1;
                }
            }
        }
        info!(
            successor = %successor,
            transferred,
            total = segments.len(),
            "WAL-protected segments handed off to successor"
        );
        Ok(())
    }

    async fn transfer_segment_shards_to(
        &self,
        successor: &oceanfs_core::NodeId,
    ) -> oceanfs_core::Result<usize> {
        // Enumerate owned segments.
        let segments = self
            .list_segments()
            .map_err(|e| oceanfs_core::Error::Leave(format!("segment list failed: {e}")))?;

        if segments.is_empty() {
            info!(successor = %successor, "no segments to transfer");
            return Ok(0);
        }

        info!(
            successor = %successor,
            count = segments.len(),
            "transferring segment shards to successor"
        );

        let mut transferred: usize = 0;

        // Transfer each segment via hinted handoff gRPC.
        for seg_id in &segments {
            let data = self
                .read_segment_data(seg_id)
                .map_err(|e| oceanfs_core::Error::Leave(format!("segment read failed: {e}")))?;

            let data = match data {
                Some(d) => d,
                None => continue,
            };

            // Push segment data to successor via hinted handoff.
            if let Err(e) = self.push_data_to_node(successor, seg_id, &data).await {
                warn!(
                    successor = %successor,
                    segment_id = %seg_id,
                    error = %e,
                    "failed to transfer segment shard"
                );
                continue;
            }
            transferred += 1;
        }

        Ok(transferred)
    }
}

impl NodeLeaveHandler {
    /// Pushes segment data to a remote node via `HealingRpcClient::hinted_handoff`.
    async fn push_data_to_node(
        &self,
        node: &oceanfs_core::NodeId,
        segment_id: &oceanfs_core::SegmentId,
        data: &[u8],
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = self
            .membership
            .address_of(node)
            .ok_or_else(|| format!("no address for node {node}"))?;

        let channel = {
            let pooled = self
                .pool
                .get_channel(addr)
                .await
                .map_err(|e| format!("connection pool error for {node}: {e}"))?;

            pooled.channel().clone()
        };

        use oceanfs_core::Hlc;
        use oceanfs_durability::{healing_rpc::HintRequest, HealingRpcClient};

        let mut client = HealingRpcClient::new(channel);
        let proto_seg: oceanfs_core::proto::common::SegmentId = (*segment_id).into();
        let proto_node: oceanfs_core::proto::common::NodeId = node.clone().into();

        let request = tonic::Request::new(HintRequest {
            intended_for: Some(proto_node),
            segment_id: Some(proto_seg),
            data: Bytes::copy_from_slice(data),
            hlc: Some(Hlc::zero().into()),
        });

        let timeout_ms = 5000u64;
        let response = tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            client.hinted_handoff_single(request),
        )
        .await
        .map_err(|_| format!("gRPC hinted_handoff to {node} timed out after {timeout_ms}ms"))?
        .map_err(|s| format!("gRPC hinted_handoff to {node} failed: {s}"))?;

        if !response.into_inner().accepted {
            return Err(format!("node {node} rejected hinted handoff").into());
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BackgroundTasks
// ---------------------------------------------------------------------------

/// Aggregated join handles and cancellation tokens for background loops.
pub struct BackgroundTasks {
    /// Gossip protocol task handle (Membership drives gossip internally;
    /// this handle waits for cancellation).
    pub(crate) gossip: JoinHandle<()>,
    /// Gossip cancellation token.
    pub(crate) gossip_cancel: CancellationToken,

    /// Garbage collector task.
    pub(crate) gc: JoinHandle<()>,
    /// GC cancellation token.
    pub(crate) gc_cancel: CancellationToken,

    /// Anti-entropy Merkle exchange task.
    pub(crate) anti_entropy: JoinHandle<()>,
    /// Anti-entropy cancellation token.
    pub(crate) ae_cancel: CancellationToken,

    /// Scrub scheduler task.
    pub(crate) scrub: JoinHandle<()>,
    /// Scrub cancellation token.
    pub(crate) scrub_cancel: CancellationToken,

    /// Orphan reaper task.
    pub(crate) orphan_reaper: JoinHandle<()>,
    /// Reaper cancellation token.
    pub(crate) reaper_cancel: CancellationToken,

    /// Prefetch engine background pre-warmer (only if prefetch is enabled).
    pub(crate) prefetch: Option<JoinHandle<()>>,
    /// Prefetch cancellation token.
    pub(crate) prefetch_cancel: CancellationToken,

    /// Failure detector task handle (Membership drives FD internally;
    /// this handle waits for cancellation).
    pub(crate) failure_detector: JoinHandle<()>,
    /// Failure detector cancellation token.
    pub(crate) fd_cancel: CancellationToken,

    /// EC Heal worker task.
    pub(crate) heal: JoinHandle<()>,
    /// Heal worker cancellation token.
    pub(crate) heal_cancel: CancellationToken,

    /// Hinted handoff delivery watcher task.
    pub(crate) hinted_handoff_delivery: Option<JoinHandle<()>>,
    /// Hinted handoff delivery cancellation token.
    pub(crate) delivery_cancel: CancellationToken,

    /// Hinted handoff WAL prune task.
    pub(crate) hinted_handoff_prune: JoinHandle<()>,
    /// Hinted handoff WAL prune cancellation token.
    pub(crate) hint_prune_cancel: CancellationToken,

    /// gRPC server task handle for graceful shutdown.
    pub(crate) grpc_server: Option<JoinHandle<()>>,
    /// gRPC server cancellation token.
    pub(crate) grpc_shutdown: CancellationToken,

    /// Health check loop cancellation token.
    pub(crate) health_check_cancel: CancellationToken,
}

/// Resolves a RocksDB property string to a u64. Used by tests.
#[cfg(test)]
fn property_as_u64(store: &oceanfs_storage::RocksDbMetadataStore, name: &str) -> Option<u64> {
    store.property(name).and_then(|v| v.parse::<u64>().ok())
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

/// A running OceanFS node.
///
/// Owns live references to all subsystem components so they remain
/// alive for the lifetime of the node. The acceleration dispatcher
/// is probed at startup per ADR-0006 and cached here for consumers
/// (encoders, decoders, hash accelerators).
pub struct Node {
    /// Node configuration.
    config: Arc<NodeConfig>,
    /// Acceleration dispatcher probed at startup (ADR-0006).
    pub(crate) accel: Arc<oceanfs_accel::AccelDispatcher>,
    /// Bound HTTP server socket address.
    server_addr: SocketAddr,
    /// Bound gRPC server socket address.
    grpc_addr: SocketAddr,
    /// Cancellation token for the HTTP server graceful shutdown.
    http_shutdown: CancellationToken,
    /// Cancellation token for the gRPC server graceful shutdown.
    grpc_shutdown: CancellationToken,
    /// Background task handles and cancellation tokens.
    background: BackgroundTasks,
    /// Graceful leave handler for WAL and segment handoff.
    leave_handler: Arc<NodeLeaveHandler>,
    /// Cluster membership for leave signaling.
    membership: Arc<oceanfs_membership::Membership>,
    /// Metadata store (held for graceful shutdown flush).
    metadata_store: Arc<oceanfs_storage::RocksDbMetadataStore>,
    /// WAL writer (held for graceful shutdown sync).
    wal_writer: Arc<oceanfs_storage::WalWriter>,
}

impl Node {
    /// Starts an OceanFS node: wires all subsystems, binds servers,
    /// spawns background tasks, and returns a running [`Node`].
    ///
    /// # Errors
    ///
    /// Returns an error if RocksDB cannot be opened, ports cannot be
    /// bound, or any subsystem fails to initialize.
    pub async fn start(config: NodeConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let config = Arc::new(Self::validate_config(config)?);

        // Configure the rayon global thread pool for EC encode/decode.
        // Leave 2 cores for tokio's async runtime to avoid CPU oversubscription
        // when both compute (EC) and I/O (gRPC, RocksDB) run concurrently.
        if let Err(e) = rayon::ThreadPoolBuilder::new()
            .num_threads(
                std::thread::available_parallelism()
                    .map(|n| n.get().saturating_sub(2).max(1))
                    .unwrap_or(2),
            )
            .thread_name(|i| format!("oceanfs-rayon-{i}"))
            .build_global()
        {
            tracing::warn!(error = %e, "rayon global pool already initialized; using existing");
        }

        info!(
            node_id = %config.node_id,
            listen_addr = %config.listen_addr,
            grpc_addr = %config.grpc_listen_addr,
            "Starting OceanFS node"
        );

        // ---- 0. Storage pool registry (ADR-0029) + role-pinned paths ----
        // The registry probes every configured pool root at boot: the
        // `Fatal` policy refuses to start on an unprobeable root, the
        // `Degraded` policy registers the pool as Degraded (f4 falls back
        // to the legacy path for that role with a WARN). The role-pinned
        // dirs resolve ONCE here — the write path never re-resolves them
        // (perf guidelines 3.4/7.1: boot-time only, no locks in the hot
        // path). Legacy mode (no pools) resolves byte-for-byte to today's
        // `{data_dir}/{metadata,wal,event-wal,hints}` layout.
        let pool_registry = Arc::new(
            oceanfs_storage::PoolRegistry::from_config(&config.storage, &config.data_dir)
                .map_err(|e| format!("storage pool registry: {e}"))?,
        );
        let paths =
            crate::pool_paths::pool_paths(&pool_registry, &config.data_dir, &config.hint_wal_dir);

        // ---- 1. Open metadata store ----
        let metadata_config =
            MetadataConfig { data_dir: paths.metadata.clone(), ..Default::default() };
        let metadata_store = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&metadata_config)
                .map_err(|e| format!("failed to open metadata store: {e}"))?,
        );

        // ---- 2. Probe acceleration hardware ----
        let accel_config = AccelConfig::default();
        let accel = Arc::new(oceanfs_accel::AccelDispatcher::new(accel_config));

        // ---- 3. Construct routing ----
        let ring_config = RingConfig {
            vnodes_per_node: config.vnodes_per_node,
            replication_factor: config.replication_factor as u8,
        };
        let ring = oceanfs_routing::Ring::new(ring_config);
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));

        // ---- 4. Construct membership ----
        let grpc_addr: SocketAddr = config
            .grpc_listen_addr
            .parse()
            .map_err(|e| format!("invalid grpc_listen_addr: {e}"))?;
        // ADR-0028 D1: the announced membership address is the membership
        // plane's listen address with the data-plane's advertised IP
        // substituted for 0.0.0.0 (the gRPC address is already the
        // reachable IP — the deploy scripts write the node's IP there).
        let membership_addr: SocketAddr = config
            .membership_listen_addr
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 9002)));
        let membership_announce_addr = if membership_addr.ip().is_unspecified() {
            oceanfs_membership::plane::membership_address(
                &config.membership_listen_addr,
                Some(&grpc_addr.ip().to_string()),
            )
        } else {
            membership_addr
        };
        let gossip_config = config.gossip.clone();
        let membership = Arc::new(oceanfs_membership::Membership::new(
            NodeId::new(&config.node_id),
            membership_announce_addr,
            grpc_addr,
            gossip_config,
            ring_cache.clone(),
        ));

        // ---- 4a. Rejoin state (ADR-0022) ----
        // Load the persisted incarnation and fallback seeds so a restart
        // rejoins as the same identity with a bumped incarnation (D1) and
        // can re-contact the cluster when configured seeds are unreachable
        // or empty (D3).
        let membership_state_store =
            MembershipStateStore::new(default_state_path(&config.data_dir));
        let durable_state = membership_state_store.load().map_err(|e| {
            format!(
                "failed to load membership state at {}: {e}",
                default_state_path(&config.data_dir).display()
            )
        })?;

        // Announce with persisted + 1; first boot keeps 1 (spec §13.1).
        let announce_incarnation = durable_state.self_incarnation.map_or(1, |p| p + 1);

        // Write-through the bump BEFORE announcing: if the process dies
        // after announcing but before persisting, the next restart would
        // re-announce the same incarnation and be rejected as stale.
        membership_state_store
            .save_incarnation(announce_incarnation)
            .map_err(|e| format!("failed to persist self incarnation: {e}"))?;
        info!(
            node_id = %config.node_id,
            incarnation = announce_incarnation,
            fallback_seeds = durable_state.fallback_seeds.len(),
            "rejoin state loaded: announcing with bumped incarnation"
        );

        // ---- 5. Construct connection pools ----
        let rpc_config = RpcConfig::default();
        let quickack = rpc_config.quickack;
        let busy_poll = rpc_config.busy_poll_us;
        // ADR-0028 D1: the membership plane has its own dedicated pool
        // (per-peer 2, probe-derived timeouts) so probe/gossip latency is
        // never coupled to the data plane's channel semaphore.
        let membership_pool = oceanfs_membership::plane::membership_pool(
            config.gossip.failure_timeout_ms / 3,
            rpc_config.tls_cert_path.clone(),
        );
        let pool = Arc::new(oceanfs_network::ConnectionPool::new(rpc_config));
        membership.set_pool(membership_pool.clone());

        // ---- 6. Construct storage components ----
        let segment_size = SegmentSizeConfig::default();
        let wal_config = WalConfig { data_dir: paths.wal.clone(), ..WalConfig::default() };
        let wal_writer = Arc::new(
            oceanfs_storage::WalWriter::open(&wal_config)
                .await
                .map_err(|e| format!("failed to open WAL writer: {e}"))?,
        );

        // Per-core segment shards for write concurrency (perf rule §2.5).
        let shard_count =
            shard::derive_shard_count(config.segment_shard_count, config.segment_shard_count_max);
        // Scale buffer pool max chunks by shard count (Item 8, D8.5).
        let total_pool_chunks = config.buffer_pool_max_chunks * shard_count;
        // Validate memory budget (Item 8, D8.3). The budget is the real
        // buffer-pool memory: per-shard pool bytes (chunk bytes × max
        // chunks) × shard count. The old call multiplied by segment size
        // as well, producing a 2.2 TB false positive on every boot (F5).
        let _ = crate::startup::validate_shard_memory_budget(
            shard_count,
            config.buffer_pool_chunk_bytes * config.buffer_pool_max_chunks,
        );
        let shard_buffer_pool = Arc::new(oceanfs_storage::BufferPool::new(
            config.buffer_pool_chunk_bytes,
            total_pool_chunks,
        ));
        let shard_small = Arc::new(
            SegmentShard::new(shard_count, SizeTier::Small, &segment_size, &shard_buffer_pool)
                .map_err(|e| format!("failed to create small segment shard: {e}"))?,
        );
        let shard_standard = Arc::new(
            SegmentShard::new(shard_count, SizeTier::Standard, &segment_size, &shard_buffer_pool)
                .map_err(|e| format!("failed to create standard segment shard: {e}"))?,
        );
        // Keep clones for metric registration — the originals are moved
        // into the write coordinator below.
        let shard_small_metrics = Arc::clone(&shard_small);
        let shard_standard_metrics = Arc::clone(&shard_standard);

        // Segment pools for pipeline parallelism (perf rule §2.7).
        // Created before WAL replay so that replayed entries can be
        // reconstructed into active segments (C4-storage, D6).
        let pool_config = PoolConfig::default();
        // EC codec for the segment pools: work items carry (k, m, strip)
        // so the seal worker computes and persists per-segment parity at
        // seal time (single scheduler — the parallel encode runs on the
        // blocking pool). Matches the heal codec configuration.
        let pool_ec_config = oceanfs_core::CodecConfig::default();
        // The pools consume `pool_ec_config` below (the machine's
        // seal-on-zero freeze uses the same codec).
        // Seal-time EC parity routes through the accel dispatcher so the
        // encode is observable (accel_encode_ops_total, duration
        // histograms, fallbacks) — the accel tier is exercised on the
        // write path, not just in isolation.
        let pool_ec_encoder: Option<std::sync::Arc<dyn oceanfs_ec::Encoder>> = Some(accel.clone());
        // The lifecycle registry is constructed FIRST (ADR-0025
        // Decision 2): the pools hold it for the read path and the
        // in-flight attach, and the coordinator wraps the same instance
        // (construction order: registry → pools → coordinator).
        let lifecycle_registry =
            Arc::new(oceanfs_storage::SegmentLifecycleRegistry::new(&config.lifecycle));
        let segment_pool_small = Arc::new(
            SegmentPool::new(
                pool_config.clone(),
                SizeTier::Small,
                &segment_size,
                shard_buffer_pool.clone(),
                Some(pool_ec_config.clone()),
                pool_ec_encoder.clone(),
                Arc::clone(&lifecycle_registry),
            )
            .map_err(|e| format!("failed to create small segment pool: {e}"))?,
        );
        let segment_pool_standard = Arc::new(
            SegmentPool::new(
                pool_config,
                SizeTier::Standard,
                &segment_size,
                shard_buffer_pool.clone(),
                Some(pool_ec_config),
                pool_ec_encoder,
                Arc::clone(&lifecycle_registry),
            )
            .map_err(|e| format!("failed to create standard segment pool: {e}"))?,
        );

        // ADR-0001: tiered segment sizing driven by SegmentSizeConfig.
        // ---- f5: pool-aware segment placement + resolution ----
        // Sealed segments spread across the node's data pools: the sealer
        // selects the target once per segment (PlacementPolicy over the
        // registry snapshot) and stamps `pool_id` on the metadata; every
        // reader/GC store resolves the owning root through this resolver
        // (the lifecycle registry's `SegmentMetadata.pool_id`, durable via
        // the event WAL + checkpoint).
        // Legacy mode (no pools configured) must pass an EMPTY pool list:
        // the registry's implicit data pool (root = data_dir) is a
        // runtime fallback, not a placement target — the sealer's legacy
        // branch (empty `data_pools` → `data_dir`, pool_id 0) keeps
        // today's byte-for-byte layout.
        let data_pools =
            if config.storage.pools.is_empty() { Vec::new() } else { pool_registry.data_pools() };
        let segment_legacy_dir = config.data_dir.join("segments");
        let pool_id_for: oceanfs_storage::PoolIdResolver = {
            let registry = Arc::clone(&lifecycle_registry);
            Arc::new(move |segment_id: &oceanfs_core::SegmentId| {
                registry.get(*segment_id).map(|entry| entry.metadata.pool_id)
            })
        };
        let seal_config = oceanfs_storage::SealConfig {
            data_pools: data_pools.clone(),
            target_size_bytes: segment_size.default_target_size,
            seal_timeout_ms: 5000,
            data_dir: segment_legacy_dir.clone(),
            io_mode: oceanfs_storage::io::IoReadMode::from_config(config.read_cache_segments),
            write_mode: oceanfs_storage::io::SegmentWriteMode::probe(segment_legacy_dir.clone()),
            // Seal pipeline batching (userland-configurable): the fsync
            // group-commit window and the early-flush trigger size.
            fsync_batch_timeout_ms: config.seal_fsync_batch_timeout_ms,
            fsync_max_waiters: config.seal_fsync_max_waiters,
        };
        // SegmentSealer is the authoritative persistence path. Sealed
        // segments are written to {data_dir}/segments/ (legacy) or the
        // selected data pool root (pool mode, ADR-0029 f5) with the
        // configured I/O mode (O_DIRECT or buffered). The shared segment
        // data store is used by anti-entropy and healing below.
        let segment_dir = segment_legacy_dir.clone();
        // The seal worker runs BEFORE the WAL replay (replayed segments
        // seal during replay), so the segment directory must already
        // exist when the first replay seal fires.
        if let Err(e) = std::fs::create_dir_all(&segment_dir) {
            return Err(std::io::Error::other(format!(
                "cannot create segments directory {:?}: {e}",
                segment_dir
            ))
            .into());
        }

        // ---- 6b. Segment lifecycle coordinator (ADR-0025 phase 2) ----
        // The ONLY writer of segment lifecycle state. The write path
        // reserves through it before the first WAL entry of each
        // segment; the seal path (via the flush coordinator) seals
        // through it; the orphan reaper deletes through it. The
        // registry is seeded from the segments CF so the coordinator
        // is the complete single writer over EXISTING data too (the
        // reaper's request_delete validates against it) — a pure
        // registry fold, no CF writes.
        //
        // The event WAL (ADR-0024) is the coordinator's durable
        // side-effect: every transition appends its event first; the CF
        // write becomes a derived mirror performed after the event
        // (dual-read verification surface — the event log is the source
        // of truth for segment lifecycle).
        // The event WAL dir is composed from `{data_dir}/event-wal` in
        // legacy mode exactly like every other data path (data WAL at
        // `{data_dir}/wal`, metadata at `{data_dir}/metadata`); in pool
        // mode it rides the pinned wal pool root (`{wal pool}/event-wal`,
        // ADR-0024 + ADR-0029 §D8 role pinning). The crate-level default
        // (`/var/lib/oceanfs/event-wal`) is the system-layout default, and
        // the node always overrides it — the same pattern applied to
        // `WalConfig` above. Without this, any non-root run (dev, e2e,
        // tests) fails to open the event WAL with permission denied.
        let event_wal_config = oceanfs_core::EventWalConfig {
            event_wal_dir: paths.event_wal.clone(),
            ..config.event_wal.clone()
        };
        let event_wal = Arc::new(
            oceanfs_storage::EventWal::open(
                event_wal_config.event_wal_dir.clone(),
                &event_wal_config,
            )
            .await
            .map_err(|e| format!("failed to open segment event WAL: {e}"))?,
        );
        // The event log's own GC (ADR-0024 Decision 3): byte-threshold
        // snapshots of the folded registry + truncation of covered
        // events. Checkpoint files live beside the event WAL files.
        let event_checkpoint = Arc::new(
            oceanfs_storage::EventCheckpoint::open(
                event_wal_config.event_wal_dir.clone(),
                event_wal.clone(),
            )
            .map_err(|e| format!("failed to open event WAL checkpoint manager: {e}"))?,
        );
        let lifecycle = Arc::new(
            oceanfs_storage::SegmentLifecycleCoordinator::with_registry(Arc::clone(
                &lifecycle_registry,
            ))
            // Phase 2: event appends become the durable side-effect;
            // the CF write is demoted to a derived mirror.
            .with_event_wal(event_wal.clone())
            // Checkpoint trigger: threshold-only, off the append path.
            .with_checkpoint(event_checkpoint.clone(), event_wal_config.clone())
            // The seal pools: the pending-seal drain freezes partial
            // segments through them (seal-on-zero — no idle timer).
            .with_seal_pools(vec![segment_pool_small.clone(), segment_pool_standard.clone()]),
        );
        let sealer = Arc::new(oceanfs_storage::SegmentSealer::new(
            seal_config,
            wal_writer.clone(),
            lifecycle.clone(),
        ));
        // ---- 7. Construct durability workers ----
        let gc_config = oceanfs_durability::GcConfig::new(
            config.gc_interval_sec,
            config.tombstone_ttl_sec,
            config.gc_compact_threshold,
            config.gc_max_concurrent_compactions,
            config.gc_compaction_queue_capacity,
        );
        // GC compaction repacks live blobs into new segments — it must
        // persist the repacked data through the segment data store (the
        // compactor reads the old segment's bytes and writes the new
        // segment's .dat before the metadata swap; without the store, a
        // metadata-only remap would leave objects pointing at a segment
        // with no on-disk data).
        // GC compaction is a state machine (ADR-0025 Decision 4): the
        // compactor requests every transition from the lifecycle
        // coordinator and unlinks the old .dat through the shard store
        // only after the durable delete.
        let gc_worker = Arc::new(
            oceanfs_durability::GarbageCollector::new(gc_config.clone())
                .with_data_store(Arc::new(oceanfs_durability::DiskSegmentStore::new(
                    data_pools.clone(),
                    segment_legacy_dir.clone(),
                    pool_id_for.clone(),
                )))
                .with_lifecycle(lifecycle.clone())
                .with_shard_store(Arc::new(oceanfs_durability::DiskSegmentShardStore::new(
                    data_pools.clone(),
                    segment_legacy_dir.clone(),
                    pool_id_for.clone(),
                ))),
        );

        // Construct IncrementalMerkleTree for anti-entropy by scanning
        // the machine's Sealed entries — supersedes ADR-0018 Decision
        // 1's segments-CF scan (ADR-0025 Decision 3).
        let merkle_tree_config = oceanfs_durability::merkle::MerkleTreeConfig::default();

        let merkle_tree = {
            Arc::new(
                oceanfs_durability::merkle::IncrementalMerkleTree::rebuild_from_segment_scan(
                    &lifecycle_registry,
                    &merkle_tree_config,
                )
                .map_err(|e| {
                    std::io::Error::other(format!(
                        "failed to rebuild Merkle tree from the machine scan: {e}"
                    ))
                })?,
            )
        };

        let ae_worker = Arc::new(oceanfs_durability::AntiEntropy::new(
            oceanfs_durability::AntiEntropyConfig::new(
                config.ae_interval_sec,
                config.ae_peer_count,
            )
            .with_core(oceanfs_core::AntiEntropyConfig {
                continuous_enabled: config.anti_entropy.continuous_enabled,
                continuous_max_segments: config.anti_entropy.continuous_max_segments,
                sampling_enabled: config.anti_entropy.sampling_enabled,
                sampling_interval_sec: config.anti_entropy.sampling_interval_sec,
                sampling_fraction: config.anti_entropy.sampling_fraction,
            }),
            membership.clone(),
            Arc::clone(&lifecycle_registry),
            pool.clone(),
            Arc::new(oceanfs_durability::DiskSegmentStore::new(
                data_pools.clone(),
                segment_legacy_dir.clone(),
                pool_id_for.clone(),
            )),
            merkle_tree.clone(),
        ));
        let mut scrub_config = oceanfs_durability::ScrubConfig::default();
        scrub_config.set_interval_sec(config.scrub_interval_sec);
        scrub_config.set_parallel_nodes(config.scrub_parallel_nodes);
        let scrub_worker = Arc::new(oceanfs_durability::ScrubCoordinator::new(scrub_config));
        // OrphanReaper deletes segment data files from disk when reclaiming
        // orphaned segments after GC compaction.
        let reaper_shard_store: Arc<dyn oceanfs_durability::SegmentShardStore> =
            Arc::new(oceanfs_durability::DiskSegmentShardStore::new(
                data_pools.clone(),
                segment_legacy_dir.clone(),
                pool_id_for.clone(),
            ));
        let reaper = Arc::new(oceanfs_durability::OrphanReaper::new(
            metadata_store.clone(),
            lifecycle.clone(),
            reaper_shard_store.clone(),
            gc_config,
        ));

        // ---- 7b. Construct segment data store (shared by heal and gRPC) ----
        // DiskSegmentStore reads/writes the authoritative segment files.
        let heal_data_store: Arc<dyn oceanfs_durability::SegmentDataStore> =
            Arc::new(oceanfs_durability::DiskSegmentStore::new(
                data_pools.clone(),
                segment_legacy_dir.clone(),
                pool_id_for.clone(),
            ));

        // ---- 7c. Construct heal dispatch pipeline ----
        let heal_config = oceanfs_durability::HealConfig::default()
            .with_max_concurrent_heals(config.heal_parallel_segments)
            .with_heal_throttle_bytes_sec(config.heal_throttle_bytes_sec);
        let heal_queue = Arc::new(oceanfs_durability::HealQueue::new(heal_config.queue_capacity()));
        // Initialize the global heal sender so scrub and anti-entropy can
        // call enqueue_heal() without direct queue access.
        oceanfs_durability::heal::init_global_queue(heal_queue.sender());
        let heal_codec_config = oceanfs_core::CodecConfig::default();
        // The heal decoder routes through the accel dispatcher so decode
        // repair work is observable (accel_decode_ops_total, duration
        // histograms) and the tier is consistent across sites.
        let heal_decoder: Arc<dyn oceanfs_ec::Decoder> = accel.clone();
        // Clone before move into HealWorker — used by ReadCoordinator as well.
        let ec_decoder = heal_decoder.clone();

        // ---- 7d. Construct per-operation timeouts (Item 4) ----
        // Must be constructed before heal, hinted_handoff, write_coordinator,
        // and read_coordinator so they can accept it via their with_timeouts() setters.
        let op_timeouts = Arc::new(config.operation_timeouts);

        let heal_worker = oceanfs_durability::HealWorker::new(
            heal_config,
            heal_queue.clone(),
            heal_decoder,
            lifecycle.clone(),
            heal_data_store.clone(),
        )
        .with_timeouts(op_timeouts.clone());

        // ---- 8. Construct caches ----
        let l1_policy: Box<dyn oceanfs_cache::eviction::EvictionPolicy> = match config
            .eviction_policy_l1
        {
            oceanfs_core::EvictionPolicyType::Gdsf => {
                Box::new(oceanfs_cache::eviction::GdsfPolicy::new(
                    oceanfs_cache::eviction::GdsfConfig::default(),
                ))
            }
            oceanfs_core::EvictionPolicyType::TtlLru => Box::new(
                oceanfs_cache::eviction::TtlLruPolicy::new(oceanfs_cache::eviction::TtlLruConfig {
                    default_ttl_ms: config.object_cache_ttl_ms,
                }),
            ),
            oceanfs_core::EvictionPolicyType::Adaptive => {
                tracing::warn!(
                    "Adaptive eviction policy not yet implemented; falling back to GDSF for L1"
                );
                Box::new(oceanfs_cache::eviction::GdsfPolicy::new(
                    oceanfs_cache::eviction::GdsfConfig::default(),
                ))
            }
            _ => {
                tracing::warn!("Unknown L1 eviction policy; falling back to GDSF");
                Box::new(oceanfs_cache::eviction::GdsfPolicy::new(
                    oceanfs_cache::eviction::GdsfConfig::default(),
                ))
            }
        };
        let l2_policy: Box<dyn oceanfs_cache::eviction::EvictionPolicy> = match config
            .eviction_policy_l2
        {
            oceanfs_core::EvictionPolicyType::TtlLru => Box::new(
                oceanfs_cache::eviction::TtlLruPolicy::new(oceanfs_cache::eviction::TtlLruConfig {
                    default_ttl_ms: config.metadata_cache_ttl_ms,
                }),
            ),
            oceanfs_core::EvictionPolicyType::Gdsf => {
                Box::new(oceanfs_cache::eviction::GdsfPolicy::new(
                    oceanfs_cache::eviction::GdsfConfig::default(),
                ))
            }
            oceanfs_core::EvictionPolicyType::Adaptive => {
                tracing::warn!(
                    "Adaptive eviction policy not yet implemented; falling back to TTL-LRU for L2"
                );
                Box::new(oceanfs_cache::eviction::TtlLruPolicy::new(
                    oceanfs_cache::eviction::TtlLruConfig::default(),
                ))
            }
            _ => {
                tracing::warn!("Unknown L2 eviction policy; falling back to TTL-LRU");
                Box::new(oceanfs_cache::eviction::TtlLruPolicy::new(
                    oceanfs_cache::eviction::TtlLruConfig::default(),
                ))
            }
        };
        let object_cache = Arc::new(oceanfs_cache::ObjectCache::new(
            oceanfs_cache::ObjectCacheConfig {
                enabled: config.object_cache_enabled,
                max_size_bytes: config.object_cache_size_bytes,
                ttl_ms: config.object_cache_ttl_ms,
                max_blob_size: config.object_cache_max_blob_size,
                ..Default::default()
            },
            l1_policy,
        ));
        let metadata_cache = Arc::new(oceanfs_cache::MetadataCache::new(
            oceanfs_cache::MetadataCacheConfig {
                enabled: config.metadata_cache_enabled,
                max_size_bytes: config.metadata_cache_size_bytes,
                ttl_ms: config.metadata_cache_ttl_ms,
                ..Default::default()
            },
            l2_policy,
        ));
        let negative_cache =
            Arc::new(oceanfs_cache::NegativeCache::new(oceanfs_cache::NegativeCacheConfig {
                enabled: config.negative_cache_enabled,
                size_bytes: config.negative_cache_size_bytes,
                rebuild_interval_sec: config.negative_cache_rebuild_sec,
                ..Default::default()
            }));

        // ---- 9. Construct prefetch engine ----
        let prefetch_config = oceanfs_cache::PrefetchConfig {
            enabled: config.prefetch_enabled,
            after_list: config.prefetch_after_list,
            after_get: config.prefetch_after_get,
            ..Default::default()
        };
        let prefetch_store: Arc<dyn oceanfs_storage_api::MetadataStore> =
            Arc::new(PrefetchStoreAdapter { store: metadata_store.clone() });
        let prefetch_engine = Arc::new(oceanfs_cache::PrefetchEngine::new(
            prefetch_config,
            metadata_cache.clone(),
            Some(object_cache.clone()),
            prefetch_store,
        ));

        // ---- 10. Construct bridge adapter ----
        let metadata_ops: Arc<dyn MetadataOps> =
            Arc::new(MetadataStoreAdapter::new(metadata_store.clone()));

        // ---- 11. Construct I/O infrastructure ----
        let hlc_clock = Arc::new(HlcClock::new());
        let disk_io = Arc::new(oceanfs_storage::io::DiskIo::new());
        let io_mode = oceanfs_storage::io::IoReadMode::from_config(config.read_cache_segments);

        // Build the mmap segment cache when read-optimised mode is enabled.
        let mmap_cache = if io_mode == oceanfs_storage::io::IoReadMode::Mmap {
            Some(Arc::new(oceanfs_storage::io::SegmentFileCache::new(
                config.segment_cache_max_entries,
            )))
        } else {
            None
        };

        // Disk-backed segment reader: reads sealed segment files from disk
        // via the configured I/O mode (mmap / O_DIRECT / buffered).
        // Replaces the previous InMemorySegmentReader — segment data is read
        // on demand from the filesystem. No startup preload, no unbounded
        // HashMap growth.
        let segment_reader: Arc<dyn oceanfs_storage::io::SegmentReader> = Arc::new(
            oceanfs_storage::io::DiskSegmentReader::new(
                io_mode,
                disk_io.clone(),
                mmap_cache,
                segment_dir.clone(),
                Some(accel.clone()),
                Some(accel.clone()),
            )
            // Pool-aware resolution (ADR-0029 f5): sealed segments read
            // from the owning data pool root.
            .with_data_pools(data_pools.clone(), segment_legacy_dir.clone(), pool_id_for.clone())
            .with_evict_after_read(!config.read_cache_segments),
        );

        let hinted_handoff = Arc::new(
            HintedHandoff::new_with_pool(pool.clone())
                .with_membership(membership.clone())
                .with_timeouts(op_timeouts.clone()),
        );

        // Construct the persistent per-node HintWAL directory and
        // HintedHandoffManager for durable hinted handoff (ADR-0018 Decision 2).
        // The hints WAL lives on the pinned hints pool root when
        // configured; the legacy `hint_wal_dir` override (or
        // `{data_dir}/hints`) applies otherwise (resolved in pool_paths).
        let hints_dir = paths.hints.clone();
        let hint_delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> = Arc::new(
            GrpcHintDeliveryClient::new(pool.clone())
                // The hint receiver fetches segment-ref data back
                // from THIS node's gRPC listener (remote_addr on the
                // receiver is the ephemeral source port — dead by
                // fetch time).
                .with_self_grpc_addr(
                    config
                        .grpc_listen_addr
                        .parse::<std::net::SocketAddr>()
                        .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 9001))),
                ),
        );
        let hint_config = HintedHandoffConfig {
            wal_dir: hints_dir.clone(),
            inline_threshold_bytes: config.hint_inline_threshold_bytes,
            max_batch_size: config.hint_max_batch_size,
            max_batch_bytes: 32 * 1024 * 1024,
        };
        let hinted_handoff_manager = Arc::new(
            HintedHandoffManager::new(hints_dir.clone(), hint_delivery_client, hint_config.clone())
                .with_membership(membership.clone())
                .with_timeouts(op_timeouts.clone()), // Delivery contract (ADR-0027 as amended): hints are
                                                     // NEVER dropped at the sender — deliver everything, the
                                                     // receiver's HLC-LWW apply is the single gate. The old
                                                     // obsolete pre-check dropped hints based on the sender's
                                                     // view of distributed state, which could diverge from
                                                     // the truth (the churn residual class).
        );

        // Replay existing hints from the WAL into in-memory queues.
        let _replayed = hinted_handoff_manager.replay_and_enqueue().await?;

        // Clone pool Arcs for the read path — the originals are consumed
        // by WriteCoordinator below. PoolFallbackReader checks active
        // (unsealed) segments before falling back to DiskSegmentReader,
        // closing the read-after-write gap for recently-written data.
        let active_pools: Vec<Arc<SegmentPool>> =
            vec![segment_pool_small.clone(), segment_pool_standard.clone()];
        // Retained for the live `segment_active_count` metric poller.
        let active_pools_for_metrics = active_pools.clone();
        let segment_reader = Arc::new(oceanfs_storage::io::PoolFallbackReader::new(
            active_pools,
            segment_reader.clone(),
        ));

        // Clone for the seal notifier closure (the engine itself is
        // consumed by the background-task spawn later).
        let ae_worker_notifier = Arc::clone(&ae_worker);
        // Cluster-readiness gate (phase-3 churn fix): a node that just
        // (re)joined a cluster has a ring containing only itself until
        // its membership pull converges; with the adaptive quorum such a
        // window ACKs writes with a single durable copy (silent
        // under-replication). While the gate is closed the write path
        // returns 503 instead. Single-node deployments (no seeds, no
        // fallback seeds) never close the gate.
        let ready_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let is_cluster_node = !config.gossip.seed_nodes.is_empty()
            || membership_state_store
                .load()
                .map(|state| !state.fallback_seeds.is_empty())
                .unwrap_or(false);
        if is_cluster_node {
            let gate_membership = membership.clone();
            let gate = ready_gate.clone();
            let gate_timeout_secs = config.cluster_ready_timeout_sec.max(1);
            tokio::spawn(async move {
                // Open the gate when the ring reaches 2 nodes (enough
                // for w=2 semantics) or after the configured bound —
                // the rejoin pull takes seconds; the bound keeps a node
                // whose seeds are unreachable from stalling writes
                // forever (it would serve stale data anyway — the 503s
                // it emits while gated are the safer failure mode).
                // The timeout is config (`cluster_ready_timeout_sec`)
                // because convergence scales with the gossip profile.
                let deadline =
                    tokio::time::Instant::now() + std::time::Duration::from_secs(gate_timeout_secs);
                loop {
                    let ring_nodes = gate_membership.ring().snapshot().node_count();
                    if ring_nodes >= 2 || tokio::time::Instant::now() >= deadline {
                        gate.store(true, std::sync::atomic::Ordering::Release);
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            });
        } else {
            ready_gate.store(true, std::sync::atomic::Ordering::Release);
        }
        let write_coordinator = Arc::new(
            WriteCoordinator::new(
                ring_cache.clone(),
                membership.clone(),
                pool.clone(),
                NodeId::new(&config.node_id),
                hlc_clock.clone(),
                metadata_store.clone(),
                segment_size.clone(),
                shard_small,
                shard_standard,
                segment_pool_small.clone(),
                segment_pool_standard.clone(),
                sealer.clone(),
                lifecycle.clone(),
                hinted_handoff_manager.clone(),
            )
            .with_timeouts(op_timeouts.clone())
            // Per-bucket compression: buckets opting in via
            // `compression.tier != None` compress chunks on the write
            // path through the accel dispatcher (blocking pool).
            .with_compressor(Some(accel.clone()))
            // Cluster-readiness gate: while the ring is still converging
            // after (re)join, writes fail with 503 instead of
            // under-replicating (see the gate task above).
            .with_ready_gate(ready_gate)
            // Step 1c (honest quorum): cluster nodes require the ring
            // view to satisfy the requested write quorum; single-node
            // deployments (no seeds) keep the adaptive capping — the
            // default bucket policy (w=2) would otherwise reject every
            // write on a permanently 1-node ring.
            .with_quorum_requires_ring(is_cluster_node)
            .with_hint_inline_threshold(config.hint_inline_threshold_bytes)
            // Continuous anti-entropy: every successful seal updates the
            // incremental Merkle tree (with its seal-time root) so
            // recently-written segments participate in the root exchange
            // without waiting for the next startup rebuild.
            .with_segment_sealed_notifier(Arc::new(move |segment_id, merkle_root| {
                ae_worker_notifier.on_segment_sealed(segment_id, merkle_root);
            })),
        );

        // Start background seal worker — drains filled segments from both
        // pools and writes them to disk via the segment sealer (Epic 3).
        let _seal_handle = write_coordinator.start_seal_worker();
        // Clone for the hint-applier adapter (the coordinator is moved
        // into the S3 handler state below).
        let write_coordinator_for_applier = Arc::clone(&write_coordinator);

        // ---- 6a. Startup recovery: the machine path (ADR-0025 phase 2) ----
        // Deterministic recovery: fold the event log into the registry
        // (state = fold(events)) — the event log is the ONLY durable
        // writer (ADR-0025 Decision 3 final form; the CF mirror is
        // removed) — then rebuild Reserved-unsealed segments from the
        // data WAL (adopt the durable `.dat` or replay the entries),
        // then resolve incomplete compaction units (rows 7-9). The
        // startup cost is bounded by the checkpoint threshold, never by
        // lifetime event volume.
        let startup_rebuild_gauge = oceanfs_core::Gauge::new(
            "oceanfs_startup_rebuild_ms".into(),
            "Startup rebuild duration (checkpoint + fold + data-WAL pass + compaction recovery), ms"
                .into(),
            oceanfs_core::LabelSet::empty(),
        );
        let rebuild_start = std::time::Instant::now();
        let wal_reader = oceanfs_storage::wal::WalReader::open(&wal_config)
            .map_err(|e| format!("failed to open WAL reader: {e}"))?;
        // Load the latest checkpoint (ADR-0024 Decision 3): its snapshot
        // seeds the registry; the fold starts at its covered position —
        // startup replay is bounded by the byte threshold, not by
        // lifetime event volume. Without a checkpoint the fold starts at
        // the earliest retained event.
        let fold_start = match event_checkpoint
            .load_checkpoint()
            .map_err(|e| format!("failed to load event WAL checkpoint: {e}"))?
        {
            Some((snapshot, covered)) => {
                lifecycle.seed_from_checkpoint(&snapshot);
                info!(covered = ?covered, "event WAL checkpoint loaded; folding events after it");
                covered
            }
            None => oceanfs_storage::EventWalPos { file_seq: 0, offset: 0 },
        };
        let recovery_outcome = lifecycle
            .rebuild_with_data_wal(
                event_wal.read_from(fold_start),
                &wal_reader,
                &sealer,
                |data| {
                    oceanfs_durability::MerkleTree::build(data, 0).map(|tree| tree.root().hash())
                },
                &wal_writer,
            )
            .await
            .map_err(|e| format!("event-WAL recovery failed: {e}"))?;
        info!(
            folded = recovery_outcome.folded_segments,
            dropped_empty = recovery_outcome.dropped_empty_reserves,
            re_sealed = recovery_outcome.re_sealed_segments,
            adopted = recovery_outcome.adopted_segments,
            swept = recovery_outcome.swept_entries,
            "event-WAL recovery complete (ADR-0025 phase 2)"
        );
        // Rows 7-9: incomplete compaction units — the folded registry's
        // `repacked_from` markers, one objects-CF read per unit. Each
        // action deletes through the coordinator (durable before
        // unlink) and sweeps the `.dat` (idempotent).
        let compaction_actions = recover_incomplete_compactions(
            lifecycle.registry(),
            &StoreObjectLookup(
                Arc::clone(&metadata_store) as Arc<dyn oceanfs_storage_api::MetadataStore>
            ),
        )
        .map_err(|e| format!("compaction recovery failed: {e}"))?;
        for action in &compaction_actions {
            let (segment_id, label) = match action {
                CompactionRecoveryAction::FinishOldDeletion(id) => (*id, "finish_old_deletion"),
                CompactionRecoveryAction::SweepNewOrphan(id) => (*id, "sweep_new_orphan"),
                CompactionRecoveryAction::SweepOldDat(id) => (*id, "sweep_old_dat"),
            };
            if !matches!(action, CompactionRecoveryAction::SweepOldDat(_)) {
                if let Err(e) = lifecycle.request_delete(segment_id).await {
                    warn!(
                        segment_id = %segment_id,
                        error = %e,
                        "compaction recovery delete failed (startup continues; the reaper retries)"
                    );
                }
            }
            // Sweep the `.dat` through the pool-aware shard store (the
            // segment's pool id resolves to its root; ADR-0029 f5).
            // Idempotent; a residue the resolver cannot place (an
            // unregistered orphan on a non-zero pool) is backstopped by
            // the orphan reaper's multi-root listing.
            if let Err(e) = reaper_shard_store.delete_shards(segment_id) {
                warn!(
                    segment_id = %segment_id,
                    error = %e,
                    "compaction recovery sweep failed (startup continues; the reaper retries)"
                );
            }
            info!(segment_id = %segment_id, action = label, "compaction recovery action applied");
        }
        let rebuild_ms = rebuild_start.elapsed().as_millis() as u64;
        startup_rebuild_gauge.set(rebuild_ms);
        info!(rebuild_ms, "startup rebuild complete");
        // Retention liveness is machine-backed (ADR-0024 §Retention): an
        // entry at position p of segment S is garbage iff S is sealed
        // with data_wal_pos ≥ p, or deleted. Entries whose segment has
        // no registry entry are unreachable (the reserve-before-entry
        // invariant) — sweepable.
        {
            let registry = Arc::clone(&lifecycle_registry);
            wal_writer.set_liveness(Arc::new(move |id, pos| match registry.get(id) {
                Some(entry) => oceanfs_storage::entry_is_garbage(&entry, &pos),
                None => true,
            }));
        }

        let read_coordinator = Arc::new(
            ReadCoordinator::new_with_metadata(
                ring_cache.clone(),
                NodeId::new(&config.node_id),
                None,
                metadata_ops.clone(),
            )
            .with_segment_reader(segment_reader.clone())
            .with_connection_pool(pool.clone())
            .with_membership(membership.clone())
            .with_decoder(ec_decoder.clone())
            .with_ec_codec(heal_codec_config.data_shards, heal_codec_config.parity_shards)
            // Read-path decompression for compressed chunks (paired
            // with the write path's per-bucket compression).
            .with_compressor(Some(accel.clone()))
            .with_timeouts(op_timeouts.clone())
            .with_default_fetch_strategy(config.default_fetch_strategy)
            .with_hlc_clock(hlc_clock.clone()),
        );

        // Router handles request forwarding to correct coordinator nodes.
        let router = Arc::new(Router::new(
            ring_cache.clone(),
            membership.clone(),
            pool.clone(),
            NodeId::new(&config.node_id),
        ));

        // ---- 12. Construct handlers ----
        let bucket_store = Arc::new(BucketConfigStore::new());
        // Bounded write queue: at most `max_inflight_writes` concurrent
        // PUTs; requests beyond the bound wait up to `write_queue_ms`
        // then receive 503 SlowDown (backpressure propagates to the HTTP
        // layer instead of failing mid-write).
        let write_queue = Arc::new(tokio::sync::Semaphore::new(config.max_inflight_writes));
        let write_queue_timeout =
            std::time::Duration::from_millis(config.operation_timeouts.write_queue_ms);
        // Clone for the hint-fetch reader (the coordinator is moved into
        // the S3 handler below).
        let read_coordinator_for_hints = read_coordinator.clone();
        let s3_handler = S3Handler::new_with_caches_and_backpressure(
            write_coordinator,
            read_coordinator,
            metadata_ops,
            bucket_store.clone(),
            Some(object_cache.clone()),
            Some(metadata_cache.clone()),
            Some(negative_cache.clone()),
            Some(write_queue),
            write_queue_timeout,
        )
        .with_prefetch_engine(prefetch_engine.clone())
        .with_router(router);

        let metrics = Arc::new(oceanfs_server::admin::MetricsRegistry::new());
        let metrics_for_late_registration = Arc::clone(&metrics);

        // Register subsystem metrics into the central registry.
        metrics.register_gauge(startup_rebuild_gauge);
        object_cache.register_metrics(&*metrics);
        metadata_cache.register_metrics(&*metrics);
        negative_cache.register_metrics(&*metrics);
        accel.register_metrics(&*metrics);
        heal_worker.register_metrics(&*metrics);
        shard_buffer_pool.register_metrics(&*metrics);
        s3_handler.register_metrics(&*metrics);

        // Phase D: durability subsystem counters.
        gc_worker.register_metrics(&*metrics);
        reaper.register_metrics(&*metrics);
        scrub_worker.register_metrics(&*metrics);
        ae_worker.register_metrics(&*metrics);
        // The manager is the component that actually stores and delivers
        // hints; its counters are the authoritative
        // hinted_handoff_hints_{stored,delivered,expired}_total series.
        // (The legacy HintedHandoff — the gRPC *receiver* — is not
        // registered: its counters had inverted semantics and stayed 0.)
        hinted_handoff_manager.register_metrics(&*metrics);
        pool.register_metrics(&*metrics);
        // Segment shard gauges (`segment_active_count` — Phase 2
        // asserts the segment pipeline is producing segments).
        shard_small_metrics.register_metrics(&*metrics);
        shard_standard_metrics.register_metrics(&*metrics);
        wal_writer.register_metrics(&*metrics);
        sealer.register_metrics(&*metrics);
        // Lifecycle registry-size gauges (ADR-0025 Decision 5 — the
        // registry's O(live segments) memory cost is metric-visible).
        lifecycle.register_metrics(&*metrics);
        // Event WAL metrics (ADR-0024 — bytes, files, append count).
        event_wal.register_metrics(&*metrics);
        // Checkpoint metrics (checkpoint bytes written, bytes truncated).
        event_checkpoint.register_metrics(&*metrics);
        // Storage pool metrics (ADR-0029 — status, bytes free/total,
        // I/O error counter per pool).
        pool_registry.register_metrics(&*metrics);

        // Register RocksDB property gauges into the central metrics registry.
        metadata_store.metrics().register(&*metrics);
        // Start the background RocksDB metrics polling task (every 30s).
        metadata_store.start_metrics_task();

        // Register process-level gauges.
        let proc_mem_gauge =
            metrics.gauge("process_resident_memory_bytes", "Resident memory in bytes");
        let proc_fd_gauge = metrics.gauge("process_open_fds", "Open file descriptors");
        // Storage WAL file count — the Phase 2 `wal_not_unbounded`
        // invariant (sealed segments must keep the WAL consumed).
        let wal_count_gauge = metrics.gauge("wal_file_count", "Storage WAL files present");
        // Live segment-pipeline gauge: the shard registration above sets
        // the initial value; this poller refreshes it from the pools'
        // Appending slots, which churn as segments fill and seal.
        let active_segments_gauge =
            metrics.gauge("segment_active_count", "Active segment groups in the sharded pool");

        // Spawn a background poller for process-level metrics (every 15s).
        // RocksDB metrics are polled separately by metadata_store.start_metrics_task().
        let wal_dir = wal_config.data_dir.clone();
        let _process_poller = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            loop {
                interval.tick().await;
                if let Ok(mem) = read_process_memory_bytes() {
                    proc_mem_gauge.set(mem);
                }
                if let Ok(fds) = read_process_open_fds() {
                    proc_fd_gauge.set(fds);
                }
                let wal_config = WalConfig { data_dir: wal_dir.clone(), ..WalConfig::default() };
                wal_count_gauge.set(oceanfs_storage::count_wal_files(&wal_config) as u64);
                let live = active_pools_for_metrics.iter().map(|p| p.active_count()).sum::<usize>();
                active_segments_gauge.set(live as u64);
            }
        });

        let admin_handler = AdminHandler::new_with_cluster(
            bucket_store,
            metrics,
            membership.clone(),
            ring_cache.clone(),
        )
        .with_scrub(scrub_worker.clone(), metadata_store.clone(), heal_data_store.clone())
        .with_lifecycle_registry(Arc::clone(&lifecycle_registry))
        .with_caches(
            Some(object_cache.clone()),
            Some(metadata_cache.clone()),
            Some(negative_cache.clone()),
        )
        .with_accel(accel.clone());

        // ---- 13. Build axum router ----
        // Auth middleware is config-driven: when `s3_auth_enabled = true`,
        // all S3 routes require valid SigV4 credentials. When disabled,
        // requests pass through without authentication.
        //
        // Access keys are loaded from {data_dir}/access_keys.toml
        // (TOML format: [[keys]]\naccess_key = "..."\nsecret_key = "...")
        let auth_middleware = if config.s3_auth_enabled {
            let keys_path = config.data_dir.join("access_keys.toml");
            let verifier = if keys_path.exists() {
                match oceanfs_server::auth::KeyStore::load(&keys_path) {
                    Ok(store) => {
                        info!(path = %keys_path.display(), "loaded access keys for S3 auth");
                        Some(oceanfs_server::auth::SigV4Verifier::new(store))
                    }
                    Err(e) => {
                        warn!(
                            path = %keys_path.display(),
                            error = %e,
                            "failed to load access keys — auth will reject all requests"
                        );
                        None
                    }
                }
            } else {
                warn!("s3_auth_enabled but no access_keys.toml found at {}", keys_path.display());
                None
            };
            AuthMiddleware::new(true, verifier)
        } else {
            AuthMiddleware::passthrough()
        };
        // `DefaultBodyLimit` rejects oversized requests before any
        // handler runs, which historically made 413s invisible in the
        // node log (F6). The logging middleware below is added *after*
        // the limit layer — axum runs the last-added layer first — so it
        // wraps the limit and observes its 413 responses. The closure
        // captures only the `usize` value, not the whole config.
        let max_body_size = config.max_body_size;
        let app = axum::Router::new()
            .merge(s3_handler.into_router_with_auth(auth_middleware))
            .merge(admin_handler.into_router())
            .layer(axum::extract::DefaultBodyLimit::max(max_body_size))
            .layer(axum::middleware::from_fn(
                move |req: axum::extract::Request, next: axum::middleware::Next| async move {
                    let uri = req.uri().clone();
                    let resp = next.run(req).await;
                    if resp.status() == axum::http::StatusCode::PAYLOAD_TOO_LARGE {
                        tracing::error!(
                            uri = %uri,
                            max_body_size,
                            "request body rejected by max_body_size limit"
                        );
                    }
                    resp
                },
            ));

        // ---- 14. Bind HTTP server ----
        let http_listener = tokio::net::TcpListener::bind(&config.listen_addr)
            .await
            .map_err(|e| format!("failed to bind HTTP server on {}: {e}", config.listen_addr))?;
        let server_addr = http_listener.local_addr()?;

        let http_shutdown = CancellationToken::new();
        let http_shutdown_signal = http_shutdown.clone();

        tokio::spawn(async move {
            if let Err(e) = axum::serve(http_listener, app.into_make_service())
                .with_graceful_shutdown(http_shutdown_signal.cancelled_owned())
                .await
            {
                error!("HTTP server error: {e}");
            }
        });

        // ---- 15. Bind gRPC server ----
        let grpc_addr: SocketAddr = config
            .grpc_listen_addr
            .parse()
            .map_err(|e| format!("invalid grpc_listen_addr: {e}"))?;

        // Build gRPC service implementations.
        let segment_service = oceanfs_server::grpc::segment_service::SegmentGrpcService::new(
            heal_data_store.clone(),
            Some(metadata_store.clone()),
            shard_buffer_pool.clone(),
            hlc_clock.clone(),
        )
        // Replica appends register their segments in the lifecycle
        // machine: without registration the GC and the orphan reaper
        // never see the receiver's .dat files (the fleet disk-fill
        // root cause).
        .with_lifecycle(lifecycle.clone());
        // ADR-0028 D1: the membership services (gossip + probe) move to
        // the membership plane — the data-plane server below hosts only
        // Segment/Healing/Cache/Scrub.
        let gossip_service =
            oceanfs_membership::grpc::gossip_service::GossipGrpcService::new(membership.clone());
        let probe_service = oceanfs_membership::grpc::probe_service::ProbeGrpcService::new(
            NodeId::new(&config.node_id),
            membership.clone(),
            membership_pool.clone(),
            config.gossip.failure_timeout_ms / 3,
        );

        let healing_service = oceanfs_durability::healing_service::HealingGrpcService::new(
            hinted_handoff.clone(),
            metadata_store.clone(),
            Arc::clone(&lifecycle_registry),
            heal_data_store.clone(),
            hlc_clock.clone(),
        )
        .with_local_node_id(NodeId::new(&config.node_id))
        // Hint materialization: hints are resolved BY KEY — the
        // receiver asks the origin for the object's CURRENT state (the
        // metadata is the truth; a GC'd/reaped hinted version was
        // deleted or superseded) and applies it with HLC-LWW. Hints
        // carry no blob data, so they stay small for multipart/GB
        // blobs.
        .with_hint_object_fetcher(Arc::new(oceanfs_durability::GrpcHintObjectFetcher::new(
            pool.clone(),
        )))
        .with_hint_object_reader(Arc::new(
            oceanfs_server::read::ReadCoordinatorHintObjectReader::new(read_coordinator_for_hints),
        ))
        // Hint apply goes through the node's OWN segment pipeline (the
        // write coordinator's local append): the hinted data lands in a
        // local segment with REAL chunk refs instead of the historical
        // inline-in-metadata storage — which ballooned the objects CF
        // (16 MiB blobs) and collapsed the orphan reaper's metadata
        // scan (the fleet disk-fill root cause).
        .with_hint_object_applier(Arc::new(
            oceanfs_server::WriteCoordinatorHintObjectApplier::new(write_coordinator_for_applier),
        ));
        let cache_service = oceanfs_server::grpc::cache_service::CacheGrpcService::new(
            Some(object_cache.clone()),
            Some(metadata_cache.clone()),
        );
        let scrub_service = oceanfs_durability::scrub_service::ScrubGrpcService::new(
            metadata_store.clone(),
            heal_data_store.clone(),
        );

        // Build tonic Server with all services registered. The default
        // gRPC message limit (4 MiB) rejects hinted-handoff batches:
        // hints carry the blob data inline (the phase-3 churn fix), and
        // batches can reach tens of MiB. The healing service (hint
        // receiver) gets a 64 MiB decode limit — the client-side
        // max_batch_bytes cap (32 MiB) keeps batches comfortably inside.
        let grpc_router = tonic::transport::Server::builder()
            // The append replication receiver must accept chunks up to
            // max_body_size (16 MiB in the load-test profile): the 4 MiB
            // tonic default made every >4 MiB replica write fail with
            // OutOfRange "decoded message length too large" — the write
            // degraded to the slow hint path and replicas lagged at
            // verify time (fleet read-quorum failures hot-15/hot-57;
            // the 2 MiB local profile never exceeded the default).
            .add_service(
                oceanfs_storage::SegmentRpcServer::new(segment_service)
                    .max_decoding_message_size(64 * 1024 * 1024),
            )
            .add_service(
                oceanfs_durability::HealingRpcServer::new(healing_service)
                    .max_decoding_message_size(64 * 1024 * 1024),
            )
            .add_service(oceanfs_cache::CacheRpcServer::new(cache_service))
            .add_service(oceanfs_durability::ScrubRpcServer::new(scrub_service));

        // Create gRPC shutdown token before spawning so it can be used
        // by both the gRPC server and BackgroundTasks.
        let grpc_shutdown = CancellationToken::new();
        let _grpc_shutdown_signal = grpc_shutdown.clone();

        let grpc_server_handle = tokio::spawn(async move {
            use std::os::unix::io::AsRawFd;

            use tokio_stream::StreamExt;

            let listener = match create_reuseport_listener(grpc_addr) {
                Ok(l) => l,
                Err(e) => {
                    error!("gRPC listener creation failed for {grpc_addr}: {e}");
                    return;
                }
            };

            let stream =
                tokio_stream::wrappers::TcpListenerStream::new(listener).map(move |conn| {
                    if let Ok(ref stream) = conn {
                        apply_opts_to_fd(stream.as_raw_fd(), quickack, busy_poll);
                    }
                    conn
                });

            if let Err(e) = grpc_router.serve_with_incoming(stream).await {
                error!("gRPC server error: {e}");
            }
        });

        // ---- 15b. Bind the membership plane (ADR-0028 D1) ----
        // A separate listener on membership_listen_addr hosting ONLY the
        // membership services: GossipRpc (push/pull) + ProbeRpc (SWIM).
        // Isolation from the data plane is the point — probe latency must
        // not inherit the data plane's tail (16 MiB streams, hint
        // batches). Bound BEFORE membership.start(): peers probe and
        // push to this listener immediately after the join announcement.
        let membership_router = tonic::transport::Server::builder()
            .add_service(oceanfs_network::GossipRpcServer::new(gossip_service))
            .add_service(oceanfs_network::gossip::probe_rpc_server::ProbeRpcServer::new(
                probe_service,
            ));

        let membership_listener = match create_reuseport_listener(membership_addr) {
            Ok(l) => l,
            Err(e) => {
                error!("membership plane listener creation failed for {membership_addr}: {e}");
                return Err(format!(
                    "membership plane listener creation failed for {membership_addr}: {e}"
                )
                .into());
            }
        };

        tokio::spawn(async move {
            // Same socket treatment as the data plane (perf 4.3):
            // quickack + busy-poll on accepted membership connections —
            // probe latency is the detection bound.
            use std::os::unix::io::AsRawFd;

            use tokio_stream::StreamExt;

            let stream = tokio_stream::wrappers::TcpListenerStream::new(membership_listener).map(
                move |conn| {
                    if let Ok(ref stream) = conn {
                        apply_opts_to_fd(stream.as_raw_fd(), quickack, busy_poll);
                    }
                    conn
                },
            );
            if let Err(e) = membership_router.serve_with_incoming(stream).await {
                error!("membership plane server error: {e}");
            }
        });

        // ---- 15c. Bootstrap membership: start failure detection +
        // gossip, then join the ring. MUST happen after the gRPC server
        // is bound: peers probe and deliver hinted handoffs to our gRPC
        // listener immediately after the join announcement, and a join
        // that precedes the bind produces join-time false Suspects and
        // refused hint deliveries (t5/t21).
        membership.start().map_err(|e| format!("failed to start membership: {e}"))?;
        // Register the gossip metrics AFTER start(): the gossip
        // protocol + its counters/histograms are created inside
        // start() — an earlier registration captured None and the
        // gossip series never appeared (the timing-metrics run
        // queried an empty metric).
        membership.register_membership_metrics(&*metrics_for_late_registration);
        let join_incarnation = Incarnation::new(announce_incarnation);
        let join_fallback_seeds = durable_state.fallback_seeds.clone();
        if let Err(e) = membership.join(join_incarnation, &join_fallback_seeds).await {
            // A transient seed outage at boot must not isolate the node:
            // with configured seeds the old behavior ABORTED the process
            // (and the unit is Restart=no, so the node stayed down); with
            // empty configured seeds (restart path) it started as a
            // singleton with no retry. Instead, warn and rejoin in the
            // background — the cluster-readiness gate keeps writes
            // refused until the ring converges.
            warn!(error = %e, "initial cluster join failed; retrying in the background");
        }

        // Background rejoin: retry the (idempotent) join every 3s until
        // the ring reaches 2 nodes. Covers the seedless-restart path
        // (fallback seeds) and fleet nodes that boot before their seed
        // comes up. Exits once joined.
        if is_cluster_node {
            let retry_membership = membership.clone();
            let retry_incarnation = join_incarnation;
            let retry_fallback = join_fallback_seeds.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
                loop {
                    interval.tick().await;
                    if retry_membership.ring().snapshot().node_count() >= 2 {
                        return;
                    }
                    if let Err(e) = retry_membership.join(retry_incarnation, &retry_fallback).await
                    {
                        tracing::debug!(error = %e, "rejoin retry failed");
                    }
                }
            });
        }

        // After a successful join, snapshot the known member addresses as
        // fallback seeds. Events emitted during join are missed by the
        // watcher spawned later (broadcast channels do not replay), so
        // this write also captures members learned from the seed pull.
        // Self is excluded: its own old address is useless after a
        // restart (t43).
        //
        // A seedless singleton join (no configured seeds, all fallback
        // seeds down at restart time) must NOT wipe the persisted list:
        // the snapshot would contain only self → `save_fallback_seeds([])`
        // — and every later restart would then have no seeds at all,
        // stranding the node forever (observed in the churn run: node-0
        // restarted at inc 2 with fallback_seeds=2, then inc 3/4/5 with
        // fallback_seeds=0 after the wipe). The persisted list is the
        // last-known truth; only a join that actually learned peers may
        // replace it.
        {
            let self_id = NodeId::new(&config.node_id);
            let seeds: Vec<String> = membership
                .nodes_full()
                .iter()
                .filter(|(id, _, _, _, _, _, _)| *id != self_id)
                .map(|(_, _, _, _, membership_addr, _, _)| membership_addr.to_string())
                .collect();
            if seeds.is_empty() {
                tracing::debug!(
                    node_id = %config.node_id,
                    "join learned no peers — keeping the persisted fallback seeds"
                );
            } else if let Err(e) = membership_state_store.save_fallback_seeds(&seeds) {
                warn!(error = %e, "failed to persist fallback seeds after join");
            }
        }

        // ---- 16. Spawn background tasks ----
        let mut background = Self::spawn_background_tasks(
            gc_worker,
            metadata_store.clone(),
            Arc::clone(&lifecycle_registry),
            ae_worker,
            scrub_worker,
            reaper,
            prefetch_engine,
            heal_worker,
            heal_data_store.clone(),
            hinted_handoff_manager.clone(),
            &config,
        );
        background.grpc_shutdown = grpc_shutdown;
        background.grpc_server = Some(grpc_server_handle);

        // ---- 17. Spawn hinted handoff delivery watcher ----
        // Watches for membership events and drains the handoff buffer
        // for nodes that are (or return to) ALIVE. Any Alive event —
        // including an Alive→Alive address update from a rejoin
        // (ADR-0022, t21) — triggers delivery: `deliver_pending` is a
        // no-op when nothing is buffered. On the same events it also
        // records the node's address in the persisted fallback-seed
        // list (ADR-0022 D3) — incrementally from the event itself, so
        // the write never races the membership manager's apply step.
        let hh = hinted_handoff_manager.clone();
        let seed_store = membership_state_store.clone();
        let self_node_id = NodeId::new(&config.node_id);
        let mut events = membership.subscribe();
        let delivery_token = background.delivery_cancel.clone();
        let mut sweep_interval = tokio::time::interval(std::time::Duration::from_secs(
            config.hint_delivery_sweep_sec.max(1),
        ));
        let delivery_handle = tokio::spawn(async move {
            // Bounded retry helper shared by the event path and the sweep
            // path. The returning node's gRPC listener may still be
            // binding when the Alive event lands; a failed batch is
            // re-enqueued, so retries are safe (duplicates are
            // overwritten on the receiving side).
            //
            // Drains the ENTIRE queue per invocation: each iteration
            // delivers one batch (bounded by max_batch_size and
            // max_batch_bytes). The old single-batch drain could not
            // keep up with a full outage's hint debt — with the stable
            // N-node topology every mutation during an outage becomes
            // debt, and 7000 hints at 256/batch would take ~27 sweeps
            // (~135s) to drain, longer than the test settle window.
            // The batch cap (64) bounds one invocation so a
            // persistently-rejected batch cannot spin forever.
            async fn drain_hints(hh: &HintedHandoffManager, node_id: NodeId) {
                let mut batches = 0u32;
                while hh.pending_count(&node_id) > 0 && batches < 64 {
                    batches += 1;
                    match hh.deliver_pending(node_id.clone()).await {
                        Ok(0) => {
                            // Queue empty (or nothing deliverable this
                            // round) — stop.
                            break;
                        }
                        Ok(delivered) => {
                            info!(
                                node = %node_id,
                                delivered,
                                batch = batches,
                                "hinted handoff delivery"
                            );
                        }
                        Err(e) => {
                            warn!(
                                node = %node_id,
                                attempt = batches,
                                error = %e,
                                "hinted handoff delivery failed"
                            );
                            // A failed batch is re-enqueued at the
                            // front; a few quick retries cover the
                            // returning node's listener still binding,
                            // then give up this sweep (the next sweep
                            // retries).
                            if batches < 5 {
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                continue;
                            }
                            break;
                        }
                    }
                }
            }

            loop {
                tokio::select! {
                    _ = delivery_token.cancelled() => {
                        info!("Hinted handoff delivery watcher cancelled");
                        break;
                    }
                    _ = sweep_interval.tick() => {
                        // Periodic delivery sweep. Event-driven delivery
                        // is missed when THIS node is down during the
                        // recipient's Alive event, or when the event
                        // lands before the recipient's gRPC listener is
                        // ready. The sweep re-resolves addresses at sweep
                        // time, so pending hints drain as soon as the
                        // recipient is actually reachable — delivery is
                        // eventually-convergent under churn.
                        for node_id in hh.nodes_with_pending() {
                            drain_hints(&hh, node_id).await;
                        }
                    }
                    event = events.recv() => {
                        match event {
                            Ok(ev) if ev.new_state == oceanfs_core::NodeState::Alive => {
                                info!(
                                    node = %ev.node_id,
                                    "node returned to cluster; delivering pending hinted handoffs"
                                );
                                // Only spend retries when hints are actually
                                // buffered.
                                if hh.pending_count(&ev.node_id) > 0 {
                                    drain_hints(&hh, ev.node_id.clone()).await;
                                }
                                // Record the member address as a fallback
                                // seed (ADR-0022 D3). Self is skipped: the
                                // node's own old address is useless after a
                                // restart (t43). The MEMBERSHIP PLANE
                                // address is recorded — the join dials
                                // fallback seeds for the gossip pull
                                // (ADR-0028 D1); the data-plane address in
                                // `ev.address` would dial a port that does
                                // not serve GossipRpc (observed: persisted
                                // data addresses made every rejoin pull
                                // fail with Unimplemented and strand the
                                // restarted bootstrap node). Detector
                                // recovery events carry
                                // membership_address=None and are skipped —
                                // the address was recorded when the node
                                // joined.
                                if ev.node_id != self_node_id {
                                    if let Some(addr) = ev.membership_address {
                                        if let Err(e) = seed_store
                                            .add_fallback_seed(&addr.to_string())
                                        {
                                            warn!(error = %e, "failed to persist fallback seed");
                                        }
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                warn!(skipped = n, "hinted handoff watcher lagged");
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                info!("Membership event channel closed; stopping delivery watcher");
                                break;
                            }
                        }
                    }
                }
            }
        });
        background.hinted_handoff_delivery = Some(delivery_handle);

        // Start periodic connection pool health checks (perf rule §4.1).
        // The health check loop runs until cancelled during shutdown.
        let health_cancel = background.health_check_cancel.clone();
        pool.start_health_check_loop(health_cancel);

        info!(
            node_id = %config.node_id,
            http_addr = %server_addr,
            grpc_addr = %grpc_addr,
            "OceanFS node started"
        );

        // ---- 18. Construct graceful leave handler ----
        let leave_handler = Arc::new(NodeLeaveHandler {
            wal_writer: wal_writer.clone(),
            segment_dir: segment_dir.clone(),
            pool: pool.clone(),
            membership: membership.clone(),
        });

        Ok(Node {
            config,
            accel,
            server_addr,
            grpc_addr,
            http_shutdown,
            grpc_shutdown: background.grpc_shutdown.clone(),
            background,
            leave_handler,
            membership,
            metadata_store,
            wal_writer: wal_writer.clone(),
        })
    }

    /// Returns the acceleration dispatcher probed at startup (ADR-0006).
    ///
    /// Consumers (encoders, decoders, hash accelerators) acquire the
    /// dispatcher to submit work to the best available hardware tier.
    pub fn accel(&self) -> &Arc<oceanfs_accel::AccelDispatcher> {
        &self.accel
    }

    /// Returns the bound HTTP server address.
    pub fn server_addr(&self) -> SocketAddr {
        self.server_addr
    }

    /// Returns the bound gRPC server address.
    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }

    /// Gracefully shuts down the node.
    ///
    /// Sequence: graceful leave → cancel gRPC → cancel HTTP → cancel background
    /// tasks → wait for tasks → flush WAL → close metadata → drop subsystems.
    ///
    /// # Errors
    ///
    /// Returns an error if any background task panicked or timed out.
    pub async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        info!(node_id = %self.config.node_id, "Shutting down OceanFS node");

        // ---- 1. Graceful leave: handoff WAL and segment shards to successor ----
        let leave_result = self.membership.leave(Some(self.leave_handler.as_ref())).await;
        if let Err(e) = leave_result {
            warn!(error = %e, "graceful leave handoff failed; continuing shutdown");
        }

        // ---- 2. Cancel gRPC server (stop accepting new RPCs, drain in-flight) ----
        self.grpc_shutdown.cancel();

        // ---- 3. Signal the HTTP server to stop accepting connections and drain ----
        self.http_shutdown.cancel();

        // ---- 4. Signal all background tasks to stop ----
        self.background.gossip_cancel.cancel();
        self.background.gc_cancel.cancel();
        self.background.ae_cancel.cancel();
        self.background.scrub_cancel.cancel();
        self.background.reaper_cancel.cancel();
        self.background.prefetch_cancel.cancel();
        self.background.fd_cancel.cancel();
        self.background.heal_cancel.cancel();
        self.background.delivery_cancel.cancel();
        self.background.hint_prune_cancel.cancel();
        self.background.health_check_cancel.cancel();

        // ---- 5. Wait for background tasks with a timeout ----
        let _ = tokio::time::timeout(Duration::from_secs(10), async {
            let _ = tokio::try_join!(
                async { self.background.gossip.await.map_err(|e| format!("{e}")) },
                async { self.background.gc.await.map_err(|e| format!("{e}")) },
                async { self.background.anti_entropy.await.map_err(|e| format!("{e}")) },
                async { self.background.scrub.await.map_err(|e| format!("{e}")) },
                async { self.background.orphan_reaper.await.map_err(|e| format!("{e}")) },
                async { self.background.failure_detector.await.map_err(|e| format!("{e}")) },
                async { self.background.heal.await.map_err(|e| format!("{e}")) },
                async { self.background.hinted_handoff_prune.await.map_err(|e| format!("{e}")) },
            );
        })
        .await;

        // Wait for prefetch handle separately (it may be None).
        if let Some(pf) = self.background.prefetch {
            let _ = tokio::time::timeout(Duration::from_secs(5), pf).await;
        }

        // Wait for hinted handoff delivery handle (may be None).
        if let Some(dh) = self.background.hinted_handoff_delivery {
            let _ = tokio::time::timeout(Duration::from_secs(5), dh).await;
        }

        // Wait for gRPC server handle (may be None).
        if let Some(grpc_handle) = self.background.grpc_server {
            let _ = tokio::time::timeout(Duration::from_secs(5), grpc_handle).await;
        }

        // ---- 6. Flush WAL writer to disk ----
        if let Err(e) = self.wal_writer.sync().await {
            warn!(error = %e, "WAL sync failed during shutdown");
        }

        // ---- 7. Close metadata store (flush RocksDB) ----
        if let Err(e) = self.metadata_store.close() {
            warn!(error = %e, "metadata store close failed during shutdown");
        }

        // ---- 8. Membership shutdown (cancels internal gossip + FD) ----
        self.membership.shutdown();

        info!(node_id = %self.config.node_id, "OceanFS node shut down");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Validates and normalizes node configuration.
    fn validate_config(config: NodeConfig) -> Result<NodeConfig, Box<dyn std::error::Error>> {
        let mut cfg = config;

        std::fs::create_dir_all(&cfg.data_dir)
            .map_err(|e| format!("cannot create data directory {:?}: {e}", cfg.data_dir))?;

        if cfg.node_id.is_empty() {
            cfg.node_id = "oceanfs-node".to_string();
        }

        // Validate auth config at startup rather than at first request time.
        // If S3 auth is enabled but the key file is missing or invalid,
        // log a warning but don't block startup — the system will reject
        // all authenticated requests until valid keys are provided.
        if cfg.s3_auth_enabled {
            let keys_path = cfg.data_dir.join("access_keys.toml");
            if keys_path.exists() {
                match std::fs::read_to_string(&keys_path) {
                    Ok(content) => {
                        if let Err(e) = toml::from_str::<toml::Value>(&content) {
                            warn!(
                                path = %keys_path.display(),
                                error = %e,
                                "access_keys.toml is invalid TOML — S3 auth will reject all requests"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            path = %keys_path.display(),
                            error = %e,
                            "cannot read access_keys.toml — S3 auth will reject all requests"
                        );
                    }
                }
            } else {
                warn!("s3_auth_enabled but no access_keys.toml found at {}", keys_path.display());
            }
        }

        Ok(cfg)
    }

    /// Spawns all background task loops.
    #[allow(clippy::too_many_arguments)]
    fn spawn_background_tasks(
        gc_worker: Arc<oceanfs_durability::GarbageCollector>,
        metadata_store: Arc<oceanfs_storage::RocksDbMetadataStore>,
        lifecycle_registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
        ae_worker: Arc<oceanfs_durability::AntiEntropy>,
        scrub_worker: Arc<oceanfs_durability::ScrubCoordinator>,
        reaper: Arc<oceanfs_durability::OrphanReaper>,
        prefetch_engine: Arc<oceanfs_cache::PrefetchEngine>,
        heal_worker: oceanfs_durability::HealWorker,
        data_store: Arc<dyn oceanfs_durability::SegmentDataStore>,
        hinted_handoff_manager: Arc<HintedHandoffManager>,
        config: &oceanfs_core::NodeConfig,
    ) -> BackgroundTasks {
        // The background tasks hold the registry across 'static spawns.
        let gc_registry = Arc::clone(&lifecycle_registry);
        let scrub_registry = Arc::clone(&lifecycle_registry);

        // Gossip: Membership drives the gossip protocol internally via
        // Membership::start(). This task holds a cancellation-aware standby
        // so the shutdown sequence can await it cleanly.
        let gossip_cancel = CancellationToken::new();
        let gossip_token = gossip_cancel.clone();
        let gossip = tokio::spawn(async move {
            gossip_token.cancelled().await;
            info!("Gossip task cancelled");
        });

        // GC: runs every gc_interval_sec from config.
        let gc_cancel = CancellationToken::new();
        let gc_token = gc_cancel.clone();
        let gc_store = metadata_store.clone();
        let gc_interval = Duration::from_secs(config.gc_interval_sec);
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let gc = tokio::spawn(async move {
            if io_idle {
                oceanfs_storage::io::apply_background_io_class("gc");
            }
            if cpu_idle {
                oceanfs_storage::io::apply_background_cpu_sched("gc");
            }
            let mut interval = tokio::time::interval(gc_interval);
            loop {
                tokio::select! {
                    _ = gc_token.cancelled() => {
                        info!("GC task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = gc_worker.run_cycle(gc_store.clone(), &gc_registry).await
                        {
                            warn!("GC cycle error: {e}");
                        }
                    }
                }
            }
        });

        // Anti-entropy: runs every ae_interval_sec from config.
        let ae_cancel = CancellationToken::new();
        let ae_token = ae_cancel.clone();
        let ae_interval_secs = config.ae_interval_sec;
        // Continuous mode exchanges Merkle ROOTS with peers via the
        // incremental tree — it never reads segment data, so per-cycle
        // cost is O(sealed segments) metadata calls instead of reading
        // every segment file (GBs per cycle on the phase-2 SUT, which
        // stalled cycles for 90s+ under load and spiked RSS). The full
        // cycle (reads all data + rebuilds trees) stays available for
        // `continuous_enabled = false`.
        let ae_continuous = config.anti_entropy.continuous_enabled;
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let ae = tokio::spawn(async move {
            if io_idle {
                oceanfs_storage::io::apply_background_io_class("anti-entropy");
            }
            if cpu_idle {
                oceanfs_storage::io::apply_background_cpu_sched("anti-entropy");
            }
            let mut interval = tokio::time::interval(Duration::from_secs(ae_interval_secs));
            loop {
                tokio::select! {
                    _ = ae_token.cancelled() => {
                        info!("Anti-entropy task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        let result = if ae_continuous {
                            ae_worker.run_continuous_cycle().await
                        } else {
                            ae_worker.run_cycle().await
                        };
                        if let Err(e) = result {
                            warn!("Anti-entropy cycle error: {e}");
                        }
                    }
                }
            }
        });

        // Scrub: runs every scrub_interval_sec from config.
        let scrub_cancel = CancellationToken::new();
        let scrub_token = scrub_cancel.clone();
        let _scrub_store = metadata_store.clone();
        let scrub_data = data_store;
        let scrub_interval_secs = config.scrub_interval_sec;
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let scrub = tokio::spawn(async move {
            if io_idle {
                oceanfs_storage::io::apply_background_io_class("scrub");
            }
            if cpu_idle {
                oceanfs_storage::io::apply_background_cpu_sched("scrub");
            }
            let mut interval = tokio::time::interval(Duration::from_secs(scrub_interval_secs));
            loop {
                tokio::select! {
                    _ = scrub_token.cancelled() => {
                        info!("Scrub task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        match scrub_worker
                            .run_cycle(Arc::clone(&scrub_registry), scrub_data.clone())
                            .await
                        {
                            Ok(report) => {
                                if report.segments_corrupt() > 0 {
                                    warn!(
                                        corrupt = report.segments_corrupt(),
                                        "scrub detected corrupt segments"
                                    );
                                }
                            }
                            Err(e) => warn!("Scrub cycle error: {e}"),
                        }
                    }
                }
            }
        });

        // Orphan reaper: runs every orphan_reaper_interval_sec from config.
        let reaper_cancel = CancellationToken::new();
        let reaper_token = reaper_cancel.clone();
        let reaper_interval = Duration::from_secs(config.orphan_reaper_interval_sec);
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let orphan_reaper = tokio::spawn(async move {
            if io_idle {
                oceanfs_storage::io::apply_background_io_class("orphan-reaper");
            }
            if cpu_idle {
                oceanfs_storage::io::apply_background_cpu_sched("orphan-reaper");
            }
            let mut interval = tokio::time::interval(reaper_interval);
            loop {
                tokio::select! {
                    _ = reaper_token.cancelled() => {
                        info!("Orphan reaper task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = reaper.run_cycle().await {
                            warn!("Orphan reaper cycle error: {e}");
                        }
                    }
                }
            }
        });

        // Prefetch background pre-warmer: PrefetchEngine runs its own internal
        // worker (spawned in PrefetchEngine::new()). This task holds the engine
        // Arc alive and waits for cancellation. When prefetch is disabled, the
        // engine silently drops all queued tasks.
        let prefetch_cancel = CancellationToken::new();
        let prefetch_token = prefetch_cancel.clone();
        let prefetch = Some(tokio::spawn(async move {
            // Hold the engine alive for the lifetime of this task.
            let _engine = prefetch_engine;
            prefetch_token.cancelled().await;
            info!("Prefetch task cancelled");
        }));

        // SWIM failure detector: Membership drives the failure detector
        // internally via Membership::start(). This task holds a cancellation-
        // aware standby so the shutdown sequence can await it cleanly.
        let fd_cancel = CancellationToken::new();
        let fd_token = fd_cancel.clone();
        let failure_detector = tokio::spawn(async move {
            fd_token.cancelled().await;
            info!("Failure detector task cancelled");
        });

        // EC Heal worker: drains the HealQueue and repairs corrupt shards.
        let heal_cancel = CancellationToken::new();
        let heal_token = heal_cancel.clone();
        let io_idle = config.background_io_class_idle;
        let cpu_idle = config.background_cpu_sched_idle;
        let heal = tokio::spawn(async move {
            if io_idle {
                oceanfs_storage::io::apply_background_io_class("heal");
            }
            if cpu_idle {
                oceanfs_storage::io::apply_background_cpu_sched("heal");
            }
            heal_worker.run(heal_token).await;
            info!("Heal worker task completed");
        });

        // Hinted handoff delivery watcher token — the watcher itself is
        // spawned after BackgroundTasks is constructed so we can store
        // the join handle retroactively.
        let delivery_cancel = CancellationToken::new();

        // Hinted handoff WAL periodic prune — removes expired entries
        // from all per-node WAL files to bound storage growth.
        let hint_prune_cancel = CancellationToken::new();
        let hint_prune_token = hint_prune_cancel.clone();
        let hint_ttl_secs = config.hint_ttl_sec;
        let hint_prune_interval = Duration::from_secs(config.hint_prune_interval_sec);
        let hinted_handoff_prune = tokio::spawn(async move {
            let mut interval = tokio::time::interval(hint_prune_interval);
            loop {
                tokio::select! {
                    _ = hint_prune_token.cancelled() => {
                        info!("Hinted handoff WAL prune task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        match hinted_handoff_manager.prune_all_expired(hint_ttl_secs).await {
                            Ok(0) => {}
                            Ok(n) => {
                                info!(pruned = n, "pruned expired hinted handoff entries from per-node WALs");
                            }
                            Err(e) => {
                                warn!(error = %e, "hinted handoff WAL prune cycle error");
                            }
                        }
                    }
                }
            }
        });

        BackgroundTasks {
            gossip,
            gossip_cancel,
            gc,
            gc_cancel,
            anti_entropy: ae,
            ae_cancel,
            scrub,
            scrub_cancel,
            orphan_reaper,
            reaper_cancel,
            prefetch,
            prefetch_cancel,
            failure_detector,
            fd_cancel,
            heal,
            heal_cancel,
            hinted_handoff_delivery: None,
            delivery_cancel,
            hinted_handoff_prune,
            hint_prune_cancel,
            grpc_server: None,
            grpc_shutdown: CancellationToken::new(),
            health_check_cancel: CancellationToken::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Process metrics helpers
// ---------------------------------------------------------------------------

/// Reads the resident memory size from `/proc/self/statm`.
///
/// Returns the resident set size in pages multiplied by the page size
/// (typically 4096), yielding total resident memory in bytes.
///
/// # Errors
///
/// Returns an error if `/proc/self/statm` cannot be read or parsed.
fn read_process_memory_bytes() -> Result<u64, std::io::Error> {
    let statm = std::fs::read_to_string("/proc/self/statm")?;
    // Format: size resident shared text lib data dt (in pages)
    let parts: Vec<&str> = statm.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected statm format",
        ));
    }
    let resident_pages: u64 =
        parts[1].parse().map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let page_size = 4096u64; // Linux default page size
    Ok(resident_pages * page_size)
}

/// Counts the number of open file descriptors from `/proc/self/fd`.
///
/// # Errors
///
/// Returns an error if the directory cannot be read.
fn read_process_open_fds() -> Result<u64, std::io::Error> {
    let entries = std::fs::read_dir("/proc/self/fd")?;
    Ok(entries.count() as u64)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{net::TcpStream, time::Duration};

    use oceanfs_storage_api::MetadataStore;
    use tempfile::TempDir;

    use super::*;

    fn test_config(tmp: &TempDir) -> NodeConfig {
        NodeConfig {
            data_dir: tmp.path().to_path_buf(),
            listen_addr: "127.0.0.1:0".into(),
            grpc_listen_addr: "127.0.0.1:0".into(),
            // Ephemeral membership plane port — the default 0.0.0.0:9002
            // conflicts across parallel test nodes (ADR-0028 D1).
            membership_listen_addr: "127.0.0.1:0".into(),
            // The event WAL lives under the temp data dir (the default
            // /var/lib/oceanfs/event-wal is not writable in tests).
            event_wal: oceanfs_core::EventWalConfig {
                event_wal_dir: tmp.path().join("event-wal"),
                ..Default::default()
            },
            ..NodeConfig::default()
        }
    }

    #[test]
    fn validate_config_creates_data_directory() {
        let tmp = TempDir::new().expect("tempdir");
        let config = test_config(&tmp);
        let validated = Node::validate_config(config).expect("validate");
        assert!(validated.data_dir.exists(), "data_dir should exist after validation");
    }

    #[test]
    fn validate_config_defaults_empty_node_id() {
        let tmp = TempDir::new().expect("tempdir");
        let mut config = test_config(&tmp);
        config.node_id = String::new();
        let validated = Node::validate_config(config).expect("validate");
        assert_eq!(validated.node_id, "oceanfs-node");
    }

    #[test]
    fn node_config_uses_segment_size_default() {
        let segment_size = SegmentSizeConfig::default();
        assert_eq!(segment_size.inline_threshold_bytes, 4096);
        assert_eq!(segment_size.small_threshold_bytes, 262144);
        assert_eq!(segment_size.default_target_size, 4 * 1024 * 1024);
    }

    #[tokio::test]
    async fn node_start_with_valid_config_succeeds() {
        let tmp = TempDir::new().expect("tempdir");
        let config = test_config(&tmp);
        let result = Node::start(config).await;
        assert!(
            result.is_ok(),
            "Node::start should succeed with valid config: {}",
            result.as_ref().err().map(|e| e.to_string()).unwrap_or_default()
        );
        // Clean shutdown.
        result.unwrap().shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn node_start_with_invalid_addr_errors() {
        let tmp = TempDir::new().expect("tempdir");
        let config_invalid = NodeConfig {
            listen_addr: "not-a-valid-socket-addr".into(),
            grpc_listen_addr: "127.0.0.1:0".into(),
            ..test_config(&tmp)
        };
        let result = Node::start(config_invalid).await;
        assert!(result.is_err(), "invalid listen_addr should error");
        let err_msg = result.err().unwrap().to_string();
        assert!(
            err_msg.contains("HTTP server") || err_msg.contains("bind"),
            "error should mention bind failure: {err_msg}"
        );
    }

    #[tokio::test]
    async fn node_shutdown_releases_ports() {
        let tmp = TempDir::new().expect("tempdir");
        let config = test_config(&tmp);
        let node = Node::start(config).await.expect("start");
        let http_addr = node.server_addr();
        let grpc_addr_val = node.grpc_addr();

        // Shut down.
        node.shutdown().await.expect("shutdown");

        // Give the OS time to release ports.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify HTTP port is released.
        let http_reconnect = TcpStream::connect_timeout(&http_addr, Duration::from_secs(1));
        assert!(http_reconnect.is_err(), "HTTP port {http_addr} should be released after shutdown");

        // Verify gRPC port is released.
        let grpc_reconnect = TcpStream::connect_timeout(&grpc_addr_val, Duration::from_secs(1));
        assert!(
            grpc_reconnect.is_err(),
            "gRPC port {grpc_addr_val} should be released after shutdown"
        );
    }

    #[tokio::test]
    async fn node_config_uses_segment_size_config() {
        let tmp = TempDir::new().expect("tempdir");
        let config = test_config(&tmp);
        let node = Node::start(config).await.expect("start");
        // Verify the acceleration dispatcher is available and probed (ADR-0006).
        let _accel = node.accel();
        // Verify the bound HTTP address is non-zero.
        assert!(node.server_addr().port() > 0, "HTTP server port should be non-zero");
        node.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn background_tasks_spawns_all_handles() {
        let tmp = TempDir::new().expect("tempdir");
        let metadata_config =
            MetadataConfig { data_dir: tmp.path().join("metadata"), ..Default::default() };
        let metadata_store = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&metadata_config)
                .expect("open metadata store"),
        );
        let gc_config = oceanfs_durability::GcConfig::default();
        let gc_worker = Arc::new(oceanfs_durability::GarbageCollector::new(gc_config.clone()));

        // Wire minimal membership and connection pool for AntiEntropy construction
        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(oceanfs_membership::Membership::new(
            oceanfs_core::NodeId::new("test-node"),
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        let pool =
            Arc::new(oceanfs_network::ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let lifecycle_registry =
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            ));
        let ae_worker = Arc::new(oceanfs_durability::AntiEntropy::new(
            oceanfs_durability::AntiEntropyConfig::default(),
            membership,
            Arc::clone(&lifecycle_registry),
            pool,
            Arc::new(oceanfs_durability::InMemorySegmentStore::new()),
            Arc::new(oceanfs_durability::merkle::IncrementalMerkleTree::new(
                oceanfs_durability::merkle::MerkleTreeConfig::default(),
            )),
        ));
        let scrub_config = oceanfs_durability::ScrubConfig::default();
        let scrub_worker = Arc::new(oceanfs_durability::ScrubCoordinator::new(scrub_config));
        let reaper_shard_store: Arc<dyn oceanfs_durability::SegmentShardStore> =
            Arc::new(oceanfs_durability::InMemorySegmentShardStore::new(4194304));
        let lifecycle = Arc::new(oceanfs_storage::SegmentLifecycleCoordinator::with_registry(
            Arc::clone(&lifecycle_registry),
        ));
        let reaper = Arc::new(oceanfs_durability::OrphanReaper::new(
            metadata_store.clone(),
            lifecycle,
            reaper_shard_store.clone(),
            gc_config,
        ));

        let prefetch_config = oceanfs_cache::PrefetchConfig::default();
        let prefetch_store: Arc<dyn oceanfs_storage_api::MetadataStore> =
            Arc::new(PrefetchStoreAdapter { store: metadata_store.clone() });
        let _metadata_cache = Arc::new(oceanfs_cache::MetadataCache::new(
            oceanfs_cache::MetadataCacheConfig::default(),
            Box::new(oceanfs_cache::eviction::TtlLruPolicy::new(
                oceanfs_cache::eviction::TtlLruConfig::default(),
            )),
        ));
        let prefetch_engine = Arc::new(oceanfs_cache::PrefetchEngine::new(
            prefetch_config,
            _metadata_cache,
            None,
            prefetch_store,
        ));

        // Create minimal heal worker for testing
        let heal_config = oceanfs_durability::HealConfig::default();
        let heal_queue = Arc::new(oceanfs_durability::HealQueue::new(heal_config.queue_capacity()));
        let heal_decoder: Arc<dyn oceanfs_ec::Decoder> =
            Arc::new(oceanfs_ec::CauchyEncoder::new(oceanfs_core::CodecConfig::default()));
        let bg_data_store: Arc<dyn oceanfs_durability::SegmentDataStore> =
            Arc::new(oceanfs_durability::InMemorySegmentStore::new());
        let heal_data_store: Arc<dyn oceanfs_durability::SegmentDataStore> =
            Arc::new(oceanfs_durability::InMemorySegmentStore::new());
        let heal_lifecycle = Arc::new(oceanfs_storage::SegmentLifecycleCoordinator::new(
            &oceanfs_core::LifecycleConfig::default(),
        ));
        let heal_worker = oceanfs_durability::HealWorker::new(
            heal_config,
            heal_queue,
            heal_decoder,
            heal_lifecycle,
            heal_data_store,
        );

        let hints_dir = tmp.path().join("bg_hints");
        let hint_delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> =
            Arc::new(GrpcHintDeliveryClient::new(Arc::new(oceanfs_network::ConnectionPool::new(
                oceanfs_core::RpcConfig::default(),
            ))));
        let hint_config =
            HintedHandoffConfig { wal_dir: hints_dir.clone(), ..HintedHandoffConfig::default() };
        let hinted_handoff_manager =
            Arc::new(HintedHandoffManager::new(hints_dir, hint_delivery_client, hint_config));

        let bg = Node::spawn_background_tasks(
            gc_worker,
            metadata_store.clone(),
            Arc::clone(&lifecycle_registry),
            ae_worker,
            scrub_worker,
            reaper,
            prefetch_engine,
            heal_worker,
            bg_data_store,
            hinted_handoff_manager,
            &NodeConfig::default(),
        );

        // Verify handles are not finished immediately (they are pending).
        assert!(!bg.gossip.is_finished());
        assert!(!bg.gc.is_finished());
        assert!(!bg.anti_entropy.is_finished());
        assert!(!bg.scrub.is_finished());
        assert!(!bg.orphan_reaper.is_finished());
        assert!(bg.prefetch.is_some());
        assert!(!bg.failure_detector.is_finished());
        assert!(!bg.heal.is_finished());
        assert!(!bg.hinted_handoff_prune.is_finished());

        // Cancel all and wait.
        bg.gossip_cancel.cancel();
        bg.gc_cancel.cancel();
        bg.ae_cancel.cancel();
        bg.scrub_cancel.cancel();
        bg.reaper_cancel.cancel();
        bg.prefetch_cancel.cancel();
        bg.fd_cancel.cancel();
        bg.heal_cancel.cancel();
        bg.hint_prune_cancel.cancel();
    }
    #[test]
    fn prefetch_store_adapter_get_object_metadata_nonexistent() {
        let tmp = TempDir::new().expect("tempdir");
        let metadata_config =
            MetadataConfig { data_dir: tmp.path().join("metadata"), ..Default::default() };
        let store = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&metadata_config)
                .expect("open metadata store"),
        );
        let adapter = PrefetchStoreAdapter { store };
        let bucket = BucketId::new("test-bucket");
        let key = ObjectKey::new("nonexistent-key");
        let result = adapter.get_object_metadata(&bucket, &key).expect("get_object_metadata");
        assert!(result.is_none(), "nonexistent key should return None");
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn process_memory_bytes_returns_non_zero() {
        let mem = super::read_process_memory_bytes().expect("read memory");
        assert!(mem > 0, "resident memory should be > 0");
    }

    #[test]
    fn process_open_fds_returns_non_zero() {
        let fds = super::read_process_open_fds().expect("read fds");
        assert!(fds > 0, "open fds should be > 0");
    }

    // --- property_as_u64 tests ---

    #[test]
    fn property_as_u64_parses_rocksdb_integer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
            data_dir: dir.path().join("meta"),
            block_cache_size: 1024,
            memtable_size: 1024,
            ..Default::default()
        })
        .expect("open metadata store");

        // Estimate number of keys should parse to a valid u64.
        let val = super::property_as_u64(&store, "rocksdb.estimate-num-keys");
        assert!(val.is_some(), "estimate-num-keys should return a value");
        // New store should have few keys.
        assert!(val.unwrap() <= 10_000);
    }

    #[test]
    fn property_as_u64_unknown_property_returns_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
            data_dir: dir.path().join("meta"),
            block_cache_size: 1024,
            memtable_size: 1024,
            ..Default::default()
        })
        .expect("open metadata store");

        assert_eq!(super::property_as_u64(&store, "rocksdb.nonexistent"), None);
    }

    // ── Hinted Handoff Watcher (4.4) ──────────────────────────────

    /// Verifies that the membership event watcher correctly processes
    /// ALIVE events and calls `deliver_pending` on the hinted handoff.
    ///
    /// Uses an mpsc channel to confirm the watcher saw the event and
    /// attempted delivery.
    #[tokio::test]
    async fn hinted_handoff_watcher_delivers_on_alive_event() {
        use std::{net::SocketAddr, sync::Arc, time::Duration};

        use oceanfs_core::{Hlc, Incarnation, NodeId, NodeState, SegmentId};
        use oceanfs_durability::{HintRecord, HintedHandoff};
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;
        use tokio::sync::{broadcast, mpsc};
        use tokio_util::sync::CancellationToken;

        let addr: SocketAddr = "127.0.0.1:9100".parse().unwrap();
        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("watcher-test"),
            addr,
            addr,
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
        let handoff =
            Arc::new(HintedHandoff::new_with_pool(pool).with_membership(membership.clone()));

        // Store a hint for a returning node.
        let target = NodeId::new("returning-node");

        // Register the node as SUSPECT first so transitioning to ALIVE
        // triggers a state-change event.
        membership.upsert_node(
            target.clone(),
            NodeState::Suspect,
            Incarnation::new(1),
            Some("127.0.0.1:9200".parse().unwrap()),
        );

        let hint = HintRecord {
            intended_for: target.clone(),
            segment_id: SegmentId::new(),
            offset: 0,
            length: 42,
            timestamp: Hlc::zero(),
            data: vec![1, 2, 3].into(),
            stored_at_secs: 0,
        };
        handoff.handoff(target.clone(), hint).await.unwrap();
        assert_eq!(handoff.pending_count(&target), 1);

        // Channel to verify the watcher processes events.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let hh = handoff.clone();
        let token = cancel.clone();

        // Spawn the watcher — same logic as production.
        let mut events = membership.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    ev = events.recv() => {
                        match ev {
                            Ok(ev) if ev.new_state == NodeState::Alive &&
                                      ev.old_state != NodeState::Alive => {
                                let _ = tx.send(ev.node_id.clone());
                                let _ = hh.deliver_pending(ev.node_id).await;
                            }
                            Ok(_) => {}
                            Err(broadcast::error::RecvError::Closed) => break,
                            Err(broadcast::error::RecvError::Lagged(_)) => {}
                        }
                    }
                }
            }
        });

        // Mark the target node as returning to the cluster.
        membership.upsert_node(
            target.clone(),
            NodeState::Alive,
            Incarnation::new(2),
            Some("127.0.0.1:9200".parse().unwrap()),
        );

        // Wait for the watcher to process the event (with timeout).
        let received = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(received.is_ok(), "watcher should process ALIVE event within 2 seconds");
        assert_eq!(received.unwrap(), Some(target.clone()));

        // Delivery fails without a real gRPC server, but the watcher
        // should have attempted it (hints remain pending).
        assert_eq!(
            handoff.pending_count(&target),
            1,
            "hints retained after failed delivery attempt"
        );

        cancel.cancel();
    }

    /// Verifies the watcher ignores non-ALIVE state changes.
    #[tokio::test]
    async fn hinted_handoff_watcher_ignores_non_alive_events() {
        use std::{net::SocketAddr, sync::Arc};

        use oceanfs_core::{Incarnation, NodeId, NodeState};
        use oceanfs_membership::Membership;
        use tokio::sync::mpsc;
        use tokio_util::sync::CancellationToken;

        let addr: SocketAddr = "127.0.0.1:9100".parse().unwrap();
        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("ignorer"),
            addr,
            addr,
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        // hinted handoff not needed for this test — we verify watcher
        // event discrimination via the mpsc channel alone.

        let target = NodeId::new("suspect-node");
        // Pre-register the node in membership so state changes are detected.
        membership.upsert_node(
            target.clone(),
            NodeState::Alive,
            Incarnation::new(1),
            Some("127.0.0.1:9300".parse().unwrap()),
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let mut events = membership.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    ev = events.recv() => {
                        match ev {
                            Ok(ev) if ev.new_state == NodeState::Alive &&
                                      ev.old_state != NodeState::Alive => {
                                let _ = tx.send(ev.node_id.clone());
                            }
                            Ok(_) => {
                                // Non-ALIVE or same-state: send a dummy to confirm
                                // we saw it but didn't trigger delivery.
                                let _ = tx.send(NodeId::new("non-alive-seen"));
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        // Transition node to SUSPECT (not ALIVE).
        membership.upsert_node(
            target.clone(),
            NodeState::Suspect,
            Incarnation::new(2),
            Some("127.0.0.1:9300".parse().unwrap()),
        );

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv()).await;
        assert!(received.is_ok(), "watcher should process SUSPECT event");
        assert_eq!(
            received.unwrap(),
            Some(NodeId::new("non-alive-seen")),
            "non-ALIVE event should not trigger delivery"
        );

        cancel.cancel();
    }

    // ── Graceful Leave Handler (4.5) ──────────────────────────────

    /// Verifies that the `NodeLeaveHandler` correctly implements
    /// `GracefulLeaveHandler::handoff_wal_to()` by flushing the WAL.
    #[tokio::test]
    #[ignore = "requires running gRPC server"]
    async fn leave_handler_handoff_wal_flushes_and_reports_success() {
        use std::sync::Arc;

        use oceanfs_core::{NodeId, WalConfig};
        use oceanfs_membership::{GracefulLeaveHandler, Membership};
        use oceanfs_network::ConnectionPool;

        // Setup: real WAL in temp dir.
        let dir = tempfile::tempdir().unwrap();
        let wal_writer = Arc::new(
            oceanfs_storage::WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("leave-test"),
            "127.0.0.1:9100".parse().unwrap(),
            "127.0.0.1:9100".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let handler = super::NodeLeaveHandler {
            wal_writer,
            segment_dir: dir.path().join("segments"),
            pool,
            membership,
        };

        // handoff_wal_to should sync and succeed even without a real successor.
        let result =
            GracefulLeaveHandler::handoff_wal_to(&handler, &NodeId::new("successor")).await;
        assert!(result.is_ok(), "WAL handoff should succeed");
    }

    /// Verifies that `transfer_segment_shards_to` handles an empty blob store.
    #[tokio::test]
    #[ignore = "requires running gRPC server"]
    async fn leave_handler_transfer_empty_blob_store_returns_zero() {
        use std::sync::Arc;

        use oceanfs_core::NodeId;
        use oceanfs_membership::{GracefulLeaveHandler, Membership};
        use oceanfs_network::ConnectionPool;

        let dir = tempfile::tempdir().unwrap();
        let wal_writer = Arc::new(
            oceanfs_storage::WalWriter::open(&oceanfs_core::WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("empty-blob"),
            "127.0.0.1:9200".parse().unwrap(),
            "127.0.0.1:9200".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let handler = super::NodeLeaveHandler {
            wal_writer,
            segment_dir: dir.path().join("segments"),
            pool,
            membership,
        };

        let transferred =
            GracefulLeaveHandler::transfer_segment_shards_to(&handler, &NodeId::new("successor"))
                .await
                .unwrap();
        assert_eq!(transferred, 0, "empty blob store transfers 0 segments");
    }

    /// Verifies that `transfer_segment_shards_to` enumerates segments
    /// and handles gRPC failure gracefully (all transfers fail without
    /// a server running → 0 transferred).
    #[tokio::test]
    async fn leave_handler_transfer_segments_handles_grpc_failure() {
        use std::sync::Arc;

        use oceanfs_core::{NodeId, SegmentId};
        use oceanfs_membership::{GracefulLeaveHandler, Membership};
        use oceanfs_network::ConnectionPool;

        let dir = tempfile::tempdir().unwrap();
        let wal_writer = Arc::new(
            oceanfs_storage::WalWriter::open(&oceanfs_core::WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        // Write some segments to the segments directory.
        let seg_dir = dir.path().join("segments");
        std::fs::create_dir_all(&seg_dir).unwrap();
        for i in 0..3 {
            let id = SegmentId::new();
            let mut data = vec![0u8; 76]; // header
            data.extend_from_slice(&[i as u8; 64]);
            std::fs::write(seg_dir.join(format!("{id}.dat")), &data).unwrap();
        }

        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let addr: std::net::SocketAddr = "127.0.0.1:9300".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new("blob-test"),
            addr,
            addr,
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        // Register the successor in membership so address resolution works.
        membership.upsert_node(
            NodeId::new("successor"),
            oceanfs_core::NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            Some(addr),
        );
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let handler = super::NodeLeaveHandler {
            wal_writer,
            segment_dir: dir.path().join("segments"),
            pool,
            membership,
        };

        // Transfer: gRPC to successor will fail (no server), but we verify
        // the enumeration happened and attempts were made.
        let transferred =
            GracefulLeaveHandler::transfer_segment_shards_to(&handler, &NodeId::new("successor"))
                .await
                .unwrap();
        // All transfers fail because no gRPC server — count is 0.
        assert_eq!(transferred, 0, "transfers fail without gRPC server");
    }

    /// Verifies that `Membership::leave()` with a handler calls the handler
    /// instead of sleeping. Uses a mock handler that records calls.
    #[tokio::test]
    async fn membership_leave_calls_handler_instead_of_sleeping() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };

        use oceanfs_core::{NodeId, RingConfig};
        use oceanfs_membership::Membership;
        use oceanfs_routing::Ring;

        struct RecordingHandler {
            wal_called: AtomicBool,
            shard_called: AtomicBool,
        }

        #[async_trait::async_trait]
        impl oceanfs_membership::GracefulLeaveHandler for RecordingHandler {
            async fn handoff_wal_to(&self, _: &NodeId) -> oceanfs_core::Result<()> {
                self.wal_called.store(true, Ordering::SeqCst);
                Ok(())
            }
            async fn transfer_segment_shards_to(&self, _: &NodeId) -> oceanfs_core::Result<usize> {
                self.shard_called.store(true, Ordering::SeqCst);
                Ok(42)
            }
        }

        let ring = Ring::new(RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("leave-call-test"),
            "127.0.0.1:9400".parse().unwrap(),
            "127.0.0.1:9400".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache.clone(),
        ));

        // Add a successor node so handoff targets a real node.
        let mut write_ring = Ring::new(RingConfig::default());
        write_ring.add_node(NodeId::new("successor"));
        ring_cache.update(write_ring);
        // Register successor in membership for address resolution.
        membership.upsert_node(
            NodeId::new("successor"),
            oceanfs_core::NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            Some("127.0.0.1:9500".parse().unwrap()),
        );

        let handler = RecordingHandler {
            wal_called: AtomicBool::new(false),
            shard_called: AtomicBool::new(false),
        };

        // leave() requires started membership; start background tasks.
        membership.start().unwrap();
        membership.join(oceanfs_core::Incarnation::new(1), &[]).await.unwrap();

        let result = membership.leave(Some(&handler)).await;
        assert!(result.is_ok(), "leave should succeed");

        assert!(handler.wal_called.load(Ordering::SeqCst), "WAL handoff was called");
        assert!(handler.shard_called.load(Ordering::SeqCst), "shard transfer was called");
    }

    // ── gRPC segment transfer integration test ────────────────────

    /// Verifies that `transfer_segment_shards_to` successfully pushes
    /// segment data to a real gRPC healing service and the recipient
    /// stores the received hints.
    #[tokio::test]
    async fn leave_handler_transfer_via_grpc_received_by_successor() {
        use std::sync::Arc;

        use oceanfs_core::{Incarnation, NodeId, NodeState, SegmentId};
        use oceanfs_durability::{
            anti_entropy::InMemorySegmentStore, healing_service::HealingGrpcService,
            HealingRpcServer, HintedHandoff, SegmentDataStore,
        };
        use oceanfs_membership::{GracefulLeaveHandler, Membership};
        use oceanfs_network::ConnectionPool;
        use tonic::transport::Server;

        // ---- Setup server (successor) ----
        let server_handoff = Arc::new(HintedHandoff::new());
        let server_store: Arc<dyn SegmentDataStore> = Arc::new(InMemorySegmentStore::new());
        let server_meta = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&oceanfs_core::MetadataConfig {
                data_dir: {
                    let d = tempfile::tempdir().unwrap();
                    let p = d.path().to_path_buf();
                    // Keep tempdir alive
                    std::mem::forget(d);
                    p.join("meta")
                },
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        );
        // Use a fixed port for the test gRPC server.
        let bound_addr: std::net::SocketAddr = "127.0.0.1:15550".parse().unwrap();
        let healing_svc = HealingGrpcService::new(
            server_handoff.clone(),
            server_meta.clone(),
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &oceanfs_core::LifecycleConfig::default(),
            )),
            server_store,
            Arc::new(HlcClock::new()),
        );

        let server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(HealingRpcServer::new(healing_svc))
                .serve(bound_addr)
                .await
                .unwrap();
        });

        // Give the server a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // ---- Setup client (leaving node) ----
        let dir = tempfile::tempdir().unwrap();
        let wal_writer = Arc::new(
            oceanfs_storage::WalWriter::open(&oceanfs_core::WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        // Write test segments to the segments directory.
        let seg_dir = dir.path().join("segments");
        std::fs::create_dir_all(&seg_dir).unwrap();
        let seg_a = SegmentId::new();
        let seg_b = SegmentId::new();
        let mut data_a = vec![0u8; 76];
        data_a.extend_from_slice(b"segment A data for graceful leave");
        std::fs::write(seg_dir.join(format!("{seg_a}.dat")), &data_a).unwrap();
        let mut data_b = vec![0u8; 76];
        data_b.extend_from_slice(b"segment B data for graceful leave");
        std::fs::write(seg_dir.join(format!("{seg_b}.dat")), &data_b).unwrap();

        // Build ring with successor node.
        let mut ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        ring.add_node(NodeId::new("leaver"));
        ring.add_node(NodeId::new("successor"));
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));

        let membership = Arc::new(Membership::new(
            NodeId::new("leaver"),
            "127.0.0.1:9999".parse().unwrap(),
            "127.0.0.1:9999".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache.clone(),
        ));
        // Register successor with the actual bound address for gRPC.
        membership.upsert_node(
            NodeId::new("successor"),
            NodeState::Alive,
            Incarnation::new(1),
            Some(bound_addr),
        );
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let handler = super::NodeLeaveHandler {
            wal_writer: wal_writer.clone(),
            segment_dir: dir.path().join("segments"),
            pool,
            membership,
        };

        // ---- Execute transfer ----
        let transferred =
            GracefulLeaveHandler::transfer_segment_shards_to(&handler, &NodeId::new("successor"))
                .await
                .unwrap();

        assert_eq!(transferred, 2, "both segments should transfer successfully via gRPC");

        // ---- Verify successor received the hints ----
        let pending = server_handoff.pending_count(&NodeId::new("successor"));
        assert_eq!(pending, 2, "successor should have 2 pending hints");
        assert_eq!(server_handoff.total_pending_count(), 2, "total hints should match");

        // Drop server and clean up.
        server_task.abort();
    }
}
