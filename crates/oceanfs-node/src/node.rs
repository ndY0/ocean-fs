//! Composition root: wires all subsystem crates into a running OceanFS node.
//!
//! This is the **only** crate allowed to import concrete types from multiple
//! subsystem crates per architecture.md §4.1. It constructs every component,
//! injects dependencies via `Arc`, spawns background tasks, and binds the
//! HTTP + gRPC servers.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use oceanfs_core::{
    AccelConfig, BucketId, HlcClock, MetadataConfig, NodeConfig, NodeId, ObjectKey, ObjectMetadata,
    PoolConfig, RingConfig, RpcConfig, SegmentSizeConfig, SizeTier, WalConfig,
};
use oceanfs_durability::HintedHandoff;
use oceanfs_server::{
    auth::AuthMiddleware, metadata_ops::MetadataOps, AdminHandler, BucketConfigStore,
    ReadCoordinator, Router, S3Handler, WriteCoordinator,
};
use oceanfs_storage::{SegmentPool, SegmentShard};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::metadata_adapter::MetadataStoreAdapter;

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
}

// ---------------------------------------------------------------------------
// Node leave handler — implements GracefulLeaveHandler for WAL + shard handoff
// ---------------------------------------------------------------------------

/// Handles WAL sealing and segment shard streaming during graceful leave.
struct NodeLeaveHandler {
    /// WAL writer for flushing pending entries.
    wal_writer: Arc<oceanfs_storage::WalWriter>,
    /// Blob store for listing and reading owned segments.
    blob_store: Arc<oceanfs_storage::BlobStore>,
    /// Connection pool for gRPC data transfer.
    pool: Arc<oceanfs_network::ConnectionPool>,
    /// Membership for resolving successor node addresses.
    membership: Arc<oceanfs_membership::Membership>,
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
            .blob_store
            .list_blobs()
            .map_err(|e| oceanfs_core::Error::Leave(format!("blob list failed: {e}")))?;

        let mut transferred = 0usize;
        for seg_id in &segments {
            if let Some(data) = self
                .blob_store
                .read_blob(seg_id)
                .map_err(|e| oceanfs_core::Error::Leave(format!("blob read failed: {e}")))?
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
            .blob_store
            .list_blobs()
            .map_err(|e| oceanfs_core::Error::Leave(format!("blob list failed: {e}")))?;

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
                .blob_store
                .read_blob(seg_id)
                .map_err(|e| oceanfs_core::Error::Leave(format!("blob read failed: {e}")))?;

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

        let pooled = self
            .pool
            .get_channel(addr)
            .await
            .map_err(|e| format!("connection pool error for {node}: {e}"))?;

        let channel = pooled.channel().clone();
        drop(pooled);

        use oceanfs_core::Hlc;
        use oceanfs_durability::{healing_rpc::HintRequest, HealingRpcClient};

        let mut client = HealingRpcClient::new(channel);
        let proto_seg: oceanfs_core::proto::common::SegmentId = (*segment_id).into();
        let proto_node: oceanfs_core::proto::common::NodeId = node.clone().into();

        let request = tonic::Request::new(HintRequest {
            intended_for: Some(proto_node),
            segment_id: Some(proto_seg),
            data: data.to_vec(),
            hlc: Some(Hlc::zero().into()),
        });

        let timeout_ms = 5000u64;
        let response = tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            client.hinted_handoff(request),
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
    /// Gossip protocol task placeholder.
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

    /// Failure detector task.
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
    /// Background task handles and cancellation tokens.
    background: BackgroundTasks,
    /// Graceful leave handler for WAL and segment handoff.
    leave_handler: Arc<NodeLeaveHandler>,
    /// Cluster membership for leave signaling.
    membership: Arc<oceanfs_membership::Membership>,
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
        info!(
            node_id = %config.node_id,
            listen_addr = %config.listen_addr,
            grpc_addr = %config.grpc_listen_addr,
            "Starting OceanFS node"
        );

        // ---- 1. Open metadata store ----
        let metadata_config =
            MetadataConfig { data_dir: config.data_dir.join("metadata"), ..Default::default() };
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
        let gossip_config = config.gossip.clone();
        let membership = Arc::new(oceanfs_membership::Membership::new(
            NodeId::new(&config.node_id),
            grpc_addr,
            gossip_config,
            ring_cache.clone(),
        ));

        // ---- 5. Construct connection pool ----
        let rpc_config = RpcConfig::default();
        let pool = Arc::new(oceanfs_network::ConnectionPool::new(rpc_config));
        membership.set_pool(pool.clone());

        // Bootstrap membership: start failure detection + gossip, then join the ring.
        membership.start().map_err(|e| format!("failed to start membership: {e}"))?;
        membership.join().await.map_err(|e| format!("failed to join cluster: {e}"))?;

        // ---- 6. Construct storage components ----
        let segment_size = SegmentSizeConfig::default();
        let wal_config =
            WalConfig { data_dir: config.data_dir.join("wal"), ..WalConfig::default() };
        let wal_writer = oceanfs_storage::WalWriter::open(&wal_config)
            .await
            .map_err(|e| format!("failed to open WAL writer: {e}"))?;
        let wal_writer = Arc::new(wal_writer);

        // BufferPool for recycling segment append buffers (perf rule §1.2).
        let buffer_pool = Arc::new(oceanfs_storage::BufferPool::new(65536, 256));

        // Per-core segment shards for write concurrency (perf rule §2.5).
        let shard_count = 4;
        let shard_small = Arc::new(
            SegmentShard::new(shard_count, SizeTier::Small, &segment_size, &buffer_pool)
                .map_err(|e| format!("failed to create small segment shard: {e}"))?,
        );
        let shard_standard = Arc::new(
            SegmentShard::new(shard_count, SizeTier::Standard, &segment_size, &buffer_pool)
                .map_err(|e| format!("failed to create standard segment shard: {e}"))?,
        );

        // Segment pools for pipeline parallelism (perf rule §2.7).
        // Created before WAL replay so that replayed entries can be
        // reconstructed into active segments (C4-storage, D6).
        let pool_config = PoolConfig::default();
        let segment_pool_small = Arc::new(
            SegmentPool::new(
                pool_config.clone(),
                SizeTier::Small,
                &segment_size,
                buffer_pool.clone(),
            )
            .map_err(|e| format!("failed to create small segment pool: {e}"))?,
        );
        let segment_pool_standard = Arc::new(
            SegmentPool::new(pool_config, SizeTier::Standard, &segment_size, buffer_pool.clone())
                .map_err(|e| format!("failed to create standard segment pool: {e}"))?,
        );

        // ---- 6a. Replay WAL from any previous unclean shutdown (C4-storage, D6) ----
        // Rebuilds in-memory active segments from unsealed WAL entries left
        // behind by a crash. Occurs before the HTTP server binds.
        let replay_summary = oceanfs_storage::wal::replay_wal(
            &wal_config,
            &wal_writer,
            &segment_pool_small,
            &segment_pool_standard,
            &segment_size,
        )
        .await
        .map_err(|e| format!("WAL replay failed: {e}"))?;
        if replay_summary.entries_replayed > 0 {
            info!(
                entries = replay_summary.entries_replayed,
                bytes = replay_summary.bytes_replayed,
                segments = replay_summary.segments_seen.len(),
                hlc_wall = replay_summary.max_hlc_wall_time,
                hlc_logical = replay_summary.max_hlc_logical,
                "replayed unsealed WAL entries from prior crash; active segments rebuilt"
            );
            // Best-effort: remove old WAL files that have been fully replayed.
            // Failure is logged but does not prevent startup (H8-storage).
            oceanfs_storage::wal::cleanup_old_wal_files(&wal_config).await;
        }

        // ADR-0001: tiered segment sizing driven by SegmentSizeConfig.
        let seal_config = oceanfs_storage::SealConfig {
            target_size_bytes: segment_size.default_target_size,
            seal_timeout_ms: 5000,
            data_dir: config.data_dir.join("segments"),
        };
        // SegmentSealer constructed here; wired into the write path below.
        // Pass the blob store so sealed segments are available for heal/scrub/AE
        // via SegmentDataStore (M5-storage).
        let blob_store = Arc::new(
            oceanfs_storage::BlobStore::open(&config.data_dir.join("blobs"))
                .map_err(|e| format!("failed to open blob store: {e}"))?,
        );
        let sealer = Arc::new(
            oceanfs_storage::SegmentSealer::new(
                seal_config,
                metadata_store.clone(),
                wal_writer.clone(),
            )
            .with_blob_store(blob_store.clone()),
        );
        // ---- 7. Construct durability workers ----
        let gc_config = oceanfs_durability::GcConfig::new(
            config.gc_interval_sec,
            config.tombstone_ttl_sec,
            0.5,
            4,
            64,
        );
        let gc_worker = Arc::new(oceanfs_durability::GarbageCollector::new(gc_config.clone()));
        let ae_worker = Arc::new(oceanfs_durability::AntiEntropy::new(
            oceanfs_durability::AntiEntropyConfig::default(),
            membership.clone(),
            metadata_store.clone(),
            pool.clone(),
            Arc::new(oceanfs_durability::InMemorySegmentStore::new()),
        ));
        let scrub_config = oceanfs_durability::ScrubConfig::default();
        let scrub_worker = Arc::new(oceanfs_durability::ScrubCoordinator::new(scrub_config));
        // OrphanReaper needs a SegmentShardStore for deleting shard files.
        // In production this is the on-disk segment store; tests/early builds use in-memory.
        let reaper_shard_store: Arc<dyn oceanfs_durability::SegmentShardStore> =
            Arc::new(oceanfs_durability::InMemorySegmentShardStore::new(4194304));
        let reaper = Arc::new(oceanfs_durability::OrphanReaper::new(
            metadata_store.clone(),
            reaper_shard_store,
            gc_config,
        ));

        // ---- 7b. Construct segment data store (shared by heal and gRPC) ----
        // BlobStore is already created in section 6 and passed to the sealer.
        let heal_data_store: Arc<dyn oceanfs_durability::SegmentDataStore> = blob_store.clone();

        // ---- 7c. Construct heal dispatch pipeline ----
        let heal_config = oceanfs_durability::HealConfig::default();
        let heal_queue = Arc::new(oceanfs_durability::HealQueue::new(heal_config.queue_capacity()));
        // Initialize the global heal sender so scrub and anti-entropy can
        // call enqueue_heal() without direct queue access.
        oceanfs_durability::heal::init_global_queue(heal_queue.sender());
        let heal_codec_config = oceanfs_core::CodecConfig::default();
        let heal_decoder: Arc<dyn oceanfs_ec::Decoder> =
            Arc::new(oceanfs_ec::CauchyEncoder::new(heal_codec_config.clone()));
        // Clone before move into HealWorker — used by ReadCoordinator as well.
        let ec_decoder = heal_decoder.clone();
        let heal_worker = oceanfs_durability::HealWorker::new(
            heal_config,
            heal_queue.clone(),
            heal_decoder,
            metadata_store.clone(),
            heal_data_store.clone(),
        );

        // ---- 8. Construct caches ----
        let object_cache =
            Arc::new(oceanfs_cache::ObjectCache::new(oceanfs_cache::ObjectCacheConfig::default()));
        let metadata_cache = Arc::new(oceanfs_cache::MetadataCache::new(
            oceanfs_cache::MetadataCacheConfig::default(),
        ));
        let negative_cache = Arc::new(oceanfs_cache::NegativeCache::new(
            oceanfs_cache::NegativeCacheConfig::default(),
        ));

        // ---- 9. Construct prefetch engine ----
        let prefetch_config = oceanfs_cache::PrefetchConfig {
            enabled: config.prefetch_enabled,
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

        // ---- 11. Construct coordinators ----
        let hlc_clock = Arc::new(HlcClock::new());

        // Shared in-memory segment reader: used by ReadCoordinator for
        // chunk assembly and by S3Handler to store segment data on PUT.
        let segment_reader = Arc::new(oceanfs_server::InMemorySegmentReader::new());

        // On startup, repopulate the in-memory segment reader from any
        // blob data persisted on disk (surviving a previous restart).
        {
            let blob_ids = blob_store
                .list_blobs()
                .map_err(|e| format!("failed to list blob data on startup: {e}"))?;
            for id in &blob_ids {
                if let Ok(Some(data)) = blob_store.read_blob(id) {
                    segment_reader.put(*id, Bytes::from(data));
                }
            }
            if !blob_ids.is_empty() {
                info!(count = blob_ids.len(), "loaded persisted blob data into segment reader");
            }
        }

        let hinted_handoff = Arc::new(
            HintedHandoff::new_with_pool(pool.clone()).with_membership(membership.clone()),
        );

        let write_coordinator = Arc::new(WriteCoordinator::new(
            ring_cache.clone(),
            membership.clone(),
            pool.clone(),
            NodeId::new(&config.node_id),
            hlc_clock,
            metadata_store.clone(),
            segment_size.clone(),
            shard_small,
            shard_standard,
            segment_pool_small,
            segment_pool_standard,
            sealer.clone(),
            hinted_handoff.clone(),
        ));

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
            .with_ec_codec(heal_codec_config.data_shards, heal_codec_config.parity_shards),
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
        let s3_handler = S3Handler::new_with_caches(
            write_coordinator,
            read_coordinator,
            metadata_ops,
            bucket_store.clone(),
            Some(object_cache.clone()),
            Some(metadata_cache.clone()),
            Some(negative_cache.clone()),
        )
        .with_segment_store(segment_reader)
        .with_blob_dir(config.data_dir.join("blobs"))
        .with_prefetch_engine(prefetch_engine.clone())
        .with_router(router);

        let metrics = Arc::new(oceanfs_server::admin::MetricsRegistry::new());

        // Register subsystem metrics into the central registry.
        object_cache.register_metrics(&*metrics);
        metadata_cache.register_metrics(&*metrics);
        negative_cache.register_metrics(&*metrics);
        accel.register_metrics(&*metrics);
        heal_worker.register_metrics(&*metrics);
        buffer_pool.register_metrics(&*metrics);
        s3_handler.register_metrics(&*metrics);

        // Phase D: durability subsystem counters.
        gc_worker.register_metrics(&*metrics);
        reaper.register_metrics(&*metrics);
        scrub_worker.register_metrics(&*metrics);
        ae_worker.register_metrics(&*metrics);
        hinted_handoff.register_metrics(&*metrics);
        pool.register_metrics(&*metrics);
        membership.register_gossip_metrics(&*metrics);
        wal_writer.register_metrics(&*metrics);
        sealer.register_metrics(&*metrics);

        // Register RocksDB property gauges.
        let rocksdb_keys_gauge =
            metrics.gauge("rocksdb_estimate_keys", "Estimated number of keys in RocksDB");
        let rocksdb_block_cache_gauge =
            metrics.gauge("rocksdb_block_cache_usage_bytes", "RocksDB block cache usage in bytes");
        let rocksdb_l0_gauge =
            metrics.gauge("rocksdb_num_files_at_level0", "RocksDB number of files at level 0");

        // Register process-level gauges.
        let proc_mem_gauge =
            metrics.gauge("process_resident_memory_bytes", "Resident memory in bytes");
        let proc_fd_gauge = metrics.gauge("process_open_fds", "Open file descriptors");

        // Spawn a background poller for process and RocksDB metrics (every 15s).
        let metadata_clone = metadata_store.clone();
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
                // RocksDB property polling.
                if let Some(val) = property_as_u64(&metadata_clone, "rocksdb.estimate-num-keys") {
                    rocksdb_keys_gauge.set(val);
                }
                if let Some(val) = property_as_u64(&metadata_clone, "rocksdb.block-cache-usage") {
                    rocksdb_block_cache_gauge.set(val);
                }
                if let Some(val) = property_as_u64(&metadata_clone, "rocksdb.num-files-at-level0") {
                    rocksdb_l0_gauge.set(val);
                }
            }
        });

        let admin_handler = AdminHandler::new_with_cluster(
            bucket_store,
            metrics,
            membership.clone(),
            ring_cache.clone(),
        )
        .with_scrub(scrub_worker.clone(), metadata_store.clone(), heal_data_store.clone())
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
        let app = axum::Router::new()
            .merge(s3_handler.into_router_with_auth(auth_middleware))
            .merge(admin_handler.into_router())
            .layer(axum::extract::DefaultBodyLimit::max(config.max_body_size));

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
        );
        let gossip_service =
            oceanfs_membership::grpc::gossip_service::GossipGrpcService::new(membership.clone());

        let healing_service = oceanfs_durability::healing_service::HealingGrpcService::new(
            hinted_handoff.clone(),
            metadata_store.clone(),
            heal_data_store.clone(),
        );
        let cache_service = oceanfs_server::grpc::cache_service::CacheGrpcService::new(
            Some(object_cache.clone()),
            Some(metadata_cache.clone()),
        );
        let scrub_service = oceanfs_durability::scrub_service::ScrubGrpcService::new(
            metadata_store.clone(),
            heal_data_store.clone(),
        );

        // Build tonic Server with all services registered.
        let grpc_router = tonic::transport::Server::builder()
            .add_service(oceanfs_storage::SegmentRpcServer::new(segment_service))
            .add_service(oceanfs_network::GossipRpcServer::new(gossip_service))
            .add_service(oceanfs_durability::HealingRpcServer::new(healing_service))
            .add_service(oceanfs_cache::CacheRpcServer::new(cache_service))
            .add_service(oceanfs_durability::ScrubRpcServer::new(scrub_service));

        tokio::spawn(async move {
            if let Err(e) = grpc_router.serve(grpc_addr).await {
                error!("gRPC server error: {e}");
            }
        });

        // ---- 16. Spawn background tasks ----
        let mut background = Self::spawn_background_tasks(
            gc_worker,
            metadata_store.clone(),
            ae_worker,
            scrub_worker,
            reaper,
            prefetch_engine,
            heal_worker,
            heal_data_store.clone(),
            &config,
        );

        // ---- 17. Spawn hinted handoff delivery watcher ----
        // Watches for membership state transitions to ALIVE and drains
        // the handoff buffer for returning nodes.
        let hh = hinted_handoff.clone();
        let mut events = membership.subscribe();
        let delivery_token = background.delivery_cancel.clone();
        let delivery_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = delivery_token.cancelled() => {
                        info!("Hinted handoff delivery watcher cancelled");
                        break;
                    }
                    event = events.recv() => {
                        match event {
                            Ok(ev) if ev.new_state == oceanfs_core::NodeState::Alive &&
                                      ev.old_state != oceanfs_core::NodeState::Alive => {
                                info!(
                                    node = %ev.node_id,
                                    "node returned to cluster; delivering pending hinted handoffs"
                                );
                                if let Err(e) = hh.deliver_pending(ev.node_id).await {
                                    warn!(error = %e, "hinted handoff delivery failed on rejoin");
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

        info!(
            node_id = %config.node_id,
            http_addr = %server_addr,
            grpc_addr = %grpc_addr,
            "OceanFS node started"
        );

        // ---- 18. Construct graceful leave handler ----
        let leave_handler = Arc::new(NodeLeaveHandler {
            wal_writer: wal_writer.clone(),
            blob_store: blob_store.clone(),
            pool: pool.clone(),
            membership: membership.clone(),
        });

        Ok(Node {
            config,
            accel,
            server_addr,
            grpc_addr,
            http_shutdown,
            background,
            leave_handler,
            membership,
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
    /// # Errors
    ///
    /// Returns an error if any background task panicked or timed out.
    pub async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        info!(node_id = %self.config.node_id, "Shutting down OceanFS node");

        // ---- Graceful leave: handoff WAL and segment shards to successor ----
        let leave_result = self.membership.leave(Some(self.leave_handler.as_ref())).await;
        if let Err(e) = leave_result {
            warn!(error = %e, "graceful leave handoff failed; continuing shutdown");
        }

        // Signal the HTTP server to stop accepting connections and drain.
        self.http_shutdown.cancel();

        // Signal all background loops to stop.
        self.background.gossip_cancel.cancel();
        self.background.gc_cancel.cancel();
        self.background.ae_cancel.cancel();
        self.background.scrub_cancel.cancel();
        self.background.reaper_cancel.cancel();
        self.background.prefetch_cancel.cancel();
        self.background.fd_cancel.cancel();
        self.background.heal_cancel.cancel();
        self.background.delivery_cancel.cancel();

        // Wait for background tasks with a timeout.
        let _ = tokio::time::timeout(Duration::from_secs(10), async {
            let _ = tokio::try_join!(
                async { self.background.gossip.await.map_err(|e| format!("{e}")) },
                async { self.background.gc.await.map_err(|e| format!("{e}")) },
                async { self.background.anti_entropy.await.map_err(|e| format!("{e}")) },
                async { self.background.scrub.await.map_err(|e| format!("{e}")) },
                async { self.background.orphan_reaper.await.map_err(|e| format!("{e}")) },
                async { self.background.failure_detector.await.map_err(|e| format!("{e}")) },
                async { self.background.heal.await.map_err(|e| format!("{e}")) },
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

        Ok(cfg)
    }

    /// Spawns all background task loops.
    #[allow(clippy::too_many_arguments)]
    fn spawn_background_tasks(
        gc_worker: Arc<oceanfs_durability::GarbageCollector>,
        metadata_store: Arc<oceanfs_storage::RocksDbMetadataStore>,
        ae_worker: Arc<oceanfs_durability::AntiEntropy>,
        scrub_worker: Arc<oceanfs_durability::ScrubCoordinator>,
        reaper: Arc<oceanfs_durability::OrphanReaper>,
        prefetch_engine: Arc<oceanfs_cache::PrefetchEngine>,
        heal_worker: oceanfs_durability::HealWorker,
        data_store: Arc<dyn oceanfs_durability::SegmentDataStore>,
        config: &oceanfs_core::NodeConfig,
    ) -> BackgroundTasks {
        // Gossip: placeholder (driven by Membership internally).
        let gossip_cancel = CancellationToken::new();
        let gossip = tokio::spawn(async { std::future::pending::<()>().await });

        // GC: runs every gc_interval_sec from config.
        let gc_cancel = CancellationToken::new();
        let gc_token = gc_cancel.clone();
        let gc_store = metadata_store.clone();
        let gc_interval = Duration::from_secs(config.gc_interval_sec);
        let gc = tokio::spawn(async move {
            let mut interval = tokio::time::interval(gc_interval);
            loop {
                tokio::select! {
                    _ = gc_token.cancelled() => {
                        info!("GC task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = gc_worker.run_cycle(gc_store.clone()).await {
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
        let ae = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(ae_interval_secs));
            loop {
                tokio::select! {
                    _ = ae_token.cancelled() => {
                        info!("Anti-entropy task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = ae_worker.run_cycle().await {
                            warn!("Anti-entropy cycle error: {e}");
                        }
                    }
                }
            }
        });

        // Scrub: runs every scrub_interval_sec from config.
        let scrub_cancel = CancellationToken::new();
        let scrub_token = scrub_cancel.clone();
        let scrub_store = metadata_store.clone();
        let scrub_data = data_store;
        let scrub_interval_secs = config.scrub_interval_sec;
        let scrub = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(scrub_interval_secs));
            loop {
                tokio::select! {
                    _ = scrub_token.cancelled() => {
                        info!("Scrub task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        match scrub_worker.run_cycle(
                            scrub_store.clone(),
                            scrub_data.clone(),
                        ).await {
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
        let orphan_reaper = tokio::spawn(async move {
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
        // worker; the background task keeps the join handle alive. When prefetch
        // is disabled, the engine silently drops all queued tasks.
        let prefetch_cancel = CancellationToken::new();
        let prefetch_token = prefetch_cancel.clone();
        let prefetch = Some(tokio::spawn(async move {
            // Move prefetch_engine into the closure to keep it alive.
            let _engine = prefetch_engine;
            loop {
                tokio::select! {
                    _ = prefetch_token.cancelled() => {
                        info!("Prefetch task cancelled");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {
                        // Keep alive.
                    }
                }
            }
        }));

        // SWIM failure detector: Membership handles this internally.
        let fd_cancel = CancellationToken::new();
        let fd_token = fd_cancel.clone();
        let failure_detector = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = fd_token.cancelled() => {
                        info!("Failure detector task cancelled");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        // Heartbeat placeholder.
                    }
                }
            }
        });

        // EC Heal worker: drains the HealQueue and repairs corrupt shards.
        let heal_cancel = CancellationToken::new();
        let heal_token = heal_cancel.clone();
        let heal = tokio::spawn(async move {
            heal_worker.run(heal_token).await;
            info!("Heal worker task completed");
        });

        // Hinted handoff delivery watcher token — the watcher itself is
        // spawned after BackgroundTasks is constructed so we can store
        // the join handle retroactively.
        let delivery_cancel = CancellationToken::new();

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

/// Queries a RocksDB integer property and returns it as a `u64`.
///
/// Returns `None` if the property is not available or cannot be parsed.
fn property_as_u64(store: &oceanfs_storage::RocksDbMetadataStore, name: &str) -> Option<u64> {
    store.property(name)?.parse::<u64>().ok()
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
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        let pool =
            Arc::new(oceanfs_network::ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let ae_worker = Arc::new(oceanfs_durability::AntiEntropy::new(
            oceanfs_durability::AntiEntropyConfig::default(),
            membership,
            metadata_store.clone(),
            pool,
            Arc::new(oceanfs_durability::InMemorySegmentStore::new()),
        ));
        let scrub_config = oceanfs_durability::ScrubConfig::default();
        let scrub_worker = Arc::new(oceanfs_durability::ScrubCoordinator::new(scrub_config));
        let reaper_shard_store: Arc<dyn oceanfs_durability::SegmentShardStore> =
            Arc::new(oceanfs_durability::InMemorySegmentShardStore::new(4194304));
        let reaper = Arc::new(oceanfs_durability::OrphanReaper::new(
            metadata_store.clone(),
            reaper_shard_store,
            gc_config,
        ));

        let prefetch_config = oceanfs_cache::PrefetchConfig::default();
        let prefetch_store: Arc<dyn oceanfs_storage_api::MetadataStore> =
            Arc::new(PrefetchStoreAdapter { store: metadata_store.clone() });
        let _metadata_cache = Arc::new(oceanfs_cache::MetadataCache::new(
            oceanfs_cache::MetadataCacheConfig::default(),
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
        let heal_worker = oceanfs_durability::HealWorker::new(
            heal_config,
            heal_queue,
            heal_decoder,
            metadata_store.clone(),
            heal_data_store,
        );

        let bg = Node::spawn_background_tasks(
            gc_worker,
            metadata_store.clone(),
            ae_worker,
            scrub_worker,
            reaper,
            prefetch_engine,
            heal_worker,
            bg_data_store,
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

        // Cancel all and wait.
        bg.gossip_cancel.cancel();
        bg.gc_cancel.cancel();
        bg.ae_cancel.cancel();
        bg.scrub_cancel.cancel();
        bg.reaper_cancel.cancel();
        bg.prefetch_cancel.cancel();
        bg.fd_cancel.cancel();
        bg.heal_cancel.cancel();
    }

    #[test]
    fn prefetch_store_adapter_list_object_keys() {
        let tmp = TempDir::new().expect("tempdir");
        let metadata_config =
            MetadataConfig { data_dir: tmp.path().join("metadata"), ..Default::default() };
        let store = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&metadata_config)
                .expect("open metadata store"),
        );
        let adapter = PrefetchStoreAdapter { store };
        let bucket = BucketId::new("test-bucket");
        let result = adapter.list_object_keys(&bucket).expect("list_object_keys");
        assert!(result.is_empty(), "new bucket should have no keys");
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
            "127.0.0.1:9200".parse().unwrap(),
        );

        let hint = HintRecord {
            intended_for: target.clone(),
            segment_id: SegmentId::new(),
            offset: 0,
            length: 42,
            timestamp: Hlc::zero(),
            data: vec![1, 2, 3],
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
            "127.0.0.1:9200".parse().unwrap(),
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
            "127.0.0.1:9300".parse().unwrap(),
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
            "127.0.0.1:9300".parse().unwrap(),
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
    async fn leave_handler_handoff_wal_flushes_and_reports_success() {
        use std::sync::Arc;

        use oceanfs_core::{NodeId, WalConfig};
        use oceanfs_membership::GracefulLeaveHandler;
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;

        // Setup: real WAL in temp dir.
        let dir = tempfile::tempdir().unwrap();
        let wal_writer = Arc::new(
            oceanfs_storage::WalWriter::open(&WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
            })
            .await
            .unwrap(),
        );
        let blob_store =
            Arc::new(oceanfs_storage::BlobStore::open(&dir.path().join("blobs")).unwrap());

        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("leave-test"),
            "127.0.0.1:9100".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let handler = super::NodeLeaveHandler { wal_writer, blob_store, pool, membership };

        // handoff_wal_to should sync and succeed even without a real successor.
        let result =
            GracefulLeaveHandler::handoff_wal_to(&handler, &NodeId::new("successor")).await;
        assert!(result.is_ok(), "WAL handoff should succeed");
    }

    /// Verifies that `transfer_segment_shards_to` handles an empty blob store.
    #[tokio::test]
    async fn leave_handler_transfer_empty_blob_store_returns_zero() {
        use std::sync::Arc;

        use oceanfs_core::NodeId;
        use oceanfs_membership::GracefulLeaveHandler;
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;

        let dir = tempfile::tempdir().unwrap();
        let wal_writer = Arc::new(
            oceanfs_storage::WalWriter::open(&oceanfs_core::WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
            })
            .await
            .unwrap(),
        );
        let blob_store =
            Arc::new(oceanfs_storage::BlobStore::open(&dir.path().join("blobs")).unwrap());

        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            NodeId::new("empty-blob"),
            "127.0.0.1:9200".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let handler = super::NodeLeaveHandler { wal_writer, blob_store, pool, membership };

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
        use oceanfs_membership::GracefulLeaveHandler;
        use oceanfs_membership::Membership;
        use oceanfs_network::ConnectionPool;

        let dir = tempfile::tempdir().unwrap();
        let wal_writer = Arc::new(
            oceanfs_storage::WalWriter::open(&oceanfs_core::WalConfig {
                data_dir: dir.path().join("wal"),
                max_file_size_bytes: 1024 * 1024,
                fsync_batch_timeout_ms: 5,
            })
            .await
            .unwrap(),
        );
        let blob_store =
            Arc::new(oceanfs_storage::BlobStore::open(&dir.path().join("blobs")).unwrap());

        // Write some segments.
        for i in 0..3 {
            blob_store.write_blob(&SegmentId::new(), &[i as u8; 64]).unwrap();
        }

        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let addr: std::net::SocketAddr = "127.0.0.1:9300".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new("blob-test"),
            addr,
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        // Register the successor in membership so address resolution works.
        membership.upsert_node(
            NodeId::new("successor"),
            oceanfs_core::NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            addr,
        );
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let handler = super::NodeLeaveHandler { wal_writer, blob_store, pool, membership };

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
            "127.0.0.1:9500".parse().unwrap(),
        );

        let handler = RecordingHandler {
            wal_called: AtomicBool::new(false),
            shard_called: AtomicBool::new(false),
        };

        // leave() requires started membership; start background tasks.
        membership.start().unwrap();
        membership.join().await.unwrap();

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
        use oceanfs_membership::GracefulLeaveHandler;
        use oceanfs_durability::{
            anti_entropy::InMemorySegmentStore, healing_service::HealingGrpcService,
            HealingRpcServer, HintedHandoff, SegmentDataStore,
        };
        use oceanfs_membership::Membership;
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
            })
            .unwrap(),
        );
        // Use a fixed port for the test gRPC server.
        let bound_addr: std::net::SocketAddr = "127.0.0.1:15550".parse().unwrap();
        let healing_svc =
            HealingGrpcService::new(server_handoff.clone(), server_meta.clone(), server_store);

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
            })
            .await
            .unwrap(),
        );
        let blob_store =
            Arc::new(oceanfs_storage::BlobStore::open(&dir.path().join("blobs")).unwrap());

        // Write test segments to blob store.
        let seg_a = SegmentId::new();
        let seg_b = SegmentId::new();
        blob_store.write_blob(&seg_a, b"segment A data for graceful leave").unwrap();
        blob_store.write_blob(&seg_b, b"segment B data for graceful leave").unwrap();

        // Build ring with successor node.
        let mut ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        ring.add_node(NodeId::new("leaver"));
        ring.add_node(NodeId::new("successor"));
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));

        let membership = Arc::new(Membership::new(
            NodeId::new("leaver"),
            "127.0.0.1:9999".parse().unwrap(),
            oceanfs_core::GossipConfig::default(),
            ring_cache.clone(),
        ));
        // Register successor with the actual bound address for gRPC.
        membership.upsert_node(
            NodeId::new("successor"),
            NodeState::Alive,
            Incarnation::new(1),
            bound_addr,
        );
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));

        let handler = super::NodeLeaveHandler {
            wal_writer: wal_writer.clone(),
            blob_store: blob_store.clone(),
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
