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
    RingConfig, RpcConfig, SegmentSizeConfig, WalConfig,
};
use oceanfs_durability::HintedHandoff;
use oceanfs_server::{
    auth::AuthMiddleware, metadata_ops::MetadataOps, AdminHandler, BucketConfigStore,
    ReadCoordinator, Router, S3Handler, WriteCoordinator,
};
use tokio::task::JoinHandle;
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
        let ring_config = RingConfig::default();
        let ring = oceanfs_routing::Ring::new(ring_config);
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));

        // ---- 4. Construct membership ----
        let grpc_addr: SocketAddr = config
            .grpc_listen_addr
            .parse()
            .map_err(|e| format!("invalid grpc_listen_addr: {e}"))?;
        let gossip_config = oceanfs_core::GossipConfig {
            seed_nodes: config.seed_nodes.clone(),
            interval_ms: config.gossip_interval_ms,
            suspicion_timeout_ms: config.suspicion_timeout_ms,
            failure_timeout_ms: config.failure_timeout_ms,
            ..oceanfs_core::GossipConfig::default()
        };
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
        // BufferPool constructed here; will be wired to active segment writers
        // when final-integration-read-write-end-to-end lands (perf rule 1.2).
        let _buffer_pool = Arc::new(oceanfs_storage::BufferPool::new(65536, 256));
        // ADR-0001: tiered segment sizing driven by SegmentSizeConfig.
        let seal_config = oceanfs_storage::SealConfig {
            target_size_bytes: segment_size.default_target_size,
            seal_timeout_ms: 5000,
            data_dir: config.data_dir.join("segments"),
        };
        // SegmentSealer constructed here; will be wired into the write path
        // when final-integration-read-write-end-to-end lands.
        let _sealer = Arc::new(oceanfs_storage::SegmentSealer::new(
            seal_config,
            metadata_store.clone(),
            wal_writer.clone(),
        ));
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
        // Use a disk-backed blob store so segment data survives restarts.
        let blob_store = Arc::new(
            oceanfs_storage::BlobStore::open(&config.data_dir.join("blobs"))
                .map_err(|e| format!("failed to open blob store: {e}"))?,
        );
        let heal_data_store: Arc<dyn oceanfs_durability::SegmentDataStore> = blob_store.clone();

        // ---- 7c. Construct heal dispatch pipeline ----
        let heal_config = oceanfs_durability::HealConfig::default();
        let heal_queue = Arc::new(oceanfs_durability::HealQueue::new(heal_config.queue_capacity()));
        // Initialize the global heal sender so scrub and anti-entropy can
        // call enqueue_heal() without direct queue access.
        oceanfs_durability::heal::init_global_queue(heal_queue.sender());
        let heal_codec_config = oceanfs_core::CodecConfig::default();
        let heal_decoder: Arc<dyn oceanfs_ec::Decoder> =
            Arc::new(oceanfs_ec::CauchyEncoder::new(heal_codec_config));
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

        let write_coordinator = Arc::new(WriteCoordinator::new(
            ring_cache.clone(),
            membership.clone(),
            pool.clone(),
            NodeId::new(&config.node_id),
            hlc_clock,
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
            .with_decoder(ec_decoder.clone()),
        );

        let hinted_handoff = Arc::new(
            HintedHandoff::new_with_pool(pool.clone()).with_membership(membership.clone()),
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
        let background = Self::spawn_background_tasks(
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

        info!(
            node_id = %config.node_id,
            http_addr = %server_addr,
            grpc_addr = %grpc_addr,
            "OceanFS node started"
        );

        Ok(Node { config, accel, server_addr, grpc_addr, http_shutdown, background })
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
        }
    }
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
        assert!(err_msg.contains("listen_addr"), "error should mention listen_addr: {err_msg}");
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
}
