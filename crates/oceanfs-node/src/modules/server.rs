//! Server subsystem bundle (c3 — composition-root decomposition).
//!
//! Owns the construction previously inline in `Node::start()` §8–§13 and
//! §15's gRPC service construction: the L1/L2/L3 caches + eviction
//! policies, the prefetch engine (with its store adapter), the metadata
//! bridge adapter, the shared HLC clock, the write/read coordinators +
//! forwarding router, the S3 + admin handlers, and the axum router
//! assembly; plus the four data-plane gRPC service implementations
//! (segment/healing/cache/scrub with their decode caps baked into the
//! wrapped RPC servers). The membership-plane services (gossip/probe)
//! were re-seated to the membership module by c4.
//!
//! What stays in `Node::start()` is the *binding* — HTTP/tonic listener
//! creation and `serve` spawns moved to the data-plane module by c4
//! (`DataPlaneModule::serve`), the membership-plane bind + bootstrap
//! moved to the membership module (`MembershipModule::start_plane_and_join`),
//! and the node-side metric registrations (owners outside this module).
//!
//! The sealed-segment notifier that used to fan out of the write
//! coordinator moved storage-side first (c3a — the seal pipeline); the
//! coordinator chains here are pure construction with no notifier wiring
//! left.

use std::sync::Arc;

use oceanfs_core::{
    BucketId, Hlc, HlcClock, NodeConfig, NodeId, ObjectKey, ObjectMetadata, SegmentSizeConfig,
    Tombstone,
};

use crate::{
    metadata_adapter::MetadataStoreAdapter, node::RepairRequest, pool_manifest,
    routing_cache::ManifestCache,
};

/// The server subsystem bundle (c3).
///
/// `router` feeds the data plane's HTTP bind; `grpc` feeds the data-plane
/// tonic bind (decode caps already applied) — both consumed by
/// [`crate::modules::data_plane::DataPlaneModule::serve`]. The
/// membership-plane services (gossip/probe) were re-seated to
/// [`crate::modules::membership::MembershipModule`] by c4 (see the NOTE
/// below). `prefetch_engine` is kept alive by the node's background
/// prefetch task (§16).
pub(crate) struct ServerModule {
    /// The assembled axum router (S3 + admin merged, auth middleware,
    /// body-limit + 413-logging layers) — moved into the data plane's
    /// HTTP serve by `DataPlaneModule::serve` (c4).
    pub(crate) router: axum::Router,
    /// The four data-plane gRPC services, tonic-wrapped with their
    /// message-size caps — assembled into the data-plane tonic server by
    /// `DataPlaneModule::serve` (c4).
    pub(crate) grpc: DataPlaneServices,
    /// The prefetch engine — the §16 background pre-warmer task holds it
    /// alive for the node's lifetime.
    pub(crate) prefetch_engine: Arc<oceanfs_cache::PrefetchEngine>,
}

// NOTE (c4 planes split): the gossip/probe membership-plane services
// were re-seated OUT of this module into
// `crate::modules::membership::MembershipModule::start_plane_and_join`
// — they wrap only membership-plane inputs (membership, the plane's
// dedicated pool, node id, gossip timeout) and belong to the membership
// plane, not the data plane. This module serves the DATA plane only
// (segment/healing/cache/scrub).

/// The data-plane gRPC services, wrapped and capped by the module.
///
/// Segment + healing carry the 64 MiB decode limit (hint batches and
/// over-4-MiB replica appends — the tonic 4 MiB default rejected both);
/// cache/scrub keep the default. Constructed in the same order the
/// tonic bind adds them (segment, healing, cache, scrub).
pub(crate) struct DataPlaneServices {
    /// Segment service (append replication receiver + shard fetch).
    pub(crate) segment: oceanfs_storage::SegmentRpcServer<
        oceanfs_server::grpc::segment_service::SegmentGrpcService,
    >,
    /// Healing service (hinted handoff receiver, merkle, re-replication).
    pub(crate) healing: oceanfs_durability::HealingRpcServer<
        oceanfs_durability::healing_service::HealingGrpcService,
    >,
    /// Cache-invalidation service.
    pub(crate) cache:
        oceanfs_cache::CacheRpcServer<oceanfs_server::grpc::cache_service::CacheGrpcService>,
    /// Scrub-trigger service.
    pub(crate) scrub:
        oceanfs_durability::ScrubRpcServer<oceanfs_durability::scrub_service::ScrubGrpcService>,
}

/// Bounded-channel-backed [`RepairSink`](oceanfs_durability::healing_service::RepairSink)
/// — the g5 `request_re_replication` handler's enqueue target on the
/// acquiring node. The target's `ReRepWorker` drains the queue.
struct WorkerQueueSink {
    tx: tokio::sync::mpsc::Sender<RepairRequest>,
}

#[async_trait::async_trait]
impl oceanfs_durability::healing_service::RepairSink for WorkerQueueSink {
    async fn enqueue(&self, request: RepairRequest) -> Result<(), String> {
        self.tx.try_send(request).map_err(|e| match e {
            tokio::sync::mpsc::error::TrySendError::Full(_) => "repair queue full".to_string(),
            tokio::sync::mpsc::error::TrySendError::Closed(_) => "repair queue closed".to_string(),
        })
    }
}

/// Minimal adapter wrapping `oceanfs_storage::RocksDbMetadataStore` to
/// implement the `oceanfs_storage_api::MetadataStore` trait needed by
/// `PrefetchEngine`.
pub(crate) struct PrefetchStoreAdapter {
    pub(crate) store: Arc<oceanfs_storage::RocksDbMetadataStore>,
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

impl ServerModule {
    /// Builds the server subsystem bundle.
    ///
    /// Owns the construction previously inline in `Node::start()` §8–§13
    /// (caches + policies, prefetch, bridge adapter, coordinators,
    /// handlers, axum router) and §15's gRPC service construction. Pure
    /// sequential construction in the original order — a move, not a
    /// redesign; the only fallible step is the re-replication worker's
    /// queue sender (guaranteed present by the time `Node::start()`
    /// calls this — the worker is built by c2 above).
    ///
    /// # Parameters
    ///
    /// `config` is the validated node config; `storage` is the c1 bundle
    /// (stores, pools, sealer, lifecycle, registry, reader, accel);
    /// `durability` is the c2 bundle (timeouts, codec, scrub, repair
    /// dispatcher + worker); `membership` is the membership module's
    /// membership Arc and `pool` the data-plane module's connection pool
    /// (c4 — the planes split; `membership_pool` was re-seated with the
    /// gossip/probe services to the membership module); `ring_cache` and
    /// `manifest_cache` are the routing (§3) and peer-manifest (§4b)
    /// caches; `hinted_handoff` is the legacy gRPC hint receiver
    /// (§11, node-owned) the healing service consumes; the
    /// `hinted_handoff_manager` is the durable hint manager (also
    /// node-owned — the write coordinator enqueues through it);
    /// `ready_gate` is the cluster-readiness gate the write coordinator
    /// consults (§11, node-owned); `is_cluster_node` selects the honest
    /// quorum mode; `announce_incarnation` is the boot incarnation the
    /// pool-attach manifest re-declaration falls back to; `metrics` is
    /// the node's central registry — module-owned series (caches, S3
    /// handler, healing service) register here during build.
    ///
    /// # Errors
    ///
    /// Returns an error only when the re-replication worker's queue
    /// sender is unavailable (a c2 construction-order invariant
    /// violation).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build(
        config: &NodeConfig,
        storage: &crate::modules::storage::StorageModule,
        durability: &crate::modules::durability::DurabilityModule,
        membership: Arc<oceanfs_membership::Membership>,
        pool: Arc<oceanfs_network::ConnectionPool>,
        ring_cache: Arc<oceanfs_routing::RingCache>,
        manifest_cache: Arc<ManifestCache>,
        hinted_handoff: Arc<oceanfs_durability::HintedHandoff>,
        hinted_handoff_manager: Arc<oceanfs_durability::HintedHandoffManager>,
        ready_gate: Arc<std::sync::atomic::AtomicBool>,
        is_cluster_node: bool,
        announce_incarnation: u64,
        metrics: Arc<oceanfs_server::admin::MetricsRegistry>,
    ) -> Result<Self, String> {
        let self_id = NodeId::new(&config.node_id);

        // ---- caches + eviction policies (§8) ----
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
                // [review][config][high]
                // missing config from userland
                // [end]
                Box::new(oceanfs_cache::eviction::TtlLruPolicy::new(
                    oceanfs_cache::eviction::TtlLruConfig::default(),
                ))
            }
            _ => {
                tracing::warn!("Unknown L2 eviction policy; falling back to TTL-LRU");
                // [review][config][high]
                // same remark
                // [end]
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

        // ---- prefetch engine (§9) ----
        let prefetch_config = oceanfs_cache::PrefetchConfig {
            enabled: config.prefetch_enabled,
            after_list: config.prefetch_after_list,
            after_get: config.prefetch_after_get,
            ..Default::default()
        };
        let prefetch_store: Arc<dyn oceanfs_storage_api::MetadataStore> =
            Arc::new(PrefetchStoreAdapter { store: storage.metadata_store.clone() });
        let prefetch_engine = Arc::new(oceanfs_cache::PrefetchEngine::new(
            prefetch_config,
            metadata_cache.clone(),
            Some(object_cache.clone()),
            prefetch_store,
        ));

        // ---- bridge adapter (§10) ----
        let metadata_ops: Arc<dyn oceanfs_server::metadata_ops::MetadataOps> =
            Arc::new(MetadataStoreAdapter::new(storage.metadata_store.clone()));

        // ---- coordinators + forwarding router (§11 server parts) ----
        // The shared HLC clock (write path stamps, read path compares).
        let hlc_clock = Arc::new(HlcClock::new());

        let write_coordinator = Arc::new(
            oceanfs_server::WriteCoordinator::new(
                ring_cache.clone(),
                membership.clone(),
                pool.clone(),
                self_id.clone(),
                hlc_clock.clone(),
                storage.metadata_store.clone(),
                SegmentSizeConfig::default(),
                storage.shard_small.clone(),
                storage.shard_standard.clone(),
                storage.segment_pool_small.clone(),
                storage.segment_pool_standard.clone(),
                storage.sealer.clone(),
                storage.lifecycle.clone(),
                hinted_handoff_manager.clone(),
            )
            .with_timeouts(durability.op_timeouts.clone())
            // Per-bucket compression: buckets opting in via
            // `compression.tier != None` compress chunks on the write
            // path through the accel dispatcher (blocking pool).
            .with_compressor(Some(storage.accel.clone()))
            // Cluster-readiness gate: while the ring is still converging
            // after (re)join, writes fail with 503 instead of
            // under-replicating (see the gate task in node.rs §11).
            .with_ready_gate(ready_gate)
            // Step 1c (honest quorum): cluster nodes require the ring
            // view to satisfy the requested write quorum; single-node
            // deployments (no seeds) keep the adaptive capping — the
            // default bucket policy (w=2) would otherwise reject every
            // write on a permanently 1-node ring.
            .with_quorum_requires_ring(is_cluster_node)
            .with_hint_inline_threshold(config.hint_inline_threshold_bytes)
            // Peer-side routing hint (ADR-0029 §D5): replica targets
            // whose manifest reports write_degraded / zero Healthy data
            // pools are excluded; Phase A all-healthy = neutral.
            .with_routing_hint(manifest_cache.clone())
            // g2 (ADR-0029 §D3): the hint enqueue path rejects new debt
            // while the hints pool is Dead.
            .with_pool_registry(storage.registry.clone()),
        );

        // Clone for the hint-applier adapter (the coordinator is moved
        // into the S3 handler state below).
        let write_coordinator_for_applier = Arc::clone(&write_coordinator);

        let read_coordinator = Arc::new(
            oceanfs_server::ReadCoordinator::new_with_metadata(
                ring_cache.clone(),
                self_id.clone(),
                None,
                metadata_ops.clone(),
            )
            .with_segment_reader(storage.segment_reader.clone())
            .with_connection_pool(pool.clone())
            .with_membership(membership.clone())
            .with_decoder(durability.ec_decoder.clone())
            .with_ec_codec(
                durability.codec_config.data_shards,
                durability.codec_config.parity_shards,
            )
            // Read-path decompression for compressed chunks (paired
            // with the write path's per-bucket compression).
            .with_compressor(Some(storage.accel.clone()))
            .with_timeouts(durability.op_timeouts.clone())
            .with_default_fetch_strategy(config.default_fetch_strategy)
            .with_hlc_clock(hlc_clock.clone())
            // Peer-side routing hint (ADR-0029 §D5): replica candidates
            // with zero Healthy data pools are excluded from the gRPC
            // fetch path; Phase A all-healthy = neutral.
            .with_routing_hint(manifest_cache.clone())
            // Local-availability gate (g6): the SAME pool registry the
            // write coordinator consults — a Dead metadata pool rejects
            // reads with 503 (single source of truth).
            .with_pool_registry(storage.registry.clone()),
        );

        // Router handles request forwarding to correct coordinator nodes.
        let router = Arc::new(oceanfs_server::Router::new(
            ring_cache.clone(),
            membership.clone(),
            pool.clone(),
            self_id.clone(),
        ));

        // ---- handlers (§12) ----
        let bucket_store = Arc::new(oceanfs_server::BucketConfigStore::new());
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
        let s3_handler = oceanfs_server::S3Handler::new_with_caches_and_backpressure(
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

        // Module-owned metric series register during build (the S3
        // handler is consumed by the router below, so its registration
        // must precede the merge).
        object_cache.register_metrics(&*metrics);
        metadata_cache.register_metrics(&*metrics);
        negative_cache.register_metrics(&*metrics);
        s3_handler.register_metrics(&*metrics);

        // Runtime pool attach (ADR-0029 §D8, f8): after `POST
        // /admin/pools` registers a pool, re-declare the NodeManifest
        // (f6) so peers see the new capacity and re-seed the routing
        // cache's self entry (f7). The incarnation tracks the CURRENT
        // one (a rejoin bumps it) with the boot value as the fallback.
        let attach_membership = membership.clone();
        let attach_registry = storage.registry.clone();
        let attach_cache = manifest_cache.clone();
        let attach_self_id = self_id.clone();
        let attach_boot_incarnation = announce_incarnation;
        let attach_metrics = metrics.clone();
        let on_pool_attached: Arc<dyn Fn() -> Result<(), String> + Send + Sync> =
            Arc::new(move || {
                let incarnation = attach_membership
                    .incarnation_of(&attach_self_id)
                    .map(|inc| inc.value())
                    .unwrap_or(attach_boot_incarnation);
                let manifest = pool_manifest::build_node_manifest(incarnation, &attach_registry);
                attach_membership.set_self_manifest(manifest.clone());
                attach_cache.update(attach_self_id.clone(), Arc::new(manifest));
                // Register the attached pool's metric series with the
                // global registry (idempotent — existing series are kept).
                attach_registry.register_metrics(&*attach_metrics);
                Ok(())
            });

        let admin_handler = oceanfs_server::AdminHandler::new_with_cluster(
            bucket_store,
            metrics.clone(),
            membership.clone(),
            ring_cache.clone(),
        )
        .with_scrub(
            durability.scrub.clone(),
            storage.metadata_store.clone(),
            storage.data_store.clone(),
        )
        .with_lifecycle_registry(Arc::clone(&storage.lifecycle_registry))
        .with_caches(
            Some(object_cache.clone()),
            Some(metadata_cache.clone()),
            Some(negative_cache.clone()),
        )
        .with_accel(storage.accel.clone())
        .with_pool_attach(storage.registry.clone(), on_pool_attached)
        // Live wal-pool remount (g7, ADR-0035): the coordinator owns the
        // replaced-wal drain + write-resume gate (built into the
        // durability module, which holds the ReRepWorker + AE handles).
        .with_wal_remount({
            let wal_recovery = durability.wal_recovery.clone();
            Arc::new(move || {
                let wal_recovery = Arc::clone(&wal_recovery);
                Box::pin(async move { wal_recovery.live_remount().await })
            })
        });

        // ---- axum router assembly (§13) ----
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
                        tracing::info!(path = %keys_path.display(), "loaded access keys for S3 auth");
                        Some(oceanfs_server::auth::SigV4Verifier::new(store))
                    }
                    Err(e) => {
                        tracing::warn!(
                            path = %keys_path.display(),
                            error = %e,
                            "failed to load access keys — auth will reject all requests"
                        );
                        None
                    }
                }
            } else {
                tracing::warn!(
                    "s3_auth_enabled but no access_keys.toml found at {}",
                    keys_path.display()
                );
                None
            };
            oceanfs_server::auth::AuthMiddleware::new(true, verifier)
        } else {
            oceanfs_server::auth::AuthMiddleware::passthrough()
        };
        // `DefaultBodyLimit` rejects oversized requests before any
        // handler runs, which historically made 413s invisible in the
        // node log (F6). The logging middleware below is added *after*
        // the limit layer — axum runs the last-added layer first — so it
        // wraps the limit and observes its 413 responses. The closure
        // captures only the `usize` value, not the whole config.
        let max_body_size = config.max_body_size;
        let router = axum::Router::new()
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

        // ---- Data-plane gRPC service construction (§15) ----
        // ADR-0028 D1: the membership services (gossip + probe) live on
        // the membership plane (constructed by the membership module,
        // c4) — the data-plane server hosts only
        // Segment/Healing/Cache/Scrub.
        let segment_service = oceanfs_server::grpc::segment_service::SegmentGrpcService::new(
            storage.data_store.clone(),
            Some(storage.metadata_store.clone()),
            storage.shard_buffer_pool.clone(),
            hlc_clock.clone(),
        )
        // Replica appends register their segments in the lifecycle
        // machine: without registration the GC and the orphan reaper
        // never see the receiver's .dat files (the fleet disk-fill
        // root cause).
        .with_lifecycle(storage.lifecycle.clone())
        // Late metadata appends referencing a locally compacted-away
        // segment are translated through the remap alias (g3 Option A —
        // GAP-1 closure).
        .with_remap_alias(Arc::clone(&storage.remap_alias));

        let mut healing_service = oceanfs_durability::healing_service::HealingGrpcService::new(
            hinted_handoff.clone(),
            storage.metadata_store.clone(),
            Arc::clone(&storage.lifecycle_registry),
            storage.data_store.clone(),
            hlc_clock.clone(),
        )
        .with_local_node_id(self_id)
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
        .with_hint_object_applier(Arc::new(oceanfs_server::WriteCoordinatorHintObjectApplier::new(
            write_coordinator_for_applier,
        )))
        // g3 `loss-announcement` Option A (compaction remap): the remap
        // handler records the alias + chunk table so the append handler
        // translates late chunk refs, re-points local rows, and deletes
        // the stale replica through the machine + shard store.
        .with_remap_alias(Arc::clone(&storage.remap_alias))
        .with_lifecycle_coordinator(storage.lifecycle.clone())
        // ADR-0017 amendment: inbound hint batches acquire a Tier-0
        // (repair) permit from the shared budget — the review anchor
        // (per-RPC Semaphore(16)) is closed by this shared cross-RPC gate.
        .with_repair_budget(durability.budget.clone())
        // g3 `loss-announcement` (data-pool death): verified held
        // segments enqueue re-replication repairs — the repair sink is
        // the HOLDER-side dispatcher (ADR-0030 target-pull).
        .with_repair_sink(durability.repair_dispatcher.clone());
        // g5 `request_re_replication` (ADR-0030): the acquiring node's
        // healing service routes incoming re-replication requests into
        // the LOCAL ReRepWorker queue (the worker pulls + writes +
        // stamps). The worker queue sender is guaranteed present before
        // `run` is spawned (the worker is constructed above).
        let rep_worker_queue = durability
            .rep_worker
            .sender()
            .ok_or_else(|| std::io::Error::other("re-replication worker queue unavailable"))
            .map_err(|e| e.to_string())?;
        healing_service = healing_service
            .with_replication_request_sink(Arc::new(WorkerQueueSink { tx: rep_worker_queue }));
        healing_service.register_metrics(&*metrics);
        let cache_service = oceanfs_server::grpc::cache_service::CacheGrpcService::new(
            Some(object_cache),
            Some(metadata_cache),
        );
        let scrub_service = oceanfs_durability::scrub_service::ScrubGrpcService::new(
            Arc::clone(&storage.lifecycle_registry),
            storage.data_store.clone(),
        );

        // Wrap + cap the data-plane services. The default gRPC message
        // limit (4 MiB) rejects hinted-handoff batches: hints carry the
        // blob data inline (the phase-3 churn fix), and batches can
        // reach tens of MiB. The healing service (hint receiver) gets a
        // 64 MiB decode limit — the client-side max_batch_bytes cap (32
        // MiB) keeps batches comfortably inside. The append replication
        // receiver must accept chunks up to max_body_size (16 MiB in
        // the load-test profile): the 4 MiB tonic default made every
        // >4 MiB replica write fail with OutOfRange "decoded message
        // length too large" — the write degraded to the slow hint path
        // and replicas lagged at verify time (fleet read-quorum
        // failures hot-15/hot-57; the 2 MiB local profile never
        // exceeded the default).
        let grpc = DataPlaneServices {
            segment: oceanfs_storage::SegmentRpcServer::new(segment_service)
                .max_decoding_message_size(64 * 1024 * 1024),
            healing: oceanfs_durability::HealingRpcServer::new(healing_service)
                .max_decoding_message_size(64 * 1024 * 1024),
            cache: oceanfs_cache::CacheRpcServer::new(cache_service),
            scrub: oceanfs_durability::ScrubRpcServer::new(scrub_service),
        };

        Ok(ServerModule { router, grpc, prefetch_engine })
    }
}

/// Spawns the server-owned prefetch pre-warmer keep-alive (c5 — the
/// worker owns its startup sequence; the background bundler calls this
/// with the engine Arc, since `ServerModule`'s other fields are moved
/// into the data-plane binds before the bundler runs).
///
/// PrefetchEngine runs its own internal worker (spawned in
/// `PrefetchEngine::new()`); this task holds the engine Arc alive and
/// waits for cancellation — when prefetch is disabled the engine
/// silently drops all queued tasks.
pub(crate) fn spawn_prefetch_loop(
    prefetch_engine: Arc<oceanfs_cache::PrefetchEngine>,
    bg: &mut crate::node::BackgroundTasks,
) {
    use tracing::info;

    let prefetch_token = bg.prefetch_cancel.clone();
    bg.prefetch = Some(tokio::spawn(async move {
        // Hold the engine alive for the lifetime of this task.
        let _engine = prefetch_engine;
        prefetch_token.cancelled().await;
        info!("Prefetch task cancelled");
    }));
}
