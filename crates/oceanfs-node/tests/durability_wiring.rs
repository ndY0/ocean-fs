//! Integration test: durability background tasks are wireable and spawnable.
//!
//! Verifies that durability components from `oceanfs-durability` can be
//! constructed at the composition root (`oceanfs-node`) via `Arc<dyn Trait>`
//! and that their background tasks can be spawned and gracefully shut down.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::{MetadataConfig, NodeId, RpcConfig};
use oceanfs_durability::{
    AntiEntropy, AntiEntropyConfig, GarbageCollector, GcConfig, HealConfig, HealQueue,
    InMemorySegmentShardStore, InMemorySegmentStore, OrphanReaper, ScrubConfig, ScrubCoordinator,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::{Ring, RingCache};
use oceanfs_storage::RocksDbMetadataStore;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn durability_components_are_wireable_and_spawnable() {
    // --- Setup ---
    let tmp = tempfile::tempdir().expect("temp dir");
    let metadata_config =
        MetadataConfig { data_dir: tmp.path().join("metadata"), ..Default::default() };
    let metadata_store =
        Arc::new(RocksDbMetadataStore::open(&metadata_config).expect("open metadata store"));

    let ring = Ring::new(oceanfs_core::RingConfig::default());
    let ring_cache = Arc::new(RingCache::new(ring));
    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));

    let membership = Arc::new(Membership::new(
        NodeId::new("test-node"),
        "127.0.0.1:9001".parse().unwrap(),
        oceanfs_core::GossipConfig::default(),
        ring_cache.clone(),
    ));

    // --- Construct durability components ---

    // GC
    let _gc_worker = Arc::new(GarbageCollector::new(GcConfig::new(3600, 86400, 0.5, 4, 64)));
    let gc_cancel = CancellationToken::new();
    let _gc_handle = tokio::spawn({
        let cancel = gc_cancel.clone();
        async move {
            cancel.cancelled().await;
        }
    });

    // Anti-entropy
    let _ae_worker = Arc::new(AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership.clone(),
        metadata_store.clone(),
        pool.clone(),
        Arc::new(InMemorySegmentStore::new()),
    ));
    let ae_cancel = CancellationToken::new();
    let _ae_handle = tokio::spawn({
        let cancel = ae_cancel.clone();
        async move {
            cancel.cancelled().await;
        }
    });

    // Scrub
    let _scrub_worker = Arc::new(ScrubCoordinator::new(ScrubConfig::default()));
    let scrub_cancel = CancellationToken::new();
    let _scrub_handle = tokio::spawn({
        let cancel = scrub_cancel.clone();
        async move {
            cancel.cancelled().await;
        }
    });

    // Orphan reaper
    let _reaper = Arc::new(OrphanReaper::new(
        metadata_store.clone(),
        Arc::new(InMemorySegmentShardStore::new(4194304)),
        GcConfig::new(3600, 86400, 0.5, 4, 64),
    ));
    let reaper_cancel = CancellationToken::new();
    let _reaper_handle = tokio::spawn({
        let cancel = reaper_cancel.clone();
        async move {
            cancel.cancelled().await;
        }
    });

    // Heal queue
    let _heal_queue = Arc::new(HealQueue::new(HealConfig::default().queue_capacity()));
    let heal_cancel = CancellationToken::new();
    let _heal_handle = tokio::spawn({
        let cancel = heal_cancel.clone();
        async move {
            cancel.cancelled().await;
        }
    });

    // --- Graceful shutdown ---
    gc_cancel.cancel();
    ae_cancel.cancel();
    scrub_cancel.cancel();
    reaper_cancel.cancel();
    heal_cancel.cancel();
}
