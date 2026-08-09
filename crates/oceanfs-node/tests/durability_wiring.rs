//! Integration test: durability background tasks are wireable and spawnable.
//!
//! Verifies that durability components from `oceanfs-durability` can be
//! constructed at the composition root (`oceanfs-node`) via `Arc<dyn Trait>`
//! and that their background tasks can be spawned and gracefully shut down.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::{MetadataConfig, NodeId, RpcConfig};
use oceanfs_durability::{
    merkle::{IncrementalMerkleTree, MerkleTreeConfig, MerkleWal},
    AntiEntropy, AntiEntropyConfig, GarbageCollector, GcConfig, HealConfig, HealQueue,
    InMemorySegmentShardStore, InMemorySegmentStore, OrphanReaper, ScrubConfig, ScrubCoordinator,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::{Ring, RingCache};

/// Creates a test IncrementalMerkleTree backed by a temp MerkleWal.
fn make_test_tree() -> Arc<IncrementalMerkleTree> {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("merkle.wal");
    let wal = Arc::new(MerkleWal::open(&wal_path).unwrap());
    std::mem::forget(dir);
    Arc::new(IncrementalMerkleTree::new(wal, MerkleTreeConfig::default()))
}
use oceanfs_storage::RocksDbMetadataStore;
use oceanfs_storage_api::MetadataStore;
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
        make_test_tree(),
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

/// T6.1: `GarbageCollector::run_cycle()` accepts `Arc<dyn MetadataStore>`.
#[tokio::test]
async fn test_gc_accepts_trait_object() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let metadata_config =
        MetadataConfig { data_dir: tmp.path().join("metadata"), ..Default::default() };
    let store: Arc<dyn MetadataStore> =
        Arc::new(RocksDbMetadataStore::open(&metadata_config).expect("open metadata store"));

    // GC accepts Arc<dyn MetadataStore> via coercion.
    let gc = GarbageCollector::new(GcConfig::default());
    let stats = gc.run_cycle(store).await.expect("GC cycle with trait object");
    assert_eq!(stats.segments_scanned, 0);
    assert_eq!(stats.segments_compacted, 0);
}

/// T6.2: `ScrubCoordinator` accepts `Arc<dyn MetadataStore>`.
#[tokio::test]
async fn test_scrub_accepts_trait_object() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let metadata_config =
        MetadataConfig { data_dir: tmp.path().join("metadata"), ..Default::default() };
    let store: Arc<dyn MetadataStore> =
        Arc::new(RocksDbMetadataStore::open(&metadata_config).expect("open metadata store"));

    let data_store: Arc<dyn oceanfs_durability::SegmentDataStore> =
        Arc::new(InMemorySegmentStore::new());
    let coord = ScrubCoordinator::new(ScrubConfig::default());
    let report = coord.run_cycle(store, data_store).await.expect("scrub with trait object");
    assert_eq!(report.segments_total(), 0);
}

/// T6.3: `AntiEntropy::new()` accepts `Arc<dyn MetadataStore>`.
#[tokio::test]
async fn test_anti_entropy_accepts_trait_object() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let metadata_config =
        MetadataConfig { data_dir: tmp.path().join("metadata"), ..Default::default() };
    let store: Arc<dyn MetadataStore> =
        Arc::new(RocksDbMetadataStore::open(&metadata_config).expect("open metadata store"));

    let ring = Ring::new(oceanfs_core::RingConfig::default());
    let ring_cache = Arc::new(RingCache::new(ring));
    let membership = Arc::new(Membership::new(
        NodeId::new("test-ae-trait"),
        "127.0.0.1:9002".parse().unwrap(),
        oceanfs_core::GossipConfig::default(),
        ring_cache.clone(),
    ));
    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    let data_store: Arc<dyn oceanfs_durability::SegmentDataStore> =
        Arc::new(InMemorySegmentStore::new());

    // AntiEntropy::new accepts Arc<dyn MetadataStore>.
    let ae = AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership,
        store,
        pool,
        data_store,
        make_test_tree(),
    );
    let stats = ae.run_cycle().await.expect("AE cycle with trait object");
    assert_eq!(stats.segments_compared, 0);
}
