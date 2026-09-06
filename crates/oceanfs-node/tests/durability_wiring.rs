//! Integration test: durability background tasks are wireable and spawnable.
//!
//! Verifies that durability components from `oceanfs-durability` can be
//! constructed at the composition root (`oceanfs-node`) via `Arc<dyn Trait>`
//! and that their background tasks can be spawned and gracefully shut down.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::{MetadataConfig, NodeId, RpcConfig, SegmentMetadata};
use oceanfs_durability::{
    merkle::{IncrementalMerkleTree, MerkleTreeConfig},
    peer_selection::PartitionPlanner,
    AntiEntropy, AntiEntropyConfig, GarbageCollector, GcConfig, HealConfig, HealQueue,
    InMemorySegmentStore, OrphanReaper, ScrubConfig, ScrubCoordinator, SegmentPartition,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::{Ring, RingCache};

/// Creates a test IncrementalMerkleTree.
fn make_test_tree() -> Arc<IncrementalMerkleTree> {
    Arc::new(IncrementalMerkleTree::new(MerkleTreeConfig::default()))
}

/// Test scrub planner that keeps every segment in the self partition.
struct LocalPlanner;

impl PartitionPlanner for LocalPlanner {
    fn plan_partitions(
        &self,
        segments: &[SegmentMetadata],
        self_id: &NodeId,
    ) -> Vec<SegmentPartition> {
        vec![SegmentPartition {
            node_id: self_id.clone(),
            segment_ids: segments.iter().map(|s| s.segment_id).collect(),
        }]
    }
}

fn make_coord(config: ScrubConfig) -> ScrubCoordinator {
    ScrubCoordinator::new(config, Arc::new(LocalPlanner), NodeId::new("test-node"))
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
        "127.0.0.1:9001".parse().unwrap(),
        oceanfs_core::GossipConfig::default(),
        ring_cache.clone(),
    ));

    // --- Construct durability components ---
    // f3 shape (ADR-0032 D4): every consumer receives clones of ONE
    // shared store — the composition root constructs once in
    // StorageModule and wires the same Arc into GC, AE, heal, the
    // reaper and scrub.
    let shared_store: Arc<dyn oceanfs_storage_api::SegmentDataStore> =
        Arc::new(InMemorySegmentStore::new());

    // GC
    let _gc_worker = Arc::new(
        GarbageCollector::new(GcConfig::new(3600, 86400, 0.5, 4, 64))
            .with_data_store(Arc::clone(&shared_store)),
    );
    let gc_cancel = CancellationToken::new();
    let _gc_handle = tokio::spawn({
        let cancel = gc_cancel.clone();
        async move {
            cancel.cancelled().await;
        }
    });

    // Anti-entropy
    let wiring_registry =
        Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
            &oceanfs_core::LifecycleConfig::default(),
        ));
    let _ae_worker = Arc::new(AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership.clone(),
        Arc::clone(&wiring_registry),
        pool.clone(),
        Arc::clone(&shared_store),
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
    let _scrub_worker = Arc::new(make_coord(ScrubConfig::default()));
    let scrub_cancel = CancellationToken::new();
    let _scrub_handle = tokio::spawn({
        let cancel = scrub_cancel.clone();
        async move {
            cancel.cancelled().await;
        }
    });

    // Orphan reaper (lifecycle coordinator seeded at startup, as the
    // composition root does)
    let lifecycle =
        Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator::with_registry(
            Arc::clone(&wiring_registry),
        ));
    let _reaper = Arc::new(OrphanReaper::new(
        metadata_store.clone(),
        lifecycle,
        Arc::clone(&shared_store),
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

    // GC accepts Arc<dyn MetadataStore> via coercion; the machine's
    // registry is the segment set (ADR-0025 Decision 3).
    let gc = GarbageCollector::new(GcConfig::default());
    let gc_registry = oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    );
    let stats = gc.run_cycle(store, &gc_registry).await.expect("GC cycle with trait object");
    assert_eq!(stats.segments_scanned, 0);
    assert_eq!(stats.segments_compacted, 0);
}

/// T6.2: `ScrubCoordinator` accepts `Arc<dyn MetadataStore>`.
#[tokio::test]
async fn test_scrub_accepts_trait_object() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let metadata_config =
        MetadataConfig { data_dir: tmp.path().join("metadata"), ..Default::default() };
    let _store: Arc<dyn MetadataStore> =
        Arc::new(RocksDbMetadataStore::open(&metadata_config).expect("open metadata store"));

    let data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore> =
        Arc::new(InMemorySegmentStore::new());
    let coord = make_coord(ScrubConfig::default());
    let scrub_registry =
        Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
            &oceanfs_core::LifecycleConfig::default(),
        ));
    let report =
        coord.run_cycle(Arc::clone(&scrub_registry), data_store).await.expect("scrub cycle");
    assert_eq!(report.segments_total(), 0);
}

/// T6.3: `AntiEntropy::new()` accepts `Arc<dyn MetadataStore>`.
#[tokio::test]
async fn test_anti_entropy_accepts_trait_object() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let metadata_config =
        MetadataConfig { data_dir: tmp.path().join("metadata"), ..Default::default() };
    let _store: Arc<dyn MetadataStore> =
        Arc::new(RocksDbMetadataStore::open(&metadata_config).expect("open metadata store"));

    let ring = Ring::new(oceanfs_core::RingConfig::default());
    let ring_cache = Arc::new(RingCache::new(ring));
    let membership = Arc::new(Membership::new(
        NodeId::new("test-ae-trait"),
        "127.0.0.1:9002".parse().unwrap(),
        "127.0.0.1:9002".parse().unwrap(),
        oceanfs_core::GossipConfig::default(),
        ring_cache.clone(),
    ));
    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    let data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore> =
        Arc::new(InMemorySegmentStore::new());

    // AntiEntropy::new accepts the machine's registry.
    let ae = AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership,
        Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
            &oceanfs_core::LifecycleConfig::default(),
        )),
        pool,
        data_store,
        make_test_tree(),
    );
    let stats = ae.run_cycle().await.expect("AE cycle with trait object");
    assert_eq!(stats.segments_compared, 0);
}

/// Epic README DoD (Integration): a node-crate test exercises the wiring
/// — a sealed segment whose `storage_locations` names one alive+healthy
/// holder and one **Dead** holder is exchanged/scrubbed only with the
/// eligible holder.
///
/// Drives the real composition-root selectors (`ManifestPeerSelector` for
/// AE, `ManifestPartitionPlanner` for scrub) attached to the real workers
/// over a membership with exactly that shape.
#[tokio::test]
async fn holder_aware_wiring_excludes_dead_holder() {
    use oceanfs_core::{Incarnation, NodeState, SegmentId, SegmentMetadata, SizeTier};
    use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
    use oceanfs_node::peer_selection::{ManifestPartitionPlanner, ManifestPeerSelector};

    // --- Membership: self + one alive/healthy holder + one Dead holder.
    let ring = Ring::new(oceanfs_core::RingConfig::default());
    let ring_cache = Arc::new(RingCache::new(ring));
    let membership = Arc::new(Membership::new(
        NodeId::new("test-node"),
        "127.0.0.1:9003".parse().unwrap(),
        "127.0.0.1:9003".parse().unwrap(),
        oceanfs_core::GossipConfig::default(),
        ring_cache.clone(),
    ));
    membership.upsert_node(
        NodeId::new("alive-holder"),
        NodeState::Alive,
        Incarnation::new(1),
        Some("127.0.0.1:9101".parse().unwrap()),
    );
    membership.upsert_node(
        NodeId::new("dead-holder"),
        NodeState::Dead,
        Incarnation::new(1),
        Some("127.0.0.1:9102".parse().unwrap()),
    );
    // Both carry healthy manifests — only the Dead membership state must
    // remove dead-holder from the eligible set.
    let healthy =
        NodeManifest::from_pools(1, &[PoolManifest::new(0, "data", "healthy", false, 1 << 40, 1)]);
    membership.set_peer_manifest(NodeId::new("alive-holder"), healthy.clone());
    membership.set_peer_manifest(NodeId::new("dead-holder"), healthy);

    // --- Registry: one segment shared with both holders + one local-only.
    let registry = Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
        &oceanfs_core::LifecycleConfig::default(),
    ));
    let shared_id = SegmentId::new();
    let local_id = SegmentId::new();
    let shared_meta = SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id: shared_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::smallvec![
            NodeId::new("alive-holder"),
            NodeId::new("dead-holder"),
        ],
        sealed_at: Some(1700000000000),
    };
    let local_meta = SegmentMetadata {
        pool_id: 0,
        total_bytes: 0,
        segment_id: local_id,
        ec_k: 4,
        ec_m: 2,
        size_tier: SizeTier::Standard,
        merkle_root: None,
        storage_locations: smallvec::SmallVec::new(),
        sealed_at: Some(1700000000000),
    };
    registry.reserve(shared_id, shared_meta.clone()).unwrap();
    registry.seal(shared_id, shared_meta.clone()).unwrap();
    registry.reserve(local_id, local_meta.clone()).unwrap();
    registry.seal(local_id, local_meta.clone()).unwrap();

    // --- Anti-entropy wiring: the shared segment is exchanged only with
    // alive-holder; the local-only segment stays local.
    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    let data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore> =
        Arc::new(InMemorySegmentStore::new());
    let ae = AntiEntropy::new(
        AntiEntropyConfig::default(),
        membership.clone(),
        Arc::clone(&registry),
        pool,
        data_store,
        make_test_tree(),
    )
    .with_peer_selector(Arc::new(ManifestPeerSelector::new(
        membership.clone(),
        NodeId::new("test-node"),
    )));

    let (holder_groups, local_only) = ae.holder_exchange_groups();
    assert_eq!(
        holder_groups,
        vec![(NodeId::new("alive-holder"), 1)],
        "AE must exchange the shared segment only with the alive+healthy holder"
    );
    assert!(
        !holder_groups.iter().any(|(id, _)| id.as_str() == "dead-holder"),
        "the Dead holder must never be an AE exchange partner"
    );
    assert_eq!(local_only, 1, "the local-only segment is not remotely exchanged");

    // --- Scrub wiring: the plan never assigns the Dead holder; the
    // shared segment goes to alive-holder, the local-only stays in the
    // self partition.
    let scrub = ScrubCoordinator::new(
        ScrubConfig::default(),
        Arc::new(ManifestPartitionPlanner::new(membership, NodeId::new("test-node"))),
        NodeId::new("test-node"),
    );
    let partitions = scrub.plan_cycle_partitions(&[shared_meta.clone(), local_meta.clone()]);
    assert!(
        !partitions.iter().any(|p| p.node_id.as_str() == "dead-holder"),
        "the Dead holder must never appear as a scrub partition node"
    );

    let mut seen: Vec<SegmentId> =
        partitions.iter().flat_map(|p| p.segment_ids.iter().copied()).collect();
    seen.sort();
    let mut expected = vec![shared_id, local_id];
    expected.sort();
    assert_eq!(seen, expected, "every sealed segment is planned exactly once");

    let alive_partition =
        partitions.iter().find(|p| p.node_id.as_str() == "alive-holder").expect("alive partition");
    assert!(alive_partition.segment_ids.contains(&shared_id));
    let self_partition =
        partitions.iter().find(|p| p.node_id.as_str() == "test-node").expect("self partition");
    assert!(self_partition.segment_ids.contains(&local_id));
}
