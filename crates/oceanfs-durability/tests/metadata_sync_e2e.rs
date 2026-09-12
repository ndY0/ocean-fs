#![allow(clippy::unwrap_used, clippy::expect_used)]
//! In-process multi-node metadata-sync e2e (ADR-0038 D4/D5; ae2 / S2).
//!
//! A responder node (journal + metadata store behind a real tonic
//! `HealingRpc` server) and a consumer node (the `MetadataSync` worker
//! over a real `ConnectionPool` + `Membership`) exercise the full path:
//! FetchJournal → ownership filter → FetchMetadataRows → HLC-LWW apply,
//! plus the gap, kill-switch, and pre-enable-divergence boundaries.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use oceanfs_core::{
    BucketId, GossipConfig, HashOutput, Hlc, HlcClock, Incarnation, LifecycleConfig,
    MetadataConfig, MetadataSyncConfig, NodeId, NodeState, ObjectKey, ObjectMetadata, RingConfig,
    RpcConfig,
};
use oceanfs_durability::{
    healing_service::HealingGrpcService, BootstrapEnqueuer, BootstrapReason, HealingRpcServer,
    HintedHandoff, InMemorySegmentStore, MetadataSync, MetadataSyncService, WatermarkStore,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::{hash_key, Ring, RingCache};
use oceanfs_storage::{MetadataJournal, RocksDbMetadataStore};
use oceanfs_storage_api::MetadataStore;
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;

const BUCKET: &str = "default";
const KEY: &str = "sync-me";

struct TestNode {
    node_id: NodeId,
    metadata_dir: TempDir,
    journal_dir: TempDir,
    store: Arc<RocksDbMetadataStore>,
    journal: Arc<MetadataJournal>,
}

impl TestNode {
    fn new(node_id: &str, with_journal: bool) -> Self {
        let metadata_dir = tempfile::tempdir().unwrap();
        let journal_dir = tempfile::tempdir().unwrap();
        let config = MetadataConfig {
            data_dir: metadata_dir.path().to_path_buf(),
            block_cache_size: 4 * 1024 * 1024,
            memtable_size: 4 * 1024 * 1024,
            max_open_files: 512,
            ..Default::default()
        };
        let journal = if with_journal {
            Some(Arc::new(
                MetadataJournal::open(journal_dir.path(), &NodeId::new(node_id)).unwrap(),
            ))
        } else {
            None
        };
        let store =
            Arc::new(RocksDbMetadataStore::open_with_journal(&config, journal.clone()).unwrap());
        let journal = journal.unwrap_or_else(|| {
            // A service still needs a journal handle; an enabled responder
            // with no journal attached serves an empty stream.
            Arc::new(MetadataJournal::open(journal_dir.path(), &NodeId::new(node_id)).unwrap())
        });
        Self { node_id: NodeId::new(node_id), metadata_dir, journal_dir, store, journal }
    }

    fn service(&self, watermarks: Arc<WatermarkStore>, metadata_sync: bool) -> HealingGrpcService {
        let handoff = Arc::new(HintedHandoff::new_with_pool(Arc::new(ConnectionPool::new(
            RpcConfig::default(),
        ))));
        let data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore> =
            Arc::new(InMemorySegmentStore::new());
        let service = HealingGrpcService::new(
            handoff,
            Arc::clone(&self.store) as Arc<dyn MetadataStore>,
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &LifecycleConfig::default(),
            )),
            data_store,
            Arc::new(HlcClock::new()),
        )
        .with_local_node_id(self.node_id.clone());
        if metadata_sync {
            service.with_metadata_sync(MetadataSyncService::new(
                Arc::clone(&self.journal),
                watermarks,
                Arc::clone(&self.store) as Arc<dyn MetadataStore>,
            ))
        } else {
            service
        }
    }
}

async fn serve(service: HealingGrpcService) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(HealingRpcServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

fn consumer_membership(self_id: &str, peer_id: &str, peer_addr: SocketAddr) -> Arc<Membership> {
    let ring = Ring::new(RingConfig::default());
    let ring_cache = Arc::new(RingCache::new(ring));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let membership = Arc::new(Membership::new(
        NodeId::new(self_id),
        addr,
        addr,
        GossipConfig::default(),
        ring_cache,
    ));
    // In production the plane's join registers self; in this harness both
    // self and the peer must be in the ring for ownership lookups.
    membership.upsert_node(NodeId::new(self_id), NodeState::Alive, Incarnation::new(1), Some(addr));
    membership.upsert_node(
        NodeId::new(peer_id),
        NodeState::Alive,
        Incarnation::new(1),
        Some(peer_addr),
    );
    membership
}

fn worker(
    membership: Arc<Membership>,
    store: Arc<RocksDbMetadataStore>,
    journal: Arc<MetadataJournal>,
    watermarks: Arc<WatermarkStore>,
) -> MetadataSync {
    let mut config = MetadataSyncConfig::default();
    config.enabled = true;
    config.max_inflight_pulls = 2;
    worker_with_config(membership, store, journal, watermarks, config)
}

fn worker_with_config(
    membership: Arc<Membership>,
    store: Arc<RocksDbMetadataStore>,
    journal: Arc<MetadataJournal>,
    watermarks: Arc<WatermarkStore>,
    config: MetadataSyncConfig,
) -> MetadataSync {
    let worker = MetadataSync::new(
        NodeId::new("node-b"),
        &config,
        membership,
        Arc::new(ConnectionPool::new(RpcConfig::default())),
        Duration::from_secs(5),
        store,
        journal,
        watermarks,
    )
    .unwrap();
    worker
}

fn object(key: &str, hlc: Hlc) -> ObjectMetadata {
    ObjectMetadata {
        object_key: ObjectKey::new(key),
        size: 5,
        blake3_hash: Some(HashOutput::from_bytes([7u8; 32])),
        chunks: smallvec::SmallVec::new(),
        inline_data: Some(bytes::Bytes::from_static(b"hello")),
        created_at: 1_700_000_000_000,
        hlc,
    }
}

#[derive(Default)]
struct RecordingBootstrap {
    calls: Mutex<Vec<(NodeId, BootstrapReason)>>,
}

impl BootstrapEnqueuer for RecordingBootstrap {
    fn enqueue(&self, peer: &NodeId, reason: BootstrapReason) {
        self.calls.lock().unwrap().push((peer.clone(), reason));
    }
}

/// The DoD scenario: a row missed on the consumer heals through the
/// journal path with no scan anywhere.
#[tokio::test]
async fn missed_row_heals_through_journal_pull_and_point_fetch() {
    let responder = TestNode::new("node-a", true);
    let responder_watermarks =
        Arc::new(WatermarkStore::open(&responder.metadata_dir.path().join("watermarks")).unwrap());
    let addr = serve(responder.service(Arc::clone(&responder_watermarks), true)).await;
    responder
        .store
        .put_object_in_bucket(&BucketId::new(BUCKET), object(KEY, Hlc::new(100, 0)))
        .unwrap();

    let consumer = TestNode::new("node-b", true);
    let watermarks =
        Arc::new(WatermarkStore::open(&consumer.metadata_dir.path().join("watermarks")).unwrap());
    let membership = consumer_membership("node-b", "node-a", addr);
    let sync = Arc::new(worker(
        membership,
        Arc::clone(&consumer.store),
        Arc::clone(&consumer.journal),
        Arc::clone(&watermarks),
    ));

    // Nothing on the consumer before the cycle.
    assert!(consumer
        .store
        .get_object(&BucketId::new(BUCKET), &ObjectKey::new(KEY))
        .unwrap()
        .is_none());

    let peers = sync.run_cycle_once().await.unwrap();
    assert_eq!(peers, 1, "one peer contacted");

    let applied = consumer
        .store
        .get_object(&BucketId::new(BUCKET), &ObjectKey::new(KEY))
        .unwrap()
        .expect("the missed row was pulled and applied");
    assert_eq!(applied.hlc, Hlc::new(100, 0));
    assert_eq!(applied.inline_data.as_deref(), Some(&b"hello"[..]));

    let metrics = sync.metrics();
    assert!(metrics.entries_consumed_total.get() >= 1);
    assert_eq!(metrics.rows_applied_total.get(), 1);

    // The applied state is journaled on the consumer too (capture at the
    // store choke point), so its own journal covers the restored state.
    let mut journal_entries = Vec::new();
    if let oceanfs_storage::JournalRead::Entries { entries, .. } =
        consumer.journal.read_range(0, 100, u64::MAX).unwrap()
    {
        journal_entries = entries;
    }
    assert_eq!(journal_entries.len(), 1);
    assert_eq!(journal_entries[0].op, oceanfs_storage::JournalOp::Put);

    let watermark = watermarks.get(&NodeId::new("node-a"));
    assert_eq!(watermark.consumed, 1, "consumed advanced");
}

/// A missed DELETE converges to the tombstone; a re-PUT of the old live
/// row never resurrects it.
#[tokio::test]
async fn missed_delete_converges_and_never_resurrects() {
    let responder = TestNode::new("node-a", true);
    let responder_watermarks =
        Arc::new(WatermarkStore::open(&responder.metadata_dir.path().join("watermarks")).unwrap());
    let addr = serve(responder.service(Arc::clone(&responder_watermarks), true)).await;
    let bucket = BucketId::new(BUCKET);
    let key = ObjectKey::new(KEY);
    responder.store.put_object_in_bucket(&bucket, object(KEY, Hlc::new(10, 0))).unwrap();
    responder.store.delete_object(&bucket, &key, Hlc::new(11, 0)).unwrap();

    let consumer = TestNode::new("node-b", true);
    let watermarks =
        Arc::new(WatermarkStore::open(&consumer.metadata_dir.path().join("watermarks")).unwrap());
    let sync = Arc::new(worker(
        consumer_membership("node-b", "node-a", addr),
        Arc::clone(&consumer.store),
        Arc::clone(&consumer.journal),
        Arc::clone(&watermarks),
    ));

    sync.run_cycle_once().await.unwrap();
    assert!(consumer.store.get_object(&bucket, &key).unwrap().is_none());
    let tombstone = consumer.store.get_tombstone(&bucket, &key).unwrap().unwrap();
    assert_eq!(tombstone.hlc, Hlc::new(11, 0));

    // A stale re-PUT of the pre-delete version must not resurrect it.
    let mut stale = object(KEY, Hlc::new(10, 0));
    stale.hlc = Hlc::new(10, 0);
    assert_eq!(
        consumer.store.sync_apply_object(&bucket, stale).unwrap(),
        oceanfs_storage::SyncApplyOutcome::LocalWins
    );
    assert!(consumer.store.get_object(&bucket, &key).unwrap().is_none());
}

/// J2: an entry whose row never committed (crash between journal fsync
/// and row commit) is a consumed no-op — the consumer fetches, finds
/// nothing, and advances without error.
#[tokio::test]
async fn orphan_journal_entry_is_a_consumed_noop() {
    let responder = TestNode::new("node-a", true);
    let responder_watermarks =
        Arc::new(WatermarkStore::open(&responder.metadata_dir.path().join("watermarks")).unwrap());
    let addr = serve(responder.service(Arc::clone(&responder_watermarks), true)).await;
    // Journal an entry with no matching row (the J2 crash window).
    responder
        .journal
        .append(oceanfs_storage::JournalOp::Put, b"default\0ghost", Hlc::new(50, 0))
        .unwrap();

    let consumer = TestNode::new("node-b", true);
    let watermarks =
        Arc::new(WatermarkStore::open(&consumer.metadata_dir.path().join("watermarks")).unwrap());
    let bootstrap = Arc::new(RecordingBootstrap::default());
    let sync = Arc::new(
        worker(
            consumer_membership("node-b", "node-a", addr),
            Arc::clone(&consumer.store),
            Arc::clone(&consumer.journal),
            Arc::clone(&watermarks),
        )
        .with_bootstrap(Arc::clone(&bootstrap) as Arc<dyn BootstrapEnqueuer>),
    );

    sync.run_cycle_once().await.unwrap();
    assert!(consumer
        .store
        .get_object(&BucketId::new(BUCKET), &ObjectKey::new("ghost"))
        .unwrap()
        .is_none());
    assert_eq!(watermarks.get(&NodeId::new("node-a")).consumed, 1, "the no-op entry was consumed");
    assert!(bootstrap.calls.lock().unwrap().is_empty(), "no gap, no bootstrap");
}

/// A responder epoch change with prior consumer progress is a gap: the
/// signal is recorded and a bootstrap is enqueued — never a silent skip.
#[tokio::test]
async fn epoch_change_after_progress_records_gap_and_enqueues_bootstrap() {
    let responder = TestNode::new("node-a", true);
    let responder_watermarks =
        Arc::new(WatermarkStore::open(&responder.metadata_dir.path().join("watermarks")).unwrap());
    let addr = serve(responder.service(Arc::clone(&responder_watermarks), true)).await;
    responder
        .store
        .put_object_in_bucket(&BucketId::new(BUCKET), object(KEY, Hlc::new(20, 0)))
        .unwrap();

    let consumer = TestNode::new("node-b", true);
    let watermarks =
        Arc::new(WatermarkStore::open(&consumer.metadata_dir.path().join("watermarks")).unwrap());
    let bootstrap = Arc::new(RecordingBootstrap::default());
    let sync = Arc::new(
        worker(
            consumer_membership("node-b", "node-a", addr),
            Arc::clone(&consumer.store),
            Arc::clone(&consumer.journal),
            Arc::clone(&watermarks),
        )
        .with_bootstrap(Arc::clone(&bootstrap) as Arc<dyn BootstrapEnqueuer>),
    );

    // First cycle: progress.
    sync.run_cycle_once().await.unwrap();
    assert!(watermarks.get(&NodeId::new("node-a")).consumed > 0);

    // The responder loses its journal (pool/journal loss) and starts a
    // new epoch on a fresh directory.
    std::fs::remove_dir_all(responder.journal_dir.path()).unwrap();
    let fresh =
        Arc::new(MetadataJournal::open(responder.journal_dir.path(), &responder.node_id).unwrap());
    let addr2 = serve(
        HealingGrpcService::new(
            Arc::new(HintedHandoff::new_with_pool(Arc::new(ConnectionPool::new(
                RpcConfig::default(),
            )))),
            Arc::clone(&responder.store) as Arc<dyn MetadataStore>,
            Arc::new(oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry::new(
                &LifecycleConfig::default(),
            )),
            Arc::new(InMemorySegmentStore::new()),
            Arc::new(HlcClock::new()),
        )
        .with_metadata_sync(MetadataSyncService::new(
            fresh,
            Arc::clone(&responder_watermarks),
            Arc::clone(&responder.store) as Arc<dyn MetadataStore>,
        )),
    )
    .await;

    let consumer2 = TestNode::new("node-b2", true);
    let watermarks2 =
        Arc::new(WatermarkStore::open(&consumer2.metadata_dir.path().join("watermarks")).unwrap());
    // Reuse the already-advanced watermark state by copying it.
    let previous = watermarks.get(&NodeId::new("node-a"));
    watermarks2.set_consumed(&NodeId::new("node-a"), previous.epoch.unwrap(), previous.consumed);
    let bootstrap2 = Arc::new(RecordingBootstrap::default());
    let sync2 = Arc::new(
        worker(
            consumer_membership("node-b2", "node-a", addr2),
            Arc::clone(&consumer2.store),
            Arc::clone(&consumer2.journal),
            Arc::clone(&watermarks2),
        )
        .with_bootstrap(Arc::clone(&bootstrap2) as Arc<dyn BootstrapEnqueuer>),
    );

    sync2.run_cycle_once().await.unwrap();
    assert!(sync2.metrics().gap_detected_total.get() > 0, "the epoch change was detected");
    assert_eq!(bootstrap2.calls.lock().unwrap().len(), 1, "a bootstrap was enqueued for the gap");
}

/// Mixed kill-switch mesh: a disabled peer answers `unavailable`, is
/// skipped without error, and does not pin the trim frontier.
#[tokio::test]
async fn disabled_peer_is_skipped_and_excluded_from_the_frontier() {
    let disabled = TestNode::new("node-a", false);
    let disabled_watermarks =
        Arc::new(WatermarkStore::open(&disabled.metadata_dir.path().join("watermarks")).unwrap());
    let addr = serve(disabled.service(Arc::clone(&disabled_watermarks), false)).await;

    let consumer = TestNode::new("node-b", true);
    let watermarks =
        Arc::new(WatermarkStore::open(&consumer.metadata_dir.path().join("watermarks")).unwrap());
    let sync = Arc::new(worker(
        consumer_membership("node-b", "node-a", addr),
        Arc::clone(&consumer.store),
        Arc::clone(&consumer.journal),
        Arc::clone(&watermarks),
    ));

    sync.run_cycle_once().await.unwrap();
    assert_eq!(sync.metrics().gap_detected_total.get(), 0, "disabled is not a gap");
    assert!(
        watermarks
            .frontier(&[NodeId::new("node-a")], *consumer.journal.epoch().as_bytes())
            .is_none(),
        "a disabled peer does not pin the frontier"
    );
}

/// Pre-enable divergence is NOT healed by the change edge (S2 boundary):
/// a row written before the journal existed has no entry, so no pull can
/// discover it. The S3 enable-time bootstrap is the healing path.
#[tokio::test]
async fn pre_enable_divergence_is_not_healed_by_s2() {
    // The responder's row was written with NO journal attached.
    let responder = TestNode::new("node-a", false);
    responder
        .store
        .put_object_in_bucket(&BucketId::new(BUCKET), object(KEY, Hlc::new(30, 0)))
        .unwrap();
    let responder_watermarks =
        Arc::new(WatermarkStore::open(&responder.metadata_dir.path().join("watermarks")).unwrap());
    let addr = serve(responder.service(Arc::clone(&responder_watermarks), true)).await;

    let consumer = TestNode::new("node-b", true);
    let watermarks =
        Arc::new(WatermarkStore::open(&consumer.metadata_dir.path().join("watermarks")).unwrap());
    let sync = Arc::new(worker(
        consumer_membership("node-b", "node-a", addr),
        Arc::clone(&consumer.store),
        Arc::clone(&consumer.journal),
        Arc::clone(&watermarks),
    ));

    sync.run_cycle_once().await.unwrap();
    assert!(
        consumer.store.get_object(&BucketId::new(BUCKET), &ObjectKey::new(KEY)).unwrap().is_none(),
        "the change edge only heals changes applied after enablement"
    );
    assert_eq!(sync.metrics().entries_consumed_total.get(), 0);
}

/// Bidirectional watermark exchange (ADR-0038 D3): a pull carries the
/// requester's `consumed(responder)` as an ack, and the response carries
/// the responder's `consumed(requester)` back — both stores advance
/// without a third RPC.
#[tokio::test]
async fn bidirectional_acks_advance_both_watermark_stores() {
    let responder = TestNode::new("node-a", true);
    let responder_watermarks =
        Arc::new(WatermarkStore::open(&responder.metadata_dir.path().join("watermarks")).unwrap());
    let addr = serve(responder.service(Arc::clone(&responder_watermarks), true)).await;
    responder
        .store
        .put_object_in_bucket(&BucketId::new(BUCKET), object(KEY, Hlc::new(90, 0)))
        .unwrap();

    let consumer = TestNode::new("node-b", true);
    let watermarks =
        Arc::new(WatermarkStore::open(&consumer.metadata_dir.path().join("watermarks")).unwrap());
    let sync = Arc::new(worker(
        consumer_membership("node-b", "node-a", addr),
        Arc::clone(&consumer.store),
        Arc::clone(&consumer.journal),
        Arc::clone(&watermarks),
    ));

    // Pull 1: consumes seq 1; from_seq was 0, so no ack is recorded yet.
    sync.run_cycle_once().await.unwrap();
    assert_eq!(watermarks.get(&NodeId::new("node-a")).consumed, 1);

    // Pull 2: from_seq = 1 -> the responder records acked(node-b) = 1.
    sync.run_cycle_once().await.unwrap();
    assert!(
        responder_watermarks.get(&NodeId::new("node-b")).acked >= 1,
        "the responder recorded the requester's ack"
    );

    // The responder's own consumed(requester) rides the response back:
    // the requester's acked(responder) advances.
    // A's consumed(B) refers to B's journal epoch; the response carries
    // that epoch so B can validate the ack.
    responder_watermarks.set_consumed(
        &NodeId::new("node-b"),
        *consumer.journal.epoch().as_bytes(),
        7,
    );
    sync.run_cycle_once().await.unwrap();
    assert_eq!(
        watermarks.get(&NodeId::new("node-a")).acked,
        7,
        "the requester learned the responder's consumed(requester)"
    );
}

/// Ownership filter: a key this node does not currently co-own is skipped
/// (`keys_skipped_total{not_owner}`), never fetched or applied.
#[tokio::test]
async fn non_owned_keys_are_skipped_not_applied() {
    let responder = TestNode::new("node-a", true);
    let responder_watermarks =
        Arc::new(WatermarkStore::open(&responder.metadata_dir.path().join("watermarks")).unwrap());
    let addr = serve(responder.service(Arc::clone(&responder_watermarks), true)).await;

    // A four-node ring: node-b owns only a subset of the keyspace.
    let ring_cache = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
    let self_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let membership = Arc::new(Membership::new(
        NodeId::new("node-b"),
        self_addr,
        self_addr,
        GossipConfig::default(),
        ring_cache,
    ));
    for id in ["node-a", "node-b", "node-c", "node-d"] {
        let peer_addr = if id == "node-a" { Some(addr) } else { None };
        membership.upsert_node(NodeId::new(id), NodeState::Alive, Incarnation::new(1), peer_addr);
    }
    let unowned = (0..100_000u32)
        .map(|index| format!("unowned-{index}"))
        .find(|candidate| {
            let holders = membership.ring().lookup(&hash_key(candidate.as_bytes()));
            !holders.contains(&NodeId::new("node-b"))
        })
        .expect("a key node-b does not own");
    responder
        .store
        .put_object_in_bucket(&BucketId::new(BUCKET), object(&unowned, Hlc::new(95, 0)))
        .unwrap();

    let consumer = TestNode::new("node-b", true);
    let watermarks =
        Arc::new(WatermarkStore::open(&consumer.metadata_dir.path().join("watermarks")).unwrap());
    let sync = Arc::new(worker(
        membership,
        Arc::clone(&consumer.store),
        Arc::clone(&consumer.journal),
        Arc::clone(&watermarks),
    ));

    sync.run_cycle_once().await.unwrap();
    assert!(sync.metrics().keys_skipped_not_owner.get() >= 1, "the unowned key was skipped");
    assert!(
        consumer
            .store
            .get_object(&BucketId::new(BUCKET), &ObjectKey::new(&unowned))
            .unwrap()
            .is_none(),
        "an unowned key is never applied"
    );
}

/// A byte-capped point-fetch stream must not consume keys it never
/// delivered: the watermark stops below the first unfetched entry and the
/// next cycle retries it.
#[tokio::test]
async fn byte_capped_point_fetch_holds_the_watermark_for_unfetched_keys() {
    let responder = TestNode::new("node-a", true);
    let responder_watermarks =
        Arc::new(WatermarkStore::open(&responder.metadata_dir.path().join("watermarks")).unwrap());
    let addr = serve(responder.service(Arc::clone(&responder_watermarks), true)).await;
    let big = |key: &str, hlc: Hlc| ObjectMetadata {
        object_key: ObjectKey::new(key),
        size: 200_000,
        blake3_hash: Some(HashOutput::from_bytes([1u8; 32])),
        chunks: smallvec::SmallVec::new(),
        inline_data: Some(bytes::Bytes::from(vec![0xAB; 200_000])),
        created_at: 0,
        hlc,
    };
    responder
        .store
        .put_object_in_bucket(&BucketId::new(BUCKET), big("cap-1", Hlc::new(100, 0)))
        .unwrap();
    responder
        .store
        .put_object_in_bucket(&BucketId::new(BUCKET), big("cap-2", Hlc::new(101, 0)))
        .unwrap();

    let consumer = TestNode::new("node-b", true);
    let watermarks =
        Arc::new(WatermarkStore::open(&consumer.metadata_dir.path().join("watermarks")).unwrap());
    let mut config = MetadataSyncConfig::default();
    config.enabled = true;
    // Cap the stream at the 64 KiB floor so the first 200 KB row
    // truncates the response.
    config.max_bytes_per_cycle = 1;
    let sync = Arc::new(worker_with_config(
        consumer_membership("node-b", "node-a", addr),
        Arc::clone(&consumer.store),
        Arc::clone(&consumer.journal),
        Arc::clone(&watermarks),
        config,
    ));

    sync.run_cycle_once().await.unwrap();
    assert_eq!(
        watermarks.get(&NodeId::new("node-a")).consumed,
        1,
        "the watermark stops below the unfetched second key"
    );
    assert!(consumer
        .store
        .get_object(&BucketId::new(BUCKET), &ObjectKey::new("cap-1"))
        .unwrap()
        .is_some());
    assert!(consumer
        .store
        .get_object(&BucketId::new(BUCKET), &ObjectKey::new("cap-2"))
        .unwrap()
        .is_none());

    sync.run_cycle_once().await.unwrap();
    assert_eq!(watermarks.get(&NodeId::new("node-a")).consumed, 2);
    assert!(consumer
        .store
        .get_object(&BucketId::new(BUCKET), &ObjectKey::new("cap-2"))
        .unwrap()
        .is_some());
}
