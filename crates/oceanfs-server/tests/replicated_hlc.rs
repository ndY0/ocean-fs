//! Integration test: replicated metadata carries the coordinator's HLC
//! end to end (hlc-causality-closure G2/G3/G4).
//!
//! ## Test Flow
//!
//! 1. Build a 3-node ring and a `WriteCoordinator` for node n1.
//! 2. Spawn real gRPC `SegmentGrpcService` servers for n2 and n3, each
//!    backed by its own real RocksDB metadata store + HLC clock.
//! 3. PUT through the coordinator with quorum 2.
//! 4. Assert the replica's persisted `ObjectMetadata.hlc` equals the
//!    coordinator's stamped `WriteResult.hlc` (not zero), and the
//!    replica's clock merged the remote timestamp (receive rule).
//!
//! A second test performs concurrent same-key writes from two different
//! coordinators and asserts the causality substrate: every replica
//! carries a non-zero coordinator-stamped HLC, and the LWW winner is
//! deterministically the maximum of the two stamped HLCs.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use oceanfs_core::{
    BucketId, ConflictResolver, GossipConfig, HashKey, Hlc, HlcClock, Incarnation, LwwResolver,
    MetadataConfig, NodeId, NodeState, ObjectKey, PoolConfig, Resolution, RingConfig, RpcConfig,
    SegmentSizeConfig, SizeTier, WalConfig,
};
use oceanfs_durability::SegmentDataStore;
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_routing::{hash_key, Ring, RingCache};
use oceanfs_server::{grpc::segment_service::SegmentGrpcService, WriteCoordinator, WriteRequest};
use oceanfs_storage::{
    BufferPool, Error as StorageError, RocksDbMetadataStore, SealConfig, SegmentPool,
    SegmentRpcClient, SegmentRpcServer, SegmentSealer, SegmentShard, WalWriter,
};
use tonic::transport::Server;

// In-memory segment data store for the replica servers.
struct InMemorySegments {
    data: parking_lot::Mutex<HashMap<oceanfs_core::SegmentId, Bytes>>,
}

impl InMemorySegments {
    fn new() -> Self {
        Self { data: parking_lot::Mutex::new(HashMap::new()) }
    }
}

impl SegmentDataStore for InMemorySegments {
    fn write_segment_data(
        &self,
        segment_id: &oceanfs_core::SegmentId,
        data: &[u8],
    ) -> Result<(), StorageError> {
        self.data.lock().insert(*segment_id, Bytes::copy_from_slice(data));
        Ok(())
    }

    fn read_segment_data(
        &self,
        segment_id: &oceanfs_core::SegmentId,
    ) -> Result<Bytes, StorageError> {
        self.data.lock().get(segment_id).cloned().ok_or(StorageError::SegmentNotFound(*segment_id))
    }
}

/// A spawned replica: gRPC server backed by a real RocksDB store.
struct Replica {
    _client: SegmentRpcClient<tonic::transport::Channel>,
    store: Arc<RocksDbMetadataStore>,
    clock: Arc<HlcClock>,
}

async fn spawn_replica() -> (SocketAddr, Replica) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        RocksDbMetadataStore::open(&MetadataConfig {
            data_dir: dir.path().join("meta"),
            block_cache_size: 1024,
            memtable_size: 1024,
            ..Default::default()
        })
        .unwrap(),
    );
    spawn_replica_with_store(store).await
}

/// Spawns a replica gRPC server backed by a caller-provided store, so a
/// node's coordinator-local store and its replica-serving store are the
/// SAME RocksDB instance (as in production).
async fn spawn_replica_with_store(store: Arc<RocksDbMetadataStore>) -> (SocketAddr, Replica) {
    let clock = Arc::new(HlcClock::new());
    let service = SegmentGrpcService::new(
        Arc::new(InMemorySegments::new()),
        Some(store.clone()),
        Arc::new(BufferPool::new(65536, 1024)),
        clock.clone(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(SegmentRpcServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (addr, Replica { _client: SegmentRpcClient::new(channel), store, clock })
}

struct Coord {
    coord: WriteCoordinator,
    membership: Arc<Membership>,
}

async fn make_coordinator(
    node_id: &str,
    nodes: &[&str],
    local_store: Arc<RocksDbMetadataStore>,
) -> Coord {
    let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
    for n in nodes {
        ring.add_node(NodeId::new(*n));
    }
    let ring_cache = Arc::new(RingCache::new(ring));
    let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let membership = Arc::new(Membership::new(
        NodeId::new(node_id),
        addr,
        GossipConfig::default(),
        ring_cache.clone(),
    ));
    for n in nodes {
        membership.upsert_node(NodeId::new(*n), NodeState::Alive, Incarnation::new(1), Some(addr));
    }
    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    let hlc_clock = Arc::new(HlcClock::new());

    // Segment pipeline (in-memory / temp dir).
    let dir = tempfile::tempdir().unwrap();
    let size_config = SegmentSizeConfig::default();
    let buffer_pool = Arc::new(BufferPool::new(65536, 16));
    let shard_small =
        Arc::new(SegmentShard::new(4, SizeTier::Small, &size_config, &buffer_pool).unwrap());
    let shard_standard =
        Arc::new(SegmentShard::new(4, SizeTier::Standard, &size_config, &buffer_pool).unwrap());
    let pool_cfg = PoolConfig::default();
    let segment_pool_small = Arc::new(
        SegmentPool::new(
            pool_cfg.clone(),
            SizeTier::Small,
            &size_config,
            buffer_pool.clone(),
            None,
            None,
        )
        .unwrap(),
    );
    let segment_pool_standard = Arc::new(
        SegmentPool::new(pool_cfg, SizeTier::Standard, &size_config, buffer_pool, None, None)
            .unwrap(),
    );
    let wal = Arc::new(
        WalWriter::open(&WalConfig {
            data_dir: dir.path().join("wal"),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            ..Default::default()
        })
        .await
        .unwrap(),
    );
    let seal_config = SealConfig {
        target_size_bytes: size_config.default_target_size,
        seal_timeout_ms: 5000,
        data_dir: dir.path().join("segments"),
        io_mode: oceanfs_storage::io::IoReadMode::Buffered,
        write_mode: oceanfs_storage::io::SegmentWriteMode::Rename,
        ..Default::default()
    };
    let sealer = Arc::new(SegmentSealer::new(seal_config, local_store.clone(), wal));

    let hints_dir = dir.path().join("hints");
    let delivery_client: Arc<dyn oceanfs_durability::HintDeliveryClient> =
        Arc::new(oceanfs_durability::GrpcHintDeliveryClient::new(pool.clone()));
    let hint_config = oceanfs_durability::HintedHandoffConfig {
        wal_dir: hints_dir.clone(),
        ..Default::default()
    };
    let hinted_handoff = Arc::new(oceanfs_durability::HintedHandoffManager::new(
        hints_dir,
        delivery_client,
        hint_config.clone(),
    ));

    let coord = WriteCoordinator::new(
        ring_cache,
        membership.clone(),
        pool,
        NodeId::new(node_id),
        hlc_clock,
        local_store,
        size_config,
        shard_small,
        shard_standard,
        segment_pool_small,
        segment_pool_standard,
        sealer,
        hinted_handoff,
        hint_config,
    );
    Coord { coord, membership }
}

fn write_request(key: &str, data: &[u8], quorum: u8) -> WriteRequest {
    WriteRequest {
        bucket: BucketId::new("test"),
        key: ObjectKey::new(key),
        hash_key: HashKey::from_bytes(hash_key(key.as_bytes())),
        data: Bytes::copy_from_slice(data),
        write_quorum: quorum,
        ack_after_wal: true,
        ec_async: false,
        policy: None,
    }
}

// ---------------------------------------------------------------------------
// DoD: 3-node e2e — replicated metadata carries the coordinator's HLC.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replicated_put_persists_coordinator_hlc_on_replica() {
    let local_store = {
        let dir = tempfile::tempdir().unwrap();
        Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: dir.path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        )
    };
    let coord = make_coordinator("n1", &["n1", "n2", "n3"], local_store).await;

    // Spawn real replica servers for n2 and n3.
    let (addr2, replica2) = spawn_replica().await;
    let (addr3, _replica3) = spawn_replica().await;
    coord.membership.upsert_node(
        NodeId::new("n2"),
        NodeState::Alive,
        Incarnation::new(1),
        Some(addr2),
    );
    coord.membership.upsert_node(
        NodeId::new("n3"),
        NodeState::Alive,
        Incarnation::new(1),
        Some(addr3),
    );

    let result = coord.coord.put(write_request("hlc-obj", b"replicated payload", 2)).await.unwrap();
    let stamped = result.hlc;
    assert!(stamped > Hlc::zero(), "coordinator must stamp a real HLC: {stamped:?}");

    // The replica (n2) must have persisted the SAME timestamp (G3).
    let replicated = replica2
        .store
        .get_object(&BucketId::new("test"), &ObjectKey::new("hlc-obj"))
        .unwrap()
        .expect("replicated metadata must exist on the replica");
    assert_eq!(replicated.hlc, stamped, "replica must persist the coordinator's HLC, not zero",);

    // The replica's clock must have merged the remote timestamp (G2) —
    // its wall time never lags the write's wall time.
    assert!(
        replica2.clock.now().wall_time() >= stamped.wall_time(),
        "replica clock must have merged the remote HLC",
    );
}

// ---------------------------------------------------------------------------
// DoD: T45-style — concurrent same-key writes from two nodes converge on a
// deterministic LWW winner whose HLC is the max of the stamped HLCs.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_same_key_writes_stamp_deterministic_lww_winner() {
    // Three replica stores — one per node — each also serving as the
    // coordinator-local store of its node.
    let make_store = || {
        let dir = tempfile::tempdir().unwrap();
        Arc::new(
            RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: dir.path().join("meta"),
                block_cache_size: 1024,
                memtable_size: 1024,
                ..Default::default()
            })
            .unwrap(),
        )
    };
    let store1 = make_store();
    let store2 = make_store();
    let store3 = make_store();

    let coord1 = make_coordinator("n1", &["n1", "n2", "n3"], store1.clone()).await;
    let coord2 = make_coordinator("n2", &["n1", "n2", "n3"], store2.clone()).await;

    // Each node's gRPC server is backed by the SAME store the node's
    // coordinator writes locally — one store per node, as in production.
    let (addr1, _replica1) = spawn_replica_with_store(store1.clone()).await;
    let (addr2, _replica2) = spawn_replica_with_store(store2.clone()).await;
    let (addr3, _replica3) = spawn_replica_with_store(store3.clone()).await;
    for (member, addr) in [
        (&coord1.membership, [(NodeId::new("n2"), addr2), (NodeId::new("n3"), addr3)]),
        (&coord2.membership, [(NodeId::new("n1"), addr1), (NodeId::new("n3"), addr3)]),
    ] {
        for (node, addr) in addr {
            member.upsert_node(node, NodeState::Alive, Incarnation::new(1), Some(addr));
        }
    }

    let body_a: &[u8] = b"Version from node n1";
    let body_b: &[u8] = b"Version from node n2";
    let key = "same-key";

    // Concurrent writes from two different coordinators to the same key.
    let (result_a, result_b) = tokio::join!(
        coord1.coord.put(write_request(key, body_a, 2)),
        coord2.coord.put(write_request(key, body_b, 2)),
    );
    let result_a = result_a.unwrap();
    let result_b = result_b.unwrap();
    let hlc_a = result_a.hlc;
    let hlc_b = result_b.hlc;
    assert!(hlc_a > Hlc::zero(), "node n1 must stamp a real HLC");
    assert!(hlc_b > Hlc::zero(), "node n2 must stamp a real HLC");

    // Every replica carries one of the two coordinator-stamped versions —
    // never zero. (Arrival order on a given replica is nondeterministic;
    // that is exactly what the read path resolves via LWW.)
    for (name, store) in [("n1", &store1), ("n2", &store2), ("n3", &store3)] {
        let meta = store
            .get_object(&BucketId::new("test"), &ObjectKey::new(key))
            .unwrap()
            .expect("replicated metadata must exist");
        assert!(
            meta.hlc == hlc_a || meta.hlc == hlc_b,
            "{name} must carry a coordinator-stamped HLC, got {:?}",
            meta.hlc,
        );
    }

    // The LWW winner is deterministic: the max of the two stamped HLCs,
    // tie-broken by node id (G7).
    let resolver = LwwResolver;
    let winner_from_n1 = resolver.resolve(&hlc_a, &hlc_b, &NodeId::new("n1"), &NodeId::new("n2"));
    let expected_max = hlc_a.max(hlc_b);
    match winner_from_n1 {
        Resolution::AcceptRemote => {
            assert_eq!(hlc_b, expected_max, "remote winner must be the max")
        }
        Resolution::AcceptLocal => {
            assert_eq!(hlc_a, expected_max, "local winner must be the max");
            // Equal HLCs can only prefer local via the node-id tie-break.
            if hlc_a == hlc_b {
                assert!(NodeId::new("n1").as_str() > NodeId::new("n2").as_str());
            }
        }
        Resolution::Merge => panic!("LWW resolver must never merge"),
        _ => panic!("unexpected resolution variant"),
    }
}
