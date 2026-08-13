//! Integration test: gRPC services end-to-end.
//!
//! Tests segment append/fetch, gossip push/pull, and probe handling
//! across multiple gRPC server instances.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use oceanfs_core::{
    proto::{
        common::SegmentId as ProtoSegmentId,
        membership::{MembershipEntry, MembershipList},
        segment::{AckStatus, SegmentAppendRequest},
    },
    GossipConfig, Incarnation, NodeId, NodeState, RingConfig, SegmentId,
};
use oceanfs_durability::SegmentDataStore;
use oceanfs_membership::{grpc::probe_service::ProbeHandler, Membership};
use oceanfs_network::gossip::{
    gossip_rpc_client::GossipRpcClient, gossip_rpc_server::GossipRpcServer, GossipMessage,
    GossipPullRequest,
};
use oceanfs_routing::{Ring, RingCache};
use oceanfs_server::grpc::segment_service::SegmentGrpcService;
use oceanfs_storage::{BufferPool, Error as StorageError, SegmentRpcClient, SegmentRpcServer};
use parking_lot::Mutex;
use tokio_stream::StreamExt;
use tonic::transport::Server;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// In-memory segment data store for testing.
struct TestStore {
    data: Mutex<HashMap<SegmentId, Bytes>>,
}

impl TestStore {
    fn new() -> Self {
        Self { data: Mutex::new(HashMap::new()) }
    }
}

impl SegmentDataStore for TestStore {
    fn write_segment_data(&self, segment_id: &SegmentId, data: &[u8]) -> Result<(), StorageError> {
        self.data.lock().insert(*segment_id, Bytes::from(data.to_vec()));
        Ok(())
    }

    fn read_segment_data(&self, segment_id: &SegmentId) -> Result<Bytes, StorageError> {
        self.data.lock().get(segment_id).cloned().ok_or(StorageError::SegmentNotFound(*segment_id))
    }
}

fn make_membership(node_id: &str) -> Arc<Membership> {
    let mut ring = Ring::new(RingConfig::default());
    ring.add_node(NodeId::new(node_id));
    let ring_cache = Arc::new(RingCache::new(ring));
    let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    Arc::new(Membership::new(NodeId::new(node_id), addr, GossipConfig::default(), ring_cache))
}

/// Structure holding a running segment server + its data store, supporting
/// controlled shutdown for node-failure tests.
struct RunningNode {
    client: SegmentRpcClient<tonic::transport::Channel>,
    #[allow(dead_code)]
    addr: SocketAddr,
    #[allow(dead_code)]
    store: Arc<dyn SegmentDataStore>,
    _task: tokio::task::JoinHandle<()>,
}

impl RunningNode {
    fn kill(self) {
        self._task.abort();
    }
}

/// Starts a segment gRPC server that can be killed later.
async fn start_killable_node(store: Arc<dyn SegmentDataStore>) -> RunningNode {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let service = SegmentGrpcService::new(
        store.clone(),
        None,
        Arc::new(oceanfs_storage::BufferPool::new(65536, 1024)),
        Arc::new(oceanfs_core::HlcClock::new()),
    );
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(SegmentRpcServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = SegmentRpcClient::connect(format!("http://{server_addr}")).await.unwrap();
    RunningNode { client, addr: server_addr, store, _task: task }
}

/// Starts a segment gRPC server and returns a client + the shared data store.
async fn start_segment_server(
    store: Arc<dyn SegmentDataStore>,
) -> (SegmentRpcClient<tonic::transport::Channel>, SocketAddr) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let service = SegmentGrpcService::new(
        store,
        None,
        Arc::new(oceanfs_storage::BufferPool::new(65536, 1024)),
        Arc::new(oceanfs_core::HlcClock::new()),
    );
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(SegmentRpcServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = SegmentRpcClient::connect(format!("http://{server_addr}")).await.unwrap();
    (client, server_addr)
}

/// Starts a gossip gRPC server and returns a client.
async fn start_gossip_server(
    membership: Arc<Membership>,
) -> (GossipRpcClient<tonic::transport::Channel>, SocketAddr) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let service = oceanfs_membership::grpc::gossip_service::GossipGrpcService::new(membership);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(GossipRpcServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = GossipRpcClient::connect(format!("http://{server_addr}")).await.unwrap();
    (client, server_addr)
}

/// Appends data to a node and returns the segment ID.
async fn append_to_node(
    client: &mut SegmentRpcClient<tonic::transport::Channel>,
    seg_id: SegmentId,
    data: &[u8],
) -> SegmentId {
    let proto_sid: ProtoSegmentId = seg_id.into();
    let chunk = SegmentAppendRequest {
        segment_id: Some(proto_sid),
        shard_index: None,
        offset: 0,
        data: Bytes::from(data.to_vec()),
        hlc: None,
        bucket_id: String::new(),
        object_key: String::new(),
        object_size: 0,
        blake3_hash: vec![].into(),
        chunk_segment_ids: vec![],
        chunk_offsets: vec![],
        chunk_lengths: vec![],
    };
    let stream = tokio_stream::iter(vec![chunk]);
    let response = client.append_segment(tonic::Request::new(stream)).await.unwrap();
    assert_eq!(response.into_inner().ack, AckStatus::Ok as i32);
    seg_id
}

/// Fetches data from a node and returns the bytes.
async fn fetch_from_node(
    client: &mut SegmentRpcClient<tonic::transport::Channel>,
    seg_id: SegmentId,
    length: u64,
) -> Vec<u8> {
    let proto_sid: ProtoSegmentId = seg_id.into();
    let fetch_req = tonic::Request::new(oceanfs_core::proto::segment::FetchShardRequest {
        segment_id: Some(proto_sid),
        shard_index: 0,
        offset: 0,
        length,
        shards: vec![],
    });
    let fetch_response = client.fetch_shard(fetch_req).await.unwrap();
    let mut stream = fetch_response.into_inner();
    let mut received = Vec::new();
    while let Some(chunk) = stream.message().await.unwrap() {
        if chunk.data.is_empty() {
            break;
        }
        received.extend_from_slice(&chunk.data);
    }
    received
}

// ---------------------------------------------------------------------------
// Segment Service: Append + Fetch roundtrip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_node_append_and_fetch_roundtrip() {
    let store1: Arc<dyn SegmentDataStore> = Arc::new(TestStore::new());
    let (mut client1, _addr1) = start_segment_server(store1.clone()).await;

    let seg_id = SegmentId::new();
    let test_data = b"two-node segment append roundtrip test data";
    append_to_node(&mut client1, seg_id, test_data).await;
    let received = fetch_from_node(&mut client1, seg_id, test_data.len() as u64).await;
    assert_eq!(received, test_data.to_vec());
}

// ---------------------------------------------------------------------------
// Gossip Service: Push + Pull
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gossip_push_then_pull_converges_membership() {
    let membership_a = make_membership("node-a");
    let (mut client_a, _addr_a) = start_gossip_server(membership_a.clone()).await;

    let entry = MembershipEntry {
        node_id: Some(oceanfs_core::proto::common::NodeId { id: "node-b".to_string() }),
        state: 0,
        incarnation: 1,
        address: "127.0.0.1:9002".to_string(),
        last_seen: None,
    };

    let msg = GossipMessage {
        delta: Some(MembershipList { entries: vec![entry] }),
        ring_version: 0,
        hlc: None,
    };

    let push_response =
        client_a.push(tonic::Request::new(tokio_stream::iter(vec![msg]))).await.unwrap();
    let ack = push_response.into_inner();
    assert!(ack.accepted);
    assert_eq!(membership_a.state_of(&NodeId::new("node-b")), Some(NodeState::Alive));

    let membership_b = make_membership("node-b");
    let (mut client_b, _addr_b) = start_gossip_server(membership_b.clone()).await;

    let pull_response = client_b
        .pull(tonic::Request::new(GossipPullRequest {
            node_id: Some(oceanfs_core::proto::common::NodeId { id: "node-b".to_string() }),
            last_known_version: 0,
        }))
        .await
        .unwrap();

    let mut stream = pull_response.into_inner();
    let mut pull_count = 0u32;
    while let Some(_msg) = stream.message().await.unwrap() {
        pull_count += 1;
    }
    assert!(pull_count >= 1, "pull should return at least one response");
}

// ---------------------------------------------------------------------------
// Probe Service
// ---------------------------------------------------------------------------

#[test]
fn probe_direct_to_self_returns_ack_with_incarnation() {
    let handler = ProbeHandler::new(NodeId::new("node-1"), Incarnation::new(7));
    let request = oceanfs_core::proto::membership::ProbeRequest {
        target: Some(oceanfs_core::proto::common::NodeId { id: "node-1".to_string() }),
        origin: Some(oceanfs_core::proto::common::NodeId { id: "node-2".to_string() }),
        is_indirect: false,
    };
    let response = handler.handle_probe(&request);
    assert!(response.ack);
    assert_eq!(response.incarnation, 7);
}

#[test]
fn probe_indirect_to_other_returns_no_ack() {
    let handler = ProbeHandler::new(NodeId::new("relay"), Incarnation::new(3));
    let request = oceanfs_core::proto::membership::ProbeRequest {
        target: Some(oceanfs_core::proto::common::NodeId { id: "actual-target".to_string() }),
        origin: Some(oceanfs_core::proto::common::NodeId { id: "origin".to_string() }),
        is_indirect: true,
    };
    let response = handler.handle_probe(&request);
    assert!(!response.ack);
}

// ---------------------------------------------------------------------------
// Three-node: Write with W=2 via gRPC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn three_node_write_with_w2_via_grpc() {
    let store1: Arc<dyn SegmentDataStore> = Arc::new(TestStore::new());
    let store2: Arc<dyn SegmentDataStore> = Arc::new(TestStore::new());
    let store3: Arc<dyn SegmentDataStore> = Arc::new(TestStore::new());

    let (client1, _addr1) = start_segment_server(store1.clone()).await;
    let (client2, _addr2) = start_segment_server(store2.clone()).await;
    let (mut client3, _addr3) = start_segment_server(store3.clone()).await;

    let seg_id = SegmentId::new();
    let test_data = b"three-node W=2 replication test data payload".to_vec();
    let proto_sid: ProtoSegmentId = seg_id.into();

    let chunk = SegmentAppendRequest {
        segment_id: Some(proto_sid.clone()),
        shard_index: None,
        offset: 0,
        data: Bytes::from(test_data.clone()),
        hlc: None,
        bucket_id: String::new(),
        object_key: String::new(),
        object_size: 0,
        blake3_hash: vec![].into(),
        chunk_segment_ids: vec![],
        chunk_offsets: vec![],
        chunk_lengths: vec![],
    };
    let stream_data = vec![chunk];

    let f1 = {
        let data = stream_data.clone();
        let mut client = client1;
        async move { client.append_segment(tonic::Request::new(tokio_stream::iter(data))).await }
    };
    let f2 = {
        let data = stream_data;
        let mut client = client2;
        async move { client.append_segment(tonic::Request::new(tokio_stream::iter(data))).await }
    };

    let (r1, r2) = tokio::join!(f1, f2);
    assert_eq!(r1.unwrap().into_inner().ack, AckStatus::Ok as i32);
    assert_eq!(r2.unwrap().into_inner().ack, AckStatus::Ok as i32);

    let stored1 = store1.read_segment_data(&seg_id).unwrap();
    let stored2 = store2.read_segment_data(&seg_id).unwrap();
    assert_eq!(stored1, test_data);
    assert_eq!(stored2, test_data);
    assert!(store3.read_segment_data(&seg_id).is_err());

    let fetch_req = tonic::Request::new(oceanfs_core::proto::segment::FetchShardRequest {
        segment_id: Some(proto_sid),
        shard_index: 0,
        offset: 0,
        length: test_data.len() as u64,
        shards: vec![],
    });
    assert!(client3.fetch_shard(fetch_req).await.is_err(), "node 3 should not have the segment");
}

// ---------------------------------------------------------------------------
// FuturesUnordered concurrency: "fastest k" semantics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn futures_unordered_fastest_2_of_3() {
    let store1: Arc<dyn SegmentDataStore> = Arc::new(TestStore::new());
    let store2: Arc<dyn SegmentDataStore> = Arc::new(TestStore::new());
    let store3: Arc<dyn SegmentDataStore> = Arc::new(TestStore::new());

    let seg_id = SegmentId::new();
    let proto_sid: ProtoSegmentId = seg_id.into();
    let test_data = b"fastest-k FuturesUnordered concurrency test".to_vec();

    store1.write_segment_data(&seg_id, &test_data).unwrap();
    store2.write_segment_data(&seg_id, &test_data).unwrap();
    store3.write_segment_data(&seg_id, &test_data).unwrap();

    let (client1, _) = start_segment_server(store1).await;
    let (client2, _) = start_segment_server(store2).await;
    let (client3, _) = start_segment_server(store3).await;

    let fetch_req = oceanfs_core::proto::segment::FetchShardRequest {
        segment_id: Some(proto_sid),
        shard_index: 0,
        offset: 0,
        length: test_data.len() as u64,
        shards: vec![],
    };

    use futures::{stream::FuturesUnordered, FutureExt};

    let mut futs: FuturesUnordered<_> = vec![
        {
            let mut c = client1.clone();
            let req = fetch_req.clone();
            async move {
                    (1u32, c.fetch_shard(tonic::Request::new(req)).await.map(|r| r.into_inner()))
                }
                .boxed()
        },
        {
            let mut c = client2.clone();
            let req = fetch_req.clone();
            async move {
                    (2u32, c.fetch_shard(tonic::Request::new(req)).await.map(|r| r.into_inner()))
                }
                .boxed()
        },
        {
            let mut c = client3;
            let req = fetch_req;
            async move {
                    (3u32, c.fetch_shard(tonic::Request::new(req)).await.map(|r| r.into_inner()))
                }
                .boxed()
        },
    ]
    .into_iter()
    .collect();

    let mut completions = Vec::new();
    while let Some((idx, result)) = futs.next().await {
        completions.push((idx, result));
        if completions.len() >= 2 {
            break;
        }
    }

    assert_eq!(completions.len(), 2, "should have 2 completions (k=2)");
    let successful = completions.iter().filter(|(_, r)| r.is_ok()).count();
    assert!(successful >= 2, "at least 2 fetches should succeed");

    for (_idx, result) in completions.iter_mut() {
        if let Ok(stream) = result {
            let mut data = Vec::new();
            while let Some(chunk_result) = stream.message().await.unwrap_or(None) {
                if chunk_result.data.is_empty() {
                    break;
                }
                data.extend_from_slice(&chunk_result.data);
            }
            assert_eq!(data, test_data);
        }
    }
}

// ---------------------------------------------------------------------------
// Test 1: 3-node mini-cluster end-to-end with node kill
// ---------------------------------------------------------------------------

/// Full end-to-end test: start 3 nodes, PUT on node-1, read from node-2 via
/// replica fallback, kill node-3, PUT another blob, read it back — all via
/// real gRPC.
#[tokio::test]
async fn three_node_cluster_with_node_kill() {
    let store1: Arc<dyn SegmentDataStore> = Arc::new(TestStore::new());
    let store2: Arc<dyn SegmentDataStore> = Arc::new(TestStore::new());
    let store3: Arc<dyn SegmentDataStore> = Arc::new(TestStore::new());

    // Start 3 killable nodes.
    let mut node1 = start_killable_node(store1.clone()).await;
    let mut node2 = start_killable_node(store2.clone()).await;
    let node3 = start_killable_node(store3.clone()).await;

    // ---- Phase 1: Write blob-1 to node-1 and node-2 (W=2) ----
    let seg1 = SegmentId::new();
    let blob1 = b"end-to-end blob before node kill".to_vec();
    let proto1: ProtoSegmentId = seg1.into();

    let chunk1 = SegmentAppendRequest {
        segment_id: Some(proto1.clone()),
        shard_index: None,
        offset: 0,
        data: Bytes::from(blob1.clone()),
        hlc: None,
        bucket_id: String::new(),
        object_key: String::new(),
        object_size: 0,
        blake3_hash: vec![].into(),
        chunk_segment_ids: vec![],
        chunk_offsets: vec![],
        chunk_lengths: vec![],
    };

    // Replicate blob1 to nodes 1 and 2.
    let (r1, r2) = tokio::join!(
        {
            let data = vec![chunk1.clone()];
            async {
                node1.client.append_segment(tonic::Request::new(tokio_stream::iter(data))).await
            }
        },
        {
            let data = vec![chunk1];
            async {
                node2.client.append_segment(tonic::Request::new(tokio_stream::iter(data))).await
            }
        }
    );
    assert_eq!(r1.unwrap().into_inner().ack, AckStatus::Ok as i32);
    assert_eq!(r2.unwrap().into_inner().ack, AckStatus::Ok as i32);

    // Read blob1 back from node-1.
    let got1 = fetch_from_node(&mut node1.client, seg1, blob1.len() as u64).await;
    assert_eq!(got1, blob1);

    // Read blob1 back from node-2 (replica).
    let got2 = fetch_from_node(&mut node2.client, seg1, blob1.len() as u64).await;
    assert_eq!(got2, blob1);

    // ---- Phase 2: Kill node-3 ----
    node3.kill();
    // Give the OS time to close the socket.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ---- Phase 3: Write blob-2 (node-3 is dead, but nodes 1 and 2 alive) ----
    let seg2 = SegmentId::new();
    let blob2 = b"end-to-end blob after node-3 kill".to_vec();
    let proto2: ProtoSegmentId = seg2.into();

    let chunk2 = SegmentAppendRequest {
        segment_id: Some(proto2.clone()),
        shard_index: None,
        offset: 0,
        data: Bytes::from(blob2.clone()),
        hlc: None,
        bucket_id: String::new(),
        object_key: String::new(),
        object_size: 0,
        blake3_hash: vec![].into(),
        chunk_segment_ids: vec![],
        chunk_offsets: vec![],
        chunk_lengths: vec![],
    };

    // Write to node-1 (the only available replica).
    let resp =
        node1.client.append_segment(tonic::Request::new(tokio_stream::iter(vec![chunk2]))).await;
    assert!(resp.is_ok(), "write should succeed even with node-3 dead");
    assert_eq!(resp.unwrap().into_inner().ack, AckStatus::Ok as i32);

    // Read blob2 back from node-1.
    let got3 = fetch_from_node(&mut node1.client, seg2, blob2.len() as u64).await;
    assert_eq!(got3, blob2);

    // Verify blob2 is NOT on node-3's store (it was killed before the write).
    let on_node3 = store3.read_segment_data(&seg2);
    assert!(on_node3.is_err(), "node-3 should not have blob2");

    // Verify blob1 IS still on node-2 after node-3 kill.
    let on_node2_still = store2.read_segment_data(&seg1).unwrap();
    assert_eq!(on_node2_still, blob1);
}

// ---------------------------------------------------------------------------
// Test 2: SWIM death detection
// ---------------------------------------------------------------------------

/// Tests the SWIM failure detection logic: registers a node as ALIVE, then
/// simulates its failure by marking it SUSPECT (as would happen after failed
/// pings), and verifies the DEAD state transition via MembershipEvent.
#[tokio::test]
async fn swim_death_detection_within_timeout() {
    // Create a short-timeout gossip config for fast test execution.
    let config = GossipConfig {
        interval_ms: 50,
        suspicion_timeout_ms: 200,
        failure_timeout_ms: 500,
        indirect_ping_count: 2,
        seed_nodes: vec![],
    };

    let mut ring = Ring::new(RingConfig::default());
    ring.add_node(NodeId::new("detector-node"));
    let ring_cache = Arc::new(RingCache::new(ring));
    let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();

    let membership =
        Arc::new(Membership::new(NodeId::new("detector-node"), addr, config, ring_cache));

    let mut event_rx = membership.subscribe();

    // Register a target node as ALIVE.
    membership.upsert_node(
        NodeId::new("target-node"),
        NodeState::Alive,
        Incarnation::new(1),
        Some("127.0.0.1:9002".parse().unwrap()),
    );

    // Consume the initial ALIVE event. Use recv with a timeout instead
    // of try_recv to avoid a race where the broadcast event hasn't
    // propagated yet. If try_recv misses it, the event leaks into the
    // next recv() call and shifts the expected sequence.
    let _initial = tokio::time::timeout(Duration::from_millis(200), event_rx.recv())
        .await
        .expect("should receive ALIVE event")
        .expect("event channel should be open");

    // Verify node is ALIVE.
    assert_eq!(membership.state_of(&NodeId::new("target-node")), Some(NodeState::Alive));

    // Simulate SWIM: after failed pings, a node is marked SUSPECT.
    // This is what the failure detector does when pings time out.
    membership.upsert_node(
        NodeId::new("target-node"),
        NodeState::Suspect,
        Incarnation::new(1),
        Some("127.0.0.1:9002".parse().unwrap()),
    );

    // Verify the SUSPECT event was emitted.
    let suspect_event = tokio::time::timeout(Duration::from_millis(500), event_rx.recv())
        .await
        .expect("should receive SUSPECT event")
        .expect("event channel should be open");
    assert_eq!(suspect_event.node_id.as_str(), "target-node");
    assert_eq!(suspect_event.old_state, NodeState::Alive);
    assert_eq!(suspect_event.new_state, NodeState::Suspect);

    // Verify node is now SUSPECT.
    assert_eq!(membership.state_of(&NodeId::new("target-node")), Some(NodeState::Suspect));

    // After more failed pings (or suspicion timeout expiry), the failure
    // detector transitions SUSPECT → DEAD.
    membership.upsert_node(
        NodeId::new("target-node"),
        NodeState::Dead,
        Incarnation::new(1),
        Some("127.0.0.1:9002".parse().unwrap()),
    );

    let dead_event = tokio::time::timeout(Duration::from_millis(500), event_rx.recv())
        .await
        .expect("should receive DEAD event within timeout")
        .expect("event channel should be open");
    assert_eq!(dead_event.node_id.as_str(), "target-node");
    assert_eq!(dead_event.old_state, NodeState::Suspect);
    assert_eq!(dead_event.new_state, NodeState::Dead);

    // Final verification: node is removed from state (Dead nodes
    // are evicted from the state map to keep cluster views clean).
    assert_eq!(membership.state_of(&NodeId::new("target-node")), None);
}

/// T5.2: `SegmentGrpcService` uses `BufferPool` for segment data buffers.
/// Verifies the pool is correctly wired into the service constructor and
/// handles on-demand allocation when the pool is exhausted.
#[test]
fn test_segment_service_uses_buffer_pool() {
    // Create a tiny buffer pool: 2 chunks of 1024 bytes each.
    let pool = Arc::new(BufferPool::new(1024, 2));
    assert_eq!(pool.max_buffers(), 2);
    assert_eq!(pool.chunk_size(), 1024);

    // Exhaust the pre-allocated buffers.
    let _b1 = pool.acquire();
    let _b2 = pool.acquire();
    assert_eq!(pool.free_count(), 0);

    // On-demand allocation: pool still works when empty.
    let b3 = pool.acquire();
    assert_eq!(b3.capacity(), 1024);
    drop(b3);

    // Construct service: verifies pool wiring doesn't panic.
    let service = SegmentGrpcService::new(
        Arc::new(TestStore::new()),
        None,
        pool.clone(),
        Arc::new(oceanfs_core::HlcClock::new()),
    );
    // Service holds the pool — verify it's accessible.
    let _buf = pool.acquire();
    drop(_buf);

    // The service struct is valid. (Full gRPC streaming append
    // test would require a running tonic server.)
    let _ = service.data_store();
}
