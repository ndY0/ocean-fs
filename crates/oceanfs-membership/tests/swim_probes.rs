//! Integration test: real SWIM probes over the membership plane
//! (ADR-0028 D2/D3).
//!
//! Spins up three in-process nodes, each with its own `Membership`,
//! membership-plane pool, and `ProbeRpc` tonic server. Verifies:
//!
//! 1. Direct probes ack with the target's incarnation.
//! 2. Indirect probes relay through a third node.
//! 3. A killed target is SUSPECT within the detection bound
//!    (interval + 2×ping_timeout), then DEAD after suspicion_timeout.
//! 4. The rejoined node (higher incarnation, ADR-0022) is re-admitted
//!    via gossip and recovered.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{net::SocketAddr, sync::Arc, time::Duration};

use oceanfs_core::{
    proto::membership::{ProbeRequest, ProbeResponse},
    GossipConfig, Incarnation, NodeId, NodeState, RingConfig,
};
use oceanfs_membership::{grpc::probe_service::ProbeGrpcService, plane, Membership};
use oceanfs_network::gossip::{probe_rpc_client::ProbeRpcClient, probe_rpc_server::ProbeRpcServer};
use oceanfs_routing::{Ring, RingCache};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

struct TestNode {
    membership: Arc<Membership>,
    /// The probe server's bound address (also the announced membership
    /// address).
    probe_addr: SocketAddr,
    shutdown: CancellationToken,
}

fn gossip_config() -> GossipConfig {
    GossipConfig {
        interval_ms: 50,
        suspicion_timeout_ms: 200,
        failure_timeout_ms: 300,
        indirect_ping_count: 2,
        fanout_k: 3,
        seed_nodes: Vec::new(),
    }
}

async fn start_node(name: &str, config: GossipConfig) -> TestNode {
    // Bind the probe listener FIRST so the announced address is the
    // real bound address (ephemeral port).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let probe_addr = listener.local_addr().unwrap();

    let mut ring = Ring::new(RingConfig::default());
    ring.add_node(NodeId::new(name));
    let ring_cache = Arc::new(RingCache::new(ring));

    let membership = Arc::new(Membership::new(
        NodeId::new(name),
        probe_addr,
        probe_addr,
        config.clone(),
        ring_cache,
    ));
    let pool = plane::membership_pool(config.failure_timeout_ms / 3, None);
    membership.set_pool(pool.clone());

    let service = ProbeGrpcService::new(
        NodeId::new(name),
        membership.clone(),
        pool,
        config.failure_timeout_ms / 3,
    );
    let gossip_service =
        oceanfs_membership::grpc::gossip_service::GossipGrpcService::new(membership.clone());

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        // The full membership plane: probe + gossip services (the
        // rejoin path disseminates via gossip push).
        let _ = Server::builder()
            .add_service(ProbeRpcServer::new(service))
            .add_service(oceanfs_network::GossipRpcServer::new(gossip_service))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown_signal.cancelled_owned(),
            )
            .await;
    });

    membership.start().unwrap();
    membership.join(Incarnation::new(1), &[]).await.unwrap();
    TestNode { membership, probe_addr, shutdown }
}

async fn probe_direct(
    client: &mut ProbeRpcClient<tonic::transport::Channel>,
    target: &str,
    origin: &str,
) -> ProbeResponse {
    client
        .probe(tonic::Request::new(ProbeRequest {
            target: Some(oceanfs_core::proto::common::NodeId { id: target.to_string() }),
            origin: Some(oceanfs_core::proto::common::NodeId { id: origin.to_string() }),
            is_indirect: false,
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn probe_indirect(
    client: &mut ProbeRpcClient<tonic::transport::Channel>,
    target: &str,
    origin: &str,
) -> ProbeResponse {
    client
        .probe(tonic::Request::new(ProbeRequest {
            target: Some(oceanfs_core::proto::common::NodeId { id: target.to_string() }),
            origin: Some(oceanfs_core::proto::common::NodeId { id: origin.to_string() }),
            is_indirect: true,
        }))
        .await
        .unwrap()
        .into_inner()
}

/// Polls a condition until it holds or the timeout elapses.
async fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// ---------------------------------------------------------------------------
// 1 + 2: direct + relayed probes over real services
// ---------------------------------------------------------------------------

#[tokio::test]
async fn direct_probe_acks_with_local_incarnation() {
    let config = gossip_config();
    let node = start_node("node-a", config).await;

    let client = ProbeRpcClient::connect(format!("http://{}", node.probe_addr)).await.unwrap();
    let mut client = client;

    let response = probe_direct(&mut client, "node-a", "node-b").await;
    assert!(response.ack, "direct probe to self must ack");
    assert_eq!(response.incarnation, 1, "ack carries the announced incarnation");

    node.shutdown.cancel();
}

#[tokio::test]
async fn indirect_probe_relays_through_third_node() {
    let config = gossip_config();
    let a = start_node("node-a", config.clone()).await;
    let b = start_node("node-b", config.clone()).await;
    let c = start_node("node-c", config).await;

    // Cross-register so the relay can resolve the target's address.
    a.membership.upsert_node(
        NodeId::new("node-b"),
        NodeState::Alive,
        Incarnation::new(1),
        Some(b.probe_addr),
    );
    a.membership.upsert_node(
        NodeId::new("node-c"),
        NodeState::Alive,
        Incarnation::new(1),
        Some(c.probe_addr),
    );
    c.membership.upsert_node(
        NodeId::new("node-b"),
        NodeState::Alive,
        Incarnation::new(1),
        Some(b.probe_addr),
    );

    // A asks C to probe B on its behalf (is_indirect).
    let client = ProbeRpcClient::connect(format!("http://{}", c.probe_addr)).await.unwrap();
    let mut client = client;
    let response = probe_indirect(&mut client, "node-b", "node-a").await;
    assert!(response.ack, "relayed probe through C must ack");
    assert_eq!(response.incarnation, 1, "relayed ack carries the target's incarnation");

    a.shutdown.cancel();
    b.shutdown.cancel();
    c.shutdown.cancel();
}

// ---------------------------------------------------------------------------
// 3 + 4: kill → SUSPECT → DEAD → rejoin recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn killed_target_is_suspected_then_dead_and_rejoin_recovers() {
    let config = gossip_config();
    let a = start_node("node-a", config.clone()).await;
    let b = start_node("node-b", config.clone()).await;
    let c = start_node("node-c", config).await;

    // Cross-register every peer in every membership so each detector
    // probes the others (the detector's alive-nodes sync reads the
    // membership state).
    for (m, self_id) in [
        (a.membership.clone(), "node-a"),
        (b.membership.clone(), "node-b"),
        (c.membership.clone(), "node-c"),
    ] {
        for (peer, peer_addr) in
            [("node-a", a.probe_addr), ("node-b", b.probe_addr), ("node-c", c.probe_addr)]
        {
            if peer != self_id {
                m.upsert_node(
                    NodeId::new(peer),
                    NodeState::Alive,
                    Incarnation::new(1),
                    Some(peer_addr),
                );
            }
        }
    }

    // --- Phase 1: kill B ---
    b.shutdown.cancel();

    // Detection bound: interval (50ms) + direct timeout (100ms) +
    // indirect timeout (100ms) + suspicion_timeout (200ms) → Dead within
    // ~450ms; assert with a generous margin.
    let suspected = poll_until(Duration::from_secs(2), || {
        a.membership.state_of(&NodeId::new("node-b")) == Some(NodeState::Suspect)
    })
    .await;
    assert!(suspected, "node-b must be SUSPECT on node-a within the detection bound");

    let dead = poll_until(Duration::from_secs(2), || {
        a.membership.state_of(&NodeId::new("node-b")) == Some(NodeState::Dead)
    })
    .await;
    assert!(dead, "node-b must be DEAD on node-a after the suspicion timeout");

    // --- Phase 2: B restarts and rejoins at a higher incarnation ---
    // Simulate the real restart (ADR-0022): a FRESH membership instance
    // with the same node id, a new probe address, and the persisted
    // incarnation bumped (2). It re-contacts the cluster via a fallback
    // seed (A's address — the seedless-restart path), pulls the
    // membership, announces Alive(2), and its gossip push carries the
    // re-announcement to A, which re-admits (strictly higher incarnation
    // beats the retained Dead, and the fresh address is adopted).
    let b2_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b2_addr = b2_listener.local_addr().unwrap();
    let b2_ring = {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("node-b"));
        Arc::new(RingCache::new(ring))
    };
    let b2 = Arc::new(Membership::new(
        NodeId::new("node-b"),
        b2_addr,
        b2_addr,
        gossip_config(),
        b2_ring,
    ));
    let b2_pool = plane::membership_pool(100, None);
    b2.set_pool(b2_pool.clone());
    let b2_service = ProbeGrpcService::new(NodeId::new("node-b"), b2.clone(), b2_pool, 100);
    let b2_gossip = oceanfs_membership::grpc::gossip_service::GossipGrpcService::new(b2.clone());
    let b2_shutdown = CancellationToken::new();
    let b2_signal = b2_shutdown.clone();
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(ProbeRpcServer::new(b2_service))
            .add_service(oceanfs_network::GossipRpcServer::new(b2_gossip))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(b2_listener),
                b2_signal.cancelled_owned(),
            )
            .await;
    });
    b2.start().unwrap();
    // The seedless-restart path: contact the last-known member (A).
    b2.join(Incarnation::new(2), &[a.probe_addr.to_string()]).await.unwrap();

    let recovered = poll_until(Duration::from_secs(3), || {
        a.membership.state_of(&NodeId::new("node-b")) == Some(NodeState::Alive)
    })
    .await;
    if !recovered {
        // Diagnostic state probe (localize the failure side).
        let b_id = NodeId::new("node-b");
        let a_id = NodeId::new("node-a");
        eprintln!(
            "DIAG a.view_of_b={:?} addr={:?} | b2.view_of_a={:?} | b2.view_of_self={:?} | c.view_of_b={:?}",
            a.membership.state_of(&b_id),
            a.membership.address_of(&b_id),
            b2.state_of(&a_id),
            b2.state_of(&b_id),
            c.membership.state_of(&b_id),
        );
    }
    assert!(recovered, "node-b must be re-admitted as Alive on node-a after the rejoin");
    assert_eq!(
        a.membership.address_of(&NodeId::new("node-b")),
        Some(b2_addr),
        "the rejoin must update A's view to B's fresh address (ADR-0022)"
    );

    b2_shutdown.cancel();
    a.shutdown.cancel();
    c.shutdown.cancel();
}
