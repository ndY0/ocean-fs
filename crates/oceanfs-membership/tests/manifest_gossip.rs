//! Integration test: `NodeManifest` gossip convergence (f6, ADR-0029 D2).
//!
//! A 3-node local cluster (no data plane — membership plane only): each
//! node declares its storage-pool manifest via `set_self_manifest`, joins
//! through a seed, and the push-pull gossip plane must converge so that
//! every peer's membership view carries all three manifests with matching
//! pool counts. This is the epic DoD's "manifest propagates" item.
//!
//! The test asserts the WHOLE point of the feature: peers learn the
//! manifest through gossip alone (no direct queries), and the wire
//! round-trip preserves pool count + per-pool fields.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{net::SocketAddr, sync::Arc, time::Duration};

use oceanfs_core::{GossipConfig, Incarnation, NodeId, RingConfig, RpcConfig};
use oceanfs_membership::{
    grpc::gossip_service::GossipGrpcService,
    manifest::{NodeManifest, PoolManifest},
    Membership,
};
use oceanfs_network::{gossip::gossip_rpc_server::GossipRpcServer, ConnectionPool};
use oceanfs_routing::{Ring, RingCache};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// A distinct manifest per node: the free-bytes watermark tells the
/// assertions which node's manifest they are looking at.
fn test_manifest(node_index: u64) -> NodeManifest {
    NodeManifest::from_pools(
        1,
        &[
            PoolManifest::new(0, "data", "healthy", false, (1 << 40) + node_index, 2),
            PoolManifest::new(1, "wal", "healthy", false, 1 << 30, 1),
        ],
    )
}

struct TestNode {
    membership: Arc<Membership>,
}

/// Boots one node: ring, membership, gossip server, connection pool,
/// then start → set_self_manifest → join.
///
/// The gossip listener is bound FIRST and its actual address is both
/// the node's announced membership address and the seed address peers
/// dial — gossip ticks dial the announced address, so it must be a
/// real listener.
async fn boot_node(
    node_id: &str,
    announce_addr: SocketAddr,
    manifest: NodeManifest,
    seeds: &[String],
) -> (TestNode, SocketAddr) {
    let listener = tokio::net::TcpListener::bind(announce_addr).await.expect("bind");
    let grpc_addr = listener.local_addr().expect("listen addr");

    let mut ring = Ring::new(RingConfig::default());
    ring.add_node(NodeId::new(node_id));
    let ring_cache = Arc::new(RingCache::new(ring));

    // Fast gossip (250 ms) so convergence is quick; suspicion/failure
    // timeouts huge so the probe-less test never flips peers to
    // Suspect/Dead mid-assertion.
    let config = GossipConfig {
        interval_ms: 250,
        suspicion_timeout_ms: 60_000,
        failure_timeout_ms: 120_000,
        seed_nodes: seeds.to_vec(),
        ..GossipConfig::default()
    };

    let membership =
        Arc::new(Membership::new(NodeId::new(node_id), grpc_addr, grpc_addr, config, ring_cache));

    let serve_membership = membership.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(GossipRpcServer::new(GossipGrpcService::new(serve_membership)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("gossip server failed");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
    membership.set_pool(pool);

    membership.start().expect("start");
    membership.set_self_manifest(manifest.clone());
    membership.join(Incarnation::new(1), &[]).await.expect("join");

    (TestNode { membership }, grpc_addr)
}

/// The f6 DoD integration item: each peer's view contains all three
/// manifests with matching pool counts, reached through gossip alone.
#[test]
fn manifests_converge_on_all_three_peers() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // ---- Node A: the seed (no seed nodes; first node). ----
        let a_addr: SocketAddr = "127.0.0.1:9100".parse().unwrap();
        let a_manifest = test_manifest(0);
        let (node_a, a_grpc_addr) = boot_node("node-a", a_addr, a_manifest.clone(), &[]).await;

        // ---- Nodes B and C: join through A's gossip listener. ----
        let b_addr: SocketAddr = "127.0.0.1:9101".parse().unwrap();
        let b_manifest = test_manifest(1);
        let (node_b, _b_grpc_addr) =
            boot_node("node-b", b_addr, b_manifest.clone(), &[a_grpc_addr.to_string()]).await;

        let c_addr: SocketAddr = "127.0.0.1:9102".parse().unwrap();
        let c_manifest = test_manifest(2);
        let (node_c, _c_grpc_addr) =
            boot_node("node-c", c_addr, c_manifest.clone(), &[a_grpc_addr.to_string()]).await;

        let nodes = [("node-a", &node_a), ("node-b", &node_b), ("node-c", &node_c)];
        let expected = [("node-a", &a_manifest), ("node-b", &b_manifest), ("node-c", &c_manifest)];
        // Bind plain references: the array iteration yields
        // (&&str, &&NodeManifest) pairs.
        let expected: Vec<(&str, &NodeManifest)> =
            expected.iter().map(|(id, manifest)| (*id, *manifest)).collect();

        // ---- Convergence: every node sees every peer's manifest. ----
        // Pull-based convergence is bounded by the gossip interval
        // (250 ms) × a few rounds; the deadline is generous for CI.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut converged = false;
        while tokio::time::Instant::now() < deadline {
            let all_visible = nodes.iter().all(|(_, node)| {
                expected.iter().all(|(peer_id, manifest)| {
                    node.membership.manifest_of(&NodeId::new(*peer_id)).as_ref() == Some(*manifest)
                })
            });
            if all_visible {
                converged = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert!(converged, "all three manifests must converge on every peer within the deadline");

        // ---- The assertions the DoD pins: every peer's view carries
        // all three manifests with matching pool counts. ----
        for (self_id, node) in &nodes {
            let view = node.membership.nodes();
            assert_eq!(view.len(), 3, "{self_id} must see all three nodes, got: {view:?}");
            for (peer_id, manifest) in &expected {
                let seen = node
                    .membership
                    .manifest_of(&NodeId::new(*peer_id))
                    .unwrap_or_else(|| panic!("{self_id} must have {peer_id}'s manifest"));
                assert_eq!(
                    seen.pools().len(),
                    manifest.pools().len(),
                    "{self_id} sees {peer_id} with {} pools, expected {}",
                    seen.pools().len(),
                    manifest.pools().len()
                );
                assert_eq!(seen, **manifest, "{self_id}'s view of {peer_id}");
            }
        }
    });
}
