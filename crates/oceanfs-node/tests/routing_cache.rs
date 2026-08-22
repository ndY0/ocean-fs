//! Integration test: `ManifestCache` populated by the gossip plane and
//! consulted by the routing hint (f7, ADR-0029 §D5).
//!
//! A 3-node local cluster (membership plane only): each node's
//! `ManifestCache` is wired to its membership events exactly like the
//! production subscriber (node.rs step 15e). After gossip convergence
//! every cache holds all three manifests; a synthetic status flip on
//! one node (all its data pools → Dead, via `set_self_manifest`) then
//! propagates through the version-bumped gossip entry and the read-path
//! exclusion flips — the "cached routing state" epic-DoD item.
//!
//! Phase A manifests are all Healthy, so the filters are neutral until
//! the synthetic flip — this test proves the structure works.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{net::SocketAddr, sync::Arc, time::Duration};

use oceanfs_core::{GossipConfig, Incarnation, NodeId, RingConfig};
use oceanfs_membership::{
    grpc::{gossip_service::GossipGrpcService, probe_service::ProbeGrpcService},
    manifest::{NodeManifest, PoolManifest},
    plane, Membership,
};
use oceanfs_network::gossip::{
    gossip_rpc_server::GossipRpcServer, probe_rpc_server::ProbeRpcServer,
};
use oceanfs_node::routing_cache::ManifestCache;
use oceanfs_routing::{Ring, RingCache};
use oceanfs_server::routing_hint::RoutingHint;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// A distinct manifest per node: the data-pool count identifies the node.
fn test_manifest(node_index: u64) -> NodeManifest {
    NodeManifest::from_pools(
        1,
        &[
            PoolManifest::new(0, "data", "healthy", false, (1 << 40) + node_index, 2),
            PoolManifest::new(1, "wal", "healthy", false, 1 << 30, 1),
        ],
    )
}

/// The synthetic Phase-B flip: every data pool reported Dead.
fn dead_manifest(base: &NodeManifest) -> NodeManifest {
    let pools = base
        .pools()
        .iter()
        .map(|p| {
            if p.role() == "data" {
                PoolManifest::new(
                    p.id(),
                    "data",
                    "dead",
                    false,
                    p.capacity_free_bytes(),
                    p.weight(),
                )
            } else {
                p.clone()
            }
        })
        .collect::<Vec<_>>();
    NodeManifest::from_pools(base.incarnation(), &pools)
}

struct TestNode {
    membership: Arc<Membership>,
    cache: Arc<ManifestCache>,
}

/// Spawns the production-shaped cache subscriber (node.rs step 15e):
/// version-bumped entries update the cache, Dead/Left evict.
fn spawn_cache_subscriber(membership: Arc<Membership>, cache: Arc<ManifestCache>) {
    let mut events = membership.subscribe();
    let shutdown = membership.shutdown_token();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                event = events.recv() => {
                    match event {
                        Ok(ev) => {
                            match ev.new_state {
                                oceanfs_core::NodeState::Dead
                                | oceanfs_core::NodeState::Left => {
                                    cache.remove(&ev.node_id);
                                }
                                _ => {
                                    if let Some(manifest) = ev.manifest {
                                        cache.update(ev.node_id, manifest);
                                    }
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = shutdown.cancelled() => break,
            }
        }
    });
}

/// Boots one node: gossip listener on a fixed address, membership,
/// start → set_self_manifest → join, plus the cache subscriber.
async fn boot_node(
    node_id: &str,
    announce_addr: SocketAddr,
    manifest: NodeManifest,
    seeds: &[String],
) -> TestNode {
    let listener = tokio::net::TcpListener::bind(announce_addr).await.expect("bind");
    let grpc_addr = listener.local_addr().expect("listen addr");

    let mut ring = Ring::new(RingConfig::default());
    ring.add_node(NodeId::new(node_id));
    let ring_cache = Arc::new(RingCache::new(ring));

    // Fast gossip so convergence is quick; huge suspicion/failure
    // timeouts so the probe-less harness never flips nodes mid-test.
    let config = GossipConfig {
        interval_ms: 250,
        suspicion_timeout_ms: 60_000,
        failure_timeout_ms: 120_000,
        seed_nodes: seeds.to_vec(),
        ..GossipConfig::default()
    };

    // Probe-derived ping timeout (membership plane, ADR-0028 D1).
    let probe_ping_timeout_ms = config.failure_timeout_ms / 3;

    let membership =
        Arc::new(Membership::new(NodeId::new(node_id), grpc_addr, grpc_addr, config, ring_cache));

    let cache = Arc::new(ManifestCache::new());
    spawn_cache_subscriber(membership.clone(), cache.clone());

    // The membership plane pool (probe-derived timeouts) is shared by
    // the gossip pushes and the SWIM probes (ADR-0028 D1).
    let pool = plane::membership_pool(probe_ping_timeout_ms, None);
    membership.set_pool(pool.clone());

    // Serve BOTH planes on the same listener: gossip push/pull AND the
    // probe service — without probes, the failure detector marks every
    // peer Suspect (detector-origin entries beat Alive announcements
    // in the authority-class merge) and corrupts the version state the
    // routing cache depends on.
    let serve_membership = membership.clone();
    let serve_pool = pool.clone();
    let serve_node_id = NodeId::new(node_id);
    tokio::spawn(async move {
        Server::builder()
            .add_service(GossipRpcServer::new(GossipGrpcService::new(serve_membership.clone())))
            .add_service(ProbeRpcServer::new(ProbeGrpcService::new(
                serve_node_id,
                serve_membership,
                serve_pool,
                probe_ping_timeout_ms,
            )))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("gossip server failed");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    membership.start().expect("start");
    membership.set_self_manifest(manifest.clone());
    // Seed the self entry into the local cache (the production node
    // does this at step 15d; set_self_manifest emits no event).
    cache.update(NodeId::new(node_id), Arc::new(manifest));
    membership.join(Incarnation::new(1), &[]).await.expect("join");

    TestNode { membership, cache }
}

/// The f7 DoD integration item: post-convergence every cache holds all
/// three manifests, and a synthetic status flip propagates and flips the
/// read-path exclusion.
#[test]
fn caches_converge_and_status_flip_changes_routing() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // ---- Node A: the seed. ----
        let a_addr: SocketAddr = "127.0.0.1:9200".parse().unwrap();
        let a_manifest = test_manifest(0);
        let node_a = boot_node("node-a", a_addr, a_manifest, &[]).await;

        // ---- Nodes B and C: join through A. ----
        let b_addr: SocketAddr = "127.0.0.1:9201".parse().unwrap();
        let b_manifest = test_manifest(1);
        let node_b = boot_node("node-b", b_addr, b_manifest.clone(), &[a_addr.to_string()]).await;

        let c_addr: SocketAddr = "127.0.0.1:9202".parse().unwrap();
        let c_manifest = test_manifest(2);
        let node_c = boot_node("node-c", c_addr, c_manifest, &[a_addr.to_string()]).await;

        let nodes = [("node-a", &node_a), ("node-b", &node_b), ("node-c", &node_c)];

        // ---- Convergence: every cache holds all three manifests. ----
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut converged = false;
        while tokio::time::Instant::now() < deadline {
            let all_present = nodes.iter().all(|(_, node)| {
                ["node-a", "node-b", "node-c"]
                    .iter()
                    .all(|peer| node.cache.get(&NodeId::new(*peer)).is_some())
            });
            if all_present {
                converged = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(converged, "all three manifests must converge into every cache");

        for (self_id, node) in &nodes {
            assert_eq!(node.cache.len(), 3, "{self_id} cache must hold 3 manifests");
            // Phase A neutral: no healthy node is excluded.
            assert!(!node.cache.exclude_read_candidate(&NodeId::new("node-b")));
            assert!(!node.cache.exclude_write_target(&NodeId::new("node-b")));
        }

        // ---- Synthetic status flip: node B's data pools → Dead. ----
        let flipped = dead_manifest(&b_manifest);
        // B re-declares: version bump → gossip propagates the new
        // manifest; the local cache is re-seeded like the production
        // health monitor would.
        node_b.membership.set_self_manifest(flipped.clone());
        node_b.cache.update(NodeId::new("node-b"), Arc::new(flipped));

        // ---- The flip propagates and changes the read-path route. ----
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut flipped_visible = false;
        while tokio::time::Instant::now() < deadline {
            let all_flipped = nodes
                .iter()
                .all(|(_, node)| node.cache.exclude_read_candidate(&NodeId::new("node-b")));
            if all_flipped {
                flipped_visible = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(flipped_visible, "the synthetic Dead flip must propagate to every peer's cache");

        // The write path excludes node B too (zero Healthy data pools).
        for (self_id, node) in &nodes {
            assert!(
                node.cache.exclude_write_target(&NodeId::new("node-b")),
                "{self_id} must exclude node-b as a write target after the flip"
            );
            // The healthy node A stays eligible everywhere.
            assert!(
                !node.cache.exclude_read_candidate(&NodeId::new("node-a")),
                "{self_id} must keep routing reads to healthy node-a"
            );
        }
    });
}
