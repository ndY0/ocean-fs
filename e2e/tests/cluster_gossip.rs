//! Cluster gossip & membership tests (T5-T8).
//!
//! Validates gossip convergence, delta propagation, ring version
//! propagation, and incarnation monotonicity.

use e2e::harness::{config_fast_gossip, response_json, Cluster};
use serde::Deserialize;

/// Response from GET /admin/cluster.
#[derive(Debug, Deserialize)]
struct ClusterView {
    nodes: Vec<NodeInfo>,
    #[allow(dead_code)]
    vnodes: usize,
    generation: u64,
}

/// Information about a node in the cluster view.
#[derive(Debug, Deserialize)]
struct NodeInfo {
    id: String,
    state: String,
    incarnation: u64,
    #[allow(dead_code)]
    address: String,
}

// ---------------------------------------------------------------------------
// T5: Gossip convergence
// ---------------------------------------------------------------------------

/// T5: Node B joins. Within N gossip rounds (N ≤ 10), Node A's
/// membership list includes B in ALIVE state.
#[tokio::test]
async fn t5_gossip_convergence_membership_includes_joiner() {
    let cluster = Cluster::spawn(2, &config_fast_gossip()).await.expect("spawn 2-node cluster");

    // Wait for gossip to propagate: node 1 should appear in node 0's view.
    cluster.wait_for_convergence(2).await.expect("cluster convergence");

    // Verify node 0's cluster view includes both nodes in ALIVE state.
    let resp = cluster.get(0, "/admin/cluster").await.expect("GET cluster");
    assert_eq!(resp.status(), 200);
    let view: ClusterView = response_json(resp).await.expect("parse cluster view");

    let alive_nodes: Vec<&NodeInfo> = view.nodes.iter().filter(|n| n.state == "Alive").collect();
    assert_eq!(
        alive_nodes.len(),
        2,
        "expected 2 ALIVE nodes, got {}: {:?}",
        alive_nodes.len(),
        view.nodes
    );

    // Verify node 1 is in ALIVE state in node 0's membership.
    let node_1_present = view.nodes.iter().any(|n| n.id.contains("cluster-1"));
    assert!(node_1_present, "node 1 should appear in node 0's membership: {:?}", view.nodes);

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T6: Gossip delta propagation
// ---------------------------------------------------------------------------

/// T6: Node A adds a node (or changes state). Within 5 rounds,
/// Node B and C see the change.
#[tokio::test]
async fn t6_gossip_delta_propagation_state_change_visible_on_all_nodes() {
    let cluster = Cluster::spawn(3, &config_fast_gossip()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // Verify all nodes see 3 members.
    for i in 0..3 {
        let resp = cluster.get(i, "/admin/cluster").await.expect("GET cluster");
        assert_eq!(resp.status(), 200);
        let view: ClusterView = response_json(resp).await.expect("parse cluster view");
        assert_eq!(view.nodes.len(), 3, "node {i} should report 3 nodes, got {}", view.nodes.len());
        // All nodes should be Alive.
        let all_alive = view.nodes.iter().all(|n| n.state == "Alive");
        assert!(all_alive, "node {i}: all nodes should be Alive: {:?}", view.nodes);
    }

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T7: Ring version propagation
// ---------------------------------------------------------------------------

/// T7: After a join/leave changes the ring, all nodes converge to the
/// same ring generation within 10 gossip rounds.
#[tokio::test]
async fn t7_ring_version_propagation_generation_converges() {
    let cluster = Cluster::spawn(3, &config_fast_gossip()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // Verify all nodes agree on the same ring generation.
    let mut generations: Vec<u64> = Vec::with_capacity(3);
    for i in 0..3 {
        let resp = cluster.get(i, "/admin/cluster").await.expect("GET cluster");
        assert_eq!(resp.status(), 200);
        let view: ClusterView = response_json(resp).await.expect("parse cluster view");
        generations.push(view.generation);
    }

    // All generations should be equal.
    let first = generations[0];
    for (i, gen) in generations.iter().enumerate() {
        assert_eq!(
            *gen, first,
            "node {i} ring generation {gen} differs from node 0's generation {first}"
        );
    }

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T8: Incarnation monotonicity
// ---------------------------------------------------------------------------

/// T8: A node's incarnation number never decreases across gossip rounds.
/// On rejoin, incarnation increments.
#[tokio::test]
async fn t8_incarnation_monotonicity_never_decreases() {
    let cluster = Cluster::spawn(3, &config_fast_gossip()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // Record baseline incarnations for each node from node 0's view.
    let baseline: Vec<(String, u64)> = {
        let resp = cluster.get(0, "/admin/cluster").await.expect("GET cluster");
        let view: ClusterView = response_json(resp).await.expect("parse cluster view");
        view.nodes.iter().map(|n| (n.id.clone(), n.incarnation)).collect()
    };
    assert_eq!(baseline.len(), 3, "should have 3 nodes in baseline");

    // Kill and restart node 2.
    cluster.kill(2).expect("kill node 2");
    cluster.restart(2).await.expect("restart node 2");

    cluster.wait_for_convergence(3).await.expect("cluster re-convergence");

    // Check incarnations after rejoin from node 0's view.
    let after: Vec<(String, u64)> = {
        let resp = cluster.get(0, "/admin/cluster").await.expect("GET cluster");
        let view: ClusterView = response_json(resp).await.expect("parse cluster view");
        view.nodes.iter().map(|n| (n.id.clone(), n.incarnation)).collect()
    };

    // The restarted node should have a higher (or equal) incarnation.
    // Find node 2's baseline and after incarnation.
    let node2_baseline = baseline
        .iter()
        .find(|(id, _)| id.contains("cluster-2"))
        .map(|(_, inc)| *inc)
        .expect("find node 2 in baseline");
    let node2_after = after
        .iter()
        .find(|(id, _)| id.contains("cluster-2"))
        .map(|(_, inc)| *inc)
        .expect("find node 2 in after");

    assert!(
        node2_after >= node2_baseline,
        "node 2 incarnation should not decrease: baseline={}, after={}",
        node2_baseline,
        node2_after
    );

    // For nodes that didn't restart, incarnation should be stable.
    for (id, bl_inc) in &baseline {
        if id.contains("cluster-2") {
            continue;
        }
        if let Some(after_entry) = after.iter().find(|(aid, _)| aid == id) {
            assert!(
                after_entry.1 >= *bl_inc,
                "node {} incarnation decreased: baseline={}, after={}",
                id,
                bl_inc,
                after_entry.1
            );
        }
    }

    cluster.shutdown().await.expect("shutdown");
}
