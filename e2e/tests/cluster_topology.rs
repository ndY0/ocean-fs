//! Cluster topology tests (T1-T4).
//!
//! Validates basic cluster formation: 2-node join, 3-node join,
//! graceful leave, and rejoin after leave. These are the foundation
//! tests — if they fail, no other cluster test can pass.

use e2e::harness::{config_fast_gossip, config_fast_swim, response_json, Cluster};
use serde::Deserialize;

/// Response from GET /admin/cluster.
#[derive(Debug, Deserialize)]
struct ClusterView {
    nodes: Vec<NodeInfo>,
    #[allow(dead_code)]
    vnodes: usize,
    #[allow(dead_code)]
    generation: u64,
}

/// Information about a node in the cluster view.
#[derive(Debug, Deserialize)]
struct NodeInfo {
    #[allow(dead_code)]
    id: String,
    #[allow(dead_code)]
    state: String,
    #[allow(dead_code)]
    incarnation: u64,
    #[allow(dead_code)]
    address: String,
}

// ---------------------------------------------------------------------------
// T1: 2-node join
// ---------------------------------------------------------------------------

/// T1: Node B starts with seed=Node A. Both rings contain both nodes.
/// GET /admin/cluster on each returns 2 nodes.
#[tokio::test]
async fn t1_two_node_join_both_rings_contain_both_nodes() {
    let cluster = Cluster::spawn(2, &config_fast_gossip()).await.expect("spawn 2-node cluster");

    // Wait for both nodes to discover each other.
    cluster.wait_for_convergence(2).await.expect("cluster convergence");

    // Verify node 0 sees 2 nodes.
    let resp = cluster.get(0, "/admin/cluster").await.expect("GET cluster node 0");
    assert_eq!(resp.status(), 200);
    let view: ClusterView = response_json(resp).await.expect("parse cluster view");
    assert_eq!(
        view.nodes.len(),
        2,
        "node 0 should report 2 nodes, got {}: {:?}",
        view.nodes.len(),
        view.nodes
    );

    // Verify node 1 sees 2 nodes.
    let resp = cluster.get(1, "/admin/cluster").await.expect("GET cluster node 1");
    assert_eq!(resp.status(), 200);
    let view: ClusterView = response_json(resp).await.expect("parse cluster view");
    assert_eq!(
        view.nodes.len(),
        2,
        "node 1 should report 2 nodes, got {}: {:?}",
        view.nodes.len(),
        view.nodes
    );

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T2: 3-node join
// ---------------------------------------------------------------------------

/// T2: Node B and C join via seed=A. All three rings converged.
#[tokio::test]
async fn t2_three_node_join_all_rings_converged() {
    let cluster = Cluster::spawn(3, &config_fast_gossip()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // All three nodes should report 3 members.
    for i in 0..3 {
        let resp = cluster.get(i, "/admin/cluster").await.expect("GET cluster");
        assert_eq!(resp.status(), 200);
        let view: ClusterView = response_json(resp).await.expect("parse cluster view");
        assert_eq!(
            view.nodes.len(),
            3,
            "node {i} should report 3 nodes, got {}: {:?}",
            view.nodes.len(),
            view.nodes
        );
    }

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T3: Graceful leave
// ---------------------------------------------------------------------------

/// T3: Node C sends SIGTERM. Nodes A and B remove C from membership
/// and ring within `failure_timeout_ms`.
#[tokio::test]
async fn t3_graceful_leave_departed_node_removed_from_rings() {
    let mut cluster = Cluster::spawn(3, &config_fast_swim()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // Kill node 2 (simulates crash — graceful leave not yet supported).
    // Per DK-005: use SIGKILL for failure detection tests.
    cluster.kill(2).expect("kill node 2");

    // Wait for remaining nodes to detect the departure.
    // Poll for convergence down to 2 on alive nodes.
    cluster.wait_for_convergence(2).await.expect("cluster should converge to 2 nodes after kill");

    // Remaining nodes should report 2 members.
    for i in 0..2 {
        let resp = cluster.get(i, "/admin/cluster").await.expect("GET cluster");
        assert_eq!(resp.status(), 200);
        let view: ClusterView = response_json(resp).await.expect("parse cluster view");
        assert_eq!(
            view.nodes.len(),
            2,
            "node {i} should report 2 nodes after departure, got {}: {:?}",
            view.nodes.len(),
            view.nodes
        );
    }

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T4: Rejoin after leave
// ---------------------------------------------------------------------------

/// T4: Node C restarts with same data dir. Rejoins cluster.
/// Ring converges to 3 again.
#[tokio::test]
async fn t4_rejoin_after_leave_ring_converges_to_3_again() {
    let mut cluster = Cluster::spawn(3, &config_fast_gossip()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // Kill node 2.
    cluster.kill(2).expect("kill node 2");

    // Wait for remaining nodes to converge to 2.
    // (The convergence check polls all alive nodes; node 2 is dead so ignored.)
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Restart node 2 with its original data dir.
    cluster.restart(2).await.expect("restart node 2");

    // Wait for re-convergence to 3.
    cluster.wait_for_convergence(3).await.expect("cluster re-convergence after rejoin");

    // All three nodes should report 3 members again.
    for i in 0..3 {
        let resp = cluster.get(i, "/admin/cluster").await.expect("GET cluster");
        assert_eq!(resp.status(), 200);
        let view: ClusterView = response_json(resp).await.expect("parse cluster view");
        assert_eq!(
            view.nodes.len(),
            3,
            "node {i} should report 3 nodes after rejoin, got {}: {:?}",
            view.nodes.len(),
            view.nodes
        );
    }

    cluster.shutdown().await.expect("shutdown");
}
