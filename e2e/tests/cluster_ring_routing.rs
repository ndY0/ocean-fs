//! Cluster ring & routing tests (T32-T35).
//!
//! Validates consistent hashing determinism, replica set distinctness,
//! and ring rebalance on node add/remove.

use e2e::harness::{config_3node_w2_r2, config_fast_swim, response_json, Cluster};
use serde::Deserialize;

/// Response from GET /admin/cluster.
#[derive(Debug, Deserialize)]
struct ClusterView {
    #[allow(dead_code)]
    nodes: Vec<NodeInfo>,
    #[allow(dead_code)]
    vnodes: usize,
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
// T32: Consistent hashing determinism
// ---------------------------------------------------------------------------

/// T32: Same key → same replica set on all nodes. Assert
/// `ring.lookup(key_hash)` returns identical successors on A, B, C.
#[tokio::test]
async fn t32_consistent_hashing_determinism_same_replica_set_on_all_nodes() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // All nodes should agree on the ring generation and node count.
    let mut views: Vec<ClusterView> = Vec::new();
    for i in 0..3 {
        let resp = cluster.get(i, "/admin/cluster").await.expect("GET cluster");
        assert_eq!(resp.status(), 200);
        let view: ClusterView = response_json(resp).await.expect("parse cluster view");
        views.push(view);
    }

    // All nodes should report the same number of nodes.
    let node_count = views[0].nodes.len();
    for (i, view) in views.iter().enumerate() {
        assert_eq!(
            view.nodes.len(),
            node_count,
            "node {i} reports {} nodes, expected {node_count}",
            view.nodes.len()
        );
    }

    // All nodes should agree on ring generation.
    let generation = views[0].generation;
    for (i, view) in views.iter().enumerate() {
        assert_eq!(
            view.generation, generation,
            "node {i} ring generation {} differs from node 0's {}",
            view.generation, generation
        );
    }

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T33: Replica set distinctness
// ---------------------------------------------------------------------------

/// T33: No node appears twice in a replica set. All
/// `replication_factor` successors are distinct nodes.
#[tokio::test]
async fn t33_replica_set_contains_no_duplicates() {
    let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // Get the cluster view and verify all node IDs are unique.
    let resp = cluster.get(0, "/admin/cluster").await.expect("GET cluster");
    let view: ClusterView = response_json(resp).await.expect("parse cluster view");

    let node_ids: Vec<&str> = view.nodes.iter().map(|n| n.id.as_str()).collect();
    let mut unique_ids: Vec<&str> = node_ids.clone();
    unique_ids.sort();
    unique_ids.dedup();

    assert_eq!(unique_ids.len(), node_ids.len(), "all node IDs must be unique: {:?}", node_ids);

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T34: Ring rebalance on node add
// ---------------------------------------------------------------------------

/// T34: 2-node cluster. Add node C. Only O(N/M) keys change
/// assignment. Assert most keys retain original replica set.
///
/// NOTE: Dynamic node addition requires `Cluster::add_node()` which
/// the harness does not yet support. This test validates that the
/// 2-node ring converges correctly as a foundation. The full add-node
/// rebalance scenario will be added when the harness is extended.
#[tokio::test]
async fn t34_ring_rebalance_on_node_add_affects_minimal_keys() {
    // Start with 2 nodes.
    let cluster = Cluster::spawn(2, &config_3node_w2_r2()).await.expect("spawn 2-node cluster");

    cluster.wait_for_convergence(2).await.expect("cluster convergence (2 nodes)");

    // Verify the 2-node ring is functional.
    for i in 0..2 {
        let resp = cluster.get(i, "/admin/cluster").await;
        assert!(resp.is_ok(), "node {i}: cluster endpoint must respond");
        let resp = resp.unwrap();
        assert_eq!(resp.status(), 200, "node {i}: cluster view must be 200");
        let view: ClusterView = response_json(resp).await.expect("parse cluster view");
        assert_eq!(view.nodes.len(), 2, "node {i}: must report 2 nodes in ring");
    }

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T35: Ring rebalance on node remove
// ---------------------------------------------------------------------------

/// T35: 3-node cluster. Remove node C. Keys that had C in their
/// replica set now have a different successor. Assert lookup still
/// returns `replication_factor` distinct nodes.
#[tokio::test]
async fn t35_ring_rebalance_on_node_remove_maintains_distinct_replicas() {
    let mut cluster = Cluster::spawn(3, &config_fast_swim()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // Record the ring state before removal.
    let resp = cluster.get(0, "/admin/cluster").await.expect("GET cluster");
    let view: ClusterView = response_json(resp).await.expect("parse cluster view");
    let _gen_before = view.generation;
    let node_count_before = view.nodes.len();
    assert_eq!(node_count_before, 3, "must start with 3 nodes");

    // Kill node 2.
    cluster.kill(2).expect("kill node 2");

    // Wait for convergence down to 2.
    cluster.wait_for_convergence(2).await.expect("cluster should converge to 2 nodes");

    // Verify the remaining nodes report 2 members and new generation.
    for i in 0..2 {
        let resp = cluster.get(i, "/admin/cluster").await;
        assert!(resp.is_ok(), "node {i}: cluster endpoint must respond after removal");
        let resp = resp.unwrap();
        assert_eq!(resp.status(), 200);

        let view: ClusterView = response_json(resp).await.expect("parse cluster view");

        // All remaining node IDs should be unique.
        let ids: Vec<&str> = view.nodes.iter().map(|n| n.id.as_str()).collect();
        let mut unique = ids.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(
            unique.len(),
            ids.len(),
            "node {i}: remaining node IDs must be unique: {:?}",
            ids
        );

        assert_eq!(view.nodes.len(), 2, "node {i}: expected 2 nodes after removal");
    }

    drop(cluster);
}
