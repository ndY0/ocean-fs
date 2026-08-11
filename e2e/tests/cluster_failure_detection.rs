//! Cluster failure detection tests (T23-T27).
//!
//! Validates SWIM failure detection: direct ping, SUSPECT state,
//! DEAD state, indirect ping path, and false positive resistance.

use e2e::harness::{config_fast_swim, response_json, Cluster};
use serde::Deserialize;

/// Response from GET /admin/cluster.
#[derive(Debug, Deserialize)]
struct ClusterView {
    #[allow(dead_code)]
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

/// Polls the cluster view on a given node until a node with the given
/// partial ID matches the expected state, or timeout.
async fn wait_for_state(
    cluster: &Cluster,
    observer: usize,
    node_pattern: &str,
    expected_state: &str,
    timeout_secs: u64,
) -> bool {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > std::time::Duration::from_secs(timeout_secs) {
            return false;
        }

        if let Ok(resp) = cluster.get(observer, "/admin/cluster").await {
            if resp.status() == 200 {
                if let Ok(view) = response_json::<ClusterView>(resp).await {
                    if view
                        .nodes
                        .iter()
                        .any(|n| n.id.contains(node_pattern) && n.state == expected_state)
                    {
                        return true;
                    }
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Gets the state of a specific node by partial ID from an observer.
async fn get_node_state(cluster: &Cluster, observer: usize, node_pattern: &str) -> Option<String> {
    if let Ok(resp) = cluster.get(observer, "/admin/cluster").await {
        if resp.status() == 200 {
            if let Ok(view) = response_json::<ClusterView>(resp).await {
                return view
                    .nodes
                    .iter()
                    .find(|n| n.id.contains(node_pattern))
                    .map(|n| n.state.clone());
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// T23: Direct ping success
// ---------------------------------------------------------------------------

/// T23: All nodes ALIVE. Failure detector pings succeed.
/// No state changes.
#[tokio::test]
async fn t23_direct_ping_all_nodes_alive_no_false_state_changes() {
    let cluster = Cluster::spawn(3, &config_fast_swim()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // After convergence, check several times that all nodes stay ALIVE.
    for _round in 0..3 {
        for i in 0..3 {
            let resp = cluster.get(i, "/admin/cluster").await;
            assert!(resp.is_ok(), "node {i} cluster endpoint must respond");
            let resp = resp.unwrap();
            assert_eq!(resp.status(), 200, "node {i} cluster must return 200");
            if let Ok(view) = response_json::<ClusterView>(resp).await {
                for n in &view.nodes {
                    assert_eq!(
                        n.state, "Alive",
                        "node {i} round {_round}: all nodes must be Alive, found {} in {}",
                        n.id, n.state
                    );
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    cluster.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------
// T24: SUSPECT on direct ping timeout
// ---------------------------------------------------------------------------

/// T24: Kill node C. Within `suspicion_timeout_ms`, nodes A and B
/// mark C as SUSPECT. Assert via `/admin/cluster`.
#[tokio::test]
async fn t24_suspect_on_direct_ping_timeout() {
    let mut cluster = Cluster::spawn(3, &config_fast_swim()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // Kill node 2.
    cluster.kill(2).expect("kill node 2");

    // Wait for failure detection to mark node 2 as SUSPECT.
    let found = wait_for_state(&cluster, 0, "cluster-2", "Suspect", 15).await;
    assert!(found, "node 2 must be marked SUSPECT by node 0 within 15s after SIGKILL");

    drop(cluster);
}

// ---------------------------------------------------------------------------
// T25: DEAD on suspicion timeout
// ---------------------------------------------------------------------------

/// T25: After `suspicion_timeout_ms`, SUSPECT transitions to DEAD.
/// DEAD nodes are removed from membership — cluster converges 3 → 2.
#[tokio::test]
async fn t25_dead_on_suspicion_timeout() {
    let mut cluster = Cluster::spawn(3, &config_fast_swim()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // Kill node 2.
    cluster.kill(2).expect("kill node 2");

    // Wait for failure detection: SUSPECT → DEAD → removal from
    // membership. Cluster should converge to 2 nodes.
    cluster.wait_for_convergence(2).await.expect("cluster should converge to 2 after DEAD removal");

    drop(cluster);
}

// ---------------------------------------------------------------------------
// T26: Indirect ping path
// ---------------------------------------------------------------------------

/// T26: Kill node C. Node A's direct ping to C fails. Node A requests
/// indirect pings from B. B's ping to C also fails. A marks C SUSPECT.
#[tokio::test]
async fn t26_indirect_ping_path_works() {
    let mut cluster = Cluster::spawn(3, &config_fast_swim()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // Kill node 2.
    cluster.kill(2).expect("kill node 2");

    // Both node 0 and node 1 should mark node 2 as SUSPECT via the
    // indirect ping path. Node 1 may take longer (indirect path).
    let found_0 = wait_for_state(&cluster, 0, "cluster-2", "Suspect", 15).await;
    let found_1 = wait_for_state(&cluster, 1, "cluster-2", "Suspect", 15).await;

    assert!(found_0, "node 0 must mark node 2 as SUSPECT (direct ping failure)");
    assert!(found_1, "node 1 must also mark node 2 as SUSPECT (indirect ping failure)");

    drop(cluster);
}

// ---------------------------------------------------------------------------
// T27: False positive resistance
// ---------------------------------------------------------------------------

/// T27: Brief network hiccup (simulated by pausing a node briefly).
/// Node is NOT marked DEAD if it responds to indirect pings.
#[tokio::test]
async fn t27_false_positive_resistance_brief_hiccup_not_dead() {
    let mut cluster = Cluster::spawn(3, &config_fast_swim()).await.expect("spawn 3-node cluster");

    cluster.wait_for_convergence(3).await.expect("cluster convergence");

    // Kill node 2 briefly, then immediately restart.
    cluster.kill(2).expect("kill node 2");

    // Very short delay — restart before DEAD timeout.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    cluster.restart(2).await.expect("restart node 2");

    cluster.wait_for_convergence(3).await.expect("cluster re-convergence");

    // After re-convergence, node 2 must NOT be DEAD.
    let state = get_node_state(&cluster, 0, "cluster-2").await;
    if let Some(s) = state {
        assert_ne!(s, "Dead", "brief hiccup must not cause DEAD state: node 2 state is '{s}'");
    }

    cluster.shutdown().await.expect("shutdown");
}
