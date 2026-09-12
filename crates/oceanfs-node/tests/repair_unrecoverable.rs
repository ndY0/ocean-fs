//! ae1 S1 integration: the re-replication dispatcher's real sweep loop
//! classifies a no-live-holder repair as terminal and resumes it when a
//! recorded holder returns.
//!
//! The unit tests drive the private sweep directly; this test exercises
//! the public wiring end to end: `RepairDispatcher::run` with the real
//! 5 s cadence, a real `Membership`, and the public
//! `with_unrecoverable_sweeps`/`is_unrecoverable`/`unrecoverable_len`
//! API.

use std::{sync::Arc, time::Duration};

use oceanfs_core::{
    GossipConfig, Incarnation, LifecycleConfig, NodeId, NodeState, RingConfig, SizeTier,
};
use oceanfs_durability::healing_service::{ReRepRequest, RepairReason, RepairSink};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_node::repair::{ManifestRepairTargetSelector, RepairDispatcher};
use oceanfs_routing::{Ring, RingCache};
use oceanfs_storage::segment::lifecycle::SegmentLifecycleCoordinator;
use tokio_util::sync::CancellationToken;

fn membership_with_self() -> (Arc<Membership>, NodeId) {
    let self_id = NodeId::new("n1");
    let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
    ring.add_node(self_id.clone());
    let ring = Arc::new(RingCache::new(ring));
    let membership = Arc::new(Membership::new(
        self_id.clone(),
        "127.0.0.1:19100".parse().expect("self grpc addr"),
        "127.0.0.1:19101".parse().expect("self gossip addr"),
        GossipConfig::default(),
        ring,
    ));
    (membership, self_id)
}

fn request(segment_id: oceanfs_core::SegmentId, holders: Vec<NodeId>) -> ReRepRequest {
    ReRepRequest {
        origin: NodeId::new("origin"),
        segment_id,
        holders,
        reason: RepairReason::Reconciliation,
        retry_count: 0,
        merkle_root: None,
        tier: SizeTier::Standard,
        ec_k: 1,
        ec_m: 0,
    }
}

/// A parked repair whose recorded holder is absent from membership
/// becomes terminal through the real sweep loop (N=1), and a recorded
/// holder returning clears the marker (both directions).
#[tokio::test(flavor = "multi_thread")]
async fn dispatcher_loop_classifies_and_resumes_terminal_repair() {
    let (membership, self_id) = membership_with_self();
    let dispatcher = Arc::new(
        RepairDispatcher::new(
            Arc::new(ManifestRepairTargetSelector::new(membership.clone(), self_id.clone())),
            Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            membership.clone(),
            Arc::new(SegmentLifecycleCoordinator::new(&LifecycleConfig::default())),
            self_id,
        )
        .with_unrecoverable_sweeps(1),
    );

    let shutdown = CancellationToken::new();
    let runner = tokio::spawn({
        let dispatcher = dispatcher.clone();
        let shutdown = shutdown.clone();
        async move { dispatcher.run(shutdown).await }
    });

    let segment_id = oceanfs_core::SegmentId::new();
    // "n2" is a recorded holder but is NOT in membership → no live holder.
    dispatcher
        .enqueue(request(segment_id, vec![NodeId::new("n2")]))
        .await
        .expect("enqueue is always ok");

    // First sweep (5 s cadence) classifies it terminal.
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if dispatcher.is_unrecoverable(&segment_id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("terminal classification within one sweep");
    assert_eq!(dispatcher.unrecoverable_len(), 1, "one terminal segment");
    assert_eq!(dispatcher.pending_len(), 0, "terminal segments are not parked");

    // The recorded holder returns → the next sweep clears the marker and
    // the request re-enters normal parking (a live holder but no free
    // target is the honest cannot-reach-RF state, not unrecoverable).
    membership.upsert_node(
        NodeId::new("n2"),
        NodeState::Alive,
        Incarnation::new(1),
        Some("127.0.0.1:19200".parse().expect("holder addr")),
    );
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if !dispatcher.is_unrecoverable(&segment_id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("terminal marker cleared when the recorded holder returns");
    assert_eq!(dispatcher.unrecoverable_len(), 0, "terminal set is empty again");
    assert_eq!(dispatcher.pending_len(), 1, "resumed request parks normally");

    shutdown.cancel();
    let _ = runner.await;
}
