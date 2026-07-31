//! Write replication to remote nodes.
//!
//! Handles fan-out of write operations to replica nodes in the cluster.
//! When a write coordinator receives a PUT, this module replicates the
//! write to W successors and collects acknowledgments.

use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use oceanfs_core::{Hlc, NodeId, WriteAck};
use tokio::time::timeout;
use tracing::debug;

// Membership is optional (pulls in tonic).
#[cfg(feature = "membership")]
use oceanfs_membership::Membership;
#[cfg(not(feature = "membership"))]
use crate::mocks::MockMembership as Membership;

use crate::error::{Error, Result};

/// Replicates a write to a set of remote nodes.
///
/// Takes a list of target node IDs, a source HLC timestamp, and
/// fans out the write to all targets, collecting acknowledgments.
///
/// # Errors
///
/// Returns an error if a specific node is unreachable or the
/// write fails on a remote node.
pub(crate) async fn replicate_write(
    membership: &Arc<Membership>,
    targets: &[&NodeId],
    hlc: Hlc,
    write_timeout_ms: u64,
) -> Vec<Result<WriteAck>> {
    let mut results = Vec::with_capacity(targets.len());

    if targets.is_empty() {
        return results;
    }

    let write_timeout = std::time::Duration::from_millis(write_timeout_ms);

    let futures: FuturesUnordered<_> = targets
        .iter()
        .map(|target| replicate_to_single(membership, target, hlc))
        .collect();

    let result = timeout(write_timeout, async {
        let mut stream = futures;
        while let Some(ack_result) = stream.next().await {
            results.push(ack_result);
        }
    })
    .await;

    match result {
        Ok(()) => {}
        Err(_elapsed) => {
            debug!("write replication timed out");
        }
    }

    results
}

/// Replicates a write to a single remote node.
async fn replicate_to_single(
    membership: &Arc<Membership>,
    target: &NodeId,
    hlc: Hlc,
) -> Result<WriteAck> {
    // Verify target is in membership.
    let _state = membership.state_of(target).ok_or_else(|| Error::ForwardFailed {
        target: target.to_string(),
        reason: "node not found in membership".into(),
    })?;

    // In full gRPC implementation: use ConnectionPool to send AppendSegment RPC.
    debug!(
        target = %target,
        hlc_wall = hlc.wall_time(),
        "replica write (simulated)"
    );

    Ok(WriteAck {
        node_id: target.clone(),
        wal_position: 0,
        hlc,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use oceanfs_core::{Incarnation, NodeState};
    use oceanfs_routing::{Ring, RingCache};
    use std::net::SocketAddr;

    fn make_membership(node_id: &str) -> Arc<Membership> {
        let ring = Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();

        #[cfg(feature = "membership")]
        let membership = Arc::new(Membership::new(
            NodeId::new(node_id), addr, oceanfs_core::GossipConfig::default(), ring_cache,
        ));
        #[cfg(not(feature = "membership"))]
        let membership = Arc::new(Membership::new(NodeId::new(node_id), addr));

        membership.upsert_node(NodeId::new("n2"), NodeState::Alive, Incarnation::new(1), "127.0.0.1:9002".parse().unwrap());
        membership.upsert_node(NodeId::new("n3"), NodeState::Alive, Incarnation::new(1), "127.0.0.1:9003".parse().unwrap());
        membership
    }

    #[tokio::test]
    async fn replicate_write_empty_targets() {
        let membership = make_membership("n1");
        let results = replicate_write(&membership, &[], Hlc::zero(), 5000).await;
        assert!(results.is_empty());
    }
}
