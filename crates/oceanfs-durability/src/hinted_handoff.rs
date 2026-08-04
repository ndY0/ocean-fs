//! Hinted handoff — buffers writes for temporarily unreachable nodes.
//!
//! When a replica node is unreachable during a write, the coordinator
//! selects a fallback node that stores the write with a hint
//! `{intended_for: unreachable_node}`. When the intended node returns
//! (detected via membership gossip), the fallback node pushes the
//! buffered data and clears the hint.
//!
//! ## Interface
//!
//! - [`HintedHandoff::handoff()`]: store a write for an unreachable node.
//! - [`HintedHandoff::deliver_pending()`]: push buffered hints to a
//!   returned node.
//! - [`HintedHandoff::pending_count()`]: return the number of pending hints.
//!
//! Per performance guideline §2.6 (bounded channels) and §4.5 (adaptive
//! per-operation timeouts).

use std::{collections::HashMap, sync::Arc};

use oceanfs_core::{Hlc, NodeId, OperationTimeouts, SegmentId};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::{
    error::{Error, Result},
    healing_rpc::{HintRequest, HintResponse},
    HealingRpcClient,
};

/// Maximum number of pending hints across all nodes.
const MAX_PENDING_HINTS: usize = 10_000;

/// Maximum hints per node to prevent a single node from monopolizing storage.
const MAX_HINTS_PER_NODE: usize = 1_000;

/// A buffered write intended for a specific node.
///
/// Each hint is keyed by the intended node. When the node returns,
/// all hints for that node are delivered in batch.
#[derive(Debug, Clone)]
pub struct HintRecord {
    /// The node this write was originally intended for.
    pub intended_for: NodeId,
    /// The segment containing the blob data.
    pub segment_id: SegmentId,
    /// Byte offset of the blob within the segment.
    pub offset: u64,
    /// Length of the blob in bytes.
    pub length: u32,
    /// HLC timestamp of the original write (for conflict resolution on delivery).
    pub timestamp: Hlc,
    /// The actual blob data to deliver.
    pub data: Vec<u8>,
}

/// Manages hinted handoff storage and delivery.
///
/// Stores pending writes for unreachable nodes and delivers them
/// when the nodes return to the cluster.
///
/// # Examples
///
/// ```
/// use oceanfs_core::NodeId;
/// use oceanfs_durability::HintedHandoff;
///
/// let handoff = HintedHandoff::new();
/// assert_eq!(handoff.pending_count(&NodeId::new("n1")), 0);
/// ```
pub struct HintedHandoff {
    /// Pending hints, keyed by the intended recipient node.
    /// Uses a `RwLock<HashMap>` for read-heavy access (reads >> writes).
    hints: RwLock<HashMap<NodeId, Vec<HintRecord>>>,
    /// Connection pool for delivering hints to returning nodes.
    pool: Arc<ConnectionPool>,
    /// Membership for resolving node addresses.
    membership: Option<Arc<Membership>>,
}

impl HintedHandoff {
    /// Creates a new hinted handoff buffer with a connection pool and optional membership.
    pub fn new_with_pool_and_membership(
        pool: Arc<ConnectionPool>,
        membership: Option<Arc<Membership>>,
    ) -> Self {
        Self { hints: RwLock::new(HashMap::new()), pool, membership }
    }

    /// Creates a new empty hinted handoff buffer (without a connection pool).
    /// Used primarily for testing.
    pub fn new_with_pool(pool: Arc<ConnectionPool>) -> Self {
        Self { hints: RwLock::new(HashMap::new()), pool, membership: None }
    }

    /// Creates a new empty hinted handoff buffer (without a connection pool).
    /// Used primarily for testing.
    pub fn new() -> Self {
        Self {
            hints: RwLock::new(HashMap::new()),
            pool: Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default())),
            membership: None,
        }
    }

    /// Stores a hinted write for a temporarily unreachable node.
    ///
    /// Called by the write coordinator when a replica node is unreachable
    /// and a fallback node is used instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the hint storage is at capacity.
    pub async fn handoff(&self, intended_for: NodeId, entry: HintRecord) -> Result<()> {
        {
            let hints = self.hints.read();
            let per_node_count = hints.get(&intended_for).map(|v| v.len()).unwrap_or(0);
            if per_node_count >= MAX_HINTS_PER_NODE {
                return Err(Error::Internal(format!(
                    "hinted handoff storage full for node {intended_for}: {per_node_count} hints"
                )));
            }

            let total: usize = hints.values().map(|v| v.len()).sum();
            if total >= MAX_PENDING_HINTS {
                return Err(Error::Internal(format!(
                    "hinted handoff storage full: {total} hints total"
                )));
            }
        }

        debug!(
            intended_for = %intended_for,
            segment_id = %entry.segment_id,
            "storing hinted handoff"
        );

        {
            let mut hints = self.hints.write();
            let pending = hints.entry(intended_for.clone()).or_default();
            pending.push(entry);
        }

        info!(
            intended_for = %intended_for,
            pending_count = self.pending_count(&intended_for),
            "hinted handoff stored"
        );

        Ok(())
    }

    /// Delivers all pending hints for a returned node via gRPC.
    ///
    /// Called when a node transitions to `Alive` state in the membership.
    /// Pushes buffered hint data to the node via `HealingRpcClient::hinted_handoff`,
    /// then clears delivered hints from local storage.
    ///
    /// # Returns
    ///
    /// The number of hints successfully delivered.
    ///
    /// # Errors
    ///
    /// Returns an error if delivery fails for all hints.
    pub async fn deliver_pending(&self, node: NodeId) -> Result<usize> {
        let hints_to_deliver = {
            let hints = self.hints.read();
            hints.get(&node).cloned().unwrap_or_default()
        };

        if hints_to_deliver.is_empty() {
            debug!(node = %node, "no pending hints to deliver");
            return Ok(0);
        }

        info!(
            node = %node,
            count = hints_to_deliver.len(),
            "delivering pending hints via gRPC"
        );

        let mut delivered = 0usize;

        for hint in &hints_to_deliver {
            match self.deliver_single(&node, hint).await {
                Ok(()) => {
                    delivered += 1;
                    debug!(
                        node = %node,
                        segment_id = %hint.segment_id,
                        "hint delivered successfully"
                    );
                }
                Err(e) => {
                    warn!(
                        node = %node,
                        segment_id = %hint.segment_id,
                        error = %e,
                        "hint delivery failed; will retry later"
                    );
                }
            }
        }

        // Remove successfully delivered hints.
        if delivered > 0 {
            let mut hints = self.hints.write();
            if let Some(pending) = hints.get_mut(&node) {
                pending.drain(..delivered.min(pending.len()));
                if pending.is_empty() {
                    hints.remove(&node);
                }
            }
        }

        Ok(delivered)
    }

    /// Returns the number of pending hints for a given node.
    pub fn pending_count(&self, node: &NodeId) -> usize {
        let hints = self.hints.read();
        hints.get(node).map(|v| v.len()).unwrap_or(0)
    }

    /// Returns the total number of pending hints across all nodes.
    pub fn total_pending_count(&self) -> usize {
        let hints = self.hints.read();
        hints.values().map(|v| v.len()).sum()
    }

    /// Sets the membership reference for address resolution.
    pub fn with_membership(mut self, membership: Arc<Membership>) -> Self {
        self.membership = Some(membership);
        self
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Delivers a single hint to a remote node via gRPC `HealingRpc::hinted_handoff`.
    ///
    /// 1. Resolves the node's address from membership.
    /// 2. Acquires a channel from the connection pool.
    /// 3. Builds a `HealingRpcClient` and sends the hint via gRPC.
    /// 4. Returns `Ok(())` on successful delivery.
    async fn deliver_single(&self, node: &NodeId, hint: &HintRecord) -> Result<()> {
        let timeout_ms = OperationTimeouts::default().hint_delivery_ms;

        let membership = self
            .membership
            .as_ref()
            .ok_or_else(|| Error::Internal("no membership available for hint delivery".into()))?;

        let addr = membership.address_of(node).ok_or_else(|| Error::ForwardFailed {
            target: node.to_string(),
            reason: "node address not found in membership".into(),
        })?;

        let pooled = self.pool.get_channel(addr).await.map_err(|e| Error::ForwardFailed {
            target: node.to_string(),
            reason: format!("connection pool error: {e}"),
        })?;

        let channel = pooled.channel().clone();
        drop(pooled);

        let mut client = HealingRpcClient::new(channel);

        let proto_intended: oceanfs_core::proto::common::NodeId = hint.intended_for.clone().into();
        let proto_segment_id: oceanfs_core::proto::common::SegmentId = hint.segment_id.into();
        let proto_hlc: oceanfs_core::proto::common::HlcTimestamp = hint.timestamp.into();

        let request = HintRequest {
            intended_for: Some(proto_intended),
            segment_id: Some(proto_segment_id),
            data: hint.data.clone(),
            hlc: Some(proto_hlc),
        };

        let delivery = async {
            let response: tonic::Response<HintResponse> =
                client.hinted_handoff(request).await.map_err(|status| Error::ForwardFailed {
                    target: node.to_string(),
                    reason: format!("gRPC hint delivery failed: {status}"),
                })?;

            let resp = response.into_inner();
            if !resp.accepted {
                return Err(Error::ForwardFailed {
                    target: node.to_string(),
                    reason: "remote node rejected hint".into(),
                });
            }

            Ok(())
        };

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), delivery).await;

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_elapsed) => Err(Error::Timeout { elapsed_ms: timeout_ms }),
        }
    }
}

impl Default for HintedHandoff {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn new_handoff_is_empty() {
        let hh = HintedHandoff::new();
        assert_eq!(hh.pending_count(&NodeId::new("n1")), 0);
        assert_eq!(hh.total_pending_count(), 0);
    }

    #[tokio::test]
    async fn handoff_creates_hint() {
        let hh = HintedHandoff::new();
        let node = NodeId::new("n1");

        let hint = HintRecord {
            intended_for: node.clone(),
            segment_id: SegmentId::new(),
            offset: 0,
            length: 100,
            timestamp: Hlc::zero(),
            data: vec![1, 2, 3],
        };

        hh.handoff(node.clone(), hint).await.unwrap();
        assert_eq!(hh.pending_count(&node), 1);
        assert_eq!(hh.total_pending_count(), 1);
    }

    #[tokio::test]
    async fn handoff_multiple_hints() {
        let hh = HintedHandoff::new();
        let node_a = NodeId::new("a");
        let node_b = NodeId::new("b");

        hh.handoff(
            node_a.clone(),
            HintRecord {
                intended_for: node_a.clone(),
                segment_id: SegmentId::new(),
                offset: 0,
                length: 50,
                timestamp: Hlc::zero(),
                data: vec![1],
            },
        )
        .await
        .unwrap();

        hh.handoff(
            node_a.clone(),
            HintRecord {
                intended_for: node_a.clone(),
                segment_id: SegmentId::new(),
                offset: 50,
                length: 50,
                timestamp: Hlc::zero(),
                data: vec![2],
            },
        )
        .await
        .unwrap();

        hh.handoff(
            node_b.clone(),
            HintRecord {
                intended_for: node_b.clone(),
                segment_id: SegmentId::new(),
                offset: 0,
                length: 200,
                timestamp: Hlc::zero(),
                data: vec![3],
            },
        )
        .await
        .unwrap();

        assert_eq!(hh.pending_count(&node_a), 2);
        assert_eq!(hh.pending_count(&node_b), 1);
        assert_eq!(hh.total_pending_count(), 3);
    }

    #[tokio::test]
    async fn deliver_pending_clears_hints() {
        let hh = HintedHandoff::new();
        let node = NodeId::new("n1");

        hh.handoff(
            node.clone(),
            HintRecord {
                intended_for: node.clone(),
                segment_id: SegmentId::new(),
                offset: 0,
                length: 100,
                timestamp: Hlc::zero(),
                data: vec![1, 2, 3],
            },
        )
        .await
        .unwrap();

        // Without membership, delivery will fail with an internal error.
        // The hint remains pending after failed delivery.
        let delivered = hh.deliver_pending(node.clone()).await.unwrap();
        assert_eq!(delivered, 0, "no membership means delivery fails");
        assert_eq!(hh.pending_count(&node), 1, "failed delivery keeps hints");
    }

    #[tokio::test]
    async fn deliver_pending_no_hints_returns_zero() {
        let hh = HintedHandoff::new();
        let delivered = hh.deliver_pending(NodeId::new("unknown")).await.unwrap();
        assert_eq!(delivered, 0);
    }

    #[tokio::test]
    async fn pending_count_accurate_after_handoff() {
        let hh = HintedHandoff::new();
        let node = NodeId::new("target");

        assert_eq!(hh.pending_count(&node), 0);

        hh.handoff(
            node.clone(),
            HintRecord {
                intended_for: node.clone(),
                segment_id: SegmentId::new(),
                offset: 0,
                length: 42,
                timestamp: Hlc::zero(),
                data: vec![1],
            },
        )
        .await
        .unwrap();

        assert_eq!(hh.pending_count(&node), 1);
    }

    #[tokio::test]
    async fn handoff_rejects_when_node_at_capacity() {
        let hh = HintedHandoff::new();
        let node = NodeId::new("full-node");

        // Fill up to MAX_HINTS_PER_NODE (1000).
        for i in 0..1000 {
            hh.handoff(
                node.clone(),
                HintRecord {
                    intended_for: node.clone(),
                    segment_id: SegmentId::new(),
                    offset: i as u64,
                    length: 10,
                    timestamp: Hlc::zero(),
                    data: vec![i as u8],
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(hh.pending_count(&node), 1000);

        // One more should be rejected.
        let result = hh
            .handoff(
                node.clone(),
                HintRecord {
                    intended_for: node.clone(),
                    segment_id: SegmentId::new(),
                    offset: 1000,
                    length: 10,
                    timestamp: Hlc::zero(),
                    data: vec![0],
                },
            )
            .await;
        assert!(result.is_err(), "should reject when node at capacity");
    }

    // ── Duplicate hint behavior ──────────────────────────────────

    #[tokio::test]
    async fn handoff_duplicate_hints_are_stored_separately() {
        // Current behavior: duplicate hints (same data) are stored
        // multiple times. No deduplication is performed.
        let hh = HintedHandoff::new();
        let node = NodeId::new("dup-node");

        let hint = HintRecord {
            intended_for: node.clone(),
            segment_id: SegmentId::new(),
            offset: 0,
            length: 10,
            timestamp: Hlc::zero(),
            data: vec![1, 2, 3],
        };

        // Store the same hint twice.
        hh.handoff(node.clone(), hint.clone()).await.unwrap();
        hh.handoff(node.clone(), hint).await.unwrap();

        // Both are counted — no deduplication.
        assert_eq!(hh.pending_count(&node), 2, "duplicate hints are stored as separate entries");
        assert_eq!(hh.total_pending_count(), 2);
    }

    // ── Delivery with unreachable remote ─────────────────────────

    #[tokio::test]
    async fn deliver_pending_with_unreachable_remote_retains_hints() {
        // When membership is present but the remote node's gRPC server
        // is not running, delivery should fail and hints should be
        // retained for retry.
        use std::{net::SocketAddr, sync::Arc};

        let node_id = NodeId::new("returned-node");
        let addr: SocketAddr = "127.0.0.1:19999".parse().unwrap();

        // Create a membership with the returned node but no gRPC server.
        let ring = oceanfs_routing::Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(oceanfs_routing::RingCache::new(ring));
        let membership = Arc::new(Membership::new(
            node_id.clone(),
            addr,
            oceanfs_core::GossipConfig::default(),
            ring_cache,
        ));
        membership.upsert_node(
            node_id.clone(),
            oceanfs_core::NodeState::Alive,
            oceanfs_core::Incarnation::new(1),
            addr,
        );

        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
        let hh = HintedHandoff::new_with_pool_and_membership(pool, Some(membership));

        // Store a hint for the returned node.
        let hint = HintRecord {
            intended_for: node_id.clone(),
            segment_id: SegmentId::new(),
            offset: 0,
            length: 42,
            timestamp: Hlc::zero(),
            data: vec![9, 8, 7],
        };
        hh.handoff(node_id.clone(), hint).await.unwrap();
        assert_eq!(hh.pending_count(&node_id), 1);

        // Attempt delivery. The gRPC server is not running, so delivery
        // should fail (connection refused). Hints should be retained.
        let delivered = hh.deliver_pending(node_id.clone()).await.unwrap();
        assert_eq!(delivered, 0, "delivery should fail with no gRPC server");
        assert_eq!(hh.pending_count(&node_id), 1, "hints retained after failed delivery");

        // A second delivery attempt should also fail (hints still retained).
        let delivered2 = hh.deliver_pending(node_id.clone()).await.unwrap();
        assert_eq!(delivered2, 0, "retry should also fail");
        assert_eq!(hh.pending_count(&node_id), 1, "hints still retained after retry failure");
    }
}
