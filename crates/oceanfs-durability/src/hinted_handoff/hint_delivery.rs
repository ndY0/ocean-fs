//! Batched hinted handoff delivery manager.
//!
//! `HintedHandoffManager` bridges the `HintWal` (persistent write-ahead log)
//! with in-memory queues and batched gRPC delivery. When a node returns
//! to the cluster, all pending hints for that node are drained from the
//! queue and sent in a single RPC call.
//!
//! ## Architecture
//!
//! ```text
//! enqueue(record)
//!   ├→ HintWal::write_hint()      [persist to WAL]
//!   └→ queues[record.intended_for] [in-memory for fast lookup]
//!
//! drain_and_deliver(target)
//!   ├→ drain queues[target]
//!   ├→ build HintedHandoffRequest { hints: repeated }
//!   ├→ gRPC: client.hinted_handoff(request)
//!   └→ on success: HintWal::truncate_after(last_position)
//! ```

use std::{collections::VecDeque, net::SocketAddr, sync::Arc, time::Duration};

use dashmap::DashMap;
use oceanfs_core::{NodeId, OperationTimeouts};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use tracing::{debug, info, warn};

use crate::{
    error::{Error, Result},
    healing_rpc::healing_rpc_client::HealingRpcClient,
    hinted_handoff_rpc::{HintRecord, HintedHandoffRequest, HintedHandoffResponse},
    HintWal,
};

/// Configuration for hinted handoff delivery.
///
/// Controls the WAL file location, inline/blob threshold,
/// and maximum batch size per delivery.
#[derive(Debug, Clone)]
pub struct HintedHandoffConfig {
    /// Path to the hinted handoff WAL file.
    pub wal_path: std::path::PathBuf,
    /// Maximum blob size stored inline in the hinted handoff WAL (bytes).
    /// Blobs above this threshold are stored as segment references.
    /// Default: 4096 (4 KB).
    pub inline_threshold_bytes: u64,
    /// Maximum hints per batched gRPC delivery call.
    /// Default: 256.
    pub max_batch_size: usize,
}

impl Default for HintedHandoffConfig {
    fn default() -> Self {
        Self {
            wal_path: std::path::PathBuf::from("/var/lib/oceanfs/hints.wal"),
            inline_threshold_bytes: 4096,
            max_batch_size: 256,
        }
    }
}

/// Client abstraction for delivering hinted handoff records.
///
/// Allows testing with mock gRPC clients without requiring a live server.
#[async_trait::async_trait]
pub trait HintDeliveryClient: Send + Sync {
    /// Delivers a batch of hint records to a remote node.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    async fn deliver_hints(
        &self,
        target_addr: SocketAddr,
        request: HintedHandoffRequest,
        timeout_ms: u64,
    ) -> std::result::Result<HintedHandoffResponse, Error>;
}

/// Real gRPC-based hint delivery client.
///
/// Uses `ConnectionPool` to acquire a channel and `HealingRpcClient`
/// to perform the hinted handoff RPC.
pub struct GrpcHintDeliveryClient {
    pool: Arc<ConnectionPool>,
}

impl GrpcHintDeliveryClient {
    /// Creates a new gRPC hint delivery client.
    pub fn new(pool: Arc<ConnectionPool>) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl HintDeliveryClient for GrpcHintDeliveryClient {
    async fn deliver_hints(
        &self,
        target_addr: SocketAddr,
        request: HintedHandoffRequest,
        timeout_ms: u64,
    ) -> std::result::Result<HintedHandoffResponse, Error> {
        let pooled =
            self.pool.get_channel(target_addr).await.map_err(|e| Error::ForwardFailed {
                target: target_addr.to_string(),
                reason: format!("connection pool error: {e}"),
            })?;

        let channel = pooled.channel().clone();
        drop(pooled);

        let mut client = HealingRpcClient::new(channel);

        let delivery = async {
            let response =
                client.hinted_handoff(request).await.map_err(|status| Error::ForwardFailed {
                    target: target_addr.to_string(),
                    reason: format!("gRPC hint delivery failed: {status}"),
                })?;

            Ok(response.into_inner())
        };

        match tokio::time::timeout(Duration::from_millis(timeout_ms), delivery).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(e),
            Err(_elapsed) => Err(Error::Timeout { elapsed_ms: timeout_ms }),
        }
    }
}

/// Manages hinted handoff persistence and delivery.
///
/// On `enqueue()`, writes the hint to the WAL for durability and
/// adds it to an in-memory queue keyed by the intended recipient node.
/// On `drain_and_deliver()`, drains all pending hints for a node
/// and sends them in a single batched gRPC call.
///
/// # Examples
///
/// ```ignore
/// // Requires tokio runtime; see integration tests.
/// use oceanfs_durability::{HintedHandoffManager, HintedHandoffConfig, HintWal};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let wal = Arc::new(HintWal::open("/tmp/hints.wal").await?);
/// let config = HintedHandoffConfig::default();
/// let manager = HintedHandoffManager::new(wal, config);
/// # Ok(())
/// # }
/// ```
pub struct HintedHandoffManager {
    /// Persistent WAL for hint records.
    hint_wal: Arc<HintWal>,
    /// Delivery client (gRPC or mock).
    delivery_client: Arc<dyn HintDeliveryClient>,
    /// In-memory queues: `NodeId → VecDeque<(start_position, end_position, HintRecord)>`.
    /// Uses `DashMap` for lock-free concurrent access across nodes.
    queues: DashMap<NodeId, VecDeque<(u64, u64, HintRecord)>>,
    /// Configuration.
    config: HintedHandoffConfig,
    /// Per-operation timeout configuration.
    timeouts: Arc<OperationTimeouts>,
    /// Membership for address resolution.
    membership: Option<Arc<Membership>>,
}

impl HintedHandoffManager {
    /// Creates a new hinted handoff manager.
    ///
    /// Requires a WAL for persistence and a delivery client for gRPC communication.
    /// To populate in-memory queues from an existing WAL, call `replay_and_enqueue()`.
    pub fn new(
        hint_wal: Arc<HintWal>,
        delivery_client: Arc<dyn HintDeliveryClient>,
        config: HintedHandoffConfig,
    ) -> Self {
        Self {
            hint_wal,
            delivery_client,
            queues: DashMap::new(),
            config,
            timeouts: Arc::new(OperationTimeouts::default()),
            membership: None,
        }
    }

    /// Sets the membership reference for address resolution.
    #[must_use]
    pub fn with_membership(mut self, membership: Arc<Membership>) -> Self {
        self.membership = Some(membership);
        self
    }

    /// Sets the per-operation timeout configuration.
    #[must_use]
    pub fn with_timeouts(mut self, timeouts: Arc<OperationTimeouts>) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Replays all records from the WAL and enqueues them in memory.
    ///
    /// Call this at startup to repopulate the in-memory queues from
    /// the persistent WAL after a restart.
    ///
    /// # Returns
    ///
    /// The number of records replayed and enqueued.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL replay fails.
    pub async fn replay_and_enqueue(&self) -> Result<usize> {
        let records = self.hint_wal.replay().await?;
        let count = records.len();

        for (start, end, record) in records {
            if let Some(target) = record.intended_for() {
                let mut queue = self.queues.entry(target.clone()).or_default();
                queue.push_back((start, end, record));
            } else {
                warn!(position = start, "replayed hint record with no intended_for; skipping");
            }
        }

        info!(count, "replayed and enqueued hint records from WAL");

        Ok(count)
    }

    /// Enqueues a hint record for delivery.
    ///
    /// Writes the record to the WAL for durability, then adds it to the
    /// in-memory queue for the intended recipient.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails.
    pub async fn enqueue(&self, record: HintRecord) -> Result<()> {
        let target = record
            .intended_for()
            .ok_or_else(|| Error::Internal("hint record has no intended_for field".into()))?;

        // Write to WAL first for durability.
        let (position, end_position) = self.hint_wal.write_hint(&record).await?;

        // Then add to in-memory queue.
        let mut queue = self.queues.entry(target.clone()).or_default();
        queue.push_back((position, end_position, record));

        debug!(
            target = %target,
            position,
            queue_len = queue.len(),
            "enqueued hint record"
        );

        Ok(())
    }

    /// Drains all pending hints for a target node and delivers them in a batch.
    ///
    /// 1. Drains the in-memory queue for `target`.
    /// 2. Builds a `HintedHandoffRequest` with all pending hints.
    /// 3. Resolves the target's address via membership.
    /// 4. Sends the batched request via gRPC.
    /// 5. On success: truncates the WAL and clears the queue.
    /// 6. On failure: re-enqueues hints at the front of the queue and returns an error.
    ///
    /// # Returns
    ///
    /// The number of hints successfully delivered.
    ///
    /// # Errors
    ///
    /// Returns an error if delivery fails.
    pub async fn drain_and_deliver(&self, target: NodeId) -> Result<usize> {
        // Drain the queue for this target.
        let drained: Vec<(u64, u64, HintRecord)> = {
            let mut queue = self.queues.entry(target.clone()).or_default();
            let batch_size = queue.len().min(self.config.max_batch_size);
            let items: Vec<_> = queue.drain(..batch_size).collect();
            items
        };

        if drained.is_empty() {
            debug!(target = %target, "no pending hints to deliver");
            return Ok(0);
        }

        info!(
            target = %target,
            count = drained.len(),
            "delivering batched hints"
        );

        // Resolve the target's address.
        let addr = match &self.membership {
            Some(membership) => {
                membership.address_of(&target).ok_or_else(|| Error::ForwardFailed {
                    target: target.to_string(),
                    reason: "node address not found in membership".into(),
                })?
            }
            None => {
                // No membership configured — use a dummy address for testing.
                // Real delivery via gRPC requires membership for address resolution;
                // mock clients used in tests accept any address.
                "127.0.0.1:0"
                    .parse::<SocketAddr>()
                    .map_err(|e| Error::Internal(format!("failed to parse dummy address: {e}")))?
            }
        };

        // Build the batched request.
        let hints: Vec<HintRecord> = drained.iter().map(|(_, _, r)| r.clone()).collect();
        let request = HintedHandoffRequest { hints };

        // Deliver via gRPC.
        let timeout_ms = self.timeouts.hint_delivery_ms;
        let result = self.delivery_client.deliver_hints(addr, request, timeout_ms).await;

        match result {
            Ok(resp) => {
                if !resp.accepted {
                    // Re-enqueue: delivery was attempted but remote node rejected.
                    self.reenqueue_front(&target, drained);
                    return Err(Error::ForwardFailed {
                        target: target.to_string(),
                        reason: "remote node rejected batched hint delivery".into(),
                    });
                }

                // Success — truncate WAL precisely after the last delivered record.
                let last_end_position = drained.last().map(|(_, end, _)| *end).unwrap_or(0);
                let _ = self.hint_wal.truncate_after(last_end_position).await;

                let delivered = drained.len();
                info!(
                    target = %target,
                    delivered,
                    accepted = resp.accepted_count,
                    "batched hint delivery succeeded"
                );

                Ok(delivered)
            }
            Err(e) => {
                // Delivery failed — re-enqueue hints for retry.
                warn!(
                    target = %target,
                    error = %e,
                    count = drained.len(),
                    "batched hint delivery failed; re-enqueuing for retry"
                );
                self.reenqueue_front(&target, drained);
                Err(e)
            }
        }
    }

    /// Returns the number of pending hints for a given node.
    pub fn pending_count(&self, target: &NodeId) -> usize {
        self.queues.get(target).map(|q| q.len()).unwrap_or(0)
    }

    /// Returns the total number of pending hints across all nodes.
    pub fn total_pending_count(&self) -> usize {
        self.queues.iter().map(|entry| entry.value().len()).sum()
    }

    /// Delivers all pending hints for a returned node (convenience wrapper).
    ///
    /// This is an alias for `drain_and_deliver` for backward compatibility
    /// with code that used the legacy `HintedHandoff::deliver_pending`.
    pub async fn deliver_pending(&self, target: NodeId) -> Result<usize> {
        self.drain_and_deliver(target).await
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Re-enqueues drained records at the front of the queue (LIFO re-insertion).
    fn reenqueue_front(&self, target: &NodeId, records: Vec<(u64, u64, HintRecord)>) {
        let mut queue = self.queues.entry(target.clone()).or_default();
        for (start, end, record) in records.into_iter().rev() {
            queue.push_front((start, end, record));
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use oceanfs_core::{BucketId, SegmentId};
    use tempfile::tempdir;

    use super::*;

    /// A mock delivery client for testing.
    struct MockDeliveryClient {
        /// Records incoming requests.
        requests: StdMutex<Vec<(SocketAddr, HintedHandoffRequest)>>,
        /// Responses to return (popped from front).
        responses: StdMutex<VecDeque<std::result::Result<HintedHandoffResponse, Error>>>,
        /// If true, record all requests even when responses are empty.
        _record_all: bool,
    }

    impl MockDeliveryClient {
        fn new() -> Self {
            Self {
                requests: StdMutex::new(Vec::new()),
                responses: StdMutex::new(VecDeque::new()),
                _record_all: true,
            }
        }

        fn add_response(&self, resp: std::result::Result<HintedHandoffResponse, Error>) {
            self.responses.lock().unwrap().push_back(resp);
        }

        fn take_requests(&self) -> Vec<(SocketAddr, HintedHandoffRequest)> {
            self.requests.lock().unwrap().drain(..).collect()
        }
    }

    #[async_trait::async_trait]
    impl HintDeliveryClient for MockDeliveryClient {
        async fn deliver_hints(
            &self,
            target_addr: SocketAddr,
            request: HintedHandoffRequest,
            _timeout_ms: u64,
        ) -> std::result::Result<HintedHandoffResponse, Error> {
            self.requests.lock().unwrap().push((target_addr, request.clone()));
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(HintedHandoffResponse { accepted: true, accepted_count: 0 }))
        }
    }

    fn make_test_config(wal_path: std::path::PathBuf) -> HintedHandoffConfig {
        HintedHandoffConfig { wal_path, ..HintedHandoffConfig::default() }
    }

    // ── T1.5: Batched delivery ────────────────────────────────────────

    #[tokio::test]
    async fn test_hinted_handoff_batched_delivery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("hints.wal");
        let wal = Arc::new(HintWal::open(&wal_path).await.unwrap());

        let mock = Arc::new(MockDeliveryClient::new());
        // Add two success responses (one per node drain).
        mock.add_response(Ok(HintedHandoffResponse { accepted: true, accepted_count: 5 }));
        mock.add_response(Ok(HintedHandoffResponse { accepted: true, accepted_count: 3 }));

        let manager =
            HintedHandoffManager::new(wal, mock.clone(), make_test_config(wal_path.clone()));

        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");

        // Enqueue 5 hints for node_a.
        for i in 0..5 {
            let record = HintRecord::new_inline(
                node_a.clone(),
                BucketId::new("bucket-a"),
                format!("key-a-{i}"),
                vec![i as u8].into(),
            );
            manager.enqueue(record).await.unwrap();
        }

        // Enqueue 3 hints for node_b.
        for i in 0..3 {
            let record = HintRecord::new_inline(
                node_b.clone(),
                BucketId::new("bucket-b"),
                format!("key-b-{i}"),
                vec![i as u8].into(),
            );
            manager.enqueue(record).await.unwrap();
        }

        assert_eq!(manager.pending_count(&node_a), 5);
        assert_eq!(manager.pending_count(&node_b), 3);

        // Deliver node_a's hints.
        let delivered_a = manager.drain_and_deliver(node_a.clone()).await.unwrap();
        assert_eq!(delivered_a, 5);
        assert_eq!(manager.pending_count(&node_a), 0);
        assert_eq!(manager.pending_count(&node_b), 3, "node_b unchanged");

        // Deliver node_b's hints.
        let delivered_b = manager.drain_and_deliver(node_b.clone()).await.unwrap();
        assert_eq!(delivered_b, 3);
        assert_eq!(manager.pending_count(&node_b), 0);

        // Verify delivery requests.
        let requests = mock.take_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].1.hints.len(), 5);
        assert_eq!(requests[1].1.hints.len(), 3);
    }

    // ── T1.6: Delivery failure re-enqueues ───────────────────────────

    #[tokio::test]
    async fn test_hinted_handoff_delivery_failure_reenqueues() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("hints.wal");
        let wal = Arc::new(HintWal::open(&wal_path).await.unwrap());

        let mock = Arc::new(MockDeliveryClient::new());
        // First attempt fails.
        mock.add_response(Err(Error::ForwardFailed {
            target: "node-a".into(),
            reason: "connection refused".into(),
        }));
        // Second attempt succeeds.
        mock.add_response(Ok(HintedHandoffResponse { accepted: true, accepted_count: 3 }));

        let manager =
            HintedHandoffManager::new(wal, mock.clone(), make_test_config(wal_path.clone()));

        let node_a = NodeId::new("node-a");

        // Enqueue 3 hints.
        for i in 0..3 {
            let record = HintRecord::new_inline(
                node_a.clone(),
                BucketId::new("b"),
                format!("key-{i}"),
                vec![i as u8].into(),
            );
            manager.enqueue(record).await.unwrap();
        }
        assert_eq!(manager.pending_count(&node_a), 3);

        // First attempt fails.
        let result = manager.drain_and_deliver(node_a.clone()).await;
        assert!(result.is_err(), "first delivery should fail");
        assert_eq!(manager.pending_count(&node_a), 3, "hints re-enqueued");

        // Second attempt succeeds.
        let delivered = manager.drain_and_deliver(node_a.clone()).await.unwrap();
        assert_eq!(delivered, 3);
        assert_eq!(manager.pending_count(&node_a), 0);
    }

    // ── No hints to deliver returns 0 ────────────────────────────────

    #[tokio::test]
    async fn test_drain_empty_returns_zero() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("hints.wal");
        let wal = Arc::new(HintWal::open(&wal_path).await.unwrap());
        let mock = Arc::new(MockDeliveryClient::new());

        let manager = HintedHandoffManager::new(wal, mock, make_test_config(wal_path));
        let result = manager.drain_and_deliver(NodeId::new("nobody")).await.unwrap();
        assert_eq!(result, 0);
    }

    // ── Replay repopulates queues ────────────────────────────────────

    #[tokio::test]
    async fn test_replay_repopulates_queues() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("hints.wal");

        // Write records directly to the WAL.
        let wal1 = HintWal::open(&wal_path).await.unwrap();
        for i in 0..4 {
            let record = HintRecord::new_inline(
                NodeId::new("n1"),
                BucketId::new("b"),
                format!("key-{i}"),
                vec![i as u8].into(),
            );
            wal1.write_hint(&record).await.unwrap();
        }
        drop(wal1);

        // Reopen with manager and replay.
        let wal2 = Arc::new(HintWal::open(&wal_path).await.unwrap());
        let mock = Arc::new(MockDeliveryClient::new());
        let manager = HintedHandoffManager::new(wal2, mock, make_test_config(wal_path));

        let count = manager.replay_and_enqueue().await.unwrap();
        assert_eq!(count, 4);
        assert_eq!(manager.pending_count(&NodeId::new("n1")), 4);
    }
}
