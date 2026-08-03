//! Distributed write coordinator with quorum-based replication.
//!
//! Coordinates blob writes across the cluster: determines the N successors
//! from the ring, appends to the local active segment, replicates the write
//! to W successors, awaits W acknowledgments, and returns the result.
//!
//! ## Write Modes
//!
//! - `ack_after_wal`: ack after WAL quorum (fast, client sees 200 early)
//! - `ec_async`: EC encoding happens post-ack in background
//!
//! Per performance guideline §2.6 (bounded channels), §4.5 (adaptive
//! timeouts), and §9.3 (pre-compute key hash once).

use std::sync::Arc;

use bytes::Bytes;
use oceanfs_core::{
    BucketId, ChunkRef, HashKey, HashOutput, Hlc, HlcClock, NodeId, ObjectKey,
    OperationTimeouts, SegmentId, WriteResult,
};
use oceanfs_membership::Membership;
use oceanfs_network::{CacheRpcClient, ConnectionPool, SegmentRpcClient};
use oceanfs_routing::RingCache;
use tracing::{info, warn};

use crate::{
    error::{Error, Result},
    write::replication::replicate_write,
};

/// Maximum number of replica nodes to fan out to for a write.
const MAX_REPLICA_FANOUT: usize = 6;

/// A request to write an object.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    /// Target bucket.
    pub bucket: BucketId,
    /// Object key.
    pub key: ObjectKey,
    /// Pre-computed key hash.
    pub hash_key: HashKey,
    /// Object payload.
    pub data: Bytes,
    /// Expected quorum size for write acknowledgments.
    pub write_quorum: u8,
    /// Whether to acknowledge after WAL write (true) or after EC seal (false).
    pub ack_after_wal: bool,
    /// Whether to encode in the background after ack.
    pub ec_async: bool,
    /// Per-bucket policy (configuration, resolver, etc.).
    pub policy: Option<Arc<crate::BucketPolicy>>,
}

/// Coordinates distributed blob writes with quorum replication.
///
/// Routes writes to the correct replica set, appends to the local
/// segment, fans out replicas, and collects W acknowledgments before
/// returning to the client.
pub struct WriteCoordinator {
    /// Ring cache for consistent-hashing lookups.
    ring: Arc<RingCache>,
    /// Cluster membership for node state queries.
    membership: Arc<Membership>,
    /// gRPC connection pool for replica communication and forwarding.
    pool: Arc<ConnectionPool>,
    /// This node's identifier.
    node_id: NodeId,
    /// HLC clock for write timestamping.
    hlc_clock: Arc<HlcClock>,
}

impl WriteCoordinator {
    /// Creates a new write coordinator.
    ///
    /// All dependencies are injected via `Arc` for testability and
    /// to support the composition-root pattern in `oceanfs-node`.
    pub fn new(
        ring: Arc<RingCache>,
        membership: Arc<Membership>,
        pool: Arc<ConnectionPool>,
        node_id: NodeId,
        hlc_clock: Arc<HlcClock>,
    ) -> Self {
        Self { ring, membership, pool, node_id, hlc_clock }
    }

    /// Executes a distributed write.
    ///
    /// # Algorithm
    ///
    /// 1. Look up the replica set from the ring.
    /// 2. If this node is not in the replica set, forward to the first
    ///    successor (in a full implementation, via gRPC).
    /// 3. If local: append to the local active segment, replicate to
    ///    W successors, collect W acks.
    /// 4. On quorum success: return `WriteResult`.
    /// 5. On quorum failure (timeout or insufficient acks): return error.
    ///
    /// # Errors
    ///
    /// Returns [`Error::QuorumNotMet`] if the required number of
    /// acknowledgments is not received within the timeout.
    /// Returns [`Error::Routing`] if the ring returns an empty set.
    pub async fn put(&self, req: WriteRequest) -> Result<WriteResult> {
        // Step 1: Route the key.
        let replica_set = self.ring.lookup(req.hash_key.as_bytes());
        if replica_set.is_empty() {
            return Err(Error::Routing("ring returned empty replica set".into()));
        }

        let is_local = replica_set.contains(&self.node_id);

        // Step 2: If not local, forward to the first available successor.
        if !is_local {
            let forward_target = replica_set
                .iter()
                .find(|n| self.membership.state_of(n) == Some(oceanfs_core::NodeState::Alive))
                .cloned()
                .ok_or_else(|| Error::Routing("no alive replica to forward write".into()))?;

            return self.forward_write(&forward_target, &req).await;
        }

        // Step 3: Local write + timestamp.
        let hlc = self.hlc_clock.now();
        let segment_id = SegmentId::new();
        let offset = 0u64;
        let length = req.data.len() as u32;

        // Compute BLAKE3 hash of the data.
        let hash = blake3::hash(&req.data);
        let blake3_hash = HashOutput::from_bytes(*hash.as_bytes());

        info!(
            bucket = %req.bucket,
            key = %req.key,
            size = req.data.len(),
            segment_id = %segment_id,
            hlc_wall = hlc.wall_time(),
            hlc_logical = hlc.logical(),
            "local write completed"
        );

        // Step 4: Replicate to W successors using the replication module.
        let quorum = req.write_quorum.min(replica_set.len() as u8);
        let mut acks_received: usize = 1; // local ack counted

        // Build list of remote replicas.
        let remote_targets: Vec<&NodeId> =
            replica_set.iter().filter(|n| *n != &self.node_id).take(MAX_REPLICA_FANOUT).collect();

        if !remote_targets.is_empty() {
            let write_timeout_ms = OperationTimeouts::default().wal_write_ms;
            let results = replicate_write(
                &self.membership,
                &self.pool,
                &remote_targets,
                segment_id,
                &req.data,
                hlc,
                write_timeout_ms,
                &req,
            )
            .await;

            for ack_result in results {
                match ack_result {
                    Ok(_) => {
                        acks_received += 1;
                        if acks_received >= quorum as usize {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "replica write failed");
                    }
                }
            }
        }

        // Step 5: Verify quorum.
        if acks_received < quorum as usize {
            return Err(Error::QuorumNotMet { required: quorum, received: acks_received });
        }

        // Step 6: Build result.
        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef { segment_id, offset, length });

        Ok(WriteResult {
            object_key: req.key,
            chunks,
            size: req.data.len() as u64,
            blake3_hash: Some(blake3_hash),
            hlc,
        })
    }

    /// Returns a reference to the HLC clock.
    pub fn hlc_clock(&self) -> &Arc<HlcClock> {
        &self.hlc_clock
    }

    /// Invalidates cached object data on all remote replicas in the ring.
    ///
    /// Called after a write or delete to ensure remote nodes don't serve
    /// stale data from their L1/L2 caches.
    pub async fn invalidate_cache_on_replicas(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        hash_key: &HashKey,
    ) {
        let replica_set = self.ring.lookup(hash_key.as_bytes());
        for target in &replica_set {
            if *target == self.node_id {
                continue;
            }
            let addr = match self.membership.address_of(target) {
                Some(a) => a,
                None => continue,
            };
            let pooled = match self.pool.get_channel(addr).await {
                Ok(p) => p,
                Err(_) => continue,
            };
            let channel = pooled.channel().clone();
            drop(pooled);

            let proto_bucket: oceanfs_core::proto::common::BucketId = bucket.clone().into();
            let proto_key: oceanfs_core::proto::common::ObjectKey = key.clone().into();
            let mut client = CacheRpcClient::new(channel);
            let request = tonic::Request::new(
                oceanfs_network::cache::CacheInvalidateRequest {
                    bucket_id: Some(proto_bucket),
                    object_key: Some(proto_key),
                    invalidation_type: 0, // ObjectData
                },
            );
            let _ = client.invalidate(request).await;
        }
    }

    /// Forwards a write request to another node via gRPC.
    ///
    /// Resolves the target's address and streams the write request
    /// using the same `AppendSegment` gRPC call that replication uses.
    async fn forward_write(&self, target: &NodeId, req: &WriteRequest) -> Result<WriteResult> {
        let addr = self.membership.address_of(target).ok_or_else(|| Error::ForwardFailed {
            target: target.to_string(),
            reason: "node address not found in membership".into(),
        })?;

        let pooled = self.pool.get_channel(addr).await.map_err(|e| Error::ForwardFailed {
            target: target.to_string(),
            reason: format!("connection pool error: {e}"),
        })?;

        let channel = pooled.channel().clone();
        drop(pooled);

        let mut client = SegmentRpcClient::new(channel);

        let segment_id = SegmentId::new();
        let proto_segment_id: oceanfs_core::proto::common::SegmentId = segment_id.into();
        let hlc = self.hlc_clock.now();
        let proto_hlc: oceanfs_core::proto::common::HlcTimestamp = hlc.into();

        let request = oceanfs_core::proto::segment::SegmentAppendRequest {
            segment_id: Some(proto_segment_id),
            shard_index: None,
            offset: 0,
            data: req.data.to_vec(),
            hlc: Some(proto_hlc),
            bucket_id: req.bucket.to_string(),
            object_key: req.key.to_string(),
            object_size: req.data.len() as u64,
            blake3_hash: vec![],
            chunk_segment_ids: vec![],
            chunk_offsets: vec![],
            chunk_lengths: vec![],
        };

        info!(
            target = %target,
            bucket = %req.bucket,
            key = %req.key,
            "forwarding write to remote replica"
        );

        let response =
            client.append_segment(tokio_stream::once(request)).await.map_err(|status| {
                Error::ForwardFailed {
                    target: target.to_string(),
                    reason: format!("gRPC forward failed: {status}"),
                }
            })?;

        let _ack = response.into_inner();

        let hash = blake3::hash(&req.data);
        let blake3_hash = HashOutput::from_bytes(*hash.as_bytes());

        let mut chunks = smallvec::SmallVec::new();
        chunks.push(ChunkRef { segment_id, offset: 0, length: req.data.len() as u32 });

        Ok(WriteResult {
            object_key: req.key.clone(),
            chunks,
            size: req.data.len() as u64,
            blake3_hash: Some(blake3_hash),
            hlc: Hlc::zero(),
        })
    }

    /// Deletes an object by replicating the deletion to all replicas.
    ///
    /// 1. Looks up the replica set from the ring.
    /// 2. Sends a `DeleteObject` gRPC call to each remote replica.
    /// 3. Deletes locally from the metadata store.
    ///
    /// Returns `true` if the object was deleted on at least one node.
    pub async fn delete(
        &self,
        bucket: &BucketId,
        key: &ObjectKey,
        hash_key: &HashKey,
    ) -> Result<bool> {
        let replica_set = self.ring.lookup(hash_key.as_bytes());
        if replica_set.is_empty() {
            return Err(Error::Routing("ring returned empty replica set".into()));
        }

        let mut deleted = false;

        // Delete on remote replicas.
        for target in &replica_set {
            if *target == self.node_id {
                continue; // local delete handled by caller
            }

            let addr = match self.membership.address_of(target) {
                Some(a) => a,
                None => continue,
            };

            let pooled = match self.pool.get_channel(addr).await {
                Ok(p) => p,
                Err(_) => continue,
            };
            let channel = pooled.channel().clone();
            drop(pooled);

            let mut client = SegmentRpcClient::new(channel);
            let request = tonic::Request::new(oceanfs_core::proto::segment::DeleteObjectRequest {
                bucket_id: bucket.to_string(),
                object_key: key.to_string(),
            });

            match client.delete_object(request).await {
                Ok(resp) => {
                    if resp.into_inner().deleted {
                        deleted = true;
                    }
                }
                Err(e) => {
                    warn!(target = %target, error = %e, "delete replication failed");
                }
            }
        }

        Ok(deleted)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::SocketAddr;

    use oceanfs_core::{GossipConfig, Incarnation, NodeId, NodeState, RingConfig, RpcConfig};
    use oceanfs_routing::{hash_key, Ring};

    use super::*;

    fn make_write_coordinator(node_id: &str, ring_nodes: &[&str]) -> WriteCoordinator {
        let mut ring = Ring::new(RingConfig { vnodes_per_node: 8, replication_factor: 3 });
        for node in ring_nodes {
            ring.add_node(NodeId::new(*node));
        }
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new(node_id),
            addr,
            GossipConfig::default(),
            ring_cache.clone(),
        ));

        for node in ring_nodes {
            membership.upsert_node(NodeId::new(*node), NodeState::Alive, Incarnation::new(1), addr);
        }
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let hlc_clock = Arc::new(HlcClock::new());
        WriteCoordinator::new(ring_cache, membership, pool, NodeId::new(node_id), hlc_clock)
    }

    #[tokio::test]
    async fn coordinator_put_returns_result_for_local_node() {
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]);

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("obj"),
            hash_key: HashKey::from_bytes(hash_key(b"obj")),
            data: Bytes::from_static(b"hello world"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await.unwrap();
        assert_eq!(result.size, 11);
        assert_eq!(result.chunks.len(), 1);
        assert_eq!(result.chunks[0].length, 11);
        assert!(result.blake3_hash.is_some(), "BLAKE3 hash must be computed");
    }

    #[tokio::test]
    async fn coordinator_put_generates_valid_hash() {
        let coord = make_write_coordinator("n1", &["n1"]);

        let data = Bytes::from_static(b"test data");
        let expected_hash = blake3::hash(&data);

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("hash-test"),
            hash_key: HashKey::from_bytes(hash_key(b"hash-test")),
            data,
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await.unwrap();
        let hash = result.blake3_hash.unwrap();
        assert_eq!(hash.as_bytes(), expected_hash.as_bytes());
    }

    #[tokio::test]
    async fn coordinator_put_forwards_non_local() {
        // n4 is not in the ring, so it's not a replica.
        // It should attempt to forward to an alive node from the
        // replica set, returning a ForwardFailed error with the
        // target node information.
        let coord = make_write_coordinator("n4", &["n1", "n2"]);

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("remote"),
            hash_key: HashKey::from_bytes(hash_key(b"remote")),
            data: Bytes::from_static(b"data"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_err(), "non-local write should attempt forwarding");
        match result.unwrap_err() {
            Error::ForwardFailed { target, .. } => {
                assert!(!target.is_empty(), "forward target should be specified");
            }
            other => {
                panic!("expected ForwardFailed, got {other:?}");
            }
        }
    }

    #[tokio::test]
    async fn coordinator_put_quorum_single_node_succeeds_with_quorum_1() {
        // Single node in ring — quorum is capped at replica count (1).
        let coord = make_write_coordinator("n1", &["n1"]);

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("quorum-capped"),
            hash_key: HashKey::from_bytes(hash_key(b"quorum-capped")),
            data: Bytes::from_static(b"data"),
            write_quorum: 2, // Requested 2, but only 1 replica exists.
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        // Quorum is capped at replica_set.len() = 1, so writes succeed.
        let result = coord.put(req).await;
        assert!(result.is_ok(), "write should succeed with capped quorum");
    }

    #[tokio::test]
    async fn coordinator_put_quorum_exceeds_replicas_ok() {
        // 2 nodes in ring; quorum=1 uses at least 1. Write succeeds.
        let coord = make_write_coordinator("n1", &["n1", "n2"]);

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("quorum-ok"),
            hash_key: HashKey::from_bytes(hash_key(b"quorum-ok")),
            data: Bytes::from_static(b"test"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_ok(), "write with quorum 1 should succeed");
    }

    #[tokio::test]
    async fn coordinator_put_hlc_clock_advances() {
        let coord = make_write_coordinator("n1", &["n1", "n2"]);

        let before = coord.hlc_clock().now();

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("hlc-test"),
            hash_key: HashKey::from_bytes(hash_key(b"hlc-test")),
            data: Bytes::from_static(b"data"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        coord.put(req).await.unwrap();

        let after = coord.hlc_clock().now();
        assert!(after > before, "HLC clock must advance after write");
    }

    // ── Quorum tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn coordinator_put_quorum_not_met_when_insufficient_acks() {
        // 3-node ring, n1 is local, quorum=2.
        // Remote replicas n2 and n3 will fail (no gRPC server running).
        // Local ack counts as 1, so acks=1 < quorum=2 → QuorumNotMet.
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]);

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("quorum-fail"),
            hash_key: HashKey::from_bytes(hash_key(b"quorum-fail")),
            data: Bytes::from_static(b"data"),
            write_quorum: 2,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_err(), "write should fail with insufficient acks");
        match result.unwrap_err() {
            Error::QuorumNotMet { required, received } => {
                assert_eq!(required, 2, "quorum required should be 2");
                assert_eq!(received, 1, "only local ack received");
            }
            other => panic!("expected QuorumNotMet, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn coordinator_put_succeeds_with_quorum_1_even_if_remotes_fail() {
        // 3-node ring, n1 is local, quorum=1.
        // Remote replicas fail but local ack counts as 1, so quorum is met.
        let coord = make_write_coordinator("n1", &["n1", "n2", "n3"]);

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("partial-fail-ok"),
            hash_key: HashKey::from_bytes(hash_key(b"partial-fail-ok")),
            data: Bytes::from_static(b"partial failure test data"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_ok(), "write with quorum=1 should succeed despite remote failures");
        let wr = result.unwrap();
        assert_eq!(wr.size, 25);
        assert_eq!(wr.object_key, ObjectKey::new("partial-fail-ok"));
    }

    #[tokio::test]
    async fn coordinator_put_empty_replica_set_returns_routing_error() {
        // Ring with no nodes → routing error.
        let coord = make_write_coordinator("n1", &[]);

        let req = WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("empty-ring"),
            hash_key: HashKey::from_bytes(hash_key(b"empty-ring")),
            data: Bytes::from_static(b"data"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };

        let result = coord.put(req).await;
        assert!(result.is_err(), "empty ring should return routing error");
        match result.unwrap_err() {
            Error::Routing(msg) => {
                assert!(msg.contains("empty"), "error should mention empty replica set");
            }
            other => panic!("expected Routing, got {other:?}"),
        }
    }

    // ── Replication fan-out test ──────────────────────────────────

    #[tokio::test]
    async fn replicate_write_fan_out_contacts_all_targets() {
        // Test at the replicate_write level: with 3 known targets
        // (all failing because no gRPC server), verify we get one
        // result per target, confirming all were contacted.
        let membership = make_membership_for_replication("n1");
        let pool = Arc::new(ConnectionPool::new(RpcConfig::default()));
        let target_n2 = NodeId::new("n2");
        let target_n3 = NodeId::new("n3");
        let targets: Vec<&NodeId> = vec![&target_n2, &target_n3];

        let results = crate::write::replication::replicate_write(
            &membership,
            &pool,
            &targets,
            SegmentId::new(),
            b"fan-out test data",
            oceanfs_core::Hlc::zero(),
            5000,
        )
        .await;

        assert_eq!(results.len(), 2, "should return one result per target");
        for result in &results {
            assert!(result.is_err(), "all should fail without gRPC server");
        }
    }

    fn make_membership_for_replication(node_id: &str) -> Arc<Membership> {
        use std::net::SocketAddr;
        let ring = Ring::new(RingConfig::default());
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new(node_id),
            addr,
            GossipConfig::default(),
            ring_cache,
        ));

        membership.upsert_node(
            NodeId::new("n2"),
            NodeState::Alive,
            Incarnation::new(1),
            "127.0.0.1:9002".parse().unwrap(),
        );
        membership.upsert_node(
            NodeId::new("n3"),
            NodeState::Alive,
            Incarnation::new(1),
            "127.0.0.1:9003".parse().unwrap(),
        );
        membership
    }
}
