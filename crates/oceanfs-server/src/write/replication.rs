//! Write replication to remote nodes.
//!
//! Handles fan-out of write operations to replica nodes via the
//! ConnectionPool. Uses `SegmentRpcClient` over gRPC to stream
//! append requests and collect acknowledgments.

use std::{sync::Arc, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use oceanfs_core::{
    proto::segment::{SegmentAppendRequest, SegmentAppendResponse},
    Hlc, NodeId, SegmentId, WriteAck,
};
use oceanfs_membership::Membership;
use oceanfs_network::ConnectionPool;
use oceanfs_storage::SegmentRpcClient;
use tracing::{debug, warn};

use crate::error::{Error, Result};

/// Replicates a write to a set of remote nodes using parallel fan-out.
///
/// Creates a `FuturesUnordered` of all replication tasks and races them
/// against a timeout. Returns ack results for each target, paired with
/// the target's NodeId so callers know which node succeeded or failed.
///
/// # Errors
///
/// Returns an error per-target if the node is unreachable or the
/// write fails on a remote node. Individual failures do not abort
/// the remaining fan-out tasks.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn replicate_write(
    membership: &Arc<Membership>,
    pool: &Arc<ConnectionPool>,
    targets: &[&NodeId],
    segment_id: SegmentId,
    data: &[u8],
    hlc: Hlc,
    write_timeout_ms: u64,
    req: &super::coordinator::WriteRequest,
) -> Vec<(NodeId, Result<WriteAck>)> {
    if targets.is_empty() {
        return vec![];
    }

    let deadline = Duration::from_millis(write_timeout_ms);
    let timeout = tokio::time::sleep(deadline);
    tokio::pin!(timeout);

    let mut futs: FuturesUnordered<_> = targets
        .iter()
        .map(|target| {
            let target = (*target).clone();
            async move {
                let result =
                    replicate_to_single(pool, membership, &target, segment_id, data, hlc, req)
                        .await;
                (target, result)
            }
        })
        .collect();

    let mut results = Vec::with_capacity(targets.len());

    loop {
        tokio::select! {
            biased;

            () = &mut timeout => {
                warn!(
                    target_count = targets.len(),
                    "write replication timed out after {}ms",
                    write_timeout_ms
                );
                break;
            }
            Some((target, result)) = futs.next() => {
                results.push((target, result));
                if results.len() >= targets.len() {
                    break;
                }
            }
        }
    }

    results
}

/// Replicates a write to a single remote node via gRPC AppendSegment.
///
/// 1. Resolves the target's `SocketAddr` from Membership.
/// 2. Acquires a gRPC channel from the ConnectionPool.
/// 3. Constructs a `SegmentRpcClient` and streams the append request.
/// 4. Returns the server's `SegmentAppendResponse` as a `WriteAck`.
async fn replicate_to_single(
    pool: &Arc<ConnectionPool>,
    membership: &Arc<Membership>,
    target: &NodeId,
    segment_id: SegmentId,
    data: &[u8],
    hlc: Hlc,
    req: &super::coordinator::WriteRequest,
) -> Result<WriteAck> {
    let addr = membership.address_of(target).ok_or_else(|| Error::ForwardFailed {
        target: target.to_string(),
        reason: "node address not found in membership".into(),
    })?;

    debug!(
        target = %target,
        addr = %addr,
        segment_id = %segment_id,
        hlc_wall = hlc.wall_time(),
        "replicating write via gRPC AppendSegment"
    );

    let pooled = pool.get_channel(addr).await.map_err(|e| Error::ForwardFailed {
        target: target.to_string(),
        reason: format!("connection pool error: {e}"),
    })?;

    let channel = pooled.channel().clone();
    drop(pooled);

    let mut client = SegmentRpcClient::new(channel);

    let proto_segment_id: oceanfs_core::proto::common::SegmentId = segment_id.into();
    let proto_hlc: oceanfs_core::proto::common::HlcTimestamp = hlc.into();

    let request = SegmentAppendRequest {
        segment_id: Some(proto_segment_id),
        shard_index: None,
        offset: 0,
        data: data.to_vec(),
        hlc: Some(proto_hlc),
        bucket_id: req.bucket.to_string(),
        object_key: req.key.to_string(),
        object_size: data.len() as u64,
        blake3_hash: vec![],
        chunk_segment_ids: vec![segment_id.as_uuid().as_bytes().to_vec()],
        chunk_offsets: vec![0],
        chunk_lengths: vec![data.len() as u32],
    };

    let stream = tokio_stream::once(request);
    let response: tonic::Response<SegmentAppendResponse> =
        client.append_segment(stream).await.map_err(|status| Error::ForwardFailed {
            target: target.to_string(),
            reason: format!("gRPC append failed: {status}"),
        })?;

    let ack = response.into_inner();

    debug!(
        target = %target,
        wal_position = ack.wal_position,
        "write replicated successfully"
    );

    Ok(WriteAck { node_id: target.clone(), wal_position: ack.wal_position, hlc })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::net::SocketAddr;

    use bytes::Bytes;
    use oceanfs_core::{BucketId, Hlc, Incarnation, NodeState, ObjectKey, SegmentId};
    use oceanfs_routing::{Ring, RingCache};

    use super::*;

    fn make_membership(node_id: &str) -> Arc<Membership> {
        let ring = Ring::new(oceanfs_core::RingConfig::default());
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Arc::new(Membership::new(
            NodeId::new(node_id),
            addr,
            oceanfs_core::GossipConfig::default(),
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

    #[tokio::test]
    async fn replicate_write_empty_targets() {
        let membership = make_membership("n1");
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
        let req = crate::write::coordinator::WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("test"),
            hash_key: oceanfs_core::HashKey::from_bytes([0u8; 32]),
            data: Bytes::from_static(b"data"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };
        let results = replicate_write(
            &membership,
            &pool,
            &[],
            SegmentId::new(),
            b"data",
            Hlc::zero(),
            5000,
            &req,
        )
        .await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn replicate_write_to_unknown_node_fails() {
        let membership = make_membership("n1");
        let pool = Arc::new(ConnectionPool::new(oceanfs_core::RpcConfig::default()));
        let unknown = NodeId::new("nobody");
        let req = crate::write::coordinator::WriteRequest {
            bucket: BucketId::new("test"),
            key: ObjectKey::new("test"),
            hash_key: oceanfs_core::HashKey::from_bytes([0u8; 32]),
            data: Bytes::from_static(b"data"),
            write_quorum: 1,
            ack_after_wal: true,
            ec_async: false,
            policy: None,
        };
        let results = replicate_write(
            &membership,
            &pool,
            &[&unknown],
            SegmentId::new(),
            b"data",
            Hlc::zero(),
            5000,
            &req,
        )
        .await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1.is_err());
    }
}
