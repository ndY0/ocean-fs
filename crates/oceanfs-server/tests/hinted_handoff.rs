//! Integration test: hinted handoff lifecycle.
//!
//! Tests hint creation, storage lifecycle, and capacity enforcement.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use oceanfs_core::{Hlc, NodeId, SegmentId};
use oceanfs_server::{HintRecord, HintedHandoff};

#[tokio::test]
async fn handoff_create_deliver_cleanup() {
    let hh = HintedHandoff::new();
    let target = NodeId::new("node-b");

    let hint = HintRecord {
        intended_for: target.clone(),
        segment_id: SegmentId::new(),
        offset: 0,
        length: 100,
        timestamp: Hlc::zero(),
        data: vec![1, 2, 3],
    };
    hh.handoff(target.clone(), hint).await.unwrap();
    assert_eq!(hh.pending_count(&target), 1);

    // Without a membership reference, gRPC delivery fails and hints are preserved.
    let delivered = hh.deliver_pending(target.clone()).await.unwrap();
    assert_eq!(delivered, 0, "no membership means delivery cannot succeed");
    assert_eq!(hh.pending_count(&target), 1, "failed delivery preserves hints");
}

#[tokio::test]
async fn handoff_multiple_hints_stored_and_counted() {
    let hh = HintedHandoff::new();
    let target = NodeId::new("node-c");

    for i in 0..5 {
        hh.handoff(
            target.clone(),
            HintRecord {
                intended_for: target.clone(),
                segment_id: SegmentId::new(),
                offset: i * 64,
                length: 64,
                timestamp: Hlc::zero(),
                data: vec![i as u8; 64],
            },
        )
        .await
        .unwrap();
    }
    assert_eq!(hh.pending_count(&target), 5);
    assert_eq!(hh.total_pending_count(), 5);
}

#[tokio::test]
async fn handoff_unknown_node_has_zero_pending() {
    let hh = HintedHandoff::new();
    assert_eq!(hh.pending_count(&NodeId::new("ghost")), 0);
}

#[tokio::test]
async fn deliver_to_node_with_no_hints_returns_zero() {
    let hh = HintedHandoff::new();
    let result = hh.deliver_pending(NodeId::new("empty")).await.unwrap();
    assert_eq!(result, 0);
}

#[tokio::test]
async fn handoff_bounded_capacity_rejects_excess() {
    let hh = HintedHandoff::new();
    let node = NodeId::new("full");

    // Fill to per-node limit (MAX_HINTS_PER_NODE = 1_000).
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
    assert!(result.is_err(), "should reject above per-node capacity");
}
