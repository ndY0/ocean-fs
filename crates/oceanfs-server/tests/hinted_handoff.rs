//! Integration test: hinted handoff lifecycle.
//!
//! Tests hint creation, delivery on node return, and cleanup.

#![cfg(all(feature = "membership", feature = "network"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use oceanfs_core::{Hlc, NodeId, SegmentId};
use oceanfs_server::{HintRecord, HintedHandoff};

#[tokio::test]
async fn handoff_create_deliver_cleanup() {
    let hh = HintedHandoff::new();
    let target = NodeId::new("node-b");

    // Create a hint for an unreachable node.
    let hint = HintRecord {
        intended_for: target.clone(),
        segment_id: SegmentId::new(),
        offset: 0,
        length: 100,
        timestamp: Hlc::zero(),
    };
    hh.handoff(target.clone(), hint).await.unwrap();
    assert_eq!(hh.pending_count(&target), 1);

    // Simulate node returning — deliver pending hints.
    let delivered = hh.deliver_pending(target.clone()).await.unwrap();
    assert_eq!(delivered, 1);
    assert_eq!(hh.pending_count(&target), 0, "hints cleared after delivery");
}

#[tokio::test]
async fn handoff_multiple_hints_delivered_in_batch() {
    let hh = HintedHandoff::new();
    let target = NodeId::new("node-c");

    for i in 0..5 {
        hh.handoff(target.clone(), HintRecord {
            intended_for: target.clone(),
            segment_id: SegmentId::new(),
            offset: i * 64,
            length: 64,
            timestamp: Hlc::zero(),
        }).await.unwrap();
    }
    assert_eq!(hh.pending_count(&target), 5);

    let delivered = hh.deliver_pending(target.clone()).await.unwrap();
    assert_eq!(delivered, 5);
    assert_eq!(hh.pending_count(&target), 0);
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
