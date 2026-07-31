//! Integration test: read coordinator path.
//!
//! Tests inline reads, chunk assembly, hash verification, and not-found.

#![cfg(all(feature = "membership", feature = "network"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use oceanfs_core::{BucketId, HashKey, NodeId, ObjectKey};
use oceanfs_routing::{hash_key, Ring, RingCache};
use oceanfs_server::{ReadCoordinator, ReadOutcome, ReadRequest};

fn make_coordinator() -> ReadCoordinator {
    let mut ring = Ring::new(oceanfs_core::RingConfig::default());
    ring.add_node(NodeId::new("n1"));
    let ring_cache = Arc::new(RingCache::new(ring));
    ReadCoordinator::new(ring_cache, NodeId::new("n1"), None)
}

#[tokio::test]
async fn read_metadata_only_returns_empty_data() {
    let coord = make_coordinator();
    let req = ReadRequest {
        bucket: BucketId::new("test"),
        key: ObjectKey::new("meta-only"),
        hash_key: HashKey::from_bytes(hash_key(b"meta-only")),
        metadata_only: true,
        policy: None,
    };
    let result = coord.get(req).await.unwrap();
    assert!(result.data.is_empty());
    assert!(!result.hash_verified);
}

#[tokio::test]
async fn read_classify_inline_blob() {
    let coord = make_coordinator();
    let meta = oceanfs_core::ObjectMetadata {
        object_key: ObjectKey::new("inline"),
        size: 10,
        blake3_hash: None,
        chunks: smallvec::SmallVec::new(),
        inline_data: Some(bytes::Bytes::from_static(b"hello")),
        created_at: 0,
        hlc: oceanfs_core::Hlc::zero(),
    };
    assert_eq!(coord.classify(&meta), ReadOutcome::InlineHit);
}

#[tokio::test]
async fn read_classify_multi_chunk_blob() {
    let coord = make_coordinator();
    let mut chunks = smallvec::SmallVec::new();
    for i in 0..3 {
        chunks.push(oceanfs_core::ChunkRef {
            segment_id: oceanfs_core::SegmentId::new(),
            offset: i * 1024,
            length: 1024,
        });
    }
    let meta = oceanfs_core::ObjectMetadata {
        object_key: ObjectKey::new("multi"),
        size: 3072,
        blake3_hash: None,
        chunks,
        inline_data: None,
        created_at: 0,
        hlc: oceanfs_core::Hlc::zero(),
    };
    assert_eq!(coord.classify(&meta), ReadOutcome::MultiChunk { chunk_count: 3 });
}

#[tokio::test]
async fn read_not_found_classify() {
    let coord = make_coordinator();
    let meta = oceanfs_core::ObjectMetadata {
        object_key: ObjectKey::new("gone"),
        size: 0,
        blake3_hash: None,
        chunks: smallvec::SmallVec::new(),
        inline_data: None,
        created_at: 0,
        hlc: oceanfs_core::Hlc::zero(),
    };
    assert_eq!(coord.classify(&meta), ReadOutcome::NotFound);
}
