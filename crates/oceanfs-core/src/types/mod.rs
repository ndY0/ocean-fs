//! Shared types used across all OceanFS crates.
//!
//! These are the fundamental domain types — identifiers, hashes, metadata,
//! configuration, codecs, heal pipeline, node/cluster operations, and cache
//! invalidation — that every subsystem references.
//!
//! This module is a re-export facade. The concrete type definitions live in
//! the sub-modules below, and this file only declares sub-modules and
//! re-exports their public API. Downstream consumers continue to write
//! `use oceanfs_core::types::SegmentId` with zero changes.

mod bucket;
mod cache;
mod codec;
mod config;
mod hash;
mod heal;
mod id;
mod metadata;
mod node;

// --- Identifier types ---
// --- Cache types ---
// --- Bucket policy types ---
pub use bucket::BucketPolicy;
pub use cache::CacheInvalidateRequest;
// --- Codec types ---
pub use codec::{CodecConfig, CodecType, EncodingPlan};
// --- Configuration types ---
pub use config::{
    CompressConfig, CompressionTier, GossipConfig, GpuConfig, HealConfig, NvcompCodec,
    NvcompConfig, PoolConfig, RpcConfig, SegmentSizeConfig, SizeTier,
};
// --- Hash types ---
pub use hash::HashKey;
// --- Heal pipeline types ---
pub use heal::{HealRequest, HealStats, ShardIndex};
pub use id::{BucketId, NodeId, ObjectKey, SegmentId};
// --- Metadata types ---
pub use metadata::{
    ChunkRef, ObjectMetadata, SegmentIndexEntry, SegmentMetadata, StorageLocation, Tombstone,
};
// --- Node / cluster operation types ---
pub use node::{
    Incarnation, IntendedFor, NodeState, OperationType, PeerAddress, VnodeRange, WriteAck,
    WriteQuorum, WriteResult,
};
// HashOutput moved to oceanfs-hash per ADR-0008; re-exported for backward compatibility.
pub use oceanfs_hash::HashOutput;
