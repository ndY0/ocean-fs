//! OceanFS core types and traits.
//!
//! `oceanfs-core` is the foundation crate — it has zero internal dependencies.
//! It provides shared types (`SegmentId`, `NodeId`, `BucketId`), the HLC
//! clock, error types, and configuration structs used by every other crate.
//!
//! # Crate Purity
//!
//! This crate must never depend on any other `oceanfs-*` crate. CI enforces
//! this via `cargo tree --edges normal -p oceanfs-core | grep oceanfs-`
//! which must produce no output.

// ---------------------------------------------------------------------------
// Lint attributes
// ---------------------------------------------------------------------------
#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    missing_docs
)]
// ---------------------------------------------------------------------------

mod config;
mod conflict;
mod error;
mod hlc;
mod timeouts;
mod types;

pub use config::{AccelConfig, AuthConfig, MetadataConfig, NodeConfig, RingConfig, WalConfig};
pub use conflict::{ConflictResolver, LwwResolver, Resolution};
pub use error::{Error, Result};
pub use hlc::{Hlc, HlcClock};
pub use timeouts::OperationTimeouts;
pub use types::{
    BucketId, CacheInvalidateRequest, ChunkRef, CodecConfig, CodecType, CompressConfig,
    CompressionTier, EncodingPlan, GpuConfig, GossipConfig, HashKey, HashOutput, Incarnation,
    IntendedFor, MetadataStore, NodeId, NodeState, NvcompCodec, NvcompConfig, ObjectKey,
    ObjectMetadata, OperationType, PeerAddress, PoolConfig, RpcConfig, SegmentId,
    SegmentIndexEntry, SegmentMetadata, SegmentSizeConfig, SizeTier, StorageLocation,
    Tombstone, VnodeRange, WriteAck, WriteQuorum, WriteResult,
};
