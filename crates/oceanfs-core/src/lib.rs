//! OceanFS core types and traits.
//!
//! `oceanfs-core` is the foundation crate — it depends only on `oceanfs-hash`
//! (for `HashOutput`). It provides shared types (`SegmentId`, `NodeId`, `BucketId`),
//! the HLC clock, error types, and configuration structs used by every other crate.
//!
//! # Crate Purity
//!
//! This crate may only depend on `oceanfs-hash` among all `oceanfs-*` crates.
//! The purity check is: `cargo tree --edges normal -p oceanfs-core | grep oceanfs-`
//! must produce only `oceanfs-hash`.

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
pub mod metrics;
mod timeouts;
mod types;

// Generated protobuf message types (common, segment, membership).
// Service stubs are generated in oceanfs-network.
#[allow(missing_docs, clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::all)]
pub mod proto {
    /// Generated protobuf types for `oceanfs.common` package.
    #[allow(missing_docs)]
    pub mod common {
        include!("generated/oceanfs.common.rs");
    }
    /// Generated protobuf types for `oceanfs.segment` package.
    #[allow(missing_docs)]
    pub mod segment {
        include!("generated/oceanfs.segment.rs");
    }
    /// Generated protobuf types for `oceanfs.membership` package.
    #[allow(missing_docs)]
    pub mod membership {
        include!("generated/oceanfs.membership.rs");
    }
}

pub mod proto_convert;

pub use config::{
    shard, AccelConfig, AntiEntropyConfig, AuthConfig, CompressionConfig, LifecycleConfig,
    MetadataConfig, NodeConfig, RingConfig, WalConfig,
};
pub use conflict::{ConflictResolver, LwwResolver, Resolution};
pub use error::{Error, Result};
pub use hlc::{Hlc, HlcClock};
pub use metrics::{
    sub_millisecond_histogram_config, validate_counter_name, Counter, Gauge, Histogram,
    HistogramConfig, LabelSet, MetricRegistrar,
};
pub use proto_convert::ConversionError;
pub use timeouts::OperationTimeouts;
pub use types::{
    BucketId, CacheInvalidateRequest, ChunkRef, CodecConfig, CodecType, CompressConfig,
    CompressionTier, EncodingPlan, EvictionPolicyType, FetchStrategy, FetchStrategyConfig,
    GossipConfig, GpuConfig, HashKey, HashOutput, HealConfig, HealRequest, HealStats, Incarnation,
    IntendedFor, NodeId, NodeState, NvcompCodec, NvcompConfig, ObjectKey, ObjectMetadata,
    OperationType, PeerAddress, PoolConfig, RpcConfig, SegmentId, SegmentIndexEntry,
    SegmentMetadata, SegmentSizeConfig, ShardIndex, SizeTier, StorageLocation, Tombstone,
    VnodeRange, WriteAck, WriteQuorum, WriteResult,
};
