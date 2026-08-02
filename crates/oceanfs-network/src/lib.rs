//! Internal networking and gRPC transport.
//!
//! Manages a persistent connection pool of gRPC channels per peer node,
//! with HTTP/2 multiplexing, keepalive, and idle eviction. Provides the
//! `RpcClient` trait for service-specific stubs and re-exports generated
//! gRPC client types for all OceanFS services.
//!
//! ## Generated Services
//!
//! This crate generates client and server stubs for:
//! - `SegmentRpc` — segment append / shard fetch
//! - `GossipRpc` — membership gossip push / pull
//! - `HealingRpc` — hinted handoff / Merkle exchange
//! - `CacheRpc` — cache invalidation
//!
//! Message types (common, segment, membership) are generated in
//! `oceanfs-core` and referenced via `extern_path`.

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
// Internal infrastructure fields and functions are wired by integration code.
#![allow(dead_code)]

mod client;
mod pool;
mod tls;

pub use client::RpcClient;
pub use pool::{ConnectionPool, PooledChannel};

// ---------------------------------------------------------------------------
// Generated gRPC service stubs
// ---------------------------------------------------------------------------

/// Generated gRPC client and server stubs for gossip services.
#[allow(missing_docs, clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::all)]
pub mod gossip {
    include!("generated/oceanfs.gossip.rs");
}

/// Generated gRPC client and server stubs for storage services.
#[allow(missing_docs, clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::all)]
pub mod storage {
    include!("generated/oceanfs.storage.rs");
}

/// Generated gRPC client and server stubs for healing services.
#[allow(missing_docs, clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::all)]
pub mod healing {
    include!("generated/oceanfs.healing.rs");
}

/// Generated gRPC client and server stubs for cache invalidation services.
#[allow(missing_docs, clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::all)]
pub mod cache {
    include!("generated/oceanfs.cache.rs");
}

/// Generated gRPC client and server stubs for distributed scrub services.
#[allow(missing_docs, clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::all)]
pub mod scrub {
    include!("generated/oceanfs.scrub.rs");
}

// Re-export generated client types for ergonomic use.
pub use cache::{
    cache_rpc_client::CacheRpcClient,
    cache_rpc_server::{CacheRpc, CacheRpcServer},
};
pub use gossip::gossip_rpc_client::GossipRpcClient;
// Re-export generated server traits.
pub use gossip::gossip_rpc_server::{GossipRpc, GossipRpcServer};
pub use healing::{
    healing_rpc_client::HealingRpcClient,
    healing_rpc_server::{HealingRpc, HealingRpcServer},
};
pub use scrub::{
    scrub_rpc_client::ScrubRpcClient,
    scrub_rpc_server::{ScrubRpc, ScrubRpcServer},
};
pub use storage::{
    segment_rpc_client::SegmentRpcClient,
    segment_rpc_server::{SegmentRpc, SegmentRpcServer},
};
