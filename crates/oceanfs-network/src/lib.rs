//! Internal networking and gRPC transport.
//!
//! Manages a persistent connection pool of gRPC channels per peer node,
//! with HTTP/2 multiplexing, keepalive, and idle eviction. Provides the
//! `RpcClient` trait for service-specific stubs.
//!
//! ## Generated Services
//!
//! This crate generates client and server stubs for:
//! - `GossipRpc` — membership gossip push / pull
//!
//! Storage, healing, scrub, and cache service stubs are generated in
//! their owning crates (oceanfs-storage, oceanfs-cache) per architecture §2.4.

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

// Re-export generated client and server types for ergonomic use.
pub use gossip::{
    gossip_rpc_client::GossipRpcClient,
    gossip_rpc_server::{GossipRpc, GossipRpcServer},
};
