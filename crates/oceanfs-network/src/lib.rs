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
//!
//! ## Unsafe Code Policy
//!
//! This crate uses `#![deny(unsafe_code)]` rather than `#![forbid(unsafe_code)]`
//! to permit `#[allow(unsafe_code)]` on individual `libc::setsockopt` wrappers
//! for Linux socket tuning (`SO_BUSY_POLL`). These are advisory hints with
//! trivial safety invariants. All other unsafe is forbidden. See ADR-0012.

#![deny(unsafe_code)]
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
mod socket_opts;
mod tls;

pub use client::RpcClient;
pub use pool::{ConnectionPool, PooledChannel};
pub use socket_opts::{
    apply_opts_to_fd, create_reuseport_listener, set_busy_poll, set_quickack, set_reuseport,
};

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
