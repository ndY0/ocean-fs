//! Internal networking and gRPC transport.
//!
//! Manages a persistent connection pool of gRPC channels per peer node,
//! with HTTP/2 multiplexing, keepalive, and idle eviction. Provides the
//! `RpcClient` trait for service-specific stubs.

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
