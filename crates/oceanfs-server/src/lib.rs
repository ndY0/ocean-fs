//! S3-compatible HTTP server and request coordinators.
//!
//! This crate defines **what the system does**: S3 API handlers, write
//! and read coordinators, admin endpoints, and authentication middleware.
//! It depends only on traits and core types — never on concrete storage
//! or networking implementations.
//!
//! # Dependency Inversion
//!
//! Concrete implementations (RocksDB metadata, gRPC connection pool,
//! SWIM membership) are wired together in `oceanfs_node::Node`.
//! This crate only imports `oceanfs-core` for types and defines the
//! traits that storage, networking, and membership must implement.

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

mod error;
mod router;

pub use router::Router;
