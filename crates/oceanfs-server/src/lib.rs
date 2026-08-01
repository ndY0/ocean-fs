//! S3-compatible HTTP server and request coordinators.
//!
//! This crate provides the HTTP server layer (S3-compatible REST API),
//! distributed read/write coordinators, and the integration glue
//! between the storage, routing, membership, and networking crates.
//!
//! ## Architecture
//!
//! - [`S3Handler`]: axum-based HTTP handlers for S3 object/bucket operations
//! - [`WriteCoordinator`]: orchestrates blob writes with quorum replication
//! - [`ReadCoordinator`]: parallel shard fetch and blob reconstruction
//! - [`AdminHandler`]: cluster health, metrics, and admin endpoints
//! - [`BucketConfigStore`]: per-bucket policy management with `ArcSwap`
//! - [`Router`]: request routing via consistent hashing

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

mod admin;
pub mod auth;
mod bucket_config;
mod error;
mod hinted_handoff;
pub(crate) mod metadata_ops;
mod read;
mod read_coordinator;
mod router;
mod s3_handler;
pub mod s3_xml;
mod write;
mod write_coordinator;

pub use admin::AdminHandler;
pub use bucket_config::{BucketConfigStore, BucketPolicy};
pub use error::{Error, Result};
pub use hinted_handoff::{HintRecord, HintedHandoff};
// Re-export core types used in the server's public API.
pub use oceanfs_core::HashKey;
pub use read_coordinator::{ReadCoordinator, ReadOutcome, ReadRequest, ReadResult};
pub use router::{RouteRequest, RouteResponse, Router};
pub use s3_handler::S3Handler;
pub use write_coordinator::{WriteCoordinator, WriteRequest};
