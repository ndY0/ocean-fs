//! S3-compatible HTTP server and request coordinators.

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
mod bucket_config;
mod error;
mod hinted_handoff;
mod read;
mod read_coordinator;
mod router;
mod s3_handler;
mod write;
mod write_coordinator;

pub use admin::AdminHandler;
pub use bucket_config::{BucketConfigStore, BucketPolicy};
pub use error::{Error, Result};
pub use hinted_handoff::{HintRecord, HintedHandoff};
pub use read_coordinator::{ReadCoordinator, ReadOutcome, ReadRequest, ReadResult};
pub use router::{RouteRequest, RouteResponse, Router};
pub use s3_handler::S3Handler;
pub use write_coordinator::{WriteCoordinator, WriteRequest};

// Re-export core types used in the server's public API.
pub use oceanfs_core::HashKey;
