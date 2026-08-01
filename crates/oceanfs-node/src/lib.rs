//! OceanFS node — composition root.
//!
//! Wires together all concrete implementations from the subsystem crates
//! (storage, routing, membership, networking, caching) and starts the
//! server. This is the **only** crate that imports concrete types across
//! subsystem boundaries.
//!
//! # Background Tasks
//!
//! Manages long-running background operations: healing, scrubbing,
//! garbage collection, anti-entropy, and gossip protocol.

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

mod metadata_adapter;
mod node;

pub use metadata_adapter::MetadataStoreAdapter;
pub use node::{BackgroundTasks, Node};

// Re-export common types used by node consumers.
pub use oceanfs_core::NodeConfig;
