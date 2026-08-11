//! Storage API crate — interface contracts for OceanFS storage backends.
//!
//! This crate defines the traits that every storage backend must implement:
//! [`SegmentStore`], [`MetadataStore`], [`BlobStore`], and [`WalWriter`].
//! It depends only on [`oceanfs_core`] and serves as the common interface
//! between coordinators/durability tasks and concrete storage implementations.
//!
//! # Architecture
//!
//! ```text
//! oceanfs-core
//!     ↓
//! oceanfs-storage-api   ← THIS CRATE (traits only, no implementations)
//!     ↓                    ↓                    ↓
//! oceanfs-storage     oceanfs-server     oceanfs-durability
//! (RocksDB impls)     (consumes traits)  (consumes traits)
//! ```
//!
//! # Multi-Backend Readiness
//!
//! By separating interface from implementation, `oceanfs-storage-api` enables
//! alternative storage backends (FUSE, S3, in-memory) without coupling to
//! RocksDB internals. Test mocks can implement these traits without linking
//! any storage engine.

#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    missing_docs
)]
// async_trait generates #[must_use] on methods returning Result,
// which is redundant (Result is already #[must_use]). This lint fires
// in nightly-2026-08-10+ clippy and is denied via workspace RUSTFLAGS.
#![allow(clippy::double_must_use)]

mod blob_store;
pub mod error;
mod metadata_store;
mod segment_store;
mod wal_writer;

pub use blob_store::BlobStore;
pub use metadata_store::{BatchOp, MetadataStore};
// Re-export relevant oceanfs-core types used in trait signatures.
pub use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata, SegmentId, SegmentMetadata};
pub use segment_store::{SegmentHandle, SegmentStore};
pub use wal_writer::WalWriter;
