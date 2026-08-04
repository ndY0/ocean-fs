//! Segment storage engine.
//!
//! Manages the lifecycle of segments: buffering writes in active segments,
//! persisting to the WAL, sealing segments when full, encoding via erasure
//! coding, storing metadata in RocksDB, and distributing shards across
//! the cluster.
//!
//! # Architecture
//!
//! The storage engine has four main components:
//! - **Segment buffer:** in-memory append-only `BytesMut` buffers
//! - **WAL:** sequential write-ahead log for crash recovery
//! - **Metadata:** RocksDB-backed object and segment metadata
//! - **Segment store:** manages sealed segments on disk

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

mod blob_store;
mod buffer_pool;
mod error;
pub mod metadata;
pub mod segment;
mod traits;
pub mod wal;

pub use blob_store::BlobStore;
pub use buffer_pool::BufferPool;
pub use error::{Error, Result};
pub use metadata::{BatchOp, RocksDbMetadataStore};
pub use segment::{
    ActiveSegment, SealConfig, SegmentHandle, SegmentHeader, SegmentIndex, SegmentSealer,
    SegmentShard, SegmentSplitter, TierRouter,
};
pub use wal::{WalEntry, WalReader, WalWriter};

// ---------------------------------------------------------------------------
// Generated gRPC service stubs
// ---------------------------------------------------------------------------

/// Generated gRPC client and server stubs for storage services.
#[allow(missing_docs, clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::all)]
pub mod storage_rpc {
    include!("generated/oceanfs.storage.rs");
}

// Re-export generated client and server types for ergonomic use.
pub use storage_rpc::{
    segment_rpc_client::SegmentRpcClient,
    segment_rpc_server::{SegmentRpc, SegmentRpcServer},
};
