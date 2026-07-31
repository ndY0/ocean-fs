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

mod anti_entropy;
mod buffer_pool;
mod error;
mod gc;
pub mod metadata;
mod scrub;
pub mod segment;
pub mod wal;

pub use anti_entropy::MerkleTree;
pub use buffer_pool::BufferPool;
pub use error::{Error, Result};
pub use gc::{GarbageCollector, GcConfig};
pub use metadata::{BatchOp, MetadataStore};
pub use scrub::ScrubCoordinator;
pub use segment::{
    ActiveSegment, SealConfig, SegmentHandle, SegmentHeader, SegmentIndex, SegmentSealer,
    SegmentShard, SegmentSplitter, TierRouter,
};
pub use wal::{WalEntry, WalReader, WalWriter};
