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
// async_trait generates #[must_use] on methods returning Result,
// which is redundant (Result is already #[must_use]). This lint fires
// in nightly-2026-08-10+ clippy and is denied via workspace RUSTFLAGS.
#![allow(clippy::double_must_use)]

mod buffer_pool;
mod error;
pub mod io;
pub mod metadata;
pub mod pool;
pub mod segment;
mod traits;
pub mod wal;

pub use buffer_pool::BufferPool;
pub use error::{Error, Result};
pub use metadata::{BatchOp, RocksDbMetadataStore, RocksDbMetrics};
pub use pool::{
    resolve_pool_root, PlacementPolicy, PoolIdResolver, PoolRegistry, PoolStatus, StoragePool,
};
pub use segment::{
    entry_is_garbage, ActiveSegment, CheckpointInfo, DataWalPos, DeleteEvent, EventCheckpoint,
    EventWal, EventWalPos, EventWalReader, LifecycleEntry, RebuildOutcome, ReserveEvent,
    SealConfig, SealEvent, SealingWork, SegmentEvent, SegmentHandle, SegmentHeader, SegmentIndex,
    SegmentLifecycle, SegmentLifecycleCoordinator, SegmentLifecycleRegistry, SegmentPool,
    SegmentReadSource, SegmentSealer, SegmentShard, SegmentSplitter, SegmentState, TierRouter,
    TransitionError,
};
pub use wal::{count_wal_files, WalEntry, WalReader, WalWriter}; // ---------------------------------------------------------------------------
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
