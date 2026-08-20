//! OceanFS durability crate — background maintenance and data integrity.
//!
//! This crate contains durability background tasks (anti-entropy, garbage
//! collection, healing, scrubbing) and their gRPC service stubs. It depends
//! on `oceanfs-storage-api` for trait contracts and `oceanfs-storage` for
//! concrete implementations.
//!
//! # Architecture
//!
//! - **Anti-entropy:** Merkle tree exchange between nodes
//! - **Garbage collection:** tombstone processing and segment compaction
//! - **Healing:** EC shard repair via `HealQueue` / `HealWorker`
//! - **Scrubbing:** full cluster-wide segment scan for integrity
//! - **Hinted handoff:** buffers writes for temporarily unreachable nodes
//! - **gRPC services:** healing and scrub RPC handlers

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

pub mod anti_entropy;
pub mod error;
pub mod gc;
pub mod heal;
pub mod hinted_handoff;
pub mod merkle;
pub mod scrub;
mod segment_store_impl;

// gRPC service stubs — moved from oceanfs-server
pub mod healing_service;
pub mod scrub_service;

pub use anti_entropy::{
    AntiEntropy, AntiEntropyConfig, AntiEntropyStats, InMemorySegmentStore, LeafRange, MerkleProof,
    MerkleRoot, MerkleTree, SegmentDataStore,
};
pub use error::{Error, Result};
pub use gc::{
    recover_incomplete_compactions, CompactionRecoveryAction, CompactionState, CompactionUnit,
    DiskSegmentShardStore, GarbageCollector, GcConfig, GcStats, InMemorySegmentShardStore,
    ObjectLookup, OrphanReaper, OrphanStats, SegmentShardStore, StoreObjectLookup,
};
pub use heal::{
    enqueue_heal, HealConfig, HealQueue, HealQueueSender, HealRequest, HealStats, HealWorker,
};
pub use hinted_handoff::{
    GrpcHintDeliveryClient, GrpcHintObjectFetcher, HintDeliveryClient, HintObjectFetcher,
    HintObjectReader, HintRecord, HintWal, HintedHandoff, HintedHandoffConfig,
    HintedHandoffManager,
};
pub use scrub::{ScrubConfig, ScrubCoordinator, ScrubReport, ScrubReportBuilder};
pub use segment_store_impl::DiskSegmentStore;

// ---------------------------------------------------------------------------
// Generated gRPC service stubs
// ---------------------------------------------------------------------------

/// Generated gRPC client and server stubs for healing services.
#[allow(missing_docs, clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::all)]
pub mod healing_rpc {
    include!("generated/oceanfs.healing.rs");
}

/// Generated gRPC client and server stubs for scrub services.
#[allow(missing_docs, clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::all)]
pub mod scrub_rpc {
    include!("generated/oceanfs.scrub.rs");
}

/// Generated protobuf types for hinted handoff records and batched delivery.
#[allow(missing_docs, clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::all)]
pub mod hinted_handoff_rpc {
    include!("generated/oceanfs.hinted_handoff.rs");
}

// Re-export generated client and server types for ergonomic use.
pub use healing_rpc::{
    healing_rpc_client::HealingRpcClient,
    healing_rpc_server::{HealingRpc, HealingRpcServer},
};
pub use scrub_rpc::{
    scrub_rpc_client::ScrubRpcClient,
    scrub_rpc_server::{ScrubRpc, ScrubRpcServer},
};
