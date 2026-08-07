//! RocksDB-backed metadata persistence.
//!
//! Stores object metadata, segment metadata, and deletion tombstones
//! in three RocksDB column families. Provides strongly-typed CRUD
//! operations with batch atomic writes and prefix-range scans.

mod cf;
mod store;

pub use store::{BatchOp, RocksDbMetadataStore, RocksDbMetrics};
