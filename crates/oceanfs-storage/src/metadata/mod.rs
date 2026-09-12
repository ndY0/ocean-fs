//! RocksDB-backed metadata persistence.
//!
//! Stores object metadata, segment metadata, and deletion tombstones
//! in three RocksDB column families. Provides strongly-typed CRUD
//! operations with batch atomic writes and prefix-range scans.

mod cf;
mod codec;
pub mod journal;
mod store;

pub use codec::{object_key_bytes, split_object_row_key};
pub use journal::{
    JournalEntry, JournalEpoch, JournalMetrics, JournalOp, JournalRead, MetadataJournal,
    TrimReport, MAX_JOURNAL_KEY_BYTES, SEGMENT_TARGET_BYTES,
};
pub use store::{BatchOp, RocksDbMetadataStore, RocksDbMetrics, SyncApplyOutcome};
