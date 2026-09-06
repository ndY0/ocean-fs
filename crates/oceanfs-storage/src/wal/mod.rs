//! Write-Ahead Log.
//!
//! Provides durability for segment append operations before EC encoding
//! completes. The WAL is append-only, sequential, and uses group commit
//! for amortized fsync.
//!
//! # Architecture
//!
//! - [`WalEntry`]: binary-serializable entry (segment_id, offset, length, checksum)
//! - [`WalWriter`]: writes entries sequentially, rotates files, batch-fsyncs
//! - [`WalReader`]: replays entries on node restart
//! - `WalSyncGroup`: internal — collects pending fsync waiters (pub(crate))

mod entry;
mod reader;
mod replay;
mod sync;
mod writer;

pub use entry::WalEntry;
pub use reader::WalReader;
pub use replay::{cleanup_old_wal_files, count_wal_files, replay_wal, ReplaySummary};
/// Re-exported for the segment flush coordinator (`io/segment_flush.rs`),
/// which applies the same `sync_file_range` + `fdatasync` optimisation to
/// sealed segment files during group commit.
pub(crate) use sync::sync_file_range_write;
/// Re-exported for the segment event WAL (`segment/event_wal.rs`), which
/// instantiates its own group-commit fsync domain (ADR-0024 Decision 4).
pub(crate) use sync::WalSyncGroup;
pub use writer::{verify_wal_write, WalWriter};
