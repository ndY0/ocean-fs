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
mod sync;
mod writer;

pub use entry::WalEntry;
pub use reader::WalReader;
pub use writer::WalWriter;
