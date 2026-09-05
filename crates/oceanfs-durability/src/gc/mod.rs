//! Garbage collection — tombstone processing and segment compaction.
//!
//! The garbage collector periodically scans deletion tombstones,
//! computes liveness ratios per segment, and compacts segments whose
//! live-byte ratio falls below a configurable threshold.

mod compaction_crash;
mod compaction_recovery;
mod config;
mod garbage_collector;
mod liveness_tracker;
mod orphan_reaper;
mod segment_compactor;
mod stats;

pub use compaction_recovery::{
    recover_incomplete_compactions, CompactionRecoveryAction, CompactionState, CompactionUnit,
    ObjectLookup, StoreObjectLookup,
};
pub use config::GcConfig;
pub use garbage_collector::{CompactionRemapFn, GarbageCollector, InMemoryShardStore};
pub use orphan_reaper::{OrphanReaper, OrphanStats};
pub use stats::GcStats;
