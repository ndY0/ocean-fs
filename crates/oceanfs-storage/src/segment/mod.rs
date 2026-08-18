//! Segment types — active buffer, handle, and per-core sharding.
//!
//! The segment module manages the lifecycle of segments from active
//! (in-memory append-only buffers) to sealed (on-disk immutable).

pub mod buffer;
#[cfg(test)]
pub(crate) mod crash_matrix;
pub mod event_checkpoint;
pub mod event_wal;
pub mod handle;
pub mod header;
pub mod index;
pub mod lifecycle;
pub(crate) mod parity_section;
pub(crate) mod pool;
pub(crate) mod repair;
pub(crate) mod route_write;
pub mod sealer;
pub mod shard;
pub mod splitter;
pub mod tier;

pub use buffer::ActiveSegment;
pub use event_checkpoint::{CheckpointInfo, EventCheckpoint};
pub use event_wal::{
    DataWalPos, DeleteEvent, EventWal, EventWalPos, EventWalReader, ReserveEvent, SealEvent,
    SegmentEvent,
};
pub use handle::SegmentHandle;
pub use header::SegmentHeader;
pub use index::SegmentIndex;
pub use lifecycle::{
    entry_is_garbage, LifecycleEntry, RebuildOutcome, SegmentLifecycle,
    SegmentLifecycleCoordinator, SegmentLifecycleRegistry, SegmentReadSource, SegmentState,
    TransitionError,
};
pub use pool::{SealingWork, SegmentPool};
pub use sealer::{SealConfig, SegmentSealer};
pub use shard::SegmentShard;
pub use splitter::SegmentSplitter;
pub use tier::TierRouter;
