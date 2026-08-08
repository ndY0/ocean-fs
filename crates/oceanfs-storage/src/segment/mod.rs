//! Segment types — active buffer, handle, and per-core sharding.
//!
//! The segment module manages the lifecycle of segments from active
//! (in-memory append-only buffers) to sealed (on-disk immutable).

pub mod buffer;
pub mod handle;
pub mod header;
pub mod index;
pub(crate) mod pool;
pub(crate) mod route_write;
pub mod sealer;
pub mod shard;
pub mod splitter;
pub mod tier;

pub use buffer::ActiveSegment;
pub use handle::SegmentHandle;
pub use header::SegmentHeader;
pub use index::SegmentIndex;
pub use pool::{SealingWork, SegmentPool};
pub use sealer::{SealConfig, SegmentSealer};
pub use shard::SegmentShard;
pub use splitter::SegmentSplitter;
pub use tier::TierRouter;
