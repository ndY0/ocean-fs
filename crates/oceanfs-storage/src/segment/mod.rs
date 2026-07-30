//! Segment types — active buffer, handle, and per-core sharding.
//!
//! The segment module manages the lifecycle of segments from active
//! (in-memory append-only buffers) to sealed (on-disk immutable).

pub(crate) mod buffer;
pub mod handle;
pub(crate) mod header;
pub mod index;
pub(crate) mod sealer;
pub(crate) mod shard;
pub(crate) mod splitter;
pub(crate) mod tier;

pub use handle::SegmentHandle;
pub use index::SegmentIndex;
