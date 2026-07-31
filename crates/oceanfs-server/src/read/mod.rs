//! Read coordinator sub-modules.
//!
//! Contains parallel shard fetch and read repair logic.

pub(crate) mod fetch;
pub(crate) mod repair;

#[allow(unused_imports)]
pub(crate) use fetch::fetch_chunks;
#[allow(unused_imports)]
pub(crate) use repair::schedule_repair;
