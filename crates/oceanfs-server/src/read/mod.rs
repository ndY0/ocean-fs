//! Read coordinator sub-modules.
//!
//! Contains multi-chunk assembly, parallel shard fetch,
//! read repair logic, and the read coordinator.

pub mod assembly;
pub mod coordinator;
pub(crate) mod fetch;
pub(crate) mod repair;

#[allow(unused_imports)]
pub(crate) use fetch::fetch_chunks;
#[allow(unused_imports)]
pub(crate) use repair::schedule_repair;
