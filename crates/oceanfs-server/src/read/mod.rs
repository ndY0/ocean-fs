//! Read coordinator sub-modules.
//!
//! Contains multi-chunk assembly, parallel shard fetch, and
//! read repair logic.

pub mod assembly;
pub(crate) mod fetch;
pub(crate) mod repair;

#[allow(unused_imports)]
pub(crate) use fetch::fetch_chunks;
#[allow(unused_imports)]
pub(crate) use repair::schedule_repair;
