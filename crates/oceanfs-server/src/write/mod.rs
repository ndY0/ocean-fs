//! Write coordinator sub-modules.

pub(crate) mod coordinator;
pub(crate) mod replication;

#[allow(unused_imports)]
pub(crate) use replication::replicate_write;
