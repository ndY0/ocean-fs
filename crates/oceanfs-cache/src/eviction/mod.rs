//! Pluggable cache eviction policies.
//!
//! This module provides the [`EvictionPolicy`] trait and two concrete
//! implementations:
//!
//! - [`GdsfPolicy`] — Greedy-Dual Size Frequency for L1 object cache
//! - [`TtlLruPolicy`] — LRU with TTL for L2 metadata cache
//!
//! The trait is consumed by [`ObjectCache`](crate::ObjectCache) and
//! [`MetadataCache`](crate::MetadataCache) to replace the previous
//! O(n) linear scan eviction with O(log n) or O(1) algorithms.

mod access_metadata;
mod gdsf;
mod trait_def;
mod ttl_lru;

pub use access_metadata::AccessMetadata;
pub use gdsf::{GdsfConfig, GdsfPolicy};
pub use trait_def::{CacheKey, EvictionPolicy};
pub use ttl_lru::{TtlLruConfig, TtlLruPolicy};
