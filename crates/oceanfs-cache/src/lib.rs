//! Multi-layer caching subsystem.
//!
//! Three cache layers between the HTTP frontend and the storage engine:
//!
//! - **L1 Object Cache:** in-memory LRU of hot blob payloads (zero disk I/O)
//! - **L2 Metadata Cache:** LRU of `ObjectMetadata` entries (avoids RocksDB)
//! - **L3 Negative Cache:** Bloom filter for non-existent keys (constant-time 404)
//!
//! All caches are node-local and eventually consistent.

#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    missing_docs
)]

mod error;
mod l1_object;
mod l2_metadata;
mod l3_negative;
mod prefetch;

pub use error::{Error, Result};
pub use l1_object::{CacheStats, ObjectCache, ObjectCacheConfig};
pub use l2_metadata::{MetadataCache, MetadataCacheConfig, MetadataCacheStats};
pub use l3_negative::{NegativeCache, NegativeCacheConfig, NegativeCacheStats};
pub use prefetch::{PrefetchConfig, PrefetchEngine};

// ---------------------------------------------------------------------------
// Generated gRPC service stubs
// ---------------------------------------------------------------------------

/// Generated gRPC client and server stubs for cache invalidation services.
#[allow(missing_docs, clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::all)]
pub mod cache {
    include!("generated/oceanfs.cache.rs");
}

// Re-export generated client and server types for ergonomic use.
pub use cache::{
    cache_rpc_client::CacheRpcClient,
    cache_rpc_server::{CacheRpc, CacheRpcServer},
};
