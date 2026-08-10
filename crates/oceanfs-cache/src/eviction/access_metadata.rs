//! Per-access metadata for eviction policy decisions.
//!
//! The [`AccessMetadata`] struct carries signals from each cache access
//! to the eviction policy. Current policies (GDSF, TTL-LRU) use only
//! a subset of these fields; the remaining fields are reserved for a
//! future adaptive learner policy.

use std::collections::HashMap;

use oceanfs_core::BucketId;

/// Metadata describing a single cache access event.
///
/// Passed to [`EvictionPolicy`](super::EvictionPolicy) methods
/// (`on_access`, `on_insert`) to inform eviction decisions.
///
/// # Examples
///
/// ```
/// use oceanfs_cache::eviction::AccessMetadata;
/// use oceanfs_core::BucketId;
///
/// let meta = AccessMetadata::new(BucketId::new("photos"), 1024);
/// assert_eq!(meta.blob_size, 1024);
/// ```
#[derive(Debug, Clone)]
pub struct AccessMetadata {
    /// Wall-clock time of the access.
    pub timestamp: std::time::Instant,
    /// Size of the blob in bytes.
    pub blob_size: u64,
    /// The bucket containing the accessed object.
    pub bucket_id: BucketId,
    /// Content type of the blob (e.g., "image/jpeg"), if known.
    pub content_type: Option<String>,
    /// Extensible key-value store for future learner features.
    /// Unused by TTL-LRU and GDSF policies.
    pub extensions: HashMap<String, String>,
}

impl AccessMetadata {
    /// Creates a new access metadata record with the current time.
    ///
    /// The `content_type` and `extensions` fields are initialized
    /// to their defaults (empty). They can be populated after
    /// construction if the caller has additional signal data.
    pub fn new(bucket_id: BucketId, blob_size: u64) -> Self {
        Self {
            timestamp: std::time::Instant::now(),
            blob_size,
            bucket_id,
            content_type: None,
            extensions: HashMap::new(),
        }
    }
}
