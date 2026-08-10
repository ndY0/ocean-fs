//! Pluggable eviction policy trait and key type.

use std::fmt;

use oceanfs_core::{BucketId, ObjectKey};

use super::AccessMetadata;

/// A compound key identifying a cache entry across bucket-scoped caches.
///
/// Since eviction policies are shared across all buckets in a cache tier,
/// entries from different buckets must be disambiguated by their bucket.
///
/// # Examples
///
/// ```
/// use oceanfs_cache::eviction::CacheKey;
/// use oceanfs_core::{BucketId, ObjectKey};
///
/// let key = CacheKey::new(BucketId::new("photos"), ObjectKey::new("sunset.jpg"));
/// assert_eq!(key.bucket().as_str(), "photos");
/// assert_eq!(key.object_key().as_str(), "sunset.jpg");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    bucket: BucketId,
    object_key: ObjectKey,
}

impl CacheKey {
    /// Creates a new cache key from a bucket and object key.
    pub fn new(bucket: BucketId, object_key: ObjectKey) -> Self {
        Self { bucket, object_key }
    }

    /// Returns the bucket identifier.
    pub fn bucket(&self) -> &BucketId {
        &self.bucket
    }

    /// Returns the object key.
    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.bucket.as_str(), self.object_key.as_str())
    }
}

/// Pluggable eviction policy for the object and metadata caches.
///
/// Implementations range from simple TTL-LRU to adaptive learned policies.
/// The trait is called by the cache frontend on every access, insert,
/// eviction, and removal — the policy is strictly advisory (it selects
/// victims; the frontend performs the actual removal from the backing store).
///
/// All methods take `&self` (not `&mut self`) because the policy is called
/// from multiple concurrent cache operations. Implementations must use
/// interior mutability (atomic counters, `DashMap`, `parking_lot::Mutex`).
///
/// # Examples
///
/// ```no_run
/// use oceanfs_cache::eviction::{AccessMetadata, CacheKey, EvictionPolicy};
/// use oceanfs_core::{BucketId, ObjectKey};
///
/// struct MyPolicy;
///
/// impl EvictionPolicy for MyPolicy {
///     fn on_access(&self, _key: &CacheKey, _meta: &AccessMetadata) {}
///     fn on_insert(&self, _key: &CacheKey, _size: usize, _meta: &AccessMetadata) {}
///     fn select_victim(&self) -> Option<CacheKey> { None }
///     fn on_evict(&self, _key: &CacheKey) {}
///     fn on_remove(&self, _key: &CacheKey) {}
/// }
/// ```
pub trait EvictionPolicy: Send + Sync {
    /// Called on every cache hit.
    ///
    /// The policy may update internal state (e.g., promote in LRU order,
    /// increment a frequency counter).
    fn on_access(&self, key: &CacheKey, meta: &AccessMetadata);

    /// Called when a new entry is inserted.
    ///
    /// `size` is the entry's memory footprint in bytes. The policy may
    /// track this for size-aware eviction.
    fn on_insert(&self, key: &CacheKey, size: usize, meta: &AccessMetadata);

    /// Selects a victim for eviction.
    ///
    /// Called when the cache frontend needs to free memory. Returns the
    /// key of the entry that should be evicted, or `None` if the policy
    /// has no preference (frontend falls back to arbitrary eviction, or
    /// the policy determines no eviction is needed).
    ///
    /// Called in a loop until the cache is below its memory threshold.
    fn select_victim(&self) -> Option<CacheKey>;

    /// Called after an entry has been successfully evicted from the backing
    /// store.
    ///
    /// The policy may clean up any per-entry state.
    fn on_evict(&self, key: &CacheKey);

    /// Called when an entry is explicitly removed (invalidation, delete).
    ///
    /// Distinct from eviction — the policy should not count forced removals
    /// as evidence for its eviction heuristics.
    fn on_remove(&self, key: &CacheKey);
}
