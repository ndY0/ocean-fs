//! Cache invalidation types.
//!
//! Contains `CacheInvalidateRequest` — a request to evict stale cache entries
//! from peer nodes after an object is modified or deleted.

use super::id::{BucketId, ObjectKey};

/// A request to invalidate a cache entry, propagated via gossip or direct RPC.
///
/// Sent by the node that modified or deleted an object to inform peers that
/// their stale cache entries should be evicted.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{BucketId, CacheInvalidateRequest, ObjectKey};
///
/// let req = CacheInvalidateRequest {
///     bucket: BucketId::new("my-bucket"),
///     key: ObjectKey::new("photo.jpg"),
/// };
/// assert_eq!(req.bucket.as_str(), "my-bucket");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CacheInvalidateRequest {
    /// The bucket containing the invalidated object.
    pub bucket: BucketId,
    /// The key of the invalidated object.
    pub key: ObjectKey,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn cache_invalidate_request_construction() {
        let req = CacheInvalidateRequest { bucket: BucketId::new("b"), key: ObjectKey::new("k") };
        assert_eq!(req.bucket.as_str(), "b");
        assert_eq!(req.key.as_str(), "k");
    }
}
