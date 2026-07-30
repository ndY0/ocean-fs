//! L1 Object Data Cache — in-memory LRU of hot blob payloads.
//!
//! Serves frequently accessed blobs with zero disk I/O. Bucket-scoped,
//! TTL-based eviction, size-gated insertion. DashMap for concurrent access.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use bytes::Bytes;
use dashmap::DashMap;
use oceanfs_core::{BucketId, ObjectKey};

/// Statistics for the L1 object cache.
#[derive(Debug, Default)]
pub struct ObjectCacheStats {
    /// Number of cache hits.
    pub hits: AtomicU64,
    /// Number of cache misses.
    pub misses: AtomicU64,
    /// Number of evicted entries.
    pub evictions: AtomicU64,
    /// Current cache size in bytes (approximate).
    pub size_bytes: AtomicU64,
}

/// Configuration for the L1 object cache.
#[derive(Debug, Clone)]
pub struct ObjectCacheConfig {
    /// Whether the cache is enabled.
    pub enabled: bool,
    /// Maximum cache size in bytes.
    pub max_size_bytes: u64,
    /// Time-to-live for cache entries in milliseconds (0 = no expiry).
    pub ttl_ms: u64,
    /// Maximum blob size to cache.
    pub max_blob_size: u64,
}

impl Default for ObjectCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size_bytes: 512 * 1024 * 1024,
            ttl_ms: 60_000,
            max_blob_size: 1024 * 1024,
        }
    }
}

struct CacheEntry {
    data: Bytes,
    inserted_at: Instant,
}

/// L1 object data cache — bucket-scoped LRU of blob payloads.
pub struct ObjectCache {
    config: ObjectCacheConfig,
    entries: DashMap<(BucketId, ObjectKey), CacheEntry>,
    stats: ObjectCacheStats,
}

impl ObjectCache {
    /// Creates a new object cache.
    pub fn new(config: ObjectCacheConfig) -> Self {
        Self { config, entries: DashMap::new(), stats: ObjectCacheStats::default() }
    }

    /// Retrieves a blob from the cache.
    ///
    /// Returns `None` on miss or TTL expiry.
    pub fn get(&self, bucket: &BucketId, key: &ObjectKey) -> Option<Bytes> {
        if !self.config.enabled {
            return None;
        }

        let lookup_key = (bucket.clone(), key.clone());
        if let Some(entry) = self.entries.get(&lookup_key) {
            // Check TTL.
            if self.config.ttl_ms > 0 {
                let age = entry.inserted_at.elapsed();
                if age > Duration::from_millis(self.config.ttl_ms) {
                    drop(entry);
                    self.entries.remove(&lookup_key);
                    self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.data.clone());
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Inserts a blob into the cache.
    pub fn put(&self, bucket: BucketId, key: ObjectKey, data: Bytes) {
        if !self.config.enabled {
            return;
        }
        if data.len() as u64 > self.config.max_blob_size {
            return;
        }

        let entry = CacheEntry { inserted_at: Instant::now(), data: data.clone() };

        self.entries.insert((bucket, key), entry);
        self.stats.size_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
    }

    /// Invalidates a cache entry.
    pub fn invalidate(&self, bucket: &BucketId, key: &ObjectKey) {
        self.entries.remove(&(bucket.clone(), key.clone()));
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> &ObjectCacheStats {
        &self.stats
    }

    /// Returns the hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let hits = self.stats.hits.load(Ordering::Relaxed) as f64;
        let misses = self.stats.misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total > 0.0 {
            hits / total
        } else {
            0.0
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn get_miss_returns_none() {
        let cache = ObjectCache::new(ObjectCacheConfig::default());
        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }

    #[test]
    fn put_then_get_returns_data() {
        let cache = ObjectCache::new(ObjectCacheConfig::default());
        let data = Bytes::from_static(b"hello");
        cache.put(BucketId::new("b"), ObjectKey::new("k"), data.clone());

        let got = cache.get(&BucketId::new("b"), &ObjectKey::new("k")).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn invalidate_removes_entry() {
        let cache = ObjectCache::new(ObjectCacheConfig::default());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), Bytes::from_static(b"data"));
        cache.invalidate(&BucketId::new("b"), &ObjectKey::new("k"));
        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }

    #[test]
    fn max_blob_size_gate() {
        let config = ObjectCacheConfig { max_blob_size: 10, ..Default::default() };
        let cache = ObjectCache::new(config);
        cache.put(BucketId::new("b"), ObjectKey::new("k"), Bytes::from(vec![0u8; 20]));
        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }

    #[test]
    fn hit_rate_computes_correctly() {
        let cache = ObjectCache::new(ObjectCacheConfig::default());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), Bytes::from_static(b"x"));
        cache.get(&BucketId::new("b"), &ObjectKey::new("k"));
        cache.get(&BucketId::new("b"), &ObjectKey::new("nope"));
        let rate = cache.hit_rate();
        assert!((rate - 0.5).abs() < 0.01);
    }
}
