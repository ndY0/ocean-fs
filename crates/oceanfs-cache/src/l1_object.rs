//! L1 Object Data Cache — in-memory LRU of hot blob payloads.
//!
//! Serves frequently accessed blobs with zero disk I/O. Bucket-scoped,
//! TTL-based eviction, LRU eviction when size exceeds limit.
//! DashMap for concurrent access.

use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use dashmap::DashMap;
use oceanfs_core::{BucketId, ObjectKey};

/// Statistics for the L1 object cache.
///
/// All counters use relaxed atomics for minimal overhead on the hot path.
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Number of cache hits.
    pub hits: AtomicU64,
    /// Number of cache misses.
    pub misses: AtomicU64,
    /// Number of evicted entries.
    pub evictions: AtomicU64,
    /// Current cache size in bytes (approximate).
    pub size_bytes: AtomicU64,
    /// Current number of entries (approximate).
    pub entry_count: AtomicUsize,
}

impl CacheStats {
    /// Computes the hit rate as a value between 0.0 and 1.0.
    ///
    /// Returns 0.0 if no requests have been made yet.
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed) as f64;
        let misses = self.misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total > 0.0 {
            hits / total
        } else {
            0.0
        }
    }
}

/// Configuration for the L1 object cache.
///
/// Can be specified per-bucket or used as a global default.
#[derive(Debug, Clone)]
pub struct ObjectCacheConfig {
    /// Whether the cache is enabled.
    pub enabled: bool,
    /// Maximum cache size in bytes (approximate).
    pub max_size_bytes: u64,
    /// Time-to-live for cache entries in milliseconds (0 = no expiry).
    pub ttl_ms: u64,
    /// Maximum blob size to cache. Blobs larger than this are not inserted.
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

/// A single cache entry with data and metadata.
struct CacheEntry {
    /// The cached blob payload (shared reference counted, zero-copy clone).
    data: Bytes,
    /// When this entry was inserted (used for TTL checks).
    inserted_at: Instant,
    /// Approximate last-access timestamp for LRU eviction.
    /// Stored as an opaque counter; higher = more recently accessed.
    last_access: AtomicU64,
}

impl CacheEntry {
    fn new(data: Bytes) -> Self {
        Self { data, inserted_at: Instant::now(), last_access: AtomicU64::new(0) }
    }

    fn touch(&self, generation: u64) {
        self.last_access.store(generation, Ordering::Relaxed);
    }
}

/// Per-bucket cache of blob payloads.
struct BucketCache {
    config: ObjectCacheConfig,
    entries: DashMap<ObjectKey, CacheEntry>,
}

impl BucketCache {
    fn new(config: ObjectCacheConfig) -> Self {
        Self { config, entries: DashMap::new() }
    }
}

/// Global access-generation counter for LRU ordering.
///
/// Each get/put increments this; entries touched by those operations
/// record the current generation. During eviction we find the entry
/// with the smallest generation value (oldest access).
struct LruClock {
    generation: AtomicU64,
}

impl LruClock {
    fn new() -> Self {
        Self { generation: AtomicU64::new(1) }
    }

    /// Returns the next generation number (monotonically increasing).
    fn next(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::Relaxed)
    }
}

/// L1 object data cache — bucket-scoped LRU of blob payloads.
///
/// # Examples
///
/// ```
/// use bytes::Bytes;
/// use oceanfs_cache::ObjectCache;
/// use oceanfs_cache::ObjectCacheConfig;
/// use oceanfs_core::{BucketId, ObjectKey};
///
/// let cache = ObjectCache::new(ObjectCacheConfig::default());
/// let data = Bytes::from_static(b"hello world");
/// cache.put(BucketId::new("photos"), ObjectKey::new("sunset.jpg"), data.clone());
/// let hit = cache.get(&BucketId::new("photos"), &ObjectKey::new("sunset.jpg"));
/// assert_eq!(hit, Some(data));
/// ```
pub struct ObjectCache {
    default_config: ObjectCacheConfig,
    buckets: DashMap<BucketId, Arc<BucketCache>>,
    lru_clock: LruClock,
    stats: CacheStats,
}

impl ObjectCache {
    /// Creates a new object cache with a default configuration for all buckets.
    pub fn new(config: ObjectCacheConfig) -> Self {
        Self {
            default_config: config,
            buckets: DashMap::new(),
            lru_clock: LruClock::new(),
            stats: CacheStats::default(),
        }
    }

    /// Retrieves a blob from the cache.
    ///
    /// Returns `None` on miss, TTL expiry, or if the cache is disabled.
    pub fn get(&self, bucket: &BucketId, key: &ObjectKey) -> Option<Bytes> {
        let Some(bucket_cache) = self.buckets.get(bucket) else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if !bucket_cache.config.enabled {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        if let Some(entry) = bucket_cache.entries.get(key) {
            // Check TTL.
            if bucket_cache.config.ttl_ms > 0 {
                let age = entry.inserted_at.elapsed();
                if age > Duration::from_millis(bucket_cache.config.ttl_ms) {
                    let data_len = entry.data.len();
                    drop(entry);
                    bucket_cache.entries.remove(key);
                    self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                    self.stats.entry_count.fetch_sub(1, Ordering::Relaxed);
                    self.stats.size_bytes.fetch_sub(data_len as u64, Ordering::Relaxed);
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
            // Touch for LRU.
            let gen = self.lru_clock.next();
            entry.touch(gen);
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.data.clone());
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Inserts a blob into the cache.
    ///
    /// Blobs larger than the bucket's `max_blob_size` are silently skipped.
    /// If inserting would exceed `max_size_bytes`, LRU entries are evicted
    /// until there is room.
    pub fn put(&self, bucket_id: BucketId, key: ObjectKey, data: Bytes) {
        // Get or create bucket cache.
        let bucket_cache = self
            .buckets
            .entry(bucket_id.clone())
            .or_insert_with(|| Arc::new(BucketCache::new(self.default_config.clone())))
            .clone();

        if !bucket_cache.config.enabled {
            return;
        }

        // Size-gated: skip blobs larger than the threshold.
        if data.len() as u64 > bucket_cache.config.max_blob_size {
            return;
        }

        // If key already exists, update in place (no size change detection).
        if let Some(mut existing) = bucket_cache.entries.get_mut(&key) {
            let delta = data.len() as i64 - existing.data.len() as i64;
            existing.data = data;
            let gen = self.lru_clock.next();
            existing.touch(gen);
            if delta > 0 {
                self.stats.size_bytes.fetch_add(delta as u64, Ordering::Relaxed);
            } else if delta < 0 {
                self.stats.size_bytes.fetch_sub((-delta) as u64, Ordering::Relaxed);
            }
            return;
        }

        // Check capacity and evict if needed.
        self.evict_if_needed(&bucket_cache, data.len());

        let gen = self.lru_clock.next();
        let entry = CacheEntry::new(data);
        entry.touch(gen);

        let data_len = entry.data.len();
        bucket_cache.entries.insert(key, entry);
        self.stats.size_bytes.fetch_add(data_len as u64, Ordering::Relaxed);
        self.stats.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Invalidates a cache entry for the given bucket and key.
    ///
    /// Best-effort: may miss entries due to concurrent access or TTL races.
    pub fn invalidate(&self, bucket: &BucketId, key: &ObjectKey) {
        if let Some(bucket_cache) = self.buckets.get(bucket) {
            if let Some((_k, entry)) = bucket_cache.entries.remove(key) {
                let data_len = entry.data.len();
                self.stats.size_bytes.fetch_sub(data_len as u64, Ordering::Relaxed);
                self.stats.entry_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Returns the overall hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        self.stats.hit_rate()
    }

    /// Adds or updates a bucket-specific configuration.
    ///
    /// If the bucket already exists, its config is updated.
    pub fn set_bucket_config(&self, bucket: BucketId, config: ObjectCacheConfig) {
        if let Some(mut entry) = self.buckets.get_mut(&bucket) {
            // Replace the bucket cache with a new one using the new config.
            // Existing entries are preserved by moving them into a new cache.
            let old_entries = &entry.entries;
            let new_cache = Arc::new(BucketCache {
                config,
                entries: DashMap::with_capacity(old_entries.len()),
            });
            // Move entries.
            for item in old_entries.iter() {
                let (k, v) = item.pair();
                // Only move entries that fit within the new size gate.
                if v.data.len() as u64 <= new_cache.config.max_blob_size {
                    new_cache.entries.insert(k.clone(), CacheEntry::new(v.data.clone()));
                }
            }
            *entry = new_cache;
        } else {
            self.buckets.insert(bucket, Arc::new(BucketCache::new(config)));
        }
    }

    /// Removes all entries for a given bucket.
    pub fn clear_bucket(&self, bucket: &BucketId) {
        if let Some((_k, bucket_cache)) = self.buckets.remove(bucket) {
            let removed_count = bucket_cache.entries.len();
            let removed_size: usize =
                bucket_cache.entries.iter().map(|entry| entry.data.len()).sum();
            self.stats.entry_count.fetch_sub(removed_count, Ordering::Relaxed);
            self.stats.size_bytes.fetch_sub(removed_size as u64, Ordering::Relaxed);
        }
    }

    /// Evicts LRU entries until there is enough space for `needed_bytes`.
    fn evict_if_needed(&self, bucket_cache: &BucketCache, needed_bytes: usize) {
        let max_bytes = bucket_cache.config.max_size_bytes as usize;
        let current_size = approximate_size(&bucket_cache.entries);
        let target = current_size.saturating_add(needed_bytes);

        if target <= max_bytes {
            return;
        }

        // Scan all entries, find the one with the lowest last_access.
        // Evict one entry, then check again.
        let mut evicted = 0usize;
        let mut to_remove: Option<ObjectKey> = None;

        // We limit the eviction scan to avoid blocking too long on very large caches.
        // In practice, eviction is rare, so a linear scan is acceptable.
        loop {
            let current = approximate_size(&bucket_cache.entries);
            if current.saturating_add(needed_bytes) <= max_bytes || evicted >= 100 {
                break;
            }

            let mut min_gen = u64::MAX;
            for entry in bucket_cache.entries.iter() {
                let gen = entry.last_access.load(Ordering::Relaxed);
                if gen < min_gen {
                    min_gen = gen;
                    to_remove = Some(entry.key().clone());
                }
            }

            if let Some(key) = to_remove.take() {
                if let Some((_, entry)) = bucket_cache.entries.remove(&key) {
                    let data_len = entry.data.len();
                    self.stats.size_bytes.fetch_sub(data_len as u64, Ordering::Relaxed);
                    self.stats.entry_count.fetch_sub(1, Ordering::Relaxed);
                    self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                    evicted += 1;
                }
            }
        }
    }
}

/// Approximate total size of entries in a DashMap.
fn approximate_size(entries: &DashMap<ObjectKey, CacheEntry>) -> usize {
    entries.iter().map(|e| e.data.len()).sum()
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

    #[test]
    fn lru_eviction_when_cache_full() {
        let config = ObjectCacheConfig {
            max_size_bytes: 100,
            max_blob_size: 100,
            ttl_ms: 0, // no TTL
            enabled: true,
        };
        let cache = ObjectCache::new(config);

        // Insert entries that will fill the cache.
        let data1 = Bytes::from(vec![1u8; 60]);
        let data2 = Bytes::from(vec![2u8; 60]);
        cache.put(BucketId::new("b"), ObjectKey::new("k1"), data1.clone());
        cache.put(BucketId::new("b"), ObjectKey::new("k2"), data2.clone());

        // Both should be present since 60 + 60 = 120 > 100, one should be evicted.
        // The first entry (k1) should be evicted as it's least recently accessed.
        let k1 = cache.get(&BucketId::new("b"), &ObjectKey::new("k1"));
        let k2 = cache.get(&BucketId::new("b"), &ObjectKey::new("k2"));

        // At least one should have been evicted.
        assert!(k1.is_none() || k2.is_none());
        // Eviction counter should be non-zero.
        assert!(cache.stats().evictions.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn ttl_expiry_returns_none() {
        let config = ObjectCacheConfig { ttl_ms: 10, max_blob_size: 1024, ..Default::default() };
        let cache = ObjectCache::new(config);
        cache.put(BucketId::new("b"), ObjectKey::new("k"), Bytes::from_static(b"data"));

        // Wait for TTL to expire.
        std::thread::sleep(Duration::from_millis(20));

        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }

    #[test]
    fn per_bucket_isolation() {
        let cache = ObjectCache::new(ObjectCacheConfig::default());
        cache.put(BucketId::new("b1"), ObjectKey::new("k"), Bytes::from_static(b"b1-data"));
        cache.put(BucketId::new("b2"), ObjectKey::new("k"), Bytes::from_static(b"b2-data"));

        assert_eq!(
            cache.get(&BucketId::new("b1"), &ObjectKey::new("k")),
            Some(Bytes::from_static(b"b1-data"))
        );
        assert_eq!(
            cache.get(&BucketId::new("b2"), &ObjectKey::new("k")),
            Some(Bytes::from_static(b"b2-data"))
        );
    }

    #[test]
    fn update_existing_key_replaces_value() {
        let cache = ObjectCache::new(ObjectCacheConfig::default());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), Bytes::from_static(b"old"));
        cache.put(BucketId::new("b"), ObjectKey::new("k"), Bytes::from_static(b"new"));

        let got = cache.get(&BucketId::new("b"), &ObjectKey::new("k")).unwrap();
        assert_eq!(got, Bytes::from_static(b"new"));
    }

    #[test]
    fn entry_count_tracks_insertions() {
        let cache = ObjectCache::new(ObjectCacheConfig::default());
        assert_eq!(cache.stats().entry_count.load(Ordering::Relaxed), 0);

        cache.put(BucketId::new("b"), ObjectKey::new("k1"), Bytes::from_static(b"a"));
        cache.put(BucketId::new("b"), ObjectKey::new("k2"), Bytes::from_static(b"b"));
        assert_eq!(cache.stats().entry_count.load(Ordering::Relaxed), 2);

        cache.invalidate(&BucketId::new("b"), &ObjectKey::new("k1"));
        assert_eq!(cache.stats().entry_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn disabled_cache_always_misses() {
        let config = ObjectCacheConfig { enabled: false, ..Default::default() };
        let cache = ObjectCache::new(config);
        cache.put(BucketId::new("b"), ObjectKey::new("k"), Bytes::from_static(b"data"));
        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }

    #[test]
    fn cache_stats_hit_rate_standalone() {
        let stats = CacheStats::default();
        assert_eq!(stats.hit_rate(), 0.0);

        stats.hits.store(3, Ordering::Relaxed);
        stats.misses.store(1, Ordering::Relaxed);
        assert!((stats.hit_rate() - 0.75).abs() < 0.001);
    }

    #[test]
    fn set_bucket_config_changes_existing_bucket() {
        let cache = ObjectCache::new(ObjectCacheConfig::default());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), Bytes::from_static(b"data"));

        // Disable the bucket.
        cache.set_bucket_config(
            BucketId::new("b"),
            ObjectCacheConfig { enabled: false, ..Default::default() },
        );

        // Now the bucket should miss.
        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }

    #[test]
    fn clear_bucket_removes_all_entries() {
        let cache = ObjectCache::new(ObjectCacheConfig::default());
        cache.put(BucketId::new("b"), ObjectKey::new("k1"), Bytes::from_static(b"a"));
        cache.put(BucketId::new("b"), ObjectKey::new("k2"), Bytes::from_static(b"bb"));

        cache.clear_bucket(&BucketId::new("b"));

        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k1")).is_none());
        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k2")).is_none());
        assert_eq!(cache.stats().entry_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn update_existing_with_larger_value_adjusts_size() {
        let cache = ObjectCache::new(ObjectCacheConfig {
            max_size_bytes: 1024,
            max_blob_size: 1024,
            ..Default::default()
        });
        cache.put(BucketId::new("b"), ObjectKey::new("k"), Bytes::from_static(b"small"));
        let size_before = cache.stats().size_bytes.load(Ordering::Relaxed);

        cache.put(
            BucketId::new("b"),
            ObjectKey::new("k"),
            Bytes::from_static(b"much larger value here"),
        );
        let size_after = cache.stats().size_bytes.load(Ordering::Relaxed);

        assert!(size_after > size_before);
        assert_eq!(
            cache.get(&BucketId::new("b"), &ObjectKey::new("k")),
            Some(Bytes::from_static(b"much larger value here"))
        );
    }

    #[test]
    fn update_existing_with_smaller_value_adjusts_size() {
        let cache = ObjectCache::new(ObjectCacheConfig {
            max_size_bytes: 1024,
            max_blob_size: 1024,
            ..Default::default()
        });
        cache.put(BucketId::new("b"), ObjectKey::new("k"), Bytes::from_static(b"large value here"));
        let size_before = cache.stats().size_bytes.load(Ordering::Relaxed);

        cache.put(BucketId::new("b"), ObjectKey::new("k"), Bytes::from_static(b"tiny"));
        let size_after = cache.stats().size_bytes.load(Ordering::Relaxed);

        assert!(size_after < size_before);
        assert_eq!(
            cache.get(&BucketId::new("b"), &ObjectKey::new("k")),
            Some(Bytes::from_static(b"tiny"))
        );
    }
}
