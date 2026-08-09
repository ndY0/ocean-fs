//! L2 Metadata Cache — LRU of ObjectMetadata entries.
//!
//! Avoids RocksDB lookups for hot objects. For inline blobs, a metadata
//! cache hit serves the blob directly from the cached metadata value.
//! Supports gossip-based invalidation. Per-bucket configuration with
//! LRU eviction when `max_size_bytes` is exceeded.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use oceanfs_core::{
    BucketId, CacheInvalidateRequest, Counter, Gauge, LabelSet, MetricRegistrar, ObjectKey,
    ObjectMetadata,
};

/// Statistics for the L2 metadata cache.
///
/// All counters use relaxed atomics for minimal overhead.
#[derive(Debug)]
pub struct MetadataCacheStats {
    /// Metadata cache hits (any kind).
    pub hits: Counter,
    /// Hits that served an inline blob directly (zero I/O).
    pub inline_hits: Counter,
    /// Cache misses.
    pub misses: Counter,
    /// Number of evicted entries (TTL, LRU, or gossip invalidation).
    pub evictions: Counter,
    /// Current number of entries (approximate).
    pub entry_count: Gauge,
}

/// Configuration for the L2 metadata cache.
#[derive(Debug, Clone)]
pub struct MetadataCacheConfig {
    /// Whether the cache is enabled.
    pub enabled: bool,
    /// Maximum cache size in bytes (approximate). Used for LRU eviction.
    pub max_size_bytes: u64,
    /// TTL in milliseconds (0 = no expiry).
    pub ttl_ms: u64,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self { enabled: true, max_size_bytes: 1024 * 1024 * 1024, ttl_ms: 300_000 }
    }
}

/// A single metadata cache entry.
struct MetadataEntry {
    metadata: Arc<ObjectMetadata>,
    inserted_at: Instant,
    /// Approximate last-access generation for LRU ordering.
    last_access: AtomicU64,
}

impl MetadataEntry {
    fn new(metadata: Arc<ObjectMetadata>) -> Self {
        Self { metadata, inserted_at: Instant::now(), last_access: AtomicU64::new(0) }
    }

    fn touch(&self, generation: u64) {
        self.last_access.store(generation, Ordering::Relaxed);
    }

    fn approximate_size(&self) -> usize {
        // Approximate memory: metadata struct + inline data if present.
        std::mem::size_of::<ObjectMetadata>()
            + self.metadata.inline_data.as_ref().map(|d| d.len()).unwrap_or(0)
    }
}

/// Per-bucket metadata cache.
struct BucketMetadataCache {
    config: MetadataCacheConfig,
    entries: DashMap<ObjectKey, MetadataEntry>,
}

impl BucketMetadataCache {
    fn new(config: MetadataCacheConfig) -> Self {
        Self { config, entries: DashMap::new() }
    }
}

/// Global access-generation counter for LRU ordering.
struct LruClock {
    generation: AtomicU64,
}

impl LruClock {
    fn new() -> Self {
        Self { generation: AtomicU64::new(1) }
    }

    fn next(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::Relaxed)
    }
}

/// L2 metadata cache — avoids RocksDB lookups.
///
/// # Examples
///
/// ```
/// use oceanfs_cache::MetadataCache;
/// use oceanfs_cache::MetadataCacheConfig;
/// use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata, Hlc};
///
/// let cache = MetadataCache::new(MetadataCacheConfig::default());
/// let meta = ObjectMetadata {
///     object_key: ObjectKey::new("data.txt"),
///     size: 256,
///     blake3_hash: None,
///     chunks: smallvec::SmallVec::new(),
///     inline_data: Some(bytes::Bytes::from_static(b"inline content")),
///     created_at: 0,
///     hlc: Hlc::zero(),
/// };
/// cache.put(BucketId::new("my-bucket"), ObjectKey::new("data.txt"), meta);
/// let hit = cache.get(&BucketId::new("my-bucket"), &ObjectKey::new("data.txt"));
/// assert!(hit.is_some());
/// ```
pub struct MetadataCache {
    default_config: MetadataCacheConfig,
    buckets: DashMap<BucketId, Arc<BucketMetadataCache>>,
    lru_clock: LruClock,
    stats: MetadataCacheStats,
}

impl MetadataCache {
    /// Creates a new metadata cache with the given configuration.
    pub fn new(config: MetadataCacheConfig) -> Self {
        Self {
            default_config: config,
            buckets: DashMap::new(),
            lru_clock: LruClock::new(),
            stats: MetadataCacheStats {
                hits: Counter::new(
                    "cache_hits_total".into(),
                    "L2 cache hits".into(),
                    LabelSet::new(&[("tier", "l2")]),
                ),
                inline_hits: Counter::new(
                    "cache_inline_hits_total".into(),
                    "L2 cache inline hits".into(),
                    LabelSet::empty(),
                ),
                misses: Counter::new(
                    "cache_misses_total".into(),
                    "L2 cache misses".into(),
                    LabelSet::new(&[("tier", "l2")]),
                ),
                evictions: Counter::new(
                    "cache_evictions_total".into(),
                    "L2 cache evictions".into(),
                    LabelSet::new(&[("tier", "l2")]),
                ),
                entry_count: Gauge::new(
                    "cache_entry_count".into(),
                    "L2 cache entry count".into(),
                    LabelSet::empty(),
                ),
            },
        }
    }

    /// Retrieves cached metadata.
    ///
    /// Returns `None` on miss or TTL expiry.
    pub fn get(&self, bucket: &BucketId, key: &ObjectKey) -> Option<Arc<ObjectMetadata>> {
        let Some(bucket_cache) = self.buckets.get(bucket) else {
            self.stats.misses.inc();
            return None;
        };

        if !bucket_cache.config.enabled {
            self.stats.misses.inc();
            return None;
        }

        if let Some(entry) = bucket_cache.entries.get(key) {
            if bucket_cache.config.ttl_ms > 0 {
                let age = entry.inserted_at.elapsed();
                if age > Duration::from_millis(bucket_cache.config.ttl_ms) {
                    let _entry_size = entry.approximate_size();
                    drop(entry);
                    bucket_cache.entries.remove(key);
                    self.stats.misses.inc();
                    self.stats.evictions.inc();
                    self.stats.entry_count.dec();
                    // Note: we don't track size_bytes for simplicity.
                    return None;
                }
            }
            if entry.metadata.is_inline() {
                self.stats.inline_hits.inc();
            }
            let gen = self.lru_clock.next();
            entry.touch(gen);
            self.stats.hits.inc();
            return Some(entry.metadata.clone());
        }

        self.stats.misses.inc();
        None
    }

    /// Inserts metadata into the cache.
    ///
    /// If the bucket's cache exceeds `max_size_bytes`, LRU entries are evicted.
    pub fn put(&self, bucket: BucketId, key: ObjectKey, metadata: ObjectMetadata) {
        let bucket_cache = self
            .buckets
            .entry(bucket.clone())
            .or_insert_with(|| Arc::new(BucketMetadataCache::new(self.default_config.clone())))
            .clone();

        if !bucket_cache.config.enabled {
            return;
        }

        // If the key already exists, update in place.
        if let Some(mut existing) = bucket_cache.entries.get_mut(&key) {
            existing.metadata = Arc::new(metadata);
            let gen = self.lru_clock.next();
            existing.touch(gen);
            return;
        }

        // Check capacity and evict if needed.
        self.evict_if_needed(&bucket_cache);

        let gen = self.lru_clock.next();
        let entry = MetadataEntry::new(Arc::new(metadata));
        entry.touch(gen);

        bucket_cache.entries.insert(key, entry);
        self.stats.entry_count.inc();
    }

    /// Invalidates a cache entry for the given bucket and key.
    ///
    /// Called locally after a PUT or DELETE.
    pub fn invalidate(&self, bucket: &BucketId, key: &ObjectKey) {
        if let Some(bucket_cache) = self.buckets.get(bucket) {
            if bucket_cache.entries.remove(key).is_some() {
                self.stats.evictions.inc();
                self.stats.entry_count.dec();
            }
        }
    }

    /// Handles a gossip-based invalidation request from a remote node.
    ///
    /// Called when a peer signals that a cache entry is stale. This is
    /// a best-effort operation — if the entry is not in the local cache,
    /// the request is silently ignored.
    pub fn handle_invalidation(&self, req: CacheInvalidateRequest) {
        self.invalidate(&req.bucket, &req.key);
    }

    /// Sets a bucket-specific configuration.
    ///
    /// If the bucket already exists, its config is updated and entries
    /// are migrated to a new cache with the new settings.
    pub fn set_bucket_config(&self, bucket: BucketId, config: MetadataCacheConfig) {
        if let Some(mut entry) = self.buckets.get_mut(&bucket) {
            let old_entries = &entry.entries;
            let new_cache = Arc::new(BucketMetadataCache {
                config,
                entries: DashMap::with_capacity(old_entries.len()),
            });
            for item in old_entries.iter() {
                let (k, v) = item.pair();
                let new_entry = MetadataEntry::new(v.metadata.clone());
                new_entry.touch(v.last_access.load(Ordering::Relaxed));
                new_cache.entries.insert(k.clone(), new_entry);
            }
            *entry = new_cache;
        } else {
            self.buckets.insert(bucket, Arc::new(BucketMetadataCache::new(config)));
        }
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> &MetadataCacheStats {
        &self.stats
    }

    /// Registers the cache's counters and gauges with a metrics registry.
    pub fn register_metrics(&self, reg: &dyn MetricRegistrar) {
        reg.register_counter(self.stats.hits.clone());
        reg.register_counter(self.stats.inline_hits.clone());
        reg.register_counter(self.stats.misses.clone());
        reg.register_counter(self.stats.evictions.clone());
        reg.register_gauge(self.stats.entry_count.clone());
    }

    /// Evicts LRU entries until the bucket cache is below its size limit.
    fn evict_if_needed(&self, bucket_cache: &BucketMetadataCache) {
        let max_entries = bucket_cache.config.max_size_bytes as usize
            / (std::mem::size_of::<MetadataEntry>() + 64); // rough estimate

        let mut to_remove: Option<ObjectKey> = None;

        while bucket_cache.entries.len() > max_entries && !bucket_cache.entries.is_empty() {
            let mut min_gen = u64::MAX;
            for entry in bucket_cache.entries.iter() {
                let gen = entry.last_access.load(Ordering::Relaxed);
                if gen < min_gen {
                    min_gen = gen;
                    to_remove = Some(entry.key().clone());
                }
            }

            if let Some(key) = to_remove.take() {
                bucket_cache.entries.remove(&key);
                self.stats.evictions.inc();
                self.stats.entry_count.dec();
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::Hlc;

    use super::*;

    fn make_meta(key: &str, inline: bool) -> ObjectMetadata {
        ObjectMetadata {
            object_key: ObjectKey::new(key),
            size: 100,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: if inline { Some(bytes::Bytes::from_static(b"data")) } else { None },
            created_at: 0,
            hlc: Hlc::zero(),
        }
    }

    #[test]
    fn put_then_get_returns_metadata() {
        let cache = MetadataCache::new(MetadataCacheConfig::default());
        let meta = make_meta("k", false);
        cache.put(BucketId::new("b"), ObjectKey::new("k"), meta);

        let got = cache.get(&BucketId::new("b"), &ObjectKey::new("k")).unwrap();
        assert_eq!(got.size, 100);
    }

    #[test]
    fn inline_hit_increments_counter() {
        let cache = MetadataCache::new(MetadataCacheConfig::default());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", true));
        cache.get(&BucketId::new("b"), &ObjectKey::new("k"));
        assert_eq!(cache.stats().inline_hits.get(), 1);
    }

    #[test]
    fn invalidate_removes_entry() {
        let cache = MetadataCache::new(MetadataCacheConfig::default());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", false));
        cache.invalidate(&BucketId::new("b"), &ObjectKey::new("k"));
        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }

    #[test]
    fn invalidate_increments_evictions() {
        let cache = MetadataCache::new(MetadataCacheConfig::default());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", false));
        cache.invalidate(&BucketId::new("b"), &ObjectKey::new("k"));
        assert_eq!(cache.stats().evictions.get(), 1);
    }

    #[test]
    fn handle_invalidation_removes_entry() {
        let cache = MetadataCache::new(MetadataCacheConfig::default());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", false));

        let req = CacheInvalidateRequest { bucket: BucketId::new("b"), key: ObjectKey::new("k") };
        cache.handle_invalidation(req);

        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
        assert_eq!(cache.stats().evictions.get(), 1);
    }

    #[test]
    fn handle_invalidation_of_missing_key_is_noop() {
        let cache = MetadataCache::new(MetadataCacheConfig::default());
        let req = CacheInvalidateRequest {
            bucket: BucketId::new("b"),
            key: ObjectKey::new("nonexistent"),
        };
        cache.handle_invalidation(req);
        assert_eq!(cache.stats().evictions.get(), 0);
    }

    #[test]
    fn disabled_cache_always_returns_none() {
        let config = MetadataCacheConfig { enabled: false, ..Default::default() };
        let cache = MetadataCache::new(config);
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", false));
        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }

    #[test]
    fn ttl_expiry_returns_none_and_increments_evictions() {
        let config = MetadataCacheConfig { ttl_ms: 10, ..Default::default() };
        let cache = MetadataCache::new(config);
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", false));

        std::thread::sleep(Duration::from_millis(20));

        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
        assert_eq!(cache.stats().evictions.get(), 1);
    }

    #[test]
    fn per_bucket_isolation() {
        let cache = MetadataCache::new(MetadataCacheConfig::default());
        cache.put(BucketId::new("b1"), ObjectKey::new("k"), make_meta("k-b1", false));
        cache.put(BucketId::new("b2"), ObjectKey::new("k"), make_meta("k-b2", false));

        let got1 = cache.get(&BucketId::new("b1"), &ObjectKey::new("k")).unwrap();
        let got2 = cache.get(&BucketId::new("b2"), &ObjectKey::new("k")).unwrap();
        assert_eq!(got1.object_key.as_str(), "k-b1");
        assert_eq!(got2.object_key.as_str(), "k-b2");
    }

    #[test]
    fn lru_eviction_when_cache_full() {
        let config = MetadataCacheConfig {
            max_size_bytes: 1, // Very small — effectively 0 entries.
            ttl_ms: 0,
            enabled: true,
        };
        let cache = MetadataCache::new(config);

        cache.put(BucketId::new("b"), ObjectKey::new("k1"), make_meta("k1", false));
        cache.put(BucketId::new("b"), ObjectKey::new("k2"), make_meta("k2", false));

        // At least one entry should have been evicted (max_size_bytes=1).
        let stats = cache.stats();
        assert!(stats.evictions.get() > 0, "expected some evictions when max_size_bytes=1");
    }

    /// T3.4: Metadata cache with `enabled = false` bypasses all operations.
    #[test]
    fn test_metadata_cache_disabled_bypassed() {
        let config = MetadataCacheConfig { enabled: false, ..Default::default() };
        let cache = MetadataCache::new(config);
        let bucket = BucketId::new("b");
        let key = ObjectKey::new("k");
        let meta = ObjectMetadata {
            object_key: key.clone(),
            size: 100,
            blake3_hash: None,
            chunks: smallvec::SmallVec::new(),
            inline_data: None,
            created_at: 0,
            hlc: oceanfs_core::Hlc::zero(),
        };
        cache.put(bucket.clone(), key.clone(), meta);
        // Disabled cache should always return None.
        assert!(cache.get(&bucket, &key).is_none());
    }
}
