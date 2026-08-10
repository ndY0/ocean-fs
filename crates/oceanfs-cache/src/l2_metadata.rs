//! L2 Metadata Cache — cache of ObjectMetadata entries.
//!
//! Avoids RocksDB lookups for hot objects. For inline blobs, a metadata
//! cache hit serves the blob directly from the cached metadata value.
//! Supports gossip-based invalidation. Per-bucket configuration with
//! policy-driven eviction via [`EvictionPolicy`].
//!
//! The eviction policy (e.g. [`TtlLruPolicy`]) replaces the previous
//! O(n) linear scan with O(1) / O(log n) victim selection.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use oceanfs_core::{
    BucketId, CacheInvalidateRequest, Counter, EvictionPolicyType, Gauge, LabelSet,
    MetricRegistrar, ObjectKey, ObjectMetadata,
};

use crate::eviction::{
    AccessMetadata, CacheKey, EvictionPolicy, GdsfConfig, GdsfPolicy, TtlLruConfig, TtlLruPolicy,
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
///
/// Set [`eviction_policy_type`](Self::eviction_policy_type) to override
/// the cache-wide eviction policy for a specific bucket.
#[derive(Debug, Clone)]
pub struct MetadataCacheConfig {
    /// Whether the cache is enabled.
    pub enabled: bool,
    /// Maximum cache size in bytes (approximate). Used for LRU eviction.
    pub max_size_bytes: u64,
    /// TTL in milliseconds (0 = no expiry).
    pub ttl_ms: u64,
    /// Optional per-bucket eviction policy override.
    /// `None` means use the cache-wide default policy.
    pub eviction_policy_type: Option<EvictionPolicyType>,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size_bytes: 1024 * 1024 * 1024,
            ttl_ms: 300_000,
            eviction_policy_type: None,
        }
    }
}

/// A single metadata cache entry.
struct MetadataEntry {
    metadata: Arc<ObjectMetadata>,
    inserted_at: Instant,
}

impl MetadataEntry {
    fn new(metadata: Arc<ObjectMetadata>) -> Self {
        Self { metadata, inserted_at: Instant::now() }
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

/// L2 metadata cache — avoids RocksDB lookups.
///
/// Uses a pluggable [`EvictionPolicy`] (default: [`TtlLruPolicy`](crate::eviction::TtlLruPolicy))
/// for staleness-based victim selection.
///
/// # Examples
///
/// ```
/// use oceanfs_cache::{MetadataCache, MetadataCacheConfig};
/// use oceanfs_cache::eviction::{TtlLruConfig, TtlLruPolicy};
/// use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata, Hlc};
///
/// let policy = Box::new(TtlLruPolicy::new(TtlLruConfig::default()));
/// let cache = MetadataCache::new(MetadataCacheConfig::default(), policy);
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
    /// The cache-wide default eviction policy.
    default_policy: Arc<dyn EvictionPolicy>,
    /// Per-bucket policy overrides.
    per_bucket_policies: DashMap<BucketId, Arc<dyn EvictionPolicy>>,
    stats: MetadataCacheStats,
}

impl MetadataCache {
    /// Creates a new metadata cache with the given configuration and eviction policy.
    pub fn new(config: MetadataCacheConfig, eviction_policy: Box<dyn EvictionPolicy>) -> Self {
        Self {
            default_config: config,
            buckets: DashMap::new(),
            default_policy: Arc::from(eviction_policy),
            per_bucket_policies: DashMap::new(),
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

    /// Returns the eviction policy for the given bucket, falling back
    /// to the cache-wide default if no per-bucket policy is registered.
    fn policy_for(&self, bucket: &BucketId) -> Arc<dyn EvictionPolicy> {
        self.per_bucket_policies
            .get(bucket)
            .map(|p| Arc::clone(p.value()))
            .unwrap_or_else(|| Arc::clone(&self.default_policy))
    }

    /// Constructs an L2 eviction policy from a config type.
    fn make_l2_policy(policy_type: EvictionPolicyType, ttl_ms: u64) -> Arc<dyn EvictionPolicy> {
        match policy_type {
            EvictionPolicyType::TtlLru => {
                Arc::new(TtlLruPolicy::new(TtlLruConfig { default_ttl_ms: ttl_ms }))
            }
            EvictionPolicyType::Gdsf => Arc::new(GdsfPolicy::new(GdsfConfig::default())),
            EvictionPolicyType::Adaptive => {
                tracing::warn!(
                    "Adaptive eviction policy not yet implemented; falling back to TTL-LRU for L2 bucket"
                );
                Arc::new(TtlLruPolicy::new(TtlLruConfig::default()))
            }
            _ => {
                tracing::warn!("Unknown L2 eviction policy; falling back to TTL-LRU");
                Arc::new(TtlLruPolicy::new(TtlLruConfig::default()))
            }
        }
    }

    /// Ensures a per-bucket policy exists if the config specifies one.
    fn ensure_bucket_policy(&self, bucket: &BucketId, config: &MetadataCacheConfig) {
        if let Some(policy_type) = config.eviction_policy_type {
            if !self.per_bucket_policies.contains_key(bucket) {
                let policy = Self::make_l2_policy(policy_type, config.ttl_ms);
                self.per_bucket_policies.insert(bucket.clone(), policy);
            }
        }
    }

    /// Retrieves cached metadata.
    ///
    /// Returns `None` on miss or TTL expiry. On hit, notifies
    /// the eviction policy via [`EvictionPolicy::on_access`].
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
                    drop(entry);
                    bucket_cache.entries.remove(key);
                    let cache_key = CacheKey::new(bucket.clone(), key.clone());
                    self.policy_for(bucket).on_remove(&cache_key);
                    self.stats.misses.inc();
                    self.stats.evictions.inc();
                    self.stats.entry_count.dec();
                    return None;
                }
            }
            if entry.metadata.is_inline() {
                self.stats.inline_hits.inc();
            }
            // Notify policy of access.
            let meta = AccessMetadata::new(bucket.clone(), entry.metadata.size);
            let cache_key = CacheKey::new(bucket.clone(), key.clone());
            self.policy_for(bucket).on_access(&cache_key, &meta);
            self.stats.hits.inc();
            return Some(entry.metadata.clone());
        }

        self.stats.misses.inc();
        None
    }

    /// Inserts metadata into the cache.
    ///
    /// If the bucket's cache exceeds `max_size_bytes`, the eviction
    /// policy selects victims until within limits.
    pub fn put(&self, bucket: BucketId, key: ObjectKey, metadata: ObjectMetadata) {
        let bucket_cache = self
            .buckets
            .entry(bucket.clone())
            .or_insert_with(|| {
                let config = self.default_config.clone();
                self.ensure_bucket_policy(&bucket, &config);
                Arc::new(BucketMetadataCache::new(config))
            })
            .clone();

        if !bucket_cache.config.enabled {
            return;
        }

        // If the key already exists, update in place.
        if let Some(mut existing) = bucket_cache.entries.get_mut(&key) {
            existing.metadata = Arc::new(metadata);
            let meta = AccessMetadata::new(bucket.clone(), existing.metadata.size);
            let cache_key = CacheKey::new(bucket.clone(), key);
            self.policy_for(&bucket).on_access(&cache_key, &meta);
            return;
        }

        let entry_size = std::mem::size_of::<MetadataEntry>()
            + metadata.inline_data.as_ref().map(|d| d.len()).unwrap_or(0);

        // Evict until room using the policy.
        self.evict_for_space(&bucket, &bucket_cache, entry_size);

        let meta = AccessMetadata::new(bucket.clone(), metadata.size);
        let cache_key = CacheKey::new(bucket.clone(), key.clone());
        self.policy_for(&bucket).on_insert(&cache_key, entry_size, &meta);

        let entry = MetadataEntry::new(Arc::new(metadata));
        bucket_cache.entries.insert(key, entry);
        self.stats.entry_count.inc();
    }

    /// Invalidates a cache entry for the given bucket and key.
    ///
    /// Called locally after a PUT or DELETE.
    /// Notifies the eviction policy via [`EvictionPolicy::on_remove`].
    pub fn invalidate(&self, bucket: &BucketId, key: &ObjectKey) {
        if let Some(bucket_cache) = self.buckets.get(bucket) {
            if bucket_cache.entries.remove(key).is_some() {
                let cache_key = CacheKey::new(bucket.clone(), key.clone());
                self.policy_for(bucket).on_remove(&cache_key);
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
        // Register per-bucket policy if the config specifies one.
        self.ensure_bucket_policy(&bucket, &config);
        if let Some(mut entry) = self.buckets.get_mut(&bucket) {
            let old_entries = &entry.entries;
            let new_cache = Arc::new(BucketMetadataCache {
                config,
                entries: DashMap::with_capacity(old_entries.len()),
            });
            for item in old_entries.iter() {
                let (k, v) = item.pair();
                new_cache.entries.insert(k.clone(), MetadataEntry::new(v.metadata.clone()));
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

    /// Evicts entries via the policy until the target bucket has room
    /// for `needed_bytes`, or the policy returns `None`.
    ///
    /// Uses a count-based heuristic: converts `max_size_bytes` to
    /// an approximate max entry count.
    fn evict_for_space(
        &self,
        bucket: &BucketId,
        target_bucket: &BucketMetadataCache,
        _needed_bytes: usize,
    ) {
        // Rough conversion from bytes to entry count.
        let entry_overhead = std::mem::size_of::<MetadataEntry>() + 64;
        let max_entries = (target_bucket.config.max_size_bytes as usize / entry_overhead).max(1);
        let policy = self.policy_for(bucket);

        for _ in 0..100 {
            // Evict one more than allowed, so there's room for the incoming entry.
            if target_bucket.entries.len() < max_entries {
                break;
            }

            let Some(victim) = policy.select_victim() else {
                break;
            };

            // Find the bucket containing the victim.
            if let Some(victim_bucket) = self.buckets.get(victim.bucket()) {
                if victim_bucket.entries.remove(victim.object_key()).is_some() {
                    let victim_policy = self.policy_for(victim.bucket());
                    victim_policy.on_evict(&victim);
                    self.stats.evictions.inc();
                    self.stats.entry_count.dec();
                }
            } else {
                let victim_policy = self.policy_for(victim.bucket());
                victim_policy.on_evict(&victim);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::Hlc;

    use super::*;
    use crate::eviction::TtlLruPolicy;

    fn make_policy() -> Box<dyn EvictionPolicy> {
        Box::new(TtlLruPolicy::new(crate::eviction::TtlLruConfig::default()))
    }

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
        let cache = MetadataCache::new(MetadataCacheConfig::default(), make_policy());
        let meta = make_meta("k", false);
        cache.put(BucketId::new("b"), ObjectKey::new("k"), meta);

        let got = cache.get(&BucketId::new("b"), &ObjectKey::new("k")).unwrap();
        assert_eq!(got.size, 100);
    }

    #[test]
    fn inline_hit_increments_counter() {
        let cache = MetadataCache::new(MetadataCacheConfig::default(), make_policy());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", true));
        cache.get(&BucketId::new("b"), &ObjectKey::new("k"));
        assert_eq!(cache.stats().inline_hits.get(), 1);
    }

    #[test]
    fn invalidate_removes_entry() {
        let cache = MetadataCache::new(MetadataCacheConfig::default(), make_policy());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", false));
        cache.invalidate(&BucketId::new("b"), &ObjectKey::new("k"));
        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }

    #[test]
    fn invalidate_increments_evictions() {
        let cache = MetadataCache::new(MetadataCacheConfig::default(), make_policy());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", false));
        cache.invalidate(&BucketId::new("b"), &ObjectKey::new("k"));
        assert_eq!(cache.stats().evictions.get(), 1);
    }

    #[test]
    fn handle_invalidation_removes_entry() {
        let cache = MetadataCache::new(MetadataCacheConfig::default(), make_policy());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", false));

        let req = CacheInvalidateRequest { bucket: BucketId::new("b"), key: ObjectKey::new("k") };
        cache.handle_invalidation(req);

        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
        assert_eq!(cache.stats().evictions.get(), 1);
    }

    #[test]
    fn handle_invalidation_of_missing_key_is_noop() {
        let cache = MetadataCache::new(MetadataCacheConfig::default(), make_policy());
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
        let cache = MetadataCache::new(config, make_policy());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", false));
        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }

    #[test]
    fn ttl_expiry_returns_none_and_increments_evictions() {
        let config = MetadataCacheConfig { ttl_ms: 10, ..Default::default() };
        let cache = MetadataCache::new(config, make_policy());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", false));

        std::thread::sleep(Duration::from_millis(20));

        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
        assert_eq!(cache.stats().evictions.get(), 1);
    }

    #[test]
    fn per_bucket_isolation() {
        let cache = MetadataCache::new(MetadataCacheConfig::default(), make_policy());
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
            max_size_bytes: 1,
            ttl_ms: 0,
            enabled: true,
            ..Default::default()
        };
        // Use TTL-LRU with ttl_ms=0 so all entries are immediately stale
        // and can be selected as victims.
        let policy =
            Box::new(TtlLruPolicy::new(crate::eviction::TtlLruConfig { default_ttl_ms: 0 }));
        let cache = MetadataCache::new(config, policy);

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
        let cache = MetadataCache::new(config, make_policy());
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
