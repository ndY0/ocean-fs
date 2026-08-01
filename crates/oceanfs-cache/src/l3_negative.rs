//! L3 Negative Cache — Bloom filter for non-existent keys.
//!
//! Answers "does this key exist?" without touching RocksDB. HEAD requests
//! for non-existent objects return 404 in constant time. Per-bucket Bloom
//! filters with configurable false-positive rates.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use oceanfs_core::{BucketId, MetadataStore, ObjectKey};
use parking_lot::RwLock;

/// Configuration for the negative cache.
#[derive(Debug, Clone)]
pub struct NegativeCacheConfig {
    /// Whether the cache is enabled.
    pub enabled: bool,
    /// Approximate size of each bucket's Bloom filter in bytes.
    pub size_bytes: u64,
    /// Target false-positive rate (e.g., 0.0001 = 0.01%).
    pub fp_rate: f64,
    /// Interval between automatic rebuilds in seconds (0 = no auto-rebuild).
    pub rebuild_interval_sec: u64,
}

impl Default for NegativeCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            size_bytes: 64 * 1024 * 1024,
            fp_rate: 0.0001,
            rebuild_interval_sec: 3600,
        }
    }
}

/// Statistics for the negative cache.
#[derive(Debug, Default)]
pub struct NegativeCacheStats {
    /// Correctly predicted missing keys (filter said "definitely absent").
    pub hits: AtomicU64,
    /// False positives (filter said "maybe present" but key was absent).
    pub false_positives: AtomicU64,
    /// Number of rebuilds performed.
    pub rebuilds: AtomicU64,
    /// Current number of entries across all bucket filters.
    pub entry_count: AtomicUsize,
}

/// A Bloom filter with configurable false-positive rate.
///
/// Uses double hashing (Kirsch-Mitzenmacher scheme) to derive multiple
/// hash functions from two base hashes.
struct BloomFilter {
    /// Bit array stored as bytes.
    bits: Vec<u8>,
    /// Number of hash functions to use.
    hash_count: usize,
    /// Number of bits in the filter.
    bit_count: usize,
}

impl BloomFilter {
    /// Creates a new Bloom filter with the given size and target false-positive rate.
    ///
    /// The number of hash functions is chosen to minimize the false-positive rate
    /// for the given filter size.
    fn new(size_bytes: u64, fp_rate: f64) -> Self {
        let bit_count = (size_bytes as usize) * 8;
        let byte_count = bit_count / 8;
        // Optimal number of hash functions: k = -log2(fp_rate)
        let hash_count = Self::optimal_hash_count(fp_rate);
        Self { bits: vec![0u8; byte_count], hash_count, bit_count }
    }

    /// Computes the optimal number of hash functions for a target FP rate.
    fn optimal_hash_count(fp_rate: f64) -> usize {
        if fp_rate <= 0.0 {
            return 1;
        }
        // k = -log2(p) = -ln(p) / ln(2)
        let k = (-fp_rate.ln() / std::f64::consts::LN_2).ceil() as usize;
        k.clamp(1, 16)
    }

    /// Returns `true` if the key MAY be present (possible false positive).
    fn contains(&self, bucket: &BucketId, key: &ObjectKey) -> bool {
        let (h1, h2) = hash_key(bucket, key);
        for i in 0..self.hash_count {
            let idx = hash_index(h1, h2, i, self.bit_count);
            let byte = self.bits[idx / 8];
            if (byte >> (idx % 8)) & 1 == 0 {
                return false;
            }
        }
        true
    }

    /// Inserts a key into the filter.
    fn insert(&mut self, bucket: &BucketId, key: &ObjectKey) {
        let (h1, h2) = hash_key(bucket, key);
        for i in 0..self.hash_count {
            let idx = hash_index(h1, h2, i, self.bit_count);
            self.bits[idx / 8] |= 1 << (idx % 8);
        }
    }
}

/// Per-bucket negative cache with its own Bloom filter.
struct BucketNegativeCache {
    filter: RwLock<BloomFilter>,
}

impl BucketNegativeCache {
    fn new(config: &NegativeCacheConfig) -> Self {
        Self { filter: RwLock::new(BloomFilter::new(config.size_bytes, config.fp_rate)) }
    }
}

/// L3 Negative Cache — per-bucket Bloom filters for non-existent keys.
///
/// # Examples
///
/// ```
/// use oceanfs_cache::NegativeCache;
/// use oceanfs_cache::NegativeCacheConfig;
/// use oceanfs_core::{BucketId, ObjectKey};
///
/// let cache = NegativeCache::new(NegativeCacheConfig::default());
/// cache.insert(&BucketId::new("b"), &ObjectKey::new("k"));
/// assert!(cache.contains(&BucketId::new("b"), &ObjectKey::new("k")));
/// ```
pub struct NegativeCache {
    config: NegativeCacheConfig,
    buckets: DashMap<BucketId, Arc<BucketNegativeCache>>,
    stats: NegativeCacheStats,
}

impl NegativeCache {
    /// Creates a new negative cache with the given configuration.
    pub fn new(config: NegativeCacheConfig) -> Self {
        Self { config, buckets: DashMap::new(), stats: NegativeCacheStats::default() }
    }

    /// Returns `true` if the key MAY exist (possible false positive).
    /// Returns `false` if the key DEFINITELY does not exist.
    pub fn contains(&self, bucket: &BucketId, key: &ObjectKey) -> bool {
        if !self.config.enabled {
            // When disabled, always say "maybe" to force a real lookup.
            return true;
        }

        // Get or create the bucket filter (empty filter = all keys definitely absent).
        let bucket_cache = self
            .buckets
            .entry(bucket.clone())
            .or_insert_with(|| Arc::new(BucketNegativeCache::new(&self.config)))
            .clone();

        let result = bucket_cache.filter.read().contains(bucket, key);
        if !result {
            // Definitely absent — record as a hit for the negative cache.
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Inserts a key into the filter.
    pub fn insert(&self, bucket: &BucketId, key: &ObjectKey) {
        if !self.config.enabled {
            return;
        }

        let bucket_cache = self
            .buckets
            .entry(bucket.clone())
            .or_insert_with(|| Arc::new(BucketNegativeCache::new(&self.config)))
            .clone();

        bucket_cache.filter.write().insert(bucket, key);
        self.stats.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Rebuilds the bucket's Bloom filter from the metadata store.
    ///
    /// Scans all keys in the bucket and populates a fresh filter, then
    /// atomically swaps it with the old filter. Readers see a consistent
    /// view during the rebuild.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store is unavailable.
    pub async fn rebuild(&self, metadata: Arc<dyn MetadataStore>) -> crate::Result<()> {
        // Rebuild all known buckets.
        for bucket_entry in self.buckets.iter() {
            let bucket = bucket_entry.key().clone();
            let bucket_cache = bucket_entry.value().clone();

            let keys = metadata.list_object_keys(&bucket).map_err(crate::Error::RebuildIo)?;

            let mut new_filter = BloomFilter::new(self.config.size_bytes, self.config.fp_rate);

            for (_b, k) in &keys {
                new_filter.insert(&bucket, k);
            }

            {
                let mut old = bucket_cache.filter.write();
                *old = new_filter;
            }
        }

        self.stats.rebuilds.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Records a false positive: the filter said "maybe" but the key was absent.
    pub fn record_false_positive(&self) {
        self.stats.false_positives.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> &NegativeCacheStats {
        &self.stats
    }
}

/// Compute two independent hash values for a bucket+key pair.
fn hash_key(bucket: &BucketId, key: &ObjectKey) -> (u64, u64) {
    let mut h1 = DefaultHasher::new();
    bucket.as_str().hash(&mut h1);
    key.as_str().hash(&mut h1);
    let hash1 = h1.finish();

    let mut h2 = DefaultHasher::new();
    h2.write_u64(hash1);
    h2.write_u8(0xAB); // Mix in a constant to decorrelate.
    let hash2 = h2.finish();

    (hash1, hash2)
}

/// Derives the i-th hash index from two base hashes (Kirsch-Mitzenmacher).
fn hash_index(h1: u64, h2: u64, i: usize, bit_count: usize) -> usize {
    let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
    (combined as usize) % bit_count
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_returns_false() {
        let cache = NegativeCache::new(NegativeCacheConfig {
            enabled: true,
            size_bytes: 1024,
            fp_rate: 0.01,
            rebuild_interval_sec: 3600,
        });
        assert!(!cache.contains(&BucketId::new("b"), &ObjectKey::new("k")));
    }

    #[test]
    fn insert_then_contains_returns_true() {
        let cache = NegativeCache::new(NegativeCacheConfig {
            enabled: true,
            size_bytes: 1024,
            fp_rate: 0.01,
            rebuild_interval_sec: 3600,
        });
        cache.insert(&BucketId::new("b"), &ObjectKey::new("k"));
        assert!(cache.contains(&BucketId::new("b"), &ObjectKey::new("k")));
    }

    #[test]
    fn different_key_returns_false() {
        let cache = NegativeCache::new(NegativeCacheConfig {
            enabled: true,
            size_bytes: 1024,
            fp_rate: 0.01,
            rebuild_interval_sec: 3600,
        });
        cache.insert(&BucketId::new("b"), &ObjectKey::new("k1"));
        assert!(!cache.contains(&BucketId::new("b"), &ObjectKey::new("k2")));
    }

    #[test]
    fn per_bucket_isolation() {
        let cache = NegativeCache::new(NegativeCacheConfig {
            enabled: true,
            size_bytes: 1024,
            fp_rate: 0.01,
            rebuild_interval_sec: 3600,
        });
        cache.insert(&BucketId::new("b1"), &ObjectKey::new("k"));
        assert!(cache.contains(&BucketId::new("b1"), &ObjectKey::new("k")));
        assert!(!cache.contains(&BucketId::new("b2"), &ObjectKey::new("k")));
    }

    #[test]
    fn stats_hits_incremented_on_definite_absent() {
        let cache = NegativeCache::new(NegativeCacheConfig {
            enabled: true,
            size_bytes: 1024,
            fp_rate: 0.01,
            rebuild_interval_sec: 3600,
        });
        cache.contains(&BucketId::new("b"), &ObjectKey::new("nope"));
        assert_eq!(cache.stats().hits.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn record_false_positive_increments_counter() {
        let cache = NegativeCache::new(NegativeCacheConfig::default());
        cache.record_false_positive();
        cache.record_false_positive();
        assert_eq!(cache.stats().false_positives.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn disabled_cache_always_returns_true() {
        let cache =
            NegativeCache::new(NegativeCacheConfig { enabled: false, ..Default::default() });
        // When disabled, always say "maybe" to force a real lookup.
        assert!(cache.contains(&BucketId::new("b"), &ObjectKey::new("nonexistent")));
    }

    #[test]
    fn large_filter_low_false_positive_rate() {
        // With a large filter and many inserted keys, the false-positive
        // rate should be reasonably low, though not zero.
        let cache = NegativeCache::new(NegativeCacheConfig {
            enabled: true,
            size_bytes: 1024 * 1024, // 1 MB
            fp_rate: 0.01,           // 1% target
            rebuild_interval_sec: 3600,
        });

        // Insert 1000 keys.
        for i in 0..1000 {
            cache.insert(&BucketId::new("b"), &ObjectKey::new(format!("key-{}", i)));
        }

        // Test 1000 non-inserted keys — false-positive rate should be ≤ ~5%
        // (1% target, but with this few keys it may be slightly higher).
        let mut fps = 0u64;
        let test_count = 1000u64;
        for i in 0..test_count {
            let missing_key = ObjectKey::new(format!("missing-{}", i));
            if cache.contains(&BucketId::new("b"), &missing_key) {
                fps += 1;
            }
        }

        let fp_rate = fps as f64 / test_count as f64;
        // With 1MB filter and 1000 keys, FP rate should be very low.
        assert!(fp_rate < 0.10, "false-positive rate {:.4} exceeds 10% threshold", fp_rate);
    }
}
