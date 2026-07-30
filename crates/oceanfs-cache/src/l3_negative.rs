//! L3 Negative Cache — Bloom filter for non-existent keys.
//!
//! Answers "does this key exist?" without touching RocksDB. HEAD requests
//! for non-existent objects return 404 in constant time.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, Ordering},
};

use oceanfs_core::{BucketId, ObjectKey};
use parking_lot::RwLock;

/// Configuration for the negative cache.
#[derive(Debug, Clone)]
pub struct NegativeCacheConfig {
    /// Whether the cache is enabled.
    pub enabled: bool,
    /// Size of the bloom filter in bytes.
    pub size_bytes: usize,
}

impl Default for NegativeCacheConfig {
    fn default() -> Self {
        Self { enabled: true, size_bytes: 64 * 1024 * 1024 }
    }
}

/// Statistics for the negative cache.
#[derive(Debug, Default)]
pub struct NegativeCacheStats {
    /// Correctly predicted missing keys.
    pub hits: AtomicU64,
    /// False positives (said "maybe" but key was absent).
    pub false_positives: AtomicU64,
    /// Number of rebuilds performed.
    pub rebuilds: AtomicU64,
}

/// A simple Bloom-filter-based negative cache.
pub struct NegativeCache {
    config: NegativeCacheConfig,
    bits: RwLock<Vec<u8>>,
    stats: NegativeCacheStats,
}

impl NegativeCache {
    /// Creates a new negative cache.
    pub fn new(config: NegativeCacheConfig) -> Self {
        let bit_count = config.size_bytes * 8;
        let byte_count = bit_count.div_ceil(8);
        Self {
            config,
            bits: RwLock::new(vec![0u8; byte_count]),
            stats: NegativeCacheStats::default(),
        }
    }

    /// Returns `true` if the key MAY exist (possible false positive).
    /// Returns `false` if the key DEFINITELY does not exist.
    pub fn contains(&self, bucket: &BucketId, key: &ObjectKey) -> bool {
        if !self.config.enabled {
            // When disabled, always say "maybe" to force a real lookup.
            return true;
        }

        let bits = self.bits.read();
        let byte_count = bits.len();
        if byte_count == 0 {
            return true;
        }

        let (h1, h2) = Self::hash_key(bucket, key);
        let bit_count = byte_count * 8;

        let idx1 = (h1 as usize) % bit_count;
        let idx2 = (h2 as usize) % bit_count;

        let byte1 = bits[idx1 / 8];
        let byte2 = bits[idx2 / 8];

        let bit1 = (byte1 >> (idx1 % 8)) & 1;
        let bit2 = (byte2 >> (idx2 % 8)) & 1;

        bit1 == 1 && bit2 == 1
    }

    /// Inserts a key into the filter.
    pub fn insert(&self, bucket: &BucketId, key: &ObjectKey) {
        if !self.config.enabled {
            return;
        }

        let mut bits = self.bits.write();
        let byte_count = bits.len();
        if byte_count == 0 {
            return;
        }

        let (h1, h2) = Self::hash_key(bucket, key);
        let bit_count = byte_count * 8;

        let idx1 = (h1 as usize) % bit_count;
        let idx2 = (h2 as usize) % bit_count;

        bits[idx1 / 8] |= 1 << (idx1 % 8);
        bits[idx2 / 8] |= 1 << (idx2 % 8);
    }

    /// Rebuilds the filter from a set of existing keys.
    pub fn rebuild<I>(&self, keys: I)
    where
        I: IntoIterator<Item = (BucketId, ObjectKey)>,
    {
        let mut bits = self.bits.write();
        bits.fill(0);

        for (bucket, key) in keys {
            let (h1, h2) = Self::hash_key(&bucket, &key);
            let bit_count = bits.len() * 8;
            bits[(h1 as usize % bit_count) / 8] |= 1 << (h1 as usize % bit_count % 8);
            bits[(h2 as usize % bit_count) / 8] |= 1 << (h2 as usize % bit_count % 8);
        }

        self.stats.rebuilds.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> &NegativeCacheStats {
        &self.stats
    }

    fn hash_key(bucket: &BucketId, key: &ObjectKey) -> (u64, u64) {
        let mut h1 = DefaultHasher::new();
        bucket.as_str().hash(&mut h1);
        key.as_str().hash(&mut h1);
        let hash1 = h1.finish();

        let mut h2 = DefaultHasher::new();
        h2.write_u64(hash1);
        let hash2 = h2.finish();

        (hash1, hash2)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_returns_false() {
        let cache = NegativeCache::new(NegativeCacheConfig { enabled: true, size_bytes: 1024 });
        assert!(!cache.contains(&BucketId::new("b"), &ObjectKey::new("k")));
    }

    #[test]
    fn insert_then_contains_returns_true() {
        let cache = NegativeCache::new(NegativeCacheConfig { enabled: true, size_bytes: 1024 });
        cache.insert(&BucketId::new("b"), &ObjectKey::new("k"));
        assert!(cache.contains(&BucketId::new("b"), &ObjectKey::new("k")));
    }

    #[test]
    fn different_key_returns_false() {
        let cache = NegativeCache::new(NegativeCacheConfig { enabled: true, size_bytes: 1024 });
        cache.insert(&BucketId::new("b"), &ObjectKey::new("k1"));
        assert!(!cache.contains(&BucketId::new("b"), &ObjectKey::new("k2")));
    }
}
