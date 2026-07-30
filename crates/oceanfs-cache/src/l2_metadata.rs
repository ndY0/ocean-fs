//! L2 Metadata Cache — LRU of ObjectMetadata entries.
//!
//! Avoids RocksDB lookups for hot objects. For inline blobs, a metadata
//! cache hit serves the blob directly from the cached metadata value.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use oceanfs_core::{BucketId, ObjectKey, ObjectMetadata};

/// Statistics for the L2 metadata cache.
#[derive(Debug, Default)]
pub struct MetadataCacheStats {
    /// Metadata cache hits.
    pub hits: AtomicU64,
    /// Hits that served an inline blob directly.
    pub inline_hits: AtomicU64,
    /// Cache misses.
    pub misses: AtomicU64,
}

/// Configuration for the L2 metadata cache.
#[derive(Debug, Clone)]
pub struct MetadataCacheConfig {
    /// Whether the cache is enabled.
    pub enabled: bool,
    /// Maximum cache size in bytes (approximate).
    pub max_size_bytes: u64,
    /// TTL in milliseconds (0 = no expiry).
    pub ttl_ms: u64,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self { enabled: true, max_size_bytes: 1024 * 1024 * 1024, ttl_ms: 300_000 }
    }
}

struct MetadataEntry {
    metadata: Arc<ObjectMetadata>,
    inserted_at: Instant,
}

/// L2 metadata cache — avoids RocksDB lookups.
pub struct MetadataCache {
    config: MetadataCacheConfig,
    entries: DashMap<(BucketId, ObjectKey), MetadataEntry>,
    stats: MetadataCacheStats,
}

impl MetadataCache {
    /// Creates a new metadata cache.
    pub fn new(config: MetadataCacheConfig) -> Self {
        Self { config, entries: DashMap::new(), stats: MetadataCacheStats::default() }
    }

    /// Retrieves cached metadata.
    ///
    /// Returns `None` on miss or TTL expiry.
    pub fn get(&self, bucket: &BucketId, key: &ObjectKey) -> Option<Arc<ObjectMetadata>> {
        if !self.config.enabled {
            return None;
        }

        let lookup = (bucket.clone(), key.clone());
        if let Some(entry) = self.entries.get(&lookup) {
            if self.config.ttl_ms > 0 {
                let age = entry.inserted_at.elapsed();
                if age > Duration::from_millis(self.config.ttl_ms) {
                    drop(entry);
                    self.entries.remove(&lookup);
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
            if entry.metadata.is_inline() {
                self.stats.inline_hits.fetch_add(1, Ordering::Relaxed);
            }
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.metadata.clone());
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Inserts metadata into the cache.
    pub fn put(&self, bucket: BucketId, key: ObjectKey, metadata: ObjectMetadata) {
        if !self.config.enabled {
            return;
        }
        self.entries.insert(
            (bucket, key),
            MetadataEntry { metadata: Arc::new(metadata), inserted_at: Instant::now() },
        );
    }

    /// Invalidates a cache entry.
    pub fn invalidate(&self, bucket: &BucketId, key: &ObjectKey) {
        self.entries.remove(&(bucket.clone(), key.clone()));
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> &MetadataCacheStats {
        &self.stats
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
        assert_eq!(cache.stats().inline_hits.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn invalidate_removes_entry() {
        let cache = MetadataCache::new(MetadataCacheConfig::default());
        cache.put(BucketId::new("b"), ObjectKey::new("k"), make_meta("k", false));
        cache.invalidate(&BucketId::new("b"), &ObjectKey::new("k"));
        assert!(cache.get(&BucketId::new("b"), &ObjectKey::new("k")).is_none());
    }
}
