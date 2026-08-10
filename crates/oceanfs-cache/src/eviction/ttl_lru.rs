//! LRU with TTL eviction policy for the L2 metadata cache.
//!
//! Designed for uniformly small entries (~200 bytes) where size-awareness
//! provides no benefit. TTL provides a hard coherence deadline — stale
//! metadata entries must be evicted regardless of access pattern.
//!
//! See ADR-0016 for the full design rationale.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use dashmap::DashMap;

use super::{AccessMetadata, CacheKey, EvictionPolicy};

/// Configuration for the TTL-LRU eviction policy.
///
/// # Examples
///
/// ```
/// use oceanfs_cache::eviction::TtlLruConfig;
///
/// let config = TtlLruConfig::default();
/// assert_eq!(config.default_ttl_ms, 300_000);
/// ```
#[derive(Debug, Clone)]
pub struct TtlLruConfig {
    /// Default TTL in milliseconds for cache entries.
    /// Default: 300_000 (5 minutes).
    pub default_ttl_ms: u64,
}

impl Default for TtlLruConfig {
    fn default() -> Self {
        Self { default_ttl_ms: 300_000 }
    }
}

/// Per-entry state tracked by the TTL-LRU policy.
#[derive(Debug)]
struct TtlLruEntry {
    /// Wall-clock time when the entry was inserted.
    inserted_at: Instant,
    /// Logical timestamp of last access (higher = more recent).
    last_accessed_at: AtomicU64,
}

/// LRU with TTL eviction policy.
///
/// Designed for the L2 metadata cache. Evicts stale entries (past TTL)
/// preferentially; among stale entries, evicts the least recently used.
/// If no entry exceeds TTL, returns `None` (refuses to evict).
///
/// Uses a logical clock (`AtomicU64`) for lightweight access ordering
/// without wall-clock overhead on every access.
///
/// # Examples
///
/// ```
/// use oceanfs_cache::eviction::{TtlLruConfig, TtlLruPolicy, EvictionPolicy};
///
/// let policy = TtlLruPolicy::new(TtlLruConfig::default());
/// ```
pub struct TtlLruPolicy {
    /// Maps cache keys to their LRU metadata.
    entries: DashMap<CacheKey, TtlLruEntry>,
    /// Logical timestamp counter for access ordering.
    logical_clock: AtomicU64,
    /// Default TTL in milliseconds.
    default_ttl_ms: u64,
}

impl TtlLruPolicy {
    /// Creates a new TTL-LRU policy with the given configuration.
    pub fn new(config: TtlLruConfig) -> Self {
        Self {
            entries: DashMap::new(),
            logical_clock: AtomicU64::new(1),
            default_ttl_ms: config.default_ttl_ms,
        }
    }
}

impl EvictionPolicy for TtlLruPolicy {
    fn on_access(&self, key: &CacheKey, _meta: &AccessMetadata) {
        if let Some(entry) = self.entries.get(key) {
            let ts = self.logical_clock.fetch_add(1, Ordering::Relaxed);
            entry.last_accessed_at.store(ts, Ordering::Relaxed);
        }
    }

    fn on_insert(&self, key: &CacheKey, _size: usize, _meta: &AccessMetadata) {
        let ts = self.logical_clock.fetch_add(1, Ordering::Relaxed);
        let entry =
            TtlLruEntry { inserted_at: Instant::now(), last_accessed_at: AtomicU64::new(ts) };
        self.entries.insert(key.clone(), entry);
    }

    fn select_victim(&self) -> Option<CacheKey> {
        let ttl_ms = self.default_ttl_ms;

        // Two-pass scan (O(n)):
        // 1. Prefer stale entries (past TTL) with oldest last_access
        // 2. Only if no stale entries exist, return None (refuse to evict)

        let mut best_key: Option<CacheKey> = None;
        let mut best_stale_ts: u64 = u64::MAX;
        let mut any_stale = false;

        for entry in self.entries.iter() {
            let age_ms = entry.inserted_at.elapsed().as_millis() as u64;
            let is_stale = age_ms >= ttl_ms;

            if is_stale {
                any_stale = true;
                let last_access = entry.last_accessed_at.load(Ordering::Relaxed);
                if last_access < best_stale_ts {
                    best_stale_ts = last_access;
                    best_key = Some(entry.key().clone());
                }
            }
        }

        if any_stale {
            best_key
        } else {
            None
        }
    }

    fn on_evict(&self, key: &CacheKey) {
        self.entries.remove(key);
    }

    fn on_remove(&self, key: &CacheKey) {
        self.entries.remove(key);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use oceanfs_core::{BucketId, ObjectKey};

    use super::*;

    fn make_key(bucket: &str, obj: &str) -> CacheKey {
        CacheKey::new(BucketId::new(bucket), ObjectKey::new(obj))
    }

    fn make_meta(size: u64) -> AccessMetadata {
        AccessMetadata::new(BucketId::new("test"), size)
    }

    /// T3.5: select_victim returns the stale entry with the oldest last_access.
    #[test]
    fn test_ttl_lru_select_victim_returns_stale_first() {
        let config = TtlLruConfig { default_ttl_ms: 50 };
        let policy = TtlLruPolicy::new(config);

        let key_a = make_key("b", "A");
        let key_b = make_key("b", "B");

        // Insert both.
        policy.on_insert(&key_a, 100, &make_meta(100));
        // Access B recently (higher logical timestamp).
        policy.on_access(&key_a, &make_meta(100)); // A gets timestamp 2
        policy.on_insert(&key_b, 100, &make_meta(100));
        // B gets timestamp 3

        // Sleep past TTL so both become stale.
        std::thread::sleep(Duration::from_millis(60));

        // A was accessed at ts=2, B at ts=3. A is older → should be victim.
        let victim = policy.select_victim().expect("should find a stale victim");

        assert_eq!(
            victim.object_key().as_str(),
            "A",
            "older last_access (A) should be evicted before B"
        );
    }

    /// T3.6: select_victim returns None when no entries exceed TTL.
    #[test]
    fn test_ttl_lru_returns_none_when_no_stale_entries() {
        let config = TtlLruConfig { default_ttl_ms: 5000 };
        let policy = TtlLruPolicy::new(config);

        let key_a = make_key("b", "A");
        policy.on_insert(&key_a, 100, &make_meta(100));

        // Minimal sleep — far from TTL expiry.
        std::thread::sleep(Duration::from_millis(10));

        let victim = policy.select_victim();
        assert!(victim.is_none(), "should return None when no entries exceed TTL");
    }
}
