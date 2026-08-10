//! Greedy-Dual Size Frequency (GDSF) eviction policy.
//!
//! Size-aware, priority-based eviction for the L1 object cache.
//! See ADR-0016 for the full design rationale.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;

use super::{AccessMetadata, CacheKey, EvictionPolicy};

/// Configuration for the GDSF eviction policy.
///
/// # Examples
///
/// ```
/// use oceanfs_cache::eviction::GdsfConfig;
///
/// let config = GdsfConfig::default();
/// assert_eq!(config.initial_clock, 0);
/// ```
#[derive(Debug, Clone, Default)]
pub struct GdsfConfig {
    /// Initial value of the global clock. Default: 0.
    pub initial_clock: u64,
}

/// Per-entry state tracked by the GDSF policy.
#[derive(Debug)]
struct GdsfEntry {
    /// Current priority score: `clock + (frequency / size)`.
    priority: Mutex<f64>,
    /// Number of times this entry has been accessed.
    frequency: AtomicU64,
    /// Size of the blob in bytes.
    size: usize,
}

/// Greedy-Dual Size Frequency (GDSF) eviction policy.
///
/// Designed for the L1 object cache where blob sizes vary by orders of
/// magnitude. GDSF balances size (large blobs evicted faster), frequency
/// (hot blobs resist eviction), and recency (aging via global clock).
///
/// Internal state:
/// - `DashMap<CacheKey, GdsfEntry>` for O(1) entry access
/// - `Mutex<BTreeMap<(OrderedFloat<f64>, u64), CacheKey>>` for O(log n) victim selection
/// - `AtomicU64` global clock for aging
///
/// # Examples
///
/// ```
/// use oceanfs_cache::eviction::{GdsfConfig, GdsfPolicy, EvictionPolicy};
///
/// let policy = GdsfPolicy::new(GdsfConfig::default());
/// ```
pub struct GdsfPolicy {
    /// Maps cache keys to their GDSF metadata.
    entries: DashMap<CacheKey, GdsfEntry>,
    /// Priority queue: maps (priority, tiebreaker) → CacheKey.
    /// `pop_first()` returns the entry with the lowest priority — i.e., the victim.
    queue: Mutex<std::collections::BTreeMap<(OrderedFloat<f64>, u64), CacheKey>>,
    /// Global clock value (f64 bits stored as u64). Advanced on eviction.
    global_clock: AtomicU64,
    /// Monotonically increasing counter for breaking priority ties.
    tiebreaker: AtomicU64,
}

impl GdsfPolicy {
    /// Creates a new GDSF policy with the given configuration.
    pub fn new(config: GdsfConfig) -> Self {
        Self {
            entries: DashMap::new(),
            queue: Mutex::new(std::collections::BTreeMap::new()),
            global_clock: AtomicU64::new(config.initial_clock),
            tiebreaker: AtomicU64::new(0),
        }
    }
}

impl EvictionPolicy for GdsfPolicy {
    fn on_access(&self, key: &CacheKey, _meta: &AccessMetadata) {
        if let Some(entry) = self.entries.get(key) {
            let freq = entry.frequency.fetch_add(1, Ordering::Relaxed) + 1;
            let clock_bits = self.global_clock.load(Ordering::Relaxed);
            let clock = f64::from_bits(clock_bits);
            let new_priority = clock + (freq as f64) / (entry.size as f64).max(1.0);

            // Update priority and re-insert into queue.
            {
                let mut prio = entry.priority.lock();
                *prio = new_priority;
            }

            let tiebreaker = self.tiebreaker.fetch_add(1, Ordering::Relaxed);
            let mut queue = self.queue.lock();
            // Remove old entry (we don't know old priority, but it'll be stale).
            // We insert the new (priority, tiebreaker) → key mapping.
            // The old mapping with a different tiebreaker remains until
            // `select_victim` finds the current mapping.
            queue.insert((OrderedFloat(new_priority), tiebreaker), key.clone());
        }
    }

    fn on_insert(&self, key: &CacheKey, size: usize, _meta: &AccessMetadata) {
        let clock_bits = self.global_clock.load(Ordering::Relaxed);
        let clock = f64::from_bits(clock_bits);
        let priority = clock + 1.0 / (size as f64).max(1.0);

        let entry =
            GdsfEntry { priority: Mutex::new(priority), frequency: AtomicU64::new(1), size };
        self.entries.insert(key.clone(), entry);

        let tiebreaker = self.tiebreaker.fetch_add(1, Ordering::Relaxed);
        let mut queue = self.queue.lock();
        queue.insert((OrderedFloat(priority), tiebreaker), key.clone());
    }

    fn select_victim(&self) -> Option<CacheKey> {
        let mut queue = self.queue.lock();
        // Pop entries until we find one whose stored priority matches
        // its current priority (stale entries from on_access updates are skipped).
        loop {
            let candidate = queue.pop_first()?;
            let stored_priority: f64 = *candidate.0 .0;
            let key = candidate.1;

            // Verify the entry still exists and its priority hasn't changed.
            if let Some(entry) = self.entries.get(&key) {
                let current = *entry.priority.lock();
                if (stored_priority - current).abs() < 1e-10 {
                    // Advance global clock to the evicted entry's priority
                    // so that newly inserted entries get a higher baseline.
                    let clock_bits = self.global_clock.load(Ordering::Relaxed);
                    let clock = f64::from_bits(clock_bits);
                    if stored_priority > clock {
                        self.global_clock.store(stored_priority.to_bits(), Ordering::Relaxed);
                    }
                    return Some(key);
                }
                // Stale entry — on_access already inserted a new (priority, key) pair.
            }
            // Entry was removed — skip.
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
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{BucketId, ObjectKey};

    use super::*;

    fn make_key(bucket: &str, obj: &str) -> CacheKey {
        CacheKey::new(BucketId::new(bucket), ObjectKey::new(obj))
    }

    fn make_meta(size: u64) -> AccessMetadata {
        AccessMetadata::new(BucketId::new("test"), size)
    }

    /// T3.1: on_access increases priority after repeated accesses.
    #[test]
    fn test_gdsf_on_access_increases_priority() {
        let policy = GdsfPolicy::new(GdsfConfig::default());
        let key = make_key("b", "A");

        policy.on_insert(&key, 100, &make_meta(100));

        // Read initial priority.
        let initial = {
            let entry = policy.entries.get(&key).unwrap();
            let guard = entry.priority.lock();
            *guard
        };

        // Access 5 times.
        for _ in 0..5 {
            policy.on_access(&key, &make_meta(100));
        }

        let after = {
            let entry = policy.entries.get(&key).unwrap();
            let guard = entry.priority.lock();
            *guard
        };

        assert!(
            after > initial,
            "priority should increase with repeated accesses: {initial} -> {after}"
        );
    }

    /// T3.2: select_victim returns the entry with the lowest priority (largest blob).
    #[test]
    fn test_gdsf_select_victim_returns_lowest_priority() {
        let policy = GdsfPolicy::new(GdsfConfig::default());

        // Insert large blob (low priority) and small blob (high priority).
        let large = make_key("b", "large"); // size=1000
        let small = make_key("b", "small"); // size=10

        policy.on_insert(&large, 1000, &make_meta(1000));
        policy.on_insert(&small, 10, &make_meta(10));

        let victim = policy.select_victim().expect("should have a victim");

        assert_eq!(
            victim.object_key().as_str(),
            "large",
            "larger blob (lower priority) should be evicted first, got '{}'",
            victim.object_key().as_str()
        );
    }

    /// T3.3: frequent access resists eviction — equal-size entries, one accessed more.
    #[test]
    fn test_gdsf_frequent_access_resists_eviction() {
        let policy = GdsfPolicy::new(GdsfConfig::default());

        let key_a = make_key("b", "A");
        let key_b = make_key("b", "B");

        // Same size, same initial priority.
        policy.on_insert(&key_a, 100, &make_meta(100));
        policy.on_insert(&key_b, 100, &make_meta(100));

        // Access A 50 times (boosts frequency → higher priority).
        for _ in 0..50 {
            policy.on_access(&key_a, &make_meta(100));
        }

        let victim = policy.select_victim().expect("should have a victim");

        assert_eq!(
            victim.object_key().as_str(),
            "B",
            "less frequently accessed entry should be evicted"
        );
    }

    /// T3.4: global clock advances on eviction, giving new entries higher priority.
    #[test]
    fn test_gdsf_global_clock_advances_on_eviction() {
        let policy = GdsfPolicy::new(GdsfConfig::default());

        let key_a = make_key("b", "A");
        policy.on_insert(&key_a, 100, &make_meta(100));

        // Capture clock before eviction.
        let clock_before = f64::from_bits(policy.global_clock.load(Ordering::Relaxed));

        // select_victim advances the clock.
        let victim = policy.select_victim().unwrap();
        policy.on_evict(&victim);

        let clock_after = f64::from_bits(policy.global_clock.load(Ordering::Relaxed));
        assert!(
            clock_after > clock_before,
            "global clock should advance after eviction: {clock_before} -> {clock_after}"
        );

        // Insert new key B with size=1.
        let key_b = make_key("b", "B");
        policy.on_insert(&key_b, 1, &make_meta(1));

        let new_priority = {
            let entry = policy.entries.get(&key_b).unwrap();
            let guard = entry.priority.lock();
            *guard
        };

        // New entry's priority should be clock + 1/1 >= clock_after + 1.
        assert!(
            new_priority >= clock_after + 0.9,
            "new entry priority ({new_priority}) should be >= clock_after + 1 ({})",
            clock_after + 1.0
        );
    }
}
