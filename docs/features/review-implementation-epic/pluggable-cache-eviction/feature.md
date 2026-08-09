---
feature: "Pluggable Cache Eviction"
epic: "review-implementation-epic"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: gap-closure-addendum
    reason: Item 3 (cache config pass-through) must be complete so that
      EvictionPolicy implementations can read their configuration
      (eviction policy type, TTL, size thresholds) from NodeConfig
adr:
  - 0016-pluggable-cache-eviction
  - 0005-trait-in-consuming-crate
perf:
  - "2.2 DashMap for concurrent caches"
  - "6.5 BTreeMap for ordered access"
created: 2026-08-09
updated: 2026-08-09
---

# Pluggable Cache Eviction

## Summary

L1 (object data) and L2 (metadata) cache eviction currently uses a linear
scan (review finding #13). Under load-dependent eviction, a linear scan
becomes a latency spike. This feature introduces an `EvictionPolicy` trait
in `oceanfs-cache` (per ADR-0016), with two concrete implementations: GDSF
(Greedy-Dual Size Frequency) for L1 (size-aware, priority-based eviction)
and LRU+TTL for L2 (uniform-size, staleness-deadline eviction). Both replace
the O(n) linear scan with O(log n) or O(1) structures. The `AccessMetadata`
struct carries per-access signals (timestamp, blob_size, bucket_id,
content_type, extensions) for future adaptive learner support. The cache
frontend (`ObjectCache`, `MetadataCache`) is refactored to call the policy
trait instead of iterating entries linearly.

## Scope

### In Scope
- `EvictionPolicy` trait definition in `oceanfs-cache` with 5 methods
- `AccessMetadata` struct with extension field
- `GdsfPolicy` implementation for L1 object cache using BTreeMap priority queue
- `TtlLruPolicy` implementation for L2 metadata cache using DashMap + time ordering
- Refactor `ObjectCache` to call `EvictionPolicy` methods instead of linear scan
- Refactor `MetadataCache` to call `EvictionPolicy` methods instead of linear scan
- Configuration: `NodeConfig` gets `eviction_policy_l1` and `eviction_policy_l2` fields
- Per-bucket override for eviction policy selection

### Out of Scope (for this feature)
- Adaptive learner policy (future, `EvictionPolicy` trait designed to support it)
- L3 negative cache eviction changes (Bloom filter, no eviction)
- External crate usage (`moka`, `quick_cache`) — trait boundary is the requirement
- Changing the cache backing store (DashMap stays)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-cache` | New modules: `eviction/mod.rs`, `eviction/trait.rs`, `eviction/gdsf.rs`, `eviction/ttl_lru.rs`, `eviction/access_metadata.rs`; modify `l1_object.rs` and `l2_metadata.rs` to use trait |
| `oceanfs-core` | New config fields in `NodeConfig`: `eviction_policy_l1`, `eviction_policy_l2`; new enum `EvictionPolicyType` |
| `oceanfs-server` | No changes (cache frontend API unchanged) |
| `oceanfs-node` | In `node.rs`, construct concrete policies and pass to `ObjectCache::new()`, `MetadataCache::new()` |

## Interface (Public API)

- `pub trait EvictionPolicy: Send + Sync` in `oceanfs-cache::eviction`
  - `fn on_access(&self, key: &CacheKey, meta: &AccessMetadata)`
  - `fn on_insert(&self, key: &CacheKey, size: usize, meta: &AccessMetadata)`
  - `fn select_victim(&self) -> Option<CacheKey>`
  - `fn on_evict(&self, key: &CacheKey)`
  - `fn on_remove(&self, key: &CacheKey)`

- `pub struct AccessMetadata`
  - `pub timestamp: std::time::Instant`
  - `pub blob_size: u64`
  - `pub bucket_id: BucketId`
  - `pub content_type: Option<String>`
  - `pub extensions: std::collections::HashMap<String, String>`

- `pub struct GdsfPolicy` — implements `EvictionPolicy`
  - `pub fn new(config: GdsfConfig) -> Self`
  - Internal: `DashMap<CacheKey, GdsfEntry>`, `AtomicU64` for global clock, `Mutex<BTreeMap<(i64, u64), CacheKey>>` for priority queue

- `pub struct GdsfEntry`
  - `pub priority: f64`
  - `pub frequency: u64`
  - `pub size: usize`

- `pub struct GdsfConfig`
  - `pub initial_clock: u64` (default 0)
  - No per-bucket fields; policy behavior is uniform

- `pub struct TtlLruPolicy` — implements `EvictionPolicy`
  - `pub fn new(config: TtlLruConfig) -> Self`
  - Internal: `DashMap<CacheKey, TtlLruEntry>`, `AtomicU64` for logical timestamp counter

- `pub struct TtlLruEntry`
  - `pub inserted_at: Instant`
  - `pub last_accessed_at: AtomicU64` (logical timestamp)
  - `pub ttl_ms: u64`

- `pub struct TtlLruConfig`
  - `pub default_ttl_ms: u64` (default 300000 for L2)

- `pub enum EvictionPolicyType` in `oceanfs-core`
  - `Gdsf`
  - `TtlLru`
  - `Adaptive` (reserved for future learner)

## Data Flow

```
GET /{bucket}/{key}
  ↓
ReadCoordinator → ObjectCache::get(key)
  ├→ cache hit → return data
  │   └→ eviction_policy.on_access(key, AccessMetadata { ... })
  │       GDSF: frequency++, priority = clock + frequency/size
  │       LRU:  last_accessed_at = current_logical_timestamp
  └→ cache miss → fetch from storage → insert into cache
      └→ if cache is full (current_size + blob_size > max_size):
          └→ loop:
              ├→ eviction_policy.select_victim() → Some(victim_key)
              │   GDSF: pop lowest-priority entry from BTreeMap
              │   LRU:  find entry with oldest (ttl_deadline, last_accessed_at)
              ├→ evict victim from backing store (DashMap::remove)
              ├→ eviction_policy.on_evict(victim_key)
              │   GDSF: global_clock = victim.priority
              └→ update current_size -= victim.size
          └→ insert new entry:
              ├→ eviction_policy.on_insert(key, blob_size, AccessMetadata{...})
              │   GDSF: priority = global_clock + 1/size; insert into BTreeMap
              │   LRU:  last_accessed_at = current_logical_timestamp
              └→ store blob in DashMap
```

## Definition of Done

- [ ] **D3.1** In `crates/oceanfs-cache/src/eviction/trait.rs`, define:
  ```rust
  /// Pluggable eviction policy for the object and metadata caches.
  pub trait EvictionPolicy: Send + Sync {
      /// Called on every cache hit.
      fn on_access(&self, key: &CacheKey, meta: &AccessMetadata);
      /// Called when a new entry is inserted.
      fn on_insert(&self, key: &CacheKey, size: usize, meta: &AccessMetadata);
      /// Select a victim for eviction. Returns None if no preference.
      fn select_victim(&self) -> Option<CacheKey>;
      /// Called after an entry has been successfully evicted.
      fn on_evict(&self, key: &CacheKey);
      /// Called when an entry is explicitly removed (invalidation, delete).
      fn on_remove(&self, key: &CacheKey);
  }
  ```

- [ ] **D3.2** In `crates/oceanfs-cache/src/eviction/access_metadata.rs`, define:
  ```rust
  #[derive(Debug, Clone)]
  pub struct AccessMetadata {
      pub timestamp: std::time::Instant,
      pub blob_size: u64,
      pub bucket_id: BucketId,
      pub content_type: Option<String>,
      pub extensions: HashMap<String, String>,
  }

  impl AccessMetadata {
      pub fn new(bucket_id: BucketId, blob_size: u64) -> Self {
          Self {
              timestamp: std::time::Instant::now(),
              blob_size,
              bucket_id,
              content_type: None,
              extensions: HashMap::new(),
          }
      }
  }
  ```

- [ ] **D3.3** In `crates/oceanfs-cache/src/eviction/gdsf.rs`, implement `struct GdsfPolicy`:
  ```rust
  use std::collections::BTreeMap;
  use std::sync::atomic::{AtomicU64, Ordering};
  use dashmap::DashMap;
  use parking_lot::Mutex;

  struct GdsfEntry {
      priority: AtomicF64,      // or Mutex<f64>; AtomicF64 requires `atomic_float` or parking_lot impl
      frequency: AtomicU64,
      size: usize,
  }

  pub struct GdsfPolicy {
      entries: DashMap<CacheKey, GdsfEntry>,
      /// Maps (priority, tiebreaker) → CacheKey for O(log n) victim selection.
      /// Uses negative priority so that BTreeMap::first_entry() returns lowest priority.
      /// Tiebreaker is a monotonically increasing insert counter to break ties.
      queue: Mutex<BTreeMap<(OrderedFloat<f64>, u64), CacheKey>>,
      global_clock: AtomicU64,  // f64 bits stored as u64; increment on eviction
      tiebreaker: AtomicU64,
  }
  ```
  Implement all `EvictionPolicy` methods:
  - `on_access`: increment `entry.frequency`; recompute `priority = clock + frequency/size`; update position in `queue` (remove old, insert new).
  - `on_insert`: compute `priority = global_clock + 1.0 / size`; insert into `entries` and `queue`.
  - `select_victim`: lock `queue`, pop `first_entry()` (lowest priority), return `Some(key)` or `None` if empty.
  - `on_evict`: update `global_clock` to evicted entry's priority (clock = max(clock, evicted_priority)); remove from `entries`.
  - `on_remove`: remove from `entries` and `queue`.

- [ ] **D3.4** In `crates/oceanfs-cache/src/eviction/ttl_lru.rs`, implement `struct TtlLruPolicy`:
  ```rust
  pub struct TtlLruPolicy {
      entries: DashMap<CacheKey, TtlLruEntry>,
      logical_clock: AtomicU64,
      default_ttl_ms: u64,
  }

  struct TtlLruEntry {
      inserted_at: Instant,
      last_accessed_at: AtomicU64,
  }
  ```
  Implement all `EvictionPolicy` methods:
  - `on_access`: `entry.last_accessed_at.store(logical_clock.fetch_add(1, Relaxed), Relaxed)`.
  - `on_insert`: insert with `last_accessed_at = logical_clock.fetch_add(1, Relaxed)` and `inserted_at = Instant::now()`.
  - `select_victim`: iterate all entries, find one where `inserted_at.elapsed() > ttl_ms` AND has the smallest `last_accessed_at`. Return `Some(key)` for stale entries preferentially, else oldest access. If no entry exceeds TTL, return `None` (don't evict). O(n) scan is acceptable because L2 metadata entries are ~200 bytes each and TTL filter reduces the scan set significantly. If proven problematic, replace with a BTreeSet ordered by `(is_stale, last_accessed_at)`.
  - `on_evict`: `entries.remove(key)`.
  - `on_remove`: `entries.remove(key)`.

- [ ] **D3.5** In `crates/oceanfs-cache/src/l1_object.rs`, refactor `ObjectCache`:
  - Replace the current `evict_linear_scan()` method with calls to `self.eviction_policy.select_victim()` in a loop.
  - In `ObjectCache::get()`, after a cache hit, call `self.eviction_policy.on_access(key, &meta)`.
  - In `ObjectCache::insert()`, call `self.eviction_policy.on_insert(key, blob.len(), &meta)`. If insertion causes `current_size > max_size`, loop calling `select_victim()` and `on_evict()` until under threshold.
  - In `ObjectCache::remove()` (invalidation on PUT/DELETE), call `self.eviction_policy.on_remove(key)`.
  - Add constructor parameter: `eviction_policy: Box<dyn EvictionPolicy>`.

- [ ] **D3.6** In `crates/oceanfs-cache/src/l2_metadata.rs`, refactor `MetadataCache`:
  - Apply the same pattern as L1: replace linear scan with `EvictionPolicy` calls.
  - Add constructor parameter: `eviction_policy: Box<dyn EvictionPolicy>`.
  - On `get()` hit: call `on_access()`.
  - On `insert()`: call `on_insert()`; evict via `select_victim()` loop if over capacity.
  - On `invalidate()`: call `on_remove()`.

- [ ] **D3.7** In `crates/oceanfs-core/src/config/node.rs`, add:
  ```rust
  /// Eviction policy for L1 object cache.
  /// Default: "gdsf".
  #[serde(default = "default_eviction_policy_l1")]
  pub eviction_policy_l1: EvictionPolicyType,
  /// Eviction policy for L2 metadata cache.
  /// Default: "ttl_lru".
  #[serde(default = "default_eviction_policy_l2")]
  pub eviction_policy_l2: EvictionPolicyType,
  /// TTL for L2 metadata cache entries in milliseconds.
  /// Default: 300_000 (5 minutes).
  #[serde(default = "default_metadata_cache_ttl_ms")]
  pub metadata_cache_ttl_ms: u64,
  ```
  Define `EvictionPolicyType` in `oceanfs-core/src/types/eviction.rs`:
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
  #[serde(rename_all = "snake_case")]
  pub enum EvictionPolicyType {
      Gdsf,
      TtlLru,
      Adaptive,
  }
  ```

- [ ] **D3.8** In `crates/oceanfs-node/src/node.rs`, construct the policies:
  ```rust
  let l1_policy: Box<dyn oceanfs_cache::eviction::EvictionPolicy> = match config.eviction_policy_l1 {
      EvictionPolicyType::Gdsf => Box::new(GdsfPolicy::new(GdsfConfig::default())),
      EvictionPolicyType::TtlLru => Box::new(TtlLruPolicy::new(TtlLruConfig {
          default_ttl_ms: config.object_cache_ttl_ms,
      })),
      EvictionPolicyType::Adaptive => {
          tracing::warn!("Adaptive eviction policy not yet implemented; falling back to GDSF");
          Box::new(GdsfPolicy::new(GdsfConfig::default()))
      }
  };
  let l2_policy: Box<dyn oceanfs_cache::eviction::EvictionPolicy> = match config.eviction_policy_l2 {
      EvictionPolicyType::TtlLru => Box::new(TtlLruPolicy::new(TtlLruConfig {
          default_ttl_ms: config.metadata_cache_ttl_ms,
      })),
      EvictionPolicyType::Gdsf => Box::new(GdsfPolicy::new(GdsfConfig::default())),
      EvictionPolicyType::Adaptive => {
          tracing::warn!("Adaptive eviction policy not yet implemented; falling back to TTL-LRU");
          Box::new(TtlLruPolicy::new(TtlLruConfig::default()))
      }
  };
  let object_cache = Arc::new(ObjectCache::new(object_cache_config, l1_policy));
  let metadata_cache = Arc::new(MetadataCache::new(metadata_cache_config, l2_policy));
  ```

- [ ] **D3.9** Verify that the linear scan code is removed from both caches:
  ```bash
  # After implementation, this must return zero results in production code:
  grep -rn "for.*entries.*iter\|for.*cache.*iter\|\.iter().*find\|linear_scan\|scan_entries" \
    crates/oceanfs-cache/src/l1_object.rs crates/oceanfs-cache/src/l2_metadata.rs \
    | grep -v "test" | grep -v "mod tests"
  # Expected: ZERO matches (no linear iteration over cache entries for eviction)
  ```

## Tests Required

- [ ] **T3.1** `test_gdsf_on_access_increases_priority` — In `crates/oceanfs-cache/src/eviction/gdsf.rs` test module:
  - Create `GdsfPolicy`.
  - Insert key "A" with size=100.
  - Record initial priority.
  - Call `on_access` 5 times.
  - Assert priority after 5 accesses > initial priority.

- [ ] **T3.2** `test_gdsf_select_victim_returns_lowest_priority` — In same module:
  - Insert key "A" size=1000 (large → low priority).
  - Insert key "B" size=10 (small → high priority).
  - Call `select_victim()`.
  - Assert returns `Some("A")` (the large blob, lower priority).

- [ ] **T3.3** `test_gdsf_frequent_access_resists_eviction` — In same module:
  - Insert key "A" size=100, key "B" size=100 (same size, same initial priority).
  - Access key "A" 50 times (boosts frequency → higher priority).
  - Call `select_victim()`.
  - Assert returns `Some("B")` (lower frequency).

- [ ] **T3.4** `test_gdsf_global_clock_advances_on_eviction` — In same module:
  - Insert key "A", capture `global_clock`.
  - Call `select_victim()` → returns "A", call `on_evict("A")`.
  - Insert key "B" size=1.
  - Assert key B's priority >= previous global_clock + 1/1 (clock advanced).

- [ ] **T3.5** `test_ttl_lru_select_victim_returns_stale_first` — In `crates/oceanfs-cache/src/eviction/ttl_lru.rs` test module:
  - Insert key "A" with TTL=50ms.
  - Insert key "B" with TTL=50ms, accessed recently.
  - Sleep 100ms (key A is stale, key B was recently accessed but also stale).
  - Call `select_victim()`.
  - Assert returns `Some("A")` (older last_access).

- [ ] **T3.6** `test_ttl_lru_returns_none_when_no_stale_entries` — In same module:
  - Insert key "A" with TTL=5000ms.
  - Sleep 10ms.
  - Call `select_victim()`.
  - Assert returns `None` (no entry exceeds TTL; don't evict).

- [ ] **T3.7** `test_object_cache_uses_policy_for_eviction` — In `crates/oceanfs-cache/tests/l1_policy_integration.rs`:
  - Create `ObjectCache` with max_size_bytes=1024 and GDSF policy.
  - Insert 5 blobs each of size=300 bytes (total 1500 > 1024).
  - Assert at least one eviction occurred (`stats.evictions > 0`).
  - Assert final `current_size <= 1024`.
  - Verify eviction order: the largest blob should have been evicted first.

- [ ] **T3.8** `test_metadata_cache_uses_policy_for_eviction` — In `crates/oceanfs-cache/tests/l2_policy_integration.rs`:
  - Create `MetadataCache` with max_size_bytes=500 and TTL-LRU policy (TTL=100ms).
  - Insert 8 metadata entries each ~100 bytes (total 800 > 500).
  - Sleep 150ms (all stale).
  - Trigger get() → triggers eviction.
  - Assert `current_size <= 500`.
  - Assert stale entries were evicted (not fresh ones).

- [ ] **T3.9** `test_eviction_policy_config_serde_roundtrip` — In `crates/oceanfs-core/src/config/node.rs` test module:
  - Serialize `NodeConfig { eviction_policy_l1: Gdsf, eviction_policy_l2: TtlLru, ... }` to TOML.
  - Deserialize back.
  - Assert both fields match.

- [ ] **T3.10** `test_linear_scan_code_removed_from_cache` — Run grep verification (see D3.9). Assert zero matches.

## ADR References

- [ADR-0016](../../adr/0016-pluggable-cache-eviction.md) — Full design: `EvictionPolicy` trait, GDSF for L1, LRU+TTL for L2, `AccessMetadata`, adaptive learner path
- [ADR-0005](../../adr/0005-trait-in-consuming-crate.md) — `EvictionPolicy` trait lives in `oceanfs-cache` (the consuming crate); concrete implementations also in `oceanfs-cache`; `oceanfs-node` wires them
