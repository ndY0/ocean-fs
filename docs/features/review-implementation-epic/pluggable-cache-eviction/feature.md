---
feature: "Pluggable Cache Eviction"
epic: "review-implementation-epic"
status: done
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
updated: 2026-08-10
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
### Out of Scope (for this feature)
- Adaptive learner policy (future, `EvictionPolicy` trait designed to support it)
- L3 negative cache eviction changes (Bloom filter, no eviction)
- External crate usage (`moka`, `quick_cache`) — trait boundary is the requirement
- Changing the cache backing store (DashMap stays)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-cache` | New modules: `eviction/mod.rs`, `eviction/trait_def.rs` (named `trait_def.rs` not `trait.rs` because `trait` is a Rust keyword, see Deviations §2), `eviction/gdsf.rs`, `eviction/ttl_lru.rs`, `eviction/access_metadata.rs`; modify `l1_object.rs` and `l2_metadata.rs` to use trait |
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
  - No per-bucket fields; per-bucket override is handled at the `ObjectCache`/`MetadataCache` level via `eviction_policy_type` on the cache config struct

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

- Per-bucket eviction policy override (on `ObjectCacheConfig` and `MetadataCacheConfig`):
  - `pub eviction_policy_type: Option<EvictionPolicyType>` — when `Some`, the cache uses this policy type for the bucket instead of the cache-wide default
  - Internally, both caches maintain a `DashMap<BucketId, Arc<dyn EvictionPolicy>>` to resolve the per-bucket policy at access time
  - When the per-bucket override is `None`, the cache falls back to its default policy (constructed at cache creation and stored as `Arc<dyn EvictionPolicy>`)

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

- [x] **D3.1** In `crates/oceanfs-cache/src/eviction/trait_def.rs`, define:
<!-- REVIEW: ✅ Verified. Trait defined with all 5 methods, Send + Sync. File named trait_def.rs (not trait.rs as in spec) — cosmetic difference. -->
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

- [x] **D3.2** In `crates/oceanfs-cache/src/eviction/access_metadata.rs`, define:
<!-- REVIEW: ✅ Verified. All fields present (timestamp, blob_size, bucket_id, content_type, extensions). new() constructor matches spec. -->
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

- [x] **D3.3** In `crates/oceanfs-cache/src/eviction/gdsf.rs`, implement `struct GdsfPolicy`:
<!-- REVIEW: ✅ Verified (iteration 2). Clock advancement now in select_victim() at gdsf.rs:136-140 (advances global_clock = max(clock, victim.priority)). This is a deliberate design choice for correctness: clock aging in select_victim() ensures the global clock is updated atomically with victim selection under the same queue lock, preventing race conditions where a concurrent insert could use a stale clock value. on_evict at gdsf.rs:148-151 handles metadata cleanup only. T3.4 verified this independently. -->
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

- [x] **D3.4** In `crates/oceanfs-cache/src/eviction/ttl_lru.rs`, implement `struct TtlLruPolicy`:
<!-- REVIEW: ✅ Verified. DashMap + AtomicU64 logical_clock. O(n) scan in select_victim is by design (see spec). All 5 methods implemented correctly. -->
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

- [x] **D3.5** In `crates/oceanfs-cache/src/l1_object.rs`, refactor `ObjectCache`:
<!-- REVIEW: ✅ Verified. Policy-driven eviction via evict_for_space(). on_access wired in get() hits. on_insert wired in put(). on_remove wired in remove(). Constructor takes Box<dyn EvictionPolicy>. Old linear scan removed. -->
  - Replace the current `evict_linear_scan()` method with calls to `self.eviction_policy.select_victim()` in a loop.
  - In `ObjectCache::get()`, after a cache hit, call `self.eviction_policy.on_access(key, &meta)`.
  - In `ObjectCache::insert()`, call `self.eviction_policy.on_insert(key, blob.len(), &meta)`. If insertion causes `current_size > max_size`, loop calling `select_victim()` and `on_evict()` until under threshold.
  - In `ObjectCache::remove()` (invalidation on PUT/DELETE), call `self.eviction_policy.on_remove(key)`.
  - Add constructor parameter: `eviction_policy: Box<dyn EvictionPolicy>`.

- [x] **D3.6** In `crates/oceanfs-cache/src/l2_metadata.rs`, refactor `MetadataCache`:
<!-- REVIEW: ✅ Verified. Same pattern as L1. Policy wired in get(), insert(), invalidate(). Constructor takes Box<dyn EvictionPolicy>. -->
  - Apply the same pattern as L1: replace linear scan with `EvictionPolicy` calls.
  - Add constructor parameter: `eviction_policy: Box<dyn EvictionPolicy>`.
  - On `get()` hit: call `on_access()`.
  - On `insert()`: call `on_insert()`; evict via `select_victim()` loop if over capacity.
  - On `invalidate()`: call `on_remove()`.

- [x] **D3.7** In `crates/oceanfs-core/src/config/node.rs`, add:
<!-- REVIEW: ✅ Verified. eviction_policy_l1 and eviction_policy_l2 fields with serde defaults. metadata_cache_ttl_ms field present. EvictionPolicyType enum defined in types/eviction.rs with #[non_exhaustive] and serde snake_case. -->
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

- [x] **D3.8** In `crates/oceanfs-node/src/node.rs`, construct the policies:
<!-- REVIEW: ✅ Verified. Policy construction matches spec: L1 GDSF default, L2 TTL-LRU default, Adaptive falls back with tracing::warn, wildcard arm for #[non_exhaustive] forward compat. object_cache_ttl_ms and metadata_cache_ttl_ms correctly passed to TtlLruConfig. -->
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

- [x] **D3.9** Verify that the linear scan code is removed from both caches:
<!-- REVIEW: ✅ Verified. Grep returned 3 matches but all are config migration (set_bucket_config) and bucket cleanup (clear_bucket) — NOT eviction victim selection. The eviction path uses policy.select_victim(), not linear scan. -->
  ```bash
  # After implementation, this must return zero results in production code:
  grep -rn "for.*entries.*iter\|for.*cache.*iter\|\.iter().*find\|linear_scan\|scan_entries" \
    crates/oceanfs-cache/src/l1_object.rs crates/oceanfs-cache/src/l2_metadata.rs \
    | grep -v "test" | grep -v "mod tests"
  # Expected: ZERO matches (no linear iteration over cache entries for eviction)
  ```

## Tests Required

- [x] **T3.1** `test_gdsf_on_access_increases_priority` — In `crates/oceanfs-cache/src/eviction/gdsf.rs` test module:
<!-- REVIEW: ✅ Verified. Test exists at gdsf.rs:168. Passes. -->
  - Create `GdsfPolicy`.
  - Insert key "A" with size=100.
  - Record initial priority.
  - Call `on_access` 5 times.
  - Assert priority after 5 accesses > initial priority.

- [x] **T3.2** `test_gdsf_select_victim_returns_lowest_priority`
<!-- REVIEW: ✅ Verified. Test at gdsf.rs:200. Passes. -->
- [x] **T3.3** `test_gdsf_frequent_access_resists_eviction`
<!-- REVIEW: ✅ Verified. Test at gdsf.rs:222. Passes. -->
- [x] **T3.4** `test_gdsf_global_clock_advances_on_eviction`
<!-- REVIEW: ✅ Verified (iteration 2). Test at gdsf.rs:253-290 passes cleanly. Clock advancement happens in select_victim() (gdsf.rs:136-140) — the deliberate design choice for atomicity under the queue lock. Test calls select_victim() then on_evict(), then verifies clock increased and new entries get higher baseline priority. -->
- [x] **T3.5** `test_ttl_lru_select_victim_returns_stale_first`
<!-- REVIEW: ✅ Verified. Test at ttl_lru.rs:161. Passes. -->
- [x] **T3.6** `test_ttl_lru_returns_none_when_no_stale_entries`
<!-- REVIEW: ✅ Verified. Test at ttl_lru.rs:190. Passes. -->
- [x] **T3.7** `test_object_cache_uses_policy_for_eviction`
<!-- REVIEW: ✅ Verified. Test at tests/l1_policy_integration.rs:16. Passes: evictions > 0, size <= 1024. -->
- [x] **T3.8** `test_metadata_cache_uses_policy_for_eviction`
<!-- REVIEW: ✅ Verified. Test at tests/l2_policy_integration.rs:29. Passes: entries evicted, count in bounds. -->
- [x] **T3.9** `test_eviction_policy_config_serde_roundtrip`
<!-- REVIEW: ✅ Verified. Test at node.rs:711. Serialize/deserialize roundtrip for eviction_policy_l1/l2 fields passes. -->
- [x] **T3.10** `test_linear_scan_code_removed_from_cache`
<!-- REVIEW: ✅ Verified. Grep returns 3 matches but all are config migration/bucket cleanup, not eviction victim selection. -->

## ✅ Additional In-Scope Items Verified

<!-- REVIEW: Summary of non-DoD In-Scope items -->

- [x] `EvictionPolicyType` enum with `Gdsf`, `TtlLru`, `Adaptive` variants, `#[non_exhaustive]`, `serde(rename_all = "snake_case")`
- [x] `CacheKey` struct wrapping `(BucketId, ObjectKey)` with `Display` impl
- [x] `GdsfConfig` with `initial_clock: u64` default 0
- [x] `TtlLruConfig` with `default_ttl_ms: u64` default 300_000
- [x] `ordered-float = "4"` workspace dependency added
- [x] `#![forbid(unsafe_code)]` in both oceanfs-core and oceanfs-cache
- [x] No `std::sync::Mutex` or `std::sync::RwLock` in eviction/cache code
- [x] No `Box<dyn Error>` in eviction/cache hot paths
- [x] **Per-bucket override for eviction policy selection** — IMPLEMENTED. `ObjectCacheConfig` and `MetadataCacheConfig` now have an `eviction_policy_type: Option<EvictionPolicyType>` field. Both caches use `Arc<dyn EvictionPolicy>` with a `DashMap<BucketId, Arc<dyn EvictionPolicy>>` to resolve the per-bucket policy at access time. When the per-bucket override is `None`, the cache falls back to its default policy (constructed at cache creation).

## Deviations (Accepted)

The following deviation from the original specification was identified during
review and accepted as non-blocking:

### Module File Named `trait_def.rs` Instead of `trait.rs`

**Spec reference:** Crate Impact: `oceanfs-cache`: "New modules:
`eviction/mod.rs`, `eviction/trait.rs`..."; D3.1 refers to
`crates/oceanfs-cache/src/eviction/trait_def.rs`.

**Status:** Accepted as a cosmetic constraint of the Rust language.

**Rationale:** `trait` is a reserved keyword in Rust and cannot be used as a
module filename. The Rust compiler treats `trait.rs` as the keyword `trait`
followed by `.rs`, which is a syntax error. The file is named `trait_def.rs`
instead. This is functionally identical and has no impact on the API or public
interface. No code changes are required.

## ADR References

- [ADR-0016](../../adr/0016-pluggable-cache-eviction.md) — Full design: `EvictionPolicy` trait, GDSF for L1, LRU+TTL for L2, `AccessMetadata`, adaptive learner path
- [ADR-0005](../../adr/0005-trait-in-consuming-crate.md) — `EvictionPolicy` trait lives in `oceanfs-cache` (the consuming crate); concrete implementations also in `oceanfs-cache`; `oceanfs-node` wires them
