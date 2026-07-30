---
feature: "L1 Object Data Cache"
epic: "phase-6-caching-layer"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: read-coordinator-parallel
    reason: L1 cache is checked first in the read path before any I/O
adr: []
perf:
  - "2.2: dashmap for concurrent caches"
  - "1.1: Use bytes::Bytes for blob data"
  - "11.1: Atomic counters for cache hit/miss stats"
created: 2026-07-30
updated: 2026-07-30
---

# L1 Object Data Cache

## Summary

Implement the L1 object data cache in `oceanfs-cache`. This is a bucket-scoped
in-memory LRU cache of hot blob payloads, serving frequently accessed objects
with zero disk I/O. The cache uses `dashmap` for concurrent access, TTL-based
eviction, and size-gated insertion (only cache blobs ≤ `max_blob_size`). BLAKE3
verification still runs on cache hits to detect corruption.

## Scope

### In Scope
- `ObjectCache`: per-bucket LRU cache of `(bucket_id, object_key) → Bytes`
- `dashmap::DashMap` for concurrent read/write access
- LRU eviction: access-order eviction with configurable `max_size_bytes`
- TTL eviction: entries expire after `ttl_ms` (0 = no expiry)
- Size-gated insertion: only cache blobs ≤ `object_cache_max_blob_size`
- Population: on successful GET, insert blob into cache if size-eligible
- Invalidation: on PUT or DELETE of same key, remove from cache (best-effort)
- `CacheStats`: hit/miss counters via `AtomicU64`
- Per-bucket configuration: cache size, TTL, max blob size, enabled/disabled
- Stale reads tolerated: BLAKE3 verification catches corruption; cache is performance-only
- Unit tests for insert/get, LRU eviction order, TTL expiry, size gate, invalidation

### Out of Scope
- Cross-node cache coherence (node-local only; gossip invalidation is Phase 7)
- Cache warming or persistence across restarts
- Compression of cached blobs

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `ObjectCacheConfig` |
| `oceanfs-cache` | New crate; modules: `object_cache.rs`, `stats.rs` |
| `oceanfs-cache` | Facade exports: `pub use object_cache::ObjectCache`, `pub use stats::CacheStats` |

## Interface (Public API)

- `pub struct ObjectCache` — `pub fn new(config: ObjectCacheConfig) -> Self`, `pub fn get(&self, bucket: &BucketId, key: &ObjectKey) -> Option<Bytes>`, `pub fn put(&self, bucket: BucketId, key: ObjectKey, data: Bytes)`, `pub fn invalidate(&self, bucket: &BucketId, key: &ObjectKey)`, `pub fn stats(&self) -> CacheStats`
- `pub struct ObjectCacheConfig` — `enabled: bool`, `max_size_bytes: u64`, `ttl_ms: u64`, `max_blob_size: u64`
- `pub struct CacheStats` — `hits: AtomicU64`, `misses: AtomicU64`, `evictions: AtomicU64`, `size_bytes: AtomicU64`, `entry_count: AtomicUsize`
- `pub fn hit_rate(&self) -> f64` — `hits / (hits + misses)`

## Data Flow

```
GET object read path with L1 cache:
  ObjectCache::get(bucket, key)
    ├─ Cache HIT:
    │    ├─ Check TTL: entry age < ttl_ms?
    │    │    ├─ YES → return cached Bytes
    │    │    │         (caller still verifies BLAKE3)
    │    │    └─ NO  → evict entry → proceed to MISS
    │    └─ hits.inc()
    │
    └─ Cache MISS:
         misses.inc()
           → continue to L2 metadata cache → ... → segment fetch
             → on successful GET with blob ≤ max_blob_size:
                  ObjectCache::put(bucket, key, data.clone())
                    ├─ If cache full (size > max_size_bytes):
                    │    └─ Evict LRU entries until under threshold
                    └─ Insert with current timestamp

PUT/DELETE invalidation:
  ObjectCache::invalidate(bucket, key)
    → remove entry if present (best-effort; may miss due to TTL race)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-core` and `oceanfs-cache`
- [ ] **Tests:** Unit tests: insert + get = hit, get missing key = None, LRU eviction (fill cache → oldest entry evicted), TTL expiry (insert, sleep > ttl, get = None), size gate (blob > max_blob_size → not cached), invalidation removes entry, concurrent read/write correctness (dashmap), stats counters accurate
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-cache`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `ObjectCache` documented with config example
- [ ] **ADR:** N/A (spec §5.2 covers caching layers)
- [ ] **Perf:** Rule 2.2 (DashMap), 1.1 (Bytes for zero-copy), 11.1 (AtomicU64 stats)
- [ ] **Integration:** `tests/cache_behavior.rs`: PUT object, GET twice (first miss, second hit), verify cache stats; fill cache to max, verify eviction; PUT update invalidates cache; TTL expiry verified
- [ ] **Manual:** Example in `ObjectCache` docs compiles and runs
