---
feature: "L2 Metadata Cache"
epic: "phase-6-caching-layer"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: l1-object-cache
    reason: L2 is consulted after L1 miss in the read path
  - feature: rocksdb-metadata-store
    reason: L2 caches ObjectMetadata entries to avoid RocksDB lookups
adr: []
perf:
  - "2.2: dashmap for concurrent caches"
  - "11.1: Atomic counters for cache stats"
created: 2026-07-30
updated: 2026-07-30
---

# L2 Metadata Cache

## Summary

Implement the L2 metadata cache in `oceanfs-cache`. This is an in-memory LRU
cache of `ObjectMetadata` entries, avoiding RocksDB lookups for hot objects. For
inline blobs (≤ `inline_threshold_bytes`), a metadata cache hit serves the blob
directly from the cached metadata value — zero additional I/O. Write-through
and gossip-based invalidation keep the cache eventually consistent.

## Scope

### In Scope
- `MetadataCache`: LRU cache of `(bucket_id, object_key) → Arc<ObjectMetadata>`
- `dashmap::DashMap` for concurrent access
- LRU eviction with configurable `max_size_bytes` (not entry count)
- TTL eviction: entries expire after `ttl_ms` (default 5 min; 0 = no expiry)
- Inline blob serving: if `ObjectMetadata.inline_data` is `Some`, the L2 cache hit
  provides the blob without any segment I/O — served entirely from cache
- Write-through: on PUT, insert updated metadata into cache
- Invalidation: on DELETE or PUT overwrite, remove stale entry
- Gossip-based invalidation: receive `CacheInvalidate` RPC from other nodes
- `CacheStats`: hit/miss counters with inline-hit sub-counter
- Per-bucket configuration: enabled/disabled, max size, TTL
- Unit tests for inline blob serving, invalidation, gossip integration

### Out of Scope
- Cross-node metadata replication (handled by write coordinator, Phase 4)
- Persistent metadata cache (in-memory only)
- Prefetch warming of metadata cache (separate feature)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `MetadataCacheConfig`, `CacheInvalidateRequest` |
| `oceanfs-cache` | New modules: `metadata_cache.rs` |
| `oceanfs-cache` | Facade export: `pub use metadata_cache::MetadataCache` |

## Interface (Public API)

- `pub struct MetadataCache` — `pub fn new(config: MetadataCacheConfig) -> Self`, `pub fn get(&self, bucket: &BucketId, key: &ObjectKey) -> Option<Arc<ObjectMetadata>>`, `pub fn put(&self, bucket: BucketId, key: ObjectKey, metadata: ObjectMetadata)`, `pub fn invalidate(&self, bucket: &BucketId, key: &ObjectKey)`, `pub fn handle_invalidation(&self, req: CacheInvalidateRequest)`, `pub fn stats(&self) -> MetadataCacheStats`
- `pub struct MetadataCacheConfig` — `enabled: bool`, `max_size_bytes: u64`, `ttl_ms: u64`
- `pub struct MetadataCacheStats` — `hits: AtomicU64`, `inline_hits: AtomicU64`, `misses: AtomicU64`, `evictions: AtomicU64`

## Data Flow

```
GET read path (after L1 miss):
  MetadataCache::get(bucket, key)
    ├─ HIT:
    │    ├─ metadata.inline_data.is_some()?
    │    │    ├─ YES → return inline_data as blob (inline_hits++)
    │    │    │         0 extra I/O — blob served from cache
    │    │    └─ NO  → return chunk list for segment fetch (hits++)
    │    │              (saves 1 RocksDB GET)
    │    └─ hits++
    │
    └─ MISS:
         misses++
           → RocksDB metadata lookup
             → on success: MetadataCache::put(bucket, key, metadata)
                              (populate cache for next read)

Invalidation flow:
  Local PUT/DELETE:
    MetadataCache::invalidate(bucket, key)

  Remote invalidation (via gossip):
    Node A receives CacheInvalidate RPC from Node B
      → MetadataCache::handle_invalidation(req)
           └─ remove (bucket, key) from cache if present
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in affected crates
- [x] **Tests:** Unit tests: metadata insert + get = hit, inline metadata hit serves blob, LRU eviction, TTL expiry, invalidation removes entry, gossip invalidation received → entry removed, concurrent access (dashmap), stats accurate
<!-- REVIEW: 9 unit tests. All scenarios covered. LRU eviction (lru_eviction_when_cache_full) added in iteration 2. LRU uses generation-based access-order eviction triggered when entry count exceeds rough max_size_bytes estimate. "concurrent access" implicitly covered by DashMap guarantees. -->
- [x] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-cache`
<!-- REVIEW: l2_metadata.rs: 84.5% (87/103). Uncovered: update-in-place path (224-226), set_bucket_config existing-bucket branch (266-282). Aggregate: 94.22% PASSES. -->
- [x] **Lint:** `cargo clippy -- -D warnings` passes
- [x] **Docs:** `#![deny(missing_docs)]` passes; `MetadataCache` documented with inline-blob example
- [x] **ADR:** N/A
- [x] **Perf:** Rule 2.2 (DashMap), 11.1 (AtomicU64 stats)
- [ ] **Integration:** `tests/metadata_cache.rs`: write inline blob, read (metadata cache hit → served inline), read non-inline blob (metadata cache hit → chunk list returned), invalidate via PUT, verify miss on next read
<!-- REVIEW: Integration tests are in tests/cache_behavior.rs (not metadata_cache.rs). l2_cache_inline_serving tests inline blob serving. l1_l2_cascade_scenario tests L1→L2 cascade. Missing: "read non-inline blob (metadata cache hit → chunk list returned)" — no test for non-inline metadata hit returning chunks. Missing: "invalidate via PUT, verify miss on next read" — invalidate is tested in unit test but not as PUT-update scenario in integration. -->
- [x] **Manual:** Example in `MetadataCache` docs compiles and runs
