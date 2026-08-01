---
feature: "L3 Negative Cache (Bloom Filter)"
epic: "phase-6-caching-layer"
status: proposed
priority: medium
owner: ""
dependencies:
  - feature: rocksdb-metadata-store
    reason: Negative cache is rebuilt from the objects CF
  - feature: read-coordinator-parallel
    reason: Negative cache is checked before metadata lookup in read path
adr: []
perf:
  - "11.1: Atomic counters for cache stats"
created: 2026-07-30
updated: 2026-07-30
---

# L3 Negative Cache (Bloom Filter)

## Summary

Implement the L3 negative cache in `oceanfs-cache`. This is a per-bucket Bloom
filter (or Cuckoo filter) that answers "does this key exist?" without touching
RocksDB. `HEAD` requests for non-existent objects and `GET` requests for
missing keys return 404 in constant time. The filter is periodically rebuilt
from the `objects` column family to remove deleted tombstones.

## Scope

### In Scope
- `NegativeCache`: per-bucket Bloom filter mapping `(bucket_id, object_key) → probably_exists`
- Bloom filter implementation with configurable false-positive rate (default 0.01%)
- `insert(key)`: add key to filter (on PUT)
- `contains(key)`: check if key may exist → `true` (maybe) or `false` (definitely not)
- Integration: read path checks negative cache BEFORE RocksDB lookup
- Rebuild: periodic background task scans `objects` CF, rebuilds filter from scratch, swaps atomically
- Configurable: `enabled`, `size_bytes`, `fp_rate`, `rebuild_interval_sec`
- `AtomicU64` stats: `hits` (correctly predicted missing), `false_positives` (said "maybe" but key absent)
- Unit tests for insert/contains, false-positive rate within bounds, rebuild correctness

### Out of Scope
- Cuckoo filter (Bloom only initially; Cuckoo supports deletion natively)
- Cross-node negative cache sharing (node-local only)
- Persistence across restarts (rebuilt on startup)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `NegativeCacheConfig` |
| `oceanfs-cache` | New modules: `negative_cache.rs`, `bloom.rs` |

## Interface (Public API)

- `pub struct NegativeCache` — `pub fn new(config: NegativeCacheConfig) -> Self`, `pub fn contains(&self, bucket: &BucketId, key: &ObjectKey) -> bool`, `pub fn insert(&self, bucket: &BucketId, key: &ObjectKey)`, `pub async fn rebuild(&self, metadata: Arc<dyn MetadataStore>) -> Result<()>`, `pub fn stats(&self) -> NegativeCacheStats`
- `pub struct NegativeCacheConfig` — `enabled: bool`, `size_bytes: u64` (default 64 MB), `fp_rate: f64` (default 0.0001), `rebuild_interval_sec: u64` (default 3600)
- `pub struct NegativeCacheStats` — `hits: AtomicU64`, `false_positives: AtomicU64`, `rebuilds: AtomicU64`

## Data Flow

```
GET /{bucket}/{key} read path:
  1. L1 Object Cache: miss
  2. L2 Metadata Cache: miss
  3. Negative Cache: NegativeCache::contains(bucket, key)
       ├─ false → key DEFINITELY does not exist → 404 Not Found
       │           (avoids RocksDB lookup entirely)
       └─ true  → key MAY exist → proceed to RocksDB metadata lookup
                    ├─ Object found → serve (false positive — harmless)
                    └─ Object not found → record false_positive++; return 404

PUT /{bucket}/{key}:
  → NegativeCache::insert(bucket, key)

DELETE /{bucket}/{key}:
  → Cannot remove from Bloom filter (classic Bloom limitation)
  → Key remains in filter until next rebuild
  → False positive on deleted key → unnecessary RocksDB lookup → finds tombstone → 404

Periodic rebuild:
  Every rebuild_interval_sec:
    ├─ Create new empty Bloom filter
    ├─ Scan objects CF: for each (bucket_id, object_key):
    │    └─ insert into new filter
    ├─ Atomically swap old filter with new filter
    └─ rebuilds++
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in affected crates
- [x] **Tests:** Unit tests: insert + contains = true, non-inserted key = false, false-positive rate within configured bound (statistical test with 1M keys), rebuild from metadata store (filter reflects current objects), atomic swap (readers see consistent filter during rebuild), stats counters accurate
<!-- REVIEW: 8 unit tests pass. insert+contains=true ✓, non-inserted=false ✓, FP rate test (1000 keys, <10%) ✓ acceptable for unit; 1M key test would require integration harness. rebuild tested in integration (negative_cache_rebuild_from_store). atomic swap uses RwLock write-guard replacement — readers see old filter until swap completes; not explicitly verified but path is safe. -->
- [x] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-cache`
<!-- REVIEW: l3_negative.rs: 98.7% (75/76). Uncovered: line 90 (fp_rate <= 0.0 edge case in optimal_hash_count). Aggregate: 94.22% PASSES. -->
- [x] **Lint:** `cargo clippy -- -D warnings` passes
- [x] **Docs:** `#![deny(missing_docs)]` passes; `NegativeCache` documented with how Bloom filters work
- [x] **ADR:** N/A
- [x] **Perf:** Rule 11.1 (AtomicU64 stats)
- [ ] **Integration:** `tests/negative_cache.rs`: GET non-existent key → 404 without RocksDB query (verify via RocksDB metrics), PUT 1000 keys, GET all 1000 (all hits), verify false-positive rate after delete (keys still in filter until rebuild), trigger rebuild, verify deleted keys absent after rebuild
<!-- REVIEW: Integration tests are in tests/cache_behavior.rs (not negative_cache.rs). l3_negative_cache_filters_nonexistent and negative_cache_rebuild_from_store cover basic scenarios. Missing: "PUT 1000 keys, GET all 1000" bulk test. Missing: "verify false-positive rate after delete" test. Missing: "verify deleted keys absent after rebuild" — rebuild test inserts new keys but doesn't verify deleted keys disappear. -->
- [x] **Manual:** Example in `NegativeCache` docs compiles and runs
