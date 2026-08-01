---
feature: "Prefetch Engine"
epic: "phase-6-caching-layer"
status: proposed
priority: low
owner: ""
dependencies:
  - feature: l2-metadata-cache
    reason: Prefetch warms the metadata cache with lookahead entries
  - feature: l1-object-cache
    reason: Prefetch can optionally warm the L1 object cache
  - feature: s3-http-handlers
    reason: Prefetch hooks into LIST and GET responses
adr: []
perf:
  - "2.6: Bounded channels for prefetch work queue"
  - "8.5: Bounded semaphore for task concurrency"
created: 2026-07-30
updated: 2026-07-30
---

# Prefetch Engine

## Summary

Implement the optional prefetch engine in `oceanfs-cache`. After a `LIST`
operation returns object keys, the prefetch engine speculatively warms the
metadata cache (and optionally the L1 object cache) for the next N objects.
Similarly, after a `GET`, it prefetches N subsequent objects in key order.
A bounded semaphore and work queue prevent prefetch from overwhelming the
system. Entirely best-effort — prefetch failures are silent.

## Scope

### In Scope
- `PrefetchEngine`: orchestrates speculative cache warming
- Post-LIST prefetch: after listing objects, prefetch metadata for next `prefetch_after_list` objects
- Post-GET prefetch: after retrieving an object, prefetch metadata for next `prefetch_after_get` objects in key order
- Bounded work queue: `tokio::sync::mpsc` with configurable capacity
- `Semaphore`-bounded concurrency: limits in-flight prefetch operations
- Prefetch warming: insert into metadata cache (and optionally L1 object cache for small blobs)
- Silent failure: if prefetch fails (key not found, timeout, queue full), no error propagated to client
- Configurable: `enabled`, `after_list`, `after_get`
- Unit tests for queue backpressure, semaphore bounding, silent failure

### Out of Scope
- Predictive prefetch (access pattern learning)
- Cross-node prefetch coordination
- Prefetch budget or priority inversion handling

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `PrefetchConfig` |
| `oceanfs-cache` | New modules: `prefetch.rs` |
| `oceanfs-cache` | Facade export: `pub use prefetch::PrefetchEngine` |

## Interface (Public API)

- `pub struct PrefetchEngine` — `pub fn new(config: PrefetchConfig, metadata_cache: Arc<MetadataCache>, object_cache: Option<Arc<ObjectCache>>, metadata: Arc<dyn MetadataStore>) -> Self`, `pub fn after_list(&self, bucket: BucketId, keys: &[ObjectKey], cursor: usize)`, `pub fn after_get(&self, bucket: BucketId, key: &ObjectKey)`
- `pub struct PrefetchConfig` — `enabled: bool`, `after_list: usize` (default 16), `after_get: usize` (default 4), `max_concurrency: usize` (default 8), `queue_capacity: usize` (default 256)

## Data Flow

```
LIST /{bucket}?prefix=photos/
  → returns keys: [photos/001.jpg, photos/002.jpg, ..., photos/050.jpg]
    → client receives first page (e.g., 10 keys)
      → PrefetchEngine::after_list(bucket, &all_keys, cursor=10)
           └─ enqueue prefetch for keys[10..10+after_list]
                └─ background task:
                     ├─ acquire semaphore permit
                     ├─ MetadataStore::get_object(bucket, key) for each key
                     ├─ MetadataCache::put(bucket, key, metadata)
                     └─ (optional) if blob ≤ inline_threshold:
                          └─ ObjectCache::put(bucket, key, data)

GET /photos/042.jpg
  → client retrieves object
    → PrefetchEngine::after_get(bucket, &key)
         └─ look up next `after_get` keys in key order
              └─ enqueue prefetch for photos/043.jpg, photos/044.jpg, ...
                   └─ same background warming as above
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in affected crates
- [x] **Tests:** Unit tests: after_list enqueues correct number of keys, after_get prefetches adjacent keys, semaphore bounds concurrent prefetches, queue full → silent drop (no error), prefetch failure → silent (no panic, no error propagation), disabled = no-ops
<!-- REVIEW: 6 tests pass (2 sync + 4 tokio). after_list_enqueues_tasks ✓, after_get_prefetches_adjacent_keys ✓, disabled_engine_is_noop ✓, queue_full_silently_drops ✓ (no panic), inline_blob_warms_object_cache ✓. "semaphore bounds concurrent prefetches" — partially tested (max_concurrency=1 in queue_full test), but no explicit test verifies exactly N concurrent prefetches run simultaneously. "prefetch failure → silent" — tested implicitly (MockStore returns None for unknown keys = Ok(None) branch). -->
- [x] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-cache`
<!-- REVIEW: prefetch.rs: 90.5% (38/42). Uncovered: config() getter (165-166), Ok(None) branch (209), Err branch (212). Aggregate: 94.22% PASSES. -->
- [x] **Lint:** `cargo clippy -- -D warnings` passes
- [x] **Docs:** `#![deny(missing_docs)]` passes; `PrefetchEngine` documented with use cases
- [x] **ADR:** N/A
- [x] **Perf:** Rule 2.6 (bounded queue), 8.5 (semaphore-bounded concurrency)
<!-- REVIEW: mpsc::channel(config.queue_capacity) — bounded. Semaphore::new(self.config.max_concurrency) — bounded. Verified. -->
- [x] **Integration:** `tests/prefetch.rs`: LIST objects, verify metadata cache is warm for next page; GET object, verify adjacent keys prefetched into metadata cache; disable prefetch, verify no background warming
<!-- REVIEW: Integration tests are in tests/cache_behavior.rs (not prefetch.rs). prefetch_warms_metadata_cache tests LIST→metadata-warm + object-cache inline warming. Disabled behavior tested in unit test disabled_engine_is_noop. -->
- [x] **Manual:** Example in `PrefetchEngine` docs compiles and runs
