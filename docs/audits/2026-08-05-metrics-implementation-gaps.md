---
audit_date: 2026-08-05
scope: targeted
target_crates: all (oceanfs-server, oceanfs-accel, oceanfs-cache, oceanfs-durability, oceanfs-storage, oceanfs-membership, oceanfs-node)
severity_counts:
  critical: 10
  high: 23
  medium: 8
  low: 4
---

# Audit Report: Metrics Implementation Gaps

## Summary

The OceanFS codebase has a `MetricsRegistry` and `/admin/metrics` endpoint
implemented in `oceanfs-server/src/admin.rs`, but **zero production metrics are
registered into it**. All registry usage is confined to test code. Several
subsystems maintain their own internal atomic counters or stats structs (e.g.,
`HealStats`, `AccelMetrics`, cache stats), but none are wired into the
Prometheus exposition endpoint. The spec (§9.8.1) defines 11 acceleration
metrics — none are exposed. The load test campaign requires ~30 metrics across
process health, RocksDB, gossip, healing, caching, and WAL subsystems — the
vast majority do not exist in any form. **Phase 2+ load testing is blocked**
until these gaps are addressed.

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `crates/oceanfs-server/src/admin.rs:370-371` (node.rs) | **Zero production metrics registered in MetricsRegistry.** The registry is instantiated as `Arc::new(MetricsRegistry::new())` but never populated. All `.counter()` / `.histogram()` calls exist only in `#[test]` code. `/admin/metrics` returns an empty body. | Wire subsystem stats into the registry. At minimum, register the cache hits/misses, heal attempts/successes/failures, hint counters, and acceleration fallback counters. |
| C2 | Multiple | **No process-level metrics.** `process_resident_memory_bytes`, `process_open_fds` are required by Phase 2 load tests for leak detection. Neither exists. | Add a system metrics module that reads `/proc/self/statm` and `/proc/self/fd` (Linux) or equivalent, exposing them as gauges. |
| C3 | `crates/oceanfs-storage/src/metadata/store.rs:57-62` | **No RocksDB metrics exposed.** `rocksdb_num_files_at_level_0` (and levels 1..N), `rocksdb_block_cache_hit_count`, `rocksdb_block_cache_miss_count` are critical for detecting write stalls and cache pressure. RocksDB's C++ API exposes these via `rocksdb::perf_context` or `get_property`, but they are not surfaced. | Add a periodic poll (e.g., every 10s) that queries RocksDB properties and registers the results as gauges/counters in the MetricsRegistry. |
| C4 | `crates/oceanfs-durability/src/hinted_handoff.rs` | **No hinted handoff counters.** `hinted_handoff_hints_stored_total`, `delivered_total`, `expired_total` do not exist. Only `pending_count()` (live count) is available. Phase 3-4 load tests require these for verifying hint delivery after node churn. | Add `AtomicU64` counters for stored/delivered/expired hints. Increment on each operation. Wire to MetricsRegistry. |
| C5 | `crates/oceanfs-membership/src/gossip.rs` | **No gossip message counters.** `gossip_messages_sent_total`, `received_total`, `dropped_total` do not exist. Phases 3-5 require these to assert no message loss during churn and to measure bandwidth per node. | Add counters in the gossip protocol for push/pull/ack messages. Wire to MetricsRegistry. |
| C6 | `crates/oceanfs-membership/` | **No gossip round duration or ring convergence metrics.** `gossip_round_duration_seconds`, `ring_convergence_time_seconds` do not exist. Phase 3 tests need to assert convergence within bounded rounds. | Add timing in the gossip loop and membership convergence logic. |
| C7 | `crates/oceanfs-durability/src/heal/worker.rs:112` | **HealStats exist but are not wired to the metrics registry.** `HealStats` has `heals_attempted`, `heals_succeeded`, `heals_failed`, `bytes_repaired` (all AtomicU64). Phase 3-4 load tests require `heal_requests_total`, `completed_total`, `failed_total`. | Wire `HealStats` counters into the MetricsRegistry. The data exists — it just isn't exposed. |
| C8 | `crates/oceanfs-storage/` | **No WAL metrics.** `wal_bytes_written_total`, `wal_truncations_total` do not exist. Phase 2 tests need WAL growth monitoring. | Add counters to `WalWriter` for bytes appended and truncation events. |
| C9 | `crates/oceanfs-server/src/admin.rs:44-46` | **MetricsRegistry does not support gauges.** The registry has `Counter` (monotonic, AtomicU64) and `Histogram` but no `Gauge` type. Many spec'd and load-test-required metrics are gauges: `process_resident_memory_bytes`, `process_open_fds`, `segment_active_count`, `buffer_pool_allocated_bytes`, `accel_tier_active`, `rocksdb_num_files_at_level_*`. | Add a `Gauge` type with `set(value)` and `AtomicU64`-based `inc`/`dec` to the registry. |
| C10 | `crates/oceanfs-server/src/admin.rs:60-64` | **MetricsRegistry does not support labeled metrics.** The `counter()` method accepts only a `name` and `help` — no label support. The spec requires labeled metrics (e.g., `accel_fallback_total{from_tier="gpu_cuda",to_tier="cpu_simd"}`). The current `Counter::render()` emits plain `name value`, not `name{label="value"} value`. | Add label support to `Counter`, `Gauge`, and `Histogram`. Labels are essential for distinguishing per-tier, per-operation, per-level metrics. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `crates/oceanfs-cache/src/l1_object.rs:267` | **L1 cache hits/misses not wired to registry.** `CacheStats` has `hits` and `misses` (AtomicU64), exposed via `stats()` method. The admin handler's `GET /admin/caches` reads these but they are not registered as Prometheus metrics at `/admin/metrics`. | Register `cache_hits_total{tier="l1"}` and `cache_misses_total{tier="l1"}` in the MetricsRegistry. |
| H2 | `crates/oceanfs-cache/src/l2_metadata.rs:264` | **L2 metadata cache stats not wired.** `MetadataCacheStats` has `hits`, `inline_hits`, `misses` (AtomicU64). | Wire as `cache_hits_total{tier="l2"}`, `cache_inline_hits_total`, `cache_misses_total{tier="l2"}`. |
| H3 | `crates/oceanfs-cache/src/l3_negative.rs:231` | **L3 negative cache stats not wired.** `NegativeCacheStats` has `hits`, `false_positives` (AtomicU64). | Wire as `cache_hits_total{tier="l3"}`, `cache_false_positives_total`. |
| H4 | `crates/oceanfs-accel/src/metrics.rs:27` | **AccelMetrics not wired to registry.** Has `bytes_encoded`, `bytes_decoded`, `ec_fallback_total`, `compression_fallback_total`, `runtime_fallback_total`, `encode_ops_total`, `decode_ops_total` (all AtomicU64). Accessible via `AccelDispatcher::metrics()` but not in `/admin/metrics`. | Register the AccelMetrics counters in the MetricsRegistry at node startup. Wire them in `Node::start()` after dispatcher construction. |
| H5 | `crates/oceanfs-accel/src/dispatcher.rs:272` | **EC fallback counter exists but not exposed.** `ec_fallback_count` is incremented correctly in `resolve_ec_tier()`, but only queryable via `AccelDispatcher::ec_fallback_count()`. Not pushed to Prometheus. | This is the `accel_fallback_total` counter the spec requires. Wire it. |
| H6 | `crates/oceanfs-server/src/admin.rs:105` | **`Counter` type has no `_total` suffix enforcement.** Prometheus conventions expect counter names to end in `_total`. The registry does not validate this. | Add a debug_assert or gentler warning. Not critical but improves operator experience. |
| H7 | Whole codebase | **No `segment_seal_errors_total` counter.** The spec and load tests require this. The `SegmentSealer` and `SegmentPool` do not track seal errors in any counter. | Add an error counter that increments on seal failure. |
| H8 | `crates/oceanfs-storage/src/segment/pool.rs:228` | **`active_count()` exists but no `segment_active_count` gauge.** Phase 2 tests need to monitor segment pool health. | Register a gauge updated on each pool state change. |
| H9 | `crates/oceanfs-durability/src/gc/stats.rs` | **GcStats are per-cycle values, not cumulative counters.** `segments_scanned`, `segments_compacted`, `bytes_reclaimed` are set per cycle and returned — not accumulated. Load tests need `segment_compaction_total` and `compaction_bytes_total` as cumulative counters. | Add cumulative `AtomicU64` counters on `GarbageCollector`. |
| H10 | Whole codebase | **No gRPC connection metrics.** `grpc_connections_active`, `grpc_connection_errors_total` do not exist. Phase 5 scaling tests need connection counts. | Add counters in `ConnectionPool` for active connections and connection errors. |
| H11 | `crates/oceanfs-accel/` | **Spec §9.8.1 metrics: `accel_encode_duration_seconds` not implemented.** The spec requires a histogram labeled by `tier`, `k`, `m`. No histogram is created. AccelMetrics only tracks byte counts and op counts — not latencies. | Add timing in `AccelDispatcher::encode()` and `decode()` wrapper methods, recording to a histogram. |
| H12 | `crates/oceanfs-accel/` | **Spec §9.8.1: `accel_gpu_utilization`, `accel_gpu_memory_bytes`, `accel_gpu_semaphore_wait_seconds` not implemented.** GPU metrics require NVML/nvapi bindings or CUDA management API calls — none exist. | For GPU-capable builds, add CUDA device queries via `cudarc` or `nvml-wrapper` crate. |
| H13 | `crates/oceanfs-durability/src/anti_entropy/engine.rs:769` | **AntiEntropyStats is per-cycle plain struct, not wired.** `segments_compared`, `mismatches_found`, `leaves_repaired`. Not atomic, not cumulative, not registered. | Convert to cumulative AtomicU64 counters. Wire to registry. |
| H14 | `crates/oceanfs-storage/src/buffer_pool.rs` | **BufferPool has no stats/metrics for capacity.** `buffer_pool_allocated_bytes`, `buffer_pool_available_bytes` are needed for Phase 2. Only `free_count()`, `chunk_size()`, `max_buffers()` queries exist — no registered metrics. | Add gauge metrics: `buffer_pool_buffers_available`, `buffer_pool_bytes_allocated`. |
| H15 | Whole codebase | **No `accel_tier_active` gauge.** Spec §9.8.1 requires this labeled by `tier` and `operation`. The `/admin/acceleration` endpoint returns tier info as JSON but not as a Prometheus gauge. | Register a gauge set at startup and update on fallback/recovery events. |
| H16 | Whole codebase | **`hinted_handoff_hints_expired_total` has no concept of "expired".** The `HintedHandoff` struct has no TTL-based expiration mechanism. Hints stay forever until delivered or the node is removed. Load tests need this for detecting lost hints. | Add a background task that expires hints older than `hint_ttl_sec` and increments an `expired_total` counter. |
| H17 | `crates/oceanfs-accel/src/dispatcher.rs:127` | **`AccelDispatcher` implements `Encoder`/`Decoder` but records metrics only on success** (lines 875-877, 892-894). Failed encode/decode operations are not counted. | Add `encode_errors_total` and `decode_errors_total` for observability. |
| H18 | Whole codebase | **No `accel_hash_duration_seconds` metric.** Spec §9.8.1 requires it. BLAKE3 hashing is delegated to the `blake3` crate with no timing wrapper. | Add a timing layer in the hash subsystem. |
| H19 | `crates/oceanfs-accel/src/compressor.rs` | **No compression error or duration metrics.** Spec §9.8.1 requires `accel_compress_duration_seconds`. Compressor has all the internal plumbing but no timing or error counters. | Add compression duration histogram and error counter. |
| H20 | `crates/oceanfs-server/src/admin.rs:165` | **Histogram has hardcoded, coarse buckets:** [1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000] milliseconds. These are too coarse for sub-millisecond operations (EC encode, hash). No sub-millisecond bucket exists. | Add configurable histogram buckets. Default to include [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1] for microsecond-scale latencies. |
| H21 | `crates/oceanfs-server/src/admin.rs:151` | **Histogram observes only `u64` values.** This limits precision — sub-millisecond measurements must be scaled, and nanosecond precision is impossible. | Consider `f64` observe or `Duration`-based API. |
| H22 | `crates/oceanfs-durability/src/gc/orphan_reaper.rs:20` | **OrphanStats are per-cycle plain values, not cumulative.** Load tests need visibility into GC reclaim activity. | Add cumulative AtomicU64 counters on `OrphanReaper`. |
| H23 | Whole codebase | **No scrub completion or corruption metrics.** The scrub report is logged but no counters track `scrub_segments_checked_total`, `scrub_segments_corrupt_total`. Phase 3-4 tests need to verify scrub detects corruption. | Add counters to `ScrubCoordinator`/scrub report flow. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `crates/oceanfs-server/src/admin.rs:39` | **Registry lives in the server crate but subsystems register independently.** There's no `RegistryHandle` trait or injection mechanism. Each subsystem would need a direct ref to `Arc<MetricsRegistry>`, which increases coupling. | Create a `MetricsRegistryHandle` trait in `oceanfs-core` or design a push-based observer pattern where subsystems emit events and the registry subscribes. |
| M2 | `crates/oceanfs-accel/src/metrics.rs` | **AccelMetrics has duplicated counters** with the dispatcher (`ec_fallback_total` exists in both `AccelMetrics` and `ec_fallback_count` on dispatcher). There's no single source of truth. | Consolidate: make the dispatcher the sole owner, or make AccelMetrics the sole owner. |
| M3 | `crates/oceanfs-node/src/node.rs:370` | **MetricsRegistry is created in `start()` but not stored in `Node` struct.** After construction, the registry is held only by the `AdminHandler` in the axum router. There's no way for background tasks (GC, heal, etc.) to register metrics post-startup unless they received the registry ref during construction. | Store `metrics: Arc<MetricsRegistry>` in the `Node` struct and pass it to all subsystem constructors. |
| M4 | Whole codebase | **No metrics documentation in crate-level docs.** The 11 spec'd metrics from §9.8.1 are listed only in the spec — there's no `metrics.md` or inline documentation describing what metrics exist and what they mean for operators. | Add a `docs/metrics.md` cataloging every exposed metric with type, labels, and interpretation guidance. |
| M5 | `crates/oceanfs-network/src/pool.rs` | **ConnectionPool has no visibility into pool size or utilization.** `grpc_connections_active` and `grpc_connection_errors_total` are expected by Phase 5. | Add counters for active connections, pooled channels, and connection error events. |
| M6 | `crates/oceanfs-server/src/admin.rs:179` | **Histogram uses `RwLock<Vec<u64>>` for buckets** — every `observe()` acquires a write lock on the bucket vector. This creates contention on the hot path. Perf guideline §2.5 says "use lock-free structures for hot paths." | Use per-bucket `AtomicU64` values instead of a lock-protected vector. |
| M7 | `crates/oceanfs-server/src/admin.rs:81-88` | **`gather()` holds read locks while iterating all metrics.** If a hot path is concurrently writing a histogram (acquiring a write lock), gather may stall the hot path. | Use `AtomicU64`-per-bucket approach to avoid lock contention entirely. |
| M8 | `crates/oceanfs-durability/src/heal/worker.rs:76` | **HealStats is an `Arc<HealStats>` held only by HealWorker.** No other code can read heal stats. `stats()` method returns a ref but callers must have access to the HealWorker. | Expose HealStats via the MetricsRegistry or make it a globally-registered counter set. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `crates/oceanfs-server/src/admin.rs:45-46` | **`RwLock<HashMap<...>>` for registry storage.** A `DashMap` would eliminate lock contention on metric registration (which happens at startup) while allowing concurrent reads for `gather()`. | Switch to `DashMap<String, Arc<Counter>>` for the registry. |
| L2 | `crates/oceanfs-server/src/admin.rs:134` | **`Counter::render()` allocates a new String every time.** On a metrics scrape (every 10-30s), this is negligible. But a `Display`-style write to a `fmt::Formatter` would be more idiomatic. | Implement `Display` for `Counter`/`Histogram` to write directly to the gather buffer. |
| L3 | `crates/oceanfs-server/src/admin.rs:163` | **Histogram bucket bounds are `Vec<u64>` with hardcoded defaults in `new()`.** No way to configure buckets per histogram. | Accept bucket bounds as a parameter to `histogram()` or via config. |
| L4 | Whole codebase | **No metric for S3 request rate/error rate.** `s3_requests_total{method="GET|PUT|DELETE|HEAD"}`, `s3_request_errors_total` are standard for S3-compatible systems. Not spec'd, but useful for Phase 5 load tests. | Add request counters in the S3 handler or middleware layer. |

---

## Implemented Metrics (Internal Only — NOT Exposed at /admin/metrics)

These internal counters/stats exist but are **not registered** in the Prometheus `MetricsRegistry`. They are accessible only via Rust method calls (e.g., `stats()`, `metrics()`, `getter` methods), not via `/admin/metrics`.

| Metric (Internal Name) | Type | Labels | Location | Prometheus-Ready? |
|---|---|---|---|---|
| `CacheStats::hits` | AtomicU64 counter | none | `oceanfs-cache/src/l1_object.rs:25` | No |
| `CacheStats::misses` | AtomicU64 counter | none | `oceanfs-cache/src/l1_object.rs:27` | No |
| `MetadataCacheStats::hits` | AtomicU64 counter | none | `oceanfs-cache/src/l2_metadata.rs:25` | No |
| `MetadataCacheStats::inline_hits` | AtomicU64 counter | none | `oceanfs-cache/src/l2_metadata.rs:27` | No |
| `MetadataCacheStats::misses` | AtomicU64 counter | none | `oceanfs-cache/src/l2_metadata.rs:29` | No |
| `NegativeCacheStats::hits` | AtomicU64 counter | none | `oceanfs-cache/src/l3_negative.rs:49` | No |
| `NegativeCacheStats::false_positives` | AtomicU64 counter | none | `oceanfs-cache/src/l3_negative.rs:51` | No |
| `HealStats::heals_attempted` | AtomicU64 counter | none | `oceanfs-core/src/types/heal.rs:69` | No |
| `HealStats::heals_succeeded` | AtomicU64 counter | none | `oceanfs-core/src/types/heal.rs:71` | No |
| `HealStats::heals_failed` | AtomicU64 counter | none | `oceanfs-core/src/types/heal.rs:73` | No |
| `HealStats::bytes_repaired` | AtomicU64 counter | none | `oceanfs-core/src/types/heal.rs:75` | No |
| `AccelMetrics::bytes_encoded` | AtomicU64 counter | none | `oceanfs-accel/src/metrics.rs:29` | No |
| `AccelMetrics::bytes_decoded` | AtomicU64 counter | none | `oceanfs-accel/src/metrics.rs:31` | No |
| `AccelMetrics::ec_fallback_total` | AtomicU64 counter | none | `oceanfs-accel/src/metrics.rs:33` | No |
| `AccelMetrics::compression_fallback_total` | AtomicU64 counter | none | `oceanfs-accel/src/metrics.rs:35` | No |
| `AccelMetrics::runtime_fallback_total` | AtomicU64 counter | none | `oceanfs-accel/src/metrics.rs:37` | No |
| `AccelMetrics::encode_ops_total` | AtomicU64 counter | none | `oceanfs-accel/src/metrics.rs:39` | No |
| `AccelMetrics::decode_ops_total` | AtomicU64 counter | none | `oceanfs-accel/src/metrics.rs:41` | No |
| `AccelDispatcher::ec_fallback_count` | AtomicU64 counter | none | `oceanfs-accel/src/dispatcher.rs:115` | No |
| `AccelDispatcher::compression_fallback_count` | AtomicU64 counter | none | `oceanfs-accel/src/dispatcher.rs:112` | No |
| `SegmentPool::active_count()` | method (live query) | none | `oceanfs-storage/src/segment/pool.rs:228` | No |
| `BufferPool::free_count()` | method (live query) | none | `oceanfs-storage/src/buffer_pool.rs:85` | No |
| `BufferPool::total_created()` | method (live query) | none | `oceanfs-storage/src/buffer_pool.rs:100` | No |
| `HintedHandoff::pending_count()` | method (live query) | none | `oceanfs-durability/src/hinted_handoff.rs:223` | No |
| `HintedHandoff::total_pending_count()` | method (live query) | none | `oceanfs-durability/src/hinted_handoff.rs:229` | No |

**Total internal metrics tracked:** 25 (across caches, heal, accel, buffer pool, hinted handoff, segment pool)
**Total exposed at /admin/metrics:** 0

---

## Spec Gaps — Metrics in the Spec but NOT Implemented

From docs/spec.md §9.8.1. All 11 spec'd metrics are missing or only partially tracked.

| Spec Metric | Type | Labels | Status |
|---|---|---|---|
| `accel_tier_active` | Gauge | `tier`, `operation` | **Not implemented.** No gauge type in registry. Tier info available only via JSON at `/admin/acceleration`. |
| `accel_encode_duration_seconds` | Histogram | `tier`, `k`, `m` | **Not implemented.** No timing in encode path. |
| `accel_decode_duration_seconds` | Histogram | `tier`, `k`, `m` | **Not implemented.** No timing in decode path. |
| `accel_bytes_processed_total` | Counter | `tier`, `operation` | **Partial.** `AccelMetrics` tracks bytes_encoded/decoded but not wired, not labeled by tier. |
| `accel_fallback_total` | Counter | `from_tier`, `to_tier` | **Partial.** `AccelDispatcher::ec_fallback_count` exists but not wired, not labeled. |
| `accel_runtime_fallback_total` | Counter | `from_tier`, `to_tier`, `reason` | **Partial.** `AccelMetrics::runtime_fallback_total` exists but not wired, not labeled. |
| `accel_gpu_utilization` | Gauge | `device` | **Not implemented.** No GPU monitoring. |
| `accel_gpu_memory_bytes` | Gauge | `device`, `kind` | **Not implemented.** No GPU memory tracking. |
| `accel_gpu_semaphore_wait_seconds` | Histogram | `device` | **Not implemented.** |
| `accel_compress_duration_seconds` | Histogram | `tier`, `algorithm` | **Not implemented.** |
| `accel_hash_duration_seconds` | Histogram | `tier` | **Not implemented.** |

---

## Load Test Gaps — Metrics Required by Load Tests but NOT Available

From `docs/brainstorm/load-test-campaign.md` §8.2 and per-phase requirements.

### Critical (block Phase 2+)

| Metric | Needed By | Status |
|---|---|---|
| `process_resident_memory_bytes` | Phase 2 (leak detection) | **Not implemented.** No system metrics module. |
| `process_open_fds` | Phase 2 (FD leak) | **Not implemented.** No FD counting. |
| `rocksdb_num_files_at_level_0` | Phase 2 (write stall) | **Not implemented.** RocksDB properties not queried. |
| `rocksdb_num_files_at_level_1..N` | Phase 2 | **Not implemented.** |
| `segment_seal_errors_total` | Phase 1-4 | **Not implemented.** No error counter in seal path. |
| `gossip_messages_dropped_total` | Phase 3-5 | **Not implemented.** No gossip counters. |
| `hinted_handoff_hints_stored_total` | Phase 3-4 (churn) | **Not implemented.** Only live count, no cumulative. |
| `hinted_handoff_hints_delivered_total` | Phase 3-4 | **Not implemented.** |
| `hinted_handoff_hints_expired_total` | Phase 3-4 | **Not implemented.** No TTL-based expiration. |
| `heal_requests_total` | Phase 3-4 | **Internal only.** HealStats exists but not wired. |
| `heal_requests_completed_total` | Phase 3-4 | **Internal only.** HealStats exists but not wired. |
| `heal_requests_failed_total` | Phase 3-4 | **Internal only.** HealStats exists but not wired. |
| `accel_fallback_total` | Phase 1-4 | **Internal only.** Dispatcher counter not wired. |
| `accel_runtime_fallback_total` | Phase 2-4 | **Internal only.** AccelMetrics not wired. |

### High Priority

| Metric | Needed By | Status |
|---|---|---|
| `rocksdb_block_cache_hit_count` | Phase 2 | **Not implemented.** |
| `rocksdb_block_cache_miss_count` | Phase 2 | **Not implemented.** |
| `segment_active_count` | Phase 2 | **Live query only.** Not a Prometheus gauge. |
| `gossip_messages_sent_total` | Phase 3-5 | **Not implemented.** |
| `gossip_messages_received_total` | Phase 3-5 | **Not implemented.** |
| `gossip_round_duration_seconds` | Phase 3-5 | **Not implemented.** |
| `ring_convergence_time_seconds` | Phase 3-5 | **Not implemented.** |
| `cache_hits_total{tier="l1"}` | Phase 2 | **Internal only.** CacheStats not wired. |
| `cache_misses_total{tier="l1"}` | Phase 2 | **Internal only.** |
| `cache_hits_total{tier="l2"}` | Phase 2 | **Internal only.** |
| `cache_misses_total{tier="l2"}` | Phase 2 | **Internal only.** |
| `cache_hits_total{tier="l3"}` | Phase 2 | **Internal only.** |
| `cache_misses_total{tier="l3"}` | Phase 2 | **Internal only.** |
| `segment_compaction_total` | Phase 2 | **Per-cycle value, not cumulative.** GcStats not cumulative. |
| `compaction_bytes_total` | Phase 2 | **Per-cycle value, not cumulative.** |
| `wal_bytes_written_total` | Phase 2 | **Not implemented.** |
| `wal_truncations_total` | Phase 2 | **Not implemented.** |
| `grpc_connections_active` | Phase 5 | **Not implemented.** |
| `grpc_connection_errors_total` | Phase 5 | **Not implemented.** |

### Medium Priority

| Metric | Needed By | Status |
|---|---|---|
| `buffer_pool_allocated_bytes` | Phase 2 | **Not implemented.** Gauge needed. |
| `buffer_pool_available_bytes` | Phase 2 | **Not implemented.** Gauge needed. |

---

## Coupling Hotspots

| Symbol | Crate | In-Degree | Risk |
|---|---|---|---|
| `MetricsRegistry` | oceanfs-server | **0** (no producers) | Critical — empty registry means all metrics are missing. |

---

## Dependency Graph

The `MetricsRegistry` lives in `oceanfs-server` but needs metrics from:
- `oceanfs-accel` (11 spec'd metrics)
- `oceanfs-cache` (L1/L2/L3 stats)
- `oceanfs-durability` (heal, GC, anti-entropy, hinted handoff, scrub)
- `oceanfs-storage` (RocksDB, WAL, segment pool, buffer pool)
- `oceanfs-membership` (gossip, ring convergence)

Per architecture.md's DAG constraint, `oceanfs-server` depends on `oceanfs-storage`, `oceanfs-durability`, `oceanfs-cache`, `oceanfs-accel`, and `oceanfs-membership` — which is valid. But the current architecture requires passing `Arc<MetricsRegistry>` to every subsystem constructor, which creates a spiderweb of registry references. Consider a push-based observer pattern or a global lazy-static registry to reduce coupling.

---

## ADR Compliance

| ADR | Status | Notes |
|---|---|---|
| ADR-0006 §2 (Fallback Chain) | ⚠️ Partial | Fallback logging works. `accel_fallback_total` counter exists internally on dispatcher but is not wired to Prometheus. Missing labels `from_tier`/`to_tier`. |
| ADR-0006 §9.8.1 (Metrics) | ❌ Not compliant | All 11 spec'd metrics are missing from `/admin/metrics`. |

---

## Test Coverage

| Crate | Public Symbols with Metrics | Exposed at /admin/metrics | Coverage |
|---|---|---|---|
| `oceanfs-server` (MetricsRegistry) | Counter, Histogram structs | 0 | 0% — no production registrations |
| `oceanfs-accel` (AccelMetrics) | 7 counters | 0 | 0% — internal only |
| `oceanfs-accel` (Dispatcher) | 2 fallback counters | 0 | 0% |
| `oceanfs-cache` | 7 cache stat fields | 0 | 0% |
| `oceanfs-durability` (HealStats) | 4 counters | 0 | 0% |
| `oceanfs-durability` (GcStats, OrphanStats, AntiEntropyStats) | ~12 fields | 0 | 0% |
| `oceanfs-storage` (BufferPool, SegmentPool) | 5 query methods | 0 | 0% |
| `oceanfs-membership` (gossip) | 0 | 0 | 0% — nothing exists |

---

## Recommendations

### Immediate (unblocks Phase 1-2 load testing)

1. **Add `Gauge` type to MetricsRegistry** (C9). Required for all non-counter metrics.
2. **Add label support to Counter/Gauge/Histogram** (C10). Required for per-tier/per-level distinctions.
3. **Wire existing internal counters to the registry:**
   - Cache hits/misses per tier (H1-H3) — data already exists
   - HealStats (H7/C7) — data already exists
   - AccelMetrics fallback counters (H4-H5) — data already exists
4. **Add process-level metrics** (C2): `process_resident_memory_bytes`, `process_open_fds`.
5. **Add RocksDB metrics** (C3): query `rocksdb` properties and expose as gauges.

### Short-term (unblocks Phase 3-4)

6. **Add HintedHandoff cumulative counters** (C4): stored/delivered/expired.
7. **Add gossip message counters** (C5-C6): sent/received/dropped, round duration.
8. **Add segment seal error counter** (H7).
9. **Add WAL bytes/truncation counters** (C8/H16).
10. **Add timing histograms for EC encode/decode** (H11).

### Medium-term (unblocks Phase 5+)

11. **Add gRPC connection metrics** (H10/M5).
12. **Add per-bucket `AtomicU64` histogram buckets** (M6-M7) to eliminate lock contention on the hot path.
13. **Add GPU metrics** (H12) for CUDA-enabled builds.
14. **Create `docs/metrics.md`** (M4) cataloging all exposed metrics.

### Architectural

15. **Store `Arc<MetricsRegistry>` on `Node` struct** (M3) so background tasks can register metrics post-startup.
16. **Consider a push-based observer pattern** (M1) to reduce registry coupling across 7 crates.

---

## Blocking Assessment

**Phase 1 (concurrency testing):** Partially blocked. TSAN-based testing can proceed without metrics. But the load tests' assertion `no accel_fallback_total increments` requires at minimum the acceleration fallback counter to be wired.

**Phase 2 (sustained load testing):** **Fully blocked.** Eight critical metrics are missing: process memory/FDs, RocksDB level counts, WAL metrics, segment seal errors. Without these, the test cannot detect memory leaks, FD leaks, write stalls, or WAL growth.

**Phase 3 (cluster churn):** **Fully blocked.** Requires gossip counters, hinted handoff counters, heal counters, ring convergence timing — none of which exist.

**Phase 4 (degraded mode):** **Fully blocked.** Depends on Phase 3 metrics plus heal completion tracking and hint expiration.

**Phase 5 (scale testing):** **Fully blocked.** Requires gRPC connection metrics and cluster-wide counters for hotspotting detection.

### Summary of Remediation Effort

- **Existing data needing wiring only:** ~18 internal counters (cache, heal, accel) — low effort
- **New counters needing implementation:** ~15 (hinted handoff, gossip, WAL, gRPC, seal errors, compaction) — medium effort
- **New gauge metrics needing implementation:** ~8 (process, RocksDB, segment pool, buffer pool, GPU) — medium effort
- **New histogram metrics:** ~6 (EC encode/decode, compress, hash, gossip round, GPU semaphore) — medium-high effort
- **Registry enhancements (gauge, labels):** 2 structural changes — medium effort
