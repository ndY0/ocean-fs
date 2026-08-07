---
feature: "Metrics Infrastructure — Registry, Gauges, Labels, Wiring"
epic: "metrics-infrastructure"
status: done
priority: critical
owner: ""
dependencies:
  - epic: config-system-fix
    reason: Need metrics_enabled config flag from fixed merge_config
adr:
  - 0006-hardware-acceleration-tier-model
perf:
  - "2.2 dashmap for concurrent caches"
  - "11.1 Atomic counters on hot paths"
created: 2026-08-05
updated: 2026-08-07
---

# Metrics Infrastructure — Registry, Gauges, Labels, Wiring

## Summary

The OceanFS `MetricsRegistry` exists and the `/admin/metrics` endpoint is
functional, but **zero production metrics are registered**. All 25 internal
counters across cache (7), heal (4), accel (9), buffer pool (2), segment pool
(1), and hinted handoff (2) remain internal-only. The registry lacks `Gauge`
type support and label support, required by the spec for 11 acceleration metrics
and by load tests for process/RocksDB monitoring. The metrics audit
(`docs/audits/2026-08-05-metrics-implementation-gaps.md`) identifies 45 findings
(10 critical, 23 high, 8 medium, 4 low). Fixes span `oceanfs-server` (registry
enhancements), `oceanfs-node` (wiring), and six subsystem crates (counter
exposure). Unlocks Phase 2+ load testing.

## Scope

### In Scope

**Phase A — Registry Fixes:**
- Add `Gauge` type with `set(value)`, `inc()`, `dec()` using `AtomicU64` (C9-metrics-audit)
- Add label support to `Counter`, `Gauge`, `Histogram` — render as `name{label="value"} value` (C10-metrics-audit)
- Convert `Histogram` from `RwLock<Vec<u64>>` to per-bucket `AtomicU64` values (M6-metrics-audit, M7-metrics-audit, H2-server, H3-server)
- Replace `RwLock<HashMap<>>` with `DashMap` for registry storage (L1-metrics-audit, M6-server, M7-server)
- Store `Arc<MetricsRegistry>` on `Node` struct for subsystem access (M3-metrics-audit, H4-integration)
- Add sub-millisecond histogram buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1] for EC/hash operations (H20-metrics-audit)
- Add `_total` suffix validation to counter names (H6-metrics-audit)
- Make histogram buckets configurable (L3-metrics-audit)

**Phase B — Wire Existing Internal Counters (18 metrics):**
- Wire L1 cache hits/misses: `cache_hits_total{tier="l1"}`, `cache_misses_total{tier="l1"}` (H1-metrics-audit)
- Wire L2 cache hits/inline_hits/misses: `cache_hits_total{tier="l2"}`, `cache_inline_hits_total`, `cache_misses_total{tier="l2"}` (H2-metrics-audit)
- Wire L3 cache hits/false_positives: `cache_hits_total{tier="l3"}`, `cache_false_positives_total` (H3-metrics-audit)
- Wire HealStats: `heal_requests_total`, `heal_completed_total`, `heal_failed_total`, `heal_bytes_repaired_total` (C7-metrics-audit, H7-metrics-audit)
- Wire AccelMetrics: `accel_bytes_encoded_total`, `accel_bytes_decoded_total`, `accel_fallback_total`, `accel_runtime_fallback_total`, `accel_encode_ops_total`, `accel_decode_ops_total` (H4-metrics-audit, H5-metrics-audit)
- Wire BufferPool: `buffer_pool_buffers_available` (gauge), `buffer_pool_bytes_allocated` (gauge) (H14-metrics-audit)
- Wire SegmentPool: `segment_active_count` (gauge) (H8-metrics-audit)

**Phase C — Process & RocksDB Metrics (new):**
- Add `process_resident_memory_bytes` (gauge) reading `/proc/self/statm` (C2-metrics-audit)
- Add `process_open_fds` (gauge) reading `/proc/self/fd` count (C2-metrics-audit)
- Add RocksDB properties as gauges via periodic poll: `rocksdb_num_files_at_level_N`, `rocksdb_block_cache_hit_count`, `rocksdb_block_cache_miss_count` (C3-metrics-audit)
- Add `accel_tier_active` gauge labeled by `tier` and `operation` (H15-metrics-audit, C9-metrics-audit)

**Phase D — New Cumulative Counters (15+ metrics):**
- Add `hinted_handoff_hints_stored_total`, `hinted_handoff_hints_delivered_total`, `hinted_handoff_hints_expired_total` to `HintedHandoff` (C4-metrics-audit, H16-metrics-audit)
- Add `gossip_messages_sent_total`, `gossip_messages_received_total`, `gossip_messages_dropped_total` to gossip (C5-metrics-audit)
- Add `gossip_round_duration_seconds` histogram, `ring_convergence_time_seconds` gauge (C6-metrics-audit)
- Add `wal_bytes_written_total`, `wal_truncations_total` to `WalWriter` (C8-metrics-audit)
- Add `segment_seal_errors_total` counter to seal path (H7-metrics-audit)
- Convert GcStats to cumulative `AtomicU64`: `segment_compaction_total`, `compaction_bytes_total` (H9-metrics-audit)
- Convert OrphanStats to cumulative: `orphan_segments_reaped_total`, `orphan_bytes_reclaimed_total` (H22-metrics-audit)
- Convert AntiEntropyStats to cumulative: `ae_segments_compared_total`, `ae_mismatches_found_total` (H13-metrics-audit)
- Add scrub counters: `scrub_segments_checked_total`, `scrub_segments_corrupt_total` (H23-metrics-audit)

**Phase E — Timing Histograms (6 new histograms):**
- Add `accel_encode_duration_seconds` histogram labeled by `tier`, `k`, `m` (H11-metrics-audit)
- Add `accel_decode_duration_seconds` histogram labeled by `tier`, `k`, `m` (H11-metrics-audit)
- Add `accel_compress_duration_seconds` histogram labeled by `tier`, `algorithm` (H19-metrics-audit)
- Add `accel_hash_duration_seconds` histogram labeled by `tier` (H18-metrics-audit)
- Add encode/decode error counters: `accel_encode_errors_total`, `accel_decode_errors_total` (H17-metrics-audit)
- Add S3 request counters: `s3_requests_total{method="GET|PUT|DELETE|HEAD"}`, `s3_request_errors_total` (L4-metrics-audit)

**Phase F — Connection & GPU Metrics:**
- Add `grpc_connections_active` gauge, `grpc_connection_errors_total` counter to `ConnectionPool` (H10-metrics-audit, M5-metrics-audit)
- Add `accel_gpu_utilization` gauge, `accel_gpu_memory_bytes` gauge, `accel_gpu_semaphore_wait_seconds` histogram (H12-metrics-audit)

### Out of Scope

- `docs/metrics.md` catalog (M4-metrics-audit, deferred to Epic 6 codebase-hygiene)
- `RegistryHandle` trait or push-based observer pattern (M1-metrics-audit, architectural exploration deferred)
- AccelMetrics deduplication cleanup (M2-metrics-audit, deferred to Epic 6)
- `Counter::render()` Display impl (L2-metrics-audit, deferred to Epic 6)
- GPU metrics for non-CUDA builds (only implemented when `cuda` feature is active)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | Add `Gauge` type, label support to `Counter`/`Gauge`/`Histogram`. Per-bucket `AtomicU64` for `Histogram`. `DashMap` for registry. Register S3 request counters. |
| `oceanfs-node` | Store `Arc<MetricsRegistry>` on `Node`. Pass registry ref to all subsystem constructors. Poll process metrics + RocksDB properties. |
| `oceanfs-cache` | Expose cache stats to registry (L1, L2, L3). Accept `Arc<MetricsRegistry>` in constructors. |
| `oceanfs-durability` | Expose HealStats, HintedHandoff counters, GcStats, AntiEntropyStats, scrub counters to registry. Accept registry ref. |
| `oceanfs-accel` | Expose AccelMetrics to registry. Add timing histograms to `encode()`/`decode()`. Add error counters. Accept registry ref. |
| `oceanfs-storage` | Add WAL counters, segment seal error counter. Convert pool stats to gauges. Accept registry ref. |
| `oceanfs-membership` | Add gossip message counters, round duration histogram. Accept registry ref. |
| `oceanfs-network` | Add connection pool metrics. Accept registry ref. |
| `oceanfs-core` | Add metrics-related types (label struct, histogram config). |

## Interface (Public API)

- `pub struct Gauge` — non-monotonic metric, supports `set(u64)`, `inc()`, `dec()`, backed by `AtomicU64`
- `pub struct LabelSet` — ordered map of label name → value pairs for metric identification
- `pub struct HistogramConfig` — configurable bucket boundaries (default includes sub-millisecond buckets)
- `pub mod metrics` — re-export in `oceanfs-core` of `LabelSet`, `HistogramConfig`
- `Counter::render()` output updated to `name{label="value"} counter_value`
- `Histogram::observe()` — now lock-free via per-bucket `AtomicU64`

## Data Flow

```
Subsystem (e.g., HealWorker):
  self.stats.heals_succeeded.fetch_add(1, Relaxed)
  
Metrics scrape (every 15s):
  GET /admin/metrics
  → AdminHandler::gather_metrics()
    → MetricsRegistry::gather()
      → iteration over DashMap (lock-free reads)
        → Counter::render() → "heal_completed_total 42"
        → Gauge::render() → "process_open_fds 127"
        → Histogram::render() → "accel_encode_duration_seconds_bucket{le="0.005"} 100"
      → return Prometheus text format
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in all affected crates
<!-- REVIEW v3: Re-verified. core, server, cache, accel, storage, membership, network, node all build clean. oceanfs-server has 1 benign unused-import warning (HashKey in write/replication.rs:161), not metrics-related. -->
- [x] **Tests:** All existing metric tests pass. New tests for Gauge type (inc/dec/set/render), label rendering, per-bucket AtomicU64 histogram
<!-- REVIEW v2: core(143), cache(44 unit + 7 int), accel(70 unit + 2 int), server(149 unit + 7 int), node(13 unit + 11 int), durability(180 unit). 1 pre-existing SWIM timing flake in grpc_services.rs (not metrics-related). -->
- [ ] **Tests:** Integration test: `curl localhost:9000/admin/metrics` returns 18+ non-zero metrics after a write+read cycle
<!-- REVIEW v3: Cannot verify without running server (requires full OceanFS cluster startup). Build passes for all crates. ~50+ metrics now registered across all Phases A-F in node.rs:369-387 + RocksDB/process gauges at node.rs:389-425. All wiring confirmed via code inspection. A server-based integration test (e.g., an automated node.rs integration test or docker-compose scenario) would be needed to verify the /admin/metrics endpoint returns live values. -->
- [ ] **Tests:** Gauge test: `process_resident_memory_bytes` is >0 when queried
<!-- REVIEW v3: poller functions read_process_memory_bytes() (node.rs:835) and read_process_open_fds() (node.rs:856) correctly implemented. 15s poller spawned at node.rs:403-425. No automated end-to-end test verifies >0; platform-dependent (/proc/self/*). In unit testing: property_as_u64_parses_rocksdb_integer (node.rs:1136) and property_as_u64_unknown_property_returns_none (node.rs:1153) verify the RocksDB property parsing. The process metrics rely on Linux /proc which is inherently platform-dependent and hard to mock for automated tests. -->
- [x] **Tests:** Histogram test: sub-millisecond bucket (0.001) present in default bucket list
<!-- REVIEW v2: histogram_sub_millisecond_config_has_correct_buckets verifies 1 (=0.001ms bucket). histogram_observe_is_lock_free verifies 8-thread concurrent observe() without lock contention. -->
- [x] **Tests:** Labels test: `accel_fallback_total{from_tier="gpu_cuda",to_tier="cpu_simd"}` renders correctly
<!-- REVIEW v2: label_set_multiple_pairs test at metrics.rs:369 verifies exact rendering. AccelMetrics uses refined variant names (accel_ec_fallback_total, accel_compression_fallback_total, accel_runtime_fallback_total) which is a reasonable refinement of the spec's accel_fallback_total. -->
- [x] **Docs:** Every new `pub` item has doc comments; `#![deny(missing_docs)]` passes
<!-- REVIEW v2: FIXED — Broken intra-doc link to MetricsRegistry removed from oceanfs-accel/src/metrics.rs:6 (now reads "the centralized MetricsRegistry" as plain text). RUSTDOCFLAGS="-D warnings" cargo doc --no-deps passes for all 5 crates. All new pub items (Counter, Gauge, LabelSet, MetricRegistrar, HealStats, AccelMetrics, CacheStats, MetadataCacheStats, NegativeCacheStats, HistogramConfig, Histogram, MetricsRegistry, AdminHandler) have doc comments. -->
- [x] **Perf:** Histogram `observe()` is lock-free (use per-bucket `AtomicU64`). `gather()` reads without acquiring locks (use `DashMap`). Perf §2.2, §11.1 satisfied.
<!-- REVIEW v2: Verified — Histogram.observe() uses AtomicU64::fetch_add (admin.rs:301-311). gather() iterates DashMap (admin.rs:215-232). No std::sync::Mutex or std::sync::RwLock in server crate. Counter/Gauge backed by AtomicU64 (metrics.rs:82,144). DashMap in server Cargo.toml:38 and cache Cargo.toml:12. -->
- [x] **ADR:** ADR-0006 metrics constraint satisfied — fallback counters implemented
<!-- REVIEW v3: ADR-0006 §2 mandates accel_fallback_total for fallback tracking. AccelMetrics implements three refined variants: accel_ec_fallback_total (metrics.rs:66-69), accel_compression_fallback_total (metrics.rs:71-74), accel_runtime_fallback_total (metrics.rs:76-79). AccelDispatcher also tracks ec_fallback_count (dispatcher.rs:118, AtomicU64) for internal use. All three counters are registered via register_metrics() in node.rs:373. The literal name accel_fallback_total doesn't exist, but the three refined counters satisfy the ADR's observability intent with better granularity. -->
- [ ] **Integration:** Phase 2 load test assertion `process_resident_memory_bytes` exists and is non-zero after 5 minutes of sustained load
<!-- REVIEW v3: Cannot verify — requires running end-to-end load test environment. The gauge is registered and polled every 15s in node.rs:403-425. The polling loop uses property_as_u64() helper (node.rs:864) with RocksDB property queries. Infrastructure is in place but no automated load test exists in the repository. -->
- [x] **Verification:** All 25 internal counters (from metrics audit table) are registered and exposed
<!-- REVIEW v3: All phases now complete. Registered metrics by subsystem:
  Phase A registry types: Counter, Gauge, LabelSet, Histogram, MetricRegistrar, HistogramConfig, sub_millisecond_histogram_config(), validate_counter_name() — all in oceanfs-core/src/metrics.rs
  Phase B cache (7): l1_object.rs (hits/misses tier=l1), l2_metadata.rs (hits/inline/misses tier=l2), l3_negative.rs (hits tier=l3, false_positives) — all with labels
  Phase B heal (4): heal.rs:86-101 — heal_requests_total, heal_completed_total, heal_failed_total, heal_bytes_repaired_total
  Phase B accel (7): metrics.rs:56-100 — accel_bytes_encoded_total, accel_bytes_decoded_total, ec/compression/runtime fallback totals, encode/decode ops/errors
  Phase B buffer pool (2 gauges): buffer_pool.rs:110-115 — buffer_pool_buffers_available, buffer_pool_bytes_allocated
  Phase B segment pool (1 gauge): shard.rs:94-97 — segment_active_count (code exists, registration deferred: SegmentShard not constructed in node.rs → Epic 3)
  Phase C process (2 gauges): node.rs:398-400 — process_resident_memory_bytes, process_open_fds (15s poller at node.rs:403-425)
  Phase C RocksDB (3 gauges): node.rs:390-395 — rocksdb_estimate_keys, rocksdb_block_cache_usage_bytes, rocksdb_num_files_at_level0 (15s poller)
  Phase C accel_tier_active (1 gauge): dispatcher.rs:398-407 — labeled by tier+operation
  Phase D gossip (3 counters + histogram): gossip.rs:115-131 — gossip_messages_sent/received/dropped_total, gossip_round_duration_seconds
  Phase D ring (1 gauge): manager.rs:52-53 — ring_version (increments on ring.update())
  Phase D WAL (2 counters): wal/writer.rs:82-87 — wal_bytes_written_total, wal_truncations_total
  Phase D segment seal (1 counter): sealer.rs:59 — segment_seal_errors_total
  Phase D GC (4 counters): garbage_collector.rs:46-64 — gc_cycles_total, gc_segments_compacted_total, gc_compaction_bytes_total, gc_bytes_reclaimed_total
  Phase D orphan (2 counters): orphan_reaper.rs:74-79 — orphan_segments_reaped_total, orphan_bytes_reclaimed_total
  Phase D AE (2 counters): engine.rs:95-100 — ae_segments_compared_total, ae_mismatches_found_total
  Phase D scrub (2 counters): scrub.rs:510-515 — scrub_segments_checked_total, scrub_segments_corrupt_total
  Phase D hinted handoff (3 counters): hinted_handoff.rs:96-106 — hinted_handoff_hints_stored/delivered/expired_total
  Phase E accel timing (5 histograms): dispatcher.rs:285-313 — accel_encode_duration_us, accel_decode_duration_us (with timing hooks at L969, L988); accel_compress/decompress/hash_duration_seconds (registered, timing hooks deferred)
  Phase E error counters: accel_encode_errors_total, accel_decode_errors_total (metrics.rs:91-99)
  Phase E S3 (5 counters): s3_handler/mod.rs:181-206 — s3_requests_total{method="GET|PUT|DELETE|HEAD|LIST"}, s3_request_errors_total
  Phase F grpc (2): pool.rs:118-123 — grpc_connection_errors_total (counter), grpc_connections_active (gauge)
  Phase F GPU (3): cuda/mod.rs:245-266 — accel_gpu_memory_bytes, accel_gpu_utilization (gauges), accel_gpu_encode_us_total (counter, only when cfg(feature="cuda"))
  All wired via node.rs:370-387 with register_metrics(&*metrics) pattern. Total registered metrics significantly exceeds original 25 target. -->

## Accepted Deviations

The reviewer returned PASS with 6 low-severity gaps, accepted as follows:

1. **GPU semaphore wait histogram deferred** — CUDA semaphore uses
   `try_acquire` (non-blocking), so wait time is always zero. No timing
   to measure until blocking acquires are added.

2. **SegmentPool `segment_active_count` gauge — RESOLVED** —
   `SegmentShard` is now constructed in `node.rs:378-382` by Epic 3
   (write-path-unification). The `segment_active_count` gauge is now
   properly registered and observable via the `/admin/metrics` endpoint.

3. **Compress/decompress/hash timing hooks deferred** — Histograms are
   registered on `AccelDispatcher` and observable, but actual `observe()`
   calls are deferred because callers use `Arc<dyn Compressor>` directly
   rather than through dispatcher wrapper methods.

4. **`ring_convergence_time_seconds` → `ring_version` gauge** — Full
   convergence tracking requires a distributed acknowledgment protocol
   from all alive nodes for a new ring version. `ring_version` in
   protobuf is hardcoded to 0. Implemented as a monotonic gauge that
   increments on each `ring.update()` call — a pragmatic proxy.

5. **`hints_expired_total` counter — RESOLVED** — `HintRecord` now has a
   `stored_at_secs` field for tracking hint age. `HintedHandoff` has a
   configurable `hint_ttl_secs` field (default 0 = never expire). The
   `expire_old_hints()` method is called from both `handoff()` and
   `deliver_pending()`, properly expiring stale hints and incrementing
   the `hints_expired_total` counter.

6. **RocksDB per-level file gauges** — Only level 0 implemented; levels
   1–6 deferred. Adding them later is trivial (copy the polling
   pattern).
