---
feature: "Metrics Infrastructure — Registry, Gauges, Labels, Wiring"
epic: "metrics-infrastructure"
status: proposed
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
updated: 2026-08-05
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

- [ ] **Code:** `cargo build --all-targets` succeeds in all affected crates
- [ ] **Tests:** All existing metric tests pass. New tests for Gauge type (inc/dec/set/render), label rendering, per-bucket AtomicU64 histogram
- [ ] **Tests:** Integration test: `curl localhost:9000/admin/metrics` returns 18+ non-zero metrics after a write+read cycle
- [ ] **Tests:** Gauge test: `process_resident_memory_bytes` is >0 when queried
- [ ] **Tests:** Histogram test: sub-millisecond bucket (0.001) present in default bucket list
- [ ] **Tests:** Labels test: `accel_fallback_total{from_tier="gpu_cuda",to_tier="cpu_simd"}` renders correctly
- [ ] **Docs:** Every new `pub` item has doc comments; `#![deny(missing_docs)]` passes
- [ ] **Perf:** Histogram `observe()` is lock-free (use per-bucket `AtomicU64`). `gather()` reads without acquiring locks (use `DashMap`). Perf §2.2, §11.1 satisfied.
- [ ] **ADR:** ADR-0006 §9.8.1 metrics spec satisfied — all 11 acceleration metrics exposed at `/admin/metrics`
- [ ] **Integration:** Phase 2 load test assertion `process_resident_memory_bytes` exists and is non-zero after 5 minutes of sustained load
- [ ] **Verification:** All 25 internal counters (from metrics audit table) are registered and exposed
