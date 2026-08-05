# Load Test Metrics — Requirements & Implementation Gaps

**Author:** Brainstorm Agent (Architect)
**Date:** 2026-08-05
**Context:** Detailed catalog of every metric the phased load test campaign needs,
mapped against what exists in the codebase. Based on an audit of the full
workspace (14 crates) conducted 2026-08-05.
**References:** `load-test-campaign.md`, `load-test-framework.md`,
`docs/spec.md` §9.8.1, audit report at `docs/audits/2026-08-05-metrics-implementation-gaps.md`.

---

## 1. Executive Summary

**The situation is critical: zero production metrics are exposed at
`/admin/metrics`.** The `MetricsRegistry` in `oceanfs-server/src/admin.rs`
exists and is fully functional (Prometheus text format, counter and histogram
types), but it is constructed empty and never populated. All 25 internal
counters tracked by subsystems (caches, heal, accel, buffer pool, hinted
handoff) are accessible only via Rust method calls — not via the Prometheus
endpoint.

**Phase 2+ load testing is fully blocked until this is remediated.** Phase 1
(concurrency + TSAN) can proceed with minimal metrics, but every subsequent
phase requires metrics-based assertions for leak detection, protocol health,
and failure verification.

---

## 2. What Exists (Internal Only)

The following internal counters/stats are tracked by subsystems but are **not
registered** in the Prometheus `MetricsRegistry`. They are accessible only via
Rust method calls.

### 2.1 Cache Subsystem (L1/L2/L3)

| Internal Name | Type | Location |
|---|---|---|
| `CacheStats::hits` | `AtomicU64` | `oceanfs-cache/src/l1_object.rs:25` |
| `CacheStats::misses` | `AtomicU64` | `oceanfs-cache/src/l1_object.rs:27` |
| `MetadataCacheStats::hits` | `AtomicU64` | `oceanfs-cache/src/l2_metadata.rs:25` |
| `MetadataCacheStats::inline_hits` | `AtomicU64` | `oceanfs-cache/src/l2_metadata.rs:27` |
| `MetadataCacheStats::misses` | `AtomicU64` | `oceanfs-cache/src/l2_metadata.rs:29` |
| `NegativeCacheStats::hits` | `AtomicU64` | `oceanfs-cache/src/l3_negative.rs:49` |
| `NegativeCacheStats::false_positives` | `AtomicU64` | `oceanfs-cache/src/l3_negative.rs:51` |

### 2.2 Heal Subsystem

| Internal Name | Type | Location |
|---|---|---|
| `HealStats::heals_attempted` | `AtomicU64` | `oceanfs-core/src/types/heal.rs:69` |
| `HealStats::heals_succeeded` | `AtomicU64` | `oceanfs-core/src/types/heal.rs:71` |
| `HealStats::heals_failed` | `AtomicU64` | `oceanfs-core/src/types/heal.rs:73` |
| `HealStats::bytes_repaired` | `AtomicU64` | `oceanfs-core/src/types/heal.rs:75` |

### 2.3 Acceleration Subsystem

| Internal Name | Type | Location |
|---|---|---|
| `AccelMetrics::bytes_encoded` | `AtomicU64` | `oceanfs-accel/src/metrics.rs:29` |
| `AccelMetrics::bytes_decoded` | `AtomicU64` | `oceanfs-accel/src/metrics.rs:31` |
| `AccelMetrics::ec_fallback_total` | `AtomicU64` | `oceanfs-accel/src/metrics.rs:33` |
| `AccelMetrics::compression_fallback_total` | `AtomicU64` | `oceanfs-accel/src/metrics.rs:35` |
| `AccelMetrics::runtime_fallback_total` | `AtomicU64` | `oceanfs-accel/src/metrics.rs:37` |
| `AccelMetrics::encode_ops_total` | `AtomicU64` | `oceanfs-accel/src/metrics.rs:39` |
| `AccelMetrics::decode_ops_total` | `AtomicU64` | `oceanfs-accel/src/metrics.rs:41` |
| `AccelDispatcher::ec_fallback_count` | `AtomicU64` | `oceanfs-accel/src/dispatcher.rs:115` |
| `AccelDispatcher::compression_fallback_count` | `AtomicU64` | `oceanfs-accel/src/dispatcher.rs:112` |

### 2.4 Buffer Pool & Segment Pool

| Internal Name | Type | Location |
|---|---|---|
| `SegmentPool::active_count()` | Method (live) | `oceanfs-storage/src/segment/pool.rs:228` |
| `BufferPool::free_count()` | Method (live) | `oceanfs-storage/src/buffer_pool.rs:85` |
| `BufferPool::total_created()` | Method (live) | `oceanfs-storage/src/buffer_pool.rs:100` |

### 2.5 Hinted Handoff

| Internal Name | Type | Location |
|---|---|---|
| `HintedHandoff::pending_count()` | Method (live) | `oceanfs-durability/src/hinted_handoff.rs:223` |
| `HintedHandoff::total_pending_count()` | Method (live) | `oceanfs-durability/src/hinted_handoff.rs:229` |

**Total internal stats tracked:** 25  
**Total exposed at `/admin/metrics`:** 0

---

## 3. Complete Metrics Catalog — Required by Phase

### 3.1 Phase 1 — Concurrency Correctness

Minimal metrics needed. TSAN catches the worst bugs. Only need to verify the
system didn't silently fall back to a lower acceleration tier.

| Metric | Type | Labels | Status | Effort to Wire |
|---|---|---|---|---|
| `accel_fallback_total` | Counter | `from_tier`, `to_tier` | Internal (`AccelDispatcher::ec_fallback_count`). Not wired. | Low — wire existing counter |
| `accel_runtime_fallback_total` | Counter | `from_tier`, `to_tier`, `reason` | Internal (`AccelMetrics`). Not wired. | Low — wire existing counter |
| `segment_seal_errors_total` | Counter | — | Does not exist. No error counter in seal path. | Medium — add counter + increment sites |

### 3.2 Phase 2 — Sustained Single-Node Load

This is where metrics become essential. Without them, you cannot detect leaks,
write stalls, or degradation.

**Critical (test cannot run without):**

| Metric | Type | Labels | Status | Effort |
|---|---|---|---|---|
| `process_resident_memory_bytes` | Gauge | — | Does not exist. No system metrics module. | Medium — read `/proc/self/statm` |
| `process_open_fds` | Gauge | — | Does not exist. | Low — count `/proc/self/fd` |
| `rocksdb_num_files_at_level_0` | Gauge | — | Does not exist. RocksDB properties not queried. | Medium — periodic `get_property` |
| `rocksdb_num_files_at_level_N` | Gauge | `level` | Does not exist. | Medium — same poll as above |
| `rocksdb_block_cache_hit_count` | Counter | — | Does not exist. | Medium — RocksDB property query |
| `rocksdb_block_cache_miss_count` | Counter | — | Does not exist. | Medium — same |
| `rocksdb_estimate_pending_compaction_bytes` | Gauge | — | Does not exist. Write stall indicator. | Medium |
| `segment_active_count` | Gauge | — | Live query only (`active_count()`). Not a gauge. | Low — wrap in gauge |
| `segment_seal_errors_total` | Counter | — | Does not exist. | Medium |
| `wal_bytes_written_total` | Counter | — | Does not exist. | Medium — add to `WalWriter` |
| `wal_truncations_total` | Counter | — | Does not exist. | Medium — add to `WalWriter` |
| `accel_fallback_total` | Counter | `from_tier`, `to_tier` | Internal, not wired. | Low |
| `accel_runtime_fallback_total` | Counter | `from_tier`, `to_tier`, `reason` | Internal, not wired. | Low |

**High priority (strongly desired):**

| Metric | Type | Labels | Status | Effort |
|---|---|---|---|---|
| `cache_hits_total` | Counter | `tier` (l1/l2/l3) | Internal (`CacheStats`). Not wired. | Low — 3 registrations |
| `cache_misses_total` | Counter | `tier` | Internal. Not wired. | Low |
| `cache_inline_hits_total` | Counter | — | Internal (`MetadataCacheStats`). | Low |
| `cache_false_positives_total` | Counter | — | Internal (`NegativeCacheStats`). | Low |
| `segment_compaction_total` | Counter | — | Per-cycle value, not cumulative. | Medium — make GcStats cumulative |
| `compaction_bytes_total` | Counter | — | Same. | Medium |
| `buffer_pool_buffers_available` | Gauge | — | Live query only. | Low |
| `buffer_pool_bytes_allocated` | Gauge | — | Does not exist. | Low |

### 3.3 Phase 3 — Cluster Churn

Requires distributed protocol metrics. Most of these do not exist in any form.

**Critical:**

| Metric | Type | Labels | Status | Effort |
|---|---|---|---|---|
| `gossip_messages_sent_total` | Counter | — | Does not exist. No gossip counters. | Medium |
| `gossip_messages_received_total` | Counter | — | Does not exist. | Medium |
| `gossip_messages_dropped_total` | Counter | — | Does not exist. | Medium |
| `gossip_round_duration_seconds` | Histogram | — | Does not exist. No timing. | Medium |
| `ring_generation` | Gauge | — | Does not exist. | Low |
| `ring_nodes_total` | Gauge | — | Does not exist. | Low |
| `membership_nodes_alive` | Gauge | — | Does not exist. | Low |
| `hinted_handoff_hints_stored_total` | Counter | — | Live `pending_count()` only. No cumulative. | Medium |
| `hinted_handoff_hints_delivered_total` | Counter | — | Does not exist. | Medium |
| `hinted_handoff_hints_expired_total` | Counter | — | Does not exist. No TTL mechanism. | High — requires TTL implementation |
| `heal_requests_total` | Counter | — | Internal (`HealStats`). Not wired. | Low |
| `heal_requests_completed_total` | Counter | — | Internal. Not wired. | Low |
| `heal_requests_failed_total` | Counter | — | Internal. Not wired. | Low |
| `heal_bytes_repaired_total` | Counter | — | Internal. Not wired. | Low |
| `accel_fallback_total` | Counter | `from_tier`, `to_tier` | Internal, not wired. | Low |

**High priority:**

| Metric | Type | Labels | Status | Effort |
|---|---|---|---|---|
| `anti_entropy_segments_compared_total` | Counter | — | Per-cycle value only. Not cumulative. | Medium |
| `anti_entropy_mismatches_found_total` | Counter | — | Same. | Medium |
| `scrub_segments_checked_total` | Counter | — | Does not exist. | Medium |
| `scrub_segments_corrupt_total` | Counter | — | Does not exist. | Medium |

### 3.4 Phase 4 — Degraded Mode

Depends on Phase 3 metrics. Additionally requires:

| Metric | Type | Labels | Status | Effort |
|---|---|---|---|---|
| `hinted_handoff_hints_expired_total` | Counter | — | Same as Phase 3 — not implemented. | High |
| `heal_requests_failed_total` | Counter | — | Internal, not wired. | Low |
| `segment_health_status` | Gauge | `segment_id` | Does not exist. | High — requires per-segment health tracking |

### 3.5 Phase 5 — Scale Properties

| Metric | Type | Labels | Status | Effort |
|---|---|---|---|---|
| `grpc_connections_active` | Gauge | — | Does not exist. | Medium |
| `grpc_connection_errors_total` | Counter | — | Does not exist. | Medium |
| `s3_requests_total` | Counter | `method` (GET/PUT/DELETE/HEAD) | Does not exist. | Low |
| `s3_request_errors_total` | Counter | `method`, `status_code` | Does not exist. | Low |

### 3.6 Spec §9.8.1 — Acceleration Metrics (All Phases)

These are spec-mandated but none are exposed:

| Spec Metric | Type | Labels | Status |
|---|---|---|---|
| `accel_tier_active` | Gauge | `tier`, `operation` | Tier info only via JSON at `/admin/acceleration`. No Prometheus gauge. |
| `accel_encode_duration_seconds` | Histogram | `tier`, `k`, `m` | No timing in encode path. |
| `accel_decode_duration_seconds` | Histogram | `tier`, `k`, `m` | No timing in decode path. |
| `accel_bytes_processed_total` | Counter | `tier`, `operation` | Internal `AccelMetrics` tracks bytes_encoded/decoded. Not wired, not labeled. |
| `accel_fallback_total` | Counter | `from_tier`, `to_tier` | Internal `Dispatcher` counter. Not wired, not labeled. |
| `accel_runtime_fallback_total` | Counter | `from_tier`, `to_tier`, `reason` | Internal `AccelMetrics`. Not wired, not labeled. |
| `accel_gpu_utilization` | Gauge | `device` | No GPU monitoring. |
| `accel_gpu_memory_bytes` | Gauge | `device`, `kind` | No GPU memory tracking. |
| `accel_gpu_semaphore_wait_seconds` | Histogram | `device` | Not implemented. |
| `accel_compress_duration_seconds` | Histogram | `tier`, `algorithm` | Not implemented. |
| `accel_hash_duration_seconds` | Histogram | `tier` | Not implemented. |

---

## 4. Structural Registry Gaps

The `MetricsRegistry` itself has deficiencies that must be addressed before
metrics wiring can begin:

### 4.1 No Gauge Type (Critical)

The registry supports `Counter` (monotonic, increment-only) and `Histogram`,
but **not `Gauge`** (settable, up/down). Half the metrics in §3 are gauges:
memory, FDs, RocksDB levels, segment pool size, buffer pool capacity, ring
node count, GPU utilization.

**Required:** Add a `Gauge` type with `set(value: f64)`, `inc()`, `dec()`,
and `get()` backed by `AtomicU64` (stored as integer, rendered as float).

### 4.2 No Label Support (Critical)

The current `Counter::render()` emits `name value`, not
`name{label="value"} value`. Labels are essential:
- `cache_hits_total{tier="l1"}` vs `{tier="l2"}`
- `accel_fallback_total{from_tier="gpu_cuda",to_tier="cpu_simd"}`
- `rocksdb_num_files_at_level{level="0"}`

Without labels, you'd need a separate metric name for every tier/level/operation
combination — unmanageable.

**Required:** Add label support to `Counter`, `Gauge`, and `Histogram`. Accept
`&[(&str, &str)]` at registration time.

### 4.3 Histogram Lock Contention (Medium)

The current `Histogram` uses `RwLock<Vec<u64>>` for bucket values. Every
`observe()` acquires a write lock. Perf guideline §2.5 says "use lock-free
structures for hot paths." At load test throughput (thousands of ops/sec),
this lock becomes a bottleneck.

**Required:** Replace with per-bucket `AtomicU64` values. The `gather()`
method reads atomics without any lock.

### 4.4 Coarse Histogram Buckets (Medium)

Current buckets: `[1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000]`
milliseconds. No sub-millisecond bucket exists. EC encode (CPU SIMD) for a
4 MB segment is ~50µs — it falls into the `1ms` bucket with zero precision.

**Required:** Add configurable buckets. Default to include
`[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 50, 100]` ms.

### 4.5 Registry Not Stored on Node (Medium)

`MetricsRegistry` is created in `Node::start()` but not stored on the `Node`
struct. After construction, only the `AdminHandler` in the axum router holds a
reference. Background tasks (GC, heal, scrub) cannot register metrics because
they have no reference to the registry.

**Required:** Store `Arc<MetricsRegistry>` on `Node` and pass it to all
subsystem constructors (or use a global lazy-static).

---

## 5. Remediation Plan

### 5.1 Phase A — Registry Fixes (Blocks Everything)

Estimated effort: 2-3 sessions.

1. Add `Gauge` type to `MetricsRegistry`.
2. Add label support to `Counter`, `Gauge`, `Histogram`.
3. Convert `Histogram` buckets to per-bucket `AtomicU64`.
4. Store `Arc<MetricsRegistry>` on `Node`.
5. Add configurable histogram bucket bounds.

**After Phase A:** The registry is capable of hosting all required metrics.

### 5.2 Phase B — Wire Existing Data (Unblocks Phase 1-2)

Estimated effort: 1-2 sessions.

Wire the 18 existing internal counters (cache hits/misses, heal stats, accel
fallback, buffer pool) into the registry. The data already exists — this is
purely registration + expose.

**After Phase B:** 18 metrics appear at `/admin/metrics`. Phase 1 load tests
can assert `accel_fallback_total == 0`.

### 5.3 Phase C — System & Storage Metrics (Unblocks Phase 2)

Estimated effort: 2-3 sessions.

1. Add process metrics module (`/proc/self/statm`, `/proc/self/fd`).
2. Add RocksDB property polling (level files, block cache, pending compaction).
3. Add WAL counters (bytes written, truncations).
4. Add segment seal error counter.
5. Add cumulative GC/compaction counters.

**After Phase C:** Phase 2 sustained load tests can run with full leak
detection and write-stall monitoring.

### 5.4 Phase D — Distributed Protocol Metrics (Unblocks Phase 3-4)

Estimated effort: 3-4 sessions.

1. Add gossip message counters (sent/received/dropped).
2. Add gossip round duration histogram.
3. Add ring/membership gauges.
4. Add hinted handoff cumulative counters (stored/delivered/expired) + TTL
   expiration mechanism.
5. Add anti-entropy and scrub counters.
6. Add EC encode/decode timing histograms.

**After Phase D:** Phase 3-4 cluster churn and failure injection tests can run.

### 5.5 Phase E — Scale Metrics (Unblocks Phase 5)

Estimated effort: 1-2 sessions.

1. Add gRPC connection counters.
2. Add S3 request rate/error counters.
3. Add GPU metrics (for CUDA-enabled builds).

---

## 6. Summary

| Category | Count | Status |
|---|---|---|
| Internal counters (exist, not wired) | 25 | Need wiring only |
| New counters to implement | ~20 | Need new code |
| New gauges to implement | ~12 | Need new code + Gauge type |
| New histograms to implement | ~6 | Need new code + bucket fix |
| Registry structural fixes | 5 | Must be done first |

**Bottom line:** Phase 1 can begin today (TSAN catches the worst bugs, no
metrics needed for basic concurrency correctness). Phase 2+ requires the
remediation plan above. The most leveraged first step is Phase A (registry
fixes) + Phase B (wire existing data), which delivers ~18 metrics with
relatively low effort.
