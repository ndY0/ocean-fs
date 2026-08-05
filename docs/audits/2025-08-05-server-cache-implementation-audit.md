---
audit_date: 2025-08-05
scope: full
target_crates: oceanfs-server, oceanfs-cache
severity_counts:
  critical: 2
  high: 7
  medium: 11
  low: 5
---

# Audit Report: OceanFS Server, S3 API, Admin API & Caching Implementation Audit

## Summary

The OceanFS server (`oceanfs-server`) and caching (`oceanfs-cache`) subsystems have achieved a **solid production-ready baseline** for single-node operation, with 91% of cluster-mode e2e tests passing (39/43). The S3 API is complete for object CRUD (PUT/GET/HEAD/DELETE), the three-tier cache cascade (L1→L2→L3) is fully wired, and all gRPC services are implemented and registered. Write coordination handles quorum writes, forwarding, HLC timestamps, and replica fanout. Read coordination handles metadata lookups, inline serving, multi-chunk assembly, and streaming BLAKE3 verification.

The most significant gaps relate to distributed correctness: **read repair corrective pushes are not implemented** (same-HLC comparison), **EC decode is not integrated** into the shard-level fetch path, **conflict resolution for concurrent writes lacks multi-replica HLC comparison** (T45 fails), and **MetricsRegistry has zero production metrics registered**. These are blocking gaps for production multi-node correctness.

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `read_coordinator.rs:431-441` | Read repair calls `schedule_repair(meta.hlc, meta.hlc, ...)` — both arguments are the **same HLC**, making the conflict comparison degenerate. No actual multi-replica HLC gathering from remote fetch happens. | Implement HLC metadata extraction from `FetchShard` responses. Compare HLCs from N replicas. Push corrected data to stale nodes via gRPC. |
| C2 | `read_coordinator.rs:494-509` | `decode_ec_shards()` exists and compiles with `#[allow(dead_code)]` but is never called. The fetch path operates at chunk level, not shard level. Reads that must fall back to parity shards will fail rather than reconstruct from EC parity. | Integrate `decode_ec_shards()` into `read/fetch.rs` for shard-level fetch. Implement per-shard gRPC `FetchShard` calls so parity shards can be retrieved and decoded. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `read_coordinator.rs:386-403` | `ReadTuningConfig` fields (`parallel_fetch`, `use_fastest_k`, `stripe_parallelism`) are read from policy but **only logged**, never functionally applied. Parallel fetch and fastest-k are defaults of `FuturesUnordered`; serial fallback or semaphore-bounded stripe parallelism is not implemented. | Wire `stripe_parallelism` to a `tokio::sync::Semaphore`. Implement serial fetch path when `parallel_fetch = false`. |
| H2 | `admin.rs:44-47` | `MetricsRegistry` uses `parking_lot::RwLock<HashMap<>>` for both counters and histograms maps. Per perf §2.2, `DashMap` is recommended for concurrent read-heavy maps. Currently 0 production metrics registered, so this is a latent contention risk. | Replace `RwLock<HashMap>` with `DashMap`. Register at minimum HTTP request counters, cache stats, and EC operation counters. |
| H3 | `admin.rs` (entire file) | `MetricsRegistry` supports Counter and Histogram only. **No Gauge support**, **no label support** (flat metric names). Prometheus metrics endpoint returns empty data in production because zero metrics are registered outside test code. | Add `Gauge` metric type. Add label support (or document namespaced-flat as intentional). Register production metrics in `node.rs` wiring. |
| H4 | `e2e/tests/cluster_concurrency.rs:95` | **T45 FAILS**: Concurrent writes to the same key from different nodes. Both writes succeed (each node becomes local coordinator), but `ReadCoordinator` doesn't fetch from multiple replicas and compare HLCs via `ConflictResolver`. Nodes may return different versions. | Implement multi-replica fetch in `ReadCoordinator::get_object()` that compares HLCs from N replicas and serves the winning version. |
| H5 | `e2e/tests/cluster_hinted_handoff.rs:55` | **T21 FAILS**: Hint delivery when successor returns is not wired. `HintedHandoff` buffers writes during node failure but delivery to returned nodes isn't connected. `Cluster::restart()` assigns new ephemeral ports causing rejoin failure anyway (T43 interaction). | Wire hinted handoff delivery: on node rejoin detection, drain the handoff buffer via gRPC `HintedHandoff` calls. Preserve ports across restart in the `Cluster` harness. |
| H6 | `e2e/tests/cluster_lifecycle.rs:153` | **T43 FAILS**: `Cluster::restart()` assigns new random ports; old peers have stale addresses. Pre-crash data is readable from local storage but other nodes can't reach the restarted node. | Preserve assigned ports in a port-file in the temp data directory; re-read them on restart. |
| H7 | `s3_handler/handlers.rs` | **POST /{bucket}?policy** endpoint for setting/updating bucket policy is **not implemented**. No handler exists for bucket policy modification. | Add `put_bucket_policy` handler. Route in `S3Handler::into_router()`. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `admin.rs:510` | `/admin/segments` returns `encoding: 0` always. Encoding state is not tracked in segment metadata. | Add encoding-state tracking to segment metadata or compute from active segment pool state. |
| M2 | `admin.rs:605` | `/admin/metrics` returns empty data (no metrics registered). Counter and Histogram code works but nothing calls `registry.counter()` in production code. | Register metrics in `node.rs` for: HTTP request counts, cache hits/misses, write/read operations, segment operations. |
| M3 | `s3_handler/handlers.rs:418` | `invalidate_cache_on_replicas()` called **twice** on delete — appears to be a copy-paste duplication at line 417-418. | Remove the duplicate call. |
| M4 | `read/fetch.rs:226-244` | gRPC `FetchShard` streams data into a `Vec<u8>` via `extend_from_slice`. For large data, this doubles memory usage. `BytesMut` and `bytes::BufMut` would be more efficient. | Use `BytesMut` as accumulation buffer, convert to `Bytes` on finalization. |
| M5 | `write/coordinator.rs:314` | `forward_write()` returns `Hlc::zero()` for forwarded writes, losing the actual HLC timestamp from the forwarding node. | Return the HLC from the forwarding node's clock (already computed at line 268 `hlc`), not `Hlc::zero()`. |
| M6 | `admin.rs:60-73` | `MetricsRegistry::counter()` and `histogram()` acquire a **write lock** on every registration (including re-registration by name, which is the common path). | Use `parking_lot::RwLock::upgradable_read()` or switch to `DashMap` for lookups before write. |
| M7 | `admin.rs:81-87` | `gather()` acquires a **read lock** that blocks concurrent `counter()`/`histogram()` registrations. Since `gather()` iterates lock content and formats text (non-trivial duration), this creates a blocking window on the hot registration path. | Use `DashMap` for lock-free reads during `gather()`. |
| M8 | `s3_handler/handlers.rs:303-307` | GET-triggered prefetch passes `&[]` (empty adjacent list). Per known deviation DEV-004, adjacent-key discovery is not implemented. | Implement per-bucket key ordering in the metadata store with range-scan support for next-N-key prefetch. |
| M9 | `read/repair.rs:26-91` | `perform_read_repair()` resolves conflicts but never actually pushes corrected data. The "full implementation" comment at line 44-47 documents the intent but the code path is a no-op. | Add gRPC push of corrected data to stale node in the `AcceptRemote` branch. |
| M10 | `node.rs:63, 598, 732` | Three "placeholder" comments remain in node.rs: gossip task placeholder, gossip comment, heartbeat placeholder. These are documentation gaps. | Remove or replace placeholder comments with actual implementation references. |
| M11 | `s3_handler/mod.rs:205-219` | Route registration uses `/{bucket}` for bucket PUT/GET/DELETE and `/{bucket}/{*key}` for object operations. This is correct for S3 path-style but the catch-all `{*key}` does not handle nested virtual-host-style paths. | Document this as path-style only. Virtual-host-style support is future work. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `s3_handler/mod.rs` | No CORS middleware is applied. `tower-http` is available in workspace dependencies but no CORS layer is added to the axum router. | Add `tower_http::cors::CorsLayer` to the S3 handler router for browser-based S3 clients. |
| L2 | `prefetch.rs:110-119` | Prefetch worker is spawned only if a tokio runtime handle is available. In synchronous contexts (e.g., if called before runtime start), prefetch is silently disabled with no warning. | Log a warning when runtime handle is unavailable to aid debugging. |
| L3 | `admin.rs:478` | `/admin/cluster` returns `vnodes: 256` as a hardcoded constant when `ring_cache` is present, regardless of actual vnode count. | Read `vnodes_per_node * node_count` from ring configuration. |
| L4 | `read_coordinator.rs:36-39` | `DEFAULT_READ_TIMEOUT_MS` is a `LazyLock` that reads `OperationTimeouts::default()` but the value is never used (marked `#[allow(dead_code)]`). | Remove dead code or use it in the timeout path. |
| L5 | `l3_negative.rs` | Negative cache `contains()` returns `true` for "definitely absent" (inverted from standard Bloom filter semantics where `true` = "maybe present"). This is intentionally designed for the L3 check path but may confuse readers. | Add a prominent doc comment explaining the inverted semantics at the struct level. |

## Coupling Hotspots

| Symbol | Crate | In-Degree | Risk |
|---|---|---|---|
| `NodeId::new` | oceanfs-core | 436 | Low — foundational ID type |
| `NodeConfig::default` | oceanfs-core | 132 | Low — config default |
| `RocksDbMetadataStore::open` | oceanfs-storage | 111 | Medium — storage init |
| `put_segment` | oceanfs-storage | 83 | Medium — segment write path |
| `NodeId::as_str` | oceanfs-core | 71 | Low — display method |
| `HashOutput::from_bytes` | oceanfs-hash | 68 | Low — hash constructor |
| `Hlc::zero` | oceanfs-core | 63 | Medium — used widely in tests |
| `Encoder::encode` (trait) | oceanfs-ec | 63 | Medium — EC trait method |
| `Ring::new` | oceanfs-routing | 58 | Low — ring constructor |
| `HlcClock::new` | oceanfs-core | 56 | Low — clock constructor |
| `Cluster::spawn` (test) | e2e | 55 | Low — test harness |

The dependency graph respects the DAG constraint. No circular dependencies detected between crates. All internal dependencies follow the architecture.md spec:
- `oceanfs-core` → `oceanfs-hash` only (purity check passes)
- `oceanfs-server` depends on core, storage, routing, membership, network, cache
- `oceanfs-node` composes server + all deps

## Guideline Violations

| Guideline | Location | Violation |
|---|---|---|
| Arch §2.1 (traits in consuming crate) | `read_coordinator.rs:151-163` | `SegmentReader` trait is defined in `oceanfs-server` (the consuming crate) — ✅ compliant |
| Arch §3.1 (lib.rs facade) | `oceanfs-server/src/lib.rs` | All public exports through `pub use` in lib.rs — ✅ compliant |
| Coding §5.1 (pub items must have doc) | Various | All `pub struct`/`pub fn` have doc comments — ✅ compliant |
| Perf §2.2 (DashMap) | `admin.rs:45-46` | `RwLock<HashMap>` used instead of `DashMap` for metrics counters/histograms maps |
| Perf §2.3 (parking_lot) | `admin.rs` | Uses `parking_lot::RwLock` — ✅ compliant (no `std::sync` locks) |
| Perf §7.2 (RwLock for read-heavy) | `admin.rs:45-46` | Correctly uses `RwLock` for read-heavy metrics access — ✅ compliant |
| Perf §8.1 (FuturesUnordered) | `read/fetch.rs:102-125` | `FuturesUnordered` used for parallel chunk fetches — ✅ compliant |
| Perf §11.1 (Atomic counters) | `admin.rs` + `cache/*.rs` | All counters use `AtomicU64` with `Relaxed` ordering — ✅ compliant |
| Coding §4.1 (unit tests colocated) | All modules | Tests colocated in `#[cfg(test)] mod tests` — ✅ compliant |

## ADR Compliance

| ADR | Status | Notes |
|---|---|---|
| 0001 (segment packing) | ✅ Compliant | Inline data path exercised; small/standard/multi tiered; `inline_threshold_bytes=4096` matches ADR |
| 0005 (trait-in-consuming-crate) | ✅ Compliant | `SegmentReader` trait lives in `oceanfs-server` |
| 0006 (GPU acceleration) | ✅ Compliant | `AccelDispatcher` wired via `oceanfs-accel`, probed at startup |

## Test Coverage

| Crate | Public Symbols | Tests | Coverage % |
|---|---|---|---|
| `oceanfs-server` | 30+ public types | ~172 tests (unit + integration) | ~75% production paths |
| `oceanfs-cache` | 4 public types + prefetch | 35+ tests across 4 modules | ~94% (l3_negative.rs: 98.7%, l1_object: 85%+, l2_metadata: 84.5%, prefetch: 90.5%) |
| `e2e/` (cluster tests) | — | 46 test functions (43 tests, 39 pass) | 91% pass rate |
| `oceanfs-node` | — | 73 tests | Covers node lifecycle, e2e single-node |

### Known Test Gaps

| Test | Status | Gap Description |
|---|---|---|
| T21 (hinted handoff delivery) | ❌ FAIL | Hint delivery to returned node not wired |
| T43 (crash recovery rejoin) | ❌ FAIL | Random port re-assignment prevents rejoin |
| T45 (concurrent writes same key) | ❌ FAIL | No multi-replica HLC comparison in ReadCoordinator |
| T24/T26 (SWIM indirect ping) | ⚠️ FLAKY | Intermittent timing in 3-node with fast-gossip config |
| T4 (rejoin after leave) | ⚠️ FLAKY | Depends on T43 port preservation |

## System Status Summary

### S3 HTTP API

| Endpoint | Status | Notes |
|---|---|---|
| PUT /{bucket}/{key} | ✅ Complete | Quorum writes, HLC, BLAKE3, cache invalidation, blob persistence |
| GET /{bucket}/{key} | ✅ Complete | L1→L2→L3 cascade, chunk assembly, BLAKE3 verify, prefetch |
| HEAD /{bucket}/{key} | ✅ Complete | L3 negative check, metadata-only |
| DELETE /{bucket}/{key} | ✅ Complete | Tombstone, cache invalidation, L3 insert, replica delete fanout |
| PUT /{bucket} | ✅ Complete | Bucket creation with default policy |
| GET /{bucket} (LIST) | ✅ Complete | Prefix filter, S3 XML response |
| DELETE /{bucket} | ✅ Complete | Non-empty check |
| POST /{bucket}?policy | ❌ Not implemented | No handler |

### Admin API

| Endpoint | Status | Notes |
|---|---|---|
| GET /admin/health | ✅ Complete | JSON: status + version |
| GET /admin/cluster | ✅ Complete | Real membership + ring data |
| GET /admin/segments | ⚠️ Partial | encoding=0 always (M1) |
| GET /admin/caches | ✅ Complete | Real L1/L2/L3 stats |
| POST /admin/scrub | ✅ Complete | 202 Accepted path |
| GET /admin/metrics | ⚠️ Empty | 0 metrics registered (M2) |
| GET /admin/acceleration | ✅ Complete | Tier + backend status |

### Caching Layers

| Layer | Status | Notes |
|---|---|---|
| L1 Object Cache | ✅ Complete | DashMap LRU, TTL, size-gated, wired in GET, invalidation on PUT/DELETE |
| L2 Metadata Cache | ✅ Complete | DashMap LRU, TTL, inline serving, wired in GET, invalidation on PUT/DELETE |
| L3 Negative Cache | ✅ Complete | Bloom filter per bucket, wired in GET/HEAD, populated on DELETE |
| Cache Coherence | ✅ Complete | gRPC CacheInvalidate fanout on PUT/DELETE (write_coordinator.rs:212-243) |
| Prefetch Engine | ✅ Complete | Bounded channel + semaphore, wired in GET/LIST, adjacent-key discovery deferred |

### Write Coordinator

| Capability | Status | Notes |
|---|---|---|
| Quorum writes (W=N) | ✅ Complete | Configurable W, capped at replica count |
| Write forwarding | ✅ Complete | gRPC AppendSegment to successor |
| HLC timestamping | ✅ Complete | Clock advances on write |
| Replica fanout | ✅ Complete | gRPC streaming to N-1 successors |
| Cache invalidation | ✅ Complete | Fanout to all replicas |
| Delete replication | ✅ Complete | DeleteObject RPC to all replicas |
| Hinted handoff | ⚠️ Partial | Write accepted on unreachable (T20), delivery on return not wired (T21) |

### Read Coordinator

| Capability | Status | Notes |
|---|---|---|
| Metadata lookup | ✅ Complete | MetadataOps adapter |
| Inline data serving | ✅ Complete | Zero segment I/O |
| Multi-chunk assembly | ✅ Complete | Order-guaranteed, streaming BLAKE3 |
| Parallel shard fetch | ✅ Complete | FuturesUnordered, local reader + gRPC fallback |
| BLAKE3 verification | ✅ Complete | Hash mismatch returns error |
| Read repair | ❌ Incomplete | Same-HLC comparison, no corrective push (C1) |
| EC decode integration | ❌ Not integrated | decode_ec_shards dead code (C2) |
| Multi-replica HLC comparison | ❌ Not implemented | T45 gap (H4) |

### gRPC Services

| Service | RPC Methods | Status |
|---|---|---|
| SegmentGrpcService | AppendSegment, FetchShard, DeleteObject | ✅ Complete |
| GossipGrpcService | GossipPush, GossipPull | ✅ Complete |
| HealingGrpcService | HintedHandoff, MerkleExchange | ✅ Complete |
| CacheGrpcService | CacheInvalidate | ✅ Complete |
| ScrubGrpcService | TriggerScrub | ✅ Complete |
| ProbeService (SWIM) | Probe | ✅ Complete (registered in membership, gossip-as-ping-proxy) |

### MetricsRegistry

| Feature | Status |
|---|---|
| Counter (AtomicU64) | ✅ Implemented |
| Histogram (fixed buckets) | ✅ Implemented |
| Gauge | ❌ Not supported |
| Labels | ❌ Not supported |
| Production metrics | ❌ 0 registered |
| Lock mechanism | ⚠️ `parking_lot::RwLock` (should be `DashMap` per perf §2.2) |

### Auth & Middleware

| Feature | Status |
|---|---|
| SigV4 authentication | ✅ Complete (sigv4.rs, 410 lines, 11 tests) |
| Auth middleware | ✅ Complete (tower Layer, config-driven passthrough) |
| Key store (TOML) | ✅ Complete (key_store.rs) |
| Body size limiting | ✅ Complete (DefaultBodyLimit, configurable max_body_size) |
| CORS | ❌ Not implemented |
| Request ID tracking | ✅ Partial (uuid_for_error uses timestamp nanos) |

## Recommendations

### Priority 1 — Critical (block production multi-node)

1. **Implement multi-replica HLC fetch + conflict resolution in `ReadCoordinator`** (C1, H4). Without this, concurrent writes to the same key from different nodes can produce divergent results. This is the single most important correctness gap.

2. **Integrate EC decode into shard-level fetch path** (C2). Currently `decode_ec_shards()` is dead code. When a data shard is unavailable, the system must fall back to parity shard reconstruction to maintain data durability.

### Priority 2 — High (block cluster completeness)

3. **Register production metrics in `MetricsRegistry`** (H2, H3, M2). The registry infrastructure works but zero metrics are registered. At minimum: HTTP request counters, cache hit/miss counters, write/read operation counters.

4. **Wire hinted handoff delivery** (H5). T20 proves hint storage works; T21 proves delivery doesn't. The handoff buffer drains but never reaches the returned node.

5. **Preserve ports across `Cluster::restart()`** (H6). The test harness assigns new random ports on restart, preventing rejoin. Fix this to enable T43 and T4.

6. **Fix read repair to compare real HLCs from multiple replicas** (M9). Currently compares `meta.hlc` against itself. Gather HLC metadata from `FetchShard` responses.

7. **Implement `POST /{bucket}?policy`** (H7). Bucket policy modification endpoint is absent.

### Priority 3 — Medium (code quality)

8. **Replace `RwLock<HashMap>` with `DashMap` in `MetricsRegistry`** (M6, M7). Follows perf §2.2.

9. **Fix `forward_write()` returning `Hlc::zero()`** (M5). Use the actual HLC computed during forwarding.

10. **Remove duplicate `invalidate_cache_on_replicas()` call** (M3). Line 417-418 in handlers.rs.

11. **Add `Gauge` metric type and label support** (H3). Required for memory usage and per-endpoint metrics.

12. **Fix `vnodes` hardcoded constant** (L3). Read actual vnode count from ring.

### Priority 4 — Low (nice to have)

13. Add CORS middleware (L1).
14. Log warning when prefetch runtime unavailable (L2).
15. Remove `DEFAULT_READ_TIMEOUT_MS` dead code (L4).
16. Document inverted Bloom filter semantics (L5).

## Accepted Deviations (from feature docs, confirmed in this audit)

| Deviation | Description | Status |
|---|---|---|
| DEV-001 | EC decode not integrated into shard fetch | Confirmed — `decode_ec_shards()` is `#[allow(dead_code)]` |
| DEV-002 | Multi-node integration tests not implemented | Confirmed — `e2e/` tests exist but not in-process cluster tests |
| DEV-003 | Read repair corrective push not implemented | Confirmed — compares same HLC, no gRPC push |
| DEV-004 | Adjacent-key discovery for GET prefetch not implemented | Confirmed — `after_get` receives empty key list |
| D6 | WAL recovery GET after crash returns 500 | Confirmed in e2e test deviation |
| D7 | Prefetch L2 entry_count increase deferred | Confirmed — LIST 404 due to in-memory bucket store |
| D8 | 2MB default body size limit | Confirmed — `DefaultBodyLimit::max(config.max_body_size)` |
