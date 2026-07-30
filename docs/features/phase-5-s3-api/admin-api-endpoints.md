---
feature: "Admin API & Metrics"
epic: "phase-5-s3-api"
status: proposed
priority: medium
owner: ""
dependencies:
  - feature: s3-http-handlers
    reason: Admin endpoints are served on the same HTTP server (or separate port)
  - feature: swim-gossip-membership
    reason: /admin/cluster reads membership state
perf:
  - "11.1: Atomic counters on hot paths (prometheus metrics)"
  - "11.2: tracing span discipline"
created: 2026-07-30
updated: 2026-07-30
---

# Admin API & Metrics

## Summary

Implement the admin HTTP API and Prometheus metrics endpoint in
`oceanfs-server`. The admin API exposes cluster health, segment status, cache
statistics, and manual scrub triggering. The metrics endpoint serves Prometheus-
format metrics for all hot-path counters (requests, bytes, cache hits/misses,
EC operations).

## Scope

### In Scope
- `GET /admin/cluster`: cluster membership view, ring topology, node states, incarnation numbers
- `GET /admin/segments`: segment health report (count by state, sealed/unsealed, EC status)
- `GET /admin/caches`: per-tier cache hit/miss rates (object, metadata, negative)
- `POST /admin/scrub`: trigger full distributed scrub
- `GET /admin/metrics`: Prometheus text format metrics exposition
- Metrics counters (all `AtomicU64`/`AtomicUsize` with `Relaxed` ordering):
  - `http_requests_total{method, status}`
  - `blob_bytes_read_total`, `blob_bytes_written_total`
  - `cache_hits_total{tier="l1|l2|l3"}`, `cache_misses_total{tier="l1|l2|l3"}`
  - `ec_encode_ops_total`, `ec_decode_ops_total`, `ec_encode_seconds`
  - `segment_seals_total{tier="small|standard|multi"}`
  - `wal_append_ops_total`, `wal_fsync_seconds`
  - `gossip_messages_total{direction="push|pull"}`
- Metrics registry: `prometheus` crate integration, register + gather
- Admin auth placeholder: basic token or disable in dev
- Unit tests for all admin endpoints with mock data

### Out of Scope
- Distributed tracing (OpenTelemetry — future work, spec §16)
- Alerting rules (operator concern)
- Dashboard (Grafana is external)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New metric counter types (thin wrappers over atomics) |
| `oceanfs-server` | New modules: `admin/handlers.rs`, `admin/metrics.rs` |
| `oceanfs-node` | Metrics registry initialization, background metric collection tasks |

## Interface (Public API)

- `pub struct AdminHandler` — `pub fn new(membership: Arc<Membership>, metadata: Arc<dyn MetadataStore>, metrics: Arc<MetricsRegistry>) -> Self`, `pub fn into_router(self) -> axum::Router`
- `pub struct MetricsRegistry` — `pub fn new() -> Self`, `pub fn register_counter(&self, name: &str, help: &str) -> Counter`, `pub fn register_histogram(&self, name: &str, help: &str) -> Histogram`, `pub fn gather(&self) -> String`
- `pub(crate) struct ClusterView` — `nodes: Vec<NodeInfo>`, `ring_vnodes: usize`, `generation: u64`
- `pub(crate) struct SegmentReport` — `total: u64`, `sealed: u64`, `unsealed: u64`, `encoding: u64`, `by_tier: HashMap<SizeTier, u64>`

## Data Flow

```
GET /admin/cluster
  → Membership::nodes() → list of (node_id, state, incarnation, address)
    → RingCache::snapshot() → vnode count, node distribution
      → serialize to JSON → 200

GET /admin/metrics
  → MetricsRegistry::gather() → iterate all registered counters/histograms
    → format as Prometheus text exposition
      → 200 with Content-Type: text/plain

Metrics collection (hot path):
  HTTP handler entry:
    http_requests_total{method="PUT", status="200"}.inc()
  Cache lookup:
    l1_cache_hits.inc() or l1_cache_misses.inc()
  EC encode:
    let timer = ec_encode_seconds.start_timer()
    // ... encode ...
    timer.observe_duration()
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in affected crates
- [ ] **Tests:** Unit tests: all admin endpoints return valid JSON, metrics endpoint returns valid Prometheus format, counter increments are atomic and correct under concurrency, histogram observes correctly
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-server`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes
- [ ] **ADR:** N/A
- [ ] **Perf:** Rule 11.1 (AtomicU64 counters, Relaxed ordering), 11.2 (tracing spans at handler boundary only)
- [ ] **Integration:** `tests/admin_api.rs`: start node, GET /admin/cluster → verify membership data, GET /admin/metrics → verify counter values increase after PUT requests, POST /admin/scrub → verify 202 accepted
- [ ] **Manual:** `curl localhost:9000/admin/metrics` example in docs
