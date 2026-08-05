---
feature: "Metrics Scraper — Prometheus Text Parser & Snapshot Diff"
epic: "test-harness-extensions"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: gap-closure/metrics-infrastructure
    reason: Need /admin/metrics populated with actual metrics before scraper is useful
adr: []
perf: []
created: 2026-08-05
updated: 2026-08-05
---

# Metrics Scraper — Prometheus Text Parser & Snapshot Diff

## Summary

Implement `MetricsSnapshot` in `e2e/src/load/metrics.rs`. This type scrapes
`GET /admin/metrics` from an OceanFS node, parses the Prometheus text format
into a `HashMap<String, f64>`, and provides a `delta()` method for comparing
two snapshots (to compute counter differences over time). The parser must be
lightweight — no full Prometheus client library — approximately 100 lines of
Rust covering counters (gauge-style values) and histograms (extracting `_sum`,
`_count`, `_bucket`).

## Scope

### In Scope

- `MetricsSnapshot` struct: `timestamp: Instant`, `metrics: HashMap<String, f64>`
- `MetricsSnapshot::scrape(node: &NodeProcess)` → `Result<Self>` — HTTP GET `/admin/metrics`, parse response body
- Prometheus text format parser: line-by-line, skip `#` comments and `# HELP`/`# TYPE` metadata
- Counter parsing: `metric_name value` → `HashMap::insert("metric_name", value.parse())`
- Histogram parsing: `metric_name_bucket{le="0.1"} 42` → `insert("metric_name_sum", ...)`, `insert("metric_name_count", ...)`, optionally `insert("metric_name_bucket_0.1", 42)`
- Label support: `metric_name{label="value"} 5` → `insert("metric_name", 5)` (flatten labels to name for simple assertions; or preserve as `metric_name{label="value"} 5`)
- `MetricsSnapshot::delta(prev: &Self) -> HashMap<String, f64>` — for counter metrics, compute `current - previous`; returns positive diff for monotonic counters
- `MetricsSnapshot::gauge(&self, name: &str) -> Option<f64>` — read a gauge value
- Graceful error handling: malformed lines skipped with `eprintln!` warning, not panics
- No external dependencies beyond `reqwest` (already in `e2e`)

### Out of Scope

- Full `openmetrics` parsing (timestamps, exemplars)
- PromQL query execution (that's the `vm-metrics` agent skill, not in-harness)
- Metric type inference from `# TYPE` comments
- Scraping multiple nodes simultaneously (caller calls scrape per-node)

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | New module `src/load/metrics.rs`. No new dependencies. |

## Interface (Public API)

- `pub struct MetricsSnapshot` — point-in-time scrape of `/admin/metrics`
- `pub async fn scrape(node: &NodeProcess) -> Result<Self>` — HTTP GET + parse
- `pub fn delta(&self, prev: &Self) -> HashMap<String, f64>` — counter diff between two snapshots
- `pub fn gauge(&self, name: &str) -> Option<f64>` — read a specific value
- `pub fn counter(&self, name: &str) -> Option<f64>` — alias for gauge (same data, different semantics)

## Data Flow

```
GET http://127.0.0.1:{port}/admin/metrics → Prometheus text:
  # HELP accel_fallback_total Number of accel fallbacks
  # TYPE accel_fallback_total counter
  accel_fallback_total 0
  # HELP process_resident_memory_bytes RSS in bytes
  # TYPE process_resident_memory_bytes gauge
  process_resident_memory_bytes 87000000
  # HELP s3_request_latency_seconds S3 request latency
  # TYPE s3_request_latency_seconds histogram
  s3_request_latency_seconds_bucket{le="0.005"} 100
  s3_request_latency_seconds_bucket{le="0.01"} 250
  s3_request_latency_seconds_sum 12.5
  s3_request_latency_seconds_count 500

MetricsSnapshot::scrape(node):
  → HashMap<String, f64>:
    "accel_fallback_total" → 0.0
    "process_resident_memory_bytes" → 87000000.0
    "s3_request_latency_seconds_sum" → 12.5
    "s3_request_latency_seconds_count" → 500.0

delta(later, earlier):
  for each key in later.metrics:
    if both have key:
      diff = later[key] - earlier[key]
      if diff >= 0: insert(key, diff)   // counters only increase
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `e2e` crate
- [ ] **Tests:** Unit test: parse a known Prometheus text blob — 5 counters, 3 gauges, 1 histogram — all parsed correctly
- [ ] **Tests:** Unit test: malformed lines skipped (non-numeric value, empty line) — no panic
- [ ] **Tests:** Unit test: histogram with `_bucket`, `_sum`, `_count` — all three extracted
- [ ] **Tests:** Unit test: labeled metric `accel_fallback_total{from_tier="gpu"}` — parsed as flat key
- [ ] **Tests:** Unit test: `delta()` — two snapshots with incrementing counter — diff correct
- [ ] **Tests:** Unit test: `delta()` — gauge that decreased — diff is negative (acceptable)
- [ ] **Tests:** Integration test: spawn 1-node cluster, scrape `/admin/metrics`, assert at least `process_resident_memory_bytes` exists
- [ ] **Docs:** Every `pub` item has doc comments; `#![deny(missing_docs)]` passes
- [ ] **Integration:** Full end-to-end: scrape, wait, scrape, delta shows counter increment
