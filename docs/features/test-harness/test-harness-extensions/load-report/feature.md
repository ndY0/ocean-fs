---
feature: "Load Report — JSON Output, Assertions, & Prometheus Textfile"
epic: "test-harness-extensions"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: test-harness-extensions/manifest-tracker
    reason: Need ManifestSummary type for report manifest section
  - epic: test-harness-extensions/load-scenario-orchestrator
    reason: Need AggregateStats for report worker_stats section
  - epic: test-harness-extensions/metrics-scraper
    reason: Need MetricsSnapshot for report metric_snapshots section
adr: []
perf: []
created: 2026-08-05
updated: 2026-08-05
---

# Load Report — JSON Output, Assertions, & Prometheus Textfile

## Summary

Implement `LoadReport` and supporting types in `e2e/src/load/report.rs`. Every
load test produces a single `LoadReport` containing: phase, test name, seed,
duration, result (pass/fail/timeout), worker aggregate stats, manifest summary,
periodic metric snapshots, named assertions with expected/actual, and failure
details. The report is written as JSON to `target/load-reports/{phase}_{test}_{timestamp}.json`.
Additionally, the harness writes a Prometheus textfile (`load_test.prom`) with
high-level harness events (phase, objects_written, mismatches, result) for
Prometheus scraping and Grafana dashboard integration. All writes are atomic
(temp file + rename).

## Scope

### In Scope

- `LoadReport` struct: `phase`, `test`, `seed`, `duration_secs`, `result` (`ReportResult`), `worker_stats`, `manifest`, `metric_snapshots`, `assertions`, `failures` — all `Serialize`
- `ReportResult` enum: `Pass`, `Fail`, `Timeout` — all `Serialize`
- `AssertionResult` struct: `name`, `passed`, `expected` (human-readable expected), `actual` (human-readable actual) — all `Serialize`
- `FailureDetail` struct: `assertion` (name of failed assertion), `detail` (free-form string), `timestamp` — all `Serialize`
- `LoadReport::write_json(&self, output_dir: &Path)` → writes `{phase}_{test}_{timestamp}.json` atomically
- `LoadReport::write_textfile(&self, output_dir: &Path)` → writes Prometheus textfile atomically:
  - `load_test_phase{test="..."} N`
  - `load_test_objects_written_total N`
  - `load_test_mismatches_total N`
  - `load_test_result{result="pass|fail|timeout"} 1`
  - `process_rss_bytes_at_end N` (if available from last metric snapshot)
  - `process_open_fds_at_end N` (if available)
- Atomic writes: write to `{path}.tmp`, `fsync`, rename to `{path}`
- Timestamp format: ISO 8601 compact: `YYYYMMDDTHHMMSS`
- Output directory configurable via env var `LOAD_REPORT_DIR` (default `target/load-reports/`)
- Helper function `assert_that(name, condition, expected, actual)` → `AssertionResult`
- Helper function `record_failure(name, detail)` → appends to `LoadReport.failures`
- `LoadReport::finalize(&mut self)` — sets `result` based on assertions: if any assertion failed → Fail; if all passed → Pass

### Out of Scope

- Historical storage or diffing of reports (tracked separately by results branch/CI artifact)
- Grafana annotation push (nice-to-have, deferred — Grafana reads the textfile natively)
- Schema validation against a JSON Schema definition (inline documentation in report module is sufficient for now)
- Real-time streaming of assertions during test (all assertions collected and written at end)

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | New module `src/load/report.rs`. Add `serde` (Serialize) — already exists as dependency. |

## Interface (Public API)

- `pub struct LoadReport` — the complete test report
- `pub enum ReportResult` — `Pass`, `Fail`, `Timeout`
- `pub struct AssertionResult` — single named check
- `pub struct FailureDetail` — single failure description
- `pub fn assert_that(name: &str, condition: bool, expected: &str, actual: &str) -> AssertionResult`
- `pub fn write_json_atomic(report: &LoadReport, output_dir: &Path) -> Result<PathBuf>`
- `pub fn write_textfile_atomic(metrics: &HashMap<String, f64>, output_dir: &Path) -> Result<()>`

## Data Flow

```
Test function:
  report = LoadReport::new(phase=2, test="load_sustained", seed=seed)

  // During run: periodic metric snapshots
  report.metric_snapshots.push(MetricsSnapshot::scrape(&node).await?)

  // During run: assertions (some checked per-snapshot, some at end)
  report.assertions.push(assert_that(
    "memory_bounded",
    final_rss < initial_rss * 2,
    "RSS < 2× initial after 30min",
    format!("RSS: {initial_rss} → {final_rss}")
  ))

  // Collect worker stats
  report.worker_stats = orchestrator.collect()

  // Collect manifest summary
  let mismatches = manifest.verify(&cluster).await;
  report.manifest = ManifestSummary {
    objects_written: manifest.active_count() as u64,
    objects_verified: ...,
    mismatches: mismatches.len() as u64,
    mismatch_details: mismatches,
  }

  // Finalize and write
  report.finalize();
  report.write_json(Path::new("target/load-reports/"))?;
  report.write_textfile(Path::new("/var/lib/prometheus/textfile/"))?;
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `e2e` crate
- [ ] **Tests:** Unit test: `LoadReport` serializes to valid JSON with all fields populated
- [ ] **Tests:** Unit test: `assert_that` with passing condition → `passed=true`
- [ ] **Tests:** Unit test: `assert_that` with failing condition → `passed=false`
- [ ] **Tests:** Unit test: `finalize()` sets `result=Fail` when any assertion fails
- [ ] **Tests:** Unit test: `finalize()` sets `result=Pass` when all assertions pass
- [ ] **Tests:** Unit test: `write_json_atomic` — verify temp file renamed, valid JSON on disk
- [ ] **Tests:** Unit test: `write_textfile_atomic` — verify Prometheus text format valid
- [ ] **Tests:** Unit test: `ReportResult` serde — `"pass"`, `"fail"`, `"timeout"` round-trip
- [ ] **Docs:** Every `pub` item has doc comments; `#![deny(missing_docs)]` passes
- [ ] **Integration:** Run a short 10s load scenario, produce report JSON, verify file exists with correct phase and non-zero stats
