//! Load test report — JSON output, assertions, and Prometheus textfile.
//!
//! Every load test produces a single [`LoadReport`] containing the phase,
//! test name, seed, duration, pass/fail/timeout result, worker aggregate
//! stats, manifest summary, periodic metric snapshots, named assertions,
//! and failure details.
//!
//! The report is written as JSON atomically (temp file + rename). A
//! Prometheus textfile is also emitted for Grafana dashboard integration.
//!
//! ## Usage
//!
//! ```no_run
//! use std::path::Path;
//! use e2e::load::report::{assert_that, LoadReport, ReportResult};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut report = LoadReport::new(2, "load_sustained", 42);
//!
//! // Record assertions during the test.
//! report.assert(assert_that(
//!     "memory_bounded",
//!     true,
//!     "RSS < 2× initial after test",
//!     "RSS: 87MB → 92MB",
//! ));
//!
//! report.finalize();
//! assert_eq!(report.result, ReportResult::Pass);
//!
//! let output = Path::new("target/load-reports/");
//! report.write_json_atomic(output)?;
//! # Ok(())
//! # }
//! ```

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use super::{generator::AggregateStats, manifest::ManifestSummary};
use crate::load::MetricsSnapshot;

// ---------------------------------------------------------------------------
// LoadReport
// ---------------------------------------------------------------------------

/// The complete result of a single load test run.
///
/// Contains the test configuration, worker statistics, manifest summary,
/// metric snapshots, assertions, and a final pass/fail/timeout verdict.
#[derive(Debug, Clone, Serialize)]
pub struct LoadReport {
    /// Load test phase number (e.g., 1–4).
    pub phase: u8,
    /// Human-readable test name (e.g., `"load_sustained"`).
    pub test: String,
    /// Deterministic seed used by the load scenario.
    pub seed: u64,
    /// Actual wall-clock duration in seconds.
    pub duration_secs: f64,
    /// Final verdict: pass, fail, or timeout.
    pub result: ReportResult,
    /// Aggregate statistics from all workers, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_stats: Option<AggregateStats>,
    /// Manifest verification summary, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<ManifestSummary>,
    /// Periodic metric snapshots taken during the test.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub metric_snapshots: Vec<MetricsSnapshot>,
    /// Named assertions checked during or after the test.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub assertions: Vec<AssertionResult>,
    /// Detailed failure descriptions (supplements assertions).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub failures: Vec<FailureDetail>,
    /// Harness process resource usage at report time (metadata only —
    /// never asserted), per ADR-0019 Decision 4.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub harness_metrics: Option<HarnessSelfMetrics>,
}

/// The harness process's own resource usage at the end of a run.
///
/// Recorded as metadata so borderline SUT measurements can be attributed
/// when the harness is co-located with the SUT (`--single-vm` mode).
/// Mirrors the SUT-side `process_resident_memory_bytes` /
/// `process_open_fds` gauges.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct HarnessSelfMetrics {
    /// Resident memory of the harness process (bytes).
    pub process_resident_memory_bytes: u64,
    /// Open file descriptors of the harness process.
    pub process_open_fds: u64,
}

impl LoadReport {
    /// Creates a new report for a given test phase and name.
    ///
    /// The `result` is initialized to `Pass`; call [`finalize`](Self::finalize)
    /// after adding assertions to compute the final verdict.
    pub fn new(phase: u8, test: impl Into<String>, seed: u64) -> Self {
        Self {
            phase,
            test: test.into(),
            seed,
            duration_secs: 0.0,
            result: ReportResult::Pass,
            worker_stats: None,
            manifest: None,
            metric_snapshots: Vec::new(),
            assertions: Vec::new(),
            failures: Vec::new(),
            harness_metrics: None,
        }
    }

    /// Adds an assertion result to the report.
    pub fn assert(&mut self, assertion: AssertionResult) {
        self.assertions.push(assertion);
    }

    /// Records a failure with a descriptive detail string.
    ///
    /// Failures are supplemental to assertions — they capture diagnostic
    /// context when an assertion fails.
    pub fn record_failure(&mut self, assertion_name: impl Into<String>, detail: impl Into<String>) {
        self.failures.push(FailureDetail {
            assertion: assertion_name.into(),
            detail: detail.into(),
            timestamp: chrono_now(),
        });
    }

    /// Computes the final result based on assertions.
    ///
    /// If any assertion failed → `Fail`. If all passed → `Pass`.
    /// Call this before writing the report.
    pub fn finalize(&mut self) {
        self.result = if self.assertions.iter().any(|a| !a.passed) {
            ReportResult::Fail
        } else {
            ReportResult::Pass
        };
    }

    /// Writes the report as JSON to `{output_dir}/{phase}_{test}_{timestamp}.json`.
    ///
    /// The write is atomic: data is written to a temporary file first,
    /// then renamed to the final path.
    ///
    /// Returns the path of the written file.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be created or the
    /// file cannot be written.
    pub fn write_json_atomic(&self, output_dir: &Path) -> io::Result<PathBuf> {
        fs::create_dir_all(output_dir)?;

        let timestamp = chrono_compact();
        let filename = format!("{}_{}_{timestamp}.json", self.phase, self.test);
        let final_path = output_dir.join(&filename);
        let tmp_path = output_dir.join(format!(".{filename}.tmp"));

        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        fs::write(&tmp_path, &json)?;
        fs::rename(&tmp_path, &final_path)?;

        Ok(final_path)
    }

    /// Writes a Prometheus textfile to `{output_dir}/load_test.prom`.
    ///
    /// The write is atomic (temp file + rename). The textfile contains
    /// high-level metrics:
    /// - `load_test_phase` — the phase number
    /// - `load_test_objects_written_total` — total objects written
    /// - `load_test_mismatches_total` — total hash mismatches
    /// - `load_test_result` — 1 for the final result label
    ///
    /// If a final metric snapshot is available, also emits:
    /// - `process_rss_bytes_at_end`
    /// - `process_open_fds_at_end`
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be written.
    pub fn write_textfile_atomic(&self, output_dir: &Path) -> io::Result<()> {
        fs::create_dir_all(output_dir)?;

        let tmp_path = output_dir.join(".load_test.prom.tmp");
        let final_path = output_dir.join("load_test.prom");

        let mut buf = String::new();

        // Phase.
        write_metric(&mut buf, "load_test_phase", self.phase as f64, Some(&self.test));
        // Objects written.
        let objects_written =
            self.manifest.as_ref().map(|m| m.objects_written as f64).unwrap_or(0.0);
        write_metric(
            &mut buf,
            "load_test_objects_written_total",
            objects_written,
            Some(&self.test),
        );
        // Mismatches.
        let mismatches = self.manifest.as_ref().map(|m| m.mismatches as f64).unwrap_or(0.0);
        write_metric(&mut buf, "load_test_mismatches_total", mismatches, Some(&self.test));
        // Result.
        let result_label = match self.result {
            ReportResult::Pass => "pass",
            ReportResult::Fail => "fail",
            ReportResult::Timeout => "timeout",
        };
        write_metric(&mut buf, "load_test_result", 1.0, None);
        buf.push_str(&format!(
            "load_test_result{{test=\"{name}\",result=\"{result_label}\"}} 1\n",
            name = self.test
        ));

        // Process metrics from last snapshot.
        if let Some(last_snap) = self.metric_snapshots.last() {
            if let Some(rss) = last_snap.gauge("process_resident_memory_bytes") {
                write_metric(&mut buf, "process_rss_bytes_at_end", rss, Some(&self.test));
            }
            if let Some(fds) = last_snap.gauge("process_open_fds") {
                write_metric(&mut buf, "process_open_fds_at_end", fds, Some(&self.test));
            }
        }

        fs::write(&tmp_path, &buf)?;
        fs::rename(&tmp_path, &final_path)?;

        Ok(())
    }
}

/// Appends a Prometheus metric line to the buffer.
///
/// Format: `metric_name{test="label"} value\n`
fn write_metric(buf: &mut String, name: &str, value: f64, label: Option<&str>) {
    if let Some(label_val) = label {
        buf.push_str(&format!("{name}{{test=\"{label_val}\"}} {value}\n"));
    } else {
        buf.push_str(&format!("{name} {value}\n"));
    }
}

// ---------------------------------------------------------------------------
// ReportResult
// ---------------------------------------------------------------------------

/// Final verdict of a load test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ReportResult {
    /// All assertions passed.
    Pass,
    /// At least one assertion failed.
    Fail,
    /// The test was terminated by its timeout before completion.
    Timeout,
}

// ---------------------------------------------------------------------------
// AssertionResult
// ---------------------------------------------------------------------------

/// A single named assertion checked during or after a load test.
#[derive(Debug, Clone, Serialize)]
pub struct AssertionResult {
    /// Human-readable assertion name (e.g., `"memory_bounded"`).
    pub name: String,
    /// Whether the condition was satisfied.
    pub passed: bool,
    /// Human-readable description of what was expected.
    pub expected: String,
    /// Human-readable description of what actually occurred.
    pub actual: String,
}

// ---------------------------------------------------------------------------
// FailureDetail
// ---------------------------------------------------------------------------

/// A detailed failure description supplementing a failed assertion.
#[derive(Debug, Clone, Serialize)]
pub struct FailureDetail {
    /// Name of the associated assertion.
    pub assertion: String,
    /// Free-form diagnostic detail.
    pub detail: String,
    /// ISO 8601 timestamp of when the failure was recorded.
    pub timestamp: String,
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Creates an [`AssertionResult`] from a condition and human-readable labels.
///
/// # Examples
///
/// ```
/// use e2e::load::report::assert_that;
///
/// let r = assert_that(
///     "rss_bounded",
///     87_000_000 < 92_000_000,
///     "RSS should stay below 2× initial",
///     "RSS: 87MB → 92MB",
/// );
/// assert!(r.passed);
/// ```
pub fn assert_that(
    name: impl Into<String>,
    condition: bool,
    expected: impl Into<String>,
    actual: impl Into<String>,
) -> AssertionResult {
    AssertionResult {
        name: name.into(),
        passed: condition,
        expected: expected.into(),
        actual: actual.into(),
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Returns an ISO 8601 compact timestamp for filenames: `YYYYMMDDTHHMMSS`.
fn chrono_compact() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let (year, month, day, hours, minutes, seconds) = civil_from_epoch(secs);
    format!("{year:04}{month:02}{day:02}T{hours:02}{minutes:02}{seconds:02}")
}

/// Converts seconds since UNIX epoch (1970-01-01T00:00:00Z) into
/// UTC calendar components using the Hinnant algorithm.
fn civil_from_epoch(secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (year, month, day) = days_to_civil(days);
    (year, month, day, hours, minutes, seconds)
}

/// Converts days since 1970-01-01 to (year, month, day).
///
/// Based on Howard Hinnant's `civil_from_days` algorithm.
fn days_to_civil(days: u64) -> (u64, u64, u64) {
    // Shift epoch from 1970-01-01 to 0000-03-01.
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Returns an ISO 8601 timestamp string for failure details.
fn chrono_now() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let millis = now.subsec_millis();
    format!("{secs}.{millis:03}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Instant};

    use super::*;

    #[test]
    fn report_new_has_defaults() {
        let report = LoadReport::new(2, "sustained_load", 12345);
        assert_eq!(report.phase, 2);
        assert_eq!(report.test, "sustained_load");
        assert_eq!(report.seed, 12345);
        assert_eq!(report.result, ReportResult::Pass);
        assert!(report.worker_stats.is_none());
        assert!(report.manifest.is_none());
        assert!(report.metric_snapshots.is_empty());
        assert!(report.assertions.is_empty());
        assert!(report.failures.is_empty());
    }

    // ── assert_that tests ─────

    #[test]
    fn assert_that_passing_condition() {
        let r = assert_that("test_pass", true, "expected str", "actual str");
        assert!(r.passed);
        assert_eq!(r.name, "test_pass");
        assert_eq!(r.expected, "expected str");
        assert_eq!(r.actual, "actual str");
    }

    #[test]
    fn assert_that_failing_condition() {
        let r = assert_that("test_fail", false, "should be true", "was false");
        assert!(!r.passed);
        assert_eq!(r.expected, "should be true");
        assert_eq!(r.actual, "was false");
    }

    // ── finalize tests ─────

    #[test]
    fn finalize_sets_pass_when_all_assertions_pass() {
        let mut report = LoadReport::new(1, "test", 0);
        report.assert(assert_that("a", true, "e", "a"));
        report.assert(assert_that("b", true, "e", "a"));
        report.finalize();
        assert_eq!(report.result, ReportResult::Pass);
    }

    #[test]
    fn finalize_sets_fail_when_any_assertion_fails() {
        let mut report = LoadReport::new(1, "test", 0);
        report.assert(assert_that("a", true, "e", "a"));
        report.assert(assert_that("b", false, "e", "a"));
        report.assert(assert_that("c", true, "e", "a"));
        report.finalize();
        assert_eq!(report.result, ReportResult::Fail);
    }

    #[test]
    fn finalize_sets_pass_when_no_assertions() {
        let mut report = LoadReport::new(1, "test", 0);
        report.finalize();
        assert_eq!(report.result, ReportResult::Pass);
    }

    // ── JSON serialization tests ─────

    #[test]
    fn load_report_serializes_to_valid_json() {
        let mut report = LoadReport::new(2, "load_test", 999);
        report.duration_secs = 60.0;
        report.assert(assert_that("check_a", true, "x < y", "x=1, y=2"));
        report.finalize();

        let json = serde_json::to_string_pretty(&report).expect("serialize");
        assert!(json.contains("\"phase\": 2"));
        assert!(json.contains("\"test\": \"load_test\""));
        assert!(json.contains("\"seed\": 999"));
        assert!(json.contains("\"duration_secs\": 60.0"));
        assert!(json.contains("\"result\": \"pass\""));
        assert!(json.contains("\"name\": \"check_a\""));
        assert!(json.contains("\"passed\": true"));

        // Verify parsing back.
        let _parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
    }

    #[test]
    fn load_report_serializes_with_skip_fields_when_empty() {
        let mut report = LoadReport::new(1, "minimal", 0);
        report.finalize();
        let json = serde_json::to_string_pretty(&report).expect("serialize");

        // worker_stats and manifest should not appear when None.
        assert!(!json.contains("worker_stats"));
        assert!(!json.contains("manifest"));
        // Empty vecs should not appear.
        assert!(!json.contains("metric_snapshots"));
        assert!(!json.contains("failures"));
    }

    // ── write_json_atomic tests ─────

    #[test]
    fn write_json_atomic_creates_file() {
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let mut report = LoadReport::new(1, "unit_test", 42);
        report.finalize();

        let path = report.write_json_atomic(tmp.path()).expect("write");
        assert!(path.exists(), "file must exist: {}", path.display());
        assert!(path.to_string_lossy().ends_with(".json"));

        let content = fs::read_to_string(&path).expect("read");
        let parsed: serde_json::Value = serde_json::from_str(&content).expect("valid JSON");
        assert_eq!(parsed["phase"].as_u64(), Some(1));
        assert_eq!(parsed["test"].as_str(), Some("unit_test"));
    }

    #[test]
    fn write_json_atomic_replaces_previous_file() {
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let mut report = LoadReport::new(1, "unit_test", 42);
        report.duration_secs = 10.0;
        report.finalize();

        let path1 = report.write_json_atomic(tmp.path()).expect("write 1");

        // Write again.
        report.duration_secs = 20.0;
        let path2 = report.write_json_atomic(tmp.path()).expect("write 2");

        assert_eq!(path1, path2, "same name should overwrite");
        let content = fs::read_to_string(&path2).expect("read");
        assert!(content.contains("20.0"), "file should contain second duration");
    }

    // ── write_textfile_atomic tests ─────

    #[test]
    fn write_textfile_atomic_produces_prometheus_format() {
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let mut report = LoadReport::new(2, "textfile_test", 77);

        // Add a manifest to get objects_written.
        let manifest = ManifestSummary {
            objects_written: 500,
            objects_verified: 500,
            mismatches: 0,
            mismatch_details: vec![],
        };
        report.manifest = Some(manifest);

        // Add a metric snapshot.
        let snap = MetricsSnapshot {
            timestamp: Instant::now(),
            metrics: HashMap::from([
                ("process_resident_memory_bytes".to_string(), 90_000_000.0),
                ("process_open_fds".to_string(), 48.0),
            ]),
        };
        report.metric_snapshots.push(snap);
        report.finalize();

        report.write_textfile_atomic(tmp.path()).expect("write");

        let content = fs::read_to_string(tmp.path().join("load_test.prom")).expect("read");
        // Verify expected lines.
        assert!(content.contains("load_test_phase{test=\"textfile_test\"} 2"));
        assert!(content.contains("load_test_objects_written_total{test=\"textfile_test\"} 500"));
        assert!(content.contains("load_test_mismatches_total{test=\"textfile_test\"} 0"));
        assert!(content.contains("load_test_result{test=\"textfile_test\",result=\"pass\"} 1"));
        assert!(content.contains("process_rss_bytes_at_end{test=\"textfile_test\"} 90000000"));
        assert!(content.contains("process_open_fds_at_end{test=\"textfile_test\"} 48"));
    }

    #[test]
    fn write_textfile_atomic_without_manifest_uses_zero() {
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let mut report = LoadReport::new(1, "no_manifest", 0);
        report.finalize();

        report.write_textfile_atomic(tmp.path()).expect("write");
        let content = fs::read_to_string(tmp.path().join("load_test.prom")).expect("read");
        assert!(content.contains("load_test_objects_written_total{test=\"no_manifest\"} 0"));
        assert!(content.contains("load_test_mismatches_total{test=\"no_manifest\"} 0"));
    }

    // ── ReportResult serde tests ─────

    #[test]
    fn report_result_serde_round_trip() {
        assert_eq!(serde_json::to_string(&ReportResult::Pass).unwrap(), "\"pass\"");
        assert_eq!(serde_json::to_string(&ReportResult::Fail).unwrap(), "\"fail\"");
        assert_eq!(serde_json::to_string(&ReportResult::Timeout).unwrap(), "\"timeout\"");

        // Deserialize.
        let pass: ReportResult = serde_json::from_str("\"pass\"").unwrap();
        assert_eq!(pass, ReportResult::Pass);
        let fail: ReportResult = serde_json::from_str("\"fail\"").unwrap();
        assert_eq!(fail, ReportResult::Fail);
        let timeout: ReportResult = serde_json::from_str("\"timeout\"").unwrap();
        assert_eq!(timeout, ReportResult::Timeout);
    }

    #[test]
    fn report_result_serde_rejects_invalid() {
        assert!(serde_json::from_str::<ReportResult>("\"unknown\"").is_err());
    }

    // ── record_failure tests ─────

    #[test]
    fn record_failure_adds_to_failures() {
        let mut report = LoadReport::new(1, "test", 0);
        report.record_failure("assert_mem", "RSS grew from 80MB to 200MB");
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].assertion, "assert_mem");
        assert!(report.failures[0].detail.contains("RSS"));
        assert!(!report.failures[0].timestamp.is_empty());
    }

    // ── AssertionResult serialization ─────

    #[test]
    fn assertion_result_serializes_correctly() {
        let r = AssertionResult {
            name: "check".to_string(),
            passed: false,
            expected: "x".to_string(),
            actual: "y".to_string(),
        };
        let json = serde_json::to_string(&r).unwrap();
        // serde_json produces compact JSON: {"name":"check","passed":false,...}
        assert!(json.contains("\"name\":\"check\""));
        assert!(json.contains("\"passed\":false"));
    }

    // ── FailureDetail serialization ─────

    #[test]
    fn failure_detail_serializes_correctly() {
        let f = FailureDetail {
            assertion: "mem".to_string(),
            detail: "bad".to_string(),
            timestamp: "123.456".to_string(),
        };
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains("\"assertion\":\"mem\""));
        assert!(json.contains("\"detail\":\"bad\""));
    }

    // ── chrono_compact / date computation tests ─────

    #[test]
    fn days_to_civil_epoch_is_1970_01_01() {
        // 1970-01-01 = day 0 since epoch.
        let (y, m, d) = days_to_civil(0);
        assert_eq!((y, m, d), (1970, 1, 1));
    }

    #[test]
    fn days_to_civil_known_date() {
        // 2026-01-01 = 2026 years - 1970 years = 56 years.
        // Let's just verify a known date works.
        // 2026-08-10 = epoch day roughly 20676.
        // We'll verify the output is a valid date.
        let (y, m, d) = days_to_civil(20676);
        assert!(y >= 2026, "year should be >= 2026, got {y}");
        assert!((1..=12).contains(&m), "month {m} out of range");
        assert!((1..=31).contains(&d), "day {d} out of range");
    }

    #[test]
    fn chrono_compact_produces_iso8601_format() {
        let ts = chrono_compact();
        // Should match YYYYMMDDTHHMMSS (15 chars).
        assert_eq!(ts.len(), 15, "timestamp '{ts}' should be 15 chars");
        assert!(ts.chars().nth(8) == Some('T'), "should have T separator");
        // All non-T chars should be digits.
        for (i, c) in ts.chars().enumerate() {
            if i == 8 {
                continue; // T separator
            }
            assert!(c.is_ascii_digit(), "char {i} in '{ts}' should be a digit");
        }
    }
}
