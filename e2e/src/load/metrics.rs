//! Prometheus text-format parser and metrics snapshot diff.
//!
//! The [`MetricsSnapshot`] scrapes `GET /admin/metrics` from an OceanFS node,
//! parses the Prometheus text format into a `HashMap<String, f64>`, and
//! provides a [`delta`](MetricsSnapshot::delta) method for computing
//! counter differences between two snapshots.
//!
//! ## Parser
//!
//! The parser is lightweight (~80 lines). It skips `#` comments and
//! `# HELP`/`# TYPE` metadata, then splits each remaining line at the
//! last space to extract the metric name and floating-point value.
//! Labeled metrics (e.g. `http_requests_total{status="200"} 42`) are
//! stored with labels preserved in the key.
//!
//! ## Usage
//!
//! ```no_run
//! use e2e::harness::{config_standard, NodeProcess};
//! use e2e::load::MetricsSnapshot;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let node = NodeProcess::spawn(&config_standard()).await?;
//! let snap1 = MetricsSnapshot::scrape(&node).await?;
//! // ... wait, run load ...
//! let snap2 = MetricsSnapshot::scrape(&node).await?;
//! let diffs = snap2.delta(&snap1);
//! for (metric, delta) in &diffs {
//!     println!("{metric}: +{delta}");
//! }
//! # Ok(())
//! # }
//! ```

use std::{collections::HashMap, time::Instant};

use serde::Serialize;

use crate::harness::{Error, NodeProcess};

// ---------------------------------------------------------------------------
// MetricsSnapshot
// ---------------------------------------------------------------------------

/// A point-in-time scrape of `/admin/metrics` from an OceanFS node.
///
/// Contains a `HashMap<String, f64>` mapping metric names to their values.
/// Labeled metrics (e.g., `http_requests_total{status="200"}`) are stored
/// with labels preserved in the key.
///
/// # Examples
///
/// ```no_run
/// use e2e::harness::{config_standard, NodeProcess};
/// use e2e::load::MetricsSnapshot;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let node = NodeProcess::spawn(&config_standard()).await?;
/// let snap = MetricsSnapshot::scrape(&node).await?;
/// if let Some(rss) = snap.gauge("process_resident_memory_bytes") {
///     println!("RSS: {rss} bytes");
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    /// When the snapshot was taken (monotonic clock — not serialized).
    #[serde(skip)]
    pub timestamp: Instant,
    /// All parsed metric name → value mappings.
    pub metrics: HashMap<String, f64>,
}

impl MetricsSnapshot {
    /// Scrapes `GET /admin/metrics` from the given node and parses the
    /// Prometheus text-format response.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails or the response status
    /// is not 2xx. Malformed metric lines in the response body are
    /// silently skipped.
    pub async fn scrape(node: &NodeProcess) -> Result<Self, Error> {
        let resp = node.get("/admin/metrics").await?;
        if !resp.status().is_success() {
            return Err(Error::ClusterError(format!(
                "metrics endpoint returned {}",
                resp.status()
            )));
        }
        let text = resp.text().await?;
        let timestamp = Instant::now();
        let metrics = parse_prometheus_text(&text);
        Ok(Self { timestamp, metrics })
    }

    /// Reads a specific metric value by name.
    ///
    /// Returns `None` if the metric was not present in the scrape.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use e2e::load::MetricsSnapshot;
    ///
    /// let snap = MetricsSnapshot {
    ///     timestamp: std::time::Instant::now(),
    ///     metrics: HashMap::from([("cpu_usage".to_string(), 42.0)]),
    /// };
    /// assert_eq!(snap.gauge("cpu_usage"), Some(42.0));
    /// assert_eq!(snap.gauge("missing"), None);
    /// ```
    pub fn gauge(&self, name: &str) -> Option<f64> {
        self.metrics.get(name).copied()
    }

    /// Reads a specific counter value by name (alias for [`gauge`](Self::gauge)).
    ///
    /// Both counters and gauges are stored as `f64` values; the caller
    /// decides how to interpret them.
    pub fn counter(&self, name: &str) -> Option<f64> {
        self.gauge(name)
    }

    /// Computes the delta between this snapshot and a previous one.
    ///
    /// For each metric present in both snapshots, computes
    /// `current_value - previous_value`. Metrics that only appear in
    /// one snapshot are excluded from the result.
    ///
    /// Returns a map from metric name to the computed difference.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use e2e::load::MetricsSnapshot;
    ///
    /// let prev = MetricsSnapshot {
    ///     timestamp: std::time::Instant::now(),
    ///     metrics: HashMap::from([("requests_total".to_string(), 100.0)]),
    /// };
    /// let curr = MetricsSnapshot {
    ///     timestamp: std::time::Instant::now(),
    ///     metrics: HashMap::from([("requests_total".to_string(), 150.0)]),
    /// };
    /// let diffs = curr.delta(&prev);
    /// assert_eq!(diffs.get("requests_total"), Some(&50.0));
    /// ```
    pub fn delta(&self, prev: &Self) -> HashMap<String, f64> {
        let mut result = HashMap::with_capacity(self.metrics.len());

        for (name, current_value) in &self.metrics {
            if let Some(prev_value) = prev.metrics.get(name) {
                result.insert(name.clone(), current_value - prev_value);
            }
        }

        result
    }

    /// Returns the number of parsed metrics.
    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    /// Returns `true` if no metrics were parsed.
    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parses Prometheus text-format metrics from a response body.
///
/// Skips comment lines (`# ...`), blank lines, and lines whose value
/// cannot be parsed as `f64`. Each valid line is split at the last
/// space character to separate the metric name from its value.
///
/// This handles:
/// - Simple counters: `accel_fallback_total 0`
/// - Gauges: `process_resident_memory_bytes 87000000`
/// - Labeled metrics: `http_requests_total{status="200"} 42`
/// - Histogram buckets: `request_latency_bucket{le="0.1"} 100`
/// - Histogram summary: `request_latency_sum 12.5`
/// - Histogram count: `request_latency_count 500`
pub fn parse_prometheus_text(text: &str) -> HashMap<String, f64> {
    let mut metrics = HashMap::new();

    for line in text.lines() {
        let line = line.trim();

        // Skip comments and blank lines.
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Split at the last space: "metric_name{labels} value"
        // e.g., "s3_request_latency_seconds_bucket{le=\"0.005\"} 100"
        if let Some(last_space) = line.rfind(' ') {
            let name = line[..last_space].trim();
            let value_str = line[last_space + 1..].trim();

            match value_str.parse::<f64>() {
                Ok(value) => {
                    metrics.insert(name.to_string(), value);
                }
                Err(_) => {
                    eprintln!("metrics scraper: skipping malformed value in line: {line}");
                }
            }
        } else {
            eprintln!("metrics scraper: skipping line with no space separator: {line}");
        }
    }

    metrics
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::f64::consts::PI;

    use super::*;

    /// Sample Prometheus text output covering counters, gauges, histograms,
    /// and labeled metrics.
    const SAMPLE_METRICS: &str = r#"# HELP accel_fallback_total Number of accel fallbacks
# TYPE accel_fallback_total counter
accel_fallback_total 0
# HELP process_resident_memory_bytes RSS in bytes
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes 87000000
# HELP process_open_fds Number of open file descriptors
# TYPE process_open_fds gauge
process_open_fds 42
# HELP s3_request_latency_seconds S3 request latency
# TYPE s3_request_latency_seconds histogram
s3_request_latency_seconds_bucket{le="0.005"} 100
s3_request_latency_seconds_bucket{le="0.01"} 250
s3_request_latency_seconds_bucket{le="0.025"} 400
s3_request_latency_seconds_bucket{le="0.05"} 450
s3_request_latency_seconds_bucket{le="+Inf"} 500
s3_request_latency_seconds_sum 12.5
s3_request_latency_seconds_count 500
# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="PUT"} 1423
http_requests_total{method="GET"} 8920
http_requests_total{method="DELETE"} 45
# HELP storage_segment_count Number of segments
# TYPE storage_segment_count gauge
storage_segment_count 16
"#;

    // ── Parsing tests ─────

    #[test]
    fn parse_prometheus_text_parses_all_metrics() {
        let metrics = parse_prometheus_text(SAMPLE_METRICS);

        // Counter.
        assert_eq!(metrics.get("accel_fallback_total"), Some(&0.0));

        // Gauges.
        assert_eq!(metrics.get("process_resident_memory_bytes"), Some(&87_000_000.0));
        assert_eq!(metrics.get("process_open_fds"), Some(&42.0));
        assert_eq!(metrics.get("storage_segment_count"), Some(&16.0));

        // Histogram.
        assert_eq!(metrics.get("s3_request_latency_seconds_bucket{le=\"0.005\"}"), Some(&100.0));
        assert_eq!(metrics.get("s3_request_latency_seconds_bucket{le=\"0.01\"}"), Some(&250.0));
        assert_eq!(metrics.get("s3_request_latency_seconds_bucket{le=\"+Inf\"}"), Some(&500.0));
        assert_eq!(metrics.get("s3_request_latency_seconds_sum"), Some(&12.5));
        assert_eq!(metrics.get("s3_request_latency_seconds_count"), Some(&500.0));

        // Labeled metrics.
        assert_eq!(metrics.get("http_requests_total{method=\"PUT\"}"), Some(&1423.0));
        assert_eq!(metrics.get("http_requests_total{method=\"GET\"}"), Some(&8920.0));
        assert_eq!(metrics.get("http_requests_total{method=\"DELETE\"}"), Some(&45.0));

        // Verify total count: 3 gauges + 1 counter + 5 histogram buckets
        // + 1 histogram sum + 1 histogram count + 3 labeled counters = 14.
        assert_eq!(metrics.len(), 14, "expected 14 metrics, got {}", metrics.len());
    }

    #[test]
    fn parse_skips_malformed_lines() {
        let text = "\
good_counter 42
bad_line_no_value
another_good 3.14
not_a_number abc
still_good 7.0
";
        let metrics = parse_prometheus_text(text);
        assert_eq!(metrics.get("good_counter"), Some(&42.0));
        assert_eq!(metrics.get("another_good"), Some(&PI));
        assert_eq!(metrics.get("still_good"), Some(&7.0));
        // Malformed lines are skipped, no panic.
        assert_eq!(metrics.len(), 3);
    }

    #[test]
    fn parse_skips_empty_and_comment_lines() {
        let text = "\
\n
# This is a comment
# HELP some_metric A helpful description
# TYPE some_metric counter
some_metric 99
\n
";
        let metrics = parse_prometheus_text(text);
        assert_eq!(metrics.get("some_metric"), Some(&99.0));
        assert_eq!(metrics.len(), 1);
    }

    #[test]
    fn parse_handles_scientific_notation() {
        let text = "sci_metric 1.5e10\n";
        let metrics = parse_prometheus_text(text);
        assert_eq!(metrics.get("sci_metric"), Some(&15_000_000_000.0));
    }

    #[test]
    fn parse_handles_negative_values() {
        let text = "temp_celsius -5.0\n";
        let metrics = parse_prometheus_text(text);
        assert_eq!(metrics.get("temp_celsius"), Some(&-5.0));
    }

    #[test]
    fn parse_handles_infinity() {
        let text = "inf_metric +Inf\nneg_inf_metric -Inf\n";
        let metrics = parse_prometheus_text(text);
        assert_eq!(metrics.get("inf_metric"), Some(&f64::INFINITY));
        assert_eq!(metrics.get("neg_inf_metric"), Some(&f64::NEG_INFINITY));
    }

    #[test]
    fn parse_handles_nan() {
        let text = "nan_metric NaN\n";
        let metrics = parse_prometheus_text(text);
        assert!(metrics.get("nan_metric").unwrap().is_nan());
    }

    // ── Snapshot tests ─────

    #[test]
    fn gauge_returns_correct_value() {
        let snap = MetricsSnapshot {
            timestamp: Instant::now(),
            metrics: HashMap::from([("cpu".to_string(), 85.5)]),
        };
        assert_eq!(snap.gauge("cpu"), Some(85.5));
        assert_eq!(snap.gauge("missing"), None);
    }

    #[test]
    fn counter_is_alias_for_gauge() {
        let snap = MetricsSnapshot {
            timestamp: Instant::now(),
            metrics: HashMap::from([("requests".to_string(), 100.0)]),
        };
        assert_eq!(snap.counter("requests"), snap.gauge("requests"));
    }

    // ── Delta tests ─────

    #[test]
    fn delta_computes_difference_for_shared_keys() {
        let prev = MetricsSnapshot {
            timestamp: Instant::now(),
            metrics: HashMap::from([
                ("counter_a".to_string(), 100.0),
                ("counter_b".to_string(), 200.0),
                ("gauge_x".to_string(), 50.0),
            ]),
        };
        let curr = MetricsSnapshot {
            timestamp: Instant::now(),
            metrics: HashMap::from([
                ("counter_a".to_string(), 150.0),
                ("counter_b".to_string(), 200.0), // unchanged
                ("gauge_x".to_string(), 30.0),    // decreased
            ]),
        };

        let diffs = curr.delta(&prev);
        assert_eq!(diffs.get("counter_a"), Some(&50.0)); // 150 - 100
        assert_eq!(diffs.get("counter_b"), Some(&0.0)); // 200 - 200
        assert_eq!(diffs.get("gauge_x"), Some(&-20.0)); // 30 - 50
        assert_eq!(diffs.len(), 3);
    }

    #[test]
    fn delta_excludes_keys_present_in_only_one_snapshot() {
        let prev = MetricsSnapshot {
            timestamp: Instant::now(),
            metrics: HashMap::from([("old".to_string(), 10.0)]),
        };
        let curr = MetricsSnapshot {
            timestamp: Instant::now(),
            metrics: HashMap::from([("new".to_string(), 20.0)]),
        };
        let diffs = curr.delta(&prev);
        assert!(diffs.is_empty(), "no shared keys → empty delta");
    }

    #[test]
    fn delta_empty_snapshots() {
        let prev = MetricsSnapshot { timestamp: Instant::now(), metrics: HashMap::new() };
        let curr = MetricsSnapshot { timestamp: Instant::now(), metrics: HashMap::new() };
        let diffs = curr.delta(&prev);
        assert!(diffs.is_empty());
    }

    // ── Snapshot metadata tests ─────

    #[test]
    fn len_and_is_empty() {
        let snap = MetricsSnapshot {
            timestamp: Instant::now(),
            metrics: HashMap::from([("a".to_string(), 1.0)]),
        };
        assert_eq!(snap.len(), 1);
        assert!(!snap.is_empty());

        let empty = MetricsSnapshot { timestamp: Instant::now(), metrics: HashMap::new() };
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
    }

    #[test]
    fn timestamp_is_set() {
        let before = Instant::now();
        let snap = MetricsSnapshot { timestamp: Instant::now(), metrics: HashMap::new() };
        let after = Instant::now();
        assert!(snap.timestamp >= before);
        assert!(snap.timestamp <= after);
    }
}
