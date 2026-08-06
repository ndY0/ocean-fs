//! Metrics primitives — Counter, Gauge, LabelSet — and the
//! [`MetricRegistrar`] trait for registering metrics into a registry.
//!
//! These types are the building blocks for Prometheus-compatible
//! instrumentation. All counters and gauges use `AtomicU64` with
//! `Relaxed` ordering for minimal overhead on the hot path.
//!
//! The [`MetricRegistrar`] trait allows subsystems (like the cache
//! crates) to register their pre-constructed counters and gauges
//! without creating a circular dependency on the server crate.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

// ---------------------------------------------------------------------------
// LabelSet
// ---------------------------------------------------------------------------

/// An ordered set of label name-value pairs for metric identification.
///
/// Labels are rendered in Prometheus format as `{name1="value1",name2="value2"}`.
/// The label key is used to deduplicate metrics in the registry — two metrics
/// with the same name but different label sets are treated as distinct series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LabelSet {
    pairs: Vec<(String, String)>,
}

impl LabelSet {
    /// Creates a label set from an ordered slice of (name, value) pairs.
    pub fn new(pairs: &[(&str, &str)]) -> Self {
        Self { pairs: pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect() }
    }

    /// Returns an empty label set.
    pub fn empty() -> Self {
        Self { pairs: Vec::new() }
    }

    /// Returns true if there are no labels.
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// Renders labels in Prometheus format: `{key1="val1",key2="val2"}`
    pub fn render(&self) -> String {
        if self.pairs.is_empty() {
            String::new()
        } else {
            let inner: Vec<String> =
                self.pairs.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
            format!("{{{}}}", inner.join(","))
        }
    }
}

/// Validates that a counter name ends with `_total` (Prometheus best practice).
///
/// Returns the name unchanged if valid, or appends `_total` if missing.
pub fn validate_counter_name(name: &str) -> String {
    if name.ends_with("_total") {
        name.to_string()
    } else {
        format!("{name}_total")
    }
}

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

/// The shared inner state of a [`Counter`].
///
/// Wrapped in `Arc` so that `Counter` is cheaply clonable — subsystems
/// store `Counter` directly on stack/frame; the registry holds a clone
/// pointing to the same `AtomicU64`.
#[derive(Debug)]
struct CounterInner {
    name: String,
    help: String,
    labels: LabelSet,
    value: AtomicU64,
}

/// A monotonically-increasing counter with Prometheus text-format output.
///
/// Supports optional labels for dimensional metrics (e.g., `tier="l1"`).
/// Cheap to clone — inner `AtomicU64` is shared via `Arc`.
#[derive(Debug, Clone)]
pub struct Counter {
    inner: Arc<CounterInner>,
}

impl Counter {
    /// Creates a new counter with the given name, help text, and optional labels.
    pub fn new(name: String, help: String, labels: LabelSet) -> Self {
        Self { inner: Arc::new(CounterInner { name, help, labels, value: AtomicU64::new(0) }) }
    }

    /// Increments the counter by 1.
    pub fn inc(&self) {
        self.inner.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds `n` to the counter.
    pub fn add(&self, n: u64) {
        self.inner.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Returns the current counter value.
    pub fn get(&self) -> u64 {
        self.inner.value.load(Ordering::Relaxed)
    }

    /// Returns the counter name (without labels).
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns the counter's label set.
    pub fn labels(&self) -> &LabelSet {
        &self.inner.labels
    }

    /// Renders the counter in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let val = self.inner.value.load(Ordering::Relaxed);
        let labels = self.inner.labels.render();
        format!(
            "# HELP {name} {help}\n# TYPE {name} counter\n{name}{labels} {val}\n",
            name = self.inner.name,
            help = self.inner.help,
            labels = labels,
            val = val,
        )
    }
}

// ---------------------------------------------------------------------------
// Gauge
// ---------------------------------------------------------------------------

/// The shared inner state of a [`Gauge`].
#[derive(Debug)]
struct GaugeInner {
    name: String,
    help: String,
    labels: LabelSet,
    value: AtomicU64,
}

/// A non-monotonic gauge metric backed by `AtomicU64`.
///
/// Supports `set()`, `inc()`, and `dec()` operations. Cheap to clone —
/// inner state is shared via `Arc`.
#[derive(Debug, Clone)]
pub struct Gauge {
    inner: Arc<GaugeInner>,
}

impl Gauge {
    /// Creates a new gauge with the given name, help text, and optional labels.
    pub fn new(name: String, help: String, labels: LabelSet) -> Self {
        Self { inner: Arc::new(GaugeInner { name, help, labels, value: AtomicU64::new(0) }) }
    }

    /// Sets the gauge to an absolute value.
    pub fn set(&self, v: u64) {
        self.inner.value.store(v, Ordering::Relaxed);
    }

    /// Increments the gauge by 1.
    pub fn inc(&self) {
        self.inner.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the gauge by 1. Does not underflow below 0.
    pub fn dec(&self) {
        let mut current = self.inner.value.load(Ordering::Relaxed);
        while current > 0 {
            match self.inner.value.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Adds `n` to the gauge.
    pub fn add(&self, n: u64) {
        self.inner.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Subtracts `n` from the gauge. Saturates at 0.
    pub fn sub(&self, n: u64) {
        let mut current = self.inner.value.load(Ordering::Relaxed);
        loop {
            let new = current.saturating_sub(n);
            match self.inner.value.compare_exchange_weak(
                current,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Returns the gauge name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns the gauge's label set.
    pub fn labels(&self) -> &LabelSet {
        &self.inner.labels
    }

    /// Returns the current gauge value.
    pub fn get(&self) -> u64 {
        self.inner.value.load(Ordering::Relaxed)
    }

    /// Renders the gauge in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let val = self.inner.value.load(Ordering::Relaxed);
        let labels = self.inner.labels.render();
        format!(
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name}{labels} {val}\n",
            name = self.inner.name,
            help = self.inner.help,
            labels = labels,
            val = val,
        )
    }
}

// ---------------------------------------------------------------------------
// MetricRegistrar trait
// ---------------------------------------------------------------------------

/// Trait for registering pre-constructed counters and gauges into a
/// metrics registry.
///
/// This trait lives in `oceanfs-core` so that subsystems (such as the
/// cache crates) can accept a generic registrar without depending on
/// `oceanfs-server`. The server crate's `MetricsRegistry` implements
/// this trait.
pub trait MetricRegistrar {
    /// Registers a pre-constructed counter with the registry.
    ///
    /// If a counter with the same name and labels already exists,
    /// the existing instance is retained and the provided counter
    /// is discarded. `Counter` is cheap to clone — registers a snapshot.
    fn register_counter(&self, counter: Counter);

    /// Registers a pre-constructed gauge with the registry.
    fn register_gauge(&self, gauge: Gauge);

    /// Registers a pre-constructed histogram with the registry.
    fn register_histogram(&self, histogram: Arc<Histogram>);
}

// ---------------------------------------------------------------------------
// HistogramConfig
// ---------------------------------------------------------------------------

/// Configuration for histogram bucket boundaries.
///
/// # Examples
///
/// ```
/// use oceanfs_core::HistogramConfig;
///
/// let config = HistogramConfig::default();
/// assert!(config.buckets.contains(&1));
/// ```
#[derive(Debug, Clone)]
pub struct HistogramConfig {
    /// Bucket boundaries (upper bounds) in ascending order.
    pub buckets: Vec<u64>,
}

impl Default for HistogramConfig {
    fn default() -> Self {
        Self { buckets: vec![1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000] }
    }
}

/// Sub-millisecond histogram config for EC/hash timing operations.
pub fn sub_millisecond_histogram_config() -> HistogramConfig {
    HistogramConfig {
        buckets: vec![
            1,          // 0.001 ms
            5,          // 0.005 ms
            10,         // 0.01 ms
            50,         // 0.05 ms
            100,        // 0.1 ms
            500,        // 0.5 ms
            1000,       // 1 ms
            5_000,      // 5 ms
            10_000,     // 10 ms
            50_000,     // 50 ms
            100_000,    // 100 ms
            500_000,    // 500 ms
            1_000_000,  // 1 s
            5_000_000,  // 5 s
            10_000_000, // 10 s
        ],
    }
}

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

/// A lock-free histogram with per-bucket `AtomicU64` counters.
///
/// The `observe()` method uses only atomic operations — no locks
/// are acquired. This satisfies perf guidelines §11.1 and §7.1.
#[derive(Debug)]
pub struct Histogram {
    name: String,
    help: String,
    labels: LabelSet,
    sum: AtomicU64,
    count: AtomicU64,
    buckets: Vec<AtomicU64>,
    bucket_bounds: Vec<u64>,
}

impl Histogram {
    /// Creates a new histogram with the given name, help, config, and labels.
    pub fn new(name: String, help: String, config: &HistogramConfig, labels: LabelSet) -> Self {
        let num_buckets = config.buckets.len();
        let buckets: Vec<AtomicU64> = (0..num_buckets).map(|_| AtomicU64::new(0)).collect();
        Self {
            name,
            help,
            labels,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            buckets,
            bucket_bounds: config.buckets.clone(),
        }
    }

    /// Observes a value, incrementing the appropriate bucket.
    ///
    /// This method is lock-free — all bucket updates use `AtomicU64::fetch_add`.
    pub fn observe(&self, value: u64) {
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        for (i, bound) in self.bucket_bounds.iter().enumerate() {
            if value <= *bound {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    /// Returns the current sum of all observed values.
    pub fn sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    /// Returns the total count of observations.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Returns the histogram's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the histogram's label set.
    pub fn labels(&self) -> &LabelSet {
        &self.labels
    }

    /// Renders the histogram in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let sum = self.sum.load(Ordering::Relaxed);
        let count = self.count.load(Ordering::Relaxed);
        let labels = self.labels.render();

        let mut out = format!(
            "# HELP {name} {help}\n# TYPE {name} histogram\n",
            name = self.name,
            help = self.help,
        );

        let mut cumulative = 0u64;
        for (i, bound) in self.bucket_bounds.iter().enumerate() {
            cumulative = cumulative.wrapping_add(self.buckets[i].load(Ordering::Relaxed));
            out.push_str(&format!(
                "{name}_bucket{{le=\"{bound}\"}}{labels} {cumulative}\n",
                name = self.name,
                bound = bound,
                labels = labels,
                cumulative = cumulative,
            ));
        }
        out.push_str(&format!(
            "{name}_bucket{{le=\"+Inf\"}}{labels} {count}\n",
            name = self.name,
            labels = labels,
            count = count,
        ));
        out.push_str(&format!(
            "{name}_sum{labels} {sum}\n{name}_count{labels} {count}\n",
            name = self.name,
            labels = labels,
            sum = sum,
            count = count,
        ));

        out
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn counter_increment_is_correct() {
        let c = Counter::new("test_total".into(), "help".into(), LabelSet::empty());
        c.inc();
        c.inc();
        assert_eq!(c.get(), 2);
    }

    #[test]
    fn counter_add_large_value() {
        let c = Counter::new("bytes_total".into(), "help".into(), LabelSet::empty());
        c.add(1024);
        c.add(2048);
        assert_eq!(c.get(), 3072);
    }

    #[test]
    fn counter_render_includes_help_and_type() {
        let c = Counter::new("req_total".into(), "Total requests".into(), LabelSet::empty());
        c.inc();
        let rendered = c.render();
        assert!(rendered.contains("# HELP req_total Total requests"));
        assert!(rendered.contains("# TYPE req_total counter"));
        assert!(rendered.contains("req_total 1"));
    }

    #[test]
    fn gauge_set_and_get() {
        let g = Gauge::new("mem".into(), "help".into(), LabelSet::empty());
        g.set(1024);
        assert_eq!(g.get(), 1024);
        g.set(0);
        assert_eq!(g.get(), 0);
    }

    #[test]
    fn gauge_inc_and_dec() {
        let g = Gauge::new("fd".into(), "help".into(), LabelSet::empty());
        g.set(10);
        g.inc();
        g.inc();
        assert_eq!(g.get(), 12);
        g.dec();
        assert_eq!(g.get(), 11);
    }

    #[test]
    fn gauge_dec_does_not_underflow() {
        let g = Gauge::new("x".into(), "help".into(), LabelSet::empty());
        g.set(1);
        g.dec();
        assert_eq!(g.get(), 0);
        g.dec();
        assert_eq!(g.get(), 0);
    }

    #[test]
    fn gauge_add_and_sub() {
        let g = Gauge::new("x".into(), "help".into(), LabelSet::empty());
        g.add(100);
        assert_eq!(g.get(), 100);
        g.sub(30);
        assert_eq!(g.get(), 70);
        g.sub(200);
        assert_eq!(g.get(), 0);
    }

    #[test]
    fn gauge_render_includes_value() {
        let g = Gauge::new("process_open_fds".into(), "Open FDs".into(), LabelSet::empty());
        g.set(42);
        let rendered = g.render();
        assert!(rendered.contains("# HELP process_open_fds Open FDs"));
        assert!(rendered.contains("# TYPE process_open_fds gauge"));
        assert!(rendered.contains("process_open_fds 42"));
    }

    #[test]
    fn labeled_gauge_renders_labels() {
        let g = Gauge::new(
            "accel_tier_active".into(),
            "help".into(),
            LabelSet::new(&[("tier", "gpu_cuda"), ("operation", "encode")]),
        );
        g.set(1);
        let rendered = g.render();
        assert!(rendered.contains(r#"accel_tier_active{tier="gpu_cuda",operation="encode"} 1"#));
    }

    #[test]
    fn label_set_empty_renders_empty() {
        let labels = LabelSet::empty();
        assert_eq!(labels.render(), "");
    }

    #[test]
    fn label_set_single_pair() {
        let labels = LabelSet::new(&[("tier", "l1")]);
        assert_eq!(labels.render(), r#"{tier="l1"}"#);
    }

    #[test]
    fn label_set_multiple_pairs() {
        let labels = LabelSet::new(&[("from_tier", "gpu_cuda"), ("to_tier", "cpu_simd")]);
        assert_eq!(labels.render(), r#"{from_tier="gpu_cuda",to_tier="cpu_simd"}"#);
    }

    #[test]
    fn counter_with_labels_renders_correctly() {
        let labels = LabelSet::new(&[("method", "GET")]);
        let c = Counter::new("s3_requests_total".into(), "S3 requests".into(), labels);
        c.inc();
        let rendered = c.render();
        assert!(rendered.contains(r#"s3_requests_total{method="GET"} 1"#));
    }

    #[test]
    fn counter_name_gets_total_suffix() {
        let name = validate_counter_name("requests");
        assert_eq!(name, "requests_total");
        let name = validate_counter_name("already_total");
        assert_eq!(name, "already_total");
    }
}
