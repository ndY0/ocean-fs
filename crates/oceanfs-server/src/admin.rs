//! Admin API — cluster health, segment status, cache stats, and metrics.
//!
//! Exposes REST endpoints under `/admin/` for cluster visibility and
//! a `/admin/metrics` Prometheus text-format endpoint.
//!
//! Per performance guideline §11.1, all counters use `AtomicU64` with
//! relaxed ordering on the hot path.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::RwLock;
use oceanfs_core::SizeTier;
use serde::Serialize;
use tracing::instrument;

use crate::bucket_config::BucketConfigStore;

// ---------------------------------------------------------------------------
// MetricsRegistry
// ---------------------------------------------------------------------------

/// A Prometheus-compatible metrics registry.
///
/// All counters use `AtomicU64` with `Relaxed` ordering for minimal
/// overhead on the hot path. Histograms use a simple lock-protected
/// bucket accumulator.
pub struct MetricsRegistry {
    counters: RwLock<HashMap<String, Arc<Counter>>>,
    histograms: RwLock<HashMap<String, Arc<Histogram>>>,
}

impl MetricsRegistry {
    /// Creates a new empty metrics registry.
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }

    /// Registers or retrieves a counter by name.
    ///
    /// Returns an `Arc<Counter>` that can be shared across subsystems.
    /// If a counter with the given name already exists, it is returned
    /// instead of creating a new one.
    pub fn counter(&self, name: &str, help: &str) -> Arc<Counter> {
        let mut map = self.counters.write();
        map.entry(name.to_string())
            .or_insert_with(|| Arc::new(Counter::new(name.to_string(), help.to_string())))
            .clone()
    }

    /// Registers or retrieves a histogram by name.
    pub fn histogram(&self, name: &str, help: &str) -> Arc<Histogram> {
        let mut map = self.histograms.write();
        map.entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(Histogram::new(
                    name.to_string(),
                    help.to_string(),
                ))
            })
            .clone()
    }

    /// Gathers all registered metrics in Prometheus text exposition format.
    ///
    /// This is suitable for responding to `GET /admin/metrics`.
    pub fn gather(&self) -> String {
        let mut output = String::new();

        for counter in self.counters.read().values() {
            output.push_str(&counter.render());
            output.push('\n');
        }
        for histogram in self.histograms.read().values() {
            output.push_str(&histogram.render());
            output.push('\n');
        }

        output
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

/// A monotonically-increasing counter with Prometheus text-format output.
pub struct Counter {
    name: String,
    help: String,
    value: AtomicU64,
}

impl Counter {
    /// Creates a new counter with the given name and help text.
    pub fn new(name: String, help: String) -> Self {
        Self { name, help, value: AtomicU64::new(0) }
    }

    /// Increments the counter by 1.
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds `n` to the counter.
    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Returns the current counter value.
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Renders the counter in Prometheus text exposition format.
    fn render(&self) -> String {
        let val = self.value.load(Ordering::Relaxed);
        format!(
            "# HELP {} {}\n# TYPE {} counter\n{} {}\n",
            self.name, self.help, self.name, self.name, val
        )
    }
}

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

/// A simple histogram with fixed buckets.
///
/// For hot-path timing, use a dedicated timer or a higher-resolution
/// clock. This implementation is sufficient for coarse metrics like
/// batch sizes and operation latencies in milliseconds.
pub struct Histogram {
    name: String,
    help: String,
    sum: AtomicU64,
    count: AtomicU64,
    buckets: RwLock<Vec<u64>>,
    bucket_bounds: Vec<u64>,
}

impl Histogram {
    /// Creates a new histogram with the given name and help text.
    ///
    /// Buckets are the standard Prometheus defaults: [1, 5, 10, 50, 100,
    /// 250, 500, 1000, 2500, 5000, 10000].
    pub fn new(name: String, help: String) -> Self {
        let bucket_bounds = vec![1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000];
        let num_buckets = bucket_bounds.len();
        Self {
            name,
            help,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            buckets: RwLock::new(vec![0; num_buckets]),
            bucket_bounds,
        }
    }

    /// Observes a value, incrementing the appropriate bucket.
    pub fn observe(&self, value: u64) {
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        let mut buckets = self.buckets.write();
        for (i, bound) in self.bucket_bounds.iter().enumerate() {
            if value <= *bound {
                buckets[i] = buckets[i].wrapping_add(1);
                break;
            }
        }
    }

    /// Renders the histogram in Prometheus text exposition format.
    fn render(&self) -> String {
        let sum = self.sum.load(Ordering::Relaxed);
        let count = self.count.load(Ordering::Relaxed);
        let buckets = self.buckets.read();

        let mut out = format!(
            "# HELP {} {}\n# TYPE {} histogram\n",
            self.name, self.help, self.name
        );

        let mut cumulative = 0u64;
        for (i, bound) in self.bucket_bounds.iter().enumerate() {
            cumulative = cumulative.wrapping_add(buckets[i]);
            out.push_str(&format!(
                "{}_bucket{{le=\"{}\"}} {}\n",
                self.name, bound, cumulative
            ));
        }
        // +Inf bucket
        out.push_str(&format!(
            "{}_bucket{{le=\"+Inf\"}} {}\n",
            self.name, count
        ));
        out.push_str(&format!("{}_sum {}\n", self.name, sum));
        out.push_str(&format!("{}_count {}\n", self.name, count));

        out
    }
}

// ---------------------------------------------------------------------------
// ClusterView / NodeInfo / SegmentReport
// ---------------------------------------------------------------------------

/// Response for GET /admin/cluster.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterView {
    /// Nodes in the cluster with their states.
    pub nodes: Vec<NodeInfo>,
    /// Number of virtual nodes in the ring.
    pub vnodes: usize,
    /// Ring generation number (increments on membership changes).
    pub generation: u64,
}

/// Information about a single node in the cluster.
#[derive(Debug, Clone, Serialize)]
pub struct NodeInfo {
    /// Node identifier.
    pub id: String,
    /// Current state (Alive, Suspect, Dead, Leaving, Left).
    pub state: String,
    /// Incarnation number.
    pub incarnation: u64,
    /// Network address.
    pub address: String,
}

/// Response for GET /admin/segments.
#[derive(Debug, Clone, Serialize)]
pub struct SegmentReport {
    /// Total segment count.
    pub total: u64,
    /// Number of sealed segments.
    pub sealed: u64,
    /// Number of unsealed active segments.
    pub unsealed: u64,
    /// Number of segments currently being EC-encoded.
    pub encoding: u64,
    /// Segment counts broken down by size tier.
    #[serde(serialize_with = "serialize_tier_map")]
    pub by_tier: HashMap<SizeTier, u64>,
}

fn serialize_tier_map<S>(map: &HashMap<SizeTier, u64>, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let string_map: HashMap<String, u64> = map
        .iter()
        .map(|(k, v)| (format!("{:?}", k).to_lowercase(), *v))
        .collect();
    string_map.serialize(s)
}

/// Per-tier cache statistics.
#[derive(Debug, Clone, Serialize)]
pub struct CacheStats {
    /// Tier name (l1, l2, l3).
    pub tier: String,
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
}

// ---------------------------------------------------------------------------
// AppState
// ---------------------------------------------------------------------------

/// Shared state for admin handlers.
#[derive(Clone)]
pub(crate) struct AdminState {
    /// Bucket config store.
    pub buckets: Arc<BucketConfigStore>,
    /// Metrics registry.
    pub metrics: Arc<MetricsRegistry>,
}

// ---------------------------------------------------------------------------
// AdminHandler
// ---------------------------------------------------------------------------

/// Admin API handler.
///
/// Exposes cluster status, segment health, cache statistics,
/// scrub triggering, and Prometheus metrics.
pub struct AdminHandler {
    state: AdminState,
}

impl AdminHandler {
    /// Creates a new admin handler.
    pub fn new(
        buckets: Arc<BucketConfigStore>,
        metrics: Arc<MetricsRegistry>,
    ) -> Self {
        Self { state: AdminState { buckets, metrics } }
    }

    /// Consumes the handler and returns an axum `Router` for the
    /// `/admin/` prefix.
    pub fn into_router(self) -> Router {
        let state = self.state;

        Router::new()
            .route("/admin/cluster", get(cluster_view))
            .route("/admin/segments", get(segment_report))
            .route("/admin/caches", get(cache_stats))
            .route("/admin/scrub", post(trigger_scrub))
            .route("/admin/metrics", get(metrics_endpoint))
            .with_state(state)
    }
}

/// GET /admin/cluster — returns cluster membership view as JSON.
#[instrument(skip(state))]
async fn cluster_view(State(state): State<AdminState>) -> impl IntoResponse {
    // In a full implementation, this would query Membership and RingCache.
    // For now, return a placeholder with bucket count as a useful data point.
    let buckets = state.buckets.list();
    let view = ClusterView {
        nodes: Vec::new(),
        vnodes: 0,
        generation: 0,
    };
    let _ = buckets; // used when Membership is wired

    Json(view).into_response()
}

/// GET /admin/segments — returns segment health report as JSON.
#[instrument]
async fn segment_report() -> impl IntoResponse {
    let report = SegmentReport {
        total: 0,
        sealed: 0,
        unsealed: 0,
        encoding: 0,
        by_tier: HashMap::new(),
    };
    Json(report).into_response()
}

/// GET /admin/caches — returns per-tier cache hit/miss stats as JSON.
#[instrument]
async fn cache_stats() -> impl IntoResponse {
    let stats = vec![
        CacheStats { tier: "l1".into(), hits: 0, misses: 0 },
        CacheStats { tier: "l2".into(), hits: 0, misses: 0 },
        CacheStats { tier: "l3".into(), hits: 0, misses: 0 },
    ];
    Json(stats).into_response()
}

/// POST /admin/scrub — triggers a full distributed scrub.
#[instrument]
async fn trigger_scrub() -> impl IntoResponse {
    // In a full implementation, this would send a scrub command
    // to the ScrubCoordinator. For now, acknowledge the request.
    (StatusCode::ACCEPTED, "Scrub triggered").into_response()
}

/// GET /admin/metrics — returns Prometheus text-format metrics.
#[instrument(skip(state))]
async fn metrics_endpoint(State(state): State<AdminState>) -> impl IntoResponse {
    let body = state.metrics.gather();
    (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "text/plain")], body)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // --- MetricsRegistry ---

    #[test]
    fn counter_increment_is_correct() {
        let reg = MetricsRegistry::new();
        let c = reg.counter("test_total", "help");
        c.inc();
        c.inc();
        assert_eq!(c.get(), 2);
    }

    #[test]
    fn counter_add_large_value() {
        let reg = MetricsRegistry::new();
        let c = reg.counter("bytes_total", "help");
        c.add(1024);
        c.add(2048);
        assert_eq!(c.get(), 3072);
    }

    #[test]
    fn counter_render_includes_help_and_type() {
        let reg = MetricsRegistry::new();
        let c = reg.counter("req_total", "Total requests");
        c.inc();
        let rendered = c.render();
        assert!(rendered.contains("# HELP req_total Total requests"));
        assert!(rendered.contains("# TYPE req_total counter"));
        assert!(rendered.contains("req_total 1"));
    }

    #[test]
    fn same_counter_name_returns_same_instance() {
        let reg = MetricsRegistry::new();
        let c1 = reg.counter("x", "h1");
        let c2 = reg.counter("x", "h2");
        c1.inc();
        assert_eq!(c2.get(), 1);
    }

    #[test]
    fn histogram_observation_updates_sum_and_count() {
        let reg = MetricsRegistry::new();
        let h = reg.histogram("latency_ms", "Request latency");
        h.observe(5);
        h.observe(100);
        // sum and count use Relaxed ordering; in single-threaded tests this works fine
        let rendered = h.render();
        assert!(rendered.contains("latency_ms_sum 105"));
        assert!(rendered.contains("latency_ms_count 2"));
    }

    #[test]
    fn histogram_render_includes_buckets() {
        let reg = MetricsRegistry::new();
        let h = reg.histogram("l_ms", "latency");
        h.observe(45);
        h.observe(8);
        let rendered = h.render();
        assert!(rendered.contains("l_ms_bucket{le=\"50\"}"));
    }

    #[test]
    fn gather_outputs_all_registered_metrics() {
        let reg = MetricsRegistry::new();
        reg.counter("a_total", "help a");
        reg.counter("b_total", "help b");
        let out = reg.gather();
        assert!(out.contains("a_total"));
        assert!(out.contains("b_total"));
    }

    // --- AdminHandler ---

    #[test]
    fn admin_handler_can_be_constructed() {
        let buckets = Arc::new(BucketConfigStore::new());
        let metrics = Arc::new(MetricsRegistry::new());
        let handler = AdminHandler::new(buckets, metrics);
        let router = handler.into_router();
        // Verify router can be constructed
        let _ = router;
    }

    #[test]
    fn cluster_view_is_json_serializable() {
        let view = ClusterView {
            nodes: vec![NodeInfo {
                id: "n1".into(),
                state: "Alive".into(),
                incarnation: 1,
                address: "127.0.0.1:9001".into(),
            }],
            vnodes: 256,
            generation: 5,
        };
        let json = serde_json::to_string(&view).unwrap();
        assert!(json.contains("n1"));
        assert!(json.contains("Alive"));
        assert!(json.contains("256"));
    }

    #[test]
    fn segment_report_is_json_serializable() {
        let mut by_tier = HashMap::new();
        by_tier.insert(SizeTier::Inline, 10);
        by_tier.insert(SizeTier::Standard, 5);
        let report = SegmentReport {
            total: 15,
            sealed: 5,
            unsealed: 10,
            encoding: 2,
            by_tier,
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("inline"));
        assert!(json.contains("standard"));
        assert!(json.contains("15"));
    }

    #[test]
    fn cache_stats_is_json_serializable() {
        let stats = vec![
            CacheStats { tier: "l1".into(), hits: 100, misses: 5 },
            CacheStats { tier: "l2".into(), hits: 50, misses: 3 },
        ];
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("l1"));
        assert!(json.contains("100"));
        assert!(json.contains("l2"));
    }

    // --- Concurrency smoke test ---

    #[test]
    fn counter_concurrent_increments() {
        use std::thread;

        let reg = Arc::new(MetricsRegistry::new());
        let c = reg.counter("concurrent_total", "concurrency test");

        let mut handles = Vec::new();
        for _ in 0..8 {
            let c = c.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    c.inc();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(c.get(), 8000);
    }
}
