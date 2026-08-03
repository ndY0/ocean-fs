//! Admin API — cluster health, segment status, cache stats, and metrics.
//!
//! Exposes REST endpoints under `/admin/` for cluster visibility and
//! a `/admin/metrics` Prometheus text-format endpoint.
//!
//! Per performance guideline §11.1, all counters use `AtomicU64` with
//! relaxed ordering on the hot path.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
#[cfg(feature = "accel")]
use oceanfs_accel::AccelDispatcher;
#[cfg(feature = "cache")]
use oceanfs_cache::{MetadataCache, NegativeCache, ObjectCache};
use oceanfs_core::SizeTier;
use parking_lot::RwLock;
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
        Self { counters: RwLock::new(HashMap::new()), histograms: RwLock::new(HashMap::new()) }
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
            .or_insert_with(|| Arc::new(Histogram::new(name.to_string(), help.to_string())))
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

        let mut out =
            format!("# HELP {} {}\n# TYPE {} histogram\n", self.name, self.help, self.name);

        let mut cumulative = 0u64;
        for (i, bound) in self.bucket_bounds.iter().enumerate() {
            cumulative = cumulative.wrapping_add(buckets[i]);
            out.push_str(&format!("{}_bucket{{le=\"{}\"}} {}\n", self.name, bound, cumulative));
        }
        // +Inf bucket
        out.push_str(&format!("{}_bucket{{le=\"+Inf\"}} {}\n", self.name, count));
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
    let string_map: HashMap<String, u64> =
        map.iter().map(|(k, v)| (format!("{:?}", k).to_lowercase(), *v)).collect();
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
    #[allow(dead_code)]
    pub buckets: Arc<BucketConfigStore>,
    /// Metrics registry.
    pub metrics: Arc<MetricsRegistry>,
    /// Membership for cluster view (optional).
    pub membership: Option<Arc<oceanfs_membership::Membership>>,
    /// Ring cache for topology data (optional).
    pub ring_cache: Option<Arc<oceanfs_routing::RingCache>>,
    /// Scrub coordinator for manual scrub triggering (storage feature only).
    #[cfg(feature = "storage")]
    pub scrub_coordinator: Option<Arc<oceanfs_storage::ScrubCoordinator>>,
    /// Metadata store for scrub verification (storage feature only).
    #[cfg(feature = "storage")]
    pub metadata_store: Option<Arc<oceanfs_storage::MetadataStore>>,
    /// Segment data store for scrub (storage feature only).
    #[cfg(feature = "storage")]
    pub data_store: Option<Arc<dyn oceanfs_storage::SegmentDataStore>>,
    /// L1 object cache for cache stats (cache feature only).
    #[cfg(feature = "cache")]
    pub object_cache: Option<Arc<ObjectCache>>,
    /// L2 metadata cache for cache stats (cache feature only).
    #[cfg(feature = "cache")]
    pub metadata_cache: Option<Arc<MetadataCache>>,
    /// L3 negative cache for cache stats (cache feature only).
    #[cfg(feature = "cache")]
    pub negative_cache: Option<Arc<NegativeCache>>,
    /// Acceleration dispatcher for hardware status (accel feature only).
    #[cfg(feature = "accel")]
    pub accel: Option<Arc<AccelDispatcher>>,
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
    pub fn new(buckets: Arc<BucketConfigStore>, metrics: Arc<MetricsRegistry>) -> Self {
        Self {
            state: AdminState {
                buckets,
                metrics,
                membership: None,
                ring_cache: None,
                #[cfg(feature = "storage")]
                scrub_coordinator: None,
                #[cfg(feature = "storage")]
                metadata_store: None,
                #[cfg(feature = "storage")]
                data_store: None,
                #[cfg(feature = "cache")]
                object_cache: None,
                #[cfg(feature = "cache")]
                metadata_cache: None,
                #[cfg(feature = "cache")]
                negative_cache: None,
                #[cfg(feature = "accel")]
                accel: None,
            },
        }
    }

    /// Creates a new admin handler with full cluster context.
    pub fn new_with_cluster(
        buckets: Arc<BucketConfigStore>,
        metrics: Arc<MetricsRegistry>,
        membership: Arc<oceanfs_membership::Membership>,
        ring_cache: Arc<oceanfs_routing::RingCache>,
    ) -> Self {
        Self {
            state: AdminState {
                buckets,
                metrics,
                membership: Some(membership),
                ring_cache: Some(ring_cache),
                #[cfg(feature = "storage")]
                scrub_coordinator: None,
                #[cfg(feature = "storage")]
                metadata_store: None,
                #[cfg(feature = "storage")]
                data_store: None,
                #[cfg(feature = "cache")]
                object_cache: None,
                #[cfg(feature = "cache")]
                metadata_cache: None,
                #[cfg(feature = "cache")]
                negative_cache: None,
                #[cfg(feature = "accel")]
                accel: None,
            },
        }
    }

    /// Enables scrub triggering via the admin API.
    ///
    /// When configured, `POST /admin/scrub` will call
    /// `ScrubCoordinator::trigger_manual()` with the provided
    /// metadata and data stores.
    #[cfg(feature = "storage")]
    pub fn with_scrub(
        mut self,
        coordinator: Arc<oceanfs_storage::ScrubCoordinator>,
        metadata: Arc<oceanfs_storage::MetadataStore>,
        data_store: Arc<dyn oceanfs_storage::SegmentDataStore>,
    ) -> Self {
        self.state.scrub_coordinator = Some(coordinator);
        self.state.metadata_store = Some(metadata);
        self.state.data_store = Some(data_store);
        self
    }

    /// Wires cache instances for real cache statistics.
    ///
    /// When configured, `GET /admin/caches` returns real hit/miss data
    /// from the provided cache instances.
    #[cfg(feature = "cache")]
    pub fn with_caches(
        mut self,
        object_cache: Option<Arc<ObjectCache>>,
        metadata_cache: Option<Arc<MetadataCache>>,
        negative_cache: Option<Arc<NegativeCache>>,
    ) -> Self {
        self.state.object_cache = object_cache;
        self.state.metadata_cache = metadata_cache;
        self.state.negative_cache = negative_cache;
        self
    }

    /// Wires the acceleration dispatcher for hardware status reporting.
    ///
    /// When configured, `GET /admin/acceleration` returns the active
    /// acceleration tier and available backends.
    #[cfg(feature = "accel")]
    pub fn with_accel(mut self, accel: Arc<AccelDispatcher>) -> Self {
        self.state.accel = Some(accel);
        self
    }

    /// Consumes the handler and returns an axum `Router` for the
    /// `/admin/` prefix.
    pub fn into_router(self) -> Router {
        let state = self.state;

        Router::new()
            .route("/admin/health", get(health_check))
            .route("/admin/cluster", get(cluster_view))
            .route("/admin/segments", get(segment_report))
            .route("/admin/caches", get(cache_stats))
            .route("/admin/scrub", post(trigger_scrub))
            .route("/admin/metrics", get(metrics_endpoint))
            .route("/admin/acceleration", get(acceleration_status))
            .with_state(state)
    }
}

/// GET /admin/health — returns 200 OK when the node is running.
#[instrument]
async fn health_check() -> impl IntoResponse {
    let body = serde_json::json!({
        "status": "healthy",
        "version": env!("CARGO_PKG_VERSION"),
    });
    (StatusCode::OK, Json(body)).into_response()
}

/// GET /admin/cluster — returns cluster membership view as JSON.
#[instrument(skip(state))]
async fn cluster_view(State(state): State<AdminState>) -> impl IntoResponse {
    let nodes: Vec<NodeInfo> = if let Some(ref membership) = state.membership {
        membership
            .nodes_full()
            .into_iter()
            .map(|(node_id, node_state, incarnation, addr)| NodeInfo {
                id: node_id.to_string(),
                state: format!("{:?}", node_state),
                address: addr.to_string(),
                incarnation: incarnation.value(),
            })
            .collect()
    } else {
        Vec::new()
    };

    let vnodes = state.ring_cache.as_ref().map(|_rc| 256usize).unwrap_or(0);

    let view = ClusterView { nodes, vnodes, generation: 0 };
    Json(view).into_response()
}

/// GET /admin/segments — returns segment health report as JSON.
#[instrument(skip(state))]
async fn segment_report(State(state): State<AdminState>) -> impl IntoResponse {
    #[cfg(feature = "storage")]
    {
        if let Some(ref metadata) = state.metadata_store {
            let mut total: u64 = 0;
            let mut sealed: u64 = 0;
            let mut unsealed: u64 = 0;
            let mut by_tier: HashMap<SizeTier, u64> = HashMap::new();

            let segments = metadata.list_segments();
            for seg in segments.into_iter().flatten() {
                total += 1;
                if seg.is_sealed() {
                    sealed += 1;
                } else {
                    unsealed += 1;
                }
                *by_tier.entry(seg.size_tier).or_insert(0) += 1;
            }

            let report = SegmentReport {
                total,
                sealed,
                unsealed,
                encoding: 0, // encoding state not tracked in segment metadata
                by_tier,
            };
            return Json(report).into_response();
        }
    }
    let report =
        SegmentReport { total: 0, sealed: 0, unsealed: 0, encoding: 0, by_tier: HashMap::new() };
    Json(report).into_response()
}

/// GET /admin/caches — returns per-tier cache hit/miss stats as JSON.
#[instrument(skip(state))]
async fn cache_stats(State(state): State<AdminState>) -> impl IntoResponse {
    let mut stats = Vec::new();

    #[cfg(feature = "cache")]
    {
        if let Some(ref object_cache) = state.object_cache {
            let s = object_cache.stats();
            stats.push(CacheStats {
                tier: "l1".into(),
                hits: s.hits.load(Ordering::Relaxed),
                misses: s.misses.load(Ordering::Relaxed),
            });
        }
        if let Some(ref meta_cache) = state.metadata_cache {
            let s = meta_cache.stats();
            stats.push(CacheStats {
                tier: "l2".into(),
                hits: s.hits.load(Ordering::Relaxed),
                misses: s.misses.load(Ordering::Relaxed),
            });
        }
        if let Some(ref neg_cache) = state.negative_cache {
            let s = neg_cache.stats();
            stats.push(CacheStats {
                tier: "l3".into(),
                hits: s.hits.load(Ordering::Relaxed),
                misses: s.false_positives.load(Ordering::Relaxed),
            });
        }
    }

    if stats.is_empty() {
        stats = vec![
            CacheStats { tier: "l1".into(), hits: 0, misses: 0 },
            CacheStats { tier: "l2".into(), hits: 0, misses: 0 },
            CacheStats { tier: "l3".into(), hits: 0, misses: 0 },
        ];
    }

    Json(stats).into_response()
}

/// POST /admin/scrub — triggers a full distributed scrub.
#[instrument(skip(state))]
async fn trigger_scrub(State(state): State<AdminState>) -> impl IntoResponse {
    #[cfg(feature = "storage")]
    {
        if let (Some(coordinator), Some(metadata), Some(data_store)) =
            (&state.scrub_coordinator, &state.metadata_store, &state.data_store)
        {
            let coordinator = coordinator.clone();
            let metadata = metadata.clone();
            let data_store = data_store.clone();
            match coordinator.trigger_manual(metadata, data_store).await {
                Ok(()) => {
                    tracing::info!("scrub triggered via admin API");
                    return (StatusCode::ACCEPTED, "Scrub triggered").into_response();
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to trigger scrub via admin API");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to trigger scrub: {e}"),
                    )
                        .into_response();
                }
            }
        }
        tracing::warn!("scrub trigger requested but scrub coordinator not configured");
        return (StatusCode::SERVICE_UNAVAILABLE, "Scrub coordinator not configured on this node")
            .into_response();
    }
    #[cfg(not(feature = "storage"))]
    {
        let _ = state;
        (StatusCode::SERVICE_UNAVAILABLE, "Scrub not available (storage feature disabled)")
            .into_response()
    }
}

/// GET /admin/metrics — returns Prometheus text-format metrics.
#[instrument(skip(state))]
async fn metrics_endpoint(State(state): State<AdminState>) -> impl IntoResponse {
    let body = state.metrics.gather();
    (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "text/plain")], body)
}

/// GET /admin/acceleration — returns hardware acceleration status.
#[derive(Serialize)]
struct AccelerationStatus {
    active_tier: String,
    fallback_count: u64,
    healthy: bool,
}

#[instrument(skip(state))]
async fn acceleration_status(State(state): State<AdminState>) -> impl IntoResponse {
    #[cfg(feature = "accel")]
    {
        if let Some(ref accel) = state.accel {
            let status = AccelerationStatus {
                active_tier: format!("{:?}", accel.active_tier()),
                fallback_count: accel.ec_fallback_count(),
                healthy: !accel.is_ec_backend_unhealthy(),
            };
            return Json(status).into_response();
        }
    }
    // Without accel feature or dispatcher, report CPU baseline
    let status =
        AccelerationStatus { active_tier: "CpuSimd".into(), fallback_count: 0, healthy: true };
    Json(status).into_response()
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
        let report = SegmentReport { total: 15, sealed: 5, unsealed: 10, encoding: 2, by_tier };
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
