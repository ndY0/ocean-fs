//! Admin API — cluster health, segment status, cache stats, and metrics.
//!
//! Exposes REST endpoints under `/admin/` for cluster visibility and
//! a `/admin/metrics` Prometheus text-format endpoint.
//!
//! Per performance guideline §11.1, all counters use `AtomicU64` with
//! relaxed ordering on the hot path.

use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
    Json, Router,
};
use dashmap::DashMap;
#[cfg(feature = "accel")]
use oceanfs_accel::AccelDispatcher;
#[cfg(feature = "cache")]
use oceanfs_cache::{MetadataCache, NegativeCache, ObjectCache};
pub use oceanfs_core::{
    sub_millisecond_histogram_config, validate_counter_name, Counter, Gauge, LabelSet,
    MetricRegistrar,
};
use oceanfs_core::{Histogram, HistogramConfig, SizeTier};
use serde::Serialize;
use tracing::instrument;

use crate::bucket_config::BucketConfigStore;

// ---------------------------------------------------------------------------
// MetricsRegistry
// ---------------------------------------------------------------------------

/// A Prometheus-compatible metrics registry backed by `DashMap` for lock-free reads.
///
/// All counters and gauges use `AtomicU64` with `Relaxed` ordering for minimal
/// overhead on the hot path. Histograms use per-bucket `AtomicU64` for lock-free
/// observation (perf §11.1, §2.2).
pub struct MetricsRegistry {
    counters: DashMap<String, Counter>,
    gauges: DashMap<String, Gauge>,
    histograms: DashMap<String, Arc<Histogram>>,
}

impl MetricsRegistry {
    /// Creates a new empty metrics registry.
    pub fn new() -> Self {
        Self { counters: DashMap::new(), gauges: DashMap::new(), histograms: DashMap::new() }
    }

    /// Registers or retrieves a counter by name.
    ///
    /// Returns an `Arc<Counter>` that can be shared across subsystems.
    /// If a counter with the given name already exists, it is returned
    /// instead of creating a new one. Counter names are validated to
    /// end with `_total`.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_server::admin::MetricsRegistry;
    ///
    /// let reg = MetricsRegistry::new();
    /// let c = reg.counter("requests_total", "Total requests");
    /// c.inc();
    /// ```
    pub fn counter(&self, name: &str, help: &str) -> Counter {
        let name = validate_counter_name(name);
        self.counters
            .entry(name.clone())
            .or_insert_with(|| Counter::new(name, help.to_string(), LabelSet::empty()))
            .clone()
    }

    /// Registers or retrieves a labeled counter.
    ///
    /// Labels are used to distinguish different series of the same metric.
    /// For example: `cache_hits_total{tier="l1"}` and `cache_hits_total{tier="l2"}`
    /// are registered as separate counters.
    pub fn counter_with_labels(&self, name: &str, labels: &[(&str, &str)], help: &str) -> Counter {
        let name = validate_counter_name(name);
        let label_set = LabelSet::new(labels);
        let key = Self::make_key(&name, &label_set);
        self.counters
            .entry(key)
            .or_insert_with(|| Counter::new(name, help.to_string(), label_set))
            .clone()
    }

    /// Registers or retrieves a gauge by name.
    pub fn gauge(&self, name: &str, help: &str) -> Gauge {
        self.gauges
            .entry(name.to_string())
            .or_insert_with(|| Gauge::new(name.to_string(), help.to_string(), LabelSet::empty()))
            .clone()
    }

    /// Registers or retrieves a labeled gauge.
    pub fn gauge_with_labels(&self, name: &str, labels: &[(&str, &str)], help: &str) -> Gauge {
        let label_set = LabelSet::new(labels);
        let key = Self::make_key(name, &label_set);
        self.gauges
            .entry(key)
            .or_insert_with(|| Gauge::new(name.to_string(), help.to_string(), label_set))
            .clone()
    }

    /// Registers or retrieves a histogram by name with default buckets.
    pub fn histogram(&self, name: &str, help: &str) -> Arc<Histogram> {
        self.histogram_with_config(name, help, &HistogramConfig::default())
    }

    /// Registers or retrieves a histogram with custom bucket configuration.
    pub fn histogram_with_config(
        &self,
        name: &str,
        help: &str,
        config: &HistogramConfig,
    ) -> Arc<Histogram> {
        self.histograms
            .entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(Histogram::new(
                    name.to_string(),
                    help.to_string(),
                    config,
                    LabelSet::empty(),
                ))
            })
            .clone()
    }

    /// Registers or retrieves a labeled histogram.
    pub fn histogram_with_labels(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        help: &str,
        config: &HistogramConfig,
    ) -> Arc<Histogram> {
        let label_set = LabelSet::new(labels);
        let key = Self::make_key(name, &label_set);
        self.histograms
            .entry(key)
            .or_insert_with(|| {
                Arc::new(Histogram::new(name.to_string(), help.to_string(), config, label_set))
            })
            .clone()
    }

    /// Gathers all registered metrics in Prometheus text exposition format.
    ///
    /// Lock-free iteration over `DashMap` entries.
    pub fn gather(&self) -> String {
        let mut output = String::with_capacity(4096);

        for entry in self.counters.iter() {
            output.push_str(&entry.value().render());
            output.push('\n');
        }
        for entry in self.gauges.iter() {
            output.push_str(&entry.value().render());
            output.push('\n');
        }
        for entry in self.histograms.iter() {
            output.push_str(&entry.value().render());
            output.push('\n');
        }

        output
    }

    /// Creates a composite key from metric name and label set.
    fn make_key(name: &str, labels: &LabelSet) -> String {
        if labels.is_empty() {
            name.to_string()
        } else {
            format!("{name}{}", labels.render())
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricRegistrar for MetricsRegistry {
    fn register_counter(&self, counter: Counter) {
        let key = Self::make_key(counter.name(), counter.labels());
        self.counters.entry(key).or_insert(counter);
    }

    fn register_gauge(&self, gauge: Gauge) {
        let key = Self::make_key(gauge.name(), gauge.labels());
        self.gauges.entry(key).or_insert(gauge);
    }

    fn register_histogram(&self, histogram: Arc<oceanfs_core::Histogram>) {
        let key = Self::make_key(histogram.name(), histogram.labels());
        self.histograms.entry(key).or_insert(histogram);
    }
}

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------

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

/// Fixed deterministic probe hashes for the ring-consistency check.
///
/// The Phase 3 load test (`e2e/tests/load_cluster_churn.rs`) queries
/// `/admin/ring` on every alive node and asserts that the successor set
/// for each probe is identical across the fleet (ring_consistency +
/// no_split_brain assertions). The hashes are SHA-256 of the literals
/// `oceanfs-ring-probe-{0..7}` — fixed so every node answers the same
/// questions.
const RING_PROBE_HASHES: [[u8; 32]; 8] = [
    [
        0x35, 0x32, 0x17, 0x71, 0xb2, 0xbc, 0x9a, 0xf8, 0x42, 0x05, 0x77, 0xf6, 0x0f, 0x0f, 0xab,
        0xe8, 0xfb, 0x09, 0x06, 0xea, 0xdb, 0x99, 0xe5, 0xa5, 0x6b, 0x6e, 0xc2, 0x35, 0xb0, 0x91,
        0xc8, 0x89,
    ],
    [
        0x96, 0x04, 0xf0, 0x5c, 0xdf, 0x5d, 0x39, 0x7c, 0x03, 0xbf, 0xab, 0x0a, 0xe5, 0x31, 0x24,
        0x47, 0x72, 0x9a, 0xd4, 0xa8, 0xae, 0x2d, 0x16, 0xdc, 0x7d, 0xc7, 0xdb, 0xcc, 0xff, 0x4a,
        0xd1, 0x1f,
    ],
    [
        0xa1, 0xf7, 0x9d, 0x0a, 0x65, 0x8e, 0x64, 0xdb, 0x57, 0x2e, 0x94, 0x85, 0x3d, 0xca, 0x73,
        0x75, 0x28, 0xac, 0x54, 0x0b, 0x24, 0x23, 0xb1, 0x33, 0x8f, 0x67, 0xf5, 0x6e, 0x8b, 0xe0,
        0x3c, 0x2b,
    ],
    [
        0x0f, 0x6b, 0xad, 0xa3, 0xfc, 0x94, 0xb6, 0x35, 0xa4, 0x99, 0x25, 0x29, 0x57, 0xe0, 0xa5,
        0x0d, 0x4e, 0xa5, 0x23, 0x43, 0xc5, 0x43, 0x81, 0x92, 0x27, 0xd1, 0xb0, 0x93, 0x27, 0x30,
        0xea, 0x4f,
    ],
    [
        0xf3, 0xd0, 0x4c, 0x7a, 0xf6, 0x0d, 0x3c, 0xc7, 0x63, 0xd8, 0x05, 0xad, 0xa3, 0xb3, 0xd3,
        0x5e, 0x5a, 0xde, 0x65, 0x32, 0xd4, 0x12, 0xfa, 0x2b, 0x39, 0x03, 0x4b, 0x85, 0x4f, 0xc7,
        0x28, 0xcb,
    ],
    [
        0xfd, 0xe9, 0xa6, 0x63, 0x81, 0xe1, 0x6f, 0x18, 0x0e, 0x5d, 0xc3, 0x26, 0xb6, 0xf3, 0xe2,
        0xab, 0xde, 0xc0, 0x31, 0x2b, 0x17, 0x9d, 0xa5, 0x89, 0x8e, 0xb7, 0x66, 0xfb, 0x43, 0xaa,
        0xcd, 0x51,
    ],
    [
        0xc6, 0xa9, 0x1d, 0x35, 0x94, 0x44, 0xfa, 0x09, 0x42, 0x29, 0x5b, 0x9b, 0x62, 0xbc, 0x12,
        0x81, 0x47, 0x91, 0x2b, 0x90, 0xff, 0x34, 0xa0, 0xeb, 0xc2, 0x02, 0x74, 0xcb, 0xff, 0xc4,
        0x50, 0x8b,
    ],
    [
        0xc9, 0x6d, 0x3f, 0x31, 0x71, 0x0f, 0xde, 0x1e, 0xc1, 0xe6, 0x3b, 0x60, 0x8f, 0x71, 0xdf,
        0x53, 0xc3, 0x2b, 0x7a, 0xc9, 0xfb, 0xe9, 0xd6, 0x6f, 0xbb, 0xdb, 0x13, 0x3b, 0xb4, 0xc9,
        0xee, 0x09,
    ],
];

/// Response for GET /admin/ring — per-probe successor sets for the
/// ring-consistency assertions of the Phase 3 load test.
#[derive(Debug, Clone, Serialize)]
pub struct RingView {
    /// Physical node count in the ring.
    pub node_count: usize,
    /// Number of virtual nodes in the ring.
    pub vnodes: usize,
    /// Successor sets for the fixed probe hashes.
    pub probes: Vec<RingProbe>,
}

/// One probe's successor list.
#[derive(Debug, Clone, Serialize)]
pub struct RingProbe {
    /// Probe index (0..8, the fixed probe-hash table used by the admin
    /// ring view; see the private `RING_PROBE_HASHES` constant).
    pub probe: usize,
    /// Successor node IDs for this hash, in ring order.
    pub successors: Vec<String>,
}

/// GET /admin/ring — ring ownership view for the Phase 3 assertions.
///
/// Returns the successor set for each fixed probe hash as seen by THIS
/// node. The load test compares these across all alive nodes: identical
/// successor lists imply identical ring views (no split brain), while
/// any disagreement means the nodes route the same key to different
/// owners.
#[instrument(skip(state))]
async fn ring_view(State(state): State<AdminState>) -> impl IntoResponse {
    let vnodes = state
        .ring_cache
        .as_ref()
        .map(|rc| {
            let ring = rc.snapshot();
            ring.node_count() * ring.config().vnodes_per_node as usize
        })
        .unwrap_or(0);

    let node_count = state.ring_cache.as_ref().map(|rc| rc.snapshot().node_count()).unwrap_or(0);

    let probes = state
        .ring_cache
        .as_ref()
        .map(|rc| {
            let ring = rc.snapshot();
            RING_PROBE_HASHES
                .iter()
                .enumerate()
                .map(|(i, h)| RingProbe {
                    probe: i,
                    successors: ring.lookup(h).into_iter().map(|n| n.to_string()).collect(),
                })
                .collect()
        })
        .unwrap_or_default();

    Json(RingView { node_count, vnodes, probes }).into_response()
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
    pub scrub_coordinator: Option<Arc<oceanfs_durability::ScrubCoordinator>>,
    /// Metadata store for scrub verification (storage feature only).
    #[cfg(feature = "storage")]
    pub metadata_store: Option<Arc<dyn oceanfs_storage_api::MetadataStore>>,
    /// The lifecycle registry — the admin segments report enumerates
    /// the machine's live entries (ADR-0025 Decision 3).
    #[cfg(feature = "storage")]
    pub lifecycle_registry:
        Option<Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>>,
    /// Segment data store for scrub (storage feature only).
    #[cfg(feature = "storage")]
    pub data_store: Option<Arc<dyn oceanfs_storage_api::SegmentDataStore>>,
    /// The live storage-pool registry (ADR-0029, f8): `POST /admin/pools`
    /// attaches a pool at runtime without a restart. `None` when the
    /// pool surface is not wired.
    #[cfg(feature = "storage")]
    pub pool_registry: Option<Arc<oceanfs_storage::PoolRegistry>>,
    /// Post-attach hook the composition root wires: rebuild + re-gossip
    /// the `NodeManifest` (f6) and update the routing cache (f7) after a
    /// pool is registered. Errors are logged — the pool IS attached.
    #[cfg(feature = "storage")]
    pub on_pool_attached: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
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
                lifecycle_registry: None,
                #[cfg(feature = "storage")]
                data_store: None,
                #[cfg(feature = "storage")]
                pool_registry: None,
                #[cfg(feature = "storage")]
                on_pool_attached: None,
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
                lifecycle_registry: None,
                #[cfg(feature = "storage")]
                data_store: None,
                #[cfg(feature = "storage")]
                pool_registry: None,
                #[cfg(feature = "storage")]
                on_pool_attached: None,
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
        coordinator: Arc<oceanfs_durability::ScrubCoordinator>,
        metadata: Arc<dyn oceanfs_storage_api::MetadataStore>,
        data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore>,
    ) -> Self {
        self.state.scrub_coordinator = Some(coordinator);
        self.state.metadata_store = Some(metadata);
        self.state.data_store = Some(data_store);
        self
    }

    /// Wires the segment lifecycle registry for the admin segments
    /// report.
    ///
    /// When configured, `GET /admin/segments` enumerates the machine's
    /// live entries (ADR-0025 Decision 3 — the `segments` CF is
    /// removed; the registry is the only segment-state source).
    #[cfg(feature = "storage")]
    pub fn with_lifecycle_registry(
        mut self,
        registry: Arc<oceanfs_storage::segment::lifecycle::SegmentLifecycleRegistry>,
    ) -> Self {
        self.state.lifecycle_registry = Some(registry);
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

    /// Wires the runtime pool-attach surface (ADR-0029 §D8, f8).
    ///
    /// `registry` is the node's live `PoolRegistry`; `on_attached` is the
    /// composition root's post-attach hook (rebuild + re-gossip the
    /// `NodeManifest` and update the routing cache). Without this, the
    /// `POST /admin/pools` route answers `501 Not Implemented`.
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// use oceanfs_server::admin::{AdminHandler, MetricsRegistry};
    /// use oceanfs_server::BucketConfigStore;
    /// use oceanfs_storage::PoolRegistry;
    /// # let tmp = tempfile::tempdir().expect("tempdir");
    /// # let data_dir = tmp.path().join("data");
    /// # let storage = oceanfs_core::StorageConfig {
    /// #     pools: vec![
    /// #         oceanfs_core::StoragePoolConfig { name: "data-0".into(), role: oceanfs_core::PoolRole::Data, root: tmp.path().join("pool-data"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "wal-0".into(), role: oceanfs_core::PoolRole::Wal, root: tmp.path().join("pool-wal"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "meta-0".into(), role: oceanfs_core::PoolRole::Metadata, root: tmp.path().join("pool-meta"), weight: None, tech: Default::default(), health: Default::default() },
    /// #         oceanfs_core::StoragePoolConfig { name: "hints-0".into(), role: oceanfs_core::PoolRole::Hints, root: tmp.path().join("pool-hints"), weight: None, tech: Default::default(), health: Default::default() },
    /// #     ],
    /// #     missing_root_policy: Default::default(),
    /// # };
    /// let registry = PoolRegistry::from_config(&storage, &data_dir).expect("registry");
    /// let handler = AdminHandler::new(
    ///     Arc::new(BucketConfigStore::new()),
    ///     Arc::new(MetricsRegistry::new()),
    /// )
    /// .with_pool_attach(Arc::new(registry), Arc::new(|| Ok(())));
    /// # let _ = handler;
    /// ```
    #[cfg(feature = "storage")]
    pub fn with_pool_attach(
        mut self,
        registry: Arc<oceanfs_storage::PoolRegistry>,
        on_attached: Arc<dyn Fn() -> Result<(), String> + Send + Sync>,
    ) -> Self {
        self.state.pool_registry = Some(registry);
        self.state.on_pool_attached = Some(on_attached);
        self
    }

    /// Consumes the handler and returns an axum `Router` for the
    /// `/admin/` prefix.
    pub fn into_router(self) -> Router {
        let state = self.state;

        let router = Router::new()
            .route("/admin/health", get(health_check))
            .route("/admin/cluster", get(cluster_view))
            .route("/admin/ring", get(ring_view))
            .route("/admin/segments", get(segment_report))
            .route("/admin/caches", get(cache_stats))
            .route("/admin/scrub", post(trigger_scrub))
            .route("/admin/metrics", get(metrics_endpoint))
            .route("/admin/acceleration", get(acceleration_status))
            .route("/admin/buckets/{bucket}/policy", put(set_bucket_policy));

        // Runtime pool attach (ADR-0029 §D8, f8) — storage feature only.
        #[cfg(feature = "storage")]
        let router = router.route("/admin/pools", post(attach_pool));

        router.with_state(state)
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

/// PUT /admin/buckets/{bucket}/policy — replace a bucket's policy.
///
/// Accepts a full [`BucketPolicy`] JSON body (serde defaults fill
/// omitted fields; content-type agnostic). Used by the load harness to
/// opt buckets into per-bucket features such as compression
/// (`{"compression": {"tier": "Auto", "level": 3}}`).
async fn set_bucket_policy(
    State(state): State<AdminState>,
    Path(bucket): Path<String>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let policy: crate::BucketPolicy = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("invalid policy JSON: {e}") })),
            )
                .into_response();
        }
    };
    state.buckets.put(bucket.clone(), policy);
    (StatusCode::OK, Json(serde_json::json!({ "bucket": bucket, "updated": true }))).into_response()
}

/// GET /admin/cluster — returns cluster membership view as JSON.
#[instrument(skip(state))]
async fn cluster_view(State(state): State<AdminState>) -> impl IntoResponse {
    let nodes: Vec<NodeInfo> = if let Some(ref membership) = state.membership {
        membership
            .nodes_full()
            .into_iter()
            .map(|(node_id, node_state, incarnation, addr, _, _, _, _)| NodeInfo {
                id: node_id.to_string(),
                state: format!("{:?}", node_state),
                address: addr.to_string(),
                incarnation: incarnation.value(),
            })
            .collect()
    } else {
        Vec::new()
    };

    let vnodes = state
        .ring_cache
        .as_ref()
        .map(|rc| {
            let ring = rc.snapshot();
            ring.node_count() * ring.config().vnodes_per_node as usize
        })
        .unwrap_or(0);

    let view = ClusterView { nodes, vnodes, generation: 0 };
    Json(view).into_response()
}

/// GET /admin/segments — returns segment health report as JSON.
#[instrument(skip(state))]
async fn segment_report(State(state): State<AdminState>) -> impl IntoResponse {
    #[cfg(feature = "storage")]
    {
        if let Some(ref registry) = state.lifecycle_registry {
            let mut total: u64 = 0;
            let mut sealed: u64 = 0;
            let mut unsealed: u64 = 0;
            let mut by_tier: HashMap<SizeTier, u64> = HashMap::new();

            // The machine's live entries are the segment set (ADR-0025
            // Decision 3 — the `segments` CF is removed).
            registry.for_each(|_id, entry| {
                total += 1;
                if entry.state == oceanfs_storage::segment::lifecycle::SegmentState::Sealed {
                    sealed += 1;
                } else {
                    unsealed += 1;
                }
                *by_tier.entry(entry.metadata.size_tier).or_insert(0) += 1;
            });

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
    let mut stats = Vec::with_capacity(3);

    #[cfg(feature = "cache")]
    {
        if let Some(ref object_cache) = state.object_cache {
            let s = object_cache.stats();
            stats.push(CacheStats {
                tier: "l1".into(),
                hits: s.hits.get(),
                misses: s.misses.get(),
            });
        }
        if let Some(ref meta_cache) = state.metadata_cache {
            let s = meta_cache.stats();
            stats.push(CacheStats {
                tier: "l2".into(),
                hits: s.hits.get(),
                misses: s.misses.get(),
            });
        }
        if let Some(ref neg_cache) = state.negative_cache {
            let s = neg_cache.stats();
            stats.push(CacheStats {
                tier: "l3".into(),
                hits: s.hits.get(),
                misses: s.false_positives.get(),
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
            let _metadata = metadata.clone();
            let data_store = data_store.clone();
            // The lifecycle registry is wired together with the scrub
            // trigger (both optional — a storage-less node has neither);
            // a missing registry here is a misconfiguration, surfaced as
            // 503 like the missing-coordinator case below.
            let Some(registry) = state.lifecycle_registry.clone() else {
                tracing::warn!("scrub trigger requested but lifecycle registry not configured");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Lifecycle registry not configured on this node",
                )
                    .into_response();
            };
            match coordinator.trigger_manual(registry, data_store).await {
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

#[instrument(skip(_state))]
async fn acceleration_status(State(_state): State<AdminState>) -> impl IntoResponse {
    #[cfg(feature = "accel")]
    {
        if let Some(ref accel) = _state.accel {
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

/// `POST /admin/pools` — attach a storage pool at runtime (ADR-0029 §D8,
/// f8): validate → probe → register → re-gossip the manifest → `201`.
///
/// Response codes: `201` attached (with `{"pool_id": n}`); `400`
/// validation failure; `409` duplicate name/root; `500` probe failure;
/// `501` when the pool surface is not wired (no storage feature or the
/// composition root did not call `with_pool_attach`).
///
/// The admin path is per-node (the operator addresses the node whose
/// topology they are changing) — consistent with the per-node topology
/// config model (ADR-0029 §D8).
#[cfg(feature = "storage")]
#[instrument(skip(state))]
async fn attach_pool(
    State(state): State<AdminState>,
    Json(pool): Json<oceanfs_core::StoragePoolConfig>,
) -> impl IntoResponse {
    let (registry, on_attached) =
        match (state.pool_registry.as_ref(), state.on_pool_attached.as_ref()) {
            (Some(registry), Some(on_attached)) => (registry.clone(), on_attached.clone()),
            _ => {
                return (
                    StatusCode::NOT_IMPLEMENTED,
                    Json(serde_json::json!({
                        "error": "pool attach is not configured on this node",
                    })),
                )
                    .into_response();
            }
        };

    match registry.attach(pool) {
        Ok(pool_id) => {
            // The pool IS registered — re-gossip the manifest so peers
            // see the new capacity. A hook failure is logged loudly but
            // must not fail the attach (the operator can re-trigger via
            // a subsequent call or a restart).
            if let Err(e) = on_attached() {
                tracing::error!(
                    error = %e,
                    pool_id,
                    "pool attached but manifest re-gossip failed; \
                     peers may not see the new pool until the next change"
                );
            }
            (StatusCode::CREATED, Json(serde_json::json!({ "pool_id": pool_id }))).into_response()
        }
        Err(message) => {
            // Classify: duplicate name/root → 409; probe failure → 500;
            // anything else is a validation failure → 400.
            let status = if message.contains("duplicate pool name")
                || message.contains("duplicate pool root")
            {
                StatusCode::CONFLICT
            } else if message.contains("probe failed") {
                StatusCode::INTERNAL_SERVER_ERROR
            } else {
                StatusCode::BAD_REQUEST
            };
            (status, Json(serde_json::json!({ "error": message }))).into_response()
        }
    }
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

    // --- Labeled counter tests ---

    #[test]
    fn labeled_counter_distinct_by_labels() {
        let reg = MetricsRegistry::new();
        let c1 = reg.counter_with_labels("hits_total", &[("tier", "l1")], "L1 hits");
        let c2 = reg.counter_with_labels("hits_total", &[("tier", "l2")], "L2 hits");
        c1.inc();
        c1.inc();
        c2.inc();
        assert_eq!(c1.get(), 2);
        assert_eq!(c2.get(), 1);
    }

    #[test]
    fn counter_name_gets_total_suffix() {
        let reg = MetricsRegistry::new();
        let c = reg.counter("requests", "help");
        let rendered = c.render();
        assert!(rendered.contains("requests_total 0"));
    }

    // --- Gauge tests ---

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
    fn registry_gauge_deduplicates() {
        let reg = MetricsRegistry::new();
        let g1 = reg.gauge("g1", "h1");
        let g2 = reg.gauge("g1", "h2");
        g1.set(5);
        assert_eq!(g2.get(), 5);
    }

    // --- Label rendering tests ---

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

    // --- Histogram tests ---

    #[test]
    fn histogram_observe_is_lock_free() {
        // Verifies observe() doesn't panic under concurrency (AtomicU64-based).
        use std::thread;
        let h = Arc::new(Histogram::new(
            "latency".into(),
            "help".into(),
            &HistogramConfig::default(),
            LabelSet::empty(),
        ));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let h = h.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    h.observe(i % 100);
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(h.count(), 8000);
    }

    #[test]
    fn histogram_sub_millisecond_config_has_correct_buckets() {
        let config = sub_millisecond_histogram_config();
        assert!(config.buckets.contains(&1), "should have 0.001ms bucket");
        assert!(config.buckets.contains(&5));
        assert!(config.buckets.contains(&10));
        assert!(config.buckets.contains(&1_000));
        assert!(config.buckets.contains(&1_000_000));
    }

    #[test]
    fn histogram_with_labels_renders_correctly() {
        let h = Histogram::new(
            "accel_encode_duration".into(),
            "help".into(),
            &HistogramConfig::default(),
            LabelSet::new(&[("tier", "cpu_simd")]),
        );
        h.observe(50);
        let rendered = h.render();
        assert!(rendered.contains(r#"accel_encode_duration_bucket{le="50"}{tier="cpu_simd"} 1"#));
    }

    // --- DashMap registry tests ---

    #[test]
    fn registry_counter_reads_do_not_block_writes() {
        use std::thread;
        let reg = Arc::new(MetricsRegistry::new());
        let c = reg.counter("concurrent_total", "test");

        let reg_clone = reg.clone();
        let writer = thread::spawn(move || {
            for _ in 0..100 {
                reg_clone.counter("new_counter", "help");
            }
        });

        let reader = thread::spawn(move || {
            for _ in 0..1000 {
                let _ = reg.gather();
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
        // If we get here without deadlocking, DashMap works correctly.
        assert_eq!(c.get(), 0);
        c.inc();
        assert_eq!(c.get(), 1);
    }

    #[test]
    fn gather_includes_gauges_and_histograms() {
        let reg = MetricsRegistry::new();
        reg.gauge("mem_bytes", "memory");
        reg.histogram("latency", "help");
        let out = reg.gather();
        assert!(out.contains("mem_bytes"));
        assert!(out.contains("latency"));
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

    #[test]
    fn register_histogram_stores_in_registry() {
        use oceanfs_core::{Histogram, HistogramConfig, LabelSet, MetricRegistrar};

        let reg = MetricsRegistry::new();
        let config = HistogramConfig::default();
        let h = Arc::new(Histogram::new(
            "test_latency".into(),
            "Test histogram".into(),
            &config,
            LabelSet::empty(),
        ));

        reg.register_histogram(Arc::clone(&h));
        let gathered = reg.gather();
        assert!(
            gathered.contains("test_latency"),
            "gathered metrics should contain registered histogram: {gathered}"
        );
    }

    #[test]
    fn register_histogram_with_labels_renders_correctly() {
        use oceanfs_core::{Histogram, HistogramConfig, LabelSet, MetricRegistrar};

        let reg = MetricsRegistry::new();
        let config = HistogramConfig::default();
        let labels = LabelSet::new(&[("tier", "gpu"), ("op", "encode")]);
        let h =
            Arc::new(Histogram::new("latency".into(), "Test histogram".into(), &config, labels));

        reg.register_histogram(Arc::clone(&h));
        let gathered = reg.gather();
        assert!(
            gathered.contains("tier=\"gpu\"") && gathered.contains("op=\"encode\""),
            "gathered output should contain label pairs, got: {gathered}"
        );
    }
}
