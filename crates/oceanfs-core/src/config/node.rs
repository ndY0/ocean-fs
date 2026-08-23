//! Root node configuration.
//!
//! The [`NodeConfig`] struct is the top-level configuration for an OceanFS
//! node, loaded from `oceanfs.toml`. It contains all operational settings
//! including networking, storage paths, and maintenance intervals.

use std::path::PathBuf;

// ---------------------------------------------------------------------------
// AntiEntropyConfig
// ---------------------------------------------------------------------------

/// Configuration for the incremental Merkle tree anti-entropy protocol.
///
/// Controls two modes: continuous root-only exchange (triggered on every
/// segment write) and periodic sampling mode (random fraction of segments).
///
/// # Examples
///
/// ```
/// use oceanfs_core::AntiEntropyConfig;
///
/// let config = AntiEntropyConfig::default();
/// assert!(config.continuous_enabled);
/// assert_eq!(config.continuous_max_segments, 10000);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AntiEntropyConfig {
    /// Whether continuous anti-entropy is enabled (root exchange on every
    /// segment write or gossip interval).
    #[serde(default = "default_true")]
    pub continuous_enabled: bool,
    /// Maximum number of segments tracked in continuous mode before evicting
    /// the oldest. Memory is bounded: ~4.5 MB at default 10000 segments.
    #[serde(default = "default_continuous_max_segments")]
    pub continuous_max_segments: usize,
    /// Whether sampling anti-entropy is enabled (periodic random subset).
    #[serde(default = "default_true")]
    pub sampling_enabled: bool,
    /// Interval in seconds between sampling anti-entropy cycles.
    #[serde(default = "default_ae_sampling_interval_sec")]
    pub sampling_interval_sec: u64,
    /// Fraction of tracked segments to exchange per sampling cycle, in (0.0, 1.0].
    #[serde(default = "default_ae_sampling_fraction")]
    pub sampling_fraction: f64,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self {
            continuous_enabled: true,
            continuous_max_segments: 10000,
            sampling_enabled: true,
            sampling_interval_sec: 300,
            sampling_fraction: 0.05,
        }
    }
}

fn default_continuous_max_segments() -> usize {
    10000
}

fn default_ae_sampling_interval_sec() -> u64 {
    300
}

fn default_ae_sampling_fraction() -> f64 {
    0.05
}

/// Root configuration for an OceanFS node.
///
/// Loaded from `oceanfs.toml` on startup. All fields have sensible
/// defaults so that a minimal config file is sufficient for development.
///
/// # Examples
///
/// ```
/// use oceanfs_core::NodeConfig;
///
/// let config = NodeConfig::default();
/// assert_eq!(config.data_dir.to_str().unwrap(), "/var/lib/oceanfs");
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeConfig {
    /// Unique identifier for this node.
    #[serde(default = "default_node_id")]
    pub node_id: String,
    /// Directory for all persistent data (RocksDB, WAL, segments).
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    /// Storage-pool topology (ADR-0029 §D8).
    ///
    /// Empty `pools` = legacy single-`data_dir` mode, byte-for-byte today's
    /// behavior. Non-empty = one `StoragePool` per pool entry, consumed by
    /// the pool registry (feature f2).
    #[serde(default)]
    pub storage: crate::StorageConfig,
    /// Address the S3 HTTP API listens on.
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    /// Address for internal gRPC node-to-node communication.
    #[serde(default = "default_grpc_listen_addr")]
    pub grpc_listen_addr: String,
    /// Address the membership plane (gossip + SWIM probes) listens on
    /// (ADR-0028 D1).
    ///
    /// The membership protocol runs on its own listener and connection
    /// pool so that probe latency is never coupled to the data plane's
    /// behavior (16 MiB replica streams, hinted-handoff batches, healing
    /// transfers). Default: `0.0.0.0:9002`.
    #[serde(default = "default_membership_listen_addr")]
    pub membership_listen_addr: String,
    /// Gossip membership protocol configuration.
    ///
    /// Controls SWIM gossip interval, suspicion/failure timeouts,
    /// indirect ping count, and bootstrap seed nodes.
    #[serde(default)]
    pub gossip: crate::GossipConfig,
    /// Log level: trace, debug, info, warn, error.
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// Whether to enable the Prometheus metrics endpoint.
    #[serde(default = "default_metrics_enabled")]
    pub metrics_enabled: bool,
    /// Address for the Prometheus metrics HTTP endpoint.
    #[serde(default = "default_metrics_listen_addr")]
    pub metrics_listen_addr: String,
    /// Whether S3 Signature V4 authentication is enforced.
    ///
    /// When `true`, all S3 object and bucket operations require valid
    /// AWS SigV4 credentials. When `false` (default), requests pass
    /// through unauthenticated (development mode).
    #[serde(default)]
    pub s3_auth_enabled: bool,
    /// Whether the prefetch engine warms caches after LIST/GET.
    ///
    /// Enables anticipatory cache population for improved read
    /// latency. Prefetch runs as a background task and does not
    /// block request handling.
    #[serde(default)]
    pub prefetch_enabled: bool,
    /// Maximum HTTP body size in bytes (default 2 MB = 2097152).
    #[serde(default = "default_max_body_size")]
    pub max_body_size: usize,
    /// Garbage collection interval in seconds (default 3600).
    #[serde(default = "default_gc_interval")]
    pub gc_interval_sec: u64,
    /// Whether the g3 loss-announcement push is enabled (default true).
    ///
    /// The g4 reconciliation loop is the MANDATORY safety net that runs
    /// regardless; disabling announcements (tests) proves reconciliation
    /// restores RF independently of any push (ADR-0029 §D4).
    #[serde(default = "default_true")]
    pub announcements_enabled: bool,
    /// Tombstone TTL in seconds before deleted objects are
    /// permanently reclaimed (default 259200 = 3 days).
    #[serde(default = "default_tombstone_ttl")]
    pub tombstone_ttl_sec: u64,
    /// Anti-entropy Merkle verification interval in seconds
    /// (default 300).
    #[serde(default = "default_ae_interval")]
    pub ae_interval_sec: u64,
    /// Scrub cycle interval in seconds (default 604800 = 7 days).
    #[serde(default = "default_scrub_interval")]
    pub scrub_interval_sec: u64,
    /// Orphan reaper interval in seconds (default 3600).
    #[serde(default = "default_orphan_interval")]
    pub orphan_reaper_interval_sec: u64,
    /// Number of virtual nodes per physical node in the DHT ring (default 256).
    #[serde(default = "default_vnodes_per_node")]
    pub vnodes_per_node: u32,
    /// Number of replicas for each data item (default 3).
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    /// Number of gRPC channels per peer (default 4).
    #[serde(default = "default_pool_size_per_peer")]
    pub pool_size_per_peer: usize,
    /// Keepalive interval in seconds for idle channels (default 30).
    #[serde(default = "default_keepalive_sec")]
    pub keepalive_sec: u64,
    /// Connection establishment timeout in milliseconds (default 5000).
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Default per-request timeout in milliseconds (default 30000).
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Whether to cache segment reads via mmap.
    ///
    /// When `true` (read-optimized profile), frequently-accessed segment
    /// shard files are memory-mapped for zero-copy reads from the kernel
    /// page cache. When `false` (write-optimized profile), segment data
    /// files are opened with `O_DIRECT` to bypass the page cache.
    #[serde(default = "default_read_cache_segments")]
    pub read_cache_segments: bool,
    /// Whether to use io_uring for disk I/O (Linux only).
    ///
    /// When `true` and running on Linux 5.1+, `tokio-uring` provides
    /// true async disk I/O. Falls back to `tokio::fs` on non-Linux
    /// platforms or when the feature is disabled.
    #[serde(default = "default_io_uring_enabled")]
    pub io_uring_enabled: bool,
    /// Maximum number of segment files to keep memory-mapped in the
    /// `SegmentFileCache`. Only meaningful when `read_cache_segments = true`.
    /// Default: 64.
    #[serde(default = "default_segment_cache_max_entries")]
    pub segment_cache_max_entries: usize,
    /// Whether to set I/O scheduling class to `IOPRIO_CLASS_IDLE` for
    /// background task threads (GC, scrub, anti-entropy, heal, orphan
    /// reaper) on Linux (default `true` on Linux).
    ///
    /// Threads with `IOPRIO_CLASS_IDLE` only receive disk I/O bandwidth
    /// when no other thread wants it — preventing background scans from
    /// competing with client I/O for NVMe command slots.
    /// No-op on non-Linux platforms.
    #[serde(default = "default_background_io_class_idle")]
    pub background_io_class_idle: bool,
    /// Whether to set CPU scheduling policy to `SCHED_IDLE` for background
    /// task threads on Linux (default `true` on Linux).
    ///
    /// These threads only execute when no other thread wants the CPU —
    /// they literally run in idle CPU time. Requires `CAP_SYS_NICE`
    /// capability; gracefully degrades on `EPERM` with a log message.
    /// No-op on non-Linux platforms.
    #[serde(default = "default_background_cpu_sched_idle")]
    pub background_cpu_sched_idle: bool,
    /// Anti-entropy configuration.
    ///
    /// Controls the incremental Merkle tree protocol: continuous
    /// root-only exchange and periodic sampling mode.
    #[serde(default)]
    pub anti_entropy: AntiEntropyConfig,

    // ── Item 1: Garbage collection tuning ──
    /// GC compaction liveness-ratio threshold (0.0–1.0, default 0.5).
    #[serde(default = "default_gc_compact_threshold")]
    pub gc_compact_threshold: f64,
    /// Maximum concurrent compactions (default 4).
    #[serde(default = "default_gc_max_concurrent_compactions")]
    pub gc_max_concurrent_compactions: usize,
    /// Bounded channel capacity for compaction work queue (default 64).
    #[serde(default = "default_gc_compaction_queue_capacity")]
    pub gc_compaction_queue_capacity: usize,

    // ── Item 2: Scrub, anti-entropy, heal tuning ──
    /// Maximum nodes participating in distributed scrub (0 = all, default 0).
    #[serde(default)]
    pub scrub_parallel_nodes: usize,
    /// Anti-entropy peer count per cycle (default 1).
    #[serde(default = "default_ae_peer_count")]
    pub ae_peer_count: usize,
    /// Maximum concurrent heal operations (default 16).
    #[serde(default = "default_heal_parallel_segments")]
    pub heal_parallel_segments: usize,
    /// Heal throughput throttle in bytes/sec (0 = unlimited, default 0).
    #[serde(default)]
    pub heal_throttle_bytes_sec: u64,
    /// Seal-time segment-replication throughput throttle in bytes/sec
    /// (0 = unlimited, default 0). Bounds the background replication
    /// push rate so seal traffic backs off during write/read bursts —
    /// mirrors `heal_throttle_bytes_sec` (sealed-segment-replication).
    #[serde(default)]
    pub replication_throttle_bytes_sec: u64,

    // ── Item 3: Cache configuration ──
    /// Whether the L1 object cache is enabled (default true).
    #[serde(default = "default_true")]
    pub object_cache_enabled: bool,
    /// Maximum size of the L1 object cache in bytes (default 512 MB).
    #[serde(default = "default_object_cache_size_bytes")]
    pub object_cache_size_bytes: u64,
    /// TTL for cached objects in milliseconds (default 60000).
    #[serde(default = "default_object_cache_ttl_ms")]
    pub object_cache_ttl_ms: u64,
    /// Maximum blob size eligible for the object cache (default 1 MB).
    #[serde(default = "default_object_cache_max_blob_size")]
    pub object_cache_max_blob_size: u64,

    /// Whether the L2 metadata cache is enabled (default true).
    #[serde(default = "default_true")]
    pub metadata_cache_enabled: bool,
    /// Maximum size of the L2 metadata cache in bytes (default 1 GB).
    #[serde(default = "default_metadata_cache_size_bytes")]
    pub metadata_cache_size_bytes: u64,
    /// TTL for cached metadata in milliseconds (default 300000).
    #[serde(default = "default_metadata_cache_ttl_ms")]
    pub metadata_cache_ttl_ms: u64,

    /// Eviction policy for the L1 object cache (default: "gdsf").
    #[serde(default = "default_eviction_policy_l1")]
    pub eviction_policy_l1: crate::EvictionPolicyType,
    /// Eviction policy for the L2 metadata cache (default: "ttl_lru").
    #[serde(default = "default_eviction_policy_l2")]
    pub eviction_policy_l2: crate::EvictionPolicyType,

    /// Whether the L3 negative cache is enabled (default true).
    #[serde(default = "default_true")]
    pub negative_cache_enabled: bool,
    /// Size of the negative cache in bytes (default 64 MB).
    #[serde(default = "default_negative_cache_size_bytes")]
    pub negative_cache_size_bytes: u64,
    /// Rebuild interval for the negative cache in seconds (default 3600).
    #[serde(default = "default_negative_cache_rebuild_sec")]
    pub negative_cache_rebuild_sec: u64,

    /// Number of objects to prefetch after a LIST operation (default 16).
    #[serde(default = "default_prefetch_after_list")]
    pub prefetch_after_list: usize,
    /// Number of objects to prefetch after a GET operation (default 4).
    #[serde(default = "default_prefetch_after_get")]
    pub prefetch_after_get: usize,

    // ── Item 4: Per-operation timeouts ──
    /// Per-operation timeout configuration.
    #[serde(default)]
    pub operation_timeouts: crate::OperationTimeouts,

    // ── Item 4b: Write backpressure (bounded request queue) ──
    /// Maximum number of concurrent in-flight S3 PUT requests admitted
    /// to the write path. Requests beyond this bound wait up to
    /// `operation_timeouts.write_queue_ms` for a permit, then receive
    /// `503 SlowDown` (backpressure propagates to the HTTP layer instead
    /// of failing mid-write). Default: 64.
    #[serde(default = "default_max_inflight_writes")]
    pub max_inflight_writes: usize,

    // ── Item 4c: Seal pipeline batching ──
    /// Maximum time in milliseconds the seal flush coordinator collects
    /// pending segment fsync registrations before issuing the batch
    /// (group commit for segment files, mirroring the WAL's
    /// `fsync_batch_timeout_ms`). Larger windows batch more concurrent
    /// seals per barrier round but add up to `seal_fsync_batch_timeout_ms`
    /// of latency to each seal completion. Default: 10 ms.
    #[serde(default = "default_seal_fsync_batch_timeout_ms")]
    pub seal_fsync_batch_timeout_ms: u64,
    /// Maximum number of seal registrations collected into one flush
    /// batch (early-flush trigger: when this many seals are pending, the
    /// batch is flushed without waiting for the window to expire).
    /// Default: 8 (matches `max_inflight_encodes`).
    #[serde(default = "default_seal_fsync_max_waiters")]
    pub seal_fsync_max_waiters: usize,

    // ── Item 4d: Segment lifecycle machine (ADR-0025) ──
    /// Segment lifecycle registry + coordinator configuration: the shard
    /// count of the in-memory registry and the delete-eviction grace.
    /// Default: 64 shards, immediate eviction.
    #[serde(default)]
    pub lifecycle: crate::LifecycleConfig,

    // ── Item 4e: Segment event WAL (ADR-0024) ──
    /// Dedicated segment-lifecycle event log configuration: directory,
    /// rotation size, its own fsync-group batch window, and the byte
    /// threshold driving the checkpoint feature. Default: `{data_dir}/
    /// event-wal`, 64 MB files, 50 ms batch window, 64 MB checkpoint
    /// threshold.
    #[serde(default)]
    pub event_wal: crate::EventWalConfig,

    // ── Item 5: Buffer pool configuration ──
    /// Buffer pool chunk size in bytes (default 65536 = 64 KB).
    #[serde(default = "default_buffer_pool_chunk_bytes")]
    pub buffer_pool_chunk_bytes: usize,
    /// Maximum number of buffers in the pool (default 1024).
    #[serde(default = "default_buffer_pool_max_chunks")]
    pub buffer_pool_max_chunks: usize,

    // ── Item 8: Shard count configuration ──
    /// Number of segment shards. Set to 0 for auto-detect from CPU count.
    /// Default: 0 (auto).
    #[serde(default)]
    pub segment_shard_count: usize,
    /// Maximum shard count when auto-detecting. Ignored when segment_shard_count > 0.
    /// Default: 16.
    #[serde(default = "default_segment_shard_count_max")]
    pub segment_shard_count_max: usize,

    // ── Item 10: Fetch strategy ──
    /// Default fetch strategy for buckets that don't override it.
    /// Default: "local_first".
    #[serde(default)]
    pub default_fetch_strategy: crate::FetchStrategy,

    // ── Hinted handoff configuration ──
    /// Directory for per-node hinted handoff WAL files. When `None`, defaults to
    /// `"{data_dir}/hints"`.
    #[serde(default)]
    pub hint_wal_dir: Option<PathBuf>,
    /// Maximum blob size stored inline in hinted handoff WAL (bytes).
    /// Blobs above this threshold are stored as segment references.
    /// Default: 4096 (4 KB).
    #[serde(default = "default_hint_inline_threshold_bytes")]
    pub hint_inline_threshold_bytes: u64,
    /// Maximum hints per batched gRPC delivery call. Default: 256.
    #[serde(default = "default_hint_max_batch_size")]
    pub hint_max_batch_size: usize,

    /// TTL in seconds for hinted handoff entries before they are pruned
    /// from the persistent WAL (default 604800 = 7 days). Entries older
    /// than this are permanently discarded.
    #[serde(default = "default_hint_ttl_sec")]
    pub hint_ttl_sec: u64,

    /// Interval in seconds between hinted handoff WAL pruning cycles
    /// (default 3600 = 1 hour).
    #[serde(default = "default_hint_prune_interval")]
    pub hint_prune_interval_sec: u64,

    /// Interval in seconds between hinted handoff delivery sweeps
    /// (default 5).
    ///
    /// Event-driven delivery (Alive event → drain) can be missed when
    /// this node is down during the recipient's Alive event, or when the
    /// event lands before the recipient's gRPC listener is ready. The
    /// sweep retries pending hints periodically, resolving addresses at
    /// sweep time — delivery becomes eventually-convergent under churn.
    #[serde(default = "default_hint_delivery_sweep_sec")]
    pub hint_delivery_sweep_sec: u64,

    /// Cluster-readiness gate timeout in seconds (default 30).
    ///
    /// After (re)joining a cluster, a node's ring starts as a singleton
    /// until its membership pull converges; with the adaptive quorum
    /// that window would ACK writes with a single durable copy (silent
    /// under-replication). While the gate is closed, writes fail with
    /// 503. The gate opens when the ring reaches 2 nodes or this many
    /// seconds elapse (the bound keeps a node whose seeds are
    /// unreachable from stalling writes forever — the 503s it emits
    /// while gated are the safer failure mode).
    ///
    /// NOTE: convergence time scales with the gossip configuration
    /// (`[gossip] interval_ms` / `suspicion_timeout_ms` / `failure
    /// _timeout_ms`) — tune this with the gossip profile, e.g. a fast
    /// test profile can use a shorter timeout than a production profile
    /// with 30s gossip intervals.
    #[serde(default = "default_cluster_ready_timeout_sec")]
    pub cluster_ready_timeout_sec: u64,
}

fn default_node_id() -> String {
    "node-1".into()
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/oceanfs")
}
fn default_listen_addr() -> String {
    "0.0.0.0:9000".into()
}
fn default_grpc_listen_addr() -> String {
    "0.0.0.0:9001".into()
}
fn default_membership_listen_addr() -> String {
    "0.0.0.0:9002".into()
}
fn default_log_level() -> String {
    "info".into()
}
fn default_metrics_enabled() -> bool {
    true
}
fn default_metrics_listen_addr() -> String {
    "0.0.0.0:9090".into()
}
fn default_max_body_size() -> usize {
    2 * 1024 * 1024 // 2 MB
}

fn default_max_inflight_writes() -> usize {
    64
}
fn default_seal_fsync_batch_timeout_ms() -> u64 {
    10
}
fn default_seal_fsync_max_waiters() -> usize {
    8
}
fn default_gc_interval() -> u64 {
    3600
}
fn default_tombstone_ttl() -> u64 {
    259200 // 3 days
}
fn default_ae_interval() -> u64 {
    300
}
fn default_scrub_interval() -> u64 {
    604800 // 7 days
}
fn default_orphan_interval() -> u64 {
    3600
}
fn default_vnodes_per_node() -> u32 {
    256
}
fn default_replication_factor() -> u32 {
    3
}
fn default_pool_size_per_peer() -> usize {
    4
}
fn default_keepalive_sec() -> u64 {
    30
}
fn default_connect_timeout_ms() -> u64 {
    5000
}
fn default_request_timeout_ms() -> u64 {
    30000
}
fn default_read_cache_segments() -> bool {
    false
}
fn default_io_uring_enabled() -> bool {
    cfg!(target_os = "linux")
}
fn default_segment_cache_max_entries() -> usize {
    64
}
fn default_background_io_class_idle() -> bool {
    cfg!(target_os = "linux")
}
fn default_background_cpu_sched_idle() -> bool {
    cfg!(target_os = "linux")
}

// ── Item 1: GC default functions ──

fn default_gc_compact_threshold() -> f64 {
    0.5
}
fn default_gc_max_concurrent_compactions() -> usize {
    4
}
fn default_gc_compaction_queue_capacity() -> usize {
    64
}

// ── Item 2: Scrub / AE / heal default functions ──

fn default_ae_peer_count() -> usize {
    1
}
fn default_heal_parallel_segments() -> usize {
    16
}

// ── Item 3: Cache default functions ──

const fn default_true() -> bool {
    true
}
fn default_object_cache_size_bytes() -> u64 {
    512 * 1024 * 1024 // 512 MB
}
fn default_object_cache_ttl_ms() -> u64 {
    60_000
}
fn default_object_cache_max_blob_size() -> u64 {
    1024 * 1024 // 1 MB
}
fn default_metadata_cache_size_bytes() -> u64 {
    1024 * 1024 * 1024 // 1 GB
}
fn default_metadata_cache_ttl_ms() -> u64 {
    300_000
}
fn default_eviction_policy_l1() -> crate::EvictionPolicyType {
    crate::EvictionPolicyType::Gdsf
}
fn default_eviction_policy_l2() -> crate::EvictionPolicyType {
    crate::EvictionPolicyType::TtlLru
}
fn default_negative_cache_size_bytes() -> u64 {
    64 * 1024 * 1024 // 64 MB
}
fn default_negative_cache_rebuild_sec() -> u64 {
    3600
}
fn default_prefetch_after_list() -> usize {
    16
}
fn default_prefetch_after_get() -> usize {
    4
}

// ── Item 5: Buffer pool default functions ──

fn default_buffer_pool_chunk_bytes() -> usize {
    65536 // 64 KB
}
fn default_buffer_pool_max_chunks() -> usize {
    1024
}

// ── Item 8: Shard count default functions ──

fn default_segment_shard_count_max() -> usize {
    16
}

// ── Hinted handoff default functions ──

fn default_hint_inline_threshold_bytes() -> u64 {
    4096
}
fn default_hint_max_batch_size() -> usize {
    256
}

fn default_hint_ttl_sec() -> u64 {
    604800
}

fn default_hint_prune_interval() -> u64 {
    3600
}
fn default_hint_delivery_sweep_sec() -> u64 {
    5
}
fn default_cluster_ready_timeout_sec() -> u64 {
    30
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: "node-1".into(),
            data_dir: PathBuf::from("/var/lib/oceanfs"),
            storage: crate::StorageConfig::default(),
            listen_addr: "0.0.0.0:9000".into(),
            grpc_listen_addr: "0.0.0.0:9001".into(),
            membership_listen_addr: "0.0.0.0:9002".into(),
            gossip: crate::GossipConfig::default(),
            log_level: "info".into(),
            metrics_enabled: true,
            metrics_listen_addr: "0.0.0.0:9090".into(),
            s3_auth_enabled: false,
            prefetch_enabled: false,
            max_body_size: 2 * 1024 * 1024,
            gc_interval_sec: 3600,
            tombstone_ttl_sec: 259200,
            announcements_enabled: true,
            ae_interval_sec: 300,
            scrub_interval_sec: 604800,
            orphan_reaper_interval_sec: 3600,
            vnodes_per_node: 256,
            replication_factor: 3,
            pool_size_per_peer: 4,
            keepalive_sec: 30,
            connect_timeout_ms: 5000,
            request_timeout_ms: 30000,
            read_cache_segments: false,
            io_uring_enabled: cfg!(target_os = "linux"),
            segment_cache_max_entries: 64,
            max_inflight_writes: 64,
            background_io_class_idle: cfg!(target_os = "linux"),
            background_cpu_sched_idle: cfg!(target_os = "linux"),
            // Anti-entropy
            anti_entropy: AntiEntropyConfig::default(),
            // Item 1: GC
            gc_compact_threshold: 0.5,
            gc_max_concurrent_compactions: 4,
            gc_compaction_queue_capacity: 64,
            // Item 2: Scrub / AE / heal
            scrub_parallel_nodes: 0,
            ae_peer_count: 1,
            heal_parallel_segments: 16,
            heal_throttle_bytes_sec: 0,
            replication_throttle_bytes_sec: 0,
            // Item 3: Cache
            object_cache_enabled: true,
            object_cache_size_bytes: 512 * 1024 * 1024,
            object_cache_ttl_ms: 60_000,
            object_cache_max_blob_size: 1024 * 1024,
            metadata_cache_enabled: true,
            metadata_cache_size_bytes: 1024 * 1024 * 1024,
            metadata_cache_ttl_ms: 300_000,
            eviction_policy_l1: crate::EvictionPolicyType::Gdsf,
            eviction_policy_l2: crate::EvictionPolicyType::TtlLru,
            negative_cache_enabled: true,
            negative_cache_size_bytes: 64 * 1024 * 1024,
            negative_cache_rebuild_sec: 3600,
            prefetch_after_list: 16,
            prefetch_after_get: 4,
            // Item 4: Operation timeouts
            operation_timeouts: crate::OperationTimeouts::default(),
            // Item 5: Buffer pool
            buffer_pool_chunk_bytes: 65536,
            buffer_pool_max_chunks: 1024,
            // Item 4c: Seal pipeline batching
            seal_fsync_batch_timeout_ms: 10,
            seal_fsync_max_waiters: 8,
            // Item 4d: Segment lifecycle machine (ADR-0025)
            lifecycle: crate::LifecycleConfig::default(),
            // Item 4e: Segment event WAL (ADR-0024)
            event_wal: crate::EventWalConfig::default(),
            // Item 8: Shard count
            segment_shard_count: 0,
            segment_shard_count_max: 16,
            // Item 10: Fetch strategy
            default_fetch_strategy: crate::FetchStrategy::default(),
            // Hinted handoff
            hint_wal_dir: None,
            hint_inline_threshold_bytes: 4096,
            hint_max_batch_size: 256,
            hint_ttl_sec: 604800,
            hint_prune_interval_sec: 3600,
            hint_delivery_sweep_sec: 5,
            cluster_ready_timeout_sec: 30,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::{MetadataConfig, RingConfig, WalConfig};

    #[test]
    fn default_config_has_expected_listen_addr() {
        let config = NodeConfig::default();
        assert_eq!(config.listen_addr, "0.0.0.0:9000");
    }

    /// ADR-0028 D1: the membership plane defaults to its own port,
    /// distinct from the data-plane gRPC port.
    #[test]
    fn default_membership_listen_addr_is_its_own_port() {
        let config = NodeConfig::default();
        assert_eq!(config.grpc_listen_addr, "0.0.0.0:9001");
        assert_eq!(config.membership_listen_addr, "0.0.0.0:9002");
        assert_ne!(
            config.grpc_listen_addr, config.membership_listen_addr,
            "the membership plane must not share the data-plane port"
        );
    }

    /// ADR-0028 D1: an explicit membership_listen_addr round-trips
    /// through serde.
    #[test]
    fn explicit_membership_listen_addr_round_trips() {
        let toml = r#"
            node_id = "node-1"
            membership_listen_addr = "10.0.0.2:9002"
        "#;
        let config: NodeConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.membership_listen_addr, "10.0.0.2:9002");
    }

    #[test]
    fn default_config_seed_nodes_is_empty() {
        let config = NodeConfig::default();
        assert!(config.gossip.seed_nodes.is_empty());
    }

    #[test]
    fn default_seal_batching_knobs_are_sensible() {
        let config = NodeConfig::default();
        assert_eq!(config.seal_fsync_batch_timeout_ms, 10);
        assert_eq!(config.seal_fsync_max_waiters, 8);
    }

    #[test]
    fn wal_config_defaults_are_sensible() {
        let config = WalConfig::default();
        assert_eq!(config.max_file_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.fsync_batch_timeout_ms, 5);
    }

    #[test]
    fn ring_config_default_values() {
        let config = RingConfig::default();
        assert_eq!(config.vnodes_per_node, 256);
        assert_eq!(config.replication_factor, 3);
    }

    #[test]
    fn metadata_config_default_values() {
        let config = MetadataConfig::default();
        assert_eq!(config.block_cache_size, 128 * 1024 * 1024);
        assert_eq!(config.memtable_size, 64 * 1024 * 1024);
        assert_eq!(config.data_dir, std::path::PathBuf::from("/var/lib/oceanfs/metadata"));
    }

    #[test]
    fn default_read_cache_segments_is_false() {
        let config = NodeConfig::default();
        assert!(!config.read_cache_segments);
    }

    #[test]
    fn default_io_uring_enabled_matches_platform() {
        let config = NodeConfig::default();
        assert_eq!(config.io_uring_enabled, cfg!(target_os = "linux"));
    }

    #[test]
    fn default_segment_cache_max_entries_is_64() {
        let config = NodeConfig::default();
        assert_eq!(config.segment_cache_max_entries, 64);
    }

    #[test]
    fn default_background_io_class_idle_matches_platform() {
        let config = NodeConfig::default();
        assert_eq!(config.background_io_class_idle, cfg!(target_os = "linux"));
    }

    #[test]
    fn default_background_cpu_sched_idle_matches_platform() {
        let config = NodeConfig::default();
        assert_eq!(config.background_cpu_sched_idle, cfg!(target_os = "linux"));
    }

    // ── Item 1: GC config tests ──

    #[test]
    fn gc_config_default_values() {
        let config = NodeConfig::default();
        assert!((config.gc_compact_threshold - 0.5).abs() < f64::EPSILON);
        assert_eq!(config.gc_max_concurrent_compactions, 4);
        assert_eq!(config.gc_compaction_queue_capacity, 64);
    }

    #[test]
    fn gc_config_serde_roundtrip() {
        let config = NodeConfig {
            gc_compact_threshold: 0.3,
            gc_max_concurrent_compactions: 8,
            gc_compaction_queue_capacity: 128,
            ..NodeConfig::default()
        };
        let toml_str = toml::to_string(&config).unwrap();
        let roundtripped: NodeConfig = toml::from_str(&toml_str).unwrap();
        assert!((roundtripped.gc_compact_threshold - 0.3).abs() < f64::EPSILON);
        assert_eq!(roundtripped.gc_max_concurrent_compactions, 8);
        assert_eq!(roundtripped.gc_compaction_queue_capacity, 128);
    }

    // ── Item 2: Scrub / AE / Heal config tests ──

    #[test]
    fn ae_peer_count_default_is_1() {
        let config = NodeConfig::default();
        assert_eq!(config.ae_peer_count, 1);
    }

    #[test]
    fn heal_parallel_segments_default_is_16() {
        let config = NodeConfig::default();
        assert_eq!(config.heal_parallel_segments, 16);
    }

    #[test]
    fn scrub_parallel_nodes_default_is_0() {
        let config = NodeConfig::default();
        assert_eq!(config.scrub_parallel_nodes, 0);
    }

    // ── Item 3: Cache config tests ──

    #[test]
    fn object_cache_defaults() {
        let config = NodeConfig::default();
        assert!(config.object_cache_enabled);
        assert_eq!(config.object_cache_size_bytes, 512 * 1024 * 1024);
        assert_eq!(config.object_cache_ttl_ms, 60_000);
        assert_eq!(config.object_cache_max_blob_size, 1024 * 1024);
    }

    #[test]
    fn metadata_cache_defaults() {
        let config = NodeConfig::default();
        assert!(config.metadata_cache_enabled);
        assert_eq!(config.metadata_cache_size_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.metadata_cache_ttl_ms, 300_000);
    }

    #[test]
    fn negative_cache_defaults() {
        let config = NodeConfig::default();
        assert!(config.negative_cache_enabled);
        assert_eq!(config.negative_cache_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.negative_cache_rebuild_sec, 3600);
    }

    #[test]
    fn prefetch_defaults() {
        let config = NodeConfig::default();
        assert_eq!(config.prefetch_after_list, 16);
        assert_eq!(config.prefetch_after_get, 4);
    }

    /// T3.9: Eviction policy config serde roundtrip.
    #[test]
    fn test_eviction_policy_config_serde_roundtrip() {
        let config = NodeConfig {
            eviction_policy_l1: crate::EvictionPolicyType::Gdsf,
            eviction_policy_l2: crate::EvictionPolicyType::TtlLru,
            ..NodeConfig::default()
        };
        let toml_str = toml::to_string(&config).unwrap();
        let roundtripped: NodeConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(roundtripped.eviction_policy_l1, crate::EvictionPolicyType::Gdsf);
        assert_eq!(roundtripped.eviction_policy_l2, crate::EvictionPolicyType::TtlLru);
    }

    // ── Item 4: OperationTimeouts config test ──

    #[test]
    fn operation_timeouts_field_default() {
        let config = NodeConfig::default();
        assert_eq!(config.operation_timeouts.wal_write_ms, 500);
        assert_eq!(config.operation_timeouts.segment_seal_ms, 120_000);
        assert_eq!(config.operation_timeouts.gossip_roundtrip_ms, 10_000);
    }

    // ── Item 5: Buffer pool config tests ──

    #[test]
    fn buffer_pool_defaults() {
        let config = NodeConfig::default();
        assert_eq!(config.buffer_pool_chunk_bytes, 65536);
        assert_eq!(config.buffer_pool_max_chunks, 1024);
    }

    // ── Item 8: Shard count config tests ──

    #[test]
    fn shard_count_defaults() {
        let config = NodeConfig::default();
        assert_eq!(config.segment_shard_count, 0); // auto
        assert_eq!(config.segment_shard_count_max, 16);
    }

    // ── Item 10: FetchStrategy config test ──

    #[test]
    fn default_fetch_strategy_is_local_first() {
        let config = NodeConfig::default();
        assert_eq!(config.default_fetch_strategy, crate::FetchStrategy::LocalFirst);
    }
}
