//! Configuration types for the OceanFS system.
//!
//! Includes sizing configs (`SegmentSizeConfig`, `SizeTier`), network
//! configs (`RpcConfig`, `GossipConfig`), storage configs (`PoolConfig`,
//! `HealConfig`), compression configs (`CompressConfig`, `CompressionTier`,
//! `NvcompConfig`, `NvcompCodec`), and GPU configs (`GpuConfig`).

// ---------------------------------------------------------------------------
// SizeTier
// ---------------------------------------------------------------------------

/// The segment tier for a blob, based on its size.
///
/// Determines how the blob is stored: inline in metadata, packed into
/// a small segment, one blob per standard segment, or split across
/// multiple segments.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{SizeTier, SegmentSizeConfig};
///
/// let config = SegmentSizeConfig::default();
/// assert_eq!(config.classify(1024), SizeTier::Inline);
/// assert_eq!(config.classify(65536), SizeTier::Small);
/// assert_eq!(config.classify(1048576), SizeTier::Standard);
/// assert_eq!(config.classify(10485760), SizeTier::Multi);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum SizeTier {
    /// Blob stored inline in metadata (≤ `inline_threshold_bytes`).
    Inline,
    /// Blob packed into a small segment (≤ `small_threshold_bytes`).
    Small,
    /// One blob per standard segment (≤ `default_target_size`).
    Standard,
    /// Blob split across multiple segments (> `default_target_size`).
    Multi,
}

// ---------------------------------------------------------------------------
// SegmentSizeConfig
// ---------------------------------------------------------------------------

/// Configuration for tiered segment sizing.
///
/// Controls the four-tier storage strategy defined in ADR-0001:
/// inline, small segment, standard segment, and multi-segment.
///
/// # Examples
///
/// ```
/// use oceanfs_core::SegmentSizeConfig;
///
/// let config = SegmentSizeConfig::default();
/// assert_eq!(config.inline_threshold_bytes, 4096);
/// assert_eq!(config.small_threshold_bytes, 262144);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SegmentSizeConfig {
    /// Maximum blob size for inline storage (default 4 KB).
    pub inline_threshold_bytes: u64,
    /// Maximum blob size for small segment packing (default 256 KB).
    pub small_threshold_bytes: u64,
    /// Target total size for a small segment (default 64 KB).
    pub small_target_size: u64,
    /// Target total size for a standard segment (default 4 MB).
    pub default_target_size: u64,
}

impl Default for SegmentSizeConfig {
    fn default() -> Self {
        Self {
            inline_threshold_bytes: 4096,
            small_threshold_bytes: 262144,
            small_target_size: 65536,
            default_target_size: 4194304,
        }
    }
}

impl SegmentSizeConfig {
    /// Classifies a blob size into its appropriate storage tier.
    ///
    /// # Panics
    ///
    /// In debug builds: panics if `blob_size` is zero.
    pub fn classify(&self, blob_size: u64) -> SizeTier {
        debug_assert!(blob_size > 0, "blob size must be > 0");
        if blob_size <= self.inline_threshold_bytes {
            SizeTier::Inline
        } else if blob_size <= self.small_threshold_bytes {
            SizeTier::Small
        } else if blob_size <= self.default_target_size {
            SizeTier::Standard
        } else {
            SizeTier::Multi
        }
    }
}

// ---------------------------------------------------------------------------
// GossipConfig
// ---------------------------------------------------------------------------

/// Configuration for the SWIM gossip membership protocol.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct GossipConfig {
    /// Interval between gossip rounds in milliseconds.
    pub interval_ms: u64,
    /// Time in SUSPECT state before declaring DEAD.
    pub suspicion_timeout_ms: u64,
    /// Total time before declaring DEAD.
    pub failure_timeout_ms: u64,
    /// Number of peers to route indirect pings through.
    pub indirect_ping_count: u8,
    /// Number of random alive peers each gossip round pushes to
    /// (ADR-0028 D4: bounded fanout — the full-state fanout-all push is
    /// replaced by k-random push-pull rounds). Capped at alive-1.
    pub fanout_k: u8,
    /// Bootstrap nodes for cluster discovery (host:port pairs).
    pub seed_nodes: Vec<String>,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            interval_ms: 1000,
            suspicion_timeout_ms: 5000,
            failure_timeout_ms: 15000,
            indirect_ping_count: 3,
            fanout_k: 3,
            seed_nodes: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// RpcConfig
// ---------------------------------------------------------------------------

/// Configuration for the gRPC connection pool.
///
/// Controls per-peer channel pooling, keepalive, idle eviction, and timeouts.
///
/// # Examples
///
/// ```
/// use oceanfs_core::RpcConfig;
///
/// let config = RpcConfig::default();
/// assert_eq!(config.pool_size_per_peer, 4);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct RpcConfig {
    /// Number of gRPC channels to maintain per peer node.
    pub pool_size_per_peer: usize,
    /// Keepalive interval in seconds for idle channels.
    pub keepalive_sec: u64,
    /// Maximum number of idle channels across all peers.
    pub max_idle_connections: usize,
    /// Connection establishment timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// Default per-request timeout in milliseconds.
    pub request_timeout_ms: u64,
    /// Optional path to TLS certificate for mTLS.
    pub tls_cert_path: Option<std::path::PathBuf>,
    /// Interval in seconds for periodic health checks on all peer channels.
    /// Set to 0 to disable periodic health checking.
    pub health_check_interval_sec: u64,
    /// SO_BUSY_POLL timeout in microseconds for gRPC sockets (Linux only,
    /// default 50). When non-zero, the kernel busy-waits for up to this many
    /// microseconds for data on the socket, eliminating interrupt wakeup
    /// latency for small RPCs. Set to 0 to disable busy polling.
    /// Requires Linux 3.11+.
    pub busy_poll_us: u32,
    /// Enable TCP_QUICKACK on gRPC sockets (Linux only, default true).
    /// Disables delayed ACKs, eliminating up to 40ms of ack delay per RPC
    /// round-trip. Ideal for independent request-response patterns where
    /// ACKs cannot piggyback on response data.
    pub quickack: bool,
    /// Number of gRPC server sockets to bind via SO_REUSEPORT (Linux only).
    /// When > 0, creates N sockets on the same port; the kernel distributes
    /// connections via 4-tuple hash, eliminating single-accept-queue
    /// contention. Set to 0 to auto-detect (num_cpus). Set to 1 to disable.
    /// Requires Linux 3.9+.
    pub reuseport_sockets: usize,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            pool_size_per_peer: 4,
            keepalive_sec: 30,
            max_idle_connections: 256,
            connect_timeout_ms: 5000,
            request_timeout_ms: 30000,
            tls_cert_path: None,
            health_check_interval_sec: 30,
            busy_poll_us: 50,
            quickack: cfg!(target_os = "linux"),
            reuseport_sockets: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// PoolConfig
// ---------------------------------------------------------------------------

/// Configuration for the active segment pool.
///
/// Controls the number of concurrent active segments and per-core sharding
/// to decouple append latency from EC encode time.
///
/// # Examples
///
/// ```
/// use oceanfs_core::PoolConfig;
///
/// let config = PoolConfig::default();
/// assert_eq!(config.active_pool_size, 4);
/// assert_eq!(config.shard_count, 4);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct PoolConfig {
    /// Number of active segments per shard (default 4).
    /// More pool slots allow concurrent appends while segments are being sealed.
    pub active_pool_size: usize,
    /// Number of per-core shards for contention reduction (default 4).
    pub shard_count: usize,
    /// Maximum number of in-flight EC encodes (bounded by semaphore).
    pub max_inflight_encodes: usize,
    /// Capacity of the EC encoding work queue (backpressure channel).
    pub encode_queue_capacity: usize,
    /// When `true` (and an EC codec is configured), sealed segments carry
    /// EC parity: the seal worker encodes the segment's complete stripes
    /// at seal time on the blocking pool (single scheduler — the write
    /// path never touches a second thread pool) and persists the shards
    /// in the segment file for read-path repair. Default: `true`.
    pub ec_streaming_encode: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            active_pool_size: 4,
            shard_count: 4,
            max_inflight_encodes: 8,
            encode_queue_capacity: 64,
            ec_streaming_encode: true,
        }
    }
}

// ---------------------------------------------------------------------------
// CompressionTier
// ---------------------------------------------------------------------------

/// Compression acceleration tier.
///
/// Controls which compression backend is used for segment data.
/// Per ADR-0007, compression uses a two-level governance model:
/// the node sets a ceiling (maximum tier available), and per-bucket
/// configuration can only select from or downgrade from the node's
/// ceiling — it cannot upgrade.
///
/// ## Capability Ordering
///
/// ```text
/// GpuNvcomp > CpuIgzip > CpuZstd > None
/// ```
///
/// A bucket requesting a tier higher than the node ceiling is capped:
/// `effective_tier = min(requested, ceiling)`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::CompressionTier;
///
/// let tier = CompressionTier::Auto;
/// assert!(matches!(tier, CompressionTier::Auto));
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[non_exhaustive]
pub enum CompressionTier {
    /// No compression — disable compression entirely for this node or bucket.
    /// Lowest tier in the capability ordering.
    None,
    /// CPU zstd (always available). Tier 0 — terminal fallback.
    CpuZstd,
    /// ISA-L igzip (requires isa-l feature + AVX-512). Tier 1.
    CpuIgzip,
    /// nvCOMP GPU batch compression (requires cuda feature + nvCOMP library). Tier 2.
    GpuNvcomp,
    /// Automatically select the best available compression backend.
    /// Probe order: nvCOMP > ISA-L igzip > CPU zstd.
    Auto,
}

// ---------------------------------------------------------------------------
// GpuConfig
// ---------------------------------------------------------------------------

/// Configuration for GPU-accelerated operations.
///
/// Controls CUDA device selection, batch sizes, concurrency limits,
/// and error recovery behavior.
///
/// # Examples
///
/// ```
/// use oceanfs_core::GpuConfig;
///
/// let config = GpuConfig::default();
/// assert_eq!(config.device_id, 0);
/// assert_eq!(config.batch_size, 64);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct GpuConfig {
    /// CUDA device index (default 0).
    pub device_id: usize,
    /// Number of stripes per GPU kernel launch (default 64).
    pub batch_size: usize,
    /// Minimum segment size in bytes for GPU offload (default 100 MB).
    /// Segments smaller than this use the CPU path regardless of tier.
    pub min_segment_size: u64,
    /// Maximum concurrent GPU operations (semaphore permits, default 1).
    pub max_concurrent_ops: usize,
    /// Seconds to wait before retrying after a GPU failure (default 60).
    pub cooldown_sec: u64,
}

impl Default for GpuConfig {
    fn default() -> Self {
        Self {
            device_id: 0,
            batch_size: 64,
            min_segment_size: 104_857_600, // 100 MB
            max_concurrent_ops: 1,
            cooldown_sec: 60,
        }
    }
}

// ---------------------------------------------------------------------------
// CompressConfig, NvcompCodec, NvcompConfig
// ---------------------------------------------------------------------------

/// Per-bucket compression configuration.
///
/// Controls the compression tier and level applied to segment data before
/// erasure coding. Per ADR-0007, the effective tier is capped by the
/// node-level `CompressionConfig` ceiling: a bucket requesting
/// `GpuNvcomp` on a `CpuZstd`-only node will get `CpuZstd`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{CompressConfig, CompressionTier};
///
/// let config = CompressConfig::default();
/// // Default is OFF — buckets must opt in via `tier`.
/// assert_eq!(config.tier, CompressionTier::None);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompressConfig {
    /// Compression tier to use: None (off), Auto, CpuZstd, CpuIgzip, or
    /// GpuNvcomp. `None` disables compression for the bucket.
    pub tier: CompressionTier,
    /// Compression level (0-22 for zstd, 0-3 for igzip).
    /// Higher levels produce smaller output at the cost of more CPU/GPU time.
    pub level: u32,
    /// nvCOMP-specific configuration (only used when `tier` is GpuNvcomp).
    pub nvcomp: Option<NvcompConfig>,
    /// Chunks smaller than this many bytes are stored uncompressed
    /// (small payloads compress poorly and cost CPU for nothing).
    #[serde(default = "default_min_chunk_bytes")]
    pub min_chunk_bytes: usize,
}

/// Default compression skip threshold: 1 KiB.
pub fn default_min_chunk_bytes() -> usize {
    1024
}

impl Default for CompressConfig {
    fn default() -> Self {
        Self {
            tier: CompressionTier::None,
            level: 3,
            nvcomp: None,
            min_chunk_bytes: default_min_chunk_bytes(),
        }
    }
}

/// nvCOMP GPU compression codec selection.
///
/// # Examples
///
/// ```
/// use oceanfs_core::NvcompCodec;
///
/// let codec = NvcompCodec::Lz4;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum NvcompCodec {
    /// LZ4 compression (fast, moderate ratio).
    Lz4,
    /// Snappy compression (fast, moderate ratio).
    Snappy,
    /// Zstandard compression (slower, high ratio).
    Zstd,
}

/// Configuration for nvCOMP GPU-accelerated compression.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{NvcompConfig, NvcompCodec};
///
/// let config = NvcompConfig::default();
/// assert_eq!(config.codec, NvcompCodec::Lz4);
/// assert_eq!(config.batch_size, 16);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct NvcompConfig {
    /// Compression codec to use (default: LZ4).
    pub codec: NvcompCodec,
    /// Number of segments to batch for a single GPU kernel launch (default 16).
    pub batch_size: usize,
    /// CUDA device index (default 0).
    pub device_id: usize,
}

impl Default for NvcompConfig {
    fn default() -> Self {
        Self { codec: NvcompCodec::Lz4, batch_size: 16, device_id: 0 }
    }
}

// ---------------------------------------------------------------------------
// HealConfig
// ---------------------------------------------------------------------------

/// Configuration for the EC heal dispatch pipeline.
///
/// Controls concurrency, retry behavior, and throughput throttling for
/// the background heal worker that repairs corrupt segment shards.
///
/// # Examples
///
/// ```
/// use oceanfs_core::HealConfig;
///
/// let config = HealConfig::default();
/// assert_eq!(config.max_concurrent_heals(), 4);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct HealConfig {
    /// Maximum number of concurrent heal operations (bounded via semaphore).
    max_concurrent_heals: usize,
    /// Maximum retry attempts for a single heal request before giving up.
    heal_retry_limit: u32,
    /// Throughput limit in bytes per second (0 = unlimited).
    heal_throttle_bytes_sec: u64,
    /// Capacity of the bounded heal queue channel.
    queue_capacity: usize,
}

impl Default for HealConfig {
    fn default() -> Self {
        Self {
            max_concurrent_heals: 4,
            heal_retry_limit: 3,
            heal_throttle_bytes_sec: 0,
            queue_capacity: 256,
        }
    }
}

impl HealConfig {
    /// Returns the maximum number of concurrent heal operations.
    pub fn max_concurrent_heals(&self) -> usize {
        self.max_concurrent_heals
    }

    /// Returns the maximum retry attempts per heal request.
    pub fn heal_retry_limit(&self) -> u32 {
        self.heal_retry_limit
    }

    /// Returns the throughput throttle in bytes per second.
    pub fn heal_throttle_bytes_sec(&self) -> u64 {
        self.heal_throttle_bytes_sec
    }

    /// Returns the capacity of the bounded heal queue.
    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// Sets the maximum number of concurrent heal operations.
    #[must_use]
    pub fn with_max_concurrent_heals(mut self, value: usize) -> Self {
        self.max_concurrent_heals = value;
        self
    }

    /// Sets the maximum retry attempts per heal request.
    #[must_use]
    pub fn with_heal_retry_limit(mut self, value: u32) -> Self {
        self.heal_retry_limit = value;
        self
    }

    /// Sets the throughput throttle in bytes per second.
    #[must_use]
    pub fn with_heal_throttle_bytes_sec(mut self, value: u64) -> Self {
        self.heal_throttle_bytes_sec = value;
        self
    }

    /// Sets the capacity of the bounded heal queue.
    #[must_use]
    pub fn with_queue_capacity(mut self, value: usize) -> Self {
        self.queue_capacity = value;
        self
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // -- SizeTier / SegmentSizeConfig --

    #[test]
    fn classify_1kb_is_inline() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(1024), SizeTier::Inline);
    }

    #[test]
    fn classify_4096_is_inline_boundary() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(4096), SizeTier::Inline);
    }

    #[test]
    fn classify_4097_is_small() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(4097), SizeTier::Small);
    }

    #[test]
    fn classify_256kb_is_small_boundary() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(262144), SizeTier::Small);
    }

    #[test]
    fn classify_256kb_plus_one_is_standard() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(262145), SizeTier::Standard);
    }

    #[test]
    fn classify_4mb_is_standard_boundary() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(4194304), SizeTier::Standard);
    }

    #[test]
    fn classify_4mb_plus_one_is_multi() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(4194305), SizeTier::Multi);
    }

    #[test]
    fn classify_10mb_is_multi() {
        let config = SegmentSizeConfig::default();
        assert_eq!(config.classify(10485760), SizeTier::Multi);
    }

    #[test]
    #[should_panic]
    fn classify_zero_panics_in_debug() {
        let config = SegmentSizeConfig::default();
        config.classify(0);
    }

    // -- GossipConfig --

    #[test]
    fn gossip_config_default_values() {
        let cfg = GossipConfig::default();
        assert_eq!(cfg.interval_ms, 1000);
        assert_eq!(cfg.suspicion_timeout_ms, 5000);
        assert_eq!(cfg.failure_timeout_ms, 15000);
        assert_eq!(cfg.indirect_ping_count, 3);
        assert!(cfg.seed_nodes.is_empty());
    }

    // -- RpcConfig --

    #[test]
    fn rpc_config_default_values() {
        let cfg = RpcConfig::default();
        assert_eq!(cfg.pool_size_per_peer, 4);
        assert_eq!(cfg.keepalive_sec, 30);
        assert_eq!(cfg.max_idle_connections, 256);
        assert_eq!(cfg.connect_timeout_ms, 5000);
        assert_eq!(cfg.request_timeout_ms, 30000);
        assert!(cfg.tls_cert_path.is_none());
    }

    #[test]
    fn rpc_config_socket_opts_defaults() {
        let cfg = RpcConfig::default();
        assert_eq!(cfg.busy_poll_us, 50);
        assert_eq!(cfg.quickack, cfg!(target_os = "linux"));
        assert_eq!(cfg.reuseport_sockets, 0);
    }

    // -- PoolConfig --

    #[test]
    fn pool_config_default_values() {
        let cfg = PoolConfig::default();
        assert_eq!(cfg.active_pool_size, 4);
        assert_eq!(cfg.shard_count, 4);
        assert_eq!(cfg.max_inflight_encodes, 8);
        assert_eq!(cfg.encode_queue_capacity, 64);
    }

    #[test]
    fn pool_config_custom_sizes() {
        let cfg = PoolConfig {
            active_pool_size: 16,
            shard_count: 8,
            max_inflight_encodes: 32,
            encode_queue_capacity: 256,
            ..Default::default()
        };
        assert_eq!(cfg.active_pool_size, 16);
        assert_eq!(cfg.shard_count, 8);
    }

    // -- CompressionTier --

    #[test]
    fn compression_tier_variants_exist() {
        let _tiers = [
            CompressionTier::Auto,
            CompressionTier::CpuZstd,
            CompressionTier::CpuIgzip,
            CompressionTier::GpuNvcomp,
        ];
    }

    #[test]
    fn compression_tier_auto_is_not_cpu_zstd() {
        assert_ne!(CompressionTier::Auto, CompressionTier::CpuZstd);
    }

    // -- GpuConfig --

    #[test]
    fn gpu_config_default_values() {
        let cfg = GpuConfig::default();
        assert_eq!(cfg.device_id, 0);
        assert_eq!(cfg.batch_size, 64);
        assert_eq!(cfg.min_segment_size, 104_857_600);
        assert_eq!(cfg.max_concurrent_ops, 1);
        assert_eq!(cfg.cooldown_sec, 60);
    }

    #[test]
    fn gpu_config_custom() {
        let cfg = GpuConfig {
            device_id: 1,
            batch_size: 128,
            min_segment_size: 50_000_000,
            max_concurrent_ops: 4,
            cooldown_sec: 120,
        };
        assert_eq!(cfg.device_id, 1);
        assert_eq!(cfg.batch_size, 128);
        assert_eq!(cfg.max_concurrent_ops, 4);
    }

    // ── Item 2: HealConfig (T2.2) ──

    #[test]
    fn heal_config_default_values() {
        let config = HealConfig::default();
        assert_eq!(config.max_concurrent_heals(), 4);
        assert_eq!(config.heal_retry_limit(), 3);
        assert_eq!(config.heal_throttle_bytes_sec(), 0);
        assert_eq!(config.queue_capacity(), 256);
    }

    /// T2.2: HealConfig builder with_throttle_bytes_sec is respected.
    #[test]
    fn test_heal_config_throttled() {
        let config = HealConfig::default().with_heal_throttle_bytes_sec(1024);
        assert_eq!(config.heal_throttle_bytes_sec(), 1024);

        let config = config.with_max_concurrent_heals(16);
        assert_eq!(config.max_concurrent_heals(), 16);
        // Other fields unchanged.
        assert_eq!(config.heal_retry_limit(), 3);
    }

    /// NodeConfig→HealConfig flow: builder pattern preserves explicit values.
    #[test]
    fn test_heal_config_from_node_config_flow() {
        // Simulate the node.rs construction pattern:
        //   let heal_config = HealConfig::default()
        //       .with_max_concurrent_heals(config.heal_parallel_segments)
        //       .with_heal_throttle_bytes_sec(config.heal_throttle_bytes_sec);
        let heal_parallel_segments = 16;
        let heal_throttle_bytes_sec = 1048576;
        let config = HealConfig::default()
            .with_max_concurrent_heals(heal_parallel_segments)
            .with_heal_throttle_bytes_sec(heal_throttle_bytes_sec);
        assert_eq!(config.max_concurrent_heals(), 16);
        assert_eq!(config.heal_throttle_bytes_sec(), 1048576);
    }
}
